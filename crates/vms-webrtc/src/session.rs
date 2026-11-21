//! One WebRTC session: its pipeline and the steps from offer to answer.

use std::time::Duration;

use anyhow::{anyhow, Context};
use gst::glib;
use gst::prelude::*;
use gst_webrtc::{
    WebRTCDataChannel, WebRTCICEConnectionState, WebRTCICEGatheringState, WebRTCSDPType,
    WebRTCSessionDescription,
};
use reeframe_protocol::IceServer;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Instant};

use crate::offer::Offer;

/// Limit for each webrtcbin step, which normally takes milliseconds.
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// Label of the DataChannel the client opens.
const CONTROL_CHANNEL: &str = "control";

/// Something that ends a session from inside GStreamer.
pub(crate) struct Failure {
    pub session_id: String,
    pub reason: String,
}

/// The pipeline of one session and its webrtcbin.
pub(crate) struct Media {
    pub pipeline: gst::Pipeline,
    pub webrtc: gst::Element,
}

impl Media {
    /// Builds the test pattern pipeline, sending H.264 with `payload_type`.
    /// Failures inside GStreamer are reported on `failures`.
    pub(crate) fn build(
        session_id: &str,
        payload_type: u32,
        ice_servers: &[IceServer],
        failures: mpsc::UnboundedSender<Failure>,
    ) -> anyhow::Result<Self> {
        let description = format!(
            "videotestsrc is-live=true pattern=ball \
             ! video/x-raw,width=640,height=360,framerate=15/1 \
             ! videoconvert ! queue \
             ! x264enc tune=zerolatency speed-preset=ultrafast bitrate=500 key-int-max=30 \
             ! video/x-h264,profile=constrained-baseline \
             ! rtph264pay config-interval=-1 aggregate-mode=zero-latency pt={payload_type} \
             ! application/x-rtp,media=video,encoding-name=H264,payload={payload_type},clock-rate=90000 \
             ! webrtcbin name=webrtc bundle-policy=max-bundle"
        );
        let pipeline = gst::parse::launch(&description)
            .context("cannot build the session pipeline")?
            .downcast::<gst::Pipeline>()
            .map_err(|_| anyhow!("the session pipeline is not a pipeline"))?;
        let webrtc = pipeline
            .by_name("webrtc")
            .context("the session pipeline has no webrtcbin")?;

        let urls = crate::ice::urls(ice_servers);
        if let Some(stun) = &urls.stun {
            webrtc.set_property_from_str("stun-server", stun);
        }
        for turn in &urls.turn {
            if !webrtc.emit_by_name::<bool>("add-turn-server", &[turn]) {
                tracing::warn!(session_id, "webrtcbin rejected a TURN server");
            }
        }

        watch_ice_state(&webrtc, session_id, failures.clone());
        echo_control_channel(&webrtc);
        report_pipeline_errors(&pipeline, session_id, failures);
        Ok(Self { pipeline, webrtc })
    }

    pub(crate) fn start(&self) -> anyhow::Result<()> {
        self.pipeline
            .set_state(gst::State::Playing)
            .context("cannot start the session pipeline")?;
        Ok(())
    }

    pub(crate) async fn set_remote_offer(&self, offer: Offer) -> anyhow::Result<()> {
        let description = WebRTCSessionDescription::new(WebRTCSDPType::Offer, offer.sdp);
        promise(|promise| {
            self.webrtc
                .emit_by_name::<()>("set-remote-description", &[&description, promise]);
        })
        .await
        .context("webrtcbin did not accept the offer")?;
        Ok(())
    }

    /// Creates and applies the answer, then returns it once ICE gathering is
    /// complete or `gathering_limit` has passed, so the answer carries every
    /// candidate found by then.
    pub(crate) async fn answer(&self, gathering_limit: Duration) -> anyhow::Result<String> {
        let reply = promise(|promise| {
            self.webrtc
                .emit_by_name::<()>("create-answer", &[&None::<gst::Structure>, promise]);
        })
        .await
        .context("webrtcbin could not create an answer")?
        .context("webrtcbin returned no answer")?;
        let answer = reply
            .get::<WebRTCSessionDescription>("answer")
            .context("webrtcbin returned no answer")?;
        promise(|promise| {
            self.webrtc
                .emit_by_name::<()>("set-local-description", &[&answer, promise]);
        })
        .await
        .context("webrtcbin did not accept its own answer")?;

        let deadline = Instant::now() + gathering_limit;
        while self
            .webrtc
            .property::<WebRTCICEGatheringState>("ice-gathering-state")
            != WebRTCICEGatheringState::Complete
            && Instant::now() < deadline
        {
            sleep(Duration::from_millis(20)).await;
        }

        let local = self
            .webrtc
            .property::<Option<WebRTCSessionDescription>>("local-description")
            .context("webrtcbin has no local description")?;
        local
            .sdp()
            .as_text()
            .map_err(|e| anyhow!("cannot write the answer SDP: {e}"))
    }

    pub(crate) fn add_candidate(&self, mline_index: u32, candidate: &str) {
        self.webrtc
            .emit_by_name::<()>("add-ice-candidate", &[&mline_index, &candidate]);
    }

    /// Stops the pipeline. Blocks until GStreamer has released it, so it
    /// runs off the async threads.
    pub(crate) fn stop(&self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

/// Emits a webrtcbin action with a promise and waits for its reply.
async fn promise(emit: impl FnOnce(&gst::Promise)) -> anyhow::Result<Option<gst::Structure>> {
    let (tx, rx) = oneshot::channel();
    let promise = gst::Promise::with_change_func(move |reply| {
        let _ = tx.send(reply.map(|s| s.map(ToOwned::to_owned)));
    });
    emit(&promise);
    let reply = timeout(STEP_TIMEOUT, rx)
        .await
        .map_err(|_| anyhow!("no reply in time"))?
        .map_err(|_| anyhow!("the promise was dropped"))?
        .map_err(|e| anyhow!("{e:?}"))?;
    if let Some(error) = reply
        .as_ref()
        .and_then(|s| s.get::<glib::Error>("error").ok())
    {
        return Err(anyhow!("{error}"));
    }
    Ok(reply)
}

fn watch_ice_state(
    webrtc: &gst::Element,
    session_id: &str,
    failures: mpsc::UnboundedSender<Failure>,
) {
    let session_id = session_id.to_owned();
    webrtc.connect_notify(Some("ice-connection-state"), move |webrtc, _| {
        let state = webrtc.property::<WebRTCICEConnectionState>("ice-connection-state");
        tracing::debug!(session_id, ?state, "ICE connection state changed");
        if state == WebRTCICEConnectionState::Failed {
            let _ = failures.send(Failure {
                session_id: session_id.clone(),
                reason: "ICE connection failed".into(),
            });
        }
    });
}

/// Echoes every message on the client's "control" DataChannel, the
/// placeholder for camera controls.
fn echo_control_channel(webrtc: &gst::Element) {
    webrtc.connect("on-data-channel", false, |values| {
        let channel = values[1].get::<WebRTCDataChannel>().ok()?;
        if channel.label().as_deref() != Some(CONTROL_CHANNEL) {
            return None;
        }
        // The plain send calls, since the error-returning ones need
        // GStreamer 1.22 and the package supports 1.20.
        channel.connect_on_message_string(|channel, text| channel.send_string(text));
        channel.connect_on_message_data(|channel, data| channel.send_data(data));
        None
    });
}

fn report_pipeline_errors(
    pipeline: &gst::Pipeline,
    session_id: &str,
    failures: mpsc::UnboundedSender<Failure>,
) {
    let Some(bus) = pipeline.bus() else {
        return;
    };
    let session_id = session_id.to_owned();
    // Nothing pops messages from this bus, so the handler drops them all
    // after looking for errors instead of letting them pile up.
    bus.set_sync_handler(move |_, message| {
        if let gst::MessageView::Error(error) = message.view() {
            tracing::warn!(session_id, error = %error.error(), "Session pipeline error");
            let _ = failures.send(Failure {
                session_id: session_id.clone(),
                reason: format!("pipeline error: {}", error.error()),
            });
        }
        gst::BusSyncReply::Drop
    });
}
