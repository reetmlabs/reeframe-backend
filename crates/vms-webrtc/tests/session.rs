//! A full session on localhost: a second webrtcbin plays the browser. It
//! offers a receive-only video track and a "control" DataChannel, and talks
//! to the session manager directly instead of through a Coordinator.

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gst::prelude::*;
use gst_webrtc::{
    WebRTCDataChannel, WebRTCICEConnectionState, WebRTCRTPTransceiver,
    WebRTCRTPTransceiverDirection, WebRTCSDPType, WebRTCSessionDescription,
};
use gstreamer as gst;
use gstreamer_sdp as gst_sdp;
use gstreamer_webrtc as gst_webrtc;
use reeframe_protocol::Message;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;
use vms_core::{AuthClaims, AuthProvider, VmsError};
use vms_webrtc::{Config, SessionManager};

const ELEMENTS: [&str; 7] = [
    "webrtcbin",
    "nicesrc",
    "nicesink",
    "dtlssrtpenc",
    "sctpenc",
    "openh264enc",
    "rtph264depay",
];

/// True when every element a session needs is installed. Missing elements
/// skip the test locally but fail it in CI, where they are installed.
fn media_available() -> bool {
    gst::init().unwrap();
    let missing: Vec<&str> = ELEMENTS
        .into_iter()
        .filter(|name| gst::ElementFactory::find(name).is_none())
        .collect();
    if missing.is_empty() {
        return true;
    }
    assert!(
        std::env::var_os("CI").is_none(),
        "GStreamer elements missing in CI: {missing:?}"
    );
    eprintln!(
        "Skipping the WebRTC media test, missing GStreamer elements: {missing:?} \
         (install gstreamer1.0-nice and gstreamer1.0-plugins-bad)"
    );
    false
}

struct AnyToken;

#[async_trait::async_trait]
impl AuthProvider for AnyToken {
    async fn verify_token(&self, _token: &str) -> Result<AuthClaims, VmsError> {
        Ok(AuthClaims {
            user_id: uuid::Uuid::nil(),
            username: "browser".into(),
            roles: vec!["viewer".into()],
            expires_at: i64::MAX,
        })
    }

    fn provider_name(&self) -> &'static str {
        "any"
    }
}

async fn promise(emit: impl FnOnce(&gst::Promise)) -> Option<gst::Structure> {
    let (tx, rx) = oneshot::channel();
    let promise = gst::Promise::with_change_func(move |reply| {
        let _ = tx.send(reply.ok().flatten().map(ToOwned::to_owned));
    });
    emit(&promise);
    timeout(Duration::from_secs(5), rx).await.unwrap().unwrap()
}

/// The browser stand-in: a webrtcbin with a receive-only H.264 transceiver
/// and a "control" DataChannel, counting the video buffers it receives.
struct Browser {
    pipeline: gst::Pipeline,
    webrtc: gst::Element,
    buffers: Arc<AtomicUsize>,
    echoes: Arc<Mutex<Vec<String>>>,
    _channel: WebRTCDataChannel,
}

impl Browser {
    fn new(candidates: mpsc::UnboundedSender<(u32, String)>) -> Self {
        let pipeline = gst::Pipeline::new();
        let webrtc = gst::ElementFactory::make("webrtcbin")
            .property_from_str("bundle-policy", "max-bundle")
            .build()
            .unwrap();
        pipeline.add(&webrtc).unwrap();

        let caps = gst::Caps::from_str(
            "application/x-rtp,media=video,encoding-name=H264,payload=102,clock-rate=90000,\
             packetization-mode=(string)1,profile-level-id=(string)42e01f",
        )
        .unwrap();
        webrtc.emit_by_name::<WebRTCRTPTransceiver>(
            "add-transceiver",
            &[&WebRTCRTPTransceiverDirection::Recvonly, &caps],
        );

        let buffers = Arc::new(AtomicUsize::new(0));
        let pipeline_weak = pipeline.downgrade();
        let counter = buffers.clone();
        webrtc.connect_pad_added(move |_, pad| {
            if pad.direction() != gst::PadDirection::Src {
                return;
            }
            let Some(pipeline) = pipeline_weak.upgrade() else {
                return;
            };
            let sink = gst::ElementFactory::make("fakesink")
                .property("sync", false)
                .build()
                .unwrap();
            pipeline.add(&sink).unwrap();
            sink.sync_state_with_parent().unwrap();
            pad.link(&sink.static_pad("sink").unwrap()).unwrap();
            let counter = counter.clone();
            pad.add_probe(gst::PadProbeType::BUFFER, move |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
                gst::PadProbeReturn::Ok
            });
        });

        webrtc.connect("on-ice-candidate", false, move |values| {
            let mline = values[1].get::<u32>().unwrap();
            let candidate = values[2].get::<String>().unwrap();
            let _ = candidates.send((mline, candidate));
            None
        });

        pipeline.set_state(gst::State::Playing).unwrap();
        let channel = webrtc
            .emit_by_name::<Option<WebRTCDataChannel>>(
                "create-data-channel",
                &[&"control", &None::<gst::Structure>],
            )
            .expect("webrtcbin created no DataChannel");
        let echoes = Arc::new(Mutex::new(Vec::new()));
        let received = echoes.clone();
        channel.connect_on_message_string(move |_, text| {
            received
                .lock()
                .unwrap()
                .push(text.unwrap_or_default().to_owned());
        });
        channel.connect_on_open(|channel| {
            channel.send_string(Some("hello"));
        });

        Self {
            pipeline,
            webrtc,
            buffers,
            echoes,
            _channel: channel,
        }
    }

    async fn offer(&self) -> String {
        let reply = promise(|p| {
            self.webrtc
                .emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, p]);
        })
        .await
        .expect("no offer");
        let offer = reply.get::<WebRTCSessionDescription>("offer").unwrap();
        promise(|p| {
            self.webrtc
                .emit_by_name::<()>("set-local-description", &[&offer, p]);
        })
        .await;
        offer.sdp().as_text().unwrap()
    }

    async fn accept(&self, answer: &str) {
        let sdp = gst_sdp::SDPMessage::parse_buffer(answer.as_bytes()).unwrap();
        let answer = WebRTCSessionDescription::new(WebRTCSDPType::Answer, sdp);
        promise(|p| {
            self.webrtc
                .emit_by_name::<()>("set-remote-description", &[&answer, p]);
        })
        .await;
    }

    fn buffers(&self) -> usize {
        self.buffers.load(Ordering::SeqCst)
    }

    fn ice_connected(&self) -> bool {
        matches!(
            self.webrtc
                .property::<WebRTCICEConnectionState>("ice-connection-state"),
            WebRTCICEConnectionState::Connected | WebRTCICEConnectionState::Completed
        )
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

async fn wait_for(what: &str, limit: Duration, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + limit;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_browser_gets_video_and_a_data_channel_echo_until_the_session_closes() {
    if !media_available() {
        return;
    }
    let (inbound, inbound_rx) = mpsc::unbounded_channel();
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel();
    let manager = SessionManager::new(Config::default(), Arc::new(AnyToken), outbound).unwrap();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(manager.clone().run(inbound_rx, shutdown.clone()));

    let (candidates, mut candidates_rx) = mpsc::unbounded_channel();
    let browser = Browser::new(candidates);
    let sdp = browser.offer().await;
    inbound
        .send(Message::SessionOffer {
            session_id: "s1".into(),
            sdp,
            ice_servers: Vec::new(),
            site_token: "token".into(),
        })
        .unwrap();
    // Trickled after the offer, like the Coordinator relays them.
    tokio::spawn({
        let inbound = inbound.clone();
        async move {
            while let Some((mline, candidate)) = candidates_rx.recv().await {
                let _ = inbound.send(Message::SessionCandidate {
                    session_id: "s1".into(),
                    candidate,
                    sdp_mid: None,
                    sdp_mline_index: Some(mline),
                });
            }
        }
    });

    let answer = match timeout(Duration::from_secs(10), outbound_rx.recv()).await {
        Ok(Some(Message::SessionAnswer { session_id, sdp })) => {
            assert_eq!(session_id, "s1");
            sdp
        }
        other => panic!("expected a session_answer, got {other:?}"),
    };
    assert!(
        answer.contains("a=candidate"),
        "the answer carries no candidates"
    );
    browser.accept(&answer).await;

    wait_for("ICE to connect", Duration::from_secs(10), || {
        browser.ice_connected()
    })
    .await;
    wait_for("video buffers", Duration::from_secs(10), || {
        browser.buffers() >= 10
    })
    .await;
    wait_for("the DataChannel echo", Duration::from_secs(10), || {
        browser.echoes.lock().unwrap().iter().any(|m| m == "hello")
    })
    .await;
    assert_eq!(manager.session_count(), 1);

    inbound
        .send(Message::SessionClose {
            session_id: "s1".into(),
            reason: "viewer left".into(),
        })
        .unwrap();
    wait_for("the session to end", Duration::from_secs(5), || {
        manager.session_count() == 0
    })
    .await;
    // Give buffers in flight time to land, then no new video may arrive.
    sleep(Duration::from_millis(500)).await;
    let after_close = browser.buffers();
    sleep(Duration::from_secs(1)).await;
    assert_eq!(
        browser.buffers(),
        after_close,
        "video kept flowing after close"
    );

    shutdown.cancel();
    timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap();
    assert!(
        outbound_rx.try_recv().is_err(),
        "unexpected message to the Coordinator"
    );
}
