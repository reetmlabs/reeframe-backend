//! Dynamic motion/tamper analysis branch attached to a live camera tee.
//!
//! Uses the same attach/detach pattern as [`crate::ring_buffer_branch`] and
//! opens no connection of its own. Cameras typically allow two RTSP sessions,
//! used by the main pipeline and the sub-stream pipeline, so a third
//! connection for motion detection would fail on many cameras.
//!
//! Without a sub-stream, this decodes the main-resolution stream and
//! downscales it. That costs more CPU than decoding a sub-stream, but it is
//! still cheap compared to inference and needs no extra camera connection.
//!
//! Branch: `tee -> queue -> <software decoder> -> videorate(max 2 fps) ->
//! videoconvert -> videoscale -> capsfilter(GRAY8, MOTION_FRAME_WIDTH x
//! MOTION_FRAME_HEIGHT) -> appsink`. The decoder is picked from the tee's caps
//! when they first reach the queue. It is pinned to a software decoder because
//! `decodebin` may pick a hardware one, and a hardware decoder rejecting the
//! stream's profile errors out the whole camera pipeline. Every frame still has
//! to be decoded (inter frames need their references), but `videorate` drops
//! all but [`SAMPLE_RATE_FPS`] per second before conversion and scaling.

use std::time::Duration;

use gstreamer::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::event::{Event, TopicKey};
use vms_core::VmsError;

use crate::motion::{MotionAnalyzer, MotionSignal, MOTION_FRAME_HEIGHT, MOTION_FRAME_WIDTH};

/// Frames per second handed to the analyzer. `MotionAnalyzer` counts frames,
/// not time, so its frozen-feed and hysteresis windows assume this rate.
const SAMPLE_RATE_FPS: i32 = 2;

/// Software decoders to try for each tee codec, in order of preference.
fn decoder_candidates(caps_name: &str) -> &'static [&'static str] {
    match caps_name {
        "video/x-h264" => &["avdec_h264"],
        "video/x-h265" => &["avdec_h265"],
        "image/jpeg" => &["jpegdec"],
        "video/x-av1" => &["dav1ddec", "av1dec", "avdec_av1"],
        _ => &[],
    }
}

// -- Element name helpers --

fn queue_name(id: Uuid) -> String {
    format!("cam_{}_motionqueue", id.as_simple())
}
fn decode_name(id: Uuid) -> String {
    format!("cam_{}_motiondecode", id.as_simple())
}
fn rate_name(id: Uuid) -> String {
    format!("cam_{}_motionrate", id.as_simple())
}
fn convert_name(id: Uuid) -> String {
    format!("cam_{}_motionconvert", id.as_simple())
}
fn scale_name(id: Uuid) -> String {
    format!("cam_{}_motionscale", id.as_simple())
}
fn caps_name(id: Uuid) -> String {
    format!("cam_{}_motioncaps", id.as_simple())
}
fn sink_name(id: Uuid) -> String {
    format!("cam_{}_motionsink", id.as_simple())
}

/// Handle to a running per-camera motion-analysis branch. It keeps the
/// pipeline and camera ID so [`stop`](Self::stop) can detach the elements
/// without the caller tracking which pipeline (main or sub) it is on.
pub struct MotionHandle {
    pipeline: gstreamer::Pipeline,
    camera_id: Uuid,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl MotionHandle {
    /// The pipeline this branch is attached to.
    pub(crate) fn pipeline(&self) -> &gstreamer::Pipeline {
        &self.pipeline
    }

    /// Detach the GStreamer elements, signal the analyzer task to stop, and
    /// wait for it to exit.
    pub async fn stop(self) {
        if let Err(e) = detach(&self.pipeline, self.camera_id) {
            tracing::warn!(camera_id = %self.camera_id, error = %e, "Failed to detach motion branch");
        }
        let _ = self.shutdown_tx.send(());
        self.task.await.ok();
    }
}

/// Attach a motion-analysis branch to `tee_name`'s tee on `pipeline`.
///
/// `tee_name` is normally the sub-stream's tee, or the main pipeline's tee
/// when the camera has no usable sub-stream. Each analyzed frame produces zero
/// or more [`MotionSignal`]s, which are sent on `event_tx` as [`Event`]s
/// (`TopicKey::Camera`). That is the same `EventBus` bridge channel the
/// `vms-sources` adapters use, so this crate needs no `vms-engine` dependency.
///
/// Safe to call while the pipeline is `Playing`.
pub fn attach(
    pipeline: &gstreamer::Pipeline,
    tee_name: &str,
    camera_id: Uuid,
    event_tx: mpsc::UnboundedSender<Event>,
) -> Result<MotionHandle, VmsError> {
    let tee = pipeline.by_name(tee_name).ok_or_else(|| {
        VmsError::Media(format!("tee '{tee_name}' not found for camera {camera_id}"))
    })?;

    let queue = gstreamer::ElementFactory::make("queue")
        .name(queue_name(camera_id))
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()
        .map_err(|e| VmsError::Media(format!("motion queue: {e}")))?;

    let rate = gstreamer::ElementFactory::make("videorate")
        .name(rate_name(camera_id))
        .property("max-rate", SAMPLE_RATE_FPS)
        .property("drop-only", true)
        .build()
        .map_err(|e| VmsError::Media(format!("motion videorate: {e}")))?;

    let convert = gstreamer::ElementFactory::make("videoconvert")
        .name(convert_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("motion videoconvert: {e}")))?;

    let scale = gstreamer::ElementFactory::make("videoscale")
        .name(scale_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("motion videoscale: {e}")))?;

    let caps = gstreamer::Caps::builder("video/x-raw")
        .field("format", "GRAY8")
        .field("width", MOTION_FRAME_WIDTH as i32)
        .field("height", MOTION_FRAME_HEIGHT as i32)
        .build();
    let capsfilter = gstreamer::ElementFactory::make("capsfilter")
        .name(caps_name(camera_id))
        .property("caps", &caps)
        .build()
        .map_err(|e| VmsError::Media(format!("motion capsfilter: {e}")))?;

    let appsink = gstreamer_app::AppSink::builder()
        .name(sink_name(camera_id))
        .drop(true)
        .max_buffers(2u32)
        .sync(false)
        .build();

    pipeline
        .add_many([
            &queue,
            &rate,
            &convert,
            &scale,
            &capsfilter,
            appsink.upcast_ref::<gstreamer::Element>(),
        ])
        .map_err(|e| VmsError::Media(format!("motion add_many: {e}")))?;

    gstreamer::Element::link_many([
        &rate,
        &convert,
        &scale,
        &capsfilter,
        appsink.upcast_ref::<gstreamer::Element>(),
    ])
    .map_err(|e| VmsError::Media(format!("motion link chain: {e}")))?;

    // The tee's codec is only known once caps flow, so the decoder is built
    // and linked in front of `rate` when the first caps event leaves the queue.
    let queue_src = queue
        .static_pad("src")
        .ok_or_else(|| VmsError::Media("motion queue has no src pad".into()))?;
    let pipeline_weak = pipeline.downgrade();
    let rate_weak = rate.downgrade();
    queue_src.add_probe(gstreamer::PadProbeType::EVENT_DOWNSTREAM, move |pad, info| {
        let Some(gstreamer::PadProbeData::Event(ref event)) = info.data else {
            return gstreamer::PadProbeReturn::Ok;
        };
        let gstreamer::EventView::Caps(caps_event) = event.view() else {
            return gstreamer::PadProbeReturn::Ok;
        };
        if pad.is_linked() {
            return gstreamer::PadProbeReturn::Remove;
        }
        let (Some(pipeline), Some(rate)) = (pipeline_weak.upgrade(), rate_weak.upgrade()) else {
            return gstreamer::PadProbeReturn::Remove;
        };
        if let Err(e) = link_decoder(&pipeline, pad, &rate, caps_event.caps(), camera_id) {
            tracing::warn!(camera_id = %camera_id, error = %e, "Motion detection has no decoder for this stream");
        }
        gstreamer::PadProbeReturn::Remove
    });

    // -- Appsink callback: forward raw frames --
    // `drop = true` with a small `max_buffers` sheds frames in GStreamer
    // under load, and `try_send` on a small bounded channel sheds them again
    // if the analyzer task falls behind. A slow consumer never stalls the
    // streaming thread.
    let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(2);
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink
                    .pull_sample()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                let map = buffer
                    .map_readable()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let _ = frame_tx.try_send(map.as_slice().to_vec());
                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    // -- Bring new elements up from the sink backwards, then link the tee --
    // Data reaching an element still in `Null` gets `FLUSHING`, which stops
    // the queue's streaming task for good.
    for el in [
        appsink.upcast_ref::<gstreamer::Element>(),
        &capsfilter,
        &scale,
        &convert,
        &rate,
        &queue,
    ] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync motion element: {e}")))?;
    }

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("motion queue has no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->motionqueue: {e}")))?;

    // -- Analyzer task: owns the MotionAnalyzer, converts signals to Events --
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut analyzer = MotionAnalyzer::new(MOTION_FRAME_WIDTH, MOTION_FRAME_HEIGHT);
        loop {
            tokio::select! {
                frame = frame_rx.recv() => {
                    let Some(frame) = frame else { break };
                    for signal in analyzer.process_frame(&frame) {
                        let event = signal_to_event(camera_id, signal);
                        let _ = event_tx.send(event);
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    tracing::info!(camera_id = %camera_id, tee_name, "Motion detection branch attached");
    Ok(MotionHandle {
        pipeline: pipeline.clone(),
        camera_id,
        shutdown_tx,
        task,
    })
}

/// Detach the motion-analysis branch from camera `camera_id`'s tee.
///
/// Uses the same blocking pad probe as [`crate::ring_buffer_branch::detach`].
/// The tee is found through the queue's peer pad, so this works for whichever
/// tee (main or sub) `attach` used. Returns immediately; cleanup is
/// asynchronous. Safe to call while `Playing`, and a no-op if no branch is
/// attached.
fn detach(pipeline: &gstreamer::Pipeline, camera_id: Uuid) -> Result<(), VmsError> {
    let Some(queue) = pipeline.by_name(&queue_name(camera_id)) else {
        return Ok(());
    };

    let names = [
        decode_name(camera_id),
        rate_name(camera_id),
        convert_name(camera_id),
        scale_name(camera_id),
        caps_name(camera_id),
        sink_name(camera_id),
    ];
    let rest: Vec<gstreamer::Element> = names.iter().filter_map(|n| pipeline.by_name(n)).collect();

    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("motion queue has no sink pad".into()))?;
    let tee_src = queue_sink
        .peer()
        .ok_or_else(|| VmsError::Media("motion queue sink has no peer pad".into()))?;
    let tee = tee_src
        .parent_element()
        .ok_or_else(|| VmsError::Media("motion tee src pad has no parent element".into()))?;

    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);

    let pipeline_clone = pipeline.clone();
    let queue_sink_clone = queue_sink.clone();
    let queue_clone = queue.clone();
    let rest_clone = rest.clone();

    let probe_id = tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();

        queue_clone.set_state(gstreamer::State::Null).ok();
        pipeline_clone.remove(&queue_clone).ok();
        for el in &rest_clone {
            el.set_state(gstreamer::State::Null).ok();
            pipeline_clone.remove(el).ok();
        }

        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    let pipeline_clone2 = pipeline.clone();
    let tee_src_clone = tee_src.clone();
    std::thread::spawn(move || {
        let fired = rx.recv_timeout(Duration::from_secs(5)).is_ok();
        if fired {
            tracing::info!(camera_id = %camera_id, "Motion detection branch detached");
        } else {
            // A BLOCK_DOWNSTREAM probe only fires when a buffer or event
            // crosses the pad, so it never fires if upstream is dead. Without
            // forced removal, the elements would stay in the pipeline under
            // their fixed names and every later `attach()` for this camera
            // would fail at `add_many`. Five seconds with no frame means
            // nothing is flowing, so forcing the teardown is safe.
            tracing::warn!(
                camera_id = %camera_id,
                "motion detach probe timed out, forcing removal directly",
            );
            if let Some(id) = probe_id {
                tee_src_clone.remove_probe(id);
            }
            if let Some(peer) = queue_sink.peer() {
                peer.unlink(&queue_sink).ok();
            }
            queue.set_state(gstreamer::State::Null).ok();
            pipeline_clone2.remove(&queue).ok();
            for el in &rest {
                el.set_state(gstreamer::State::Null).ok();
                pipeline_clone2.remove(el).ok();
            }
        }
        tee.release_request_pad(&tee_src_clone);
    });

    Ok(())
}

/// Build the software decoder for `caps` and link it between the queue's
/// `queue_src` pad and `rate`. Runs on the streaming thread.
fn link_decoder(
    pipeline: &gstreamer::Pipeline,
    queue_src: &gstreamer::Pad,
    rate: &gstreamer::Element,
    caps: &gstreamer::CapsRef,
    camera_id: Uuid,
) -> Result<(), VmsError> {
    let caps_name = caps
        .structure(0)
        .map(|s| s.name().to_string())
        .unwrap_or_default();
    let factory = decoder_candidates(&caps_name)
        .iter()
        .find(|f| gstreamer::ElementFactory::find(f).is_some())
        .ok_or_else(|| VmsError::Media(format!("no software decoder for '{caps_name}'")))?;

    let decoder = gstreamer::ElementFactory::make(factory)
        .name(decode_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("motion {factory}: {e}")))?;
    // One thread is enough for a sub stream. The default spawns one per core,
    // each with its own frame buffers.
    if decoder.find_property("max-threads").is_some() {
        decoder.set_property("max-threads", 1i32);
    }
    pipeline
        .add(&decoder)
        .map_err(|e| VmsError::Media(format!("motion add {factory}: {e}")))?;
    decoder
        .link(rate)
        .map_err(|e| VmsError::Media(format!("motion link {factory}->videorate: {e}")))?;
    decoder
        .sync_state_with_parent()
        .map_err(|e| VmsError::Media(format!("sync motion {factory}: {e}")))?;
    let decoder_sink = decoder
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media(format!("motion {factory} has no sink pad")))?;
    queue_src
        .link(&decoder_sink)
        .map_err(|e| VmsError::Media(format!("motion link queue->{factory}: {e:?}")))?;
    Ok(())
}

fn signal_to_event(camera_id: Uuid, signal: MotionSignal) -> Event {
    let (event_type, payload) = match signal {
        MotionSignal::MotionStarted { changed_ratio } => (
            "motion_started",
            serde_json::json!({ "changed_ratio": changed_ratio }),
        ),
        MotionSignal::MotionStopped => ("motion_stopped", serde_json::json!({})),
        MotionSignal::SceneChange { changed_ratio } => (
            "scene_change",
            serde_json::json!({ "changed_ratio": changed_ratio }),
        ),
        MotionSignal::TamperDetected { variance } => (
            "tamper_detected",
            serde_json::json!({ "variance": variance }),
        ),
        MotionSignal::TamperCleared => ("tamper_cleared", serde_json::json!({})),
        MotionSignal::SignalLost => ("signal_lost", serde_json::json!({})),
        MotionSignal::SignalRestored => ("signal_restored", serde_json::json!({})),
    };
    Event::new(&TopicKey::Camera(camera_id), event_type, payload)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    /// Plays 3 s of live 25 fps H.264 into a tee with the motion branch
    /// attached and returns how many frames reached the appsink.
    async fn frames_reaching_appsink() -> usize {
        gstreamer::init().unwrap();
        let camera_id = Uuid::new_v4();
        let tee_name = format!("cam_{}_subtee", camera_id.as_simple());
        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc is-live=true pattern=ball \
             ! video/x-raw,width=320,height=240,framerate=25/1 \
             ! x264enc tune=zerolatency key-int-max=25 ! h264parse \
             ! tee name={tee_name} allow-not-linked=true"
        ))
        .unwrap()
        .downcast::<gstreamer::Pipeline>()
        .unwrap();

        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let handle = attach(&pipeline, &tee_name, camera_id, event_tx).unwrap();

        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        pipeline
            .by_name(&sink_name(camera_id))
            .unwrap()
            .static_pad("sink")
            .unwrap()
            .add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
                gstreamer::PadProbeReturn::Ok
            });

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let frames = count.load(Ordering::SeqCst);

        handle.stop().await;
        pipeline.set_state(gstreamer::State::Null).unwrap();
        frames
    }

    #[tokio::test]
    async fn only_the_sample_rate_reaches_the_analyzer() {
        let frames = frames_reaching_appsink().await;
        // 3 s at SAMPLE_RATE_FPS, plus slack for startup and the first frame.
        assert!(
            (3..=8).contains(&frames),
            "expected about {} frames at {SAMPLE_RATE_FPS} fps over 3 s, got {frames}",
            3 * SAMPLE_RATE_FPS
        );
    }

    #[test]
    fn every_relay_codec_has_a_software_decoder_candidate() {
        for caps in ["video/x-h264", "video/x-h265", "image/jpeg", "video/x-av1"] {
            assert!(!decoder_candidates(caps).is_empty(), "{caps}");
        }
    }
}
