//! Dynamic motion/tamper analysis branch attached to a live camera tee.
//!
//! Mirrors [`crate::ring_buffer_branch`]'s attach/detach pattern exactly —
//! this is *not* a standalone connection to the camera. A camera typically
//! supports only two concurrent RTSP sessions (recording already uses one,
//! the relay uses the other when active), so a third independent connection
//! just for motion detection would exceed that budget on many real cameras.
//! Tapping the tee costs nothing extra connection-wise — it's exactly the
//! "analytics later" branch the tee's own doc comment in `camera_stream.rs`
//! already anticipated.
//!
//! Trade-off: since the tee only exists on the *main* recording pipeline,
//! this decodes off the main-resolution stream rather than a low-res
//! sub-stream, then downscales. More decode CPU than sourcing from an
//! already-small sub-stream, but still just decode+downscale — cheap
//! relative to real inference — and it costs zero additional camera-side
//! connections, which is the constraint that actually matters here.
//!
//! Branch: `tee -> queue -> decodebin -> videoconvert -> videoscale ->
//! capsfilter(GRAY8, MOTION_FRAME_WIDTH x MOTION_FRAME_HEIGHT) -> appsink`.
//! The tee's encoded elementary stream (already depayed+parsed by the main
//! pipeline) needs only `decodebin`'s own dynamic output pad handled — same
//! shape as `manager.rs`'s `capture_snapshot` branch, just persistent
//! instead of one-shot.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gstreamer::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::event::{Event, TopicKey};
use vms_core::VmsError;

use crate::motion::{MotionAnalyzer, MotionSignal, MOTION_FRAME_HEIGHT, MOTION_FRAME_WIDTH};

/// Minimum spacing between analyzed frames. Motion/tamper detection doesn't
/// need every decoded frame, and analyzing at a fixed low rate keeps CPU
/// cost flat regardless of the source stream's actual frame rate.
const MIN_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

// -- Element name helpers --

fn queue_name(id: Uuid) -> String {
    format!("cam_{}_motionqueue", id.as_simple())
}
fn decode_name(id: Uuid) -> String {
    format!("cam_{}_motiondecode", id.as_simple())
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

/// Handle to a running per-camera motion-analysis branch. Holds the
/// pipeline it's attached to and its camera ID so [`stop`](Self::stop) can
/// detach the GStreamer elements itself — the caller doesn't need to
/// remember which pipeline (main or sub) motion detection ended up on.
pub struct MotionHandle {
    pipeline: gstreamer::Pipeline,
    camera_id: Uuid,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl MotionHandle {
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
/// `tee_name` is the caller's choice of which tee to attach to — the
/// sub-stream's tee by default, or the main pipeline's tee when the camera
/// has no sub-stream configured (see `MediaManager::start_camera`). Every
/// analyzed frame produces zero or more [`MotionSignal`]s, converted to
/// [`Event`]s (`TopicKey::Camera`) and sent on `event_tx` — the same
/// bridge-to-`EventBus` channel used by `vms-sources` adapters, keeping this
/// crate free of a `vms-engine` dependency.
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

    let decodebin = gstreamer::ElementFactory::make("decodebin")
        .name(decode_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("motion decodebin: {e}")))?;

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
            &decodebin,
            &convert,
            &scale,
            &capsfilter,
            appsink.upcast_ref::<gstreamer::Element>(),
        ])
        .map_err(|e| VmsError::Media(format!("motion add_many: {e}")))?;

    gstreamer::Element::link_many([
        &convert,
        &scale,
        &capsfilter,
        appsink.upcast_ref::<gstreamer::Element>(),
    ])
    .map_err(|e| VmsError::Media(format!("motion link chain: {e}")))?;

    queue
        .link(&decodebin)
        .map_err(|e| VmsError::Media(format!("motion link queue->decodebin: {e}")))?;

    // decodebin autoplugs the parser+decoder off the tee's already-depayed
    // elementary stream and exposes decoded video on a dynamic src pad once
    // it knows the stream shape (same pattern as `capture_snapshot`'s branch).
    let convert_weak = convert.downgrade();
    decodebin.connect_pad_added(move |_, src_pad| {
        let Some(caps) = src_pad.current_caps() else {
            return;
        };
        let Some(structure) = caps.structure(0) else {
            return;
        };
        if !structure.name().starts_with("video/") {
            return;
        }
        let Some(convert) = convert_weak.upgrade() else {
            return;
        };
        let Some(sink_pad) = convert.static_pad("sink") else {
            return;
        };
        if !sink_pad.is_linked() {
            src_pad.link(&sink_pad).ok();
        }
    });

    // -- Link tee -> queue --
    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("motion queue has no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->motionqueue: {e}")))?;

    // -- Appsink callback: throttle to MIN_SAMPLE_INTERVAL, forward raw frames --
    // `drop = true` + a small `max_buffers` means GStreamer itself sheds
    // frames under load; `try_send` on a small bounded channel sheds them
    // again on the Rust side if the analyzer task is ever behind — either
    // way, a slow consumer never stalls the streaming thread.
    let (frame_tx, mut frame_rx) = mpsc::channel::<Vec<u8>>(2);
    let last_sample = Arc::new(Mutex::new(Instant::now() - MIN_SAMPLE_INTERVAL));
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink
                    .pull_sample()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;

                {
                    let mut last = last_sample.lock().unwrap();
                    if last.elapsed() < MIN_SAMPLE_INTERVAL {
                        return Ok(gstreamer::FlowSuccess::Ok);
                    }
                    *last = Instant::now();
                }

                let map = buffer
                    .map_readable()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let _ = frame_tx.try_send(map.as_slice().to_vec());
                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    // -- Bring new elements up to the pipeline's current state --
    for el in [
        &queue,
        &decodebin,
        &convert,
        &scale,
        &capsfilter,
        appsink.upcast_ref::<gstreamer::Element>(),
    ] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync motion element: {e}")))?;
    }

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
/// Same blocking-pad-probe pattern as [`crate::ring_buffer_branch::detach`],
/// extended to the larger element chain this branch has. The tee itself is
/// found via the queue's connected peer pad rather than by name, so this
/// works regardless of which tee (main or sub) `attach` used. Returns
/// immediately — cleanup is asynchronous. Safe to call while `Playing`. If
/// no branch is attached for this camera, this is a no-op.
fn detach(pipeline: &gstreamer::Pipeline, camera_id: Uuid) -> Result<(), VmsError> {
    let Some(queue) = pipeline.by_name(&queue_name(camera_id)) else {
        return Ok(());
    };

    let names = [
        decode_name(camera_id),
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

    tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();

        queue_clone.set_state(gstreamer::State::Null).ok();
        pipeline_clone.remove(&queue_clone).ok();
        for el in &rest {
            el.set_state(gstreamer::State::Null).ok();
            pipeline_clone.remove(el).ok();
        }

        let _ = tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    let tee_src_clone = tee_src.clone();
    std::thread::spawn(move || {
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => tracing::info!(camera_id = %camera_id, "Motion detection branch detached"),
            Err(_) => tracing::warn!(
                camera_id = %camera_id,
                "motion detach probe timed out — releasing tee pad anyway",
            ),
        }
        tee.release_request_pad(&tee_src_clone);
    });

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
