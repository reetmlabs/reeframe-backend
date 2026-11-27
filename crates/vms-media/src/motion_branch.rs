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
//! stream's profile errors out the whole camera pipeline.
//!
//! In [`MotionMode::Keyframes`] a probe on the decoder's sink pad drops every
//! delta frame, so only keyframes are decoded. `skip-frame` on the libav
//! decoders has no keyframe-only setting. While the stream's keyframes are more
//! than [`LONG_KEYFRAME_INTERVAL`] apart, the probe lets every frame through,
//! so motion is still sampled often enough. In [`MotionMode::Full`] every frame
//! is decoded (inter frames need their references), and `videorate` drops all
//! but [`SAMPLE_RATE_FPS`] per second before conversion and scaling.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gstreamer::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::event::{Event, TopicKey};
use vms_core::VmsError;

use crate::motion::{
    MotionAnalyzer, MotionMode, MotionSignal, MOTION_FRAME_HEIGHT, MOTION_FRAME_WIDTH,
};

/// Frames per second handed to the analyzer at most. `MotionAnalyzer` works
/// on frame timestamps, so a lower rate changes only how quickly it reacts.
const SAMPLE_RATE_FPS: i32 = 2;

/// Keyframe interval above which keyframes mode decodes every frame instead.
const LONG_KEYFRAME_INTERVAL: Duration = Duration::from_secs(4);

/// Decides which encoded buffers reach the decoder in keyframes mode.
///
/// It measures the interval between keyframes and switches to every frame
/// while that interval is too long. Switches happen at a keyframe, so the
/// decoder always starts from a reference.
#[derive(Default)]
struct KeyframeGate {
    last_keyframe: Option<Duration>,
    full: bool,
}

impl KeyframeGate {
    /// Whether a buffer goes to the decoder, and the new value of "decoding
    /// every frame" if this buffer changed it.
    fn admit(&mut self, keyframe: bool, timestamp: Option<Duration>) -> (bool, Option<bool>) {
        if !keyframe {
            return (self.full, None);
        }
        let mut switched = None;
        if let Some(now) = timestamp {
            if let Some(interval) = self.last_keyframe.and_then(|last| now.checked_sub(last)) {
                let full = interval > LONG_KEYFRAME_INTERVAL;
                if full != self.full {
                    self.full = full;
                    switched = Some(full);
                }
            }
            self.last_keyframe = Some(now);
        }
        (true, switched)
    }
}

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
    mode: MotionMode,
    /// Set while keyframes mode decodes every frame because keyframes are too
    /// far apart.
    long_keyframe_interval: Arc<AtomicBool>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl MotionHandle {
    /// The pipeline this branch is attached to.
    pub(crate) fn pipeline(&self) -> &gstreamer::Pipeline {
        &self.pipeline
    }

    /// The mode actually in use, which is `Full` while a keyframes-mode
    /// camera's keyframes are too far apart.
    pub fn effective_mode(&self) -> MotionMode {
        if self.long_keyframe_interval.load(Ordering::Relaxed) {
            MotionMode::Full
        } else {
            self.mode
        }
    }

    /// Detach the GStreamer elements, signal the analyzer task to stop, and
    /// wait for it to exit. The elements are removed from the pipeline shortly
    /// after this returns.
    pub async fn stop(self) {
        self.stop_inner().await;
    }

    /// Like [`stop`](Self::stop), but also waits until the elements are out of
    /// the pipeline, so a new branch for the camera can be attached right away.
    pub async fn stop_and_wait_for_removal(self) {
        if let Some(removal) = self.stop_inner().await {
            tokio::task::spawn_blocking(move || removal.join())
                .await
                .ok();
        }
    }

    async fn stop_inner(self) -> Option<std::thread::JoinHandle<()>> {
        let removal = match detach(&self.pipeline, self.camera_id) {
            Ok(removal) => removal,
            Err(e) => {
                tracing::warn!(camera_id = %self.camera_id, error = %e, "Failed to detach motion branch");
                None
            }
        };
        let _ = self.shutdown_tx.send(());
        self.task.await.ok();
        removal
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
/// `mode` picks which frames are decoded. Safe to call while the pipeline is
/// `Playing`.
pub fn attach(
    pipeline: &gstreamer::Pipeline,
    tee_name: &str,
    camera_id: Uuid,
    mode: MotionMode,
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
    crate::branch_drop::make_leaky(&queue, camera_id, "motion");

    let rate = gstreamer::ElementFactory::make("videorate")
        .name(rate_name(camera_id))
        .property("max-rate", SAMPLE_RATE_FPS)
        .property("drop-only", true)
        .build()
        .map_err(|e| VmsError::Media(format!("motion videorate: {e}")))?;
    // An H.264 stream without timing info has no frame rate, so its decoded
    // frames have no duration, and videorate aborts the process on those.
    if let Some(rate_sink) = rate.static_pad("sink") {
        let sample = gstreamer::ClockTime::SECOND / SAMPLE_RATE_FPS as u64;
        rate_sink.add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
            if let Some(buffer) = info.buffer_mut() {
                if buffer.duration().is_none() {
                    buffer.make_mut().set_duration(sample);
                }
            }
            gstreamer::PadProbeReturn::Ok
        });
    }

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
    let long_keyframe_interval = Arc::new(AtomicBool::new(false));
    let decoder_long_interval = long_keyframe_interval.clone();
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
        if let Err(e) = link_decoder(
            &pipeline,
            pad,
            &rate,
            caps_event.caps(),
            camera_id,
            mode,
            decoder_long_interval.clone(),
        ) {
            tracing::warn!(camera_id = %camera_id, error = %e, "Motion detection has no decoder for this stream");
        }
        gstreamer::PadProbeReturn::Remove
    });

    // -- Appsink callback: forward raw frames --
    // `drop = true` with a small `max_buffers` sheds frames in GStreamer
    // under load, and `try_send` on a small bounded channel sheds them again
    // if the analyzer task falls behind. A slow consumer never stalls the
    // streaming thread.
    // Frames without a timestamp use the time since attach instead.
    let started = std::time::Instant::now();
    let (frame_tx, mut frame_rx) = mpsc::channel::<(Vec<u8>, Duration)>(2);
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
                let timestamp = buffer
                    .pts()
                    .map_or_else(|| started.elapsed(), Duration::from);
                let _ = frame_tx.try_send((map.as_slice().to_vec(), timestamp));
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
        crate::camera_stream::sync_branch_state(el)
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
                    let Some((frame, timestamp)) = frame else { break };
                    for signal in analyzer.process_frame(&frame, timestamp) {
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
        mode,
        long_keyframe_interval,
        shutdown_tx,
        task,
    })
}

/// Detach the motion-analysis branch from camera `camera_id`'s tee.
///
/// Uses the same blocking pad probe as [`crate::ring_buffer_branch::detach`].
/// The tee is found through the queue's peer pad, so this works for whichever
/// tee (main or sub) `attach` used. Returns immediately with the thread that
/// finishes the removal. Safe to call while `Playing`, and a no-op if no branch
/// is attached.
fn detach(
    pipeline: &gstreamer::Pipeline,
    camera_id: Uuid,
) -> Result<Option<std::thread::JoinHandle<()>>, VmsError> {
    let Some(queue) = pipeline.by_name(&queue_name(camera_id)) else {
        return Ok(None);
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
    let removal = std::thread::spawn(move || {
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

    Ok(Some(removal))
}

/// Build the software decoder for `caps` and link it between the queue's
/// `queue_src` pad and `rate`. Runs on the streaming thread.
fn link_decoder(
    pipeline: &gstreamer::Pipeline,
    queue_src: &gstreamer::Pad,
    rate: &gstreamer::Element,
    caps: &gstreamer::CapsRef,
    camera_id: Uuid,
    mode: MotionMode,
    long_keyframe_interval: Arc<AtomicBool>,
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
    crate::camera_stream::sync_branch_state(&decoder)
        .map_err(|e| VmsError::Media(format!("sync motion {factory}: {e}")))?;
    let decoder_sink = decoder
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media(format!("motion {factory} has no sink pad")))?;
    if mode == MotionMode::Keyframes {
        let gate = Mutex::new(KeyframeGate::default());
        decoder_sink.add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
            let Some(buffer) = info.buffer() else {
                return gstreamer::PadProbeReturn::Ok;
            };
            let keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);
            let timestamp = buffer.pts().or(buffer.dts()).map(Duration::from);
            let (pass, switched) = gate.lock().unwrap().admit(keyframe, timestamp);
            match switched {
                Some(true) => tracing::warn!(
                    camera_id = %camera_id,
                    "Keyframes are more than {}s apart, motion detection decodes every frame",
                    LONG_KEYFRAME_INTERVAL.as_secs(),
                ),
                Some(false) => tracing::info!(
                    camera_id = %camera_id,
                    "Keyframe interval is short again, motion detection decodes keyframes only",
                ),
                None => {}
            }
            if let Some(full) = switched {
                long_keyframe_interval.store(full, Ordering::Relaxed);
            }
            // `Handled` lets the binding free the buffer. With `Drop`, GStreamer
            // also unrefs the pointer the binding already cleared and logs a critical.
            if pass {
                gstreamer::PadProbeReturn::Ok
            } else {
                gstreamer::PadProbeReturn::Handled
            }
        });
    }
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

    /// Buffer counts from 3 s of the motion branch on a live stream.
    struct BranchCounts {
        /// Keyframes that entered the branch.
        keyframes: usize,
        /// Frames the decoder produced.
        decoded: usize,
        /// Frames that reached the appsink.
        analyzed: usize,
    }

    /// Plays 3 s of live 25 fps H.264 with a keyframe every second into a tee
    /// with the motion branch attached in `mode`.
    async fn run_branch(mode: MotionMode) -> BranchCounts {
        gstreamer::init().unwrap();
        let camera_id = Uuid::new_v4();
        let tee_name = format!("cam_{}_subtee", camera_id.as_simple());
        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc is-live=true pattern=ball \
             ! video/x-raw,width=320,height=240,framerate=25/1 \
             ! openh264enc complexity=low gop-size=25 ! h264parse \
             ! tee name={tee_name} allow-not-linked=true"
        ))
        .unwrap()
        .downcast::<gstreamer::Pipeline>()
        .unwrap();

        // The decoder is created once caps flow, so count its output from
        // the moment it is added.
        let decoded = Arc::new(AtomicUsize::new(0));
        let decoded_counter = decoded.clone();
        let decoder = decode_name(camera_id);
        pipeline.connect_element_added(move |_, element| {
            if element.name() != decoder.as_str() {
                return;
            }
            let counter = decoded_counter.clone();
            element.static_pad("src").unwrap().add_probe(
                gstreamer::PadProbeType::BUFFER,
                move |_, _| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    gstreamer::PadProbeReturn::Ok
                },
            );
        });

        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let handle = attach(&pipeline, &tee_name, camera_id, mode, event_tx).unwrap();

        let keyframes = Arc::new(AtomicUsize::new(0));
        let keyframe_counter = keyframes.clone();
        pipeline
            .by_name(&queue_name(camera_id))
            .unwrap()
            .static_pad("src")
            .unwrap()
            .add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
                if info
                    .buffer()
                    .is_some_and(|b| !b.flags().contains(gstreamer::BufferFlags::DELTA_UNIT))
                {
                    keyframe_counter.fetch_add(1, Ordering::SeqCst);
                }
                gstreamer::PadProbeReturn::Ok
            });

        let analyzed = Arc::new(AtomicUsize::new(0));
        let analyzed_counter = analyzed.clone();
        pipeline
            .by_name(&sink_name(camera_id))
            .unwrap()
            .static_pad("sink")
            .unwrap()
            .add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
                analyzed_counter.fetch_add(1, Ordering::SeqCst);
                gstreamer::PadProbeReturn::Ok
            });

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        handle.stop().await;
        pipeline.set_state(gstreamer::State::Null).unwrap();
        BranchCounts {
            keyframes: keyframes.load(Ordering::SeqCst),
            decoded: decoded.load(Ordering::SeqCst),
            analyzed: analyzed.load(Ordering::SeqCst),
        }
    }

    #[tokio::test]
    async fn only_the_sample_rate_reaches_the_analyzer() {
        let frames = run_branch(MotionMode::Full).await.analyzed;
        // 3 s at SAMPLE_RATE_FPS, plus slack for startup and the first frame.
        assert!(
            (3..=8).contains(&frames),
            "expected about {} frames at {SAMPLE_RATE_FPS} fps over 3 s, got {frames}",
            3 * SAMPLE_RATE_FPS
        );
    }

    #[tokio::test]
    async fn keyframes_mode_decodes_only_keyframes() {
        let counts = run_branch(MotionMode::Keyframes).await;
        assert!(
            counts.keyframes >= 2,
            "expected a keyframe per second, got {}",
            counts.keyframes
        );
        assert!(
            counts.decoded <= counts.keyframes && counts.decoded + 1 >= counts.keyframes,
            "decoded {} frames from {} keyframes",
            counts.decoded,
            counts.keyframes
        );
    }

    #[tokio::test]
    async fn full_mode_decodes_every_frame() {
        let counts = run_branch(MotionMode::Full).await;
        assert!(
            counts.decoded > 10 * counts.keyframes,
            "decoded {} frames from {} keyframes",
            counts.decoded,
            counts.keyframes
        );
    }

    #[tokio::test]
    async fn a_branch_can_be_reattached_in_another_mode_while_playing() {
        gstreamer::init().unwrap();
        let camera_id = Uuid::new_v4();
        let tee_name = format!("cam_{}_subtee", camera_id.as_simple());
        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc is-live=true ! video/x-raw,width=320,height=240,framerate=25/1 \
             ! openh264enc complexity=low gop-size=25 ! h264parse \
             ! tee name={tee_name} allow-not-linked=true"
        ))
        .unwrap()
        .downcast::<gstreamer::Pipeline>()
        .unwrap();
        pipeline.set_state(gstreamer::State::Playing).unwrap();

        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let first = attach(
            &pipeline,
            &tee_name,
            camera_id,
            MotionMode::Keyframes,
            event_tx.clone(),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        first.stop_and_wait_for_removal().await;

        let second = attach(&pipeline, &tee_name, camera_id, MotionMode::Full, event_tx).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            pipeline.by_name(&decode_name(camera_id)).is_some(),
            "new branch is decoding"
        );
        second.stop().await;
        pipeline.set_state(gstreamer::State::Null).unwrap();
    }

    /// Feeds a GOP of `gop` frames at 25 fps starting at `start` through the
    /// gate and returns how many buffers passed and the last switch seen.
    fn feed_gop(gate: &mut KeyframeGate, start: Duration, gop: u32) -> (u32, Option<bool>) {
        let mut passed = 0;
        let mut switched = None;
        for i in 0..gop {
            let ts = start + Duration::from_millis(40) * i;
            let (pass, s) = gate.admit(i == 0, Some(ts));
            passed += u32::from(pass);
            switched = switched.or(s);
        }
        (passed, switched)
    }

    #[test]
    fn gate_passes_only_keyframes_at_a_short_interval() {
        let mut gate = KeyframeGate::default();
        for n in 0..5u32 {
            let (passed, switched) = feed_gop(&mut gate, Duration::from_secs(2 * u64::from(n)), 50);
            assert_eq!((passed, switched), (1, None), "GOP {n}");
        }
    }

    #[test]
    fn gate_decodes_every_frame_while_keyframes_are_far_apart() {
        let mut gate = KeyframeGate::default();
        // 10 s GOPs: the first interval is known at the second keyframe.
        assert_eq!(feed_gop(&mut gate, Duration::ZERO, 250), (1, None));
        assert_eq!(
            feed_gop(&mut gate, Duration::from_secs(10), 250),
            (250, Some(true))
        );
        assert_eq!(
            feed_gop(&mut gate, Duration::from_secs(20), 250),
            (250, None)
        );

        // Back to 2 s GOPs. The keyframe at 30 s still ends a 10 s interval, so
        // keyframes only resume with the next one.
        assert_eq!(feed_gop(&mut gate, Duration::from_secs(30), 50), (50, None));
        assert_eq!(
            feed_gop(&mut gate, Duration::from_secs(32), 50),
            (1, Some(false))
        );
        assert_eq!(feed_gop(&mut gate, Duration::from_secs(34), 50), (1, None));
    }

    #[test]
    fn gate_ignores_buffers_without_timestamps() {
        let mut gate = KeyframeGate::default();
        assert_eq!(gate.admit(true, None), (true, None));
        assert_eq!(gate.admit(false, None), (false, None));
        assert_eq!(gate.admit(true, None), (true, None));
    }

    #[test]
    fn every_restream_codec_has_a_software_decoder_candidate() {
        for caps in ["video/x-h264", "video/x-h265", "image/jpeg", "video/x-av1"] {
            assert!(!decoder_candidates(caps).is_empty(), "{caps}");
        }
    }
}
