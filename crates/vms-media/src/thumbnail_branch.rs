//! Periodic thumbnail capture branch attached to a live camera tee.
//!
//! Works like the one-shot `capture_snapshot` branch in `manager.rs` but stays
//! attached and captures on an interval. Each capture is one JPEG file named
//! by its timestamp.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gstreamer::prelude::*;
use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::VmsError;

/// JPEG encode quality (0-100). Not configurable; only the capture interval is.
const JPEG_QUALITY: i32 = 75;

/// How long the gate stays open waiting for the decoder to emit a frame
/// before giving up until the next interval, so a stuck decoder does not keep
/// every frame flowing into it.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(5);

/// A frame whose luma variance is below this is flat (levels²).
const BLANK_VARIANCE_THRESHOLD: f64 = 4.0;
/// Flat frames with mean luma at or below / at or above these are treated
/// as black / white. Covers both limited (16-235) and full range.
const BLANK_BLACK_MAX_MEAN: f64 = 32.0;
const BLANK_WHITE_MIN_MEAN: f64 = 224.0;
/// Sample every Nth pixel on every Nth row when checking for a blank frame.
const BLANK_SAMPLE_STEP: usize = 4;

fn queue_name(id: Uuid) -> String {
    format!("cam_{}_thumbqueue", id.as_simple())
}
fn decode_name(id: Uuid) -> String {
    format!("cam_{}_thumbdecode", id.as_simple())
}
fn convert_name(id: Uuid) -> String {
    format!("cam_{}_thumbconvert", id.as_simple())
}
fn capsfilter_name(id: Uuid) -> String {
    format!("cam_{}_thumbcaps", id.as_simple())
}
fn encoder_name(id: Uuid) -> String {
    format!("cam_{}_thumbenc", id.as_simple())
}
fn sink_name(id: Uuid) -> String {
    format!("cam_{}_thumbsink", id.as_simple())
}

/// Decides which encoded frames reach the decoder. Throttling in the appsink
/// is too late: by then every frame has been decoded and JPEG-encoded.
/// Instead one keyframe per interval is let in, and the gate stays open
/// until the decoder emits a frame (decoders may need a few more input
/// frames before producing output).
#[derive(Default)]
struct CaptureGate {
    last_capture: Option<Instant>,
    capturing_since: Option<Instant>,
}

impl CaptureGate {
    /// Whether an encoded buffer may pass into the decoder.
    fn admit(&mut self, now: Instant, is_keyframe: bool, interval: Duration) -> bool {
        match self.capturing_since {
            Some(since) if now.duration_since(since) > CAPTURE_TIMEOUT => {
                self.capturing_since = None;
                self.last_capture = Some(now);
                false
            }
            Some(_) => true,
            None => {
                let due = self
                    .last_capture
                    .is_none_or(|last| now.duration_since(last) >= interval);
                if due && is_keyframe {
                    self.capturing_since = Some(now);
                }
                due && is_keyframe
            }
        }
    }

    /// Whether a decoded frame is the one this capture was waiting for.
    /// Closes the gate either way; later leftover frames are rejected.
    fn take_decoded(&mut self, now: Instant) -> bool {
        if self.capturing_since.take().is_none() {
            return false;
        }
        self.last_capture = Some(now);
        true
    }
}

/// True for an all-black or all-white frame, which is what a camera with no
/// signal usually sends, so it does not replace a real thumbnail.
fn is_blank(luma: &[u8]) -> bool {
    let n = luma.len() as f64;
    if n == 0.0 {
        return true;
    }
    let mean = luma.iter().map(|&b| b as f64).sum::<f64>() / n;
    let variance = luma.iter().map(|&b| (b as f64 - mean).powi(2)).sum::<f64>() / n;
    variance < BLANK_VARIANCE_THRESHOLD
        && (mean <= BLANK_BLACK_MAX_MEAN || mean >= BLANK_WHITE_MIN_MEAN)
}

/// Subsampled luma of an I420 frame, or `None` if it can't be mapped.
fn sample_luma(buffer: &gstreamer::BufferRef, caps: &gstreamer::CapsRef) -> Option<Vec<u8>> {
    let info = gstreamer_video::VideoInfo::from_caps(caps).ok()?;
    let frame = gstreamer_video::VideoFrameRef::from_buffer_ref_readable(buffer, &info).ok()?;
    let plane = frame.plane_data(0).ok()?;
    let stride = info.stride()[0] as usize;
    let (width, height) = (info.width() as usize, info.height() as usize);
    let mut out =
        Vec::with_capacity((width / BLANK_SAMPLE_STEP + 1) * (height / BLANK_SAMPLE_STEP + 1));
    for row in (0..height).step_by(BLANK_SAMPLE_STEP) {
        let line = plane.get(row * stride..row * stride + width)?;
        out.extend(line.iter().step_by(BLANK_SAMPLE_STEP));
    }
    Some(out)
}

/// Handle to a running per-camera thumbnail-capture branch. Like
/// `motion_branch::MotionHandle`, it holds what [`stop`](Self::stop) needs to
/// detach the elements.
pub struct ThumbnailHandle {
    pipeline: gstreamer::Pipeline,
    camera_id: Uuid,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl ThumbnailHandle {
    /// The pipeline this branch is attached to.
    pub(crate) fn pipeline(&self) -> &gstreamer::Pipeline {
        &self.pipeline
    }

    pub async fn stop(self) {
        if let Err(e) = detach(&self.pipeline, self.camera_id) {
            tracing::warn!(camera_id = %self.camera_id, error = %e, "Failed to detach thumbnail branch");
        }
        let _ = self.shutdown_tx.send(());
        self.task.await.ok();
    }
}

/// Attach a thumbnail-capture branch to `tee_name`'s tee on `pipeline`.
/// Writes one `{unix_ms}.jpg` file into `thumbnails_dir/cam_{id}/` at most
/// once per `interval`, decoding only the frames needed for it. Black or
/// white frames are skipped, leaving the previous thumbnail current. Safe
/// to call while the pipeline is `Playing`.
pub fn attach(
    pipeline: &gstreamer::Pipeline,
    tee_name: &str,
    camera_id: Uuid,
    thumbnails_dir: PathBuf,
    interval: Duration,
) -> Result<ThumbnailHandle, VmsError> {
    let tee = pipeline.by_name(tee_name).ok_or_else(|| {
        VmsError::Media(format!("tee '{tee_name}' not found for camera {camera_id}"))
    })?;

    let queue = gstreamer::ElementFactory::make("queue")
        .name(queue_name(camera_id))
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail queue: {e}")))?;

    let decodebin = gstreamer::ElementFactory::make("decodebin")
        .name(decode_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail decodebin: {e}")))?;

    let convert = gstreamer::ElementFactory::make("videoconvert")
        .name(convert_name(camera_id))
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail videoconvert: {e}")))?;

    // Fixed I420 so the blank-frame check can read the luma plane directly.
    let capsfilter = gstreamer::ElementFactory::make("capsfilter")
        .name(capsfilter_name(camera_id))
        .property(
            "caps",
            gstreamer::Caps::builder("video/x-raw")
                .field("format", "I420")
                .build(),
        )
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail capsfilter: {e}")))?;

    let encoder = gstreamer::ElementFactory::make("jpegenc")
        .name(encoder_name(camera_id))
        .property("quality", JPEG_QUALITY)
        .build()
        .map_err(|e| VmsError::Media(format!("thumbnail jpegenc: {e}")))?;

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
            &capsfilter,
            &encoder,
            appsink.upcast_ref::<gstreamer::Element>(),
        ])
        .map_err(|e| VmsError::Media(format!("thumbnail add_many: {e}")))?;

    gstreamer::Element::link_many([
        &convert,
        &capsfilter,
        &encoder,
        appsink.upcast_ref::<gstreamer::Element>(),
    ])
    .map_err(|e| VmsError::Media(format!("thumbnail link chain: {e}")))?;

    queue
        .link(&decodebin)
        .map_err(|e| VmsError::Media(format!("thumbnail link queue->decodebin: {e}")))?;

    // As in `capture_snapshot`, decodebin autoplugs a decoder for the tee's
    // depayloaded stream and exposes its video pad here.
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

    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("thumbnail queue has no sink pad".into()))?;

    let gate = Arc::new(Mutex::new(CaptureGate::default()));

    let admit_gate = gate.clone();
    queue_sink.add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
        let Some(buffer) = info.buffer() else {
            return gstreamer::PadProbeReturn::Ok;
        };
        let is_keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);
        if admit_gate
            .lock()
            .unwrap()
            .admit(Instant::now(), is_keyframe, interval)
        {
            gstreamer::PadProbeReturn::Ok
        } else {
            gstreamer::PadProbeReturn::Drop
        }
    });

    let encoder_sink = encoder
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("thumbnail jpegenc has no sink pad".into()))?;
    encoder_sink.add_probe(gstreamer::PadProbeType::BUFFER, move |pad, info| {
        if !gate.lock().unwrap().take_decoded(Instant::now()) {
            return gstreamer::PadProbeReturn::Drop;
        }
        let (Some(buffer), Some(caps)) = (info.buffer(), pad.current_caps()) else {
            return gstreamer::PadProbeReturn::Ok;
        };
        if sample_luma(buffer, &caps).is_some_and(|luma| is_blank(&luma)) {
            tracing::debug!(camera_id = %camera_id, "Skipping blank thumbnail frame");
            return gstreamer::PadProbeReturn::Drop;
        }
        gstreamer::PadProbeReturn::Ok
    });

    // Every sample reaching the appsink is an accepted capture.
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

    // Sink first, tee last: data reaching an element still in `Null` gets
    // `FLUSHING`, which stops the queue's streaming task for good.
    for el in [
        appsink.upcast_ref::<gstreamer::Element>(),
        &encoder,
        &capsfilter,
        &convert,
        &decodebin,
        &queue,
    ] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync thumbnail element: {e}")))?;
    }

    let tee_src = tee.request_pad_simple("src_%u").ok_or_else(|| {
        VmsError::Media(format!("tee src pad request failed for camera {camera_id}"))
    })?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->thumbqueue: {e}")))?;

    let cam_dir = thumbnails_dir.join(format!("cam_{}", camera_id.as_simple()));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        if let Err(e) = tokio::fs::create_dir_all(&cam_dir).await {
            tracing::error!(camera_id = %camera_id, error = %e, "Failed to create thumbnail directory");
            return;
        }
        loop {
            tokio::select! {
                frame = frame_rx.recv() => {
                    let Some(frame) = frame else { break };
                    let path = cam_dir.join(format!("{}.jpg", chrono::Utc::now().timestamp_millis()));
                    if let Err(e) = tokio::fs::write(&path, &frame).await {
                        tracing::warn!(camera_id = %camera_id, path = %path.display(), error = %e, "Failed to write thumbnail");
                    }
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    tracing::info!(camera_id = %camera_id, tee_name, "Thumbnail capture branch attached");
    Ok(ThumbnailHandle {
        pipeline: pipeline.clone(),
        camera_id,
        shutdown_tx,
        task,
    })
}

/// Detach the thumbnail-capture branch from camera `camera_id`'s tee. Same
/// blocking-pad-probe pattern as `motion_branch::detach`. No-op if no
/// branch is attached.
fn detach(pipeline: &gstreamer::Pipeline, camera_id: Uuid) -> Result<(), VmsError> {
    let Some(queue) = pipeline.by_name(&queue_name(camera_id)) else {
        return Ok(());
    };

    let names = [
        decode_name(camera_id),
        convert_name(camera_id),
        capsfilter_name(camera_id),
        encoder_name(camera_id),
        sink_name(camera_id),
    ];
    let rest: Vec<gstreamer::Element> = names.iter().filter_map(|n| pipeline.by_name(n)).collect();

    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("thumbnail queue has no sink pad".into()))?;
    let tee_src = queue_sink
        .peer()
        .ok_or_else(|| VmsError::Media("thumbnail queue sink has no peer pad".into()))?;
    let tee = tee_src
        .parent_element()
        .ok_or_else(|| VmsError::Media("thumbnail tee src pad has no parent element".into()))?;

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
            tracing::info!(camera_id = %camera_id, "Thumbnail capture branch detached");
        } else {
            // A BLOCK_DOWNSTREAM probe only fires when a buffer or event
            // crosses the pad, so it never fires if upstream is dead. Without
            // forced removal, the elements would stay in the pipeline under
            // their fixed names and every later `attach()` for this camera
            // would fail at `add_many`. Five seconds with no frame means
            // nothing is flowing, so forcing the teardown is safe.
            tracing::warn!(
                camera_id = %camera_id,
                "thumbnail detach probe timed out, forcing removal directly",
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

#[cfg(test)]
mod capture_gate_tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_secs(30);

    #[test]
    fn first_keyframe_is_admitted_and_delta_frames_before_it_are_not() {
        let mut gate = CaptureGate::default();
        let t0 = Instant::now();
        assert!(!gate.admit(t0, false, INTERVAL));
        assert!(gate.admit(t0, true, INTERVAL));
    }

    #[test]
    fn gate_stays_open_until_a_frame_is_decoded_then_waits_an_interval() {
        let mut gate = CaptureGate::default();
        let t0 = Instant::now();
        assert!(gate.admit(t0, true, INTERVAL));
        assert!(gate.admit(t0 + Duration::from_millis(40), false, INTERVAL));
        assert!(gate.take_decoded(t0 + Duration::from_millis(50)));
        assert!(!gate.admit(t0 + Duration::from_secs(1), true, INTERVAL));
        assert!(gate.admit(t0 + Duration::from_secs(31), true, INTERVAL));
    }

    #[test]
    fn leftover_decoded_frames_after_the_capture_are_rejected() {
        let mut gate = CaptureGate::default();
        let t0 = Instant::now();
        gate.admit(t0, true, INTERVAL);
        assert!(gate.take_decoded(t0));
        assert!(!gate.take_decoded(t0));
    }

    #[test]
    fn a_decoder_that_never_emits_closes_the_gate_after_the_timeout() {
        let mut gate = CaptureGate::default();
        let t0 = Instant::now();
        gate.admit(t0, true, INTERVAL);
        let late = t0 + CAPTURE_TIMEOUT + Duration::from_millis(1);
        assert!(!gate.admit(late, false, INTERVAL));
        assert!(!gate.admit(late + Duration::from_secs(1), true, INTERVAL));
    }

    #[test]
    fn black_and_white_frames_are_blank() {
        assert!(is_blank(&[0; 64]));
        assert!(is_blank(&[16; 64]));
        assert!(is_blank(&[235; 64]));
        assert!(is_blank(&[255; 64]));
    }

    #[test]
    fn grey_or_textured_frames_are_not_blank() {
        assert!(!is_blank(&[128; 64]));
        let textured: Vec<u8> = (0..64).map(|i| if i % 2 == 0 { 0 } else { 255 }).collect();
        assert!(!is_blank(&textured));
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Plays a 25 fps H.264 test pattern for `run`, with a keyframe every 10
    /// frames and SPS/PPS on each keyframe like the live tees. Returns (frames
    /// that reached the decoder, JPEGs written).
    async fn run_branch(pattern: &str, interval: Duration, run: Duration) -> (usize, usize) {
        gstreamer::init().unwrap();
        let camera_id = Uuid::new_v4();
        let tee_name = format!("cam_{}_tee", camera_id.as_simple());
        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc is-live=true pattern={pattern} ! \
             video/x-raw,width=320,height=180,framerate=25/1 ! \
             x264enc tune=zerolatency key-int-max=10 ! h264parse config-interval=-1 ! \
             video/x-h264,stream-format=byte-stream,alignment=au ! tee name={tee_name}"
        ))
        .unwrap()
        .downcast::<gstreamer::Pipeline>()
        .unwrap();
        let dir = std::env::temp_dir().join(format!("thumb_test_{}", camera_id.as_simple()));

        // Attach before playing: a tee with no src pads fails the pipeline.
        let handle = attach(&pipeline, &tee_name, camera_id, dir.clone(), interval).unwrap();

        let decoded = Arc::new(AtomicUsize::new(0));
        let counter = decoded.clone();
        pipeline
            .by_name(&queue_name(camera_id))
            .unwrap()
            .static_pad("src")
            .unwrap()
            .add_probe(gstreamer::PadProbeType::BUFFER, move |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
                gstreamer::PadProbeReturn::Ok
            });

        pipeline.set_state(gstreamer::State::Playing).unwrap();
        tokio::time::sleep(run).await;
        handle.stop().await;
        pipeline.set_state(gstreamer::State::Null).unwrap();

        let written = std::fs::read_dir(dir.join(format!("cam_{}", camera_id.as_simple())))
            .map(|entries| entries.count())
            .unwrap_or(0);
        std::fs::remove_dir_all(&dir).ok();
        (decoded.load(Ordering::SeqCst), written)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn only_frames_needed_for_each_capture_are_decoded() {
        let (decoded, written) =
            run_branch("ball", Duration::from_secs(1), Duration::from_secs(3)).await;
        // ~75 frames flow through the tee; a handful per capture should reach the decoder.
        assert!(
            (2..=4).contains(&written),
            "written = {written}, decoded = {decoded}"
        );
        assert!(
            decoded <= written * 5,
            "decoded = {decoded}, written = {written}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn black_and_white_footage_writes_no_thumbnail() {
        for pattern in ["black", "white"] {
            let (_, written) =
                run_branch(pattern, Duration::from_secs(1), Duration::from_secs(2)).await;
            assert_eq!(written, 0, "pattern {pattern}");
        }
    }
}
