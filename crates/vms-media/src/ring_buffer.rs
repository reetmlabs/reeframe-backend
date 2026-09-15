use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use gstreamer::prelude::*;
use uuid::Uuid;
use vms_core::{RingBufferMode, VmsError};

/// A single encoded video frame with its presentation timestamp.
#[derive(Clone)]
pub struct TimestampedFrame {
    /// Presentation timestamp from the GStreamer pipeline clock.
    pub pts: Duration,
    /// Encoded frame bytes (H.264 / H.265 NAL units as delivered by the appsink).
    pub data: Arc<[u8]>,
    /// True when this frame carries no delta dependency (IDR / key frame).
    pub is_keyframe: bool,
}

/// Sliding-window buffer of `TimestampedFrame`s bounded by wall-clock duration.
///
/// Frames are appended in PTS order. On each `push` the oldest frames are
/// evicted so the total span of buffered PTS values never exceeds
/// `max_duration`. At least one frame is always retained regardless of the
/// duration gap.
///
/// Always accessed behind an `Arc<Mutex<RingBuffer>>` — no internal locking.
pub struct RingBuffer {
    frames: VecDeque<TimestampedFrame>,
    max_duration: Duration,
}

impl RingBuffer {
    pub fn new(max_duration: Duration) -> Self {
        Self {
            frames: VecDeque::new(),
            max_duration,
        }
    }

    /// Append `frame` and evict frames that fall outside the rolling window.
    pub fn push(&mut self, frame: TimestampedFrame) {
        self.frames.push_back(frame);

        // Evict from the front while the span exceeds max_duration.
        // The `> 1` guard ensures we never drop the frame we just pushed.
        while self.frames.len() > 1 {
            let latest = self.frames.back().unwrap().pts;
            let front = self.frames.front().unwrap().pts;
            if latest.checked_sub(front).unwrap_or(Duration::ZERO) > self.max_duration {
                self.frames.pop_front();
            } else {
                break;
            }
        }
    }

    /// Return all frames whose PTS falls in `[event_pts − pre_secs, event_pts + post_secs]`.
    ///
    /// Returns an empty `Vec` if the buffer is empty or no frames fall in the window.
    pub fn extract(
        &self,
        pre_secs: u32,
        post_secs: u32,
        event_pts: Duration,
    ) -> Vec<TimestampedFrame> {
        let start = event_pts.saturating_sub(Duration::from_secs(u64::from(pre_secs)));
        let end = event_pts + Duration::from_secs(u64::from(post_secs));

        self.frames
            .iter()
            .filter(|f| f.pts >= start && f.pts <= end)
            .cloned()
            .collect()
    }

    /// PTS of the most recently buffered frame, or `None` if the buffer is empty.
    pub fn latest_pts(&self) -> Option<Duration> {
        self.frames.back().map(|f| f.pts)
    }

    /// Grow `max_duration` to `min_duration` if it's larger than the current
    /// value. Never shrinks — a smaller request keeps whatever history is
    /// already buffered rather than discarding it.
    pub fn grow_to(&mut self, min_duration: Duration) {
        if min_duration > self.max_duration {
            self.max_duration = min_duration;
        }
    }

    /// Number of frames currently in the buffer.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

// -- RingBufferManager --

/// Manages one [`RingBuffer`] per camera, backed by GStreamer appsink branches.
///
/// Each `start` call creates a buffer and attaches an appsink branch to the
/// camera's live tee via [`MediaManager`]. Each `stop` call detaches the branch
/// and drops the buffer.
pub struct RingBufferManager {
    buffers: DashMap<Uuid, Arc<Mutex<RingBuffer>>>,
    media: Arc<crate::MediaManager>,
}

impl RingBufferManager {
    pub fn new(media: Arc<crate::MediaManager>) -> Arc<Self> {
        Arc::new(Self {
            buffers: DashMap::new(),
            media,
        })
    }

    /// Start buffering frames for `camera_id`.
    ///
    /// Creates a [`RingBuffer`] sized to hold `duration_secs` of footage and
    /// attaches an appsink branch to the camera's live GStreamer tee. If the
    /// camera is already being buffered, its capacity is grown to
    /// `duration_secs` instead — never shrunk, so a smaller `duration_secs`
    /// from some other caller can't discard another caller's history.
    pub fn start(
        &self,
        camera_id: Uuid,
        duration_secs: u32,
        mode: RingBufferMode,
    ) -> Result<(), VmsError> {
        if mode == RingBufferMode::Disk {
            return Err(VmsError::Config(
                "RingBufferMode::Disk is not yet implemented".into(),
            ));
        }

        if let Some(existing) = self.buffers.get(&camera_id) {
            existing
                .lock()
                .expect("ring buffer mutex poisoned")
                .grow_to(Duration::from_secs(u64::from(duration_secs)));
            return Ok(());
        }

        let ring_buffer = Arc::new(Mutex::new(RingBuffer::new(Duration::from_secs(u64::from(
            duration_secs,
        )))));

        self.media
            .attach_ring_buffer(camera_id, ring_buffer.clone())?;
        self.buffers.insert(camera_id, ring_buffer);

        tracing::info!(camera_id = %camera_id, duration_secs, "Ring buffer started");
        Ok(())
    }

    /// Stop buffering frames for `camera_id` and drop the stored frames.
    ///
    /// Detaches the appsink branch from the GStreamer tee. No-op if no buffer
    /// exists for this camera.
    pub fn stop(&self, camera_id: Uuid) -> Result<(), VmsError> {
        if self.buffers.remove(&camera_id).is_none() {
            return Ok(());
        }
        self.media.detach_ring_buffer(camera_id)?;
        tracing::info!(camera_id = %camera_id, "Ring buffer stopped");
        Ok(())
    }

    /// Return a handle to the ring buffer for `camera_id`, or `None` if not running.
    ///
    /// The caller can lock the buffer to call [`RingBuffer::extract`] for clip
    /// extraction without going through the manager.
    pub fn get(&self, camera_id: Uuid) -> Option<Arc<Mutex<RingBuffer>>> {
        self.buffers.get(&camera_id).map(|e| e.clone())
    }

    /// PTS of the most recently buffered frame for `camera_id`.
    ///
    /// Returns `None` if no buffer is running for this camera or the buffer is empty.
    /// Used by the `extract_clip` action handler to anchor the extraction window
    /// to the moment the pipeline fired.
    pub fn latest_pts(&self, camera_id: Uuid) -> Option<Duration> {
        self.buffers
            .get(&camera_id)
            .and_then(|rb| rb.lock().ok().and_then(|rb| rb.latest_pts()))
    }

    /// Extract frames around `event_pts` from the ring buffer and mux them into
    /// an MP4 file in `output_dir`.
    ///
    /// Extends the pre-event window by 5 seconds to ensure at least one IDR
    /// frame is captured, then trims the result to start on the first keyframe
    /// at or before `event_pts - pre_secs`. The output clip therefore spans
    /// `[first_keyframe_before_start, event_pts + post_secs]`.
    ///
    /// The GStreamer muxer runs in `spawn_blocking` — safe to `.await` from
    /// async code.
    pub async fn extract_clip(
        &self,
        camera_id: Uuid,
        pre_secs: u32,
        post_secs: u32,
        event_pts: Duration,
        output_dir: &Path,
    ) -> Result<PathBuf, VmsError> {
        let ring = self
            .get(camera_id)
            .ok_or_else(|| VmsError::Media(format!("no ring buffer for camera {camera_id}")))?;

        // Grab a wider window so we capture an IDR frame before the clip start.
        let frames = {
            let rb = ring.lock().expect("ring buffer mutex poisoned");
            rb.extract(
                pre_secs + vms_core::action::EXTRACT_CLIP_KEYFRAME_SEARCH_SECS,
                post_secs,
                event_pts,
            )
        };

        if frames.is_empty() {
            return Err(VmsError::Media(format!(
                "ring buffer empty for camera {camera_id} at event PTS {event_pts:?}"
            )));
        }

        let frames = align_to_keyframe(frames);

        if frames.is_empty() {
            return Err(VmsError::Media(format!(
                "no keyframe found in ring buffer for camera {camera_id}"
            )));
        }

        std::fs::create_dir_all(output_dir)
            .map_err(|e| VmsError::Media(format!("create clip dir: {e}")))?;

        let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("clip_{}_{}.mp4", camera_id.as_simple(), ts);
        let output_path = output_dir.join(&filename);

        // Ring buffer always taps the main pipeline's tee (see
        // `MediaManager::attach_ring_buffer`), so the main-quality relay's
        // cached codec (if a main relay has ever been started) is the right
        // one to reuse here — falls back to H264 if it hasn't.
        let codec = self
            .media
            .relay_codec(camera_id, crate::relay::RelayQuality::Main)
            .unwrap_or_else(|| "H264".to_owned());

        let out = output_path.clone();
        tokio::task::spawn_blocking(move || mux_to_mp4(frames, &out, &codec))
            .await
            .map_err(|e| VmsError::Media(format!("spawn_blocking clip mux: {e}")))??;

        tracing::info!(
            camera_id = %camera_id,
            path = %output_path.display(),
            "Clip extracted"
        );
        Ok(output_path)
    }
}

// -- Clip extraction helpers --

/// Trim `frames` to start at the first IDR/keyframe.
fn align_to_keyframe(mut frames: Vec<TimestampedFrame>) -> Vec<TimestampedFrame> {
    match frames.iter().position(|f| f.is_keyframe) {
        Some(idx) => {
            frames.drain(..idx);
            frames
        }
        // No keyframe anywhere in the window — nothing here can be muxed
        // into a valid clip, so the caller's empty check must catch this,
        // not the muxer downstream.
        None => Vec::new(),
    }
}

/// Mux raw encoded frames into an MP4 file at `output`.
///
/// Pipeline: `appsrc -> <parser> -> mp4mux -> filesink`.
/// PTS values are normalised to start from zero. Blocks until EOS or error.
///
/// Supported `codec` values (case-insensitive): `"H264"`, `"H265"`, `"HEVC"`, `"JPEG"`.
fn mux_to_mp4(frames: Vec<TimestampedFrame>, output: &Path, codec: &str) -> Result<(), VmsError> {
    let (caps_mime, parser_name) = match codec.to_uppercase().as_str() {
        "H264" => ("video/x-h264", "h264parse"),
        "H265" | "HEVC" => ("video/x-h265", "h265parse"),
        "JPEG" => ("image/jpeg", "jpegparse"),
        other => {
            return Err(VmsError::Media(format!(
                "mux_to_mp4: unsupported codec '{other}'"
            )))
        }
    };

    gstreamer::init().ok();

    let location = output
        .to_str()
        .ok_or_else(|| VmsError::Media("clip output path is not valid UTF-8".into()))?;

    let pipeline = gstreamer::Pipeline::new();

    let appsrc = gstreamer_app::AppSrc::builder()
        .name("clip_src")
        .format(gstreamer::Format::Time)
        .build();

    let caps = if caps_mime == "image/jpeg" {
        gstreamer::Caps::builder(caps_mime).build()
    } else {
        gstreamer::Caps::builder(caps_mime)
            .field("stream-format", "byte-stream")
            .field("alignment", "au")
            .build()
    };
    appsrc.set_caps(Some(&caps));

    let parse = gstreamer::ElementFactory::make(parser_name)
        .build()
        .map_err(|e| VmsError::Media(format!("{parser_name}: {e}")))?;

    let mux = gstreamer::ElementFactory::make("mp4mux")
        .build()
        .map_err(|e| VmsError::Media(format!("mp4mux: {e}")))?;

    let sink = gstreamer::ElementFactory::make("filesink")
        .property("location", location)
        .build()
        .map_err(|e| VmsError::Media(format!("filesink: {e}")))?;

    pipeline
        .add(&appsrc)
        .map_err(|e| VmsError::Media(format!("add appsrc: {e}")))?;
    pipeline
        .add(&parse)
        .map_err(|e| VmsError::Media(format!("add {parser_name}: {e}")))?;
    pipeline
        .add(&mux)
        .map_err(|e| VmsError::Media(format!("add mp4mux: {e}")))?;
    pipeline
        .add(&sink)
        .map_err(|e| VmsError::Media(format!("add filesink: {e}")))?;

    appsrc
        .link(&parse)
        .map_err(|e| VmsError::Media(format!("link appsrc->{parser_name}: {e}")))?;
    parse
        .link(&mux)
        .map_err(|e| VmsError::Media(format!("link {parser_name}->mp4mux: {e}")))?;
    mux.link(&sink)
        .map_err(|e| VmsError::Media(format!("link mp4mux->filesink: {e}")))?;

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("play clip pipeline: {e}")))?;

    let base_pts = frames.first().map(|f| f.pts).unwrap_or(Duration::ZERO);

    for frame in &frames {
        let normalized = frame.pts.saturating_sub(base_pts);
        let mut buf = gstreamer::Buffer::from_slice(Arc::clone(&frame.data));
        {
            let b = buf.get_mut().expect("unique buffer ownership");
            b.set_pts(gstreamer::ClockTime::from_nseconds(
                normalized.as_nanos() as u64
            ));
            if !frame.is_keyframe {
                b.set_flags(gstreamer::BufferFlags::DELTA_UNIT);
            }
        }
        appsrc
            .push_buffer(buf)
            .map_err(|e| VmsError::Media(format!("push frame: {e}")))?;
    }

    appsrc
        .end_of_stream()
        .map_err(|e| VmsError::Media(format!("clip EOS: {e}")))?;

    let bus = pipeline.bus().expect("pipeline has a bus");
    let per_msg = gstreamer::ClockTime::from_seconds(10);
    loop {
        match bus.timed_pop(per_msg) {
            Some(msg) => match msg.view() {
                gstreamer::MessageView::Eos(_) => break,
                gstreamer::MessageView::Error(err) => {
                    pipeline.set_state(gstreamer::State::Null).ok();
                    return Err(VmsError::Media(format!("clip mux error: {}", err.error())));
                }
                _ => {}
            },
            None => {
                pipeline.set_state(gstreamer::State::Null).ok();
                return Err(VmsError::Media(
                    "clip mux pipeline produced no EOS — aborting".into(),
                ));
            }
        }
    }

    pipeline.set_state(gstreamer::State::Null).ok();
    Ok(())
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(pts_secs: u64) -> TimestampedFrame {
        TimestampedFrame {
            pts: Duration::from_secs(pts_secs),
            data: Arc::from(vec![pts_secs as u8].as_slice()),
            is_keyframe: false,
        }
    }

    fn keyframe(pts_secs: u64) -> TimestampedFrame {
        TimestampedFrame {
            is_keyframe: true,
            ..frame(pts_secs)
        }
    }

    // Pushing frames that span less than max_duration keeps all of them.
    #[test]
    fn no_eviction_within_window() {
        let mut rb = RingBuffer::new(Duration::from_secs(10));
        for s in 0..=5 {
            rb.push(frame(s));
        }
        assert_eq!(rb.len(), 6);
    }

    // Once the span exceeds max_duration, old frames are evicted from the front.
    #[test]
    fn evicts_old_frames_beyond_max_duration() {
        let mut rb = RingBuffer::new(Duration::from_secs(5));
        for s in 0..=10 {
            rb.push(frame(s));
        }
        // latest = 10s, max_duration = 5s -> frames at 0..4 should be evicted
        // frame at T=5 should survive (10 - 5 == 5, not > 5)
        let pts_values: Vec<u64> = rb.frames.iter().map(|f| f.pts.as_secs()).collect();
        assert!(
            pts_values.first() == Some(&5),
            "oldest kept frame should be T=5, got {pts_values:?}"
        );
        assert_eq!(*pts_values.last().unwrap(), 10);
    }

    // Growing the capacity lets subsequently pushed frames extend further
    // back before eviction kicks in.
    #[test]
    fn grow_to_extends_eviction_window() {
        let mut rb = RingBuffer::new(Duration::from_secs(5));
        for s in 0..=10 {
            rb.push(frame(s));
        }
        rb.grow_to(Duration::from_secs(20));
        for s in 11..=15 {
            rb.push(frame(s));
        }
        // Nothing older than T=15-20=-5 should have been evicted since the
        // grow, so everything from the first loop that survived it (T=5..)
        // is still present alongside the new frames.
        let pts_values: Vec<u64> = rb.frames.iter().map(|f| f.pts.as_secs()).collect();
        assert_eq!(pts_values.first(), Some(&5));
        assert_eq!(pts_values.last(), Some(&15));
    }

    // A smaller request than the current capacity is ignored, not shrunk.
    #[test]
    fn grow_to_never_shrinks() {
        let mut rb = RingBuffer::new(Duration::from_secs(20));
        for s in 0..=10 {
            rb.push(frame(s));
        }
        rb.grow_to(Duration::from_secs(5));
        // Still governed by the original 20s capacity, so nothing evicted.
        assert_eq!(rb.len(), 11);
    }

    // A single frame is never evicted even if the gap would exceed max_duration.
    #[test]
    fn single_frame_never_evicted() {
        let mut rb = RingBuffer::new(Duration::from_secs(0));
        rb.push(frame(100));
        assert_eq!(rb.len(), 1);
    }

    // extract returns only frames inside [event_pts - pre, event_pts + post].
    #[test]
    fn extract_returns_correct_window() {
        let mut rb = RingBuffer::new(Duration::from_secs(30));
        for s in 0..=20 {
            rb.push(frame(s));
        }
        // event at T=10, window [8, 12]
        let clip = rb.extract(2, 2, Duration::from_secs(10));
        let pts: Vec<u64> = clip.iter().map(|f| f.pts.as_secs()).collect();
        assert_eq!(pts, vec![8, 9, 10, 11, 12]);
    }

    // extract on an empty buffer returns an empty Vec without panicking.
    #[test]
    fn extract_empty_buffer_returns_empty() {
        let rb = RingBuffer::new(Duration::from_secs(10));
        let clip = rb.extract(5, 5, Duration::from_secs(10));
        assert!(clip.is_empty());
    }

    // extract window that starts before T=0 is clamped correctly by saturating_sub.
    #[test]
    fn extract_window_clamped_at_zero() {
        let mut rb = RingBuffer::new(Duration::from_secs(30));
        for s in 0..=5 {
            rb.push(frame(s));
        }
        // event at T=2, pre=10 -> start = saturating_sub -> 0
        let clip = rb.extract(10, 0, Duration::from_secs(2));
        let pts: Vec<u64> = clip.iter().map(|f| f.pts.as_secs()).collect();
        assert_eq!(pts, vec![0, 1, 2]);
    }

    // align_to_keyframe trims everything before the first IDR frame.
    #[test]
    fn align_to_keyframe_trims_leading_delta_frames() {
        let frames = vec![frame(0), frame(1), keyframe(2), frame(3), frame(4)];
        let aligned = align_to_keyframe(frames);
        let pts: Vec<u64> = aligned.iter().map(|f| f.pts.as_secs()).collect();
        assert_eq!(pts, vec![2, 3, 4]);
    }

    // align_to_keyframe returns all frames unchanged when the first is already a keyframe.
    #[test]
    fn align_to_keyframe_noop_when_already_aligned() {
        let frames = vec![keyframe(0), frame(1), frame(2)];
        let aligned = align_to_keyframe(frames);
        assert_eq!(aligned.len(), 3);
        assert!(aligned[0].is_keyframe);
    }

    // align_to_keyframe on a stream with no keyframes returns empty — those
    // frames can never be muxed into a valid clip, so the caller's
    // is_empty() check must catch this rather than the muxer downstream.
    #[test]
    fn align_to_keyframe_no_keyframe_returns_empty() {
        let frames = vec![frame(0), frame(1), frame(2)];
        let aligned = align_to_keyframe(frames);
        assert!(aligned.is_empty());
    }
}
