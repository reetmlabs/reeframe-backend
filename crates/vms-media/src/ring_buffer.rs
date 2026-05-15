use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use uuid::Uuid;
use vms_core::{RingBufferMode, VmsError};

/// A single encoded video frame with its presentation timestamp.
#[derive(Clone)]
pub struct TimestampedFrame {
    /// Presentation timestamp from the GStreamer pipeline clock.
    pub pts:  Duration,
    /// Encoded frame bytes (H.264 / H.265 NAL units as delivered by the appsink).
    pub data: Arc<[u8]>,
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
    frames:       VecDeque<TimestampedFrame>,
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
            let front  = self.frames.front().unwrap().pts;
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
    pub fn extract(&self, pre_secs: u32, post_secs: u32, event_pts: Duration) -> Vec<TimestampedFrame> {
        let start = event_pts.saturating_sub(Duration::from_secs(u64::from(pre_secs)));
        let end   = event_pts + Duration::from_secs(u64::from(post_secs));

        self.frames
            .iter()
            .filter(|f| f.pts >= start && f.pts <= end)
            .cloned()
            .collect()
    }

    /// Number of frames currently in the buffer.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

// ── RingBufferManager ─────────────────────────────────────────────────────────

/// Manages one [`RingBuffer`] per camera, backed by GStreamer appsink branches.
///
/// Each `start` call creates a buffer and attaches an appsink branch to the
/// camera's live tee via [`MediaManager`]. Each `stop` call detaches the branch
/// and drops the buffer.
pub struct RingBufferManager {
    buffers: DashMap<Uuid, Arc<Mutex<RingBuffer>>>,
    media:   Arc<crate::MediaManager>,
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
    /// camera is already being buffered this is a no-op.
    ///
    /// `mode = Disk` is not yet implemented — it is accepted without error and
    /// silently falls back to in-memory storage.
    pub fn start(
        &self,
        camera_id:    Uuid,
        duration_secs: u32,
        mode:         RingBufferMode,
    ) -> Result<(), VmsError> {
        if self.buffers.contains_key(&camera_id) {
            return Ok(());
        }

        if mode == RingBufferMode::Disk {
            tracing::warn!(
                camera_id = %camera_id,
                "Disk ring buffer mode is not yet implemented — using memory"
            );
        }

        let ring_buffer = Arc::new(Mutex::new(RingBuffer::new(
            Duration::from_secs(u64::from(duration_secs)),
        )));

        self.media.attach_ring_buffer(camera_id, ring_buffer.clone())?;
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
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(pts_secs: u64) -> TimestampedFrame {
        TimestampedFrame {
            pts:  Duration::from_secs(pts_secs),
            data: Arc::from(vec![pts_secs as u8].as_slice()),
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
        // latest = 10s, max_duration = 5s → frames at 0..4 should be evicted
        // frame at T=5 should survive (10 - 5 == 5, not > 5)
        let pts_values: Vec<u64> = rb.frames.iter().map(|f| f.pts.as_secs()).collect();
        assert!(pts_values.first() == Some(&5), "oldest kept frame should be T=5, got {pts_values:?}");
        assert_eq!(*pts_values.last().unwrap(), 10);
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
        // event at T=2, pre=10 → start = saturating_sub → 0
        let clip = rb.extract(10, 0, Duration::from_secs(2));
        let pts: Vec<u64> = clip.iter().map(|f| f.pts.as_secs()).collect();
        assert_eq!(pts, vec![0, 1, 2]);
    }
}
