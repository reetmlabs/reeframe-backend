use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

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
