//! Frame-difference motion, scene-change, and tamper/signal-loss detection.
//!
//! Classical computer vision with no ML dependency, cheap enough to run on
//! every live camera that has it enabled. See [`crate::motion_branch`] for how
//! it is attached to a camera's stream.
//!
//! [`MotionAnalyzer`] is pure Rust with no GStreamer types, so it can be
//! tested directly against synthetic pixel buffers and timestamps.
//!
//! Every rule is measured in stream time, not frames, so the analyzer behaves
//! the same whether it sees keyframes only (about one frame per GOP) or a
//! steady 2 fps.

use std::time::Duration;

/// Width of the downscaled grayscale frames analyzed for motion.
///
/// Must be a multiple of 4. GStreamer aligns raw video rows to 4 bytes, so for
/// `GRAY8` (1 byte per pixel) the stride equals the width only then. Any other
/// width adds row padding this module does not handle.
pub const MOTION_FRAME_WIDTH: u32 = 320;
/// Height of the downscaled grayscale frames analyzed for motion.
pub const MOTION_FRAME_HEIGHT: u32 = 180;

/// Time constant of the background estimate's exponential moving average.
/// Shorter keeps up with lighting changes; longer keeps sustained motion from
/// turning into background too quickly. At 2 fps this is a weight of about
/// 0.05 per frame.
const BACKGROUND_MEMORY: Duration = Duration::from_secs(10);

/// A pixel is considered "changed" once it differs from the background
/// estimate by more than this many grayscale levels (0-255).
const PIXEL_DIFF_THRESHOLD: f32 = 25.0;

/// Fraction of changed pixels above which the frame counts as "motion".
const MOTION_RATIO_THRESHOLD: f64 = 0.02;

/// Fraction of changed pixels above which the frame counts as a "scene
/// change" (camera moved, view obstructed) instead of localized motion.
const SCENE_CHANGE_RATIO_THRESHOLD: f64 = 0.45;

/// Population variance (in grayscale levels²) below which a frame is
/// considered "covered": a near-uniform image, such as a hand or cloth over
/// the lens.
const TAMPER_VARIANCE_THRESHOLD: f64 = 4.0;

/// How long frames must stay byte-for-byte identical before the feed counts
/// as frozen. Sensor noise makes identical frames from a working camera very
/// unlikely.
const FROZEN_AFTER: Duration = Duration::from_secs(10);

/// Identical frames needed, at least, before the feed counts as frozen, so two
/// frames far apart are not enough.
const FROZEN_MIN_FRAMES: u32 = 3;

/// How long a condition must hold before its "started" event fires. A single
/// frame is enough when frames are further apart than this.
const START_AFTER: Duration = Duration::from_secs(1);

/// How long a condition must be absent before its "stopped" event fires.
const STOP_AFTER: Duration = Duration::from_secs(3);

/// One detection produced by [`MotionAnalyzer::process_frame`].
#[derive(Debug, Clone, PartialEq)]
pub enum MotionSignal {
    /// Motion began. `changed_ratio` is the fraction of pixels that differ
    /// from the background estimate (0.0-1.0).
    MotionStarted { changed_ratio: f64 },
    /// Motion that was previously reported has stopped.
    MotionStopped,
    /// A large, sudden, frame-wide change (camera moved, obstructed, or
    /// reframed). Reported instead of motion.
    SceneChange { changed_ratio: f64 },
    /// The feed appears covered or blinded (near-uniform frame with very low
    /// pixel variance).
    TamperDetected { variance: f64 },
    /// A previously reported tamper condition has cleared.
    TamperCleared,
    /// The feed has stopped updating: consecutive frames are identical.
    SignalLost,
    /// A previously reported frozen/stalled feed has resumed updating.
    SignalRestored,
}

/// Rolling per-camera frame-difference analyzer.
///
/// Feed it downscaled grayscale frames (`MOTION_FRAME_WIDTH` x
/// `MOTION_FRAME_HEIGHT`, exactly `width * height` bytes) in capture order,
/// each with its stream timestamp. Frames must all be the same size as
/// declared at construction.
pub struct MotionAnalyzer {
    width: u32,
    height: u32,
    background: Vec<f32>,
    background_initialized: bool,
    last_frame: Option<Vec<u8>>,
    last_timestamp: Option<Duration>,
    motion: Sustained,
    scene_change: Sustained,
    tamper: Sustained,
    /// Timestamp of the first frame of the current run of identical frames.
    identical_since: Duration,
    identical_frames: u32,
    signal_lost_active: bool,
}

impl MotionAnalyzer {
    pub fn new(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            background: vec![0.0; (width * height) as usize],
            background_initialized: false,
            last_frame: None,
            last_timestamp: None,
            motion: Sustained::default(),
            scene_change: Sustained::default(),
            tamper: Sustained::default(),
            identical_since: Duration::ZERO,
            identical_frames: 0,
            signal_lost_active: false,
        }
    }

    /// Process one grayscale frame taken at `timestamp` (any monotonic stream
    /// time), updating internal state and returning any signals that fired.
    ///
    /// `frame` must be exactly `width * height` bytes (as passed to
    /// [`new`](Self::new)). A frame of any other size is ignored and returns
    /// no signals, so one malformed frame cannot panic the analyzer.
    pub fn process_frame(&mut self, frame: &[u8], timestamp: Duration) -> Vec<MotionSignal> {
        let expected_len = (self.width * self.height) as usize;
        if frame.len() != expected_len {
            return Vec::new();
        }

        // A timestamp going backwards means the stream restarted, so the
        // timers restart from this frame.
        if self.last_timestamp.is_some_and(|last| timestamp < last) {
            self.motion.restart(timestamp);
            self.scene_change.restart(timestamp);
            self.tamper.restart(timestamp);
            self.identical_since = timestamp;
            self.identical_frames = 1;
            self.last_timestamp = None;
        }
        let interval = self
            .last_timestamp
            .map_or(Duration::ZERO, |last| timestamp - last);
        self.last_timestamp = Some(timestamp);

        let mut signals = Vec::new();

        // -- Frozen / signal-lost detection (independent of the background model) --
        if self.last_frame.as_deref() == Some(frame) {
            self.identical_frames += 1;
        } else {
            self.identical_since = timestamp;
            self.identical_frames = 1;
        }
        let frozen_now = self.identical_frames >= FROZEN_MIN_FRAMES
            && timestamp - self.identical_since >= FROZEN_AFTER;
        if frozen_now && !self.signal_lost_active {
            self.signal_lost_active = true;
            signals.push(MotionSignal::SignalLost);
        } else if !frozen_now && self.signal_lost_active {
            self.signal_lost_active = false;
            signals.push(MotionSignal::SignalRestored);
        }
        self.last_frame = Some(frame.to_vec());

        // -- Variance (tamper/covered) --
        let variance = population_variance(frame);
        let tampered_this_frame = variance < TAMPER_VARIANCE_THRESHOLD;
        if let Some(active) = self.tamper.update(tampered_this_frame, timestamp) {
            signals.push(if active {
                MotionSignal::TamperDetected { variance }
            } else {
                MotionSignal::TamperCleared
            });
        }

        // -- Background diff (motion / scene change) --
        //
        // Seed the background from the first frame instead of zero. Ramping
        // a zeroed background up through the EMA would look like a frame-wide
        // change for the first several frames of any static scene. Diffing
        // starts with the second frame.
        if !self.background_initialized {
            for (bg, &px) in self.background.iter_mut().zip(frame.iter()) {
                *bg = px as f32;
            }
            self.background_initialized = true;
            return signals;
        }

        let alpha = background_alpha(interval);
        let mut changed = 0usize;
        for (bg, &px) in self.background.iter_mut().zip(frame.iter()) {
            if (*bg - px as f32).abs() > PIXEL_DIFF_THRESHOLD {
                changed += 1;
            }
            *bg += (px as f32 - *bg) * alpha;
        }
        let changed_ratio = changed as f64 / expected_len as f64;

        let scene_change_this_frame = changed_ratio >= SCENE_CHANGE_RATIO_THRESHOLD;
        if self.scene_change.update(scene_change_this_frame, timestamp) == Some(true) {
            signals.push(MotionSignal::SceneChange { changed_ratio });
        }

        // A scene change (camera covered or moved) is not also reported as
        // motion.
        let motion_this_frame = !scene_change_this_frame && changed_ratio >= MOTION_RATIO_THRESHOLD;
        if let Some(active) = self.motion.update(motion_this_frame, timestamp) {
            signals.push(if active {
                MotionSignal::MotionStarted { changed_ratio }
            } else {
                MotionSignal::MotionStopped
            });
        }

        signals
    }
}

/// Hysteresis shared by all detectors, measured in stream time.
///
/// A frame stands for the time since the previous frame, so a condition has
/// held since the last frame that did not show it. That makes one frame enough
/// once frames are further apart than [`START_AFTER`].
#[derive(Default)]
struct Sustained {
    active: bool,
    /// Timestamp of the last frame whose condition matched `active`.
    settled_at: Option<Duration>,
}

impl Sustained {
    /// Feed one frame's condition and return the new state if it flipped.
    fn update(&mut self, condition: bool, timestamp: Duration) -> Option<bool> {
        let settled_at = *self.settled_at.get_or_insert(timestamp);
        if condition == self.active {
            self.settled_at = Some(timestamp);
            return None;
        }
        let needed = if self.active { STOP_AFTER } else { START_AFTER };
        if timestamp.saturating_sub(settled_at) < needed {
            return None;
        }
        self.active = condition;
        self.settled_at = Some(timestamp);
        Some(condition)
    }

    fn restart(&mut self, timestamp: Duration) {
        self.settled_at = Some(timestamp);
    }
}

/// Weight of a new frame in the background estimate, so the estimate's memory
/// is [`BACKGROUND_MEMORY`] whatever the frame interval.
fn background_alpha(interval: Duration) -> f32 {
    1.0 - (-interval.as_secs_f32() / BACKGROUND_MEMORY.as_secs_f32()).exp()
}

/// Population variance of a byte slice's values (as grayscale levels²).
fn population_variance(frame: &[u8]) -> f64 {
    let n = frame.len() as f64;
    if n == 0.0 {
        return 0.0;
    }
    let mean = frame.iter().map(|&b| b as f64).sum::<f64>() / n;
    frame
        .iter()
        .map(|&b| {
            let d = b as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / n
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: u32 = 4;
    const H: u32 = 4;

    /// Frame interval of the 2 fps sampling in full mode.
    const FULL_INTERVAL: Duration = Duration::from_millis(500);

    fn flat_frame(value: u8) -> Vec<u8> {
        vec![value; (W * H) as usize]
    }

    fn checkerboard(a: u8, b: u8) -> Vec<u8> {
        (0..W * H).map(|i| if i % 2 == 0 { a } else { b }).collect()
    }

    /// Feeds frames to an analyzer at a fixed interval and collects signals.
    struct Feed {
        analyzer: MotionAnalyzer,
        now: Duration,
        interval: Duration,
    }

    impl Feed {
        fn new(interval: Duration) -> Self {
            Self {
                analyzer: MotionAnalyzer::new(W, H),
                now: Duration::ZERO,
                interval,
            }
        }

        /// Feed `frame` `count` times and return every signal produced.
        fn feed(&mut self, frame: &[u8], count: u32) -> Vec<MotionSignal> {
            let mut signals = Vec::new();
            for _ in 0..count {
                signals.extend(self.analyzer.process_frame(frame, self.now));
                self.now += self.interval;
            }
            signals
        }
    }

    fn has_motion_started(signals: &[MotionSignal]) -> bool {
        signals
            .iter()
            .any(|s| matches!(s, MotionSignal::MotionStarted { .. }))
    }

    /// The two-pixel change used by the motion tests: motion range, below the
    /// scene-change ratio.
    fn moved(background: &[u8]) -> Vec<u8> {
        let mut moved = background.to_vec();
        moved[0] = 220;
        moved[1] = 220;
        moved
    }

    #[test]
    fn wrong_size_frame_is_a_noop() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let signals = analyzer.process_frame(&[0u8; 3], Duration::ZERO);
        assert!(signals.is_empty());
    }

    #[test]
    fn steady_scene_reports_no_motion() {
        let mut feed = Feed::new(FULL_INTERVAL);
        // Some texture, but unchanging, and fewer frames than the frozen window.
        let signals = feed.feed(&checkerboard(50, 60), 10);
        assert!(signals.iter().all(|s| !matches!(
            s,
            MotionSignal::MotionStarted { .. } | MotionSignal::SceneChange { .. }
        )));
    }

    #[test]
    fn sustained_local_change_reports_motion_started_then_stopped() {
        let mut feed = Feed::new(FULL_INTERVAL);
        let background = flat_frame(50);
        feed.feed(&background, 20);

        let signals = feed.feed(&moved(&background), 2);
        assert!(
            signals.contains(&MotionSignal::MotionStarted {
                changed_ratio: 2.0 / 16.0,
            }),
            "expected MotionStarted after sustained local change, got {signals:?}"
        );

        let signals = feed.feed(&background, 6);
        assert!(
            signals.contains(&MotionSignal::MotionStopped),
            "expected MotionStopped once the scene settles, got {signals:?}"
        );
    }

    #[test]
    fn frame_wide_change_reports_scene_change_not_motion() {
        let mut feed = Feed::new(FULL_INTERVAL);
        feed.feed(&flat_frame(50), 20);

        // Every pixel changes, well past the scene-change ratio.
        let signals = feed.feed(&flat_frame(220), 2);
        assert!(
            signals
                .iter()
                .any(|s| matches!(s, MotionSignal::SceneChange { .. })),
            "expected SceneChange for a frame-wide difference"
        );
        assert!(
            !has_motion_started(&signals),
            "a scene change should not also report as motion"
        );
    }

    #[test]
    fn near_uniform_frame_reports_tamper() {
        let mut feed = Feed::new(FULL_INTERVAL);
        // A perfectly flat frame has zero variance, so it counts as covered.
        let signals = feed.feed(&flat_frame(10), 3);
        assert!(signals
            .iter()
            .any(|s| matches!(s, MotionSignal::TamperDetected { .. })));
    }

    #[test]
    fn textured_frame_does_not_report_tamper() {
        let mut feed = Feed::new(FULL_INTERVAL);
        let signals = feed.feed(&checkerboard(10, 240), 3);
        assert!(!signals
            .iter()
            .any(|s| matches!(s, MotionSignal::TamperDetected { .. })));
    }

    #[test]
    fn identical_consecutive_frames_report_signal_lost_then_restored() {
        let mut feed = Feed::new(FULL_INTERVAL);
        let frame = checkerboard(30, 200);

        // 10 s of identical frames at 2 fps is 21 frames.
        let signals = feed.feed(&frame, 21);
        assert!(signals.contains(&MotionSignal::SignalLost));

        // A single differing byte breaks the identical-frame run.
        let mut changed = frame.clone();
        changed[0] = changed[0].wrapping_add(1);
        let signals = feed.feed(&changed, 1);
        assert!(signals.contains(&MotionSignal::SignalRestored));
    }
}
