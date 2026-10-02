//! Frame-difference motion, scene-change, and tamper/signal-loss detection.
//!
//! Classical computer vision with no ML dependency, cheap enough to run on
//! every live camera that has it enabled. See [`crate::motion_branch`] for how
//! it is attached to a camera's stream.
//!
//! [`MotionAnalyzer`] is pure Rust with no GStreamer types, so it can be
//! tested directly against synthetic pixel buffers.

/// Width of the downscaled grayscale frames analyzed for motion.
///
/// Must be a multiple of 4. GStreamer aligns raw video rows to 4 bytes, so for
/// `GRAY8` (1 byte per pixel) the stride equals the width only then. Any other
/// width adds row padding this module does not handle.
pub const MOTION_FRAME_WIDTH: u32 = 320;
/// Height of the downscaled grayscale frames analyzed for motion.
pub const MOTION_FRAME_HEIGHT: u32 = 180;

/// Exponential-moving-average weight of each new frame in the background
/// estimate. Lower values keep sustained motion from turning into background
/// too quickly; higher values follow gradual lighting changes better.
const BACKGROUND_ALPHA: f32 = 0.05;

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

/// Consecutive processed frames that must be byte-for-byte identical before
/// the feed is considered frozen. Sensor noise makes identical frames from a
/// working camera very unlikely.
const FROZEN_FRAME_COUNT: u32 = 10;

/// Consecutive frames a condition must hold before its "started" event fires,
/// and must not hold before its "stopped" event fires. Debounces flicker
/// around a threshold.
const SUSTAIN_FRAMES: u32 = 2;

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
/// `MOTION_FRAME_HEIGHT`, exactly `width * height` bytes) in capture order.
/// Frames must all be the same size as declared at construction.
pub struct MotionAnalyzer {
    width: u32,
    height: u32,
    background: Vec<f32>,
    background_initialized: bool,
    last_frame: Option<Vec<u8>>,
    motion_over_count: u32,
    motion_under_count: u32,
    motion_active: bool,
    scene_change_over_count: u32,
    scene_change_under_count: u32,
    scene_change_active: bool,
    tamper_over_count: u32,
    tamper_under_count: u32,
    tamper_active: bool,
    identical_frame_count: u32,
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
            motion_over_count: 0,
            motion_under_count: 0,
            motion_active: false,
            scene_change_over_count: 0,
            scene_change_under_count: 0,
            scene_change_active: false,
            tamper_over_count: 0,
            tamper_under_count: 0,
            tamper_active: false,
            identical_frame_count: 0,
            signal_lost_active: false,
        }
    }

    /// Process one grayscale frame, updating internal state and returning
    /// any signals that fired as a result.
    ///
    /// `frame` must be exactly `width * height` bytes (as passed to
    /// [`new`](Self::new)). A frame of any other size is ignored and returns
    /// no signals, so one malformed frame cannot panic the analyzer.
    pub fn process_frame(&mut self, frame: &[u8]) -> Vec<MotionSignal> {
        let expected_len = (self.width * self.height) as usize;
        if frame.len() != expected_len {
            return Vec::new();
        }

        let mut signals = Vec::new();

        // -- Frozen / signal-lost detection (independent of the background model) --
        let is_identical_to_last = self.last_frame.as_deref() == Some(frame);
        if is_identical_to_last {
            self.identical_frame_count += 1;
        } else {
            self.identical_frame_count = 0;
        }
        let frozen_now = self.identical_frame_count >= FROZEN_FRAME_COUNT;
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
        update_hysteresis(
            tampered_this_frame,
            &mut self.tamper_over_count,
            &mut self.tamper_under_count,
            &mut self.tamper_active,
            |a| {
                signals.push(if a {
                    MotionSignal::TamperDetected { variance }
                } else {
                    MotionSignal::TamperCleared
                })
            },
        );

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

        let mut changed = 0usize;
        for (bg, &px) in self.background.iter_mut().zip(frame.iter()) {
            if (*bg - px as f32).abs() > PIXEL_DIFF_THRESHOLD {
                changed += 1;
            }
            *bg += (px as f32 - *bg) * BACKGROUND_ALPHA;
        }
        let changed_ratio = changed as f64 / expected_len as f64;

        let scene_change_this_frame = changed_ratio >= SCENE_CHANGE_RATIO_THRESHOLD;
        update_hysteresis(
            scene_change_this_frame,
            &mut self.scene_change_over_count,
            &mut self.scene_change_under_count,
            &mut self.scene_change_active,
            |a| {
                if a {
                    signals.push(MotionSignal::SceneChange { changed_ratio });
                }
            },
        );

        // A scene change (camera covered or moved) is not also reported as
        // motion.
        let motion_this_frame = !scene_change_this_frame && changed_ratio >= MOTION_RATIO_THRESHOLD;
        update_hysteresis(
            motion_this_frame,
            &mut self.motion_over_count,
            &mut self.motion_under_count,
            &mut self.motion_active,
            |a| {
                signals.push(if a {
                    MotionSignal::MotionStarted { changed_ratio }
                } else {
                    MotionSignal::MotionStopped
                })
            },
        );

        signals
    }
}

/// Hysteresis shared by all detectors: `active` flips only after `condition`
/// has held (or not held) for `SUSTAIN_FRAMES` consecutive calls, and
/// `on_flip` is called with the new state when it does.
fn update_hysteresis(
    condition: bool,
    over_count: &mut u32,
    under_count: &mut u32,
    active: &mut bool,
    mut on_flip: impl FnMut(bool),
) {
    if condition {
        *over_count += 1;
        *under_count = 0;
    } else {
        *under_count += 1;
        *over_count = 0;
    }

    if !*active && *over_count >= SUSTAIN_FRAMES {
        *active = true;
        on_flip(true);
    } else if *active && *under_count >= SUSTAIN_FRAMES {
        *active = false;
        on_flip(false);
    }
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

    fn flat_frame(value: u8) -> Vec<u8> {
        vec![value; (W * H) as usize]
    }

    fn checkerboard(a: u8, b: u8) -> Vec<u8> {
        (0..W * H).map(|i| if i % 2 == 0 { a } else { b }).collect()
    }

    #[test]
    fn wrong_size_frame_is_a_noop() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let signals = analyzer.process_frame(&[0u8; 3]);
        assert!(signals.is_empty());
    }

    #[test]
    fn steady_scene_reports_no_motion() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let frame = checkerboard(50, 60); // some texture, but unchanging
        for _ in 0..10 {
            let signals = analyzer.process_frame(&frame);
            assert!(signals.iter().all(|s| !matches!(
                s,
                MotionSignal::MotionStarted { .. } | MotionSignal::SceneChange { .. }
            )));
        }
    }

    #[test]
    fn sustained_local_change_reports_motion_started_then_stopped() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let background = flat_frame(50);
        // Warm up the background estimate against a stable scene.
        for _ in 0..20 {
            analyzer.process_frame(&background);
        }

        // Change 2 of 16 pixels a lot: motion range, below the scene-change ratio.
        let mut moved = background.clone();
        moved[0] = 220;
        moved[1] = 220;

        let mut started = false;
        for _ in 0..SUSTAIN_FRAMES {
            let signals = analyzer.process_frame(&moved);
            if signals.contains(&MotionSignal::MotionStarted {
                changed_ratio: 2.0 / 16.0,
            }) {
                started = true;
            }
        }
        assert!(
            started,
            "expected MotionStarted after sustained local change"
        );

        // Back to background; motion should clear after the sustain window.
        let mut stopped = false;
        for _ in 0..SUSTAIN_FRAMES {
            let signals = analyzer.process_frame(&background);
            if signals.contains(&MotionSignal::MotionStopped) {
                stopped = true;
            }
        }
        assert!(stopped, "expected MotionStopped once the scene settles");
    }

    #[test]
    fn frame_wide_change_reports_scene_change_not_motion() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let background = flat_frame(50);
        for _ in 0..20 {
            analyzer.process_frame(&background);
        }

        let inverted = flat_frame(220); // every pixel changes, well past the scene-change ratio
        let mut saw_scene_change = false;
        let mut saw_motion_started = false;
        for _ in 0..SUSTAIN_FRAMES {
            for s in analyzer.process_frame(&inverted) {
                match s {
                    MotionSignal::SceneChange { .. } => saw_scene_change = true,
                    MotionSignal::MotionStarted { .. } => saw_motion_started = true,
                    _ => {}
                }
            }
        }
        assert!(
            saw_scene_change,
            "expected SceneChange for a frame-wide difference"
        );
        assert!(
            !saw_motion_started,
            "a scene change should not also report as motion"
        );
    }

    #[test]
    fn near_uniform_frame_reports_tamper() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        // A perfectly flat frame has zero variance -> "covered".
        let mut detected = false;
        for _ in 0..SUSTAIN_FRAMES {
            for s in analyzer.process_frame(&flat_frame(10)) {
                if matches!(s, MotionSignal::TamperDetected { .. }) {
                    detected = true;
                }
            }
        }
        assert!(detected);
    }

    #[test]
    fn textured_frame_does_not_report_tamper() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let mut detected = false;
        for _ in 0..SUSTAIN_FRAMES {
            for s in analyzer.process_frame(&checkerboard(10, 240)) {
                if matches!(s, MotionSignal::TamperDetected { .. }) {
                    detected = true;
                }
            }
        }
        assert!(!detected);
    }

    #[test]
    fn identical_consecutive_frames_report_signal_lost_then_restored() {
        let mut analyzer = MotionAnalyzer::new(W, H);
        let frame = checkerboard(30, 200);

        let mut lost = false;
        for _ in 0..(FROZEN_FRAME_COUNT + 1) {
            for s in analyzer.process_frame(&frame) {
                if s == MotionSignal::SignalLost {
                    lost = true;
                }
            }
        }
        assert!(lost);

        // A single differing byte breaks the identical-frame run.
        let mut changed = frame.clone();
        changed[0] = changed[0].wrapping_add(1);
        let signals = analyzer.process_frame(&changed);
        assert!(signals.contains(&MotionSignal::SignalRestored));
    }
}
