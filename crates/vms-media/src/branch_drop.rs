//! Frame dropping for tee branches that fall behind.
//!
//! Every tee branch starts with a `queue`. A queue that blocks when full stalls
//! the tee and with it every other branch, so a branch that cannot keep up
//! drops frames instead. [`install_keyframe_gate`] drops whole runs of frames
//! up to the next keyframe, for consumers that need decodable GOPs.
//! [`make_leaky`] turns a queue into a plain `leaky=downstream` one, for
//! consumers that only sample frames.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use gstreamer::prelude::*;
use prometheus::{IntCounter, IntCounterVec, Opts};
use uuid::Uuid;

/// Minimum time between two "dropping frames" warnings for one branch.
const WARN_INTERVAL: Duration = Duration::from_secs(10);

/// `vms_branch_dropped_frames_total{camera, branch}`, shared by every camera
/// in the process. Register it with a registry to expose it.
pub fn branch_dropped_frames() -> &'static IntCounterVec {
    static COUNTER: OnceLock<IntCounterVec> = OnceLock::new();
    COUNTER.get_or_init(|| {
        IntCounterVec::new(
            Opts::new(
                "vms_branch_dropped_frames_total",
                "Frames a camera's tee branch dropped because its consumer fell behind",
            ),
            &["camera", "branch"],
        )
        .expect("valid metric definition")
    })
}

/// Counts one branch's dropped frames and logs a warning at most once per
/// [`WARN_INTERVAL`], with the number dropped since the last warning.
pub(crate) struct DropReporter {
    camera_id: Uuid,
    branch: String,
    counter: IntCounter,
    pending: Mutex<(Option<Instant>, u64)>,
}

impl DropReporter {
    pub(crate) fn new(camera_id: Uuid, branch: &str) -> Arc<Self> {
        let counter =
            branch_dropped_frames().with_label_values(&[camera_id.to_string().as_str(), branch]);
        Arc::new(Self {
            camera_id,
            branch: branch.to_owned(),
            counter,
            pending: Mutex::new((None, 0)),
        })
    }

    pub(crate) fn dropped(&self, frames: u64) {
        self.counter.inc_by(frames);
        let mut pending = self.pending.lock().unwrap();
        pending.1 += frames;
        let due = pending.0.is_none_or(|at| at.elapsed() >= WARN_INTERVAL);
        if due {
            tracing::warn!(
                camera_id = %self.camera_id,
                branch = %self.branch,
                dropped = pending.1,
                "Branch is falling behind, dropping frames",
            );
            *pending = (Some(Instant::now()), 0);
        }
    }
}

/// How a keyframe gate reports a run of dropped frames once it resumes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum GapReport {
    /// Only the rate-limited warning from [`DropReporter`].
    Warn,
    /// An error per run with the length of the gap, for branches where every
    /// lost frame is lost data.
    Error,
}

/// A queue's `max-size-*` limits, where 0 means unlimited.
struct QueueLimits {
    buffers: u32,
    bytes: u32,
    time: u64,
}

impl QueueLimits {
    fn of(queue: &gstreamer::Element) -> Self {
        Self {
            buffers: queue.property("max-size-buffers"),
            bytes: queue.property("max-size-bytes"),
            time: queue.property("max-size-time"),
        }
    }

    /// The queue's fill level as the largest fraction of any nonzero limit.
    fn fill(&self, queue: &gstreamer::Element) -> f64 {
        let mut fill: f64 = 0.0;
        if self.buffers > 0 {
            let level: u32 = queue.property("current-level-buffers");
            fill = fill.max(level as f64 / self.buffers as f64);
        }
        if self.bytes > 0 {
            let level: u32 = queue.property("current-level-bytes");
            fill = fill.max(level as f64 / self.bytes as f64);
        }
        if self.time > 0 {
            let level: u64 = queue.property("current-level-time");
            fill = fill.max(level as f64 / self.time as f64);
        }
        fill
    }
}

/// A run of frames dropped since the queue was last found full.
struct DropRun {
    first_pts: Option<gstreamer::ClockTime>,
    frames: u64,
}

/// Drop frames at `queue`'s sink pad while the queue is full, up to the next
/// keyframe that finds it at most half full, so the consumer only ever sees
/// whole GOPs and the tee never blocks on this branch.
///
/// Only the tee's streaming thread pushes into the queue, so a queue found not
/// full here cannot fill before this buffer is queued.
pub(crate) fn install_keyframe_gate(
    queue: &gstreamer::Element,
    camera_id: Uuid,
    branch: &str,
    report: GapReport,
) {
    let Some(sink) = queue.static_pad("sink") else {
        return;
    };
    let limits = QueueLimits::of(queue);
    let reporter = DropReporter::new(camera_id, branch);
    let branch = branch.to_owned();
    let run: Mutex<Option<DropRun>> = Mutex::new(None);
    let queue = queue.downgrade();
    sink.add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
        let (Some(buffer), Some(queue)) = (info.buffer(), queue.upgrade()) else {
            return gstreamer::PadProbeReturn::Ok;
        };
        let keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);
        let mut run = run.lock().unwrap();
        match run.as_mut() {
            Some(current) => {
                if keyframe && limits.fill(&queue) <= 0.5 {
                    if report == GapReport::Error {
                        let gap = buffer
                            .pts()
                            .zip(current.first_pts)
                            .map(|(now, first)| now.saturating_sub(first));
                        tracing::error!(
                            camera_id = %camera_id,
                            branch = %branch,
                            frames = current.frames,
                            gap_ms = gap.map(|g| g.mseconds()),
                            "Branch fell behind, frames are missing",
                        );
                    }
                    *run = None;
                    return gstreamer::PadProbeReturn::Ok;
                }
                current.frames += 1;
            }
            None => {
                if limits.fill(&queue) < 1.0 {
                    return gstreamer::PadProbeReturn::Ok;
                }
                *run = Some(DropRun {
                    first_pts: buffer.pts(),
                    frames: 1,
                });
            }
        }
        reporter.dropped(1);
        gstreamer::PadProbeReturn::Handled
    });
}

/// Make `queue` drop its oldest frame instead of blocking when full, counting
/// one dropped frame per `overrun`.
pub(crate) fn make_leaky(queue: &gstreamer::Element, camera_id: Uuid, branch: &str) {
    queue.set_property_from_str("leaky", "downstream");
    let reporter = DropReporter::new(camera_id, branch);
    queue.connect("overrun", false, move |_| {
        reporter.dropped(1);
        None
    });
}
