//! Frame dropping for tee branches that fall behind.
//!
//! A tee branch that blocks stalls the tee and with it every other branch, so a
//! branch that cannot keep up drops frames instead. [`install_keyframe_gate`]
//! drops whole runs of frames at a queue up to the next keyframe, for consumers
//! that need decodable GOPs. [`make_leaky`] turns a queue into a plain
//! `leaky=downstream` one, for consumers that only sample frames. Appsinks
//! attached straight to the tee drop frames in their own callbacks and report
//! them through [`DropReporter`].

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
/// keyframe that finds it not full, so the consumer only ever sees
/// whole GOPs and the tee never blocks on this branch. Each run of dropped
/// frames is logged as an error with the length of the gap.
///
/// Only the tee's streaming thread pushes into the queue, so a queue found not
/// full here cannot fill before this buffer is queued.
pub(crate) fn install_keyframe_gate(queue: &gstreamer::Element, camera_id: Uuid, branch: &str) {
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
                if keyframe && limits.fill(&queue) < 1.0 {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A `queue -> fakesink` pipeline whose queue only drains one buffer per
    /// token sent on `release`, or freely once `release` is dropped, and the
    /// PTS of every buffer the sink got.
    struct StuckQueue {
        pipeline: gstreamer::Pipeline,
        queue: gstreamer::Element,
        release: Option<std::sync::mpsc::Sender<()>>,
        received: Arc<Mutex<Vec<u64>>>,
    }

    impl StuckQueue {
        fn new(max_buffers: u32) -> Self {
            gstreamer::init().ok();
            let pipeline = gstreamer::Pipeline::new();
            let queue = gstreamer::ElementFactory::make("queue")
                .property("max-size-buffers", max_buffers)
                .property("max-size-bytes", 0u32)
                .property("max-size-time", 0u64)
                .build()
                .unwrap();
            let sink = gstreamer::ElementFactory::make("fakesink")
                .property("sync", false)
                .property("async", false)
                .build()
                .unwrap();
            pipeline.add_many([&queue, &sink]).unwrap();
            queue.link(&sink).unwrap();

            let received = Arc::new(Mutex::new(Vec::new()));
            let in_probe = received.clone();
            sink.static_pad("sink").unwrap().add_probe(
                gstreamer::PadProbeType::BUFFER,
                move |_, info| {
                    if let Some(pts) = info.buffer().and_then(|b| b.pts()) {
                        in_probe.lock().unwrap().push(pts.mseconds() / 50);
                    }
                    gstreamer::PadProbeReturn::Ok
                },
            );
            let (release, tokens) = std::sync::mpsc::channel::<()>();
            let tokens = Mutex::new(tokens);
            queue.static_pad("src").unwrap().add_probe(
                gstreamer::PadProbeType::BUFFER,
                move |_, _| {
                    let _ = tokens.lock().unwrap().recv();
                    gstreamer::PadProbeReturn::Ok
                },
            );
            Self {
                pipeline,
                queue,
                release: Some(release),
                received,
            }
        }

        fn start(&self) {
            self.pipeline.set_state(gstreamer::State::Playing).unwrap();
            let sink = self.queue.static_pad("sink").unwrap();
            sink.send_event(gstreamer::event::StreamStart::new("test"));
            sink.send_event(gstreamer::event::Caps::new(
                &gstreamer::Caps::builder("video/x-h264").build(),
            ));
            sink.send_event(gstreamer::event::Segment::new(
                &gstreamer::FormattedSegment::<gstreamer::ClockTime>::new(),
            ));
        }

        /// Pushes frame `n` (PTS `n * 50 ms`) and fails if the push blocks.
        fn push(&self, n: u64, keyframe: bool) {
            let mut buffer = gstreamer::Buffer::with_size(16).unwrap();
            {
                let buffer = buffer.get_mut().unwrap();
                buffer.set_pts(gstreamer::ClockTime::from_mseconds(n * 50));
                if !keyframe {
                    buffer.set_flags(gstreamer::BufferFlags::DELTA_UNIT);
                }
            }
            let sink = self.queue.static_pad("sink").unwrap();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = done_tx.send(sink.chain(buffer));
            });
            let flow = done_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap_or_else(|_| panic!("pushing frame {n} blocked"));
            assert!(flow.is_ok(), "pushing frame {n}: {flow:?}");
        }

        /// Lets one buffer out of the queue and waits until its level drops
        /// to `level`.
        fn release_one(&self, level: u32) {
            self.release.as_ref().unwrap().send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.queue.property::<u32>("current-level-buffers") > level {
                assert!(Instant::now() < deadline, "the queue did not drain");
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        /// Lets the queue drain and waits until it is empty.
        fn unblock(&mut self) {
            self.release.take();
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.queue.property::<u32>("current-level-buffers") > 0 {
                assert!(Instant::now() < deadline, "the queue did not drain");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl Drop for StuckQueue {
        fn drop(&mut self) {
            self.release.take();
            self.pipeline.set_state(gstreamer::State::Null).ok();
        }
    }

    fn dropped(camera_id: Uuid, branch: &str) -> u64 {
        branch_dropped_frames()
            .with_label_values(&[camera_id.to_string().as_str(), branch])
            .get()
    }

    #[test]
    fn a_full_gated_queue_drops_frames_until_a_keyframe() {
        let camera_id = Uuid::new_v4();
        let mut stuck = StuckQueue::new(4);
        install_keyframe_gate(&stuck.queue, camera_id, "restream");
        stuck.start();

        for n in 0..20 {
            stuck.push(n, n % 10 == 0);
        }
        let while_stuck = dropped(camera_id, "restream");
        assert!(while_stuck >= 14, "dropped {while_stuck} frames");

        stuck.unblock();
        stuck.push(20, false);
        stuck.push(21, true);
        stuck.push(22, false);
        std::thread::sleep(Duration::from_millis(200));

        let received = stuck.received.lock().unwrap().clone();
        assert_eq!(received[0], 0);
        assert!(received
            .windows(2)
            .take(received.len() - 3)
            .all(|w| w[1] == w[0] + 1));
        assert_eq!(received[received.len() - 2..], [21, 22], "got {received:?}");
        assert_eq!(dropped(camera_id, "restream"), while_stuck + 1);
    }

    #[test]
    fn a_gated_queue_resumes_at_a_keyframe_once_it_is_not_full() {
        let camera_id = Uuid::new_v4();
        let mut stuck = StuckQueue::new(4);
        install_keyframe_gate(&stuck.queue, camera_id, "recording");
        stuck.start();

        for n in 0..20 {
            stuck.push(n, n % 10 == 0);
        }
        stuck.release_one(3);
        stuck.push(20, false);
        stuck.push(21, true);
        stuck.unblock();
        std::thread::sleep(Duration::from_millis(200));

        let received = stuck.received.lock().unwrap().clone();
        assert_eq!(received, [0, 1, 2, 3, 4, 21]);
    }

    #[test]
    fn a_full_leaky_queue_counts_the_frames_it_drops() {
        let camera_id = Uuid::new_v4();
        let stuck = StuckQueue::new(2);
        make_leaky(&stuck.queue, camera_id, "motion");
        stuck.start();

        for n in 0..10 {
            stuck.push(n, n == 0);
        }

        assert!(dropped(camera_id, "motion") >= 6);
        assert_eq!(dropped(camera_id, "thumbnail"), 0);
    }
}
