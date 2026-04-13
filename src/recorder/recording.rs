//! Recording valve control, pre-alarm buffer management, and DB segment indexing.
//!
//! [`RecordingController`] owns the GStreamer `valve` and `splitmuxsink` elements that
//! sit on the high-res pipeline branch.  It is responsible for:
//!
//! * Toggling the valve to start/stop writing to disk.
//! * Listening for `splitmuxsink-fragment-opened` and `splitmuxsink-fragment-closed`
//!   signals and forwarding [`SegmentEvent`]s to the database indexer task via a channel.
//! * Auto-stopping timed recordings (AI, hardware, schedule) after their configured
//!   duration, with duration-extension support when a second trigger fires before the
//!   first one expires.
//!
//! The database indexer runs as a background Tokio task and is shared across all feeds.
//! Feed-specific [`RecordingController`]s communicate with it through a single
//! `mpsc::Sender<SegmentEvent>`.

use anyhow::Result;
use chrono::Utc;
use gstreamer as gst;
use gstreamer::prelude::*;
use sea_orm::{ActiveModelTrait, DatabaseConnection, Set};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::entities::recording_segment;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// What caused a recording to start.
///
/// Stored in the `recording_segments` table so the frontend can filter or
/// colour-code segments by their origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerType {
    /// The user pressed "Record" in the UI.
    User,
    /// An AI inference model detected an event on the low-res stream.
    Ai,
    /// A physical input (e.g. door sensor, PIR) fired a hardware trigger.
    Hardware,
    /// A time-based schedule rule triggered the recording.
    Schedule,
}

impl TriggerType {
    /// Convert to the string stored in the database.
    pub fn as_str(self) -> &'static str {
        match self {
            TriggerType::User => "user",
            TriggerType::Ai => "ai",
            TriggerType::Hardware => "hardware",
            TriggerType::Schedule => "schedule",
        }
    }
}

// ---------------------------------------------------------------------------
// Segment events (sent from GStreamer signal callbacks → DB indexer)
// ---------------------------------------------------------------------------

/// Events sent from `splitmuxsink` signal callbacks to the DB indexer task.
pub enum SegmentEvent {
    /// A new MP4 file was opened by `splitmuxsink`.
    Opened {
        feed_id: i32,
        file_path: String,
        trigger: TriggerType,
        /// Wall-clock start time (UTC).
        start_time: chrono::DateTime<Utc>,
    },
    /// An MP4 file was closed (the chunk is complete).
    Closed {
        file_path: String,
        /// Duration of the segment, computed from the GStreamer running-time delta.
        duration_secs: f64,
    },
}

// ---------------------------------------------------------------------------
// RecordingController
// ---------------------------------------------------------------------------

/// Controls the recording branch of a single camera's high-res GStreamer pipeline.
///
/// One instance exists per connected feed.  Thread-safe via `Arc<Mutex<…>>` internals.
pub struct RecordingController {
    /// Feed database ID (used when constructing `SegmentEvent::Opened`).
    feed_id: i32,
    /// GStreamer valve element (`drop=true` ⇒ no data flows to the muxer).
    valve: gst::Element,
    /// Channel to the shared DB indexer task.  Not read directly — cloned into signal
    /// closures in [`RecordingController::connect_mux_signals`].
    #[allow(dead_code)]
    db_tx: mpsc::Sender<SegmentEvent>,
    /// Expiry instant for a timed event recording.  `None` when no timed recording is
    /// active.
    event_stop_time: Arc<Mutex<Option<Instant>>>,
    /// The trigger type that opened the current event recording (so it can be forwarded
    /// to `SegmentEvent::Opened`).
    current_trigger: Arc<Mutex<TriggerType>>,
    /// Whether a user-commanded (manual) recording is active.
    user_recording_active: Arc<Mutex<bool>>,
}

impl RecordingController {
    /// Construct a new controller and wire up `splitmuxsink` signals.
    ///
    /// # Arguments
    /// * `feed_id` — Database ID of the owning feed.
    /// * `valve`   — The GStreamer `valve` element that gates data into the muxer.
    /// * `mux`     — The `splitmuxsink` element whose fragment signals are connected.
    /// * `db_tx`   — Sender half of the channel to the DB indexer task.
    ///
    /// # Errors
    /// Returns an error if the `splitmuxsink` signals cannot be connected.
    pub fn new(
        feed_id: i32,
        valve: gst::Element,
        mux: gst::Element,
        db_tx: mpsc::Sender<SegmentEvent>,
    ) -> Result<Self> {
        let ctrl = Self {
            feed_id,
            valve,
            db_tx: db_tx.clone(),
            event_stop_time: Arc::new(Mutex::new(None)),
            current_trigger: Arc::new(Mutex::new(TriggerType::User)),
            user_recording_active: Arc::new(Mutex::new(false)),
        };

        ctrl.connect_mux_signals(&mux, db_tx)?;
        Ok(ctrl)
    }

    /// Connect GStreamer signals from `splitmuxsink` to forward [`SegmentEvent`]s.
    fn connect_mux_signals(
        &self,
        mux: &gst::Element,
        db_tx: mpsc::Sender<SegmentEvent>,
    ) -> Result<()> {
        let feed_id = self.feed_id;
        let current_trigger = Arc::clone(&self.current_trigger);

        // `splitmuxsink-fragment-opened` fires when a new MP4 chunk is created.
        let tx_open = db_tx.clone();
        let trig_open = Arc::clone(&current_trigger);
        mux.connect("splitmuxsink-fragment-opened", false, move |args| {
            // args[0] = element, args[1] = location (String), args[2] = running-time
            if let Some(location) = args.get(1).and_then(|v| v.get::<String>().ok()) {
                let trigger = *trig_open.lock().unwrap();
                let event = SegmentEvent::Opened {
                    feed_id,
                    file_path: location,
                    trigger,
                    start_time: Utc::now(),
                };
                if let Err(e) = tx_open.blocking_send(event) {
                    warn!("Failed to send SegmentEvent::Opened for feed {}: {}", feed_id, e);
                }
            }
            None
        });

        // `splitmuxsink-fragment-closed` fires when a chunk is finalized.
        let tx_close = db_tx.clone();
        mux.connect("splitmuxsink-fragment-closed", false, move |args| {
            // args[1] = location, args[2] = running-time (ns)
            if let (Some(location), Some(rt_ns)) = (
                args.get(1).and_then(|v| v.get::<String>().ok()),
                args.get(2).and_then(|v| v.get::<u64>().ok()),
            ) {
                let duration_secs = rt_ns as f64 / 1_000_000_000.0;
                let event = SegmentEvent::Closed {
                    file_path: location,
                    duration_secs,
                };
                if let Err(e) = tx_close.blocking_send(event) {
                    warn!("Failed to send SegmentEvent::Closed for feed {}: {}", feed_id, e);
                }
            }
            None
        });

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Valve helpers
    // -----------------------------------------------------------------------

    /// Open the recording valve (allow data to flow into the muxer).
    fn open_valve(&self) {
        self.valve.set_property("drop", false);
    }

    /// Close the recording valve (drop data before the muxer).
    fn close_valve(&self) {
        // Only close if neither user recording nor an event recording is active.
        let user_active = *self.user_recording_active.lock().unwrap();
        let event_active = self.event_stop_time.lock().unwrap().is_some();
        if !user_active && !event_active {
            self.valve.set_property("drop", true);
        }
    }

    // -----------------------------------------------------------------------
    // User-commanded recording
    // -----------------------------------------------------------------------

    /// Start a user-commanded recording.
    ///
    /// Opens the valve immediately.  The recording continues until `stop_user_recording`
    /// is called, regardless of any concurrent event recordings.
    pub fn start_user_recording(&self) {
        info!("feed {}: starting user recording", self.feed_id);
        *self.current_trigger.lock().unwrap() = TriggerType::User;
        *self.user_recording_active.lock().unwrap() = true;
        self.open_valve();
    }

    /// Stop the user-commanded recording.
    ///
    /// Closes the valve only if no timed event recording is currently active.
    pub fn stop_user_recording(&self) {
        info!("feed {}: stopping user recording", self.feed_id);
        *self.user_recording_active.lock().unwrap() = false;
        self.close_valve();
    }

    // -----------------------------------------------------------------------
    // Event-triggered recording (AI / hardware / schedule)
    // -----------------------------------------------------------------------

    /// Start or extend a timed event recording.
    ///
    /// If no event recording is active, opens the valve immediately.  If one is already
    /// running and `new_stop` is later than the current stop time, the deadline is
    /// extended — no restart occurs, so the pre-alarm buffer is not re-flushed.
    ///
    /// A background OS thread polls every 500 ms and closes the valve once the deadline
    /// passes (matching the original AI-recording behaviour).
    ///
    /// # Arguments
    /// * `trigger`  — What caused this recording.
    /// * `duration` — How long to record after the trigger.
    pub fn trigger_event_recording(&self, trigger: TriggerType, duration: Duration) {
        info!(
            "feed {}: event recording triggered ({:?}) for {:?}",
            self.feed_id, trigger, duration
        );

        let new_stop = Instant::now() + duration;
        let mut stop_lock = self.event_stop_time.lock().unwrap();

        if let Some(current_stop) = *stop_lock {
            // Extend if the new deadline is later.
            if new_stop > current_stop {
                *stop_lock = Some(new_stop);
                info!("feed {}: event recording deadline extended", self.feed_id);
            }
        } else {
            // No active event recording — start one.
            *stop_lock = Some(new_stop);
            *self.current_trigger.lock().unwrap() = trigger;
            self.open_valve();

            // Spawn a timer thread to auto-stop when the deadline passes.
            let valve = self.valve.clone();
            let stop_time_shared = Arc::clone(&self.event_stop_time);
            let user_active_shared = Arc::clone(&self.user_recording_active);
            let feed_id = self.feed_id;

            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_millis(500));
                    let mut lock = stop_time_shared.lock().unwrap();
                    if let Some(stop_at) = *lock {
                        if Instant::now() >= stop_at {
                            info!("feed {}: event recording deadline reached, stopping", feed_id);
                            *lock = None;
                            // Only close the valve if the user is not also recording.
                            if !*user_active_shared.lock().unwrap() {
                                valve.set_property("drop", true);
                            }
                            break;
                        }
                    } else {
                        break;
                    }
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// DB indexer task
// ---------------------------------------------------------------------------

/// Background Tokio task that receives [`SegmentEvent`]s and writes them to the
/// `recording_segments` table.
///
/// This function runs forever and should be spawned with `tokio::spawn`.  It correlates
/// `Opened` and `Closed` events using the file path as the key.
///
/// # Arguments
/// * `db` — SeaORM database connection shared with the rest of the application.
/// * `rx` — Receiver half of the channel that [`RecordingController`] instances write to.
pub async fn recording_segment_indexer(
    db: DatabaseConnection,
    mut rx: mpsc::Receiver<SegmentEvent>,
) {
    // Maps file_path → segment row ID so we can update the row on close.
    let mut open_segments: HashMap<String, i32> = HashMap::new();

    while let Some(event) = rx.recv().await {
        match event {
            SegmentEvent::Opened {
                feed_id,
                file_path,
                trigger,
                start_time,
            } => {
                let active = recording_segment::ActiveModel {
                    feed_id: Set(feed_id),
                    file_path: Set(file_path.clone()),
                    start_time: Set(start_time.into()),
                    trigger_type: Set(trigger.as_str().to_owned()),
                    ..Default::default()
                };
                match active.insert(&db).await {
                    Ok(row) => {
                        info!("DB: opened segment id={} path={}", row.id, file_path);
                        open_segments.insert(file_path, row.id);
                    }
                    Err(e) => error!("DB: failed to insert segment for {}: {}", file_path, e),
                }
            }

            SegmentEvent::Closed {
                file_path,
                duration_secs,
            } => {
                if let Some(segment_id) = open_segments.remove(&file_path) {
                    // Read file size from the filesystem.
                    let file_size = std::fs::metadata(&file_path)
                        .map(|m| m.len() as i64)
                        .ok();

                    let active = recording_segment::ActiveModel {
                        id: Set(segment_id),
                        end_time: Set(Some(Utc::now().into())),
                        duration_secs: Set(Some(duration_secs)),
                        file_size_bytes: Set(file_size),
                        ..Default::default()
                    };
                    match active.update(&db).await {
                        Ok(_) => info!("DB: closed segment id={} ({:.1}s)", segment_id, duration_secs),
                        Err(e) => error!("DB: failed to update segment id={}: {}", segment_id, e),
                    }
                } else {
                    warn!("DB: received Closed event for unknown path: {}", file_path);
                }
            }
        }
    }
}
