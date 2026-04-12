//! HTTP handlers for controlling camera recordings.
//!
//! All recording operations ultimately open or close the GStreamer `valve` element that
//! gates data flow into `splitmuxsink`.  The pre-alarm rolling buffer (a leaky queue
//! upstream of the valve) ensures that footage from up to `pre_event_cache_duration_secs`
//! before any trigger is captured.
//!
//! | Method | Path                            | Trigger type |
//! |--------|---------------------------------|--------------|
//! | POST   | `/feeds/{id}/record/start`      | User (manual) |
//! | POST   | `/feeds/{id}/record/stop`       | — (stops user recording) |
//! | POST   | `/feeds/{id}/ai-event`          | AI inference |
//! | POST   | `/feeds/{id}/hardware-event`    | Physical sensor/trigger |
//! | POST   | `/feeds/{id}/schedule-event`    | Time-based schedule rule |

use salvo::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use serde::Deserialize;
use std::time::Duration;

use crate::entities::{feed, settings};
use crate::recorder::{CameraManager, TriggerType};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Fetch the global settings row from the database.
///
/// Returns `None` and renders a `500` response if the settings cannot be loaded.
async fn fetch_settings(
    db: &DatabaseConnection,
    res: &mut Response,
) -> Option<settings::Model> {
    match settings::Entity::find().one(db).await {
        Ok(Some(s)) => Some(s),
        _ => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Failed to fetch settings");
            None
        }
    }
}

/// Fetch a feed by ID.
///
/// Returns `None` and renders an appropriate error response if the feed is missing or the
/// query fails.
async fn fetch_feed(
    db: &DatabaseConnection,
    feed_id: i32,
    res: &mut Response,
) -> Option<feed::Model> {
    match feed::Entity::find_by_id(feed_id).one(db).await {
        Ok(Some(f)) => Some(f),
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
            None
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
            None
        }
    }
}

// ---------------------------------------------------------------------------
// User-commanded recording
// ---------------------------------------------------------------------------

/// Start a manual (user-commanded) recording on the feed's high-res stream.
///
/// The recording is written in chunks whose duration is configured by
/// `settings.recording_chunk_duration_mins`.  The recording continues until
/// `record/stop` is called.
///
/// If the camera is not yet connected, the backend connects it automatically.
#[handler]
pub async fn start_recording(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let camera_manager = dep.get::<CameraManager>("camera_manager").expect("CameraManager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let Some(settings) = fetch_settings(db, res).await else { return; };
    let Some(f) = fetch_feed(db, feed_id, res).await else { return; };

    match camera_manager.start_recording(&f, &settings) {
        Ok(_) => res.render(format!("Started recording for feed {}", f.id)),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error starting recording: {}", e));
        }
    }
}

/// Stop the manual (user-commanded) recording.
///
/// Closes the recording valve.  If an event-triggered recording (AI/hardware/schedule)
/// is concurrently active the valve stays open until that recording also ends.
#[handler]
pub async fn stop_recording(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let camera_manager = dep.get::<CameraManager>("camera_manager").expect("CameraManager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    match camera_manager.stop_recording(feed_id) {
        Ok(_) => res.render(format!("Stopped recording for feed {}", feed_id)),
        Err(e) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(format!("Error stopping recording: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// Event-triggered recordings
// ---------------------------------------------------------------------------

/// Shared body for event recording endpoints that accept a custom duration override.
#[derive(Deserialize, Debug, Default)]
struct EventRequest {
    /// Override the recording duration (seconds).  When absent, the per-feed or global
    /// default is used.
    duration_secs: Option<i32>,
}

/// Trigger an AI-detected-event recording on the feed's high-res stream.
///
/// The pre-alarm buffer ensures footage from before the trigger is captured.  If a
/// recording is already active its deadline is extended rather than restarted.
///
/// Duration priority: request body `duration_secs` → `feed.ai_recording_duration_secs` →
/// `settings.default_ai_recording_duration_secs`.
#[handler]
pub async fn handle_ai_event(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    handle_event(dep, req, res, TriggerType::Ai).await;
}

/// Trigger a hardware-event recording (e.g., PIR sensor, door contact, relay input).
///
/// Behaves identically to [`handle_ai_event`] but records `trigger_type = "hardware"` in
/// the database so the frontend can distinguish event origins.
#[handler]
pub async fn handle_hardware_event(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    handle_event(dep, req, res, TriggerType::Hardware).await;
}

/// Trigger a schedule-based recording (e.g., a cron rule decided to start recording).
///
/// The optional `duration_secs` body field allows the scheduler to specify exactly how
/// long to record.
#[handler]
pub async fn handle_schedule_event(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    handle_event(dep, req, res, TriggerType::Schedule).await;
}

/// Internal helper shared by all event-triggered recording handlers.
///
/// # Arguments
/// * `trigger` — The type of event that caused this recording.
async fn handle_event(
    dep: &mut Depot,
    req: &mut Request,
    res: &mut Response,
    trigger: TriggerType,
) {
    let db = dep.get::<DatabaseConnection>("db").expect("Database connection missing");
    let camera_manager = dep.get::<CameraManager>("camera_manager").expect("CameraManager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let Some(settings) = fetch_settings(db, res).await else { return; };
    let Some(f) = fetch_feed(db, feed_id, res).await else { return; };

    // Parse optional body; if absent or malformed use defaults.
    let body: EventRequest = req.parse_json().await.unwrap_or_default();

    // Duration precedence: body override → per-feed default → global default.
    let duration_secs = body
        .duration_secs
        .or(f.ai_recording_duration_secs)
        .unwrap_or(settings.default_ai_recording_duration_secs);

    let duration = Duration::from_secs(duration_secs as u64);

    match camera_manager.trigger_event(&f, &settings, trigger, duration) {
        Ok(_) => res.render(format!(
            "Triggered {:?} recording for feed {} ({} s)",
            trigger, f.id, duration_secs
        )),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error triggering recording: {}", e));
        }
    }
}
