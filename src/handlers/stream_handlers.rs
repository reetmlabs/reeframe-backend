//! HTTP handlers for camera stream lifecycle and quality control.
//!
//! These endpoints allow the frontend to connect/disconnect a camera and to switch the
//! quality of the live RTSP stream that the backend serves.
//!
//! | Method | Path                          | Action                                   |
//! |--------|-------------------------------|------------------------------------------|
//! | POST   | `/feeds/{id}/connect`         | Connect to camera, start both pipelines  |
//! | POST   | `/feeds/{id}/disconnect`      | Stop pipelines, remove RTSP mount points |
//! | POST   | `/feeds/{id}/stream/quality`  | Switch live stream quality (low/high)    |

use salvo::prelude::*;
use sea_orm::{DatabaseConnection, EntityTrait};
use serde::Deserialize;

use crate::entities::{feed, settings};
use crate::recorder::{CameraManager, StreamQuality};

// ---------------------------------------------------------------------------
// connect
// ---------------------------------------------------------------------------

/// Connect to the camera and start its GStreamer pipelines.
///
/// After a successful connect the camera's live stream is available at:
/// `rtsp://<backend>:8554/live/feed_{id}` (low-res by default).
/// Playback of recordings is available at:
/// `rtsp://<backend>:8554/playback/feed_{id}`.
///
/// Returns `200 OK` with a confirmation message, or an appropriate error status.
#[handler]
pub async fn connect_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let camera_manager = dep
        .get::<CameraManager>("camera_manager")
        .expect("CameraManager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let settings = match settings::Entity::find().one(db).await {
        Ok(Some(s)) => s,
        _ => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render("Failed to fetch settings");
            return;
        }
    };

    match feed::Entity::find_by_id(feed_id).one(db).await {
        Ok(Some(f)) => match camera_manager.connect(&f, &settings) {
            Ok(_) => res.render(format!(
                "Feed {} connected. Live: rtsp://<host>:8554/live/feed_{}, Playback: rtsp://<host>:8554/playback/feed_{}",
                feed_id, feed_id, feed_id
            )),
            Err(e) => {
                res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                res.render(format!("Error connecting feed {}: {}", feed_id, e));
            }
        },
        Ok(None) => { res.status_code(StatusCode::NOT_FOUND); }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// disconnect
// ---------------------------------------------------------------------------

/// Disconnect the camera, stopping its GStreamer pipelines.
///
/// Any in-progress recording is stopped gracefully.  The RTSP mount points for this feed
/// are removed from the backend server.
///
/// Returns `200 OK` with a confirmation message, or `400 Bad Request` if the feed was not
/// connected.
#[handler]
pub async fn disconnect_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let camera_manager = dep
        .get::<CameraManager>("camera_manager")
        .expect("CameraManager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    match camera_manager.disconnect(feed_id) {
        Ok(_) => res.render(format!("Feed {} disconnected", feed_id)),
        Err(e) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(format!("Error disconnecting feed {}: {}", feed_id, e));
        }
    }
}

// ---------------------------------------------------------------------------
// switch_quality
// ---------------------------------------------------------------------------

/// Request body for the quality-switch endpoint.
#[derive(Deserialize, Debug)]
struct SwitchQualityRequest {
    /// Target quality: `"low"` or `"high"`.
    quality: String,
}

/// Switch the quality of the live RTSP stream served by the backend.
///
/// The pump task picks up the change within one buffer interval (≤ ~100 ms), so the
/// client sees the quality change almost immediately without reconnecting.
///
/// # Request body (JSON)
/// ```json
/// { "quality": "high" }
/// ```
/// Valid values: `"low"`, `"high"`.
///
/// Returns `200 OK`, `400 Bad Request` for an unknown quality string, or `500` if the
/// feed is not connected.
#[handler]
pub async fn switch_quality(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let camera_manager = dep
        .get::<CameraManager>("camera_manager")
        .expect("CameraManager missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let body = match req.parse_json::<SwitchQualityRequest>().await {
        Ok(b) => b,
        Err(e) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(format!("Invalid request body: {}", e));
            return;
        }
    };

    let quality = match body.quality.as_str() {
        "low" => StreamQuality::Low,
        "high" => StreamQuality::High,
        other => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(format!(
                "Unknown quality '{}'; expected 'low' or 'high'",
                other
            ));
            return;
        }
    };

    match camera_manager.switch_quality(feed_id, quality) {
        Ok(_) => res.render(format!(
            "Feed {} live stream switched to {} quality",
            feed_id, body.quality
        )),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error switching quality: {}", e));
        }
    }
}
