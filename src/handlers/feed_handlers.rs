//! HTTP handlers for camera feed CRUD operations.
//!
//! A *feed* represents a single IP camera.  Feeds are stored in the `feeds` table and
//! reference two RTSP stream URLs (low-res and optionally high-res).
//!
//! | Method | Path          | Action                    |
//! |--------|---------------|---------------------------|
//! | POST   | `/feeds`      | Create a new feed         |
//! | GET    | `/feeds/{id}` | Retrieve a feed by ID     |
//! | PUT    | `/feeds/{id}` | Update feed configuration |
//! | DELETE | `/feeds/{id}` | Delete a feed             |

use salvo::prelude::*;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
use serde::{Deserialize, Serialize};

use crate::entities::feed;

// ---------------------------------------------------------------------------
// Request / Response DTOs
// ---------------------------------------------------------------------------

/// JSON body accepted by `POST /feeds` and `PUT /feeds/{id}`.
#[derive(Deserialize, Serialize, Extractible, Debug)]
#[salvo(extract(default_source = "body"))]
pub struct CreateFeedRequest {
    /// Human-readable name for this camera (e.g. "Front Door").
    pub name: String,

    /// Optional free-text description.
    pub description: Option<String>,

    /// RTSP URL for the **low-resolution** stream.  This stream is always started when
    /// the frontend calls `/connect` and is served by default.
    pub rtsp_url: String,

    /// RTSP URL for the **high-resolution** stream.  When absent the low-res URL is used
    /// for both live view and recording (single-stream camera fallback).
    pub rtsp_url_high: Option<String>,

    /// Arbitrary extra parameters as a JSON string (reserved for future use).
    pub parameters: Option<String>,

    /// Per-feed AI recording duration override in seconds.  When absent the global
    /// `default_ai_recording_duration_secs` setting is used.
    pub ai_recording_duration_secs: Option<i32>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Create a new camera feed record.
///
/// Inserts a row into the `feeds` table.  The camera is *not* connected automatically;
/// the frontend must call `POST /feeds/{id}/connect` separately.
///
/// # Request body
/// JSON matching [`CreateFeedRequest`].
///
/// # Response
/// `200 OK` with the newly created [`feed::Model`] as JSON.
#[handler]
pub async fn create_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let new_feed = req
        .parse_json::<CreateFeedRequest>()
        .await
        .expect("Failed to parse request");

    let feed_active = feed::ActiveModel {
        name: Set(new_feed.name),
        description: Set(new_feed.description),
        rtsp_url: Set(new_feed.rtsp_url),
        rtsp_url_high: Set(new_feed.rtsp_url_high),
        parameters: Set(new_feed.parameters),
        ai_recording_duration_secs: Set(new_feed.ai_recording_duration_secs),
        ..Default::default()
    };

    match feed_active.insert(db).await {
        Ok(f) => res.render(Json(f)),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error inserting feed: {}", e));
        }
    }
}

/// Retrieve a single camera feed by its primary key.
///
/// # Path parameter
/// * `id` — The `feeds.id` value.
///
/// # Response
/// `200 OK` with the [`feed::Model`] as JSON, or `404 Not Found`.
#[handler]
pub async fn get_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let id = req.param::<i32>("id").unwrap_or_default();

    match feed::Entity::find_by_id(id).one(db).await {
        Ok(Some(f)) => res.render(Json(f)),
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}

/// Update an existing camera feed's configuration.
///
/// The feed must be disconnected before updating its RTSP URLs; changes take effect on
/// the next `/connect` call.
///
/// # Path parameter
/// * `id` — The `feeds.id` value.
///
/// # Request body
/// JSON matching [`CreateFeedRequest`].
///
/// # Response
/// `200 OK` with the updated [`feed::Model`] as JSON, or `404 Not Found`.
#[handler]
pub async fn update_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let id = req.param::<i32>("id").unwrap_or_default();
    let updated = req
        .parse_json::<CreateFeedRequest>()
        .await
        .expect("Failed to parse request");

    match feed::Entity::find_by_id(id).one(db).await {
        Ok(Some(f)) => {
            let mut active: feed::ActiveModel = f.into();
            active.name = Set(updated.name);
            active.description = Set(updated.description);
            active.rtsp_url = Set(updated.rtsp_url);
            active.rtsp_url_high = Set(updated.rtsp_url_high);
            active.parameters = Set(updated.parameters);
            active.ai_recording_duration_secs = Set(updated.ai_recording_duration_secs);

            match active.update(db).await {
                Ok(f) => res.render(Json(f)),
                Err(e) => {
                    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
                    res.render(format!("Error updating feed: {}", e));
                }
            }
        }
        Ok(None) => {
            res.status_code(StatusCode::NOT_FOUND);
        }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching feed: {}", e));
        }
    }
}

/// Delete a camera feed record.
///
/// The feed should be disconnected before deletion to avoid orphaned GStreamer pipelines.
///
/// # Path parameter
/// * `id` — The `feeds.id` value.
///
/// # Response
/// `200 OK` on success.
#[handler]
pub async fn delete_feed(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let id = req.param::<i32>("id").unwrap_or_default();

    match feed::Entity::delete_by_id(id).exec(db).await {
        Ok(_) => res.render("Deleted"),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error deleting feed: {}", e));
        }
    }
}
