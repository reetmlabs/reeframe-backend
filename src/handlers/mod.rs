//! HTTP handler modules and router construction.
//!
//! ## API overview
//!
//! | Method | Path                             | Handler module       | Description                          |
//! |--------|----------------------------------|----------------------|--------------------------------------|
//! | GET    | `/settings`                      | `settings_handlers`  | Read global settings                 |
//! | PUT    | `/settings`                      | `settings_handlers`  | Update global settings               |
//! | POST   | `/feeds`                         | `feed_handlers`      | Create a camera feed                 |
//! | GET    | `/feeds/{id}`                    | `feed_handlers`      | Get a feed                           |
//! | PUT    | `/feeds/{id}`                    | `feed_handlers`      | Update a feed                        |
//! | DELETE | `/feeds/{id}`                    | `feed_handlers`      | Delete a feed                        |
//! | POST   | `/feeds/{id}/connect`            | `stream_handlers`    | Connect to camera, start pipelines   |
//! | POST   | `/feeds/{id}/disconnect`         | `stream_handlers`    | Disconnect camera, stop pipelines    |
//! | POST   | `/feeds/{id}/stream/quality`     | `stream_handlers`    | Switch live stream quality           |
//! | POST   | `/feeds/{id}/record/start`       | `recording_handlers` | Start user-commanded recording       |
//! | POST   | `/feeds/{id}/record/stop`        | `recording_handlers` | Stop user-commanded recording        |
//! | POST   | `/feeds/{id}/ai-event`           | `recording_handlers` | Trigger AI-event recording           |
//! | POST   | `/feeds/{id}/hardware-event`     | `recording_handlers` | Trigger hardware-event recording     |
//! | POST   | `/feeds/{id}/schedule-event`     | `recording_handlers` | Trigger schedule-event recording     |
//! | GET    | `/feeds/{id}/segments`           | `segment_handlers`   | List recording segments (time range) |
//! | GET    | `/segments/{id}`                 | `segment_handlers`   | Get a single recording segment       |

pub mod feed_handlers;
pub mod recording_handlers;
pub mod settings_handlers;
pub mod stream_handlers;
pub mod segment_handlers;

use salvo::prelude::*;
use sea_orm::DatabaseConnection;

use crate::media::CameraManager;

/// Construct the full Salvo HTTP router with all application routes.
///
/// Injects the database connection and camera manager into the router state so every
/// handler can access them via `Depot`.
///
/// # Arguments
/// * `db`             — SeaORM database connection.
/// * `camera_manager` — Shared camera / RTSP manager.
///
/// # Returns
/// A fully configured [`Router`] ready to be passed to the Salvo [`Server`].
pub fn build_router(db: DatabaseConnection, camera_manager: CameraManager) -> Router {
    Router::new()
        .hoop(affix_state::inject(db))
        .hoop(affix_state::inject(camera_manager))
        // Settings
        .push(
            Router::with_path("settings")
                .get(settings_handlers::get_settings)
                .put(settings_handlers::update_settings),
        )
        // Individual recording segment lookup
        .push(
            Router::with_path("segments/<id>")
                .get(segment_handlers::get_segment),
        )
        // Feed CRUD + nested feed actions
        .push(
            Router::with_path("feeds")
                .post(feed_handlers::create_feed)
                .push(
                    Router::with_path("<id>")
                        .get(feed_handlers::get_feed)
                        .put(feed_handlers::update_feed)
                        .delete(feed_handlers::delete_feed)
                        // Stream lifecycle & quality
                        .push(Router::with_path("connect").post(stream_handlers::connect_feed))
                        .push(Router::with_path("disconnect").post(stream_handlers::disconnect_feed))
                        .push(Router::with_path("stream/quality").post(stream_handlers::switch_quality))
                        // User recording
                        .push(Router::with_path("record/start").post(recording_handlers::start_recording))
                        .push(Router::with_path("record/stop").post(recording_handlers::stop_recording))
                        // Event-triggered recordings
                        .push(Router::with_path("ai-event").post(recording_handlers::handle_ai_event))
                        .push(Router::with_path("hardware-event").post(recording_handlers::handle_hardware_event))
                        .push(Router::with_path("schedule-event").post(recording_handlers::handle_schedule_event))
                        // Recording segment index
                        .push(Router::with_path("segments").get(segment_handlers::list_segments)),
                ),
        )
}
