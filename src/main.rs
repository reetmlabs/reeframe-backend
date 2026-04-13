//! OneWard backend entry point.
//!
//! Wires together the four top-level subsystems and starts the server:
//!
//! 1. **Database** — connects to SQLite (or `DATABASE_URL`) and creates tables.
//! 2. **Startup** — seeds the singleton settings row on first run.
//! 3. **Media** — starts the GStreamer RTSP server and DB indexer task.
//! 4. **HTTP** — binds Salvo on `127.0.0.1:5800` and serves the API.
//!
//! See [`handlers`] for the full API route listing.

mod entities;
mod handlers;
mod media;
mod startup;

use salvo::prelude::*;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::env;
use tracing_subscriber;

use crate::media::CameraManager;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db = init_db().await;
    entities::create_tables(&db).await;
    startup::ensure_default_settings(&db).await;
    let camera_manager = init_camera_manager(&db);
    let router = handlers::build_router(db, camera_manager);

    Server::new(TcpListener::new("127.0.0.1:5800").bind().await)
        .serve(router)
        .await;
}

/// Connect to the database and return an active connection pool.
///
/// The database URL is read from the `DATABASE_URL` environment variable.
/// Defaults to a local SQLite file (`./feeds.db`) when the variable is absent.
///
/// # Panics
/// Panics if the connection cannot be established.
async fn init_db() -> DatabaseConnection {
    let db_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "sqlite:./feeds.db?mode=rwc".to_string());

    let mut opt = ConnectOptions::new(db_url);
    opt.max_connections(10).min_connections(2);

    Database::connect(opt)
        .await
        .expect("Failed to connect to database")
}

/// Create and start the [`CameraManager`].
///
/// Reads the RTSP server port from the `oneward_be_rtsp_port` environment variable
/// (default: `8554`) and starts the GStreamer RTSP server and DB indexer task.
///
/// # Arguments
/// * `db` — Database connection passed to the recording segment indexer task.
///
/// # Panics
/// Panics if the RTSP server fails to start (e.g. port already in use).
fn init_camera_manager(db: &DatabaseConnection) -> CameraManager {
    let rtsp_port: u16 = env::var("oneward_be_rtsp_port")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8554);

    CameraManager::new(rtsp_port, db.clone())
        .expect("Failed to initialise CameraManager")
}
