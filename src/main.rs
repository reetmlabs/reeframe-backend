mod entities;
mod handlers;
mod recorder;

use salvo::prelude::*;
use std::env;
use crate::recorder::RecorderManager;
use handlers::{feed_handlers, recording_handlers, settings_handlers};
use sea_orm::{ActiveModelTrait, ConnectOptions, Database, Schema, ConnectionTrait, EntityTrait, Set};
use tracing_subscriber;



#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Get database URL from environment or use a default SQLite one
    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./feeds.db?mode=rwc".to_string());

    let mut opt = ConnectOptions::new(db_url);
    opt.max_connections(10)
       .min_connections(2);

    let db = Database::connect(opt).await.expect("Failed to connect to database");

    // Sync schema (for demo purposes, creating the table if it doesn't exist)
    let builder = db.get_database_backend();
    let schema = Schema::new(builder);
    let create_feed_table = builder.build(schema.create_table_from_entity(entities::feed::Entity).if_not_exists());
    let create_settings_table = builder.build(schema.create_table_from_entity(entities::settings::Entity).if_not_exists());

    db.execute_unprepared(create_feed_table.to_string().as_str()).await.expect("Failed to create feed table");
    db.execute_unprepared(create_settings_table.to_string().as_str()).await.expect("Failed to create settings table");

    // Initialize default settings if not exists
    if entities::settings::Entity::find().one(&db).await.expect("Failed to query settings").is_none() {
        let default_settings = entities::settings::ActiveModel {
            recording_chunk_duration_mins: Set(env::var("oneward_be_recording_chunk_duration_mins")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(10)),
            timezone: Set(env::var("oneward_be_timezone").unwrap_or_else(|_| "UTC".to_string())),
            ntp_server: Set(env::var("oneward_be_ntp_server").unwrap_or_else(|_| "pool.ntp.org".to_string())),
            storage_path: Set(env::var("oneward_be_storage_path").unwrap_or_else(|_| "./recordings".to_string())),
            reconnect_interval_secs: Set(env::var("oneward_be_reconnect_interval_secs")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(30)),
            gst_latency_ms: Set(env::var("oneward_be_gst_latency_ms")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(100)),
            gst_buffering_ms: Set(env::var("oneward_be_gst_buffering_ms")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(1000)),
            reserved_disk_space_mb: Set(env::var("oneward_be_reserved_disk_space_mb")
                .ok().and_then(|v| v.parse().ok()).unwrap_or(1024)),
            ..Default::default()
        };
        default_settings.insert(&db).await.expect("Failed to insert default settings");
    }

    // Initialize Recorder Manager
    let recorder_manager = RecorderManager::new();

    // Setup Salvo router
    let router = Router::new()
        .hoop(affix_state::inject(db))
        .hoop(affix_state::inject(recorder_manager))
        .push(
            Router::with_path("settings")
                .get(settings_handlers::get_settings)
                .put(settings_handlers::update_settings)
        )
        .push(
            Router::with_path("feeds")
                .post(feed_handlers::create_feed)
                .push(
                    Router::with_path("<id>")
                        .get(feed_handlers::get_feed)
                        .put(feed_handlers::update_feed)
                        .delete(feed_handlers::delete_feed)
                        .push(Router::with_path("record/start").post(recording_handlers::start_recording))
                        .push(Router::with_path("record/stop").post(recording_handlers::stop_recording))
                )
        );

    let acceptor = TcpListener::new("127.0.0.1:5800").bind().await;
    Server::new(acceptor).serve(router).await;
}
