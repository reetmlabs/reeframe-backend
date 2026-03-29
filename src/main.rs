mod entities;
mod handlers;
mod recorder;

use salvo::prelude::*;
use sea_orm::{ConnectOptions, Database, Schema, ConnectionTrait};
use std::env;
use crate::recorder::RecorderManager;
use handlers::{feed_handlers, recording_handlers};
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
    let create_table_stmt = builder.build(&schema.create_table_from_entity(entities::feed::Entity).if_not_exists());

    db.execute(create_table_stmt).await.expect("Failed to create table");

    // Initialize Recorder Manager
    let recorder_manager = RecorderManager::new();

    // Setup Salvo router
    let router = Router::new()
        .hoop(affix::inject("db", db))
        .hoop(affix::inject("recorder_manager", recorder_manager))
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
