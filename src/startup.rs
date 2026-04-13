//! Application startup helpers.
//!
//! Contains one-time initialisation logic that does not belong to any specific module:
//! seeding the singleton `settings` row when the database is first used.

use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
use std::env;

use crate::entities::settings;

/// Insert a default settings row if none exists yet.
///
/// Settings are a singleton — exactly one row lives in the `settings` table for the
/// lifetime of the application.  This function checks for that row and inserts it with
/// sensible defaults (overridable via environment variables) on first startup.
///
/// # Environment variables
/// | Variable                                    | Default          |
/// |---------------------------------------------|------------------|
/// | `oneward_be_recording_chunk_duration_mins`  | `10`             |
/// | `oneward_be_timezone`                       | `"UTC"`          |
/// | `oneward_be_ntp_server`                     | `"pool.ntp.org"` |
/// | `oneward_be_storage_path`                   | `"./recordings"` |
/// | `oneward_be_reconnect_interval_secs`        | `30`             |
/// | `oneward_be_gst_latency_ms`                 | `100`            |
/// | `oneward_be_gst_buffering_ms`               | `1000`           |
/// | `oneward_be_reserved_disk_space_mb`         | `1024`           |
///
/// # Arguments
/// * `db` — Active SeaORM database connection.
///
/// # Panics
/// Panics if the database query or insert fails.
pub async fn ensure_default_settings(db: &DatabaseConnection) {
    if settings::Entity::find()
        .one(db)
        .await
        .expect("Failed to query settings")
        .is_some()
    {
        return;
    }

    let defaults = settings::ActiveModel {
        recording_chunk_duration_mins: Set(env_int("oneward_be_recording_chunk_duration_mins", 10)),
        timezone: Set(env::var("oneward_be_timezone").unwrap_or_else(|_| "UTC".to_string())),
        ntp_server: Set(env::var("oneward_be_ntp_server").unwrap_or_else(|_| "pool.ntp.org".to_string())),
        storage_path: Set(env::var("oneward_be_storage_path").unwrap_or_else(|_| "./recordings".to_string())),
        pre_event_cache_duration_secs: Set(10),
        default_ai_recording_duration_secs: Set(10),
        reconnect_interval_secs: Set(env_int("oneward_be_reconnect_interval_secs", 30)),
        gst_latency_ms: Set(env_int("oneward_be_gst_latency_ms", 100)),
        gst_buffering_ms: Set(env_int("oneward_be_gst_buffering_ms", 1000)),
        reserved_disk_space_mb: Set(env_i64("oneward_be_reserved_disk_space_mb", 1024)),
        ..Default::default()
    };

    defaults
        .insert(db)
        .await
        .expect("Failed to insert default settings");
}

/// Read an integer environment variable, returning `default` if absent or unparseable.
fn env_int(key: &str, default: i32) -> i32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Read an i64 environment variable, returning `default` if absent or unparseable.
fn env_i64(key: &str, default: i64) -> i64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
