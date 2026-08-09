use std::collections::HashMap;
use std::path::PathBuf;

use salvo::prelude::*;
use serde::Serialize;
use serde_json::Value;
use vms_core::{setting_meta, SettingMeta, KNOWN_SETTINGS};
use vms_db::entities::setting;
use vms_engine::RetentionConfig;

use crate::{
    error::{parse_body, ApiError},
    state::AppState,
};

// -- Response DTO --

#[derive(Serialize)]
pub struct SettingDto {
    pub key: &'static str,
    /// `None` if the setting is sensitive (write-only — `auth.jwt_secret`
    /// today) or hasn't been seeded into the DB yet.
    pub value: Option<Value>,
    pub hot: bool,
    pub pending_restart: bool,
}

fn to_dto(meta: &'static SettingMeta, row: Option<setting::Model>) -> SettingDto {
    let (value, pending_restart) = match row {
        Some(row) => {
            let value = if meta.sensitive {
                None
            } else {
                serde_json::from_str(&row.value).ok()
            };
            (value, row.pending_restart)
        }
        None => (None, false),
    };
    SettingDto {
        key: meta.key,
        value,
        hot: meta.hot,
        pending_restart,
    }
}

// -- Handlers --

/// GET /system/settings
#[handler]
pub async fn list_settings(depot: &mut Depot) -> Result<Json<Vec<SettingDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let mut out = Vec::with_capacity(KNOWN_SETTINGS.len());
    for meta in KNOWN_SETTINGS {
        let row = state.settings_repo.get(meta.key).await?;
        out.push(to_dto(meta, row));
    }
    Ok(Json(out))
}

/// PATCH /system/settings
///
/// Body is a `{key: value}` map. Unknown keys (including `database.url`/
/// `encryption_key`, which are never in `KNOWN_SETTINGS`) reject the whole
/// request before anything is written — no partial application. Hot keys
/// (currently the two `recordings.*` retention settings) also take effect
/// immediately; every other key is persisted with `pending_restart: true`.
#[handler]
pub async fn update_settings(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<SettingDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: HashMap<String, Value> = parse_body(req).await?;

    if body.is_empty() {
        return Err(ApiError::bad_request("no settings provided"));
    }

    for key in body.keys() {
        if setting_meta(key).is_none() {
            return Err(ApiError::bad_request(format!(
                "'{key}' is not a known dynamic setting"
            )));
        }
    }

    for (key, value) in &body {
        let meta = setting_meta(key).expect("validated above");
        state
            .settings_repo
            .upsert(key, value.clone(), !meta.hot)
            .await?;
    }

    if body.contains_key("recordings.retention_days")
        || body.contains_key("recordings.retention_disk_threshold_percent")
    {
        apply_retention_now(state).await?;
    }

    let mut out = Vec::with_capacity(body.len());
    for key in body.keys() {
        let meta = setting_meta(key).expect("validated above");
        let row = state.settings_repo.get(key).await?;
        out.push(to_dto(meta, row));
    }
    Ok(Json(out))
}

/// POST /system/config-file
///
/// Body is a raw TOML file, same shape as `reeframe.toml`. Rejected with
/// `400` if it doesn't parse onto `AppConfig` — nothing is touched in that
/// case. On success: the existing on-disk file is backed up
/// (`<path>.bak-<timestamp>`), the upload replaces it, and every known
/// setting's value from the upload is synced into the `settings` table
/// through the same upsert-and-hot-apply-or-flag path `PATCH
/// /system/settings` uses.
#[handler]
pub async fn upload_config_file(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<SettingDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");

    let bytes = req
        .payload()
        .await
        .map_err(|e| ApiError::bad_request(format!("failed to read request body: {e}")))?
        .to_vec();

    let entries = (state.config_parser)(&bytes)
        .map_err(|e| ApiError::bad_request(format!("invalid config file: {e}")))?;

    if state.config_file_path.exists() {
        let backup_path = PathBuf::from(format!(
            "{}.bak-{}",
            state.config_file_path.display(),
            chrono::Utc::now().format("%Y%m%d%H%M%S")
        ));
        std::fs::copy(&state.config_file_path, &backup_path).map_err(|e| {
            ApiError::internal(format!("failed to back up existing config file: {e}"))
        })?;
    }
    std::fs::write(&state.config_file_path, &bytes)
        .map_err(|e| ApiError::internal(format!("failed to write uploaded config file: {e}")))?;

    for (key, value) in &entries {
        let meta = setting_meta(key).expect("entries come from KNOWN_SETTINGS");
        state
            .settings_repo
            .upsert(key, value.clone(), !meta.hot)
            .await?;
    }

    if entries.iter().any(|(key, _)| {
        *key == "recordings.retention_days" || *key == "recordings.retention_disk_threshold_percent"
    }) {
        apply_retention_now(state).await?;
    }

    let mut out = Vec::with_capacity(KNOWN_SETTINGS.len());
    for meta in KNOWN_SETTINGS {
        let row = state.settings_repo.get(meta.key).await?;
        out.push(to_dto(meta, row));
    }
    Ok(Json(out))
}

// -- Hot-apply --

/// Rebuild `RetentionConfig` from whatever is currently stored for both
/// retention keys (not just the one that was just changed, since
/// `StatMonitor::set_retention` replaces the whole config) and push it into
/// the live poller — no restart needed.
async fn apply_retention_now(state: &AppState) -> Result<(), ApiError> {
    let retention_days = current_number(state, "recordings.retention_days")
        .await?
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| ApiError::bad_request("retention_days: expected a non-negative integer"))?;

    let retention_disk_threshold_percent =
        current_number(state, "recordings.retention_disk_threshold_percent")
            .await?
            .as_f64()
            .ok_or_else(|| {
                ApiError::bad_request("retention_disk_threshold_percent: expected a number")
            })?;

    state.stat_monitor.set_retention(RetentionConfig {
        recording_repo: state.recording_repo.clone(),
        camera_repo: state.camera_repo.clone(),
        recording_dir: state.media_recording_dir.clone(),
        retention_days,
        retention_disk_threshold_percent,
    });
    Ok(())
}

async fn current_number(state: &AppState, key: &str) -> Result<Value, ApiError> {
    let row = state
        .settings_repo
        .get(key)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("setting '{key}' has no stored value")))?;
    serde_json::from_str(&row.value).map_err(|e| ApiError::bad_request(e.to_string()))
}
