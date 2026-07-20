use std::path::PathBuf;

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

// -- Sub-sections --

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// SeaORM connection URL.
    /// Examples:
    ///   sqlite://./reeframe.db?mode=rwc  (creates file if absent)
    ///   mysql://user:pass@localhost/reeframe
    ///   postgres://user:pass@localhost/reeframe
    pub url: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "sqlite://./reeframe.db?mode=rwc".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MediaConfig {
    /// Directory where MP4 chunk files are written.
    pub recording_dir: PathBuf,
    /// Duration of each recording chunk in seconds.
    pub chunk_duration_secs: u64,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            recording_dir: PathBuf::from("/var/lib/reeframe/recordings"),
            chunk_duration_secs: 300,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordingsConfig {
    /// Delete finalized recordings older than this many days. `0` disables
    /// age-based cleanup.
    pub retention_days: u32,
    /// Once disk usage on the recording directory's filesystem crosses this
    /// percentage, delete the oldest finalized recordings first until it
    /// drops back under it. `0` disables disk-threshold cleanup.
    pub retention_disk_threshold_percent: f64,
}

impl Default for RecordingsConfig {
    fn default() -> Self {
        Self {
            retention_days: 30,
            retention_disk_threshold_percent: 90.0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Address and port the HTTP server binds to.
    pub bind: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RtspConfig {
    /// Address and port the RTSP relay server binds to.
    /// Each camera is served at rtsp://{host}:{port}/{camera_id}.
    /// Set via config file or VMS_RTSP__BIND env var.
    pub bind: String,
}

impl Default for RtspConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8554".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    /// `"local"` — the BE issues and validates its own JWTs with `jwt_secret`.
    /// `"oidc"` — validate IDP-issued tokens via a JWKS endpoint; requires
    /// the `vms-ent-auth` enterprise crate and is not available in the
    /// community build.
    pub mode: String,
    /// HMAC-SHA256 signing secret for locally issued JWTs. Required when
    /// `mode = "local"`. Set via config file or VMS_AUTH__JWT_SECRET env var.
    /// Generate with: openssl rand -base64 32
    pub jwt_secret: String,
    /// Access token lifetime, in seconds.
    pub access_token_ttl_secs: i64,
    /// Refresh token lifetime, in seconds.
    pub refresh_token_ttl_secs: i64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: "local".into(),
            jwt_secret: String::new(),
            access_token_ttl_secs: 900,        // 15 minutes
            refresh_token_ttl_secs: 2_592_000, // 30 days
        }
    }
}

// -- Root config --

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub media: MediaConfig,
    pub recordings: RecordingsConfig,
    pub api: ApiConfig,
    pub rtsp: RtspConfig,
    pub auth: AuthConfig,
    /// Base64-encoded 32-byte AES-256-GCM encryption key.
    /// Set via config file or VMS_ENCRYPTION_KEY env var.
    /// Generate with: openssl rand -base64 32
    pub encryption_key: String,
    /// Minimum log level (trace | debug | info | warn | error).
    /// Overridden at runtime by the RUST_LOG env var.
    pub log_level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            media: MediaConfig::default(),
            recordings: RecordingsConfig::default(),
            api: ApiConfig::default(),
            rtsp: RtspConfig::default(),
            auth: AuthConfig::default(),
            encryption_key: String::new(),
            log_level: "info".into(),
        }
    }
}

// -- Loader --

/// Load `AppConfig` from a layered set of sources (lowest -> highest priority):
///
/// 1. Built-in defaults (`AppConfig::default()`)
/// 2. `/etc/reeframe/config.toml`  — system-wide config (silently skipped if absent)
/// 3. `./reeframe.toml`            — local override for development (silently skipped if absent)
/// 4. `VMS_*` environment variables — twelve-factor style overrides
///
/// Environment variable mapping uses a `VMS_` prefix and `__` as the nested
/// key separator:
///
/// | Env var                    | Config key                  |
/// |----------------------------|-----------------------------|
/// | `VMS_ENCRYPTION_KEY`       | `encryption_key`            |
/// | `VMS_DATABASE__URL`        | `database.url`              |
/// | `VMS_MEDIA__RECORDING_DIR` | `media.recording_dir`       |
/// | `VMS_RECORDINGS__RETENTION_DAYS` | `recordings.retention_days` |
/// | `VMS_RECORDINGS__RETENTION_DISK_THRESHOLD_PERCENT` | `recordings.retention_disk_threshold_percent` |
/// | `VMS_API__BIND`            | `api.bind`                  |
/// | `VMS_RTSP__BIND`           | `rtsp.bind`                 |
/// | `VMS_AUTH__MODE`           | `auth.mode`                 |
/// | `VMS_AUTH__JWT_SECRET`     | `auth.jwt_secret`           |
/// | `VMS_LOG_LEVEL`            | `log_level`                 |
pub fn load() -> Result<AppConfig, figment::Error> {
    Figment::new()
        .merge(Serialized::defaults(AppConfig::default()))
        .merge(Toml::file("/etc/reeframe/config.toml"))
        .merge(Toml::file("reeframe.toml"))
        .merge(Env::prefixed("VMS_").split("__"))
        .extract()
}

// -- Dynamic settings mapping --
//
// Explicit, hand-written mapping between `vms_core::KNOWN_SETTINGS` keys and
// `AppConfig` fields — deliberately not a generic/reflective mechanism,
// since the set of dynamic settings is small and fixed. Kept here (not in
// `vms-core`) because only this crate's `AppConfig` type is involved;
// `vms-api`'s settings routes only ever talk to the `settings` DB table,
// never to this function directly.

/// Read the current value of a known setting off `cfg`, as JSON — used to
/// seed the `settings` table the first time a key is seen (i.e. it's never
/// been explicitly overridden via the API), so `GET /system/settings` has
/// an authoritative answer without ever re-reading the config file again
/// after boot.
///
/// Returns `None` for a key `vms_core::setting_meta` doesn't recognize —
/// callers should already have checked that.
pub fn get_setting_value(cfg: &AppConfig, key: &str) -> Option<serde_json::Value> {
    use serde_json::json;
    Some(match key {
        "recordings.retention_days" => json!(cfg.recordings.retention_days),
        "recordings.retention_disk_threshold_percent" => {
            json!(cfg.recordings.retention_disk_threshold_percent)
        }
        "media.chunk_duration_secs" => json!(cfg.media.chunk_duration_secs),
        "media.recording_dir" => json!(cfg.media.recording_dir.to_string_lossy()),
        "auth.mode" => json!(cfg.auth.mode),
        "auth.jwt_secret" => json!(cfg.auth.jwt_secret),
        "auth.access_token_ttl_secs" => json!(cfg.auth.access_token_ttl_secs),
        "auth.refresh_token_ttl_secs" => json!(cfg.auth.refresh_token_ttl_secs),
        "rtsp.bind" => json!(cfg.rtsp.bind),
        "api.bind" => json!(cfg.api.bind),
        "log_level" => json!(cfg.log_level),
        _ => return None,
    })
}

/// Patch `cfg` in place from a DB-stored override value — the DB-wins step
/// of the `defaults < file < env < DB` precedence chain, applied once at
/// startup for every key that has an override row.
pub fn apply_setting_value(
    cfg: &mut AppConfig,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    fn as_str(v: &serde_json::Value, key: &str) -> Result<String, String> {
        v.as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("setting '{key}': expected a string"))
    }
    fn as_u64(v: &serde_json::Value, key: &str) -> Result<u64, String> {
        v.as_u64()
            .ok_or_else(|| format!("setting '{key}': expected a non-negative integer"))
    }
    fn as_u32(v: &serde_json::Value, key: &str) -> Result<u32, String> {
        as_u64(v, key)?
            .try_into()
            .map_err(|_| format!("setting '{key}': value out of range for u32"))
    }
    fn as_i64(v: &serde_json::Value, key: &str) -> Result<i64, String> {
        v.as_i64()
            .ok_or_else(|| format!("setting '{key}': expected an integer"))
    }
    fn as_f64(v: &serde_json::Value, key: &str) -> Result<f64, String> {
        v.as_f64()
            .ok_or_else(|| format!("setting '{key}': expected a number"))
    }

    match key {
        "recordings.retention_days" => cfg.recordings.retention_days = as_u32(value, key)?,
        "recordings.retention_disk_threshold_percent" => {
            cfg.recordings.retention_disk_threshold_percent = as_f64(value, key)?
        }
        "media.chunk_duration_secs" => cfg.media.chunk_duration_secs = as_u64(value, key)?,
        "media.recording_dir" => cfg.media.recording_dir = PathBuf::from(as_str(value, key)?),
        "auth.mode" => cfg.auth.mode = as_str(value, key)?,
        "auth.jwt_secret" => cfg.auth.jwt_secret = as_str(value, key)?,
        "auth.access_token_ttl_secs" => cfg.auth.access_token_ttl_secs = as_i64(value, key)?,
        "auth.refresh_token_ttl_secs" => cfg.auth.refresh_token_ttl_secs = as_i64(value, key)?,
        "rtsp.bind" => cfg.rtsp.bind = as_str(value, key)?,
        "api.bind" => cfg.api.bind = as_str(value, key)?,
        "log_level" => cfg.log_level = as_str(value, key)?,
        _ => return Err(format!("setting '{key}': not a known dynamic setting")),
    }
    Ok(())
}

/// Resolve dynamic settings against the `settings` table: for each known
/// key, a DB override (if present) wins over whatever `cfg` was just loaded
/// with from the config file/env, and its `pending_restart` flag is cleared
/// (it's applied now); a key with no override yet is seeded from `cfg`'s
/// current value so future `GET /system/settings` calls have something
/// authoritative to report. Called once at startup, after migrations run
/// and before anything downstream is constructed from `cfg`.
pub async fn resolve_dynamic_settings(
    cfg: &mut AppConfig,
    settings_repo: &vms_db::SettingsRepo,
) -> Result<(), vms_core::VmsError> {
    for meta in vms_core::KNOWN_SETTINGS {
        match settings_repo.get(meta.key).await? {
            Some(row) => {
                let value: serde_json::Value = serde_json::from_str(&row.value)?;
                if let Err(e) = apply_setting_value(cfg, meta.key, &value) {
                    tracing::warn!(key = meta.key, error = %e, "Stored setting value is invalid — keeping the config-file/env value instead");
                    continue;
                }
                if row.pending_restart {
                    settings_repo.clear_pending(meta.key).await?;
                    tracing::info!(key = meta.key, "Applied pending setting change on startup");
                }
            }
            None => {
                if let Some(current) = get_setting_value(cfg, meta.key) {
                    settings_repo.upsert(meta.key, current, false).await?;
                }
            }
        }
    }
    Ok(())
}
