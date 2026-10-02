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
    /// Minimum spacing between timeline thumbnail captures, in seconds.
    pub thumbnail_interval_secs: u64,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            recording_dir: PathBuf::from("/var/lib/reeframe/recordings"),
            chunk_duration_secs: 300,
            thumbnail_interval_secs: 30,
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
    /// IANA timezone (e.g. "Europe/Berlin") used to compute daily recording
    /// coverage day boundaries for any camera with no timezone override of
    /// its own.
    pub timezone: String,
}

impl Default for RecordingsConfig {
    fn default() -> Self {
        Self {
            retention_days: 30,
            retention_disk_threshold_percent: 90.0,
            timezone: "UTC".into(),
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
    /// `"local"`: the BE issues and validates its own JWTs with `jwt_secret`.
    /// `"oidc"`: also trusts JWTs issued by the Coordinator at `jwks_url`,
    /// verified locally against its cached public keys.
    pub mode: String,
    /// HMAC-SHA256 signing secret for locally issued JWTs. Required when
    /// `mode = "local"`. Set via config file or VMS_AUTH__JWT_SECRET env var.
    /// Generate with: openssl rand -base64 32
    pub jwt_secret: String,
    /// Access token lifetime, in seconds.
    pub access_token_ttl_secs: i64,
    /// Refresh token lifetime, in seconds.
    pub refresh_token_ttl_secs: i64,
    /// URL of Coordinator's `GET /.well-known/jwks.json` endpoint. Required
    /// when `mode = "oidc"`; ignored otherwise. Set via config file or
    /// VMS_AUTH__JWKS_URL env var.
    pub jwks_url: Option<String>,
    /// How long a fetched JWKS key set is trusted before being refetched.
    pub jwks_refresh_interval_secs: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: "local".into(),
            jwt_secret: String::new(),
            access_token_ttl_secs: 900,        // 15 minutes
            refresh_token_ttl_secs: 2_592_000, // 30 days
            jwks_url: None,
            jwks_refresh_interval_secs: 300, // 5 minutes
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// `host:port` of a gateway server for mobile app pairing. Unset by
    /// default, in which case the BE runs standalone and never connects to a
    /// gateway. Set via config file or VMS_GATEWAY__URL env var.
    pub url: Option<String>,
    /// The ID this BE is registered under in the Coordinator's `sites` table.
    /// Sent to the gateway on every connection so it knows which site the
    /// connection belongs to (required when `url` is set). Also used as the
    /// expected `aud` of Coordinator-issued tokens, so it is required when
    /// `[auth] mode = "oidc"` even without a gateway `url`.
    /// Set via config file or VMS_GATEWAY__BE_ID env var.
    pub be_id: Option<uuid::Uuid>,
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
    pub gateway: GatewayConfig,
    /// Base64-encoded 32-byte AES-256-GCM encryption key.
    /// Set via config file or VMS_ENCRYPTION_KEY env var.
    /// Generate with: openssl rand -base64 32
    pub encryption_key: String,
    /// Log level setting (trace | debug | info | warn | error). The daemon's
    /// active log filter comes from the RUST_LOG env var, because tracing is
    /// set up before this config is loaded.
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
            gateway: GatewayConfig::default(),
            encryption_key: String::new(),
            log_level: "info".into(),
        }
    }
}

// -- Loader --

/// Relative path of the local config-file override. `load()` merges it, and
/// `POST /system/config-file` backs it up and replaces it on disk.
pub const CONFIG_FILE_PATH: &str = "reeframe.toml";

/// Load `AppConfig` from a layered set of sources (lowest -> highest priority):
///
/// 1. Built-in defaults (`AppConfig::default()`)
/// 2. `/etc/reeframe/config.toml`: system-wide config (skipped if absent)
/// 3. `./reeframe.toml`: local override for development (skipped if absent)
/// 4. `VMS_*` environment variables
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
// `figment::Error` is large, but it is returned once, at startup.
#[allow(clippy::result_large_err)]
pub fn load() -> Result<AppConfig, figment::Error> {
    Figment::new()
        .merge(Serialized::defaults(AppConfig::default()))
        .merge(Toml::file("/etc/reeframe/config.toml"))
        .merge(Toml::file(CONFIG_FILE_PATH))
        .merge(Env::prefixed("VMS_").split("__"))
        .extract()
}

// -- Dynamic settings mapping --
//
// Hand-written mapping between `vms_core::KNOWN_SETTINGS` keys and
// `AppConfig` fields. The set of dynamic settings is small and fixed, so
// reflection isn't worth it. It lives here because `AppConfig` is defined in
// this crate; `vms-api`'s settings routes only use the `settings` table.

/// Read the current value of a known setting from `cfg` as JSON. Used to
/// seed the `settings` table for keys with no override yet, so
/// `GET /system/settings` can answer without re-reading the config file.
///
/// Returns `None` for an unknown key.
pub fn get_setting_value(cfg: &AppConfig, key: &str) -> Option<serde_json::Value> {
    use serde_json::json;
    Some(match key {
        "recordings.retention_days" => json!(cfg.recordings.retention_days),
        "recordings.retention_disk_threshold_percent" => {
            json!(cfg.recordings.retention_disk_threshold_percent)
        }
        "media.chunk_duration_secs" => json!(cfg.media.chunk_duration_secs),
        "media.thumbnail_interval_secs" => json!(cfg.media.thumbnail_interval_secs),
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

/// Patch `cfg` in place from a DB-stored override value. This is the last
/// step of the `defaults < file < env < DB` precedence chain.
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
        "media.thumbnail_interval_secs" => cfg.media.thumbnail_interval_secs = as_u64(value, key)?,
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

/// Resolve dynamic settings against the `settings` table at startup. A
/// stored override wins over the file/env value and has its
/// `pending_restart` flag cleared; a key with no stored row is seeded from
/// `cfg`. Must run after migrations and before anything is built from `cfg`.
pub async fn resolve_dynamic_settings(
    cfg: &mut AppConfig,
    settings_repo: &vms_db::SettingsRepo,
) -> Result<(), vms_core::VmsError> {
    for meta in vms_core::KNOWN_SETTINGS {
        match settings_repo.get(meta.key).await? {
            Some(row) => {
                let value: serde_json::Value = serde_json::from_str(&row.value)?;
                if let Err(e) = apply_setting_value(cfg, meta.key, &value) {
                    tracing::warn!(key = meta.key, error = %e, "Stored setting value is invalid, keeping the config-file/env value instead");
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

/// Validate an uploaded `POST /system/config-file` body by parsing it as an
/// `AppConfig`, so a partial or malformed file is rejected before anything
/// on disk or in the DB changes. Returns the value of every known dynamic
/// setting, which the caller syncs into the `settings` table the same way
/// `PATCH /system/settings` does. Every key is present in the result, since
/// a file missing a section fails to deserialize.
///
/// Passed to `vms-api` as a function value (`AppState::config_parser`)
/// because `vms-daemon` depends on `vms-api`, so `vms-api` can't name
/// `AppConfig` without a dependency cycle.
pub fn parse_uploaded_config(
    bytes: &[u8],
) -> Result<Vec<(&'static str, serde_json::Value)>, String> {
    let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
    let cfg: AppConfig = Figment::new()
        .merge(Toml::string(text))
        .extract()
        .map_err(|e| e.to_string())?;
    Ok(vms_core::KNOWN_SETTINGS
        .iter()
        .filter_map(|meta| get_setting_value(&cfg, meta.key).map(|v| (meta.key, v)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh install defaults to "local" auth with no `jwks_url`, so it can
    /// boot and authenticate without any Coordinator setup.
    #[test]
    fn default_auth_config_requires_no_coordinator_setup() {
        let auth = AuthConfig::default();

        assert_eq!(auth.mode, "local");
        assert!(auth.jwks_url.is_none());
    }
}
