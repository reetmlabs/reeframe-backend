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
