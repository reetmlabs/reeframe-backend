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

// -- Root config --

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub media: MediaConfig,
    pub api: ApiConfig,
    pub rtsp: RtspConfig,
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
            api: ApiConfig::default(),
            rtsp: RtspConfig::default(),
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
/// | `VMS_API__BIND`            | `api.bind`                  |
/// | `VMS_RTSP__BIND`           | `rtsp.bind`                 |
/// | `VMS_LOG_LEVEL`            | `log_level`                 |
pub fn load() -> Result<AppConfig, figment::Error> {
    Figment::new()
        .merge(Serialized::defaults(AppConfig::default()))
        .merge(Toml::file("/etc/reeframe/config.toml"))
        .merge(Toml::file("reeframe.toml"))
        .merge(Env::prefixed("VMS_").split("__"))
        .extract()
}
