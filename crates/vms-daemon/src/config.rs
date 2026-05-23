use std::path::PathBuf;

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

// -- Sub-sections --------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// SeaORM connection URL.
    /// Examples:
    ///   sqlite://./onward.db
    ///   mysql://user:pass@localhost/onward
    ///   postgres://user:pass@localhost/onward
    pub url: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "sqlite://./onward.db".into(),
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
            recording_dir: PathBuf::from("/var/lib/onward/recordings"),
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

// -- Root config ---------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub media: MediaConfig,
    pub api: ApiConfig,
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
            encryption_key: String::new(),
            log_level: "info".into(),
        }
    }
}

// -- Loader --------------------------------------------------------------------

/// Load `AppConfig` from a layered set of sources (lowest -> highest priority):
///
/// 1. Built-in defaults (`AppConfig::default()`)
/// 2. `/etc/onward/config.toml`  — system-wide config (silently skipped if absent)
/// 3. `./onward.toml`            — local override for development (silently skipped if absent)
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
/// | `VMS_LOG_LEVEL`            | `log_level`                 |
pub fn load() -> Result<AppConfig, figment::Error> {
    Figment::new()
        .merge(Serialized::defaults(AppConfig::default()))
        .merge(Toml::file("/etc/onward/config.toml"))
        .merge(Toml::file("onward.toml"))
        .merge(Env::prefixed("VMS_").split("__"))
        .extract()
}
