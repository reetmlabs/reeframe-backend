//! The registry of dynamically-configurable settings.
//!
//! Every setting here can be overridden at runtime via `PATCH
//! /system/settings` or a config-file upload, in addition to the startup
//! config file and env vars. `database.url` and `encryption_key` are absent
//! because both are needed before the DB connection that would store them
//! exists.

/// Metadata for one dynamically-configurable setting.
pub struct SettingMeta {
    /// Dotted config-key path, matching the `AppConfig` field it maps to
    /// (e.g. `"recordings.retention_days"`).
    pub key: &'static str,
    /// `true` if a change takes effect without a restart. Only the two
    /// retention settings qualify; they are pushed into the running
    /// `StatMonitor` via `set_retention`. Other settings are read at
    /// construction time, so changes apply on the next startup.
    pub hot: bool,
    /// `true` if the value is write-only and never returned by `GET
    /// /system/settings`. Currently only `auth.jwt_secret`.
    pub sensitive: bool,
}

pub const KNOWN_SETTINGS: &[SettingMeta] = &[
    SettingMeta {
        key: "recordings.retention_days",
        hot: true,
        sensitive: false,
    },
    SettingMeta {
        key: "recordings.retention_disk_threshold_percent",
        hot: true,
        sensitive: false,
    },
    SettingMeta {
        key: "recordings.timezone",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "media.chunk_duration_secs",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "media.thumbnail_interval_secs",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "media.recording_dir",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "auth.mode",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "auth.jwt_secret",
        hot: false,
        sensitive: true,
    },
    SettingMeta {
        key: "auth.access_token_ttl_secs",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "auth.refresh_token_ttl_secs",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "rtsp.bind",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "api.bind",
        hot: false,
        sensitive: false,
    },
    SettingMeta {
        key: "log_level",
        hot: false,
        sensitive: false,
    },
];

/// Look up a known setting's metadata by key. Returns `None` both for unknown
/// keys and for the bootstrap-only `database.url` and `encryption_key`, so
/// callers reject them the same way.
pub fn setting_meta(key: &str) -> Option<&'static SettingMeta> {
    KNOWN_SETTINGS.iter().find(|s| s.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_key_resolves() {
        let meta = setting_meta("recordings.retention_days").expect("known key");
        assert!(meta.hot);
        assert!(!meta.sensitive);
    }

    #[test]
    fn sensitive_key_is_marked() {
        let meta = setting_meta("auth.jwt_secret").expect("known key");
        assert!(meta.sensitive);
        assert!(!meta.hot);
    }

    #[test]
    fn bootstrap_secrets_are_excluded() {
        assert!(setting_meta("database.url").is_none());
        assert!(setting_meta("encryption_key").is_none());
    }

    #[test]
    fn unknown_key_returns_none() {
        assert!(setting_meta("nonexistent.key").is_none());
    }

    #[test]
    fn every_key_is_unique() {
        let mut keys: Vec<&str> = KNOWN_SETTINGS.iter().map(|s| s.key).collect();
        let before = keys.len();
        keys.sort();
        keys.dedup();
        assert_eq!(before, keys.len(), "duplicate key in KNOWN_SETTINGS");
    }
}
