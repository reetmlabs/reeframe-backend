//! The registry of dynamically-configurable settings.
//!
//! Every setting here can be overridden at runtime via `PATCH
//! /system/settings` or a config-file upload, instead of only through the
//! startup config file / env vars. `database.url` and `encryption_key` are
//! deliberately absent — both are needed before the DB connection they'd be
//! stored in even exists, a hard chicken-and-egg constraint, not a policy
//! choice.

/// Metadata for one dynamically-configurable setting.
pub struct SettingMeta {
    /// Dotted config-key path, matching the `AppConfig` field it maps to
    /// (e.g. `"recordings.retention_days"`).
    pub key: &'static str,
    /// `true` if changing this setting takes effect immediately (no
    /// restart needed) — currently only the two retention settings, which
    /// `StatMonitor::set_retention` already re-reads on every poll tick.
    /// Every other setting here is baked into some component at
    /// construction time, so a change is stored but only takes effect on
    /// the next startup.
    pub hot: bool,
    /// `true` if the value must never be returned by `GET
    /// /system/settings` — write-only. Only `auth.jwt_secret` today.
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

/// Look up a known setting's metadata by key. `None` means either an
/// unrecognized key, or one of the two bootstrap-only exclusions
/// (`database.url`, `encryption_key`) — callers use this to reject both
/// cases identically.
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
