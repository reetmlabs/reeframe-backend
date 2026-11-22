//! Startup checks on the installed GStreamer plugins.

use vms_core::VmsError;

/// Fail if the installed gst-libav plugin is a release known to leak memory.
///
/// A missing plugin is left to the pipelines that need it, as before.
pub fn check_installed_libav() -> Result<(), VmsError> {
    match gstreamer::Registry::get().find_plugin("libav") {
        Some(plugin) => check_libav_version(&plugin.version()),
        None => Ok(()),
    }
}

/// Reject gst-libav 1.24.0 and 1.24.1, whose `avviddec` leaks one `AVPacket`
/// per decoded frame. An unparseable version is accepted rather than blocking startup.
pub fn check_libav_version(version: &str) -> Result<(), VmsError> {
    let mut parts = version.split('.').map(str::parse::<u32>);
    let (Some(Ok(major)), Some(Ok(minor)), Some(Ok(micro))) =
        (parts.next(), parts.next(), parts.next())
    else {
        return Ok(());
    };
    if (major, minor) == (1, 24) && micro < 2 {
        return Err(VmsError::Media(format!(
            "gst-libav {version} leaks memory on every decoded video frame; \
             install gst-libav 1.24.2 or newer (see README, Building from source)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_leaking_releases() {
        for v in ["1.24.0", "1.24.1", "1.24.1.1"] {
            let err = check_libav_version(v).unwrap_err().to_string();
            assert!(err.contains("1.24.2 or newer"), "{v}: {err}");
        }
    }

    #[test]
    fn accepts_fixed_and_older_releases() {
        for v in ["1.22.0", "1.23.90", "1.24.2", "1.24.12", "1.26.0", "2.0.0"] {
            assert!(check_libav_version(v).is_ok(), "{v}");
        }
    }

    #[test]
    fn accepts_unparseable_versions() {
        for v in ["", "1.24", "unknown", "1.x.1"] {
            assert!(check_libav_version(v).is_ok(), "{v:?}");
        }
    }
}
