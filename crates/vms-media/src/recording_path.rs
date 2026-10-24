//! Checks that a stored recording path points inside the recording dir.

use std::path::{Path, PathBuf};

/// Resolve `file` and confirm it lies under `recording_dir`.
///
/// Returns the canonical path when it does, `Ok(None)` when it resolves to
/// somewhere else, and an error when `file` or `recording_dir` can't be
/// resolved, usually because it doesn't exist. Both sides are canonicalized,
/// so relative paths, `..` and symlinks pointing out are all judged by where
/// they really lead.
pub fn resolve_recording_file(
    recording_dir: &Path,
    file: &Path,
) -> std::io::Result<Option<PathBuf>> {
    let dir = std::fs::canonicalize(recording_dir)?;
    let file = std::fs::canonicalize(file)?;
    Ok(file.starts_with(&dir).then_some(file))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch dir removed on drop. Relative to the working directory, so
    /// the relative-path case is exercised for real.
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let dir = PathBuf::from(format!("./vms-media-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[test]
    fn accepts_a_relative_path_inside() {
        let root = ScratchDir::new();
        let rec = root.0.join("recordings");
        std::fs::create_dir_all(rec.join("cam_a")).unwrap();
        let chunk = rec.join("cam_a").join("chunk.mp4");
        std::fs::write(&chunk, b"").unwrap();

        let resolved = resolve_recording_file(&rec, &chunk).unwrap();

        assert_eq!(resolved, Some(std::fs::canonicalize(&chunk).unwrap()));
    }

    #[test]
    fn rejects_a_path_outside() {
        let root = ScratchDir::new();
        let rec = root.0.join("recordings");
        std::fs::create_dir_all(&rec).unwrap();
        let outside = root.0.join("secret.txt");
        std::fs::write(&outside, b"").unwrap();

        assert_eq!(resolve_recording_file(&rec, &outside).unwrap(), None);
    }

    #[test]
    fn rejects_a_dot_dot_escape() {
        let root = ScratchDir::new();
        let rec = root.0.join("recordings");
        std::fs::create_dir_all(&rec).unwrap();
        std::fs::write(root.0.join("secret.txt"), b"").unwrap();

        let escape = rec.join("..").join("secret.txt");

        assert_eq!(resolve_recording_file(&rec, &escape).unwrap(), None);
    }

    #[test]
    fn rejects_a_sibling_dir_sharing_the_prefix() {
        let root = ScratchDir::new();
        let rec = root.0.join("recordings");
        let sibling = root.0.join("recordings2");
        std::fs::create_dir_all(&rec).unwrap();
        std::fs::create_dir_all(&sibling).unwrap();
        let file = sibling.join("chunk.mp4");
        std::fs::write(&file, b"").unwrap();

        assert_eq!(resolve_recording_file(&rec, &file).unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlink_pointing_out() {
        let root = ScratchDir::new();
        let rec = root.0.join("recordings");
        std::fs::create_dir_all(&rec).unwrap();
        let outside = root.0.join("secret.txt");
        std::fs::write(&outside, b"").unwrap();
        let link = rec.join("chunk.mp4");
        std::os::unix::fs::symlink(std::fs::canonicalize(&outside).unwrap(), &link).unwrap();

        assert_eq!(resolve_recording_file(&rec, &link).unwrap(), None);
    }

    #[test]
    fn errors_for_a_missing_file() {
        let root = ScratchDir::new();

        assert!(resolve_recording_file(&root.0, &root.0.join("gone.mp4")).is_err());
    }
}
