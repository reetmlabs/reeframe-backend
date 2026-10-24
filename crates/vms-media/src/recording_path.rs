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
