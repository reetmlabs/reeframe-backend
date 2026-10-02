//! Timeline thumbnails: listing (index) and serving (JPEG bytes). There is no
//! DB row per thumbnail; the on-disk `{unix_ms}.jpg` filename written by
//! `vms_media::thumbnail_branch` serves as the index.

use std::path::Path;

use chrono::{DateTime, Utc};
use salvo::fs::NamedFile;
use salvo::prelude::*;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    error::{parse_id, ApiError},
    state::AppState,
};

// -- Response DTO --

#[derive(Serialize)]
pub struct ThumbnailDto {
    pub timestamp: DateTime<Utc>,
    pub url: String,
}

// -- Handlers --

/// GET /cameras/{id}/recordings/{recording_id}/thumbnails
///
/// Thumbnails within that chunk's `[start_time, end_time)`. For a chunk still
/// being written, the end is now.
#[handler]
pub async fn list_thumbnails(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<ThumbnailDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (camera_id, recording_id) = parse_camera_and_recording_id(req)?;

    let rec = state.recording_repo.get(recording_id).await?;
    if rec.camera_id != camera_id {
        return Err(ApiError::not_found(format!(
            "recording {recording_id} not found"
        )));
    }

    let from_ms = rec.start_time.timestamp_millis();
    let to_ms = rec
        .end_time
        .map(|t| t.timestamp_millis())
        .unwrap_or_else(|| Utc::now().timestamp_millis());

    let dir = thumbnail_dir(&state.media_recording_dir, camera_id);
    let timestamps = thumbnails_in_range(&dir, from_ms, to_ms).await?;

    let thumbnails = timestamps
        .into_iter()
        .filter_map(|ms| {
            Some(ThumbnailDto {
                timestamp: DateTime::from_timestamp_millis(ms)?,
                url: format!("/cameras/{camera_id}/thumbnails/{ms}.jpg"),
            })
        })
        .collect();

    Ok(Json(thumbnails))
}

/// GET /cameras/{id}/thumbnails/{filename}
///
/// Serves one thumbnail JPEG off disk. `filename` is validated as
/// `{digits}.jpg` before touching the filesystem.
#[handler]
pub async fn get_thumbnail(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let camera_id = parse_id(req)?;
    let filename = parse_thumbnail_filename(req)?;

    let path = thumbnail_dir(&state.media_recording_dir, camera_id).join(&filename);
    let file = NamedFile::open(&path)
        .await
        .map_err(|_| ApiError::not_found("thumbnail not found"))?;
    file.send(req.headers(), res).await;
    Ok(())
}

// -- Helpers --

fn thumbnail_dir(recording_dir: &Path, camera_id: Uuid) -> std::path::PathBuf {
    recording_dir
        .join("thumbnails")
        .join(format!("cam_{}", camera_id.as_simple()))
}

fn parse_camera_and_recording_id(req: &mut Request) -> Result<(Uuid, Uuid), ApiError> {
    let camera_id = parse_id(req)?;
    let recording_id: Uuid = req
        .param::<String>("recording_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid recording_id: expected UUID"))?;
    Ok((camera_id, recording_id))
}

fn parse_thumbnail_filename(req: &mut Request) -> Result<String, ApiError> {
    let raw = req.param::<String>("filename").unwrap_or_default();
    validate_thumbnail_filename(&raw)?;
    Ok(raw)
}

/// Accepts only `{digits}.jpg`, so path-traversal input (`..`, `/`, etc.) never
/// reaches the filesystem.
fn validate_thumbnail_filename(raw: &str) -> Result<(), ApiError> {
    let stem = raw
        .strip_suffix(".jpg")
        .ok_or_else(|| ApiError::bad_request("invalid thumbnail filename"))?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ApiError::bad_request("invalid thumbnail filename"));
    }
    Ok(())
}

async fn thumbnails_in_range(dir: &Path, from_ms: i64, to_ms: i64) -> Result<Vec<i64>, ApiError> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(ApiError::internal(format!(
                "reading thumbnails directory: {e}"
            )))
        }
    };

    let mut timestamps = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| ApiError::internal(format!("reading thumbnails directory: {e}")))?
    {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".jpg") else {
            continue;
        };
        let Ok(ts) = stem.parse::<i64>() else {
            continue;
        };
        if ts >= from_ms && ts < to_ms {
            timestamps.push(ts);
        }
    }
    timestamps.sort_unstable();
    Ok(timestamps)
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_filename_is_accepted() {
        assert!(validate_thumbnail_filename("1751385730000.jpg").is_ok());
    }

    #[test]
    fn path_traversal_attempts_are_rejected() {
        assert!(validate_thumbnail_filename("../../../etc/passwd").is_err());
        assert!(validate_thumbnail_filename("..%2F..%2Fetc%2Fpasswd.jpg").is_err());
    }

    #[test]
    fn non_digit_or_missing_extension_is_rejected() {
        assert!(validate_thumbnail_filename("").is_err());
        assert!(validate_thumbnail_filename("abc.jpg").is_err());
        assert!(validate_thumbnail_filename("123.png").is_err());
        assert!(validate_thumbnail_filename("123").is_err());
        assert!(validate_thumbnail_filename(".jpg").is_err());
    }

    async fn scratch_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("vms-thumbnails-test-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        dir
    }

    #[tokio::test]
    async fn missing_directory_yields_an_empty_list_not_an_error() {
        let dir = std::env::temp_dir().join(format!("vms-nonexistent-{}", Uuid::new_v4()));
        let result = thumbnails_in_range(&dir, 0, i64::MAX).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn lists_only_timestamps_within_range_sorted_ascending() {
        let dir = scratch_dir().await;
        for name in [
            "100.jpg",
            "300.jpg",
            "50.jpg",
            "999.jpg",
            "not-a-timestamp.jpg",
            "200.png",
        ] {
            tokio::fs::write(dir.join(name), b"").await.unwrap();
        }

        let result = thumbnails_in_range(&dir, 100, 999).await.unwrap();

        assert_eq!(result, vec![100, 300]);
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }
}
