use std::path::Path;

use chrono::{DateTime, FixedOffset, Utc};
use salvo::fs::NamedFile;
use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::entities::export_job::{self, ExportJobStatus};
use vms_db::entities::recording;
use vms_media::{resolve_recording_file, ExportChunk};

use crate::{
    error::{parse_id, ApiError},
    state::AppState,
};

// -- Request / response types --

#[derive(Deserialize)]
pub struct CreateExportBody {
    pub camera_id: Uuid,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ExportJobDto {
    pub id: Uuid,
    pub camera_id: Uuid,
    pub status: ExportJobStatus,
    pub size_bytes: Option<i64>,
    pub error: Option<String>,
    pub created_at: DateTime<FixedOffset>,
    pub completed_at: Option<DateTime<FixedOffset>>,
    /// Present only once `status == "completed"`.
    pub download_url: Option<String>,
}

impl From<export_job::Model> for ExportJobDto {
    fn from(m: export_job::Model) -> Self {
        let download_url = matches!(m.status, ExportJobStatus::Completed)
            .then(|| format!("/export-jobs/{}/download", m.id));
        Self {
            id: m.id,
            camera_id: m.camera_id,
            status: m.status,
            size_bytes: m.size_bytes,
            error: m.error,
            created_at: m.created_at,
            completed_at: m.completed_at,
            download_url,
        }
    }
}

// -- Handlers --

/// POST /recordings/export
///
/// Concatenates every chunk covering `[from, to)` into one downloadable file.
/// The first and last chunks are trimmed to the requested boundaries (see
/// `vms_media::export_range`); middle chunks are used in full. The job runs in
/// the background: this returns `202` with a job id and the caller polls
/// `GET /export-jobs/{id}` for progress.
#[handler]
pub async fn create_export(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<ExportJobDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateExportBody = crate::error::parse_body(req).await?;

    if body.from >= body.to {
        return Err(ApiError::bad_request("'from' must be earlier than 'to'"));
    }

    state
        .camera_repo
        .get(body.camera_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {} not found", body.camera_id)))?;

    let job = state
        .export_job_repo
        .create(
            body.camera_id,
            body.from.fixed_offset(),
            body.to.fixed_offset(),
        )
        .await?;

    spawn_export_job(state.clone(), job.id, body.camera_id, body.from, body.to);

    res.status_code(StatusCode::ACCEPTED);
    Ok(Json(ExportJobDto::from(job)))
}

/// GET /export-jobs/{id}
#[handler]
pub async fn get_export_job(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<ExportJobDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let job = state.export_job_repo.get(id).await?;
    Ok(Json(ExportJobDto::from(job)))
}

/// GET /export-jobs/{id}/download
#[handler]
pub async fn download_export(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let job = state.export_job_repo.get(id).await?;

    match job.status {
        ExportJobStatus::Completed => {}
        ExportJobStatus::Failed => {
            return Err(ApiError::bad_request(format!(
                "export job {id} failed: {}",
                job.error.unwrap_or_default()
            )));
        }
        ExportJobStatus::Pending | ExportJobStatus::Running => {
            return Err(ApiError::conflict(format!(
                "export job {id} is not finished yet"
            )));
        }
    }

    let file_path = job
        .file_path
        .ok_or_else(|| ApiError::not_found(format!("export job {id} has no file")))?;

    let not_found = || ApiError::not_found(format!("export job {id} has no file on disk"));
    let path = match resolve_recording_file(&state.media_recording_dir, Path::new(&file_path)) {
        Ok(Some(path)) => path,
        Ok(None) => {
            tracing::warn!(export_job_id = %id, file_path, "Refusing to serve an export file outside the recording dir");
            return Err(not_found());
        }
        Err(e) => {
            tracing::error!(export_job_id = %id, file_path, error = %e, "Export file missing on disk");
            return Err(not_found());
        }
    };

    let file = NamedFile::builder(&path)
        .attached_name(format!("export_{id}.mp4"))
        .build()
        .await
        .map_err(|e| {
            tracing::error!(export_job_id = %id, file_path, error = %e, "Export file missing on disk");
            not_found()
        })?;

    file.send(req.headers(), res).await;
    Ok(())
}

// -- Background job --

fn spawn_export_job(
    state: AppState,
    job_id: Uuid,
    camera_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_export_job(&state, job_id, camera_id, from, to).await {
            tracing::error!(export_job_id = %job_id, error = %e, "Export job failed");
            let _ = state.export_job_repo.fail(job_id, e).await;
        }
    });
}

async fn run_export_job(
    state: &AppState,
    job_id: Uuid,
    camera_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<(), String> {
    state
        .export_job_repo
        .start(job_id)
        .await
        .map_err(|e| e.to_string())?;

    let from_offset = from.fixed_offset();
    let to_offset = to.fixed_offset();

    let mut chunks = state
        .recording_repo
        .list_range_ordered(camera_id, from_offset, to_offset)
        .await
        .map_err(|e| e.to_string())?;

    // Skip the open chunk, if any: splitmuxsink is still appending to it.
    chunks.retain(|c| c.end_time.is_some());

    if chunks.is_empty() {
        return Err("no finalized recordings cover this time range".into());
    }

    let codec = chunks
        .iter()
        .find_map(|c| c.codec.clone())
        .unwrap_or_else(|| "H264".to_string());

    let export_chunks = export_chunks(&state.media_recording_dir, chunks, from_offset, to_offset)?;

    let export_dir = state.media_recording_dir.join("exports");
    tokio::fs::create_dir_all(&export_dir)
        .await
        .map_err(|e| format!("create export dir: {e}"))?;
    let output_path = export_dir.join(format!("export_{job_id}.mp4"));

    let output_path_clone = output_path.clone();
    tokio::task::spawn_blocking(move || {
        vms_media::export_range(&export_chunks, &codec, &output_path_clone)
    })
    .await
    .map_err(|e| format!("export task panicked: {e}"))?
    .map_err(|e| e.to_string())?;

    let size_bytes = tokio::fs::metadata(&output_path)
        .await
        .map(|m| m.len() as i64)
        .unwrap_or(0);

    state
        .export_job_repo
        .complete(
            job_id,
            output_path.to_string_lossy().into_owned(),
            size_bytes,
        )
        .await
        .map_err(|e| e.to_string())?;

    tracing::info!(export_job_id = %job_id, camera_id = %camera_id, size_bytes, "Export job completed");
    Ok(())
}

/// Turn finalized chunk rows into export inputs, trimming the first and last
/// to `[from, to)`. Fails if any chunk's file is missing or outside
/// `recording_dir`, so nothing outside it is read or written next to.
fn export_chunks(
    recording_dir: &Path,
    rows: Vec<recording::Model>,
    from: DateTime<FixedOffset>,
    to: DateTime<FixedOffset>,
) -> Result<Vec<ExportChunk>, String> {
    let chunk_count = rows.len();
    rows.into_iter()
        .enumerate()
        .map(|(i, c)| {
            let file_path = match resolve_recording_file(recording_dir, Path::new(&c.file_path)) {
                Ok(Some(path)) => path,
                Ok(None) => {
                    tracing::warn!(recording_id = %c.id, file_path = %c.file_path, "Refusing to export a recording outside the recording dir");
                    return Err(format!("recording {} is outside the recording dir", c.id));
                }
                Err(e) => {
                    tracing::error!(recording_id = %c.id, file_path = %c.file_path, error = %e, "Recording file missing on disk");
                    return Err(format!("recording {} file is missing on disk", c.id));
                }
            };

            let end_time = c.end_time.expect("only finalized chunks are exported");

            let trim_start_ns = if i == 0 && from > c.start_time {
                Some((from - c.start_time).num_nanoseconds().unwrap_or(0) as u64)
            } else {
                None
            };
            let trim_stop_ns = if i == chunk_count - 1 && to < end_time {
                Some((to - c.start_time).num_nanoseconds().unwrap_or(0) as u64)
            } else {
                None
            };

            Ok(ExportChunk {
                file_path: file_path.to_string_lossy().into_owned(),
                trim_start_ns,
                trim_stop_ns,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// A scratch dir under the system temp dir, removed on drop.
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("vms-export-test-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn chunk_row(file_path: &Path, start: DateTime<FixedOffset>, secs: i64) -> recording::Model {
        recording::Model {
            id: Uuid::new_v4(),
            camera_id: Uuid::new_v4(),
            file_path: file_path.to_string_lossy().into_owned(),
            chunk_index: 0,
            start_time: start,
            end_time: Some(start + chrono::Duration::seconds(secs)),
            size_bytes: None,
            codec: Some("H264".into()),
            created_at: start,
        }
    }

    #[test]
    fn chunk_inside_the_recording_dir_is_exported_from_its_resolved_path() {
        let root = ScratchDir::new();
        let rec_dir = root.0.join("recordings");
        std::fs::create_dir_all(&rec_dir).unwrap();
        let chunk = rec_dir.join("chunk.mp4");
        std::fs::write(&chunk, b"").unwrap();

        let start = Utc::now().fixed_offset();
        let from = start + chrono::Duration::seconds(10);
        let to = start + chrono::Duration::seconds(50);
        let rows = vec![chunk_row(
            &rec_dir.join("..").join("recordings").join("chunk.mp4"),
            start,
            60,
        )];

        let chunks = export_chunks(&rec_dir, rows, from, to).unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].file_path,
            std::fs::canonicalize(&chunk).unwrap().to_string_lossy()
        );
        assert_eq!(chunks[0].trim_start_ns, Some(10_000_000_000));
        assert_eq!(chunks[0].trim_stop_ns, Some(50_000_000_000));
    }

    #[test]
    fn chunk_outside_the_recording_dir_fails_the_export() {
        let root = ScratchDir::new();
        let rec_dir = root.0.join("recordings");
        std::fs::create_dir_all(&rec_dir).unwrap();
        let inside = rec_dir.join("chunk.mp4");
        let outside = root.0.join("secret.mp4");
        std::fs::write(&inside, b"").unwrap();
        std::fs::write(&outside, b"").unwrap();

        let start = Utc::now().fixed_offset();
        let bad = chunk_row(&outside, start + chrono::Duration::seconds(60), 60);
        let bad_id = bad.id;
        let rows = vec![chunk_row(&inside, start, 60), bad];

        let err = export_chunks(
            &rec_dir,
            rows,
            start,
            start + chrono::Duration::seconds(120),
        )
        .err()
        .unwrap();

        assert_eq!(
            err,
            format!("recording {bad_id} is outside the recording dir")
        );
    }

    #[test]
    fn missing_chunk_file_fails_the_export() {
        let root = ScratchDir::new();
        let start = Utc::now().fixed_offset();
        let row = chunk_row(&root.0.join("gone.mp4"), start, 60);
        let id = row.id;

        let err = export_chunks(
            &root.0,
            vec![row],
            start,
            start + chrono::Duration::seconds(60),
        )
        .err()
        .unwrap();

        assert_eq!(err, format!("recording {id} file is missing on disk"));
    }
}
