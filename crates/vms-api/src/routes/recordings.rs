use chrono::{DateTime, FixedOffset, Utc};
use salvo::fs::NamedFile;
use salvo::prelude::*;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    error::{parse_id, ApiError},
    state::AppState,
};

// -- Response DTOs --

/// A recording chunk as returned by the API. `file_path` (the on-disk
/// location) is deliberately not exposed — callers only ever need
/// `GET /recordings/{id}/stream` to actually fetch the bytes.
#[derive(Serialize)]
pub struct RecordingDto {
    pub id: Uuid,
    pub chunk_index: i32,
    pub start_time: DateTime<FixedOffset>,
    pub end_time: Option<DateTime<FixedOffset>>,
    pub size_bytes: Option<i64>,
    pub codec: Option<String>,
}

impl From<vms_db::entities::recording::Model> for RecordingDto {
    fn from(m: vms_db::entities::recording::Model) -> Self {
        Self {
            id: m.id,
            chunk_index: m.chunk_index,
            start_time: m.start_time,
            end_time: m.end_time,
            size_bytes: m.size_bytes,
            codec: m.codec,
        }
    }
}

#[derive(Serialize)]
pub struct PlaybackDto {
    pub recording_id: Uuid,
    pub stream_url: String,
    pub offset_secs: f64,
}

/// One precomputed daily-coverage row — see `vms_engine::coverage` for how
/// `session_ranges`/`coverage_seconds` are derived (a port of the frontend's
/// own `RecordingModel::dailySummaries()` merge algorithm).
#[derive(Serialize)]
pub struct DailyCoverageDto {
    pub camera_id: Uuid,
    pub date: chrono::NaiveDate,
    pub coverage_seconds: i64,
    pub session_ranges: serde_json::Value,
    pub chunk_count: i32,
    pub total_size_bytes: Option<i64>,
    pub is_finalized: bool,
    pub purged_by_retention: bool,
}

impl From<vms_db::entities::daily_recording_coverage::Model> for DailyCoverageDto {
    fn from(m: vms_db::entities::daily_recording_coverage::Model) -> Self {
        Self {
            camera_id: m.camera_id,
            date: m.day,
            coverage_seconds: m.coverage_seconds,
            session_ranges: m.session_ranges,
            chunk_count: m.chunk_count,
            total_size_bytes: m.total_size_bytes,
            is_finalized: m.is_finalized,
            purged_by_retention: m.purged_by_retention,
        }
    }
}

// -- Internal helpers --

/// Parse a bare calendar date out of a query parameter, e.g. `?from=2026-07-01`.
fn parse_query_date(req: &mut Request, name: &str) -> Result<chrono::NaiveDate, ApiError> {
    let raw = req
        .query::<String>(name)
        .ok_or_else(|| ApiError::bad_request(format!("missing '{name}' query parameter")))?;
    chrono::NaiveDate::parse_from_str(&raw, "%Y-%m-%d")
        .map_err(|_| ApiError::bad_request(format!("invalid '{name}' — expected yyyy-mm-dd")))
}

/// Parse an RFC 3339 datetime out of a query parameter, e.g.
/// `?from=2026-07-01T14:00:00Z`.
pub(crate) fn parse_query_datetime(
    req: &mut Request,
    name: &str,
) -> Result<DateTime<Utc>, ApiError> {
    let raw = req
        .query::<String>(name)
        .ok_or_else(|| ApiError::bad_request(format!("missing '{name}' query parameter")))?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| {
            ApiError::bad_request(format!(
                "invalid '{name}' — expected RFC 3339, e.g. 2026-07-01T14:00:00Z"
            ))
        })
}

// -- Handlers --

/// GET /cameras/{id}/recordings?from=<rfc3339>&to=<rfc3339>
///
/// Chunks overlapping `[from, to)`, oldest first.
#[handler]
pub async fn list_recordings(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<RecordingDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let camera_id = parse_id(req)?;
    let from = parse_query_datetime(req, "from")?;
    let to = parse_query_datetime(req, "to")?;

    if from >= to {
        return Err(ApiError::bad_request("'from' must be earlier than 'to'"));
    }

    let recordings = state
        .recording_repo
        .list_for_camera(camera_id, from.fixed_offset(), to.fixed_offset())
        .await?;

    Ok(Json(
        recordings.into_iter().map(RecordingDto::from).collect(),
    ))
}

/// GET /cameras/{id}/playback?at=<rfc3339>
///
/// Resolves a specific instant to the chunk covering it, plus how far into
/// that chunk it is — the entry point for "start playback from this point in
/// the past." `stream_url` is Range-enabled MP4 (`GET /recordings/{id}/stream`);
/// the caller loads it and seeks to `offset_secs`, no server-side transcoding
/// needed since chunks are already faststart-remuxed on close.
///
/// If `at` falls in a gap (camera offline, or outside all recorded history),
/// responds `404` with the nearest chunk boundaries on either side so the
/// caller can offer "jump to nearest available footage" instead of a dead end.
#[handler]
pub async fn get_playback(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let camera_id = parse_id(req)?;
    let at = parse_query_datetime(req, "at")?;
    let at_offset = at.fixed_offset();

    if let Some(rec) = state
        .recording_repo
        .resolve_at(camera_id, at_offset)
        .await?
    {
        let offset_secs = (at_offset - rec.start_time).num_milliseconds() as f64 / 1000.0;
        res.render(Json(PlaybackDto {
            recording_id: rec.id,
            stream_url: format!("/recordings/{}/stream", rec.id),
            offset_secs,
        }));
        return Ok(());
    }

    let (before, after) = state
        .recording_repo
        .nearest_boundaries(camera_id, at_offset)
        .await?;

    res.status_code(StatusCode::NOT_FOUND);
    res.render(Json(serde_json::json!({
        "error": "no recording covers this time",
        "nearest_before": before.map(|r| r.end_time),
        "nearest_after": after.map(|r| r.start_time),
    })));
    Ok(())
}

/// GET /recordings/{id}/stream
///
/// Serves the chunk's MP4 file directly off disk with `Accept-Ranges: bytes`
/// support (`salvo::fs::NamedFile` handles Range parsing, `206 Partial
/// Content`, ETag/If-None-Match, etc.) — a browser/player `<video>` element
/// can seek within it with zero server-side work per seek, since chunks are
/// already faststart-remuxed (`moov` before `mdat`) when they're closed.
#[handler]
pub async fn stream_recording(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let rec = state.recording_repo.get(id).await?;

    let file = NamedFile::open(&rec.file_path).await.map_err(|e| {
        tracing::error!(recording_id = %id, file_path = %rec.file_path, error = %e, "Recording file missing on disk");
        ApiError::not_found(format!("recording {id} has no file on disk"))
    })?;

    file.send(req.headers(), res).await;
    Ok(())
}

/// GET /cameras/{id}/recordings/daily-summary?from=<yyyy-mm-dd>&to=<yyyy-mm-dd>
///
/// Precomputed per-day coverage for one camera, `date` in `[from, to)` —
/// `O(days)`, not `O(chunks)`. This is what a recordings panel should call
/// for a whole visible range instead of `GET /cameras/{id}/recordings`
/// followed by client-side bucketing.
#[handler]
pub async fn list_daily_summary(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<DailyCoverageDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let camera_id = parse_id(req)?;
    let from = parse_query_date(req, "from")?;
    let to = parse_query_date(req, "to")?;

    if from >= to {
        return Err(ApiError::bad_request("'from' must be earlier than 'to'"));
    }

    let rows = state
        .daily_coverage_repo
        .list_range(camera_id, from, to)
        .await?;

    Ok(Json(rows.into_iter().map(DailyCoverageDto::from).collect()))
}

/// GET /recordings/daily-summary?camera_ids=<uuid,uuid,...>&from=&to=
///
/// Same as [`list_daily_summary`], across multiple cameras in one call — the
/// fleet-overview shape, one query instead of N per-camera requests.
#[handler]
pub async fn list_daily_summary_bulk(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<DailyCoverageDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let from = parse_query_date(req, "from")?;
    let to = parse_query_date(req, "to")?;

    if from >= to {
        return Err(ApiError::bad_request("'from' must be earlier than 'to'"));
    }

    let raw_ids = req
        .query::<String>("camera_ids")
        .ok_or_else(|| ApiError::bad_request("missing 'camera_ids' query parameter"))?;
    let camera_ids: Vec<Uuid> = raw_ids
        .split(',')
        .map(|s| s.trim().parse::<Uuid>())
        .collect::<Result<_, _>>()
        .map_err(|_| {
            ApiError::bad_request("invalid 'camera_ids' — expected comma-separated UUIDs")
        })?;

    let rows = state
        .daily_coverage_repo
        .list_range_bulk(&camera_ids, from, to)
        .await?;

    Ok(Json(rows.into_iter().map(DailyCoverageDto::from).collect()))
}
