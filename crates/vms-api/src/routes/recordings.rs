use chrono::{DateTime, FixedOffset, Utc};
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

// -- Internal helpers --

/// Parse an RFC 3339 datetime out of a query parameter, e.g.
/// `?from=2026-07-01T14:00:00Z`.
fn parse_query_datetime(req: &mut Request, name: &str) -> Result<DateTime<Utc>, ApiError> {
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
