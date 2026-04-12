//! HTTP handlers for querying the recording segment index.
//!
//! The `recording_segments` table is populated automatically by the DB indexer as
//! `splitmuxsink` opens and closes MP4 chunk files.  These endpoints expose the index
//! to the frontend so it can build a timeline slider and navigate the recording archive.
//!
//! | Method | Path                         | Description                              |
//! |--------|------------------------------|------------------------------------------|
//! | GET    | `/feeds/{id}/segments`       | List segments for a feed in a time range |
//! | GET    | `/segments/{id}`             | Get a single segment by its ID           |

use salvo::prelude::*;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::Deserialize;

use crate::entities::recording_segment;

// ---------------------------------------------------------------------------
// list_segments
// ---------------------------------------------------------------------------

/// Query parameters for the segment list endpoint.
#[derive(Deserialize, Debug)]
struct SegmentListQuery {
    /// Start of the time range (RFC3339, e.g. `2026-04-12T00:00:00Z`).
    start: Option<String>,
    /// End of the time range (RFC3339, e.g. `2026-04-12T23:59:59Z`).
    end: Option<String>,
}

/// List all recording segments for a feed, optionally filtered by time range.
///
/// Segments are returned in `start_time` ascending order.  If `start` and/or `end` query
/// parameters are omitted the filter is open-ended on that side.
///
/// # Query parameters
/// * `start` — Lower bound (RFC3339).  Segments that start at or after this time are included.
/// * `end`   — Upper bound (RFC3339).  Segments that start before this time are included.
///
/// # Response
/// JSON array of [`recording_segment::Model`].
///
/// # Errors
/// Returns `500 Internal Server Error` on database failures.
#[handler]
pub async fn list_segments(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let feed_id = req.param::<i32>("id").unwrap_or_default();

    let params = req.parse_queries::<SegmentListQuery>().unwrap_or(SegmentListQuery {
        start: None,
        end: None,
    });

    // Build the base query filtered by feed ID.
    let mut query = recording_segment::Entity::find()
        .filter(recording_segment::Column::FeedId.eq(feed_id));

    // Apply optional start/end filters on start_time.
    if let Some(start_str) = params.start {
        match chrono::DateTime::parse_from_rfc3339(&start_str) {
            Ok(dt) => {
                query = query
                    .filter(recording_segment::Column::StartTime.gte(dt.with_timezone(&chrono::Utc)));
            }
            Err(e) => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(format!("Invalid 'start' timestamp: {}", e));
                return;
            }
        }
    }

    if let Some(end_str) = params.end {
        match chrono::DateTime::parse_from_rfc3339(&end_str) {
            Ok(dt) => {
                query = query
                    .filter(recording_segment::Column::StartTime.lt(dt.with_timezone(&chrono::Utc)));
            }
            Err(e) => {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(format!("Invalid 'end' timestamp: {}", e));
                return;
            }
        }
    }

    match query.all(db).await {
        Ok(segments) => res.render(Json(segments)),
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error querying segments: {}", e));
        }
    }
}

// ---------------------------------------------------------------------------
// get_segment
// ---------------------------------------------------------------------------

/// Get a single recording segment by its primary key.
///
/// # Path parameter
/// * `id` — The `recording_segments.id` value.
///
/// # Response
/// JSON representation of [`recording_segment::Model`], or `404 Not Found`.
#[handler]
pub async fn get_segment(dep: &mut Depot, req: &mut Request, res: &mut Response) {
    let db = dep
        .get::<DatabaseConnection>("db")
        .expect("Database connection missing");
    let segment_id = req.param::<i32>("id").unwrap_or_default();

    match recording_segment::Entity::find_by_id(segment_id).one(db).await {
        Ok(Some(seg)) => res.render(Json(seg)),
        Ok(None) => { res.status_code(StatusCode::NOT_FOUND); }
        Err(e) => {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            res.render(format!("Error fetching segment: {}", e));
        }
    }
}
