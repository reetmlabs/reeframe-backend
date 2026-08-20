//! Precomputed daily recording coverage — the aggregation this crate's
//! `StatMonitor` runs periodically so the API can serve `O(days)` summaries
//! instead of the frontend re-bucketing raw chunks on every request.
//!
//! [`compute_day_coverage`] is a direct Rust port of the Qt frontend's
//! `RecordingModel::sessions()`/`dailySummaries()` merge loop
//! (`ReeframeFrontend/src/models/RecordingModel.cpp`) — same 5-second gap
//! tolerance, same "an open chunk always ends a session," same "unknown
//! size poisons the total" rule, same "an open session contributes zero
//! coverage" rule. It's given chunks already scoped to one UTC calendar day
//! (via `RecordingRepo::list_starting_in_range_for_camera`), so the FE's
//! "a new day always forces a new session" rule falls out for free — there's
//! no cross-day chunk in the input to force a split against.
//!
//! One deliberate deviation from the frontend: day boundaries here are UTC,
//! not the viewing device's local timezone (the frontend has no concept of
//! a shared canonical timezone to bucket by, since a server serves many
//! viewers who may be in different zones). A chunk within a few hours of UTC
//! midnight can land on a different calendar day here than in the frontend's
//! own on-demand fallback — accepted as a known limitation, see
//! `roadmap/BE/roadmap.md`'s "Step 16e" entry.

use chrono::{DateTime, FixedOffset};
use serde::Serialize;

use vms_db::entities::recording;

/// Chunks whose start-to-start gap is at or under this are treated as one
/// continuous session — matches `RecordingsPanel`'s `sessions(5)` call on
/// the frontend exactly, so precomputed and on-demand values agree on
/// ordinary chunk-rotation boundaries.
const GAP_TOLERANCE_SECS: i64 = 5;

#[derive(Serialize)]
pub struct SessionRange {
    pub start: DateTime<FixedOffset>,
    /// `None` marks the session that's still being recorded.
    pub end: Option<DateTime<FixedOffset>>,
    pub chunk_count: i32,
    /// `None` if any chunk in this session has an unknown size.
    pub size_bytes: Option<i64>,
}

pub struct DailyCoverageResult {
    /// Sum of merged session spans — an in-progress session contributes 0
    /// until it closes (matches the frontend's `totalCoverageSecs`, not a
    /// live `now() - start` estimate).
    pub coverage_seconds: i64,
    pub session_ranges: serde_json::Value,
    /// Raw chunk count for the day (sessions merge chunks; this doesn't).
    pub chunk_count: i32,
    /// `None` if any chunk that day has an unknown size.
    pub total_size_bytes: Option<i64>,
}

/// Merge `chunks` (already scoped to one calendar day, oldest first — see
/// [`vms_db::repos::RecordingRepo::list_starting_in_range_for_camera`])
/// into sessions and summarize them for that day.
pub fn compute_day_coverage(chunks: &[recording::Model]) -> DailyCoverageResult {
    if chunks.is_empty() {
        return DailyCoverageResult {
            coverage_seconds: 0,
            session_ranges: serde_json::Value::Array(vec![]),
            chunk_count: 0,
            total_size_bytes: None,
        };
    }

    let mut sessions: Vec<SessionRange> = Vec::new();
    let mut session_start = chunks[0].start_time;
    let mut session_end = chunks[0].end_time;
    let mut session_size = chunks[0].size_bytes;
    let mut session_chunk_count = 1i32;

    let flush = |sessions: &mut Vec<SessionRange>,
                 start: DateTime<FixedOffset>,
                 end: Option<DateTime<FixedOffset>>,
                 size_bytes: Option<i64>,
                 chunk_count: i32| {
        sessions.push(SessionRange {
            start,
            end,
            chunk_count,
            size_bytes,
        });
    };

    for chunk in &chunks[1..] {
        // An open chunk (still being written) always ends the running
        // session — nothing can legitimately follow it yet. Otherwise,
        // contiguous if the gap since the running session's end is within
        // tolerance (a negative gap, i.e. overlap, still counts).
        let contiguous = session_end
            .is_some_and(|end| (chunk.start_time - end).num_seconds() <= GAP_TOLERANCE_SECS);

        if contiguous {
            session_end = chunk.end_time;
            session_size = match (session_size, chunk.size_bytes) {
                (Some(a), Some(b)) => Some(a + b),
                _ => None,
            };
            session_chunk_count += 1;
        } else {
            flush(
                &mut sessions,
                session_start,
                session_end,
                session_size,
                session_chunk_count,
            );
            session_start = chunk.start_time;
            session_end = chunk.end_time;
            session_size = chunk.size_bytes;
            session_chunk_count = 1;
        }
    }
    flush(
        &mut sessions,
        session_start,
        session_end,
        session_size,
        session_chunk_count,
    );

    let coverage_seconds = sessions
        .iter()
        .filter_map(|s| s.end.map(|end| (end - s.start).num_seconds()))
        .sum();

    let total_size_bytes = chunks
        .iter()
        .try_fold(0i64, |acc, c| Some(acc + c.size_bytes?));

    DailyCoverageResult {
        coverage_seconds,
        session_ranges: serde_json::to_value(&sessions)
            .expect("SessionRange is plain data — serialization cannot fail"),
        chunk_count: chunks.len() as i32,
        total_size_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use uuid::Uuid;

    fn chunk(
        start: DateTime<FixedOffset>,
        end: Option<DateTime<FixedOffset>>,
        size_bytes: Option<i64>,
    ) -> recording::Model {
        recording::Model {
            id: Uuid::new_v4(),
            camera_id: Uuid::new_v4(),
            file_path: "chunk.mp4".into(),
            chunk_index: 0,
            start_time: start,
            end_time: end,
            size_bytes,
            codec: Some("H264".into()),
            created_at: start,
        }
    }

    fn at(secs_from_epoch: i64) -> DateTime<FixedOffset> {
        DateTime::from_timestamp(secs_from_epoch, 0)
            .unwrap()
            .fixed_offset()
    }

    #[test]
    fn empty_input_yields_zeroed_result() {
        let result = compute_day_coverage(&[]);
        assert_eq!(result.coverage_seconds, 0);
        assert_eq!(result.chunk_count, 0);
        assert_eq!(result.total_size_bytes, None);
        assert_eq!(result.session_ranges, serde_json::json!([]));
    }

    #[test]
    fn adjacent_chunks_merge_into_one_session() {
        // Back-to-back, zero gap — well within the 5s tolerance.
        let chunks = vec![
            chunk(at(0), Some(at(600)), Some(1000)),
            chunk(at(600), Some(at(1200)), Some(2000)),
        ];
        let result = compute_day_coverage(&chunks);
        assert_eq!(result.coverage_seconds, 1200);
        assert_eq!(result.chunk_count, 2);
        assert_eq!(result.total_size_bytes, Some(3000));
        let sessions = result.session_ranges.as_array().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["chunk_count"], 2);
    }

    #[test]
    fn gap_bigger_than_tolerance_starts_new_session() {
        let chunks = vec![
            chunk(at(0), Some(at(600)), Some(1000)),
            chunk(
                at(600) + Duration::hours(1),
                Some(at(600) + Duration::hours(1) + Duration::seconds(600)),
                Some(1000),
            ),
        ];
        let result = compute_day_coverage(&chunks);
        let sessions = result.session_ranges.as_array().unwrap();
        assert_eq!(sessions.len(), 2);
        // Coverage only counts each session's own span, not the gap between them.
        assert_eq!(result.coverage_seconds, 1200);
    }

    #[test]
    fn still_open_chunk_contributes_no_coverage_and_has_null_end() {
        let chunks = vec![chunk(at(0), None, None)];
        let result = compute_day_coverage(&chunks);
        assert_eq!(result.coverage_seconds, 0);
        let sessions = result.session_ranges.as_array().unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0]["end"].is_null());
    }

    #[test]
    fn open_chunk_mid_stream_still_forces_a_new_session_after_it() {
        // A closed chunk following an open one can't be "contiguous" with
        // it — the open chunk has no end to measure the gap from.
        let chunks = vec![
            chunk(at(0), None, Some(1000)),
            chunk(at(600), Some(at(1200)), Some(1000)),
        ];
        let result = compute_day_coverage(&chunks);
        let sessions = result.session_ranges.as_array().unwrap();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn unknown_size_poisons_session_and_day_total() {
        let chunks = vec![
            chunk(at(0), Some(at(600)), Some(1000)),
            chunk(at(600), Some(at(1200)), None),
        ];
        let result = compute_day_coverage(&chunks);
        assert_eq!(result.total_size_bytes, None);
        let sessions = result.session_ranges.as_array().unwrap();
        assert!(sessions[0]["size_bytes"].is_null());
    }
}
