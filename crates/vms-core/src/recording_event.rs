//! Recording-chunk lifecycle events.
//!
//! Distinct from [`crate::event::Event`] (the Event Bus's user-facing,
//! trigger-eligible event stream) — this is a narrow, DB-bookkeeping-only
//! channel. `vms-media` produces these as `splitmuxsink` opens/closes each
//! recording chunk; a small consumer task elsewhere (with DB access, which
//! `vms-media` deliberately has none of) turns them into `recordings` table
//! rows. Living in `vms-core` for the same reason [`crate::event::Event`]
//! does: both the producer (`vms-media`) and the consumer (`vms-daemon`)
//! already depend on this crate, so neither needs a new cross-crate edge.

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A recording chunk opening or closing, as observed directly off
/// `splitmuxsink`'s own signals/bus messages — the one place that knows the
/// real wall-clock instant a chunk started or finished, since deriving it
/// from filenames or chunk-index arithmetic drifts silently across
/// reconnects.
#[derive(Debug, Clone)]
pub enum RecordingChunkEvent {
    /// Fired synchronously from `splitmuxsink`'s `format-location-full`
    /// signal, right before it opens a new fragment file.
    Opened {
        camera_id: Uuid,
        file_path: String,
        chunk_index: i32,
        start_time: DateTime<Utc>,
        codec: Option<String>,
    },
    /// Fired from the `splitmuxsink-fragment-closed` bus message once the
    /// previous fragment file is finalized on disk.
    Closed {
        camera_id: Uuid,
        file_path: String,
        end_time: DateTime<Utc>,
        size_bytes: i64,
    },
}
