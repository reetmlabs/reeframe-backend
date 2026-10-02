//! Recording-chunk lifecycle events.
//!
//! This is a narrow channel used only for DB bookkeeping, separate from the
//! trigger-eligible [`crate::event::Event`] stream. `vms-media` produces these
//! as `splitmuxsink` opens and closes each recording chunk, and a consumer task
//! with DB access (which `vms-media` deliberately lacks) turns them into
//! `recordings` rows. The type lives here because both the producer
//! (`vms-media`) and the consumer (`vms-daemon`) already depend on this crate.

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A recording chunk opening or closing, taken directly from `splitmuxsink`
/// signals and bus messages. Those are the only reliable source of a chunk's
/// real start and end time; deriving it from filenames or chunk indices
/// drifts across reconnects.
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
    /// A fragment that `splitmuxsink` opened and closed without writing any
    /// data. An unresolved reconnect bug produces one such 0-byte fragment
    /// right before a long-lived reconnect errors out. The open row created by
    /// `Opened` should be deleted rather than backfilled as a real chunk.
    Discarded { camera_id: Uuid, file_path: String },
}
