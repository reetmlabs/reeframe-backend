//! Recording segment entity.
//!
//! Every MP4 chunk written by `splitmuxsink` is indexed here so that the frontend can
//! browse the recording archive, build a timeline slider, and request playback of any
//! time range.
//!
//! A *segment* corresponds to exactly one file on disk.  Segments are created when
//! `splitmuxsink` opens a new file (`splitmuxsink-fragment-opened` signal) and finalized
//! when it closes the file (`splitmuxsink-fragment-closed` signal).

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "recording_segments")]
pub struct Model {
    /// Auto-incremented primary key.
    #[sea_orm(primary_key)]
    pub id: i32,

    /// The feed that produced this segment.
    pub feed_id: i32,

    /// Absolute path to the MP4 file on disk.
    pub file_path: String,

    /// Wall-clock timestamp when the segment was opened (UTC, RFC3339).
    pub start_time: DateTimeWithTimeZone,

    /// Wall-clock timestamp when the segment was closed (UTC, RFC3339).  `None` while
    /// the segment is still being written.
    pub end_time: Option<DateTimeWithTimeZone>,

    /// Duration of the segment in seconds, computed from the GStreamer running-time
    /// delta reported by `splitmuxsink-fragment-closed`.  `None` while in progress.
    pub duration_secs: Option<f64>,

    /// File size in bytes, populated via `std::fs::metadata` once the file is closed.
    /// `None` while in progress.
    pub file_size_bytes: Option<i64>,

    /// What caused this recording: `"user"`, `"ai"`, `"hardware"`, or `"schedule"`.
    pub trigger_type: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    /// Each segment belongs to exactly one feed.
    #[sea_orm(
        belongs_to = "super::feed::Entity",
        from = "Column::FeedId",
        to = "super::feed::Column::Id"
    )]
    Feed,
}

impl Related<super::feed::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Feed.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
