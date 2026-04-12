//! Camera feed entity.
//!
//! A `Feed` represents one physical IP camera.  Each feed stores the RTSP URLs for both
//! stream qualities and the recording configuration.  The backend connects to both streams
//! when the frontend issues a `/connect` command; the low-res stream is served by default
//! and the high-res stream is served on demand.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "feeds")]
pub struct Model {
    /// Auto-incremented primary key.
    #[sea_orm(primary_key)]
    pub id: i32,

    /// Human-readable camera name (e.g. "Front Door").
    pub name: String,

    /// Optional free-text description.
    pub description: Option<String>,

    /// RTSP URL for the **low-resolution** stream.  This stream is always connected after
    /// `/connect` and is served to the frontend by default.
    pub rtsp_url: String,

    /// RTSP URL for the **high-resolution** stream.  When `None` the low-res URL is used
    /// for both live viewing and recording (single-stream camera fallback).
    pub rtsp_url_high: Option<String>,

    /// Arbitrary extra parameters stored as a JSON string (reserved for future use).
    pub parameters: Option<String>,

    /// Per-feed AI recording duration override (seconds).  When `None` the global setting
    /// `default_ai_recording_duration_secs` is used.
    pub ai_recording_duration_secs: Option<i32>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    /// A feed has many recording segments.
    #[sea_orm(has_many = "super::recording_segment::Entity")]
    RecordingSegments,
}

impl Related<super::recording_segment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RecordingSegments.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
