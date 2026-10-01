use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum RingBufferStorage {
    #[sea_orm(string_value = "memory")]
    Memory,
    #[sea_orm(string_value = "disk")]
    Disk,
}

/// Which stream live view relays by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum LiveViewStream {
    /// The sub stream, or the main stream when the camera has none.
    #[sea_orm(string_value = "sub")]
    Sub,
    #[sea_orm(string_value = "main")]
    Main,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "cameras")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub sub_rtsp_url: Option<String>,
    /// RTP encoding name detected via codec probe ("H264", "H265", "JPEG", "AV1").
    /// Cached to skip re-probing on daemon restart. NULL until first relay start.
    pub codec: Option<String>,
    pub manufacturer: Option<String>,
    /// Device model string, e.g. "DS-2CD2143G2-I".
    pub model: Option<String>,
    pub username: Option<String>,
    /// AES-256-GCM encrypted RTSP password; prefix `enc:v1:`.
    pub password_enc: Option<String>,
    /// Arbitrary per-camera settings (ONVIF profile, sub-stream URL, etc.).
    pub extra_config: Json,
    pub ring_buffer_duration_secs: i32,
    pub ring_buffer_storage: RingBufferStorage,
    pub enabled: bool,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    /// Per-camera override of `[recordings] retention_days`. `None` inherits
    /// the global default; `Some(0)` explicitly disables age-based cleanup
    /// for this camera.
    pub retention_days: Option<i32>,
    /// Per-camera override of `[recordings] retention_disk_threshold_percent`.
    /// `None` inherits the global default; `Some(0.0)` explicitly disables
    /// disk-threshold cleanup for this camera.
    pub retention_disk_threshold_percent: Option<f64>,
    /// Persisted operator intent — set by `POST /cameras/{id}/recording/
    /// start|stop`, independent of whether a recording branch is actually
    /// attached right now (see `MediaManager::is_recording` for that). The
    /// source of truth boot recovery and reconnect handling reconcile
    /// against, so a manually-started recording survives a restart.
    pub desired_recording: bool,
    /// Per-camera IANA timezone (e.g. "Europe/Berlin") used to compute daily
    /// recording coverage day boundaries. `None` inherits the global
    /// `[recordings] timezone` default.
    pub timezone: Option<String>,
    /// Whether motion detection runs whenever the camera is live. A pipeline
    /// with a camera `Event` trigger turns it on regardless.
    pub motion_detection_enabled: bool,
    /// Whether scrub-preview thumbnails are captured while the camera records.
    pub thumbnails_enabled: bool,
    pub live_view_stream: LiveViewStream,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::pipeline_trigger::Entity")]
    PipelineTrigger,
    #[sea_orm(has_many = "super::pipeline_camera_ref::Entity")]
    PipelineCameraRef,
}

impl Related<super::pipeline_trigger::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineTrigger.def()
    }
}

impl Related<super::pipeline_camera_ref::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineCameraRef.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
