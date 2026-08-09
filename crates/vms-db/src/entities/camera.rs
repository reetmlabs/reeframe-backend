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
