use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Resource Manager reference-counting table: records which pipelines reference
/// which cameras and what resources they require (ring buffer, analytics branch).
/// The Resource Manager aggregates these rows on pipeline enable/disable to
/// decide whether to start or stop a `RingBuffer` or `AnalyticsBranch` resource.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipeline_camera_refs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub pipeline_id: Uuid,
    #[sea_orm(primary_key, auto_increment = false)]
    pub camera_id: Uuid,
    /// `true` when the pipeline needs a ring-buffer branch on this camera
    /// (i.e. it has an `extract_clip` action node).
    pub needs_ring_buffer: bool,
    /// `true` when the pipeline needs an analytics branch on this camera
    /// (i.e. it has an analytics event trigger or action that reads detections).
    pub needs_analytics: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pipeline::Entity",
        from = "Column::PipelineId",
        to = "super::pipeline::Column::Id"
    )]
    Pipeline,
    #[sea_orm(
        belongs_to = "super::camera::Entity",
        from = "Column::CameraId",
        to = "super::camera::Column::Id"
    )]
    Camera,
}

impl Related<super::pipeline::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Pipeline.def()
    }
}

impl Related<super::camera::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Camera.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
