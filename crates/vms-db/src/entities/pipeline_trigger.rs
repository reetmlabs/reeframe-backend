use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum TriggerType {
    #[sea_orm(string_value = "schedule")]
    Schedule,
    #[sea_orm(string_value = "event")]
    Event,
    #[sea_orm(string_value = "system")]
    System,
    #[sea_orm(string_value = "manual")]
    Manual,
    #[sea_orm(string_value = "stat")]
    Stat,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipeline_triggers")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub trigger_type: TriggerType,
    /// FK to sources table; set for Event triggers sourced from an external adapter.
    pub source_id: Option<Uuid>,
    /// FK to cameras table; set for System/Event triggers scoped to one camera.
    pub camera_id: Option<Uuid>,
    /// Full typed trigger configuration serialised as JSONB.
    pub config: Json,
    pub enabled: bool,
    pub created_at: DateTimeWithTimeZone,
    /// Message from the most recent filter-evaluation failure (bad syntax,
    /// unknown identifier); `None` once the filter evaluates successfully.
    pub last_error: Option<String>,
    pub last_error_at: Option<DateTimeWithTimeZone>,
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
        belongs_to = "super::source::Entity",
        from = "Column::SourceId",
        to = "super::source::Column::Id"
    )]
    Source,
    #[sea_orm(
        belongs_to = "super::camera::Entity",
        from = "Column::CameraId",
        to = "super::camera::Column::Id"
    )]
    Camera,
    #[sea_orm(has_many = "super::pipeline_run::Entity")]
    PipelineRun,
}

impl Related<super::pipeline::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Pipeline.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl Related<super::camera::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Camera.def()
    }
}

impl Related<super::pipeline_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineRun.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
