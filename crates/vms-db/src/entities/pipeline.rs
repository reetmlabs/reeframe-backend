use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum PipelineType {
    /// User-created pipeline.
    #[sea_orm(string_value = "user")]
    User,
    /// System-managed pipeline (created internally, not editable via API).
    #[sea_orm(string_value = "system")]
    System,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipelines")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub pipeline_type: PipelineType,
    pub enabled: bool,
    /// UUID of the user who created this pipeline (references users table, not in this schema).
    pub created_by: Option<Uuid>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    /// Serialized `Vec<pipeline_validation::ValidationIssue>` from the most
    /// recent `PipelineRepo::revalidate` call — recomputed on every node/
    /// edge/trigger save and on enable, never on read.
    pub validation_issues: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::pipeline_trigger::Entity")]
    PipelineTrigger,
    #[sea_orm(has_many = "super::pipeline_node::Entity")]
    PipelineNode,
    #[sea_orm(has_many = "super::pipeline_edge::Entity")]
    PipelineEdge,
    #[sea_orm(has_many = "super::pipeline_run::Entity")]
    PipelineRun,
    #[sea_orm(has_many = "super::pipeline_source_ref::Entity")]
    PipelineSourceRef,
    #[sea_orm(has_many = "super::pipeline_camera_ref::Entity")]
    PipelineCameraRef,
}

impl Related<super::pipeline_trigger::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineTrigger.def()
    }
}

impl Related<super::pipeline_node::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineNode.def()
    }
}

impl Related<super::pipeline_edge::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineEdge.def()
    }
}

impl Related<super::pipeline_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineRun.def()
    }
}

impl Related<super::pipeline_source_ref::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineSourceRef.def()
    }
}

impl Related<super::pipeline_camera_ref::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineCameraRef.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
