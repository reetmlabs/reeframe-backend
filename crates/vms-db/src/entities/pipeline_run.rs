use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    #[sea_orm(string_value = "running")]
    Running,
    #[sea_orm(string_value = "completed")]
    Completed,
    #[sea_orm(string_value = "failed")]
    Failed,
    #[sea_orm(string_value = "cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipeline_runs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub pipeline_id: Uuid,
    /// FK to pipeline_triggers; NULL when triggered manually without a row reference.
    pub trigger_id: Option<Uuid>,
    pub triggered_at: DateTimeWithTimeZone,
    /// Snapshot of the `TriggerContext` that fired this run, serialised as JSONB.
    pub trigger_context: Json,
    pub status: RunStatus,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub error: Option<String>,
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
        belongs_to = "super::pipeline_trigger::Entity",
        from = "Column::TriggerId",
        to = "super::pipeline_trigger::Column::Id"
    )]
    PipelineTrigger,
    #[sea_orm(has_many = "super::run_node_result::Entity")]
    RunNodeResult,
}

impl Related<super::pipeline::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Pipeline.def()
    }
}

impl Related<super::pipeline_trigger::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineTrigger.def()
    }
}

impl Related<super::run_node_result::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RunNodeResult.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
