use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum NodeResultStatus {
    /// Waiting for parent nodes to complete.
    #[sea_orm(string_value = "pending")]
    Pending,
    /// Currently executing.
    #[sea_orm(string_value = "running")]
    Running,
    /// Finished without errors.
    #[sea_orm(string_value = "completed")]
    Completed,
    /// Finished with an error.
    #[sea_orm(string_value = "failed")]
    Failed,
    /// Not executed because a Condition node routed away from this branch.
    #[sea_orm(string_value = "skipped")]
    Skipped,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "run_node_results")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub run_id: Uuid,
    pub node_id: Uuid,
    pub status: NodeResultStatus,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    /// `NodeOutput` serialised as JSONB: artifact paths, delivery URLs, message IDs, etc.
    pub output: Json,
    pub error: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pipeline_run::Entity",
        from = "Column::RunId",
        to = "super::pipeline_run::Column::Id"
    )]
    PipelineRun,
    #[sea_orm(
        belongs_to = "super::pipeline_node::Entity",
        from = "Column::NodeId",
        to = "super::pipeline_node::Column::Id"
    )]
    PipelineNode,
}

impl Related<super::pipeline_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineRun.def()
    }
}

impl Related<super::pipeline_node::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineNode.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
