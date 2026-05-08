use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    /// Standard data-flow edge used for all non-condition routing.
    #[sea_orm(string_value = "default")]
    Default,
    /// Followed when a Condition node's expression evaluates to `true`.
    #[sea_orm(string_value = "true_branch")]
    TrueBranch,
    /// Followed when a Condition node's expression evaluates to `false`.
    #[sea_orm(string_value = "false_branch")]
    FalseBranch,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipeline_edges")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub from_node_id: Uuid,
    pub to_node_id: Uuid,
    pub edge_type: EdgeType,
}

/// `pipeline_edges` has two FKs to `pipeline_nodes` (`from_node_id` and
/// `to_node_id`). Because SeaORM only allows one `Related` impl per target
/// entity, both are exposed as named relations; use them directly via
/// `find_with_related` or `join` rather than `Related`.
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pipeline::Entity",
        from = "Column::PipelineId",
        to = "super::pipeline::Column::Id"
    )]
    Pipeline,
    #[sea_orm(
        belongs_to = "super::pipeline_node::Entity",
        from = "Column::FromNodeId",
        to = "super::pipeline_node::Column::Id"
    )]
    FromNode,
    #[sea_orm(
        belongs_to = "super::pipeline_node::Entity",
        from = "Column::ToNodeId",
        to = "super::pipeline_node::Column::Id"
    )]
    ToNode,
}

impl Related<super::pipeline::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Pipeline.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
