use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Resource Manager reference-counting table: records which pipelines reference
/// which external source adapters so the Resource Manager can start/stop them
/// using the Minimum Activation Principle.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "pipeline_source_refs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub pipeline_id: Uuid,
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: Uuid,
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

impl ActiveModelBehavior for ActiveModel {}
