use sea_orm_migration::prelude::*;

use super::m20260101_000009_create_pipeline_nodes::PipelineNode;

/// Set when a Transport node's destination is deleted or disabled, instead
/// of the node being dropped or the delete/disable being blocked. Mirrors
/// `pipeline_triggers.unresolved_reference` (migration 32), but scoped to
/// the individual node rather than the whole pipeline/trigger.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineNode::Table)
                    .add_column(
                        ColumnDef::new(PipelineNode::UnresolvedReference)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineNode::Table)
                    .drop_column(PipelineNode::UnresolvedReference)
                    .to_owned(),
            )
            .await
    }
}
