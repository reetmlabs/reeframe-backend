use sea_orm_migration::prelude::*;

use super::m20260101_000008_create_pipeline_triggers::PipelineTrigger;

/// Set when the trigger's source is deleted or disabled, instead of the
/// trigger row being dropped or the delete/disable being blocked. Separate
/// from `last_error`, which tracks runtime filter-evaluation failures; this
/// flag marks a structural problem with what the trigger points at.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineTrigger::Table)
                    .add_column(
                        ColumnDef::new(PipelineTrigger::UnresolvedReference)
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
                    .table(PipelineTrigger::Table)
                    .drop_column(PipelineTrigger::UnresolvedReference)
                    .to_owned(),
            )
            .await
    }
}
