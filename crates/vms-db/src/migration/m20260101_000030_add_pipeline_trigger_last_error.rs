use sea_orm_migration::prelude::*;

use super::m20260101_000008_create_pipeline_triggers::PipelineTrigger;

/// Surfaces a trigger's most recent filter-evaluation failure (bad syntax,
/// unknown identifier) to the API instead of only `tracing::warn!` —
/// previously such a trigger just silently never fired.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SQLite only supports one ALTER TABLE ADD COLUMN per statement.
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineTrigger::Table)
                    .add_column(ColumnDef::new(PipelineTrigger::LastError).text().null())
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineTrigger::Table)
                    .add_column(
                        ColumnDef::new(PipelineTrigger::LastErrorAt)
                            .timestamp_with_time_zone()
                            .null(),
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
                    .drop_column(PipelineTrigger::LastError)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineTrigger::Table)
                    .drop_column(PipelineTrigger::LastErrorAt)
                    .to_owned(),
            )
            .await
    }
}
