use sea_orm_migration::prelude::*;

use super::m20260101_000007_create_pipelines::Pipeline;

/// Persists the result of `PipelineRepo::validate_pipeline` so a read (e.g.
/// listing every pipeline) never needs to re-run graph analysis — recomputed
/// on every save and re-checked specifically when a pipeline is enabled.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Pipeline::Table)
                    .add_column(
                        ColumnDef::new(Pipeline::ValidationIssues)
                            .json_binary()
                            .not_null()
                            .default("[]"),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Pipeline::Table)
                    .drop_column(Pipeline::ValidationIssues)
                    .to_owned(),
            )
            .await
    }
}
