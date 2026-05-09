use sea_orm_migration::prelude::*;

use super::m20260101_000007_create_pipelines::Pipeline;
use super::m20260101_000008_create_pipeline_triggers::PipelineTrigger;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PipelineRun::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PipelineRun::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(PipelineRun::PipelineId).uuid().not_null())
                    .col(ColumnDef::new(PipelineRun::TriggerId).uuid())
                    .col(
                        ColumnDef::new(PipelineRun::TriggeredAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(PipelineRun::TriggerContext)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PipelineRun::Status)
                            .text()
                            .not_null()
                            .default("running"),
                    )
                    .col(ColumnDef::new(PipelineRun::CompletedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(PipelineRun::Error).text())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_runs_pipeline")
                            .from(PipelineRun::Table, PipelineRun::PipelineId)
                            .to(Pipeline::Table, Pipeline::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_runs_trigger")
                            .from(PipelineRun::Table, PipelineRun::TriggerId)
                            .to(PipelineTrigger::Table, PipelineTrigger::Id)
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_runs_pipeline")
                    .table(PipelineRun::Table)
                    .col(PipelineRun::PipelineId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_runs_triggered_at")
                    .table(PipelineRun::Table)
                    .col(PipelineRun::TriggeredAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_runs_status")
                    .table(PipelineRun::Table)
                    .col(PipelineRun::Status)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(PipelineRun::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum PipelineRun {
    Table,
    Id,
    PipelineId,
    TriggerId,
    TriggeredAt,
    TriggerContext,
    Status,
    CompletedAt,
    Error,
}
