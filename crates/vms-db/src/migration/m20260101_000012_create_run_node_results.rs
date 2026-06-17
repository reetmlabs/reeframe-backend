use sea_orm_migration::prelude::*;

use super::m20260101_000009_create_pipeline_nodes::PipelineNode;
use super::m20260101_000011_create_pipeline_runs::PipelineRun;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(RunNodeResult::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(RunNodeResult::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(RunNodeResult::RunId).uuid().not_null())
                    .col(ColumnDef::new(RunNodeResult::NodeId).uuid().not_null())
                    .col(
                        ColumnDef::new(RunNodeResult::Status)
                            .text()
                            .not_null()
                            .default("pending"),
                    )
                    .col(ColumnDef::new(RunNodeResult::StartedAt).timestamp_with_time_zone())
                    .col(ColumnDef::new(RunNodeResult::CompletedAt).timestamp_with_time_zone())
                    .col(
                        ColumnDef::new(RunNodeResult::Output)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(RunNodeResult::Error).text())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_run_node_results_run")
                            .from(RunNodeResult::Table, RunNodeResult::RunId)
                            .to(PipelineRun::Table, PipelineRun::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_run_node_results_node")
                            .from(RunNodeResult::Table, RunNodeResult::NodeId)
                            .to(PipelineNode::Table, PipelineNode::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_run_node_results_run")
                    .table(RunNodeResult::Table)
                    .col(RunNodeResult::RunId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_run_node_results_node")
                    .table(RunNodeResult::Table)
                    .col(RunNodeResult::NodeId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(RunNodeResult::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum RunNodeResult {
    #[iden = "run_node_results"]
    Table,
    Id,
    RunId,
    NodeId,
    Status,
    StartedAt,
    CompletedAt,
    Output,
    Error,
}
