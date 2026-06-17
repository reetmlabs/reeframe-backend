use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;
use super::m20260101_000002_create_sources::Source;
use super::m20260101_000007_create_pipelines::Pipeline;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PipelineTrigger::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PipelineTrigger::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(PipelineTrigger::PipelineId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PipelineTrigger::TriggerType)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(PipelineTrigger::SourceId).uuid())
                    .col(ColumnDef::new(PipelineTrigger::CameraId).uuid())
                    .col(
                        ColumnDef::new(PipelineTrigger::Config)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PipelineTrigger::Enabled)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(PipelineTrigger::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_triggers_pipeline")
                            .from(PipelineTrigger::Table, PipelineTrigger::PipelineId)
                            .to(Pipeline::Table, Pipeline::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_triggers_source")
                            .from(PipelineTrigger::Table, PipelineTrigger::SourceId)
                            .to(Source::Table, Source::Id)
                            .on_delete(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_triggers_camera")
                            .from(PipelineTrigger::Table, PipelineTrigger::CameraId)
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_triggers_pipeline")
                    .table(PipelineTrigger::Table)
                    .col(PipelineTrigger::PipelineId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_triggers_source")
                    .table(PipelineTrigger::Table)
                    .col(PipelineTrigger::SourceId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_triggers_camera")
                    .table(PipelineTrigger::Table)
                    .col(PipelineTrigger::CameraId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(PipelineTrigger::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum PipelineTrigger {
    #[iden = "pipeline_triggers"]
    Table,
    Id,
    PipelineId,
    TriggerType,
    SourceId,
    CameraId,
    Config,
    Enabled,
    CreatedAt,
}
