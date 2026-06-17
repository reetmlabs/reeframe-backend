use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;
use super::m20260101_000007_create_pipelines::Pipeline;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PipelineCameraRef::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PipelineCameraRef::PipelineId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PipelineCameraRef::CameraId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PipelineCameraRef::NeedsRingBuffer)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(PipelineCameraRef::NeedsAnalytics)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .primary_key(
                        Index::create()
                            .col(PipelineCameraRef::PipelineId)
                            .col(PipelineCameraRef::CameraId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_camera_refs_pipeline")
                            .from(PipelineCameraRef::Table, PipelineCameraRef::PipelineId)
                            .to(Pipeline::Table, Pipeline::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_camera_refs_camera")
                            .from(PipelineCameraRef::Table, PipelineCameraRef::CameraId)
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_camera_refs_camera")
                    .table(PipelineCameraRef::Table)
                    .col(PipelineCameraRef::CameraId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(PipelineCameraRef::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum PipelineCameraRef {
    #[iden = "pipeline_camera_refs"]
    Table,
    PipelineId,
    CameraId,
    NeedsRingBuffer,
    NeedsAnalytics,
}
