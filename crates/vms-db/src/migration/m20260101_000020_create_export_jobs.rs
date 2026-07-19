use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ExportJob::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ExportJob::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ExportJob::CameraId).uuid().not_null())
                    .col(
                        ColumnDef::new(ExportJob::FromTime)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExportJob::ToTime)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExportJob::Status)
                            .text()
                            .not_null()
                            .default("pending"),
                    )
                    // Set once the job completes successfully.
                    .col(ColumnDef::new(ExportJob::FilePath).text())
                    .col(ColumnDef::new(ExportJob::SizeBytes).big_integer())
                    // Set once the job fails.
                    .col(ColumnDef::new(ExportJob::Error).text())
                    .col(
                        ColumnDef::new(ExportJob::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new(ExportJob::CompletedAt).timestamp_with_time_zone())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_export_jobs_camera")
                            .from(ExportJob::Table, ExportJob::CameraId)
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_export_jobs_status")
                    .table(ExportJob::Table)
                    .col(ExportJob::Status)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ExportJob::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum ExportJob {
    #[iden = "export_jobs"]
    Table,
    Id,
    CameraId,
    FromTime,
    ToTime,
    Status,
    FilePath,
    SizeBytes,
    Error,
    CreatedAt,
    CompletedAt,
}
