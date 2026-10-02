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
                    .table(Recording::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Recording::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Recording::CameraId).uuid().not_null())
                    .col(ColumnDef::new(Recording::FilePath).text().not_null())
                    .col(ColumnDef::new(Recording::ChunkIndex).integer().not_null())
                    .col(
                        ColumnDef::new(Recording::StartTime)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    // Nullable while the chunk is still being written; backfilled on
                    // `splitmuxsink-fragment-closed`.
                    .col(ColumnDef::new(Recording::EndTime).timestamp_with_time_zone())
                    .col(ColumnDef::new(Recording::SizeBytes).big_integer())
                    .col(ColumnDef::new(Recording::Codec).text())
                    .col(
                        ColumnDef::new(Recording::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_recordings_camera")
                            .from(Recording::Table, Recording::CameraId)
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_recordings_camera_time")
                    .table(Recording::Table)
                    .col(Recording::CameraId)
                    .col(Recording::StartTime)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Recording::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Recording {
    #[iden = "recordings"]
    Table,
    Id,
    CameraId,
    FilePath,
    ChunkIndex,
    StartTime,
    EndTime,
    SizeBytes,
    Codec,
    CreatedAt,
}
