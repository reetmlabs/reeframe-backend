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
                    .table(DailyRecordingCoverage::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::CameraId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::Day)
                            .date()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::CoverageSeconds)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::SessionRanges)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::ChunkCount)
                            .integer()
                            .not_null(),
                    )
                    // `NULL` means "unknown" — poisoned by at least one
                    // contributing chunk with no known size yet, same
                    // convention `recordings.size_bytes` already uses.
                    .col(ColumnDef::new(DailyRecordingCoverage::TotalSizeBytes).big_integer())
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::IsFinalized)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::PurgedByRetention)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(DailyRecordingCoverage::ComputedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_daily_recording_coverage_camera")
                            .from(
                                DailyRecordingCoverage::Table,
                                DailyRecordingCoverage::CameraId,
                            )
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_daily_coverage_camera_day")
                    .table(DailyRecordingCoverage::Table)
                    .col(DailyRecordingCoverage::CameraId)
                    .col(DailyRecordingCoverage::Day)
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(DailyRecordingCoverage::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(Iden)]
pub enum DailyRecordingCoverage {
    #[iden = "daily_recording_coverage"]
    Table,
    Id,
    CameraId,
    Day,
    CoverageSeconds,
    SessionRanges,
    ChunkCount,
    TotalSizeBytes,
    IsFinalized,
    PurgedByRetention,
    ComputedAt,
}
