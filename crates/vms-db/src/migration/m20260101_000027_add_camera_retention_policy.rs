use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

/// Per-camera override of the global `[recordings] retention_days` /
/// `retention_disk_threshold_percent` config. `NULL` means "no override, inherit
/// the global default"; disabling cleanup is an explicit `0`.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // SQLite only supports one ALTER TABLE ADD COLUMN per statement.
        manager
            .alter_table(
                Table::alter()
                    .table(Camera::Table)
                    .add_column(ColumnDef::new(Camera::RetentionDays).integer().null())
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Camera::Table)
                    .add_column(
                        ColumnDef::new(Camera::RetentionDiskThresholdPercent)
                            .double()
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
                    .table(Camera::Table)
                    .drop_column(Camera::RetentionDays)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Camera::Table)
                    .drop_column(Camera::RetentionDiskThresholdPercent)
                    .to_owned(),
            )
            .await
    }
}
