use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

/// Persisted operator intent — "should this camera be recording" —
/// independent of whether a recording branch is actually attached right
/// now. See `MediaManager::is_recording` for the in-memory, achievable-only
/// counterpart this is *not* a replacement for.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Camera::Table)
                    .add_column(
                        ColumnDef::new(Camera::DesiredRecording)
                            .boolean()
                            .not_null()
                            .default(false),
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
                    .drop_column(Camera::DesiredRecording)
                    .to_owned(),
            )
            .await
    }
}
