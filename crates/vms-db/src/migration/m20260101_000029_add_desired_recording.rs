use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

/// Persisted operator intent ("should this camera be recording"), independent
/// of whether a recording branch is attached right now. It complements
/// `MediaManager::is_recording`, the in-memory state of what is actually
/// running, and does not replace it.
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
