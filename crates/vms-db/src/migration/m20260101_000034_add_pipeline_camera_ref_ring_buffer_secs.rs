use sea_orm_migration::prelude::*;

use super::m20260101_000014_create_pipeline_camera_refs::PipelineCameraRef;

/// Seconds the ring buffer must hold to satisfy every `extract_clip` node
/// referencing the camera, so the buffer is sized to what the nodes need.
/// `0` when `needs_ring_buffer` is `false`.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineCameraRef::Table)
                    .add_column(
                        ColumnDef::new(PipelineCameraRef::RingBufferSecs)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PipelineCameraRef::Table)
                    .drop_column(PipelineCameraRef::RingBufferSecs)
                    .to_owned(),
            )
            .await
    }
}
