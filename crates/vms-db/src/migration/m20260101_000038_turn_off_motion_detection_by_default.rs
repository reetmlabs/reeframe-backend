use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

/// Motion detection is opt-in. Cameras that got the column's `true` default
/// before this switch was chosen by anyone are turned off; the API sets the
/// value explicitly for every new camera.
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .exec_stmt(
                Query::update()
                    .table(Camera::Table)
                    .value(Camera::MotionDetectionEnabled, false)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}
