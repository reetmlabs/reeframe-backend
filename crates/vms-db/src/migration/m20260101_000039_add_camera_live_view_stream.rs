use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

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
                        ColumnDef::new(Camera::LiveViewStream)
                            .string()
                            .not_null()
                            .default("sub"),
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
                    .drop_column(Camera::LiveViewStream)
                    .to_owned(),
            )
            .await
    }
}
