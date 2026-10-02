use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;
use super::m20260101_000022_create_tile_profiles::TileProfile;
use super::m20260101_000023_create_tile_formations::TileFormation;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TileCameraBinding::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TileCameraBinding::ProfileId)
                            .uuid()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TileCameraBinding::TileId).uuid().not_null())
                    // Opaque site identifier, same as `tile_profiles.site_id`, with no local FK.
                    .col(ColumnDef::new(TileCameraBinding::SiteId).uuid().not_null())
                    .col(
                        ColumnDef::new(TileCameraBinding::CameraId)
                            .uuid()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(TileCameraBinding::ProfileId)
                            .col(TileCameraBinding::TileId)
                            .col(TileCameraBinding::SiteId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_tile_camera_bindings_profile")
                            .from(TileCameraBinding::Table, TileCameraBinding::ProfileId)
                            .to(TileProfile::Table, TileProfile::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_tile_camera_bindings_tile")
                            .from(TileCameraBinding::Table, TileCameraBinding::TileId)
                            .to(TileFormation::Table, TileFormation::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    // Unlike the frontend's local copy of this schema, which has no
                    // `cameras` table to reference, the backend owns `cameras`. Deleting a
                    // camera cascades to this binding row, and a missing row is exactly the
                    // "slot is unassigned" state, scoped to the one site that had it.
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_tile_camera_bindings_camera")
                            .from(TileCameraBinding::Table, TileCameraBinding::CameraId)
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_tile_camera_bindings_profile_site")
                    .table(TileCameraBinding::Table)
                    .col(TileCameraBinding::ProfileId)
                    .col(TileCameraBinding::SiteId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_tile_camera_bindings_camera")
                    .table(TileCameraBinding::Table)
                    .col(TileCameraBinding::CameraId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(TileCameraBinding::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum TileCameraBinding {
    #[iden = "tile_camera_bindings"]
    Table,
    ProfileId,
    TileId,
    SiteId,
    CameraId,
}
