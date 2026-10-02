use sea_orm_migration::prelude::*;

use super::m20260101_000022_create_tile_profiles::TileProfile;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TileFormation::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TileFormation::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TileFormation::ProfileId).uuid().not_null())
                    .col(ColumnDef::new(TileFormation::GridCol).integer().not_null())
                    .col(ColumnDef::new(TileFormation::GridRow).integer().not_null())
                    .col(
                        ColumnDef::new(TileFormation::ColSpan)
                            .integer()
                            .not_null()
                            .default(1),
                    )
                    .col(
                        ColumnDef::new(TileFormation::RowSpan)
                            .integer()
                            .not_null()
                            .default(1),
                    )
                    .col(
                        ColumnDef::new(TileFormation::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_tile_formations_profile")
                            .from(TileFormation::Table, TileFormation::ProfileId)
                            .to(TileProfile::Table, TileProfile::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_tile_formations_profile")
                    .table(TileFormation::Table)
                    .col(TileFormation::ProfileId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(TileFormation::Table).to_owned())
            .await
    }
}

/// Named `grid_col`/`grid_row` instead of `col`/`row`; see
/// `entities::tile_formation` for why SeaORM's entity derive can't use the
/// bare names.
#[derive(Iden)]
pub enum TileFormation {
    #[iden = "tile_formations"]
    Table,
    Id,
    ProfileId,
    GridCol,
    GridRow,
    ColSpan,
    RowSpan,
    CreatedAt,
}
