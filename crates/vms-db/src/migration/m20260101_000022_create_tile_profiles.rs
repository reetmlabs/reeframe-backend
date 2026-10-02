use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TileProfile::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TileProfile::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TileProfile::Name).text().not_null())
                    // Opaque site identifier. Site identity is owned outside this backend (by
                    // the Coordinator, or by the frontend when there is none), so there is
                    // deliberately no local `sites` table or FK.
                    .col(ColumnDef::new(TileProfile::SiteId).uuid().not_null())
                    .col(
                        ColumnDef::new(TileProfile::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(TileProfile::UpdatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_tile_profiles_site")
                    .table(TileProfile::Table)
                    .col(TileProfile::SiteId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(TileProfile::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum TileProfile {
    #[iden = "tile_profiles"]
    Table,
    Id,
    Name,
    SiteId,
    CreatedAt,
    UpdatedAt,
}
