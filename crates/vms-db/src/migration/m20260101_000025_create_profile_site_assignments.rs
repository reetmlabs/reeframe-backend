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
                    .table(ProfileSiteAssignment::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ProfileSiteAssignment::ProfileId)
                            .uuid()
                            .not_null(),
                    )
                    // Opaque site identifier, same as `tile_profiles.site_id`
                    // — no local FK.
                    .col(
                        ColumnDef::new(ProfileSiteAssignment::SiteId)
                            .uuid()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(ProfileSiteAssignment::ProfileId)
                            .col(ProfileSiteAssignment::SiteId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_profile_site_assignments_profile")
                            .from(
                                ProfileSiteAssignment::Table,
                                ProfileSiteAssignment::ProfileId,
                            )
                            .to(TileProfile::Table, TileProfile::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_profile_site_assignments_site")
                    .table(ProfileSiteAssignment::Table)
                    .col(ProfileSiteAssignment::SiteId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ProfileSiteAssignment::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum ProfileSiteAssignment {
    #[iden = "profile_site_assignments"]
    Table,
    ProfileId,
    SiteId,
}
