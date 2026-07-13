use sea_orm_migration::prelude::*;

use super::m20260101_000017_create_users::User;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ApiKey::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(ApiKey::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(ApiKey::UserId).uuid().not_null())
                    .col(ColumnDef::new(ApiKey::KeyHash).text().not_null())
                    .col(ColumnDef::new(ApiKey::Name).text().not_null())
                    .col(ColumnDef::new(ApiKey::LastUsed).timestamp_with_time_zone())
                    .col(
                        ColumnDef::new(ApiKey::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_api_keys_user")
                            .from(ApiKey::Table, ApiKey::UserId)
                            .to(User::Table, User::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_api_keys_hash_unique")
                    .table(ApiKey::Table)
                    .col(ApiKey::KeyHash)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_api_keys_user")
                    .table(ApiKey::Table)
                    .col(ApiKey::UserId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ApiKey::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum ApiKey {
    #[iden = "api_keys"]
    Table,
    Id,
    UserId,
    KeyHash,
    Name,
    LastUsed,
    CreatedAt,
}
