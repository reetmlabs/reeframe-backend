use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Contact::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Contact::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(Contact::Name).text().not_null())
                    .col(ColumnDef::new(Contact::Email).text())
                    .col(ColumnDef::new(Contact::Phone).text())
                    .col(ColumnDef::new(Contact::TelegramChatId).big_integer())
                    .col(ColumnDef::new(Contact::Extra).json_binary().not_null())
                    .col(
                        ColumnDef::new(Contact::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(Contact::UpdatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.drop_table(Table::drop().table(Contact::Table).to_owned()).await
    }
}

#[derive(Iden)]
pub enum Contact {
    Table,
    Id,
    Name,
    Email,
    Phone,
    TelegramChatId,
    Extra,
    CreatedAt,
    UpdatedAt,
}
