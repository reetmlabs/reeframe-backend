use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Source::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Source::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(Source::Name).text().not_null())
                    .col(ColumnDef::new(Source::Description).text())
                    .col(ColumnDef::new(Source::Type).text().not_null())
                    .col(ColumnDef::new(Source::Config).json_binary().not_null())
                    .col(
                        ColumnDef::new(Source::Enabled)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(Source::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(Source::UpdatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Source::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Source {
    #[iden = "sources"]
    Table,
    Id,
    Name,
    Description,
    #[iden = "type"]
    Type,
    Config,
    Enabled,
    CreatedAt,
    UpdatedAt,
}
