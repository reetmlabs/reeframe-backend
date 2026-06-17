use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Destination::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Destination::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Destination::Name).text().not_null())
                    .col(ColumnDef::new(Destination::Description).text())
                    .col(ColumnDef::new(Destination::Type).text().not_null())
                    .col(ColumnDef::new(Destination::Config).json_binary().not_null())
                    .col(
                        ColumnDef::new(Destination::Enabled)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(Destination::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(Destination::UpdatedAt)
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
            .drop_table(Table::drop().table(Destination::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Destination {
    #[iden = "destinations"]
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
