use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ContactList::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ContactList::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ContactList::Name).text().not_null())
                    .col(ColumnDef::new(ContactList::Description).text())
                    .col(
                        ColumnDef::new(ContactList::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(ContactList::UpdatedAt)
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
            .drop_table(Table::drop().table(ContactList::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum ContactList {
    Table,
    Id,
    Name,
    Description,
    CreatedAt,
    UpdatedAt,
}
