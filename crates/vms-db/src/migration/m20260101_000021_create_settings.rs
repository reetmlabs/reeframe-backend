use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Setting::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Setting::Key).text().not_null().primary_key())
                    // JSON-encoded value — settings span bool/string/number/etc.
                    .col(ColumnDef::new(Setting::Value).text().not_null())
                    // True from the moment an API-driven change to a
                    // restart-required setting is written, until the next
                    // successful startup applies it and clears this flag.
                    .col(
                        ColumnDef::new(Setting::PendingRestart)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(Setting::UpdatedAt)
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
            .drop_table(Table::drop().table(Setting::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Setting {
    #[iden = "settings"]
    Table,
    Key,
    Value,
    PendingRestart,
    UpdatedAt,
}
