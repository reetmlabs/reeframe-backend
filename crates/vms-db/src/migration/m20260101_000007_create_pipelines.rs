use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Pipeline::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Pipeline::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(Pipeline::Name).text().not_null())
                    .col(ColumnDef::new(Pipeline::Description).text())
                    .col(
                        ColumnDef::new(Pipeline::PipelineType)
                            .text()
                            .not_null()
                            .default("user"),
                    )
                    .col(
                        ColumnDef::new(Pipeline::Enabled)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(ColumnDef::new(Pipeline::CreatedBy).uuid())
                    .col(
                        ColumnDef::new(Pipeline::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(Pipeline::UpdatedAt)
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
            .drop_table(Table::drop().table(Pipeline::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Pipeline {
    #[iden = "pipelines"]
    Table,
    Id,
    Name,
    Description,
    PipelineType,
    Enabled,
    CreatedBy,
    CreatedAt,
    UpdatedAt,
}
