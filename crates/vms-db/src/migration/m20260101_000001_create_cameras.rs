use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Camera::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Camera::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(Camera::Name).text().not_null())
                    .col(ColumnDef::new(Camera::Description).text())
                    .col(ColumnDef::new(Camera::RtspUrl).text().not_null())
                    .col(ColumnDef::new(Camera::Manufacturer).text())
                    .col(ColumnDef::new(Camera::Model).text())
                    .col(ColumnDef::new(Camera::Username).text())
                    .col(ColumnDef::new(Camera::PasswordEnc).text())
                    .col(ColumnDef::new(Camera::ExtraConfig).json_binary().not_null())
                    .col(
                        ColumnDef::new(Camera::RingBufferDurationSecs)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(Camera::RingBufferStorage)
                            .text()
                            .not_null()
                            .default("memory"),
                    )
                    .col(
                        ColumnDef::new(Camera::Enabled)
                            .boolean()
                            .not_null()
                            .default(true),
                    )
                    .col(
                        ColumnDef::new(Camera::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(Camera::UpdatedAt)
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
            .drop_table(Table::drop().table(Camera::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Camera {
    #[iden = "cameras"]
    Table,
    Id,
    Name,
    Description,
    RtspUrl,
    SubRtspUrl,
    Codec,
    Manufacturer,
    Model,
    Username,
    PasswordEnc,
    ExtraConfig,
    RingBufferDurationSecs,
    RingBufferStorage,
    Enabled,
    CreatedAt,
    UpdatedAt,
    RetentionDays,
    RetentionDiskThresholdPercent,
    DesiredRecording,
    Timezone,
}
