use sea_orm_migration::prelude::*;

use super::m20260101_000001_create_cameras::Camera;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Event::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Event::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(Event::CameraId).uuid().not_null())
                    // Free text instead of a DB-level enum, mirroring
                    // `vms_core::event::Event::event_type`, which is already a plain
                    // `String` where events are bridged in.
                    .col(ColumnDef::new(Event::EventType).text().not_null())
                    .col(ColumnDef::new(Event::Payload).json_binary().not_null())
                    .col(
                        ColumnDef::new(Event::OccurredAt)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Event::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_events_camera")
                            .from(Event::Table, Event::CameraId)
                            .to(Camera::Table, Camera::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_events_camera_time")
                    .table(Event::Table)
                    .col(Event::CameraId)
                    .col(Event::OccurredAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Event::Table).to_owned())
            .await
    }
}

// Variants are the table's column names.
#[allow(clippy::enum_variant_names)]
#[derive(Iden)]
pub enum Event {
    #[iden = "events"]
    Table,
    Id,
    CameraId,
    EventType,
    Payload,
    OccurredAt,
    CreatedAt,
}
