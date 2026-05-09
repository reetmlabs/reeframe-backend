use sea_orm_migration::prelude::*;

use super::m20260101_000003_create_destinations::Destination;
use super::m20260101_000005_create_contact_lists::ContactList;
use super::m20260101_000007_create_pipelines::Pipeline;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PipelineNode::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PipelineNode::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(PipelineNode::PipelineId).uuid().not_null())
                    .col(ColumnDef::new(PipelineNode::NodeType).text().not_null())
                    .col(ColumnDef::new(PipelineNode::ActionType).text())
                    .col(ColumnDef::new(PipelineNode::DestinationId).uuid())
                    .col(ColumnDef::new(PipelineNode::ContactListId).uuid())
                    .col(ColumnDef::new(PipelineNode::Config).json_binary().not_null())
                    .col(ColumnDef::new(PipelineNode::Label).text())
                    .col(ColumnDef::new(PipelineNode::PosX).double())
                    .col(ColumnDef::new(PipelineNode::PosY).double())
                    .col(
                        ColumnDef::new(PipelineNode::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_nodes_pipeline")
                            .from(PipelineNode::Table, PipelineNode::PipelineId)
                            .to(Pipeline::Table, Pipeline::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_nodes_destination")
                            .from(PipelineNode::Table, PipelineNode::DestinationId)
                            .to(Destination::Table, Destination::Id)
                            .on_delete(ForeignKeyAction::Restrict),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_nodes_contact_list")
                            .from(PipelineNode::Table, PipelineNode::ContactListId)
                            .to(ContactList::Table, ContactList::Id)
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_nodes_pipeline")
                    .table(PipelineNode::Table)
                    .col(PipelineNode::PipelineId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(PipelineNode::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum PipelineNode {
    Table,
    Id,
    PipelineId,
    NodeType,
    ActionType,
    DestinationId,
    ContactListId,
    Config,
    Label,
    PosX,
    PosY,
    CreatedAt,
}
