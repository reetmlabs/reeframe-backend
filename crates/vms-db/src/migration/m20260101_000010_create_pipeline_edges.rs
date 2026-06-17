use sea_orm_migration::prelude::*;

use super::m20260101_000007_create_pipelines::Pipeline;
use super::m20260101_000009_create_pipeline_nodes::PipelineNode;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PipelineEdge::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PipelineEdge::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(PipelineEdge::PipelineId).uuid().not_null())
                    .col(ColumnDef::new(PipelineEdge::FromNodeId).uuid().not_null())
                    .col(ColumnDef::new(PipelineEdge::ToNodeId).uuid().not_null())
                    .col(
                        ColumnDef::new(PipelineEdge::EdgeType)
                            .text()
                            .not_null()
                            .default("default"),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_edges_pipeline")
                            .from(PipelineEdge::Table, PipelineEdge::PipelineId)
                            .to(Pipeline::Table, Pipeline::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_edges_from_node")
                            .from(PipelineEdge::Table, PipelineEdge::FromNodeId)
                            .to(PipelineNode::Table, PipelineNode::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_edges_to_node")
                            .from(PipelineEdge::Table, PipelineEdge::ToNodeId)
                            .to(PipelineNode::Table, PipelineNode::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        // Prevents duplicate edges between the same pair of nodes.
        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_edges_unique_pair")
                    .table(PipelineEdge::Table)
                    .col(PipelineEdge::FromNodeId)
                    .col(PipelineEdge::ToNodeId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_edges_pipeline")
                    .table(PipelineEdge::Table)
                    .col(PipelineEdge::PipelineId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_edges_from_node")
                    .table(PipelineEdge::Table)
                    .col(PipelineEdge::FromNodeId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_edges_to_node")
                    .table(PipelineEdge::Table)
                    .col(PipelineEdge::ToNodeId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(PipelineEdge::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum PipelineEdge {
    #[iden = "pipeline_edges"]
    Table,
    Id,
    PipelineId,
    FromNodeId,
    ToNodeId,
    EdgeType,
}
