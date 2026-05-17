use sea_orm_migration::prelude::*;

use super::m20260101_000002_create_sources::Source;
use super::m20260101_000007_create_pipelines::Pipeline;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PipelineSourceRef::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PipelineSourceRef::PipelineId)
                            .uuid()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PipelineSourceRef::SourceId)
                            .uuid()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(PipelineSourceRef::PipelineId)
                            .col(PipelineSourceRef::SourceId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_source_refs_pipeline")
                            .from(PipelineSourceRef::Table, PipelineSourceRef::PipelineId)
                            .to(Pipeline::Table, Pipeline::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipeline_source_refs_source")
                            .from(PipelineSourceRef::Table, PipelineSourceRef::SourceId)
                            .to(Source::Table, Source::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_pipeline_source_refs_source")
                    .table(PipelineSourceRef::Table)
                    .col(PipelineSourceRef::SourceId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(PipelineSourceRef::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum PipelineSourceRef {
    Table,
    PipelineId,
    SourceId,
}
