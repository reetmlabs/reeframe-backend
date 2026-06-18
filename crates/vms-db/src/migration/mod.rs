use sea_orm_migration::prelude::*;

mod m20260101_000001_create_cameras;
mod m20260101_000002_create_sources;
mod m20260101_000003_create_destinations;
mod m20260101_000004_create_contacts;
mod m20260101_000005_create_contact_lists;
mod m20260101_000006_create_contact_list_members;
mod m20260101_000007_create_pipelines;
mod m20260101_000008_create_pipeline_triggers;
mod m20260101_000009_create_pipeline_nodes;
mod m20260101_000010_create_pipeline_edges;
mod m20260101_000011_create_pipeline_runs;
mod m20260101_000012_create_run_node_results;
mod m20260101_000013_create_pipeline_source_refs;
mod m20260101_000014_create_pipeline_camera_refs;
mod m20260101_000015_add_sub_rtsp_url;
mod m20260101_000016_add_camera_codec;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260101_000001_create_cameras::Migration),
            Box::new(m20260101_000002_create_sources::Migration),
            Box::new(m20260101_000003_create_destinations::Migration),
            Box::new(m20260101_000004_create_contacts::Migration),
            Box::new(m20260101_000005_create_contact_lists::Migration),
            Box::new(m20260101_000006_create_contact_list_members::Migration),
            Box::new(m20260101_000007_create_pipelines::Migration),
            Box::new(m20260101_000008_create_pipeline_triggers::Migration),
            Box::new(m20260101_000009_create_pipeline_nodes::Migration),
            Box::new(m20260101_000010_create_pipeline_edges::Migration),
            Box::new(m20260101_000011_create_pipeline_runs::Migration),
            Box::new(m20260101_000012_create_run_node_results::Migration),
            Box::new(m20260101_000013_create_pipeline_source_refs::Migration),
            Box::new(m20260101_000014_create_pipeline_camera_refs::Migration),
            Box::new(m20260101_000015_add_sub_rtsp_url::Migration),
            Box::new(m20260101_000016_add_camera_codec::Migration),
        ]
    }
}
