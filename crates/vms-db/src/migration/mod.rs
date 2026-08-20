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
mod m20260101_000017_create_users;
mod m20260101_000018_create_api_keys;
mod m20260101_000019_create_recordings;
mod m20260101_000020_create_export_jobs;
mod m20260101_000021_create_settings;
mod m20260101_000022_create_tile_profiles;
mod m20260101_000023_create_tile_formations;
mod m20260101_000024_create_tile_camera_bindings;
mod m20260101_000025_create_profile_site_assignments;
mod m20260101_000026_create_events;
mod m20260101_000027_add_camera_retention_policy;
mod m20260101_000028_create_daily_recording_coverage;

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
            Box::new(m20260101_000017_create_users::Migration),
            Box::new(m20260101_000018_create_api_keys::Migration),
            Box::new(m20260101_000019_create_recordings::Migration),
            Box::new(m20260101_000020_create_export_jobs::Migration),
            Box::new(m20260101_000021_create_settings::Migration),
            Box::new(m20260101_000022_create_tile_profiles::Migration),
            Box::new(m20260101_000023_create_tile_formations::Migration),
            Box::new(m20260101_000024_create_tile_camera_bindings::Migration),
            Box::new(m20260101_000025_create_profile_site_assignments::Migration),
            Box::new(m20260101_000026_create_events::Migration),
            Box::new(m20260101_000027_add_camera_retention_policy::Migration),
            Box::new(m20260101_000028_create_daily_recording_coverage::Migration),
        ]
    }
}
