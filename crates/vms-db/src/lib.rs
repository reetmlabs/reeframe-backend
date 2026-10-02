//! `vms-db`: SeaORM entities, migrations, repositories, and AES-256-GCM
//! credential encryption.
//!
//! # Module overview
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`entities`] | SeaORM `Model`, `Entity`, `ActiveModel`, and `Relation` types for every database table |
//! | [`migration`] | Schema migrations, run through [`Migrator`] |
//! | [`repos`] | Per-table repositories; credential fields are encrypted on write |
//! | [`crypto`] | [`Crypto`]: AES-256-GCM encrypt/decrypt for credential fields stored in JSON columns |
//!
//! # Entity map
//!
//! | Entity module | Table | Notes |
//! |---|---|---|
//! | [`entities::camera`] | `cameras` | GStreamer pipeline source; stores encrypted RTSP password |
//! | [`entities::source`] | `sources` | External source adapters (MQTT, webhook, HA WS, etc.) |
//! | [`entities::destination`] | `destinations` | Transport targets (S3, SFTP, Telegram, Email, etc.) |
//! | [`entities::contact`] | `contacts` | Individual notification recipients |
//! | [`entities::contact_list`] | `contact_lists` | Named groups of contacts |
//! | [`entities::contact_list_member`] | `contact_list_members` | Junction: contact ↔ list |
//! | [`entities::pipeline`] | `pipelines` | Top-level pipeline record |
//! | [`entities::pipeline_trigger`] | `pipeline_triggers` | What fires a pipeline |
//! | [`entities::pipeline_node`] | `pipeline_nodes` | DAG nodes (action, transport, condition, etc.) |
//! | [`entities::pipeline_edge`] | `pipeline_edges` | DAG edges connecting nodes |
//! | [`entities::pipeline_run`] | `pipeline_runs` | Execution history record |
//! | [`entities::run_node_result`] | `run_node_results` | Per-node execution results within a run |
//! | [`entities::pipeline_source_ref`] | `pipeline_source_refs` | Resource Manager: pipeline -> source refs |
//! | [`entities::pipeline_camera_ref`] | `pipeline_camera_refs` | Resource Manager: pipeline -> camera refs |
//! | [`entities::user`] | `users` | Local accounts; bcrypt-hashed passwords |
//! | [`entities::api_key`] | `api_keys` | Long-lived tokens; SHA-256-hashed, shown once on creation |
//! | [`entities::recording`] | `recordings` | One row per recorded chunk file |
//! | [`entities::daily_recording_coverage`] | `daily_recording_coverage` | Per-camera, per-day recording summary |
//! | [`entities::export_job`] | `export_jobs` | Background clip export jobs |
//! | [`entities::event`] | `events` | Persisted camera event history |
//! | [`entities::setting`] | `settings` | Runtime-editable settings |
//! | [`entities::tile_profile`] | `tile_profiles` | Named tile/grid layouts |
//! | [`entities::tile_formation`] | `tile_formations` | Grid slots within a tile profile |
//! | [`entities::tile_camera_binding`] | `tile_camera_bindings` | Which camera fills a slot, per site |
//! | [`entities::profile_site_assignment`] | `profile_site_assignments` | Tile profiles shared to other sites |

pub mod crypto;
pub mod entities;
pub mod migration;
pub mod repos;

pub use crypto::Crypto;
pub use migration::Migrator;
pub use repos::{
    recording::OpenChunk, user::verify_password, ApiKeyRepo, CameraRepo, ContactListRepo,
    ContactRepo, DailyRecordingCoverageRepo, DestinationRepo, EventsRepo, ExportJobRepo,
    PipelineRepo, PipelineRunRepo, RecordingRepo, SettingsRepo, SourceRepo, TileLayoutRepo,
    UserRepo,
};
