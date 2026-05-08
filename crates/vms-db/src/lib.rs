//! `vms-db` — SeaORM entities and AES-256-GCM credential encryption.
//!
//! # Module overview
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`entities`] | SeaORM `Model`, `Entity`, `ActiveModel`, and `Relation` types for all 14 database tables |
//! | [`crypto`] | [`Crypto`] — AES-256-GCM encrypt/decrypt for credential fields stored in JSONB columns |
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
//! | [`entities::pipeline_source_ref`] | `pipeline_source_refs` | Resource Manager: pipeline → source refs |
//! | [`entities::pipeline_camera_ref`] | `pipeline_camera_refs` | Resource Manager: pipeline → camera refs |

pub mod crypto;
pub mod entities;

pub use crypto::Crypto;
