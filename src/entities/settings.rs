use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "settings")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub recording_chunk_duration_mins: i32,
    pub timezone: String,
    pub ntp_server: String,
    pub storage_path: String,
    pub reconnect_interval_secs: i32,
    pub gst_latency_ms: i32,
    pub gst_buffering_ms: i32,
    pub reserved_disk_space_mb: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
