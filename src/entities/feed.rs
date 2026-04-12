use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "feeds")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub parameters: Option<String>, // Store as JSON string or similar
    pub recording_quality: Option<String>, // "high", "medium", "low" or specific encoder settings
    pub ai_recording_duration_secs: Option<i32>,
    pub restream_enabled: bool,
    pub restream_port: Option<i32>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
