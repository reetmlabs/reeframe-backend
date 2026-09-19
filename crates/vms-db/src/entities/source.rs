use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    #[sea_orm(string_value = "mqtt")]
    Mqtt,
    #[sea_orm(string_value = "webhook")]
    Webhook,
    #[sea_orm(string_value = "api_poll")]
    #[serde(alias = "poller")]
    ApiPoll,
    #[sea_orm(string_value = "ha_websocket")]
    #[serde(alias = "homeassistant")]
    HaWebsocket,
    #[sea_orm(string_value = "file_watcher")]
    #[serde(alias = "filewatcher")]
    FileWatcher,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "sources")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    /// Discriminator for the adapter; determines how `config` is interpreted.
    #[sea_orm(column_name = "type")]
    pub source_type: SourceType,
    /// Adapter-specific connection config. Credential fields inside are
    /// AES-256-GCM encrypted with the `enc:v1:` prefix.
    pub config: Json,
    pub enabled: bool,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::pipeline_trigger::Entity")]
    PipelineTrigger,
    #[sea_orm(has_many = "super::pipeline_source_ref::Entity")]
    PipelineSourceRef,
}

impl Related<super::pipeline_trigger::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineTrigger.def()
    }
}

impl Related<super::pipeline_source_ref::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineSourceRef.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
