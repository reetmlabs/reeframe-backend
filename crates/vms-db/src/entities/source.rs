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

#[cfg(test)]
mod source_type_alias_tests {
    use super::SourceType;

    #[test]
    fn accepts_the_frontend_spellings() {
        assert_eq!(
            serde_json::from_str::<SourceType>("\"homeassistant\"").unwrap(),
            SourceType::HaWebsocket
        );
        assert_eq!(
            serde_json::from_str::<SourceType>("\"poller\"").unwrap(),
            SourceType::ApiPoll
        );
        assert_eq!(
            serde_json::from_str::<SourceType>("\"filewatcher\"").unwrap(),
            SourceType::FileWatcher
        );
    }

    #[test]
    fn still_accepts_the_canonical_spellings() {
        assert_eq!(
            serde_json::from_str::<SourceType>("\"ha_websocket\"").unwrap(),
            SourceType::HaWebsocket
        );
        assert_eq!(
            serde_json::from_str::<SourceType>("\"api_poll\"").unwrap(),
            SourceType::ApiPoll
        );
        assert_eq!(
            serde_json::from_str::<SourceType>("\"file_watcher\"").unwrap(),
            SourceType::FileWatcher
        );
    }

    #[test]
    fn serializes_back_to_the_canonical_spelling() {
        assert_eq!(
            serde_json::to_string(&SourceType::HaWebsocket).unwrap(),
            "\"ha_websocket\""
        );
        assert_eq!(
            serde_json::to_string(&SourceType::ApiPoll).unwrap(),
            "\"api_poll\""
        );
        assert_eq!(
            serde_json::to_string(&SourceType::FileWatcher).unwrap(),
            "\"file_watcher\""
        );
    }
}
