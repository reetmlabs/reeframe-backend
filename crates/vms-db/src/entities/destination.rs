use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum DestinationType {
    #[sea_orm(string_value = "s3")]
    S3,
    #[sea_orm(string_value = "sftp")]
    Sftp,
    #[sea_orm(string_value = "smb")]
    Smb,
    #[sea_orm(string_value = "local")]
    Local,
    #[sea_orm(string_value = "telegram")]
    Telegram,
    #[sea_orm(string_value = "email")]
    Email,
    #[sea_orm(string_value = "slack")]
    Slack,
    #[sea_orm(string_value = "webhook")]
    Webhook,
    #[sea_orm(string_value = "sms")]
    Sms,
    #[sea_orm(string_value = "push")]
    Push,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "destinations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    /// Discriminator for the transport adapter.
    #[sea_orm(column_name = "type")]
    pub dest_type: DestinationType,
    /// Adapter-specific connection config. Credential fields inside are
    /// AES-256-GCM encrypted with the `enc:v1:` prefix.
    pub config: Json,
    pub enabled: bool,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::pipeline_node::Entity")]
    PipelineNode,
}

impl Related<super::pipeline_node::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineNode.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
