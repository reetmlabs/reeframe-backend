use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "contacts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    /// Telegram chat ID (positive for users, negative for groups/channels).
    pub telegram_chat_id: Option<i64>,
    /// Extra delivery handles or metadata.
    pub extra: Json,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::contact_list_member::Entity")]
    ContactListMember,
}

impl Related<super::contact_list_member::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ContactListMember.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
