use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Junction table linking contacts to contact lists.
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "contact_list_members")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub contact_list_id: Uuid,
    #[sea_orm(primary_key, auto_increment = false)]
    pub contact_id: Uuid,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::contact_list::Entity",
        from = "Column::ContactListId",
        to = "super::contact_list::Column::Id"
    )]
    ContactList,
    #[sea_orm(
        belongs_to = "super::contact::Entity",
        from = "Column::ContactId",
        to = "super::contact::Column::Id"
    )]
    Contact,
}

impl Related<super::contact_list::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ContactList.def()
    }
}

impl Related<super::contact::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Contact.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
