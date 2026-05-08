use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "contact_lists")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::contact_list_member::Entity")]
    ContactListMember,
    #[sea_orm(has_many = "super::pipeline_node::Entity")]
    PipelineNode,
}

impl Related<super::contact_list_member::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ContactListMember.def()
    }
}

impl Related<super::pipeline_node::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineNode.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
