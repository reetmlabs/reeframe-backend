use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// A recorded motion/tamper/trigger occurrence for one camera: the persisted
/// history of what otherwise only flows transiently over the `EventBus`.
/// `event_type` is free text, mirroring `Event::event_type` in `vms-core`.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "events")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub camera_id: Uuid,
    pub event_type: String,
    pub payload: Json,
    pub occurred_at: DateTimeWithTimeZone,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::camera::Entity",
        from = "Column::CameraId",
        to = "super::camera::Column::Id"
    )]
    Camera,
}

impl Related<super::camera::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Camera.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
