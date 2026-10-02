use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Which camera fills a `tile_formation`'s slot for one specific site.
/// Composite-keyed so the same shared formation can hold a different camera
/// per site; row absence means "unassigned" (no sentinel value needed) and a
/// deleted camera cascades to remove just this row, never the formation
/// itself or any other site's binding.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "tile_camera_bindings")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub profile_id: Uuid,
    #[sea_orm(primary_key, auto_increment = false)]
    pub tile_id: Uuid,
    /// Opaque site identifier with no local FK; see the migration's comment.
    #[sea_orm(primary_key, auto_increment = false)]
    pub site_id: Uuid,
    pub camera_id: Uuid,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::tile_profile::Entity",
        from = "Column::ProfileId",
        to = "super::tile_profile::Column::Id"
    )]
    TileProfile,
    #[sea_orm(
        belongs_to = "super::tile_formation::Entity",
        from = "Column::TileId",
        to = "super::tile_formation::Column::Id"
    )]
    TileFormation,
    #[sea_orm(
        belongs_to = "super::camera::Entity",
        from = "Column::CameraId",
        to = "super::camera::Column::Id"
    )]
    Camera,
}

impl Related<super::tile_profile::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TileProfile.def()
    }
}

impl Related<super::tile_formation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TileFormation.def()
    }
}

impl Related<super::camera::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Camera.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
