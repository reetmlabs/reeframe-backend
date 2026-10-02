use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// A grid slot within a `tile_profile`: position and span only. Not
/// site-scoped, so the same formation is shared by every site the profile is
/// assigned to. Which camera (if any) fills the slot for a given site lives in
/// `tile_camera_bindings`, keyed separately per site.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "tile_formations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub profile_id: Uuid,
    /// Named `grid_col`/`grid_row` instead of `col`/`row` because SeaORM's
    /// `DeriveEntityModel` names its query-result variable `row` while building
    /// `Model`. A field also named `row` shadows it and breaks codegen for every
    /// field after it.
    pub grid_col: i32,
    pub grid_row: i32,
    pub col_span: i32,
    pub row_span: i32,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::tile_profile::Entity",
        from = "Column::ProfileId",
        to = "super::tile_profile::Column::Id"
    )]
    TileProfile,
    #[sea_orm(has_many = "super::tile_camera_binding::Entity")]
    TileCameraBinding,
}

impl Related<super::tile_profile::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TileProfile.def()
    }
}

impl Related<super::tile_camera_binding::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TileCameraBinding.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
