use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// A named tile/grid layout, owned by one site and optionally shared to
/// others via `profile_site_assignments`.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "tile_profiles")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub name: String,
    /// Opaque site identifier — no local FK; see the migration's doc comment.
    pub site_id: Uuid,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::tile_formation::Entity")]
    TileFormation,
    #[sea_orm(has_many = "super::profile_site_assignment::Entity")]
    ProfileSiteAssignment,
}

impl Related<super::tile_formation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TileFormation.def()
    }
}

impl Related<super::profile_site_assignment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProfileSiteAssignment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
