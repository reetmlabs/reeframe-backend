use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

/// Extends a `tile_profile`'s visibility to a site beyond the one that owns
/// it (`tile_profiles.site_id`) — this is the "shared across multiple sites"
/// half of the profile-sharing model.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "profile_site_assignments")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub profile_id: Uuid,
    /// Opaque site identifier — no local FK; see the migration's doc comment.
    #[sea_orm(primary_key, auto_increment = false)]
    pub site_id: Uuid,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::tile_profile::Entity",
        from = "Column::ProfileId",
        to = "super::tile_profile::Column::Id"
    )]
    TileProfile,
}

impl Related<super::tile_profile::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TileProfile.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
