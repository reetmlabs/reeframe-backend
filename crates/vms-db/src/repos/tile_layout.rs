//! Tile/matrix camera-layout profiles — moved from the FE's local sqlite
//! (`tile_profiles` / `tile_formations` / `tile_camera_bindings` /
//! `profile_site_assignments` in `AppDatabase.cpp`) onto the BE, so any
//! client (not just the desktop FE with a local sqlite file) can read and
//! write the same layout data.
//!
//! `site_id` throughout this module is an opaque identifier with no local
//! FK — Coordinator (or, today, the FE itself) is the sole owner of site
//! identity; this repo just stores whatever `site_id` a caller supplies.

use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    QueryFilter, QueryOrder,
};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::entities::{
    camera, profile_site_assignment, tile_camera_binding, tile_formation, tile_profile,
};

// -- Input types --

pub struct CreateTileProfile {
    pub name: String,
    pub site_id: Uuid,
}

pub struct CreateTileFormation {
    pub col: i32,
    pub row: i32,
    pub col_span: i32,
    pub row_span: i32,
}

pub struct UpdateTileFormation {
    pub col: Option<i32>,
    pub row: Option<i32>,
    pub col_span: Option<i32>,
    pub row_span: Option<i32>,
}

// -- Repository --

#[derive(Clone)]
pub struct TileLayoutRepo {
    db: DatabaseConnection,
}

impl TileLayoutRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    // -- Tile profiles --

    /// Profiles visible to `site_id`: the ones it owns, plus any shared to
    /// it via `profile_site_assignments` — mirrors the FE's own
    /// `loadTileProfilesForSite` UNION query.
    pub async fn list_profiles_for_site(
        &self,
        site_id: Uuid,
    ) -> Result<Vec<tile_profile::Model>, VmsError> {
        let mut profiles = tile_profile::Entity::find()
            .filter(tile_profile::Column::SiteId.eq(site_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        let assigned_ids: Vec<Uuid> = profile_site_assignment::Entity::find()
            .filter(profile_site_assignment::Column::SiteId.eq(site_id))
            .all(&self.db)
            .await
            .map_err(db_err)?
            .into_iter()
            .map(|a| a.profile_id)
            .collect();

        if !assigned_ids.is_empty() {
            let shared = tile_profile::Entity::find()
                .filter(tile_profile::Column::Id.is_in(assigned_ids))
                .all(&self.db)
                .await
                .map_err(db_err)?;
            for p in shared {
                if !profiles.iter().any(|existing| existing.id == p.id) {
                    profiles.push(p);
                }
            }
        }
        Ok(profiles)
    }

    pub async fn get_profile(&self, id: Uuid) -> Result<Option<tile_profile::Model>, VmsError> {
        tile_profile::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn create_profile(
        &self,
        input: CreateTileProfile,
    ) -> Result<tile_profile::Model, VmsError> {
        let ts = now();
        tile_profile::ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            site_id: Set(input.site_id),
            created_at: Set(ts),
            updated_at: Set(ts),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    pub async fn rename_profile(
        &self,
        id: Uuid,
        name: String,
    ) -> Result<tile_profile::Model, VmsError> {
        let existing = self.require_profile(id).await?;
        let mut active: tile_profile::ActiveModel = existing.into();
        active.name = Set(name);
        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    /// Cascades to this profile's formations, camera bindings (transitively,
    /// via each formation), and site assignments.
    pub async fn delete_profile(&self, id: Uuid) -> Result<(), VmsError> {
        let existing = self.require_profile(id).await?;
        existing.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    // -- Profile <-> site assignments --

    /// Idempotent — matches the FE's `INSERT OR IGNORE`.
    pub async fn assign_profile_to_site(
        &self,
        profile_id: Uuid,
        site_id: Uuid,
    ) -> Result<(), VmsError> {
        self.require_profile(profile_id).await?;

        let existing = profile_site_assignment::Entity::find()
            .filter(profile_site_assignment::Column::ProfileId.eq(profile_id))
            .filter(profile_site_assignment::Column::SiteId.eq(site_id))
            .one(&self.db)
            .await
            .map_err(db_err)?;
        if existing.is_some() {
            return Ok(());
        }

        profile_site_assignment::ActiveModel {
            profile_id: Set(profile_id),
            site_id: Set(site_id),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    // -- Tile formations --

    pub async fn list_formations(
        &self,
        profile_id: Uuid,
    ) -> Result<Vec<tile_formation::Model>, VmsError> {
        self.require_profile(profile_id).await?;
        tile_formation::Entity::find()
            .filter(tile_formation::Column::ProfileId.eq(profile_id))
            .order_by_asc(tile_formation::Column::CreatedAt)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn get_formation(&self, id: Uuid) -> Result<Option<tile_formation::Model>, VmsError> {
        tile_formation::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn create_formation(
        &self,
        profile_id: Uuid,
        input: CreateTileFormation,
    ) -> Result<tile_formation::Model, VmsError> {
        self.require_profile(profile_id).await?;
        tile_formation::ActiveModel {
            id: Set(Uuid::new_v4()),
            profile_id: Set(profile_id),
            grid_col: Set(input.col),
            grid_row: Set(input.row),
            col_span: Set(input.col_span),
            row_span: Set(input.row_span),
            created_at: Set(now()),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    pub async fn update_formation(
        &self,
        id: Uuid,
        input: UpdateTileFormation,
    ) -> Result<tile_formation::Model, VmsError> {
        let existing = tile_formation::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::TileFormationNotFound(id))?;

        let mut active: tile_formation::ActiveModel = existing.into();
        if let Some(v) = input.col {
            active.grid_col = Set(v);
        }
        if let Some(v) = input.row {
            active.grid_row = Set(v);
        }
        if let Some(v) = input.col_span {
            active.col_span = Set(v);
        }
        if let Some(v) = input.row_span {
            active.row_span = Set(v);
        }
        active.update(&self.db).await.map_err(db_err)
    }

    /// Cascades to any camera bindings for this tile (`ON DELETE CASCADE`).
    pub async fn delete_formation(&self, id: Uuid) -> Result<(), VmsError> {
        let existing = tile_formation::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::TileFormationNotFound(id))?;
        existing.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    // -- Camera bindings --

    pub async fn list_bindings(
        &self,
        profile_id: Uuid,
        site_id: Uuid,
    ) -> Result<Vec<tile_camera_binding::Model>, VmsError> {
        self.require_profile(profile_id).await?;
        tile_camera_binding::Entity::find()
            .filter(tile_camera_binding::Column::ProfileId.eq(profile_id))
            .filter(tile_camera_binding::Column::SiteId.eq(site_id))
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Upsert — matches the FE's `INSERT OR REPLACE`. `tile_id` must be a
    /// formation belonging to `profile_id`, checked here rather than trusted
    /// from the caller (same reasoning as `pipeline_nodes`' parent-child
    /// membership check).
    pub async fn set_binding(
        &self,
        profile_id: Uuid,
        tile_id: Uuid,
        site_id: Uuid,
        camera_id: Uuid,
    ) -> Result<tile_camera_binding::Model, VmsError> {
        self.require_formation_in_profile(profile_id, tile_id)
            .await?;
        require_camera_exists(&self.db, camera_id).await?;

        let existing = tile_camera_binding::Entity::find()
            .filter(tile_camera_binding::Column::ProfileId.eq(profile_id))
            .filter(tile_camera_binding::Column::TileId.eq(tile_id))
            .filter(tile_camera_binding::Column::SiteId.eq(site_id))
            .one(&self.db)
            .await
            .map_err(db_err)?;

        if let Some(existing) = existing {
            let mut active: tile_camera_binding::ActiveModel = existing.into();
            active.camera_id = Set(camera_id);
            active.update(&self.db).await.map_err(db_err)
        } else {
            tile_camera_binding::ActiveModel {
                profile_id: Set(profile_id),
                tile_id: Set(tile_id),
                site_id: Set(site_id),
                camera_id: Set(camera_id),
            }
            .insert(&self.db)
            .await
            .map_err(db_err)
        }
    }

    /// Clears a binding. Absence of a row *is* "unassigned" — no sentinel
    /// value needed, matching how a deleted camera cascades this row away.
    pub async fn clear_binding(
        &self,
        profile_id: Uuid,
        tile_id: Uuid,
        site_id: Uuid,
    ) -> Result<(), VmsError> {
        tile_camera_binding::Entity::delete_many()
            .filter(tile_camera_binding::Column::ProfileId.eq(profile_id))
            .filter(tile_camera_binding::Column::TileId.eq(tile_id))
            .filter(tile_camera_binding::Column::SiteId.eq(site_id))
            .exec(&self.db)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    // -- Helpers --

    async fn require_profile(&self, id: Uuid) -> Result<tile_profile::Model, VmsError> {
        tile_profile::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::TileProfileNotFound(id))
    }

    async fn require_formation_in_profile(
        &self,
        profile_id: Uuid,
        tile_id: Uuid,
    ) -> Result<(), VmsError> {
        let belongs = tile_formation::Entity::find_by_id(tile_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .is_some_and(|f| f.profile_id == profile_id);
        if !belongs {
            return Err(VmsError::TileFormationNotFound(tile_id));
        }
        Ok(())
    }
}

async fn require_camera_exists(db: &DatabaseConnection, camera_id: Uuid) -> Result<(), VmsError> {
    camera::Entity::find_by_id(camera_id)
        .one(db)
        .await
        .map_err(db_err)?
        .map(|_| ())
        .ok_or(VmsError::CameraNotFound(camera_id))
}

// -- Tests --

#[cfg(test)]
mod tests {
    use sea_orm::PaginatorTrait;
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::{
        crypto::Crypto,
        entities::camera::RingBufferStorage,
        migration::Migrator,
        repos::camera::{CameraRepo, CreateCamera},
    };

    async fn test_db() -> DatabaseConnection {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db
    }

    async fn create_camera(db: &DatabaseConnection, name: &str) -> Uuid {
        let repo = CameraRepo::new(db.clone(), Crypto::from_key([0u8; 32]));
        repo.create(CreateCamera {
            name: name.into(),
            description: None,
            rtsp_url: "rtsp://example.invalid/stream".into(),
            sub_rtsp_url: None,
            manufacturer: None,
            model: None,
            username: None,
            password: None,
            extra_config: serde_json::json!({}),
            ring_buffer_duration_secs: 300,
            ring_buffer_storage: RingBufferStorage::Memory,
            enabled: true,
            motion_detection_enabled: true,
            thumbnails_enabled: false,
        })
        .await
        .unwrap()
        .id
    }

    #[tokio::test]
    async fn profile_crud_round_trips() {
        let db = test_db().await;
        let repo = TileLayoutRepo::new(db);
        let site_id = Uuid::new_v4();

        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Default".into(),
                site_id,
            })
            .await
            .unwrap();
        assert_eq!(
            repo.get_profile(profile.id).await.unwrap().unwrap().name,
            "Default"
        );

        let renamed = repo
            .rename_profile(profile.id, "Renamed".into())
            .await
            .unwrap();
        assert_eq!(renamed.name, "Renamed");

        repo.delete_profile(profile.id).await.unwrap();
        assert!(repo.get_profile(profile.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn formation_crud_round_trips() {
        let db = test_db().await;
        let repo = TileLayoutRepo::new(db);
        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Default".into(),
                site_id: Uuid::new_v4(),
            })
            .await
            .unwrap();

        let formation = repo
            .create_formation(
                profile.id,
                CreateTileFormation {
                    col: 0,
                    row: 0,
                    col_span: 4,
                    row_span: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(repo.list_formations(profile.id).await.unwrap().len(), 1);

        let moved = repo
            .update_formation(
                formation.id,
                UpdateTileFormation {
                    col: Some(4),
                    row: Some(0),
                    col_span: None,
                    row_span: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(moved.grid_col, 4);
        assert_eq!(moved.col_span, 4); // untouched field preserved

        repo.delete_formation(formation.id).await.unwrap();
        assert!(repo.list_formations(profile.id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn binding_upsert_and_clear_round_trips() {
        let db = test_db().await;
        let camera_a = create_camera(&db, "cam-a").await;
        let camera_b = create_camera(&db, "cam-b").await;
        let repo = TileLayoutRepo::new(db);
        let site_id = Uuid::new_v4();
        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Default".into(),
                site_id,
            })
            .await
            .unwrap();
        let formation = repo
            .create_formation(
                profile.id,
                CreateTileFormation {
                    col: 0,
                    row: 0,
                    col_span: 1,
                    row_span: 1,
                },
            )
            .await
            .unwrap();

        repo.set_binding(profile.id, formation.id, site_id, camera_a)
            .await
            .unwrap();
        let bindings = repo.list_bindings(profile.id, site_id).await.unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].camera_id, camera_a);

        // Upsert: re-binding the same (profile, tile, site) replaces the
        // camera rather than adding a second row.
        repo.set_binding(profile.id, formation.id, site_id, camera_b)
            .await
            .unwrap();
        let bindings = repo.list_bindings(profile.id, site_id).await.unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].camera_id, camera_b);

        repo.clear_binding(profile.id, formation.id, site_id)
            .await
            .unwrap();
        assert!(repo
            .list_bindings(profile.id, site_id)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn a_shared_formation_has_independent_camera_bindings_per_site() {
        let db = test_db().await;
        let camera_a = create_camera(&db, "cam-a").await;
        let camera_b = create_camera(&db, "cam-b").await;
        let repo = TileLayoutRepo::new(db);
        let (site_owner, site_shared, site_uninvolved) =
            (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());

        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Shared".into(),
                site_id: site_owner,
            })
            .await
            .unwrap();
        let formation = repo
            .create_formation(
                profile.id,
                CreateTileFormation {
                    col: 0,
                    row: 0,
                    col_span: 1,
                    row_span: 1,
                },
            )
            .await
            .unwrap();
        repo.assign_profile_to_site(profile.id, site_shared)
            .await
            .unwrap();

        // The profile — and its one formation — is visible to both the
        // owning site and the site it was shared to, but not a third,
        // uninvolved site.
        let owner_profiles = repo.list_profiles_for_site(site_owner).await.unwrap();
        let shared_profiles = repo.list_profiles_for_site(site_shared).await.unwrap();
        let uninvolved_profiles = repo.list_profiles_for_site(site_uninvolved).await.unwrap();
        assert!(owner_profiles.iter().any(|p| p.id == profile.id));
        assert!(shared_profiles.iter().any(|p| p.id == profile.id));
        assert!(!uninvolved_profiles.iter().any(|p| p.id == profile.id));

        // Same formation, different camera per site.
        repo.set_binding(profile.id, formation.id, site_owner, camera_a)
            .await
            .unwrap();
        repo.set_binding(profile.id, formation.id, site_shared, camera_b)
            .await
            .unwrap();

        let owner_bindings = repo.list_bindings(profile.id, site_owner).await.unwrap();
        let shared_bindings = repo.list_bindings(profile.id, site_shared).await.unwrap();
        assert_eq!(owner_bindings.len(), 1);
        assert_eq!(owner_bindings[0].camera_id, camera_a);
        assert_eq!(shared_bindings.len(), 1);
        assert_eq!(shared_bindings[0].camera_id, camera_b);

        // The formation itself is still a single shared row, not duplicated
        // per site.
        assert_eq!(repo.list_formations(profile.id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn deleting_a_camera_clears_only_that_sites_binding() {
        let db = test_db().await;
        let camera_a = create_camera(&db, "cam-a").await;
        let camera_b = create_camera(&db, "cam-b").await;
        let repo = TileLayoutRepo::new(db.clone());
        let (site_a, site_b) = (Uuid::new_v4(), Uuid::new_v4());

        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Shared".into(),
                site_id: site_a,
            })
            .await
            .unwrap();
        let formation = repo
            .create_formation(
                profile.id,
                CreateTileFormation {
                    col: 0,
                    row: 0,
                    col_span: 1,
                    row_span: 1,
                },
            )
            .await
            .unwrap();
        repo.assign_profile_to_site(profile.id, site_b)
            .await
            .unwrap();
        repo.set_binding(profile.id, formation.id, site_a, camera_a)
            .await
            .unwrap();
        repo.set_binding(profile.id, formation.id, site_b, camera_b)
            .await
            .unwrap();

        camera::Entity::delete_by_id(camera_a)
            .exec(&db)
            .await
            .unwrap();

        // Only site_a's binding (the one pointing at the deleted camera)
        // is gone — site_b's binding, and the shared formation itself,
        // are untouched. This is deliberately stricter than the FE's own
        // sweep, which deletes the whole formation (breaking every site
        // sharing it) whenever any one site's bound camera disappears.
        assert!(repo
            .list_bindings(profile.id, site_a)
            .await
            .unwrap()
            .is_empty());
        let site_b_bindings = repo.list_bindings(profile.id, site_b).await.unwrap();
        assert_eq!(site_b_bindings.len(), 1);
        assert_eq!(site_b_bindings[0].camera_id, camera_b);
        assert!(repo.get_formation(formation.id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn assigning_a_profile_to_the_same_site_twice_is_idempotent() {
        let db = test_db().await;
        let repo = TileLayoutRepo::new(db);
        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Default".into(),
                site_id: Uuid::new_v4(),
            })
            .await
            .unwrap();
        let site_id = Uuid::new_v4();

        repo.assign_profile_to_site(profile.id, site_id)
            .await
            .unwrap();
        repo.assign_profile_to_site(profile.id, site_id)
            .await
            .unwrap();

        let count = profile_site_assignment::Entity::find()
            .filter(profile_site_assignment::Column::ProfileId.eq(profile.id))
            .filter(profile_site_assignment::Column::SiteId.eq(site_id))
            .count(repo_db(&repo))
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn deleting_a_profile_cascades_formations_bindings_and_assignments() {
        let db = test_db().await;
        let camera_a = create_camera(&db, "cam-a").await;
        let repo = TileLayoutRepo::new(db);
        let site_id = Uuid::new_v4();
        let other_site = Uuid::new_v4();

        let profile = repo
            .create_profile(CreateTileProfile {
                name: "Default".into(),
                site_id,
            })
            .await
            .unwrap();
        let formation = repo
            .create_formation(
                profile.id,
                CreateTileFormation {
                    col: 0,
                    row: 0,
                    col_span: 1,
                    row_span: 1,
                },
            )
            .await
            .unwrap();
        repo.assign_profile_to_site(profile.id, other_site)
            .await
            .unwrap();
        repo.set_binding(profile.id, formation.id, site_id, camera_a)
            .await
            .unwrap();

        repo.delete_profile(profile.id).await.unwrap();

        assert!(repo.get_formation(formation.id).await.unwrap().is_none());
        let remaining_assignments = profile_site_assignment::Entity::find()
            .filter(profile_site_assignment::Column::ProfileId.eq(profile.id))
            .count(repo_db(&repo))
            .await
            .unwrap();
        assert_eq!(remaining_assignments, 0);
    }

    /// Test-only accessor — `TileLayoutRepo::db` is private, and these two
    /// tests need it to assert on rows the repo's own API has no "count"
    /// method for.
    fn repo_db(repo: &TileLayoutRepo) -> &DatabaseConnection {
        &repo.db
    }
}
