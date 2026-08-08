use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    PaginatorTrait, QueryFilter,
};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::entities::user::{self, ActiveModel, UserRole};

/// bcrypt work factor.
const BCRYPT_COST: u32 = 12;

// -- Input types --

pub struct CreateUser {
    pub username: String,
    /// Plaintext — hashed with bcrypt before storage.
    pub password: String,
    pub role: UserRole,
}

pub struct UpdateUser {
    pub username: Option<String>,
    /// Plaintext — hashed with bcrypt before storage, if supplied.
    pub password: Option<String>,
    pub role: Option<UserRole>,
    pub enabled: Option<bool>,
}

// -- Repository --

#[derive(Clone)]
pub struct UserRepo {
    db: DatabaseConnection,
}

impl UserRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create(&self, input: CreateUser) -> Result<user::Model, VmsError> {
        let password_hash = hash_password(&input.password)?;
        let ts = now();
        ActiveModel {
            id: Set(Uuid::new_v4()),
            username: Set(input.username),
            password_hash: Set(password_hash),
            role: Set(input.role),
            enabled: Set(true),
            created_at: Set(ts),
            updated_at: Set(ts),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<user::Model>, VmsError> {
        user::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn get_by_username(&self, username: &str) -> Result<Option<user::Model>, VmsError> {
        user::Entity::find()
            .filter(user::Column::Username.eq(username))
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn list(&self) -> Result<Vec<user::Model>, VmsError> {
        user::Entity::find().all(&self.db).await.map_err(db_err)
    }

    /// Total number of users. Used by `POST /auth/setup` to refuse creating
    /// a second first-run admin once one already exists.
    pub async fn count(&self) -> Result<u64, VmsError> {
        user::Entity::find().count(&self.db).await.map_err(db_err)
    }

    pub async fn update(&self, id: Uuid, input: UpdateUser) -> Result<user::Model, VmsError> {
        let u = user::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::UserNotFound(id))?;

        let is_enabled_admin = u.role == UserRole::Admin && u.enabled;
        let would_lose_admin_status = input.enabled == Some(false)
            || matches!(&input.role, Some(role) if *role != UserRole::Admin);
        if is_enabled_admin && would_lose_admin_status {
            self.ensure_not_last_enabled_admin().await?;
        }

        let mut active: ActiveModel = u.into();

        if let Some(v) = input.username {
            active.username = Set(v);
        }
        if let Some(password) = input.password {
            active.password_hash = Set(hash_password(&password)?);
        }
        if let Some(v) = input.role {
            active.role = Set(v);
        }
        if let Some(v) = input.enabled {
            active.enabled = Set(v);
        }

        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        let u = user::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::UserNotFound(id))?;
        if u.role == UserRole::Admin && u.enabled {
            self.ensure_not_last_enabled_admin().await?;
        }
        u.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    /// The "local break-glass account" guarantee: at least one enabled local
    /// admin must always exist, so direct local/offline access keeps working
    /// unconditionally. Called by [`Self::update`] and [`Self::delete`]
    /// before applying any change that would take the last enabled admin
    /// below that floor — enforced here rather than only in a route handler
    /// so no future caller can accidentally bypass it.
    async fn ensure_not_last_enabled_admin(&self) -> Result<(), VmsError> {
        let enabled_admins = user::Entity::find()
            .filter(user::Column::Role.eq(UserRole::Admin))
            .filter(user::Column::Enabled.eq(true))
            .count(&self.db)
            .await
            .map_err(db_err)?;
        if enabled_admins <= 1 {
            return Err(VmsError::Conflict(
                "cannot delete or disable the last remaining local admin".into(),
            ));
        }
        Ok(())
    }
}

// -- Password hashing --

fn hash_password(password: &str) -> Result<String, VmsError> {
    bcrypt::hash(password, BCRYPT_COST).map_err(|e| VmsError::Encryption(format!("bcrypt: {e}")))
}

/// Verify a plaintext password against a bcrypt hash.
///
/// Returns `false` rather than an error on a malformed hash or a mismatch —
/// callers (login handlers) should treat both the same way: reject the
/// attempt, without distinguishing "bad password" from "corrupt hash" in the
/// response.
pub fn verify_password(password: &str, hash: &str) -> bool {
    bcrypt::verify(password, hash).unwrap_or(false)
}

// -- Tests --

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::migration::Migrator;

    #[test]
    fn verify_password_accepts_matching_password() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(verify_password("correct horse battery staple", &hash));
    }

    #[test]
    fn verify_password_rejects_wrong_password() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(!verify_password("wrong password", &hash));
    }

    #[test]
    fn verify_password_rejects_malformed_hash_instead_of_panicking() {
        assert!(!verify_password("anything", "not-a-bcrypt-hash"));
    }

    // -- Local-admin guarantee (Step 14b) --

    async fn test_repo() -> UserRepo {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        UserRepo::new(db)
    }

    async fn create_admin(repo: &UserRepo, username: &str) -> user::Model {
        repo.create(CreateUser {
            username: username.into(),
            password: "irrelevant-password".into(),
            role: UserRole::Admin,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn deleting_the_sole_remaining_admin_is_rejected() {
        let repo = test_repo().await;
        let admin = create_admin(&repo, "admin").await;

        let result = repo.delete(admin.id).await;

        assert!(matches!(result, Err(VmsError::Conflict(_))));
        assert!(repo.get(admin.id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn disabling_the_sole_remaining_admin_is_rejected() {
        let repo = test_repo().await;
        let admin = create_admin(&repo, "admin").await;

        let result = repo
            .update(
                admin.id,
                UpdateUser {
                    username: None,
                    password: None,
                    role: None,
                    enabled: Some(false),
                },
            )
            .await;

        assert!(matches!(result, Err(VmsError::Conflict(_))));
        assert!(repo.get(admin.id).await.unwrap().unwrap().enabled);
    }

    #[tokio::test]
    async fn demoting_the_sole_remaining_admin_to_viewer_is_rejected() {
        let repo = test_repo().await;
        let admin = create_admin(&repo, "admin").await;

        let result = repo
            .update(
                admin.id,
                UpdateUser {
                    username: None,
                    password: None,
                    role: Some(UserRole::Viewer),
                    enabled: None,
                },
            )
            .await;

        assert!(matches!(result, Err(VmsError::Conflict(_))));
        assert_eq!(
            repo.get(admin.id).await.unwrap().unwrap().role,
            UserRole::Admin
        );
    }

    #[tokio::test]
    async fn deleting_an_admin_succeeds_when_another_enabled_admin_remains() {
        let repo = test_repo().await;
        let first = create_admin(&repo, "admin-1").await;
        let _second = create_admin(&repo, "admin-2").await;

        repo.delete(first.id).await.unwrap();

        assert!(repo.get(first.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn deleting_an_already_disabled_admin_is_never_blocked() {
        let repo = test_repo().await;
        let _first = create_admin(&repo, "admin-1").await;
        let second = create_admin(&repo, "admin-2").await;
        repo.update(
            second.id,
            UpdateUser {
                username: None,
                password: None,
                role: None,
                enabled: Some(false),
            },
        )
        .await
        .unwrap();

        // `second` is disabled and no longer counts toward the floor, so
        // deleting it must not be blocked even though `first` is the only
        // *enabled* admin left.
        repo.delete(second.id).await.unwrap();

        assert!(repo.get(second.id).await.unwrap().is_none());
    }
}
