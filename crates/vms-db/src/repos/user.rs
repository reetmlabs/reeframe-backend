use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    PaginatorTrait, QueryFilter,
};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::entities::user::{self, ActiveModel, UserRole};

/// bcrypt work factor. Matches the Phase 2 multi-user design note in `RoadmapBE/roadmap.md`.
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
        u.delete(&self.db).await.map_err(db_err)?;
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
    use super::*;

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
}
