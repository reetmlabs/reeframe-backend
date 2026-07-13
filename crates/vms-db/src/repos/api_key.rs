use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::{rngs::OsRng, RngCore};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    QueryFilter,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::entities::{
    api_key::{self, ActiveModel},
    user,
};

/// Length, in raw random bytes, of a generated key (before hex encoding and
/// the `rfk_` prefix). 32 bytes gives 256 bits of entropy — plenty for a
/// bearer credential that isn't subject to online guessing (the hash lookup
/// is by exact match, not comparison-per-attempt).
const KEY_BYTES: usize = 32;

// -- Input / output types --

pub struct CreateApiKey {
    pub user_id: Uuid,
    pub name: String,
}

pub struct CreatedApiKey {
    pub model: api_key::Model,
    /// The raw key. Only ever available here, at creation time — it is
    /// never stored, so it cannot be shown again after this call returns.
    pub raw_key: String,
}

// -- Repository --

#[derive(Clone)]
pub struct ApiKeyRepo {
    db: DatabaseConnection,
}

impl ApiKeyRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create(&self, input: CreateApiKey) -> Result<CreatedApiKey, VmsError> {
        let raw_key = generate_raw_key();
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            user_id: Set(input.user_id),
            key_hash: Set(hash_key(&raw_key)),
            name: Set(input.name),
            last_used: Set(None),
            created_at: Set(now()),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;

        Ok(CreatedApiKey { model, raw_key })
    }

    pub async fn list_for_user(&self, user_id: Uuid) -> Result<Vec<api_key::Model>, VmsError> {
        api_key::Entity::find()
            .filter(api_key::Column::UserId.eq(user_id))
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Revoke a key. Errors with [`VmsError::ApiKeyNotFound`] if `key_id`
    /// doesn't exist *or* doesn't belong to `user_id` — the caller can't
    /// distinguish "not yours" from "doesn't exist" from the outside.
    pub async fn delete(&self, user_id: Uuid, key_id: Uuid) -> Result<(), VmsError> {
        let key = api_key::Entity::find_by_id(key_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .filter(|k| k.user_id == user_id)
            .ok_or(VmsError::ApiKeyNotFound(key_id))?;
        key.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    /// Resolve a raw key to the user it belongs to, bumping `last_used` on a
    /// match. Returns `Ok(None)` for an unknown or already-revoked key —
    /// this is an expected outcome for a bad credential, not an error.
    pub async fn verify_and_touch(&self, raw_key: &str) -> Result<Option<user::Model>, VmsError> {
        let Some(key) = api_key::Entity::find()
            .filter(api_key::Column::KeyHash.eq(hash_key(raw_key)))
            .one(&self.db)
            .await
            .map_err(db_err)?
        else {
            return Ok(None);
        };

        let user_id = key.user_id;
        let mut active: ActiveModel = key.into();
        active.last_used = Set(Some(now()));
        active.update(&self.db).await.map_err(db_err)?;

        user::Entity::find_by_id(user_id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }
}

// -- Key generation / hashing --

fn generate_raw_key() -> String {
    let mut bytes = [0u8; KEY_BYTES];
    OsRng.fill_bytes(&mut bytes);
    format!("rfk_{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn hash_key(raw_key: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(raw_key.as_bytes()))
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_keys_are_unique_and_prefixed() {
        let a = generate_raw_key();
        let b = generate_raw_key();
        assert_ne!(a, b);
        assert!(a.starts_with("rfk_"));
    }

    #[test]
    fn hash_is_deterministic() {
        let key = generate_raw_key();
        assert_eq!(hash_key(&key), hash_key(&key));
    }

    #[test]
    fn different_keys_hash_differently() {
        assert_ne!(hash_key(&generate_raw_key()), hash_key(&generate_raw_key()));
    }
}
