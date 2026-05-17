use sea_orm::{ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait, ModelTrait};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, decrypt_config, encrypt_config, now};
use crate::{
    crypto::Crypto,
    entities::destination::{self, ActiveModel, DestinationType},
};

// ── Input types ───────────────────────────────────────────────────────────────

pub struct CreateDestination {
    pub name: String,
    pub description: Option<String>,
    pub dest_type: DestinationType,
    /// Adapter config with credential values in **plaintext**.
    /// The repo encrypts credential fields before storage.
    pub config: serde_json::Value,
    pub enabled: bool,
}

pub struct UpdateDestination {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub dest_type: Option<DestinationType>,
    /// Full config replacement. Credential fields should be in **plaintext**.
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

// ── Repository ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DestinationRepo {
    db: DatabaseConnection,
    crypto: Crypto,
}

impl DestinationRepo {
    pub fn new(db: DatabaseConnection, crypto: Crypto) -> Self {
        Self { db, crypto }
    }

    pub async fn create(&self, input: CreateDestination) -> Result<destination::Model, VmsError> {
        let config = encrypt_config(&self.crypto, input.config)?;
        let ts = now();
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            description: Set(input.description),
            dest_type: Set(input.dest_type),
            config: Set(config),
            enabled: Set(input.enabled),
            created_at: Set(ts),
            updated_at: Set(ts),
        };
        model.insert(&self.db).await.map_err(db_err)
    }

    /// Returns the row with credential fields in the config still **encrypted**.
    pub async fn get(&self, id: Uuid) -> Result<Option<destination::Model>, VmsError> {
        destination::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    /// Returns the row with credential fields **decrypted** — ready for adapter use.
    pub async fn get_decrypted(&self, id: Uuid) -> Result<Option<destination::Model>, VmsError> {
        let Some(mut dest) = self.get(id).await? else {
            return Ok(None);
        };
        dest.config = decrypt_config(&self.crypto, dest.config)?;
        Ok(Some(dest))
    }

    pub async fn list(&self) -> Result<Vec<destination::Model>, VmsError> {
        destination::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn update(
        &self,
        id: Uuid,
        input: UpdateDestination,
    ) -> Result<destination::Model, VmsError> {
        let dest = destination::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::DestinationNotFound(id))?;

        let mut active: ActiveModel = dest.into();

        if let Some(v) = input.name {
            active.name = Set(v);
        }
        if let Some(v) = input.description {
            active.description = Set(v);
        }
        if let Some(v) = input.dest_type {
            active.dest_type = Set(v);
        }
        if let Some(cfg) = input.config {
            active.config = Set(encrypt_config(&self.crypto, cfg)?);
        }
        if let Some(v) = input.enabled {
            active.enabled = Set(v);
        }

        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        let dest = destination::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::DestinationNotFound(id))?;
        dest.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }
}
