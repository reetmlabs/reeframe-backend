use sea_orm::{ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait, ModelTrait};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, decrypt_config, encrypt_config, now};
use crate::{
    crypto::Crypto,
    entities::source::{self, ActiveModel, SourceType},
};

// -- Input types ---------------------------------------------------------------

pub struct CreateSource {
    pub name: String,
    pub description: Option<String>,
    pub source_type: SourceType,
    /// Adapter config with credential values in **plaintext**.
    /// The repo encrypts credential fields before storage.
    pub config: serde_json::Value,
    pub enabled: bool,
}

pub struct UpdateSource {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub source_type: Option<SourceType>,
    /// Full config replacement. Credential fields should be in **plaintext**.
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

// -- Repository ----------------------------------------------------------------

#[derive(Clone)]
pub struct SourceRepo {
    db: DatabaseConnection,
    crypto: Crypto,
}

impl SourceRepo {
    pub fn new(db: DatabaseConnection, crypto: Crypto) -> Self {
        Self { db, crypto }
    }

    pub async fn create(&self, input: CreateSource) -> Result<source::Model, VmsError> {
        let config = encrypt_config(&self.crypto, input.config)?;
        let ts = now();
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            description: Set(input.description),
            source_type: Set(input.source_type),
            config: Set(config),
            enabled: Set(input.enabled),
            created_at: Set(ts),
            updated_at: Set(ts),
        };
        model.insert(&self.db).await.map_err(db_err)
    }

    /// Returns the row with credential fields in the config still **encrypted**.
    pub async fn get(&self, id: Uuid) -> Result<Option<source::Model>, VmsError> {
        source::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    /// Returns the row with credential fields **decrypted** — ready for adapter use.
    pub async fn get_decrypted(&self, id: Uuid) -> Result<Option<source::Model>, VmsError> {
        let Some(mut src) = self.get(id).await? else {
            return Ok(None);
        };
        src.config = decrypt_config(&self.crypto, src.config)?;
        Ok(Some(src))
    }

    pub async fn list(&self) -> Result<Vec<source::Model>, VmsError> {
        source::Entity::find().all(&self.db).await.map_err(db_err)
    }

    pub async fn update(&self, id: Uuid, input: UpdateSource) -> Result<source::Model, VmsError> {
        let src = source::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::SourceNotFound(id))?;

        let mut active: ActiveModel = src.into();

        if let Some(v) = input.name {
            active.name = Set(v);
        }
        if let Some(v) = input.description {
            active.description = Set(v);
        }
        if let Some(v) = input.source_type {
            active.source_type = Set(v);
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
        let src = source::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::SourceNotFound(id))?;
        src.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }
}
