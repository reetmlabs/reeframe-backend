use sea_orm::{ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait, ModelTrait};
use uuid::Uuid;
use vms_core::VmsError;

use crate::{
    crypto::Crypto,
    entities::camera::{self, ActiveModel, RingBufferStorage},
};
use super::{db_err, now};

// ── Input types ───────────────────────────────────────────────────────────────

pub struct CreateCamera {
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    /// Plaintext RTSP password; encrypted before storage.
    pub password: Option<String>,
    pub extra_config: serde_json::Value,
    pub ring_buffer_duration_secs: i32,
    pub ring_buffer_storage: RingBufferStorage,
    pub enabled: bool,
}

pub struct UpdateCamera {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub rtsp_url: Option<String>,
    pub manufacturer: Option<Option<String>>,
    pub model: Option<Option<String>>,
    pub username: Option<Option<String>>,
    /// `None` = leave unchanged. `Some(None)` = clear. `Some(Some(pw))` = set new password.
    pub password: Option<Option<String>>,
    pub extra_config: Option<serde_json::Value>,
    pub ring_buffer_duration_secs: Option<i32>,
    pub ring_buffer_storage: Option<RingBufferStorage>,
    pub enabled: Option<bool>,
}

// ── Repository ────────────────────────────────────────────────────────────────

pub struct CameraRepo {
    db: DatabaseConnection,
    crypto: Crypto,
}

impl CameraRepo {
    pub fn new(db: DatabaseConnection, crypto: Crypto) -> Self {
        Self { db, crypto }
    }

    pub async fn create(&self, input: CreateCamera) -> Result<camera::Model, VmsError> {
        let password_enc = input
            .password
            .as_deref()
            .map(|p| self.crypto.encrypt(p))
            .transpose()?;

        let ts = now();
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            description: Set(input.description),
            rtsp_url: Set(input.rtsp_url),
            manufacturer: Set(input.manufacturer),
            model: Set(input.model),
            username: Set(input.username),
            password_enc: Set(password_enc),
            extra_config: Set(input.extra_config),
            ring_buffer_duration_secs: Set(input.ring_buffer_duration_secs),
            ring_buffer_storage: Set(input.ring_buffer_storage),
            enabled: Set(input.enabled),
            created_at: Set(ts),
            updated_at: Set(ts),
        };
        model.insert(&self.db).await.map_err(db_err)
    }

    /// Returns the camera row with `password_enc` still encrypted.
    /// Callers that need the plaintext password should use [`Self::get_decrypted`].
    pub async fn get(&self, id: Uuid) -> Result<Option<camera::Model>, VmsError> {
        camera::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    /// Returns `(model, plaintext_password)`. The model's `password_enc` field
    /// still contains the encrypted value; the second tuple element is the decrypted
    /// password ready for use in an RTSP URL.
    pub async fn get_decrypted(
        &self,
        id: Uuid,
    ) -> Result<Option<(camera::Model, Option<String>)>, VmsError> {
        let Some(cam) = self.get(id).await? else {
            return Ok(None);
        };
        let password = cam
            .password_enc
            .as_deref()
            .map(|enc| self.crypto.decrypt(enc))
            .transpose()?;
        Ok(Some((cam, password)))
    }

    pub async fn list(&self) -> Result<Vec<camera::Model>, VmsError> {
        camera::Entity::find().all(&self.db).await.map_err(db_err)
    }

    pub async fn update(&self, id: Uuid, input: UpdateCamera) -> Result<camera::Model, VmsError> {
        let cam = camera::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::CameraNotFound(id))?;

        let mut active: ActiveModel = cam.into();

        if let Some(v) = input.name {
            active.name = Set(v);
        }
        if let Some(v) = input.description {
            active.description = Set(v);
        }
        if let Some(v) = input.rtsp_url {
            active.rtsp_url = Set(v);
        }
        if let Some(v) = input.manufacturer {
            active.manufacturer = Set(v);
        }
        if let Some(v) = input.model {
            active.model = Set(v);
        }
        if let Some(v) = input.username {
            active.username = Set(v);
        }
        if let Some(maybe_pw) = input.password {
            let enc = maybe_pw
                .as_deref()
                .map(|p| self.crypto.encrypt(p))
                .transpose()?;
            active.password_enc = Set(enc);
        }
        if let Some(v) = input.extra_config {
            active.extra_config = Set(v);
        }
        if let Some(v) = input.ring_buffer_duration_secs {
            active.ring_buffer_duration_secs = Set(v);
        }
        if let Some(v) = input.ring_buffer_storage {
            active.ring_buffer_storage = Set(v);
        }
        if let Some(v) = input.enabled {
            active.enabled = Set(v);
        }

        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        let cam = camera::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::CameraNotFound(id))?;
        cam.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }
}
