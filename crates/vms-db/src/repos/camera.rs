use sea_orm::{ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait, ModelTrait};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::{
    crypto::Crypto,
    entities::camera::{self, ActiveModel, LiveViewStream, RingBufferStorage},
};

// -- Input types --

pub struct CreateCamera {
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub sub_rtsp_url: Option<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    /// Plaintext RTSP password; encrypted before storage.
    pub password: Option<String>,
    pub extra_config: serde_json::Value,
    pub ring_buffer_duration_secs: i32,
    pub ring_buffer_storage: RingBufferStorage,
    pub enabled: bool,
    pub motion_detection_enabled: bool,
    pub thumbnails_enabled: bool,
    pub live_view_stream: LiveViewStream,
}

pub struct UpdateCamera {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub rtsp_url: Option<String>,
    pub sub_rtsp_url: Option<Option<String>>,
    pub manufacturer: Option<Option<String>>,
    pub model: Option<Option<String>>,
    pub username: Option<Option<String>>,
    /// `None` = leave unchanged. `Some(None)` = clear. `Some(Some(pw))` = set new password.
    pub password: Option<Option<String>>,
    pub extra_config: Option<serde_json::Value>,
    pub ring_buffer_duration_secs: Option<i32>,
    pub ring_buffer_storage: Option<RingBufferStorage>,
    pub enabled: Option<bool>,
    /// `None` = leave unchanged. `Some(None)` = clear the override (inherit
    /// the global default). `Some(Some(days))` = set an explicit override.
    pub retention_days: Option<Option<i32>>,
    /// Same three-state shape as `retention_days`.
    pub retention_disk_threshold_percent: Option<Option<f64>>,
    /// `None` = leave unchanged. `Some(None)` = clear the override (inherit
    /// the global `[recordings] timezone` default). `Some(Some(tz))` = set
    /// an explicit IANA timezone override for this camera.
    pub timezone: Option<Option<String>>,
    pub motion_detection_enabled: Option<bool>,
    pub thumbnails_enabled: Option<bool>,
    pub live_view_stream: Option<LiveViewStream>,
}

// -- Repository --

#[derive(Clone)]
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
            sub_rtsp_url: Set(input.sub_rtsp_url),
            codec: Set(None),
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
            retention_days: Set(None),
            retention_disk_threshold_percent: Set(None),
            desired_recording: Set(false),
            timezone: Set(None),
            motion_detection_enabled: Set(input.motion_detection_enabled),
            thumbnails_enabled: Set(input.thumbnails_enabled),
            live_view_stream: Set(input.live_view_stream),
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
        if let Some(v) = input.sub_rtsp_url {
            active.sub_rtsp_url = Set(v);
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
        if let Some(v) = input.retention_days {
            active.retention_days = Set(v);
        }
        if let Some(v) = input.retention_disk_threshold_percent {
            active.retention_disk_threshold_percent = Set(v);
        }
        if let Some(v) = input.timezone {
            active.timezone = Set(v);
        }
        if let Some(v) = input.motion_detection_enabled {
            active.motion_detection_enabled = Set(v);
        }
        if let Some(v) = input.thumbnails_enabled {
            active.thumbnails_enabled = Set(v);
        }
        if let Some(v) = input.live_view_stream {
            active.live_view_stream = Set(v);
        }

        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    /// Update only the cached codec. Called after a successful codec probe.
    pub async fn set_codec(&self, id: Uuid, codec: &str) -> Result<(), VmsError> {
        let cam = camera::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::CameraNotFound(id))?;
        let mut active: ActiveModel = cam.into();
        active.codec = Set(Some(codec.to_owned()));
        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    /// Persist operator intent — called from the `recording/start`/`stop`
    /// routes before touching `MediaManager`, regardless of whether that
    /// call succeeds, so this always reflects the last explicit request.
    pub async fn set_desired_recording(&self, id: Uuid, desired: bool) -> Result<(), VmsError> {
        let cam = camera::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::CameraNotFound(id))?;
        let mut active: ActiveModel = cam.into();
        active.desired_recording = Set(desired);
        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)?;
        Ok(())
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
