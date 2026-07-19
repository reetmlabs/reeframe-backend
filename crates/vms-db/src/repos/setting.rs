use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait, IntoActiveModel,
};
use vms_core::VmsError;

use crate::entities::setting::{self, ActiveModel};

use super::{db_err, now};

// -- Repository --

#[derive(Clone)]
pub struct SettingsRepo {
    db: DatabaseConnection,
}

impl SettingsRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn get(&self, key: &str) -> Result<Option<setting::Model>, VmsError> {
        setting::Entity::find_by_id(key)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn get_all(&self) -> Result<Vec<setting::Model>, VmsError> {
        setting::Entity::find().all(&self.db).await.map_err(db_err)
    }

    /// Insert or update the row for `key`. Used both by `PATCH
    /// /system/settings` (explicit override) and by the startup loop
    /// (seeding a not-yet-overridden key with its config-file-resolved
    /// value, `pending_restart` always `false` in that case).
    pub async fn upsert(
        &self,
        key: &str,
        value: serde_json::Value,
        pending_restart: bool,
    ) -> Result<(), VmsError> {
        let value = value.to_string();
        match self.get(key).await? {
            Some(row) => {
                let mut active: ActiveModel = row.into_active_model();
                active.value = Set(value);
                active.pending_restart = Set(pending_restart);
                active.updated_at = Set(now());
                active.update(&self.db).await.map_err(db_err)?;
            }
            None => {
                ActiveModel {
                    key: Set(key.to_owned()),
                    value: Set(value),
                    pending_restart: Set(pending_restart),
                    updated_at: Set(now()),
                }
                .insert(&self.db)
                .await
                .map_err(db_err)?;
            }
        }
        Ok(())
    }

    /// Called once a restart-required setting's stored value has actually
    /// been applied (i.e. at the startup that follows the change) — clears
    /// the "waiting for a restart" flag.
    pub async fn clear_pending(&self, key: &str) -> Result<(), VmsError> {
        if let Some(row) = self.get(key).await? {
            let mut active: ActiveModel = row.into_active_model();
            active.pending_restart = Set(false);
            active.update(&self.db).await.map_err(db_err)?;
        }
        Ok(())
    }
}
