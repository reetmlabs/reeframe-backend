use sea_orm::{ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait, ModelTrait};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::entities::contact::{self, ActiveModel};

// -- Input types --

pub struct CreateContact {
    pub name: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub telegram_chat_id: Option<i64>,
    pub extra: serde_json::Value,
}

pub struct UpdateContact {
    pub name: Option<String>,
    pub email: Option<Option<String>>,
    pub phone: Option<Option<String>>,
    pub telegram_chat_id: Option<Option<i64>>,
    pub extra: Option<serde_json::Value>,
}

// -- Repository --

#[derive(Clone)]
pub struct ContactRepo {
    db: DatabaseConnection,
}

impl ContactRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create(&self, input: CreateContact) -> Result<contact::Model, VmsError> {
        let ts = now();
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            email: Set(input.email),
            phone: Set(input.phone),
            telegram_chat_id: Set(input.telegram_chat_id),
            extra: Set(input.extra),
            created_at: Set(ts),
            updated_at: Set(ts),
        };
        model.insert(&self.db).await.map_err(db_err)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<contact::Model>, VmsError> {
        contact::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn list(&self) -> Result<Vec<contact::Model>, VmsError> {
        contact::Entity::find().all(&self.db).await.map_err(db_err)
    }

    pub async fn update(&self, id: Uuid, input: UpdateContact) -> Result<contact::Model, VmsError> {
        let c = contact::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::ContactNotFound(id))?;

        let mut active: ActiveModel = c.into();

        if let Some(v) = input.name {
            active.name = Set(v);
        }
        if let Some(v) = input.email {
            active.email = Set(v);
        }
        if let Some(v) = input.phone {
            active.phone = Set(v);
        }
        if let Some(v) = input.telegram_chat_id {
            active.telegram_chat_id = Set(v);
        }
        if let Some(v) = input.extra {
            active.extra = Set(v);
        }

        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        let c = contact::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::ContactNotFound(id))?;
        c.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }
}
