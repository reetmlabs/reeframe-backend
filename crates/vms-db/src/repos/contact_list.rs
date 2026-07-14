use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait,
    QueryFilter,
};
use uuid::Uuid;
use vms_core::VmsError;

use super::{db_err, now};
use crate::entities::{
    contact,
    contact_list::{self, ActiveModel},
    contact_list_member,
};

// -- Input types --

pub struct CreateContactList {
    pub name: String,
    pub description: Option<String>,
}

pub struct UpdateContactList {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
}

// -- Repository --

#[derive(Clone)]
pub struct ContactListRepo {
    db: DatabaseConnection,
}

impl ContactListRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create(&self, input: CreateContactList) -> Result<contact_list::Model, VmsError> {
        let ts = now();
        let model = ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(input.name),
            description: Set(input.description),
            created_at: Set(ts),
            updated_at: Set(ts),
        };
        model.insert(&self.db).await.map_err(db_err)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<contact_list::Model>, VmsError> {
        contact_list::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn list(&self) -> Result<Vec<contact_list::Model>, VmsError> {
        contact_list::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    pub async fn update(
        &self,
        id: Uuid,
        input: UpdateContactList,
    ) -> Result<contact_list::Model, VmsError> {
        let cl = contact_list::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::ContactListNotFound(id))?;

        let mut active: ActiveModel = cl.into();

        if let Some(v) = input.name {
            active.name = Set(v);
        }
        if let Some(v) = input.description {
            active.description = Set(v);
        }

        active.updated_at = Set(now());
        active.update(&self.db).await.map_err(db_err)
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        let cl = contact_list::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::ContactListNotFound(id))?;
        cl.delete(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    // -- Membership --
    //
    // `contact_list_members` has `ON DELETE CASCADE` on both FKs, so deleting
    // a contact or a list cleans up membership rows automatically. That does
    // *not* stop an insert referencing IDs that never existed in the first
    // place, so `add_member` still needs the same proactive existence checks
    // used for every other cross-entity reference in `pipeline.rs` (9-7) —
    // otherwise a bad ID surfaces as a raw "FOREIGN KEY constraint failed"
    // `500` instead of a clean `404`.

    pub async fn add_member(
        &self,
        contact_list_id: Uuid,
        contact_id: Uuid,
    ) -> Result<(), VmsError> {
        self.require_exists(contact_list_id).await?;
        require_contact_exists(&self.db, contact_id).await?;

        let existing = contact_list_member::Entity::find()
            .filter(contact_list_member::Column::ContactListId.eq(contact_list_id))
            .filter(contact_list_member::Column::ContactId.eq(contact_id))
            .one(&self.db)
            .await
            .map_err(db_err)?;
        if existing.is_some() {
            return Ok(());
        }

        contact_list_member::ActiveModel {
            contact_list_id: Set(contact_list_id),
            contact_id: Set(contact_id),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn remove_member(
        &self,
        contact_list_id: Uuid,
        contact_id: Uuid,
    ) -> Result<(), VmsError> {
        contact_list_member::Entity::delete_many()
            .filter(contact_list_member::Column::ContactListId.eq(contact_list_id))
            .filter(contact_list_member::Column::ContactId.eq(contact_id))
            .exec(&self.db)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    pub async fn list_members(
        &self,
        contact_list_id: Uuid,
    ) -> Result<Vec<contact::Model>, VmsError> {
        self.require_exists(contact_list_id).await?;

        let member_rows = contact_list_member::Entity::find()
            .filter(contact_list_member::Column::ContactListId.eq(contact_list_id))
            .all(&self.db)
            .await
            .map_err(db_err)?;

        let mut contacts = Vec::with_capacity(member_rows.len());
        for row in member_rows {
            if let Some(c) = contact::Entity::find_by_id(row.contact_id)
                .one(&self.db)
                .await
                .map_err(db_err)?
            {
                contacts.push(c);
            }
        }
        Ok(contacts)
    }

    async fn require_exists(&self, contact_list_id: Uuid) -> Result<(), VmsError> {
        contact_list::Entity::find_by_id(contact_list_id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .map(|_| ())
            .ok_or(VmsError::ContactListNotFound(contact_list_id))
    }
}

async fn require_contact_exists(db: &DatabaseConnection, contact_id: Uuid) -> Result<(), VmsError> {
    contact::Entity::find_by_id(contact_id)
        .one(db)
        .await
        .map_err(db_err)?
        .map(|_| ())
        .ok_or(VmsError::ContactNotFound(contact_id))
}
