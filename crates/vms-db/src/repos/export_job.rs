use sea_orm::{
    prelude::DateTimeWithTimeZone, ActiveModelTrait, ActiveValue::Set, DatabaseConnection,
    EntityTrait,
};
use uuid::Uuid;
use vms_core::VmsError;

use crate::entities::export_job::{self, ActiveModel, ExportJobStatus};

use super::{db_err, now};

// -- Repository --

#[derive(Clone)]
pub struct ExportJobRepo {
    db: DatabaseConnection,
}

impl ExportJobRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Insert a new `export_jobs` row in `Pending` state. Returns
    /// immediately — the caller (an API handler) spawns the actual
    /// concat/trim work in a background task and reports progress via
    /// [`Self::start`]/[`Self::complete`]/[`Self::fail`].
    pub async fn create(
        &self,
        camera_id: Uuid,
        from_time: DateTimeWithTimeZone,
        to_time: DateTimeWithTimeZone,
    ) -> Result<export_job::Model, VmsError> {
        ActiveModel {
            id: Set(Uuid::new_v4()),
            camera_id: Set(camera_id),
            from_time: Set(from_time),
            to_time: Set(to_time),
            status: Set(ExportJobStatus::Pending),
            file_path: Set(None),
            size_bytes: Set(None),
            error: Set(None),
            created_at: Set(now()),
            completed_at: Set(None),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    pub async fn get(&self, id: Uuid) -> Result<export_job::Model, VmsError> {
        export_job::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or_else(|| VmsError::NotFound(format!("export job {id} not found")))
    }

    /// Transition `Pending` -> `Running`, once the background task actually
    /// starts working (as opposed to still sitting in the queue).
    pub async fn start(&self, id: Uuid) -> Result<(), VmsError> {
        ActiveModel {
            id: Set(id),
            status: Set(ExportJobStatus::Running),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn complete(
        &self,
        id: Uuid,
        file_path: String,
        size_bytes: i64,
    ) -> Result<(), VmsError> {
        ActiveModel {
            id: Set(id),
            status: Set(ExportJobStatus::Completed),
            file_path: Set(Some(file_path)),
            size_bytes: Set(Some(size_bytes)),
            completed_at: Set(Some(now())),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    pub async fn fail(&self, id: Uuid, error: String) -> Result<(), VmsError> {
        ActiveModel {
            id: Set(id),
            status: Set(ExportJobStatus::Failed),
            error: Set(Some(error)),
            completed_at: Set(Some(now())),
            ..Default::default()
        }
        .update(&self.db)
        .await
        .map_err(db_err)?;
        Ok(())
    }
}
