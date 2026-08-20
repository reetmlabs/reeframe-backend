use sea_orm::{
    prelude::Date, ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection,
    EntityTrait, QueryFilter, QueryOrder,
};
use uuid::Uuid;
use vms_core::VmsError;

use crate::entities::daily_recording_coverage::{self, ActiveModel};

use super::{db_err, now};

// -- Input types --

pub struct UpsertDailyCoverage {
    pub camera_id: Uuid,
    pub day: Date,
    pub coverage_seconds: i64,
    pub session_ranges: serde_json::Value,
    pub chunk_count: i32,
    pub total_size_bytes: Option<i64>,
    pub is_finalized: bool,
    pub purged_by_retention: bool,
}

// -- Repository --

#[derive(Clone)]
pub struct DailyRecordingCoverageRepo {
    db: DatabaseConnection,
}

impl DailyRecordingCoverageRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Insert or overwrite the row for `(camera_id, day)`. Called from a
    /// single-threaded background sweep, never concurrently for the same
    /// key — a plain find-then-write is enough, no need for a
    /// backend-specific `ON CONFLICT`/`ON DUPLICATE KEY` upsert (MySQL,
    /// Postgres, and SQLite all differ there).
    pub async fn upsert(
        &self,
        input: UpsertDailyCoverage,
    ) -> Result<daily_recording_coverage::Model, VmsError> {
        let existing = daily_recording_coverage::Entity::find()
            .filter(daily_recording_coverage::Column::CameraId.eq(input.camera_id))
            .filter(daily_recording_coverage::Column::Day.eq(input.day))
            .one(&self.db)
            .await
            .map_err(db_err)?;

        match existing {
            Some(row) => {
                let mut active: ActiveModel = row.into();
                active.coverage_seconds = Set(input.coverage_seconds);
                active.session_ranges = Set(input.session_ranges);
                active.chunk_count = Set(input.chunk_count);
                active.total_size_bytes = Set(input.total_size_bytes);
                active.is_finalized = Set(input.is_finalized);
                active.purged_by_retention = Set(input.purged_by_retention);
                active.computed_at = Set(now());
                active.update(&self.db).await.map_err(db_err)
            }
            None => ActiveModel {
                id: Set(Uuid::new_v4()),
                camera_id: Set(input.camera_id),
                day: Set(input.day),
                coverage_seconds: Set(input.coverage_seconds),
                session_ranges: Set(input.session_ranges),
                chunk_count: Set(input.chunk_count),
                total_size_bytes: Set(input.total_size_bytes),
                is_finalized: Set(input.is_finalized),
                purged_by_retention: Set(input.purged_by_retention),
                computed_at: Set(now()),
            }
            .insert(&self.db)
            .await
            .map_err(db_err),
        }
    }

    /// Rows for one camera, `day` in `[from, to)`, oldest first.
    pub async fn list_range(
        &self,
        camera_id: Uuid,
        from: Date,
        to: Date,
    ) -> Result<Vec<daily_recording_coverage::Model>, VmsError> {
        daily_recording_coverage::Entity::find()
            .filter(daily_recording_coverage::Column::CameraId.eq(camera_id))
            .filter(daily_recording_coverage::Column::Day.gte(from))
            .filter(daily_recording_coverage::Column::Day.lt(to))
            .order_by_asc(daily_recording_coverage::Column::Day)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Same as [`Self::list_range`], across multiple cameras at once — the
    /// fleet-overview shape, one query instead of N.
    pub async fn list_range_bulk(
        &self,
        camera_ids: &[Uuid],
        from: Date,
        to: Date,
    ) -> Result<Vec<daily_recording_coverage::Model>, VmsError> {
        daily_recording_coverage::Entity::find()
            .filter(daily_recording_coverage::Column::CameraId.is_in(camera_ids.iter().copied()))
            .filter(daily_recording_coverage::Column::Day.gte(from))
            .filter(daily_recording_coverage::Column::Day.lt(to))
            .order_by_asc(daily_recording_coverage::Column::Day)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Whether a row already exists for `(camera_id, day)` — used by the
    /// backfill pass to skip days it's already computed.
    pub async fn exists(&self, camera_id: Uuid, day: Date) -> Result<bool, VmsError> {
        daily_recording_coverage::Entity::find()
            .filter(daily_recording_coverage::Column::CameraId.eq(camera_id))
            .filter(daily_recording_coverage::Column::Day.eq(day))
            .one(&self.db)
            .await
            .map_err(db_err)
            .map(|row| row.is_some())
    }
}
