use sea_orm::{
    prelude::DateTimeWithTimeZone, ActiveModelTrait, ActiveValue::Set, ColumnTrait, Condition,
    DatabaseConnection, EntityTrait, Order, QueryFilter, QueryOrder, QuerySelect,
};
use uuid::Uuid;
use vms_core::VmsError;

use crate::entities::recording::{self, ActiveModel};

use super::{db_err, now};

// -- Input types --

pub struct OpenChunk {
    pub camera_id: Uuid,
    pub file_path: String,
    pub chunk_index: i32,
    pub start_time: DateTimeWithTimeZone,
    pub codec: Option<String>,
}

// -- Repository --

#[derive(Clone)]
pub struct RecordingRepo {
    db: DatabaseConnection,
}

impl RecordingRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Insert a row the instant `splitmuxsink` opens a new fragment
    /// (`format-location-full`) — this is the one point that gives the real
    /// wall-clock instant a chunk started; deriving it from filenames/chunk
    /// index math would drift silently across reconnects.
    pub async fn open_chunk(&self, input: OpenChunk) -> Result<recording::Model, VmsError> {
        ActiveModel {
            id: Set(Uuid::new_v4()),
            camera_id: Set(input.camera_id),
            file_path: Set(input.file_path),
            chunk_index: Set(input.chunk_index),
            start_time: Set(input.start_time),
            end_time: Set(None),
            size_bytes: Set(None),
            codec: Set(input.codec),
            created_at: Set(now()),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    /// Backfill `end_time`/`size_bytes` when `splitmuxsink-fragment-closed`
    /// fires and the previous file is finalized on disk.
    ///
    /// Looked up by `(camera_id, file_path)` rather than a DB id — the
    /// `vms-media` producer of this event has no DB access (by design, see
    /// `vms_core::RecordingChunkEvent`) and therefore never learns the row's
    /// generated id; the still-open row for this exact path is always
    /// unique, so this is an unambiguous lookup.
    pub async fn close_chunk_by_path(
        &self,
        camera_id: Uuid,
        file_path: &str,
        end_time: DateTimeWithTimeZone,
        size_bytes: i64,
    ) -> Result<(), VmsError> {
        // No `tracing` dependency in this crate — a missing match is
        // surfaced as an error so the caller (which does have `tracing`)
        // logs it, rather than being silently swallowed here.
        let row = recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::FilePath.eq(file_path))
            .filter(recording::Column::EndTime.is_null())
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or_else(|| {
                VmsError::NotFound(format!(
                    "no open recording row for camera {camera_id} at path {file_path}"
                ))
            })?;

        let mut active: ActiveModel = row.into();
        active.end_time = Set(Some(end_time));
        active.size_bytes = Set(Some(size_bytes));
        active.update(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    /// Delete the still-open row for a fragment that `splitmuxsink` opened
    /// but never wrote any real data to (`RecordingChunkEvent::Discarded`)
    /// — a byproduct of a reconnect bug, not a real chunk of footage, so it
    /// has no place in the index at all rather than being backfilled with
    /// zero duration/size.
    pub async fn discard_open_chunk(
        &self,
        camera_id: Uuid,
        file_path: &str,
    ) -> Result<(), VmsError> {
        recording::Entity::delete_many()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::FilePath.eq(file_path))
            .filter(recording::Column::EndTime.is_null())
            .exec(&self.db)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    /// Chunks overlapping `[from, to)`. A chunk still being written
    /// (`end_time IS NULL`) has an unbounded effective end, so it overlaps
    /// whenever it already started before `to`.
    pub async fn list_for_camera(
        &self,
        camera_id: Uuid,
        from: DateTimeWithTimeZone,
        to: DateTimeWithTimeZone,
    ) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::StartTime.lt(to))
            .filter(
                Condition::any()
                    .add(recording::Column::EndTime.is_null())
                    .add(recording::Column::EndTime.gt(from)),
            )
            .order_by_asc(recording::Column::StartTime)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Chunks whose `start_time` falls in `[from, to)`, oldest first — unlike
    /// [`Self::list_for_camera`], this is not overlap-based: a chunk is
    /// selected by its start time alone, matching how the daily-coverage
    /// aggregator (and the FE's own `dailySummaries()`) buckets a whole
    /// chunk into the single day its start falls on, never splitting one
    /// across a day boundary.
    pub async fn list_starting_in_range_for_camera(
        &self,
        camera_id: Uuid,
        from: DateTimeWithTimeZone,
        to: DateTimeWithTimeZone,
    ) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::StartTime.gte(from))
            .filter(recording::Column::StartTime.lt(to))
            .order_by_asc(recording::Column::StartTime)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Resolve a specific instant to the chunk covering it —
    /// `start_time <= at` and (`end_time IS NULL` or `at < end_time`), i.e.
    /// including the chunk currently being written.
    ///
    /// Ordered by `start_time DESC` in case more than one row matches (e.g. a
    /// chunk orphaned mid-recording with `end_time` never set) — the most
    /// recently started match is always the correct/current chunk, never a
    /// stale earlier one.
    pub async fn resolve_at(
        &self,
        camera_id: Uuid,
        at: DateTimeWithTimeZone,
    ) -> Result<Option<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::StartTime.lte(at))
            .filter(
                Condition::any()
                    .add(recording::Column::EndTime.is_null())
                    .add(recording::Column::EndTime.gt(at)),
            )
            .order_by_desc(recording::Column::StartTime)
            .one(&self.db)
            .await
            .map_err(db_err)
    }

    /// When [`Self::resolve_at`] finds no covering chunk (a gap — camera was
    /// offline, or `at` is before/after all recorded history), the nearest
    /// chunk boundaries on either side, so the caller can offer "jump to
    /// nearest available footage" instead of a dead end.
    pub async fn nearest_boundaries(
        &self,
        camera_id: Uuid,
        at: DateTimeWithTimeZone,
    ) -> Result<(Option<recording::Model>, Option<recording::Model>), VmsError> {
        let before = recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::EndTime.lte(at))
            .order_by_desc(recording::Column::EndTime)
            .limit(1)
            .one(&self.db)
            .await
            .map_err(db_err)?;

        let after = recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::StartTime.gte(at))
            .order_by_asc(recording::Column::StartTime)
            .limit(1)
            .one(&self.db)
            .await
            .map_err(db_err)?;

        Ok((before, after))
    }

    pub async fn get(&self, id: Uuid) -> Result<recording::Model, VmsError> {
        recording::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?
            .ok_or(VmsError::RecordingNotFound(id))
    }

    /// Chunks belonging to a camera, oldest first — used to build a
    /// concatenated export.
    pub async fn list_range_ordered(
        &self,
        camera_id: Uuid,
        from: DateTimeWithTimeZone,
        to: DateTimeWithTimeZone,
    ) -> Result<Vec<recording::Model>, VmsError> {
        self.list_for_camera(camera_id, from, to).await
    }

    /// Finalized chunks (both a real `end_time` and `size_bytes`, i.e. not
    /// the one currently being written) older than `cutoff` — the age-based
    /// half of the retention sweep.
    pub async fn list_older_than(
        &self,
        cutoff: DateTimeWithTimeZone,
    ) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::EndTime.is_not_null())
            .filter(recording::Column::EndTime.lte(cutoff))
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Same as [`Self::list_older_than`], scoped to a single camera — used
    /// when that camera has its own retention-days override.
    pub async fn list_older_than_for_camera(
        &self,
        camera_id: Uuid,
        cutoff: DateTimeWithTimeZone,
    ) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::CameraId.eq(camera_id))
            .filter(recording::Column::EndTime.is_not_null())
            .filter(recording::Column::EndTime.lte(cutoff))
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Finalized chunks across every camera, oldest first — the
    /// disk-threshold half of the retention sweep (delete the oldest
    /// chunks first until usage drops back under the configured
    /// threshold).
    pub async fn list_oldest_finalized(
        &self,
        limit: u64,
    ) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::EndTime.is_not_null())
            .order_by(recording::Column::EndTime, Order::Asc)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Same as [`Self::list_oldest_finalized`], restricted to cameras whose
    /// own disk-threshold override currently makes them eligible for cleanup.
    pub async fn list_oldest_finalized_for_cameras(
        &self,
        camera_ids: &[Uuid],
        limit: u64,
    ) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::CameraId.is_in(camera_ids.iter().copied()))
            .filter(recording::Column::EndTime.is_not_null())
            .order_by(recording::Column::EndTime, Order::Asc)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Every row still open (`end_time IS NULL`), across all cameras — used
    /// by the daemon-startup reconciliation sweep, where every such row is
    /// unconditionally left over from a previous process lifetime (this
    /// runs before any camera in the current process has opened a chunk).
    pub async fn list_open_chunks(&self) -> Result<Vec<recording::Model>, VmsError> {
        recording::Entity::find()
            .filter(recording::Column::EndTime.is_null())
            .all(&self.db)
            .await
            .map_err(db_err)
    }

    /// Backfill `end_time`/`size_bytes` on an already-fetched row — used by
    /// the startup reconciliation sweep to heal a chunk whose file turned
    /// out to be valid despite the row being left open. Takes the row
    /// itself (from [`Self::list_open_chunks`]) rather than an id, unlike
    /// [`Self::close_chunk_by_path`], since the caller already has it and a
    /// re-lookup by id would be redundant.
    pub async fn backfill_end_time(
        &self,
        row: recording::Model,
        end_time: DateTimeWithTimeZone,
        size_bytes: i64,
    ) -> Result<(), VmsError> {
        let mut active: ActiveModel = row.into();
        active.end_time = Set(Some(end_time));
        active.size_bytes = Set(Some(size_bytes));
        active.update(&self.db).await.map_err(db_err)?;
        Ok(())
    }

    pub async fn delete(&self, id: Uuid) -> Result<(), VmsError> {
        recording::Entity::delete_by_id(id)
            .exec(&self.db)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sea_orm_migration::MigratorTrait;

    use super::*;
    use crate::{
        crypto::Crypto,
        entities::camera::RingBufferStorage,
        migration::Migrator,
        repos::camera::{CameraRepo, CreateCamera},
    };

    async fn test_db() -> DatabaseConnection {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db
    }

    async fn create_camera(db: &DatabaseConnection) -> Uuid {
        let repo = CameraRepo::new(db.clone(), Crypto::from_key([0u8; 32]));
        repo.create(CreateCamera {
            name: "cam".into(),
            description: None,
            rtsp_url: "rtsp://example.invalid/stream".into(),
            sub_rtsp_url: None,
            manufacturer: None,
            model: None,
            username: None,
            password: None,
            extra_config: serde_json::json!({}),
            ring_buffer_duration_secs: 300,
            ring_buffer_storage: RingBufferStorage::Memory,
            enabled: true,
            motion_detection_enabled: true,
            thumbnails_enabled: false,
        })
        .await
        .unwrap()
        .id
    }

    #[tokio::test]
    async fn list_open_chunks_finds_only_end_time_null_rows() {
        let db = test_db().await;
        let camera_id = create_camera(&db).await;
        let repo = RecordingRepo::new(db);

        let open = repo
            .open_chunk(OpenChunk {
                camera_id,
                file_path: "/rec/open.mp4".into(),
                chunk_index: 0,
                start_time: Utc::now().fixed_offset(),
                codec: None,
            })
            .await
            .unwrap();
        let closed = repo
            .open_chunk(OpenChunk {
                camera_id,
                file_path: "/rec/closed.mp4".into(),
                chunk_index: 1,
                start_time: Utc::now().fixed_offset(),
                codec: None,
            })
            .await
            .unwrap();
        repo.close_chunk_by_path(camera_id, &closed.file_path, Utc::now().fixed_offset(), 100)
            .await
            .unwrap();

        let found = repo.list_open_chunks().await.unwrap();

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, open.id);
    }

    #[tokio::test]
    async fn backfill_end_time_sets_end_time_and_size_bytes() {
        let db = test_db().await;
        let camera_id = create_camera(&db).await;
        let repo = RecordingRepo::new(db);

        let row = repo
            .open_chunk(OpenChunk {
                camera_id,
                file_path: "/rec/orphaned.mp4".into(),
                chunk_index: 0,
                start_time: Utc::now().fixed_offset(),
                codec: None,
            })
            .await
            .unwrap();
        let end_time = Utc::now().fixed_offset();

        repo.backfill_end_time(row.clone(), end_time, 4096)
            .await
            .unwrap();

        let healed = repo.get(row.id).await.unwrap();
        assert_eq!(healed.end_time, Some(end_time));
        assert_eq!(healed.size_bytes, Some(4096));
    }
}
