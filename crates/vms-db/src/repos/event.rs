use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder,
};
use uuid::Uuid;
use vms_core::VmsError;

use super::db_err;
use crate::entities::event::{self, ActiveModel};

// -- Input types --

pub struct CreateEvent {
    pub camera_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub occurred_at: chrono::DateTime<chrono::FixedOffset>,
}

// -- Repository --

#[derive(Clone)]
pub struct EventsRepo {
    db: DatabaseConnection,
}

impl EventsRepo {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn record(&self, input: CreateEvent) -> Result<event::Model, VmsError> {
        ActiveModel {
            id: Set(Uuid::new_v4()),
            camera_id: Set(input.camera_id),
            event_type: Set(input.event_type),
            payload: Set(input.payload),
            occurred_at: Set(input.occurred_at),
            created_at: Set(super::now()),
        }
        .insert(&self.db)
        .await
        .map_err(db_err)
    }

    /// Markers for the scrub timeline: every event for `camera_id` in
    /// `[from, to)`, optionally narrowed to specific `event_types` (empty
    /// = no filter), oldest first.
    pub async fn list_for_camera(
        &self,
        camera_id: Uuid,
        from: chrono::DateTime<chrono::FixedOffset>,
        to: chrono::DateTime<chrono::FixedOffset>,
        event_types: &[String],
    ) -> Result<Vec<event::Model>, VmsError> {
        let mut query = event::Entity::find()
            .filter(event::Column::CameraId.eq(camera_id))
            .filter(event::Column::OccurredAt.gte(from))
            .filter(event::Column::OccurredAt.lt(to));
        if !event_types.is_empty() {
            query = query.filter(event::Column::EventType.is_in(event_types.iter().cloned()));
        }
        query
            .order_by_asc(event::Column::OccurredAt)
            .all(&self.db)
            .await
            .map_err(db_err)
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
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

    fn at(offset_secs: i64) -> chrono::DateTime<chrono::FixedOffset> {
        (Utc::now() + Duration::seconds(offset_secs)).fixed_offset()
    }

    #[tokio::test]
    async fn record_round_trips() {
        let db = test_db().await;
        let camera_id = create_camera(&db).await;
        let repo = EventsRepo::new(db);

        let recorded = repo
            .record(CreateEvent {
                camera_id,
                event_type: "motion_started".into(),
                payload: serde_json::json!({"score": 0.9}),
                occurred_at: at(0),
            })
            .await
            .unwrap();

        assert_eq!(recorded.camera_id, camera_id);
        assert_eq!(recorded.event_type, "motion_started");
        assert_eq!(recorded.payload, serde_json::json!({"score": 0.9}));
    }

    #[tokio::test]
    async fn list_for_camera_is_scoped_to_the_time_range_and_ordered() {
        let db = test_db().await;
        let camera_id = create_camera(&db).await;
        let repo = EventsRepo::new(db);

        for (offset, event_type) in [
            (-100, "signal_lost"),
            (-10, "motion_started"),
            (10, "motion_stopped"),
            (200, "scene_change"), // outside the queried range
        ] {
            repo.record(CreateEvent {
                camera_id,
                event_type: event_type.into(),
                payload: serde_json::json!({}),
                occurred_at: at(offset),
            })
            .await
            .unwrap();
        }

        let events = repo
            .list_for_camera(camera_id, at(-50), at(50), &[])
            .await
            .unwrap();

        assert_eq!(
            events
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>(),
            vec!["motion_started", "motion_stopped"]
        );
    }

    #[tokio::test]
    async fn list_for_camera_filters_by_event_type() {
        let db = test_db().await;
        let camera_id = create_camera(&db).await;
        let repo = EventsRepo::new(db);

        for event_type in ["motion_started", "tamper_detected"] {
            repo.record(CreateEvent {
                camera_id,
                event_type: event_type.into(),
                payload: serde_json::json!({}),
                occurred_at: at(0),
            })
            .await
            .unwrap();
        }

        let events = repo
            .list_for_camera(camera_id, at(-10), at(10), &["tamper_detected".to_string()])
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "tamper_detected");
    }

    #[tokio::test]
    async fn deleting_a_camera_cascades_its_events() {
        let db = test_db().await;
        let camera_id = create_camera(&db).await;
        let repo = EventsRepo::new(db.clone());
        repo.record(CreateEvent {
            camera_id,
            event_type: "motion_started".into(),
            payload: serde_json::json!({}),
            occurred_at: at(0),
        })
        .await
        .unwrap();

        crate::entities::camera::Entity::delete_by_id(camera_id)
            .exec(&db)
            .await
            .unwrap();

        let events = repo
            .list_for_camera(camera_id, at(-10), at(10), &[])
            .await
            .unwrap();
        assert!(events.is_empty());
    }
}
