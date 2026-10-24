//! Shared fixtures for tests that need a real database or scratch files.

use std::path::PathBuf;

use sea_orm_migration::MigratorTrait;
use uuid::Uuid;
use vms_db::entities::camera::{LiveViewStream, RingBufferStorage};
use vms_db::entities::recording;
use vms_db::repos::camera::CreateCamera;
use vms_db::{CameraRepo, Crypto, Migrator, OpenChunk, RecordingRepo};

/// A scratch dir under the system temp dir, removed on drop.
pub(crate) struct ScratchDir(pub(crate) PathBuf);

impl ScratchDir {
    pub(crate) fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("vms-engine-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// A migrated in-memory database with one camera, and its recording repo.
pub(crate) async fn recording_repo_with_camera() -> (RecordingRepo, Uuid) {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let camera = CameraRepo::new(db.clone(), Crypto::from_key([0u8; 32]))
        .create(CreateCamera {
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
            motion_detection_enabled: false,
            thumbnails_enabled: false,
            live_view_stream: LiveViewStream::Sub,
        })
        .await
        .unwrap();
    (RecordingRepo::new(db), camera.id)
}

/// Insert an open chunk row pointing at `file_path`.
pub(crate) async fn open_chunk_at(
    repo: &RecordingRepo,
    camera_id: Uuid,
    file_path: &std::path::Path,
) -> recording::Model {
    repo.open_chunk(OpenChunk {
        camera_id,
        file_path: file_path.to_string_lossy().into_owned(),
        chunk_index: 0,
        start_time: chrono::Utc::now().into(),
        codec: Some("H264".into()),
    })
    .await
    .unwrap()
}
