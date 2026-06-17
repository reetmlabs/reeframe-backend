use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::camera::{self, RingBufferStorage},
    repos::camera::{CreateCamera, UpdateCamera},
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Response DTO --

/// Camera as returned by the API — `password_enc` is never exposed.
#[derive(Serialize)]
pub struct CameraDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    /// Optional camera sub-stream URL used as the relay source (low-res).
    pub sub_rtsp_url: Option<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    pub extra_config: serde_json::Value,
    pub ring_buffer_duration_secs: i32,
    pub ring_buffer_storage: RingBufferStorage,
    pub enabled: bool,
    /// `true` if a GStreamer recording pipeline is currently active.
    pub recording: bool,
    /// RTSP relay URL served by this backend. `null` until relay is started.
    pub relay_url: Option<String>,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl CameraDto {
    fn from_model(m: camera::Model, recording: bool, relay_url: Option<String>) -> Self {
        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            rtsp_url: m.rtsp_url,
            sub_rtsp_url: m.sub_rtsp_url,
            manufacturer: m.manufacturer,
            model: m.model,
            username: m.username,
            extra_config: m.extra_config,
            ring_buffer_duration_secs: m.ring_buffer_duration_secs,
            ring_buffer_storage: m.ring_buffer_storage,
            enabled: m.enabled,
            recording,
            relay_url,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

// -- Request bodies --

#[derive(Deserialize)]
pub struct CreateCameraBody {
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub sub_rtsp_url: Option<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub extra_config: Option<serde_json::Value>,
    pub ring_buffer_duration_secs: Option<i32>,
    pub ring_buffer_storage: Option<RingBufferStorage>,
    pub enabled: Option<bool>,
}

/// All fields optional — only supplied fields are updated.
/// Clearing a nullable field to `null` is not supported in v0.1 (omit to leave unchanged).
#[derive(Deserialize)]
pub struct UpdateCameraBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub rtsp_url: Option<String>,
    pub sub_rtsp_url: Option<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub extra_config: Option<serde_json::Value>,
    pub ring_buffer_duration_secs: Option<i32>,
    pub ring_buffer_storage: Option<RingBufferStorage>,
    pub enabled: Option<bool>,
}

// -- Internal helpers --

/// Inject `user:pass@` into an RTSP URL immediately after the scheme prefix.
fn build_rtsp_url(base_url: &str, username: Option<&str>, password: Option<&str>) -> String {
    if let (Some(u), Some(p)) = (username, password) {
        if let Some(rest) = base_url.strip_prefix("rtsp://") {
            return format!("rtsp://{}:{}@{}", u, p, rest);
        }
    }
    base_url.to_string()
}

// -- Handlers --

/// GET /cameras
#[handler]
pub async fn list_cameras(depot: &mut Depot) -> Result<Json<Vec<CameraDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let cameras = state.camera_repo.list().await?;
    let dtos = cameras
        .into_iter()
        .map(|m| {
            let recording = state.media_manager.is_running(m.id);
            let relay_url = state.media_manager.relay_url(m.id);
            CameraDto::from_model(m, recording, relay_url)
        })
        .collect();
    Ok(Json(dtos))
}

/// POST /cameras
#[handler]
pub async fn create_camera(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<CameraDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateCameraBody = parse_body(req).await?;

    let input = CreateCamera {
        name: body.name,
        description: body.description,
        rtsp_url: body.rtsp_url,
        sub_rtsp_url: body.sub_rtsp_url,
        manufacturer: body.manufacturer,
        model: body.model,
        username: body.username,
        password: body.password,
        extra_config: body.extra_config.unwrap_or_else(|| serde_json::json!({})),
        ring_buffer_duration_secs: body.ring_buffer_duration_secs.unwrap_or(300),
        ring_buffer_storage: body
            .ring_buffer_storage
            .unwrap_or(RingBufferStorage::Memory),
        enabled: body.enabled.unwrap_or(true),
    };

    let camera = state.camera_repo.create(input).await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(CameraDto::from_model(camera, false, None)))
}

/// GET /cameras/{id}
#[handler]
pub async fn get_camera(req: &mut Request, depot: &mut Depot) -> Result<Json<CameraDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let camera = state
        .camera_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;
    let recording = state.media_manager.is_running(id);
    let relay_url = state.media_manager.relay_url(id);
    Ok(Json(CameraDto::from_model(camera, recording, relay_url)))
}

/// PATCH /cameras/{id}
#[handler]
pub async fn update_camera(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<CameraDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateCameraBody = parse_body(req).await?;

    let input = UpdateCamera {
        name: body.name,
        description: body.description.map(Some),
        rtsp_url: body.rtsp_url,
        sub_rtsp_url: body.sub_rtsp_url.map(Some),
        manufacturer: body.manufacturer.map(Some),
        model: body.model.map(Some),
        username: body.username.map(Some),
        password: body.password.map(Some),
        extra_config: body.extra_config,
        ring_buffer_duration_secs: body.ring_buffer_duration_secs,
        ring_buffer_storage: body.ring_buffer_storage,
        enabled: body.enabled,
    };

    let camera = state.camera_repo.update(id, input).await?;
    let recording = state.media_manager.is_running(id);
    let relay_url = state.media_manager.relay_url(id);
    Ok(Json(CameraDto::from_model(camera, recording, relay_url)))
}

/// DELETE /cameras/{id}
#[handler]
pub async fn delete_camera(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    if state.media_manager.is_running(id) {
        state.media_manager.stop_camera(id).await?;
    }

    state.camera_repo.delete(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// POST /cameras/{id}/recording/start
#[handler]
pub async fn start_recording(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<serde_json::Value>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    let (camera, password) = state
        .camera_repo
        .get_decrypted(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;

    if !camera.enabled {
        return Err(ApiError::bad_request("camera is disabled"));
    }

    let rtsp_url = build_rtsp_url(
        &camera.rtsp_url,
        camera.username.as_deref(),
        password.as_deref(),
    );

    state.media_manager.start_camera(id, &rtsp_url).await?;
    Ok(Json(serde_json::json!({"recording": true})))
}

/// POST /cameras/{id}/recording/stop
#[handler]
pub async fn stop_recording(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    state.media_manager.stop_camera(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// POST /cameras/{id}/relay/start
///
/// Starts the RTSP relay independently of recording. Probes the source URL for
/// the codec, then registers a relay factory on the RTSP server.
///
/// Source URL priority:
///   1. `sub_rtsp_url` (camera's own low-res sub-stream) — if set on the camera.
///   2. `rtsp_url` (main stream) — fallback when no sub-stream is configured.
///
/// Credentials (username / password) are injected into whichever URL is used.
#[handler]
pub async fn start_relay(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<serde_json::Value>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    let (camera, password) = state
        .camera_repo
        .get_decrypted(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;

    if !camera.enabled {
        return Err(ApiError::bad_request("camera is disabled"));
    }

    let source_url = match &camera.sub_rtsp_url {
        Some(sub) => build_rtsp_url(sub, camera.username.as_deref(), password.as_deref()),
        None => build_rtsp_url(&camera.rtsp_url, camera.username.as_deref(), password.as_deref()),
    };

    state.media_manager.start_relay(id, &source_url).await?;

    let relay_url = state.media_manager.relay_url(id);
    Ok(Json(serde_json::json!({ "relay_url": relay_url })))
}

/// POST /cameras/{id}/relay/stop
#[handler]
pub async fn stop_relay(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    state.media_manager.stop_relay(id);
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}
