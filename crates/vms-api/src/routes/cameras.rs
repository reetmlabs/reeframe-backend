use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::camera::{self, RingBufferStorage},
    repos::camera::{CreateCamera, UpdateCamera},
};
use vms_media::RelayQuality;

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
    /// Main-quality (full resolution) RTSP relay URL, intended for
    /// full-screen live view. `null` until that relay is started.
    pub relay_url: Option<String>,
    /// Sub-quality (low resolution) RTSP relay URL, intended for tile/grid
    /// live view. `null` until that relay is started, or if the camera has
    /// no `sub_rtsp_url` configured.
    pub sub_relay_url: Option<String>,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl CameraDto {
    fn from_model(
        m: camera::Model,
        recording: bool,
        relay_url: Option<String>,
        sub_relay_url: Option<String>,
    ) -> Self {
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
            sub_relay_url,
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
            let relay_url = state.media_manager.relay_url(m.id, RelayQuality::Main);
            let sub_relay_url = state.media_manager.relay_url(m.id, RelayQuality::Sub);
            CameraDto::from_model(m, recording, relay_url, sub_relay_url)
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
    Ok(Json(CameraDto::from_model(camera, false, None, None)))
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
    let relay_url = state.media_manager.relay_url(id, RelayQuality::Main);
    let sub_relay_url = state.media_manager.relay_url(id, RelayQuality::Sub);
    Ok(Json(CameraDto::from_model(
        camera,
        recording,
        relay_url,
        sub_relay_url,
    )))
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
    let relay_url = state.media_manager.relay_url(id, RelayQuality::Main);
    let sub_relay_url = state.media_manager.relay_url(id, RelayQuality::Sub);
    Ok(Json(CameraDto::from_model(
        camera,
        recording,
        relay_url,
        sub_relay_url,
    )))
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

    // Resolve credentials into the sub-stream URL too, if the camera has
    // one — `MediaManager::start_camera` starts a persistent sub-stream
    // pipeline from it (tapped by motion detection by default, and later by
    // a sub-quality relay), the same low-res-first precedence `start_relay`
    // above already uses.
    let sub_rtsp_url = camera
        .sub_rtsp_url
        .as_deref()
        .map(|sub| build_rtsp_url(sub, camera.username.as_deref(), password.as_deref()));

    state
        .media_manager
        .start_camera(id, &rtsp_url, sub_rtsp_url.as_deref())
        .await?;
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

/// Parse the `?quality=main|sub` query parameter. Defaults to `Main` when
/// absent, matching the pre-existing single-relay behavior for callers that
/// don't know about the sub-quality relay yet.
fn parse_relay_quality(req: &mut Request) -> Result<RelayQuality, ApiError> {
    match req.query::<String>("quality").as_deref() {
        None | Some("main") => Ok(RelayQuality::Main),
        Some("sub") => Ok(RelayQuality::Sub),
        Some(other) => Err(ApiError::bad_request(format!(
            "invalid quality '{other}' — expected 'main' or 'sub'"
        ))),
    }
}

/// POST /cameras/{id}/relay/start?quality=main|sub
///
/// Starts an RTSP relay mount bridged from the camera's already-running
/// main or sub-stream pipeline (`?quality=sub` requires recording to have
/// been started with a `sub_rtsp_url` configured) — never a new connection
/// to the camera. Probes the codec on first use (any quality — main and sub
/// are assumed to share one encoding), then registers the mount.
#[handler]
pub async fn start_relay(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<serde_json::Value>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let quality = parse_relay_quality(req)?;

    let camera = state
        .camera_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;

    if !camera.enabled {
        return Err(ApiError::bad_request("camera is disabled"));
    }

    let had_cached_codec = camera.codec.is_some();
    let codec = state
        .media_manager
        .start_relay(id, quality, camera.codec.as_deref())
        .await?;

    // Persist the detected codec so future daemon restarts can skip the probe.
    if !had_cached_codec {
        if let Err(e) = state.camera_repo.set_codec(id, &codec).await {
            tracing::warn!(camera_id = %id, error = %e, "Failed to persist detected codec");
        }
    }

    let relay_url = state.media_manager.relay_url(id, quality);
    Ok(Json(serde_json::json!({ "relay_url": relay_url })))
}

/// POST /cameras/{id}/relay/stop?quality=main|sub
#[handler]
pub async fn stop_relay(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let quality = parse_relay_quality(req)?;
    state.media_manager.stop_relay(id, quality);
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}
