use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::VmsError;
use vms_db::{
    entities::camera::{self, RingBufferStorage},
    repos::camera::{CreateCamera, UpdateCamera},
};

use crate::state::AppState;

// ── Response DTO ──────────────────────────────────────────────────────────────

/// Camera as returned by the API — `password_enc` is never exposed.
#[derive(Serialize)]
pub struct CameraDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    pub extra_config: serde_json::Value,
    pub ring_buffer_duration_secs: i32,
    pub ring_buffer_storage: RingBufferStorage,
    pub enabled: bool,
    /// `true` if a GStreamer recording pipeline is currently active for this camera.
    pub recording: bool,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl CameraDto {
    fn from_model(m: camera::Model, recording: bool) -> Self {
        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            rtsp_url: m.rtsp_url,
            manufacturer: m.manufacturer,
            model: m.model,
            username: m.username,
            extra_config: m.extra_config,
            ring_buffer_duration_secs: m.ring_buffer_duration_secs,
            ring_buffer_storage: m.ring_buffer_storage,
            enabled: m.enabled,
            recording,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

// ── Request bodies ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateCameraBody {
    pub name: String,
    pub description: Option<String>,
    pub rtsp_url: String,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub extra_config: Option<serde_json::Value>,
    pub ring_buffer_duration_secs: Option<i32>,
    pub ring_buffer_storage: Option<RingBufferStorage>,
    pub enabled: Option<bool>,
}

/// All fields are optional — only supplied fields are updated.
/// Setting a nullable field to `null` is not supported in v0.1 (omit to leave unchanged).
#[derive(Deserialize)]
pub struct UpdateCameraBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub rtsp_url: Option<String>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub extra_config: Option<serde_json::Value>,
    pub ring_buffer_duration_secs: Option<i32>,
    pub ring_buffer_storage: Option<RingBufferStorage>,
    pub enabled: Option<bool>,
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn err_internal(res: &mut Response, e: &VmsError) {
    tracing::error!(error = %e, "internal server error");
    res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
    res.render(Json(serde_json::json!({"error": e.to_string()})));
}

fn err_not_found(res: &mut Response, msg: &str) {
    res.status_code(StatusCode::NOT_FOUND);
    res.render(Json(serde_json::json!({"error": msg})));
}

fn err_bad_request(res: &mut Response, msg: &str) {
    res.status_code(StatusCode::BAD_REQUEST);
    res.render(Json(serde_json::json!({"error": msg})));
}

fn parse_id(req: &mut Request, res: &mut Response) -> Option<Uuid> {
    let s: String = req.param("id").unwrap_or_default();
    match s.parse::<Uuid>() {
        Ok(id) => Some(id),
        Err(_) => {
            err_bad_request(res, "invalid id: expected UUID");
            None
        }
    }
}

/// Build an authenticated RTSP URL from a base URL and optional credentials.
/// Injects `user:pass@` immediately after the `rtsp://` scheme prefix.
fn build_rtsp_url(base_url: &str, username: Option<&str>, password: Option<&str>) -> String {
    match (username, password) {
        (Some(u), Some(p)) => {
            if let Some(rest) = base_url.strip_prefix("rtsp://") {
                return format!("rtsp://{}:{}@{}", u, p, rest);
            }
        }
        _ => {}
    }
    base_url.to_string()
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// GET /cameras
#[handler]
pub async fn list_cameras(depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    match state.camera_repo.list().await {
        Ok(cameras) => {
            let dtos: Vec<CameraDto> = cameras
                .into_iter()
                .map(|m| {
                    let recording = state.media_manager.is_running(m.id);
                    CameraDto::from_model(m, recording)
                })
                .collect();
            res.render(Json(dtos));
        }
        Err(e) => err_internal(res, &e),
    }
}

/// POST /cameras
#[handler]
pub async fn create_camera(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateCameraBody = match req.parse_json().await {
        Ok(b) => b,
        Err(e) => {
            err_bad_request(res, &e.to_string());
            return;
        }
    };

    let input = CreateCamera {
        name: body.name,
        description: body.description,
        rtsp_url: body.rtsp_url,
        manufacturer: body.manufacturer,
        model: body.model,
        username: body.username,
        password: body.password,
        extra_config: body.extra_config.unwrap_or_else(|| serde_json::json!({})),
        ring_buffer_duration_secs: body.ring_buffer_duration_secs.unwrap_or(300),
        ring_buffer_storage: body.ring_buffer_storage.unwrap_or(RingBufferStorage::Memory),
        enabled: body.enabled.unwrap_or(true),
    };

    match state.camera_repo.create(input).await {
        Ok(camera) => {
            res.status_code(StatusCode::CREATED);
            res.render(Json(CameraDto::from_model(camera, false)));
        }
        Err(e) => err_internal(res, &e),
    }
}

/// GET /cameras/:id
#[handler]
pub async fn get_camera(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    match state.camera_repo.get(id).await {
        Ok(Some(camera)) => {
            let recording = state.media_manager.is_running(id);
            res.render(Json(CameraDto::from_model(camera, recording)));
        }
        Ok(None) => err_not_found(res, &format!("camera {id} not found")),
        Err(e) => err_internal(res, &e),
    }
}

/// PATCH /cameras/:id
#[handler]
pub async fn update_camera(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };
    let body: UpdateCameraBody = match req.parse_json().await {
        Ok(b) => b,
        Err(e) => {
            err_bad_request(res, &e.to_string());
            return;
        }
    };

    let input = UpdateCamera {
        name: body.name,
        description: body.description.map(Some),
        rtsp_url: body.rtsp_url,
        manufacturer: body.manufacturer.map(Some),
        model: body.model.map(Some),
        username: body.username.map(Some),
        password: body.password.map(Some),
        extra_config: body.extra_config,
        ring_buffer_duration_secs: body.ring_buffer_duration_secs,
        ring_buffer_storage: body.ring_buffer_storage,
        enabled: body.enabled,
    };

    match state.camera_repo.update(id, input).await {
        Ok(camera) => {
            let recording = state.media_manager.is_running(id);
            res.render(Json(CameraDto::from_model(camera, recording)));
        }
        Err(VmsError::CameraNotFound(_)) => {
            err_not_found(res, &format!("camera {id} not found"))
        }
        Err(e) => err_internal(res, &e),
    }
}

/// DELETE /cameras/:id
#[handler]
pub async fn delete_camera(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    if state.media_manager.is_running(id) {
        if let Err(e) = state.media_manager.stop_camera(id).await {
            err_internal(res, &e);
            return;
        }
    }

    match state.camera_repo.delete(id).await {
        Ok(()) => {
            res.status_code(StatusCode::NO_CONTENT);
        }
        Err(VmsError::CameraNotFound(_)) => {
            err_not_found(res, &format!("camera {id} not found"))
        }
        Err(e) => err_internal(res, &e),
    }
}

/// POST /cameras/:id/recording/start
#[handler]
pub async fn start_recording(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    let (camera, password) = match state.camera_repo.get_decrypted(id).await {
        Ok(Some(pair)) => pair,
        Ok(None) => {
            err_not_found(res, &format!("camera {id} not found"));
            return;
        }
        Err(e) => {
            err_internal(res, &e);
            return;
        }
    };

    if !camera.enabled {
        err_bad_request(res, "camera is disabled");
        return;
    }

    let rtsp_url = build_rtsp_url(
        &camera.rtsp_url,
        camera.username.as_deref(),
        password.as_deref(),
    );

    match state.media_manager.start_camera(id, &rtsp_url).await {
        Ok(()) => {
            res.render(Json(serde_json::json!({"recording": true})));
        }
        Err(e) => err_internal(res, &e),
    }
}

/// POST /cameras/:id/recording/stop
#[handler]
pub async fn stop_recording(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    match state.media_manager.stop_camera(id).await {
        Ok(()) => {
            res.status_code(StatusCode::NO_CONTENT);
        }
        Err(e) => err_internal(res, &e),
    }
}
