use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::camera::{self, LiveViewStream, RingBufferStorage},
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
    /// `true` if the camera's live GStreamer pipeline is currently running
    /// (viewing live does not imply recording — see `recording`).
    pub live: bool,
    /// `true` if a recording branch is currently attached and writing to
    /// disk. Implies `live`.
    pub recording: bool,
    /// Persisted operator intent — set by `recording/start`/`stop`, not by
    /// anything else. Can be `true` while `recording` is `false` (camera
    /// unreachable, mid-reconnect) — that combination means "trying to
    /// record but not currently succeeding," distinct from an operator
    /// having stopped it on purpose.
    pub desired_recording: bool,
    /// Whether motion detection runs while the camera is live. A pipeline
    /// with an `Event` trigger on this camera keeps it running anyway.
    pub motion_detection_enabled: bool,
    /// Whether scrub-preview thumbnails are captured while the camera records.
    pub thumbnails_enabled: bool,
    /// Which stream live view relays by default (`sub` falls back to `main`
    /// when the camera has no sub stream).
    pub live_view_stream: LiveViewStream,
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
        live: bool,
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
            live,
            recording,
            desired_recording: m.desired_recording,
            motion_detection_enabled: m.motion_detection_enabled,
            thumbnails_enabled: m.thumbnails_enabled,
            live_view_stream: m.live_view_stream,
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
    pub motion_detection_enabled: Option<bool>,
    pub thumbnails_enabled: Option<bool>,
    pub live_view_stream: Option<LiveViewStream>,
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
    pub motion_detection_enabled: Option<bool>,
    pub thumbnails_enabled: Option<bool>,
    pub live_view_stream: Option<LiveViewStream>,
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

/// Resolve a decrypted camera row's main + optional sub-stream RTSP URLs
/// with credentials injected — shared by every handler that may need to
/// start the camera's live pipeline (`recording/start`, `relay/start`).
fn resolve_camera_urls(camera: &camera::Model, password: Option<&str>) -> (String, Option<String>) {
    let rtsp_url = build_rtsp_url(&camera.rtsp_url, camera.username.as_deref(), password);
    let sub_rtsp_url = camera
        .sub_rtsp_url
        .as_deref()
        .map(|sub| build_rtsp_url(sub, camera.username.as_deref(), password));
    (rtsp_url, sub_rtsp_url)
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
            let live = state.media_manager.is_running(m.id);
            let recording = state.media_manager.is_recording(m.id);
            let relay_url = state.media_manager.relay_url(m.id, RelayQuality::Main);
            let sub_relay_url = state.media_manager.relay_url(m.id, RelayQuality::Sub);
            CameraDto::from_model(m, live, recording, relay_url, sub_relay_url)
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
        motion_detection_enabled: body.motion_detection_enabled.unwrap_or(false),
        thumbnails_enabled: body.thumbnails_enabled.unwrap_or(false),
        live_view_stream: body.live_view_stream.unwrap_or(LiveViewStream::Sub),
    };

    let camera = state.camera_repo.create(input).await?;
    state
        .media_manager
        .set_motion_detection_enabled(camera.id, camera.motion_detection_enabled)
        .await;
    state
        .media_manager
        .set_thumbnails_enabled(camera.id, camera.thumbnails_enabled)
        .await;
    res.status_code(StatusCode::CREATED);
    Ok(Json(CameraDto::from_model(
        camera, false, false, None, None,
    )))
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
    let live = state.media_manager.is_running(id);
    let recording = state.media_manager.is_recording(id);
    let relay_url = state.media_manager.relay_url(id, RelayQuality::Main);
    let sub_relay_url = state.media_manager.relay_url(id, RelayQuality::Sub);
    Ok(Json(CameraDto::from_model(
        camera,
        live,
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
    let urls_changed = body.rtsp_url.is_some()
        || body.sub_rtsp_url.is_some()
        || body.username.is_some()
        || body.password.is_some();

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
        retention_days: None,
        retention_disk_threshold_percent: None,
        timezone: None,
        motion_detection_enabled: body.motion_detection_enabled,
        thumbnails_enabled: body.thumbnails_enabled,
        live_view_stream: body.live_view_stream,
    };

    let camera = state.camera_repo.update(id, input).await?;
    if urls_changed && state.media_manager.is_running(id) {
        if let Some((camera, password)) = state.camera_repo.get_decrypted(id).await? {
            let (rtsp_url, sub_rtsp_url) = resolve_camera_urls(&camera, password.as_deref());
            state
                .media_manager
                .set_stream_urls(id, &rtsp_url, sub_rtsp_url.as_deref())
                .await;
        }
    }
    if body.motion_detection_enabled.is_some() {
        state
            .media_manager
            .set_motion_detection_enabled(id, camera.motion_detection_enabled)
            .await;
    }
    if body.thumbnails_enabled.is_some() {
        state
            .media_manager
            .set_thumbnails_enabled(id, camera.thumbnails_enabled)
            .await;
    }
    let live = state.media_manager.is_running(id);
    let recording = state.media_manager.is_recording(id);
    let relay_url = state.media_manager.relay_url(id, RelayQuality::Main);
    let sub_relay_url = state.media_manager.relay_url(id, RelayQuality::Sub);
    Ok(Json(CameraDto::from_model(
        camera,
        live,
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
        state.media_manager.stop_live(id).await?;
    }

    state.camera_repo.delete(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// POST /cameras/{id}/recording/start
///
/// Starts recording — brings the camera's live pipeline up first if it
/// isn't already running (recording never requires a separate "go live"
/// call first).
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

    let (rtsp_url, sub_rtsp_url) = resolve_camera_urls(&camera, password.as_deref());

    // Persisted before the actual attach attempt, and independent of
    // whether it succeeds — this is what lets boot recovery and reconnect
    // handling resume recording later even if it fails to start right now
    // (camera unreachable, etc.) instead of the intent being lost with it.
    state.camera_repo.set_desired_recording(id, true).await?;

    state
        .media_manager
        .start_recording(id, &rtsp_url, sub_rtsp_url.as_deref())
        .await?;
    Ok(Json(serde_json::json!({"recording": true})))
}

/// POST /cameras/{id}/recording/stop
///
/// Detaches the recording branch only — the live pipeline (and any active
/// relay/motion detection) keeps running. Use `DELETE /cameras/{id}` or stop
/// the relay separately to tear down live view entirely.
#[handler]
pub async fn stop_recording(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    // This is the only way intent is ever cleared — until this runs, boot
    // recovery and reconnect handling keep trying to resume recording.
    state.camera_repo.set_desired_recording(id, false).await?;

    state.media_manager.stop_recording(id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// Parse the optional `?quality=main|sub` query parameter.
fn parse_relay_quality(req: &mut Request) -> Result<Option<RelayQuality>, ApiError> {
    match req.query::<String>("quality").as_deref() {
        None => Ok(None),
        Some("main") => Ok(Some(RelayQuality::Main)),
        Some("sub") => Ok(Some(RelayQuality::Sub)),
        Some(other) => Err(ApiError::bad_request(format!(
            "invalid quality '{other}' — expected 'main' or 'sub'"
        ))),
    }
}

/// The relay quality live view uses for `camera`. A camera pinned to
/// `main` always gets main. Otherwise `requested` wins, defaulting to sub,
/// and sub falls back to main when the camera has no sub stream.
pub fn live_view_quality(camera: &camera::Model, requested: Option<RelayQuality>) -> RelayQuality {
    if camera.live_view_stream == LiveViewStream::Main {
        return RelayQuality::Main;
    }
    match requested.unwrap_or(RelayQuality::Sub) {
        RelayQuality::Sub if camera.sub_rtsp_url.is_none() => RelayQuality::Main,
        q => q,
    }
}

/// POST /cameras/{id}/relay/start?quality=main|sub
///
/// Starts an RTSP relay mount for live view — bridged from the camera's main
/// or sub-stream pipeline, starting whichever one is needed on demand if
/// it isn't already running. Defaults to the sub stream, falling back to
/// main without one; a camera whose `live_view_stream` is `main` always
/// gets main (see [`live_view_quality`]). Never requires recording.
/// Recording is a separate, explicit concern — see `POST
/// /cameras/{id}/recording/start`. Probes the codec on first use (any
/// quality — main and sub are assumed to share one encoding), then
/// registers the mount.
#[handler]
pub async fn start_relay(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<serde_json::Value>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let requested = parse_relay_quality(req)?;

    let (camera, password) = state
        .camera_repo
        .get_decrypted(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;

    if !camera.enabled {
        return Err(ApiError::bad_request("camera is disabled"));
    }

    let quality = live_view_quality(&camera, requested);
    let (rtsp_url, sub_rtsp_url) = resolve_camera_urls(&camera, password.as_deref());

    let had_cached_codec = camera.codec.is_some();
    let codec = state
        .media_manager
        .start_relay(
            id,
            quality,
            &rtsp_url,
            sub_rtsp_url.as_deref(),
            camera.codec.as_deref(),
        )
        .await?;

    // Persist the detected codec so future daemon restarts can skip the probe.
    if !had_cached_codec {
        if let Err(e) = state.camera_repo.set_codec(id, &codec).await {
            tracing::warn!(camera_id = %id, error = %e, "Failed to persist detected codec");
        }
    }

    let relay_url = state.media_manager.relay_url(id, quality);
    let quality = match quality {
        RelayQuality::Main => "main",
        RelayQuality::Sub => "sub",
    };
    Ok(Json(
        serde_json::json!({ "relay_url": relay_url, "quality": quality }),
    ))
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
    let requested = parse_relay_quality(req)?;
    let camera = state
        .camera_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;
    let quality = live_view_quality(&camera, requested);
    state.media_manager.stop_relay(id, quality).await;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

