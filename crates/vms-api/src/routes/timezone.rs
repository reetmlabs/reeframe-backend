use salvo::prelude::*;
use serde::{Deserialize, Serialize};

use vms_db::repos::camera::UpdateCamera;

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

/// Per-camera override of the global `[recordings] timezone`. `null` means
/// "no override, inherit the global default."
#[derive(Serialize)]
pub struct CameraTimezoneDto {
    pub timezone: Option<String>,
}

/// `null` clears the override (falls back to the global default); omitting
/// the field leaves the current override unchanged.
#[derive(Deserialize)]
pub struct UpdateCameraTimezoneBody {
    pub timezone: Option<Option<String>>,
}

fn to_dto(camera: vms_db::entities::camera::Model) -> CameraTimezoneDto {
    CameraTimezoneDto {
        timezone: camera.timezone,
    }
}

/// GET /cameras/{id}/timezone
#[handler]
pub async fn get_camera_timezone(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<CameraTimezoneDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let camera = state
        .camera_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;
    Ok(Json(to_dto(camera)))
}

/// PATCH /cameras/{id}/timezone
#[handler]
pub async fn update_camera_timezone(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<CameraTimezoneDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateCameraTimezoneBody = parse_body(req).await?;

    let camera = state
        .camera_repo
        .update(
            id,
            UpdateCamera {
                name: None,
                description: None,
                rtsp_url: None,
                sub_rtsp_url: None,
                manufacturer: None,
                model: None,
                username: None,
                password: None,
                extra_config: None,
                ring_buffer_duration_secs: None,
                ring_buffer_storage: None,
                enabled: None,
                retention_days: None,
                retention_disk_threshold_percent: None,
                timezone: body.timezone,
                motion_detection_enabled: None,
                thumbnails_enabled: None,
            },
        )
        .await?;

    Ok(Json(to_dto(camera)))
}
