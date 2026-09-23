use salvo::prelude::*;
use serde::{Deserialize, Serialize};

use vms_db::repos::camera::UpdateCamera;

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

/// Per-camera override of the global `[recordings]` retention settings.
/// `null` means "no override, inherit the global default."
#[derive(Serialize)]
pub struct RetentionPolicyDto {
    pub retention_days: Option<i32>,
    pub retention_disk_threshold_percent: Option<f64>,
}

/// `null` clears the override (falls back to the global default); omitting
/// a field leaves its current override unchanged.
#[derive(Deserialize)]
pub struct UpdateRetentionPolicyBody {
    pub retention_days: Option<Option<i32>>,
    pub retention_disk_threshold_percent: Option<Option<f64>>,
}

fn to_dto(camera: vms_db::entities::camera::Model) -> RetentionPolicyDto {
    RetentionPolicyDto {
        retention_days: camera.retention_days,
        retention_disk_threshold_percent: camera.retention_disk_threshold_percent,
    }
}

/// GET /cameras/{id}/retention-policy
#[handler]
pub async fn get_retention_policy(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<RetentionPolicyDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let camera = state
        .camera_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("camera {id} not found")))?;
    Ok(Json(to_dto(camera)))
}

/// PATCH /cameras/{id}/retention-policy
#[handler]
pub async fn update_retention_policy(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<RetentionPolicyDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateRetentionPolicyBody = parse_body(req).await?;

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
                retention_days: body.retention_days,
                retention_disk_threshold_percent: body.retention_disk_threshold_percent,
                timezone: None,
            },
        )
        .await?;

    Ok(Json(to_dto(camera)))
}
