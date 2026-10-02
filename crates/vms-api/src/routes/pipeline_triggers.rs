//! Pipeline trigger CRUD: `/pipelines/{id}/triggers[/{trigger_id}]`.
//!
//! This is where a trigger's `source_id`/`camera_id` are set, the two FK
//! columns used to derive `pipeline_source_refs`/`pipeline_camera_ref`.
//!
//! `config` is `vms_core::TriggerConfig` directly. Its serde tag
//! (`trigger_type`) is the discriminator, so there is no separate
//! `trigger_type` field to keep in sync (as with `action_config` in
//! `pipeline_nodes.rs`). Responses return [`PipelineTrigger`] directly because
//! it already derives `Serialize` and has no sensitive fields.

use salvo::prelude::*;
use serde::Deserialize;
use uuid::Uuid;
use vms_core::{PipelineTrigger, TriggerConfig};
use vms_db::repos::pipeline::{CreateTrigger, UpdateTrigger};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Request bodies --

#[derive(Deserialize)]
pub struct CreateTriggerBody {
    pub config: TriggerConfig,
    pub source_id: Option<Uuid>,
    pub camera_id: Option<Uuid>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

/// All fields are optional; only supplied fields are updated. `config` may
/// change a trigger's parameters but not its variant; see [`UpdateTrigger`]'s
/// doc comment.
#[derive(Deserialize)]
pub struct UpdateTriggerBody {
    pub config: Option<TriggerConfig>,
    pub source_id: Option<Uuid>,
    pub camera_id: Option<Uuid>,
    pub enabled: Option<bool>,
}

// -- Handlers --

/// POST /pipelines/{id}/triggers
#[handler]
pub async fn create_trigger(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<PipelineTrigger>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let body: CreateTriggerBody = parse_body(req).await?;

    let trigger = state
        .pipeline_repo
        .create_trigger(
            pipeline_id,
            CreateTrigger {
                config: body.config,
                source_id: body.source_id,
                camera_id: body.camera_id,
                enabled: body.enabled,
            },
        )
        .await?;

    state.refresh_pipelines().await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(trigger))
}

/// GET /pipelines/{id}/triggers
#[handler]
pub async fn list_triggers(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<PipelineTrigger>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let triggers = state.pipeline_repo.load_triggers(pipeline_id).await?;
    Ok(Json(triggers))
}

/// GET /pipelines/{id}/triggers/{trigger_id}
#[handler]
pub async fn get_trigger(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineTrigger>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, trigger_id) = parse_pipeline_and_trigger_id(req)?;

    let trigger = state
        .pipeline_repo
        .get_trigger(trigger_id)
        .await?
        .filter(|t| t.pipeline_id == pipeline_id)
        .ok_or_else(|| ApiError::not_found(format!("trigger {trigger_id} not found")))?;

    Ok(Json(trigger))
}

/// PATCH /pipelines/{id}/triggers/{trigger_id}
#[handler]
pub async fn update_trigger(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineTrigger>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, trigger_id) = parse_pipeline_and_trigger_id(req)?;
    let body: UpdateTriggerBody = parse_body(req).await?;

    require_trigger_in_pipeline(state, pipeline_id, trigger_id).await?;

    let trigger = state
        .pipeline_repo
        .update_trigger(
            trigger_id,
            UpdateTrigger {
                config: body.config,
                source_id: body.source_id.map(Some),
                camera_id: body.camera_id.map(Some),
                enabled: body.enabled,
            },
        )
        .await?;

    state.refresh_pipelines().await?;
    Ok(Json(trigger))
}

/// DELETE /pipelines/{id}/triggers/{trigger_id}
#[handler]
pub async fn delete_trigger(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let (pipeline_id, trigger_id) = parse_pipeline_and_trigger_id(req)?;

    require_trigger_in_pipeline(state, pipeline_id, trigger_id).await?;
    state.pipeline_repo.delete_trigger(trigger_id).await?;
    state.refresh_pipelines().await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

// -- Helpers --

fn parse_pipeline_and_trigger_id(req: &mut Request) -> Result<(Uuid, Uuid), ApiError> {
    let pipeline_id = parse_id(req)?;
    let trigger_id: Uuid = req
        .param::<String>("trigger_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid trigger_id: expected UUID"))?;
    Ok((pipeline_id, trigger_id))
}

async fn require_trigger_in_pipeline(
    state: &AppState,
    pipeline_id: Uuid,
    trigger_id: Uuid,
) -> Result<(), ApiError> {
    let belongs = state
        .pipeline_repo
        .get_trigger(trigger_id)
        .await?
        .is_some_and(|t| t.pipeline_id == pipeline_id);
    if !belongs {
        return Err(ApiError::not_found(format!(
            "trigger {trigger_id} not found"
        )));
    }
    Ok(())
}
