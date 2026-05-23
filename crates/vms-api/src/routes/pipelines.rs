use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::pipeline::{self, PipelineType},
    repos::pipeline::{CreatePipeline, UpdatePipeline},
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Response DTO --------------------------------------------------------------

#[derive(Serialize)]
pub struct PipelineDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub pipeline_type: PipelineType,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<pipeline::Model> for PipelineDto {
    fn from(m: pipeline::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            pipeline_type: m.pipeline_type,
            enabled: m.enabled,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

// -- Request bodies ------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreatePipelineBody {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub pipeline_type: Option<PipelineType>,
}

#[derive(Deserialize)]
pub struct UpdatePipelineBody {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
}

// -- Handlers ------------------------------------------------------------------

/// GET /pipelines
#[handler]
pub async fn list_pipelines(depot: &mut Depot) -> Result<Json<Vec<PipelineDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipelines = state.pipeline_repo.list_all().await?;
    Ok(Json(pipelines.into_iter().map(PipelineDto::from).collect()))
}

/// POST /pipelines
#[handler]
pub async fn create_pipeline(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<PipelineDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreatePipelineBody = parse_body(req).await?;

    let pipeline = state
        .pipeline_repo
        .create(CreatePipeline {
            name: body.name,
            description: body.description,
            pipeline_type: body.pipeline_type.unwrap_or(PipelineType::User),
        })
        .await?;

    res.status_code(StatusCode::CREATED);
    Ok(Json(PipelineDto::from(pipeline)))
}

/// GET /pipelines/:id
#[handler]
pub async fn get_pipeline(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    let pipeline = state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    Ok(Json(PipelineDto::from(pipeline)))
}

/// PATCH /pipelines/:id
#[handler]
pub async fn update_pipeline(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdatePipelineBody = parse_body(req).await?;

    // Verify it exists first.
    state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    state
        .pipeline_repo
        .update(
            id,
            UpdatePipeline {
                name: body.name,
                description: body.description,
            },
        )
        .await?;

    let updated = state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    Ok(Json(PipelineDto::from(updated)))
}

/// DELETE /pipelines/:id
#[handler]
pub async fn delete_pipeline(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    state.pipeline_repo.delete(id).await?;

    // Remove from registry if it was enabled.
    state.pipeline_registry.reload().await?;

    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// POST /pipelines/:id/enable
#[handler]
pub async fn enable_pipeline(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineDto>, ApiError> {
    set_enabled(req, depot, true).await
}

/// POST /pipelines/:id/disable
#[handler]
pub async fn disable_pipeline(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineDto>, ApiError> {
    set_enabled(req, depot, false).await
}

async fn set_enabled(
    req: &mut Request,
    depot: &mut Depot,
    enabled: bool,
) -> Result<Json<PipelineDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    state.pipeline_repo.set_enabled(id, enabled).await?;
    state.pipeline_registry.reload().await?;

    let updated = state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    Ok(Json(PipelineDto::from(updated)))
}
