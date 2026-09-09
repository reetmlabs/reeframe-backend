use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::{
        pipeline::{self, PipelineType},
        pipeline_run, run_node_result,
    },
    repos::pipeline::{CreatePipeline, EnableOutcome, UpdatePipeline},
    repos::{ValidationCategory, ValidationIssue, ValidationSeverity},
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- Response DTO --

#[derive(Serialize)]
pub struct PipelineDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub pipeline_type: PipelineType,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
    pub validation_error_count: usize,
    pub validation_warning_count: usize,
}

impl From<pipeline::Model> for PipelineDto {
    fn from(m: pipeline::Model) -> Self {
        // Already computed and stored by PipelineRepo::revalidate — just
        // counting, no graph analysis on this (hot, list-heavy) path.
        let issues: Vec<ValidationIssue> =
            serde_json::from_value(m.validation_issues).unwrap_or_default();
        let validation_error_count = issues
            .iter()
            .filter(|i| i.severity == ValidationSeverity::Error)
            .count();
        let validation_warning_count = issues
            .iter()
            .filter(|i| i.severity == ValidationSeverity::Warning)
            .count();

        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            pipeline_type: m.pipeline_type,
            enabled: m.enabled,
            created_at: m.created_at,
            updated_at: m.updated_at,
            validation_error_count,
            validation_warning_count,
        }
    }
}

/// One problem with a pipeline's current definition, as reported by
/// `GET /pipelines/{id}/validation`.
#[derive(Serialize)]
pub struct ValidationIssueDto {
    pub node_id: Option<Uuid>,
    pub category: ValidationCategory,
    pub severity: ValidationSeverity,
    pub message: String,
}

impl From<ValidationIssue> for ValidationIssueDto {
    fn from(i: ValidationIssue) -> Self {
        Self {
            node_id: i.node_id,
            category: i.category,
            severity: i.severity,
            message: i.message,
        }
    }
}

// -- Run / node-result DTOs --

#[derive(Serialize)]
pub struct PipelineRunDto {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub trigger_id: Option<Uuid>,
    pub triggered_at: chrono::DateTime<chrono::FixedOffset>,
    pub status: vms_db::entities::pipeline_run::RunStatus,
    pub completed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    pub error: Option<String>,
}

impl From<pipeline_run::Model> for PipelineRunDto {
    fn from(m: pipeline_run::Model) -> Self {
        Self {
            id: m.id,
            pipeline_id: m.pipeline_id,
            trigger_id: m.trigger_id,
            triggered_at: m.triggered_at,
            status: m.status,
            completed_at: m.completed_at,
            error: m.error,
        }
    }
}

#[derive(Serialize)]
pub struct NodeResultDto {
    pub id: Uuid,
    pub node_id: Uuid,
    pub status: vms_db::entities::run_node_result::NodeResultStatus,
    pub started_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    pub completed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    pub output: serde_json::Value,
    pub error: Option<String>,
}

impl From<run_node_result::Model> for NodeResultDto {
    fn from(m: run_node_result::Model) -> Self {
        Self {
            id: m.id,
            node_id: m.node_id,
            status: m.status,
            started_at: m.started_at,
            completed_at: m.completed_at,
            output: m.output,
            error: m.error,
        }
    }
}

#[derive(Serialize)]
pub struct PipelineRunDetailDto {
    #[serde(flatten)]
    pub run: PipelineRunDto,
    pub nodes: Vec<NodeResultDto>,
}

// -- Request bodies --

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

#[derive(Deserialize)]
pub struct TriggerPipelineBody {
    pub params: Option<serde_json::Value>,
}

// -- Handlers --

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
    state.refresh_pipelines().await?;

    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// POST /pipelines/:id/enable
#[handler]
pub async fn enable_pipeline(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    set_enabled(req, depot, res, true).await
}

/// POST /pipelines/:id/disable
#[handler]
pub async fn disable_pipeline(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    set_enabled(req, depot, res, false).await
}

/// Body returned instead of the pipeline when enabling is refused.
#[derive(Serialize)]
struct EnableBlockedDto {
    error: &'static str,
    issues: Vec<ValidationIssueDto>,
}

async fn set_enabled(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    enabled: bool,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    match state.pipeline_repo.set_enabled(id, enabled).await? {
        EnableOutcome::BlockedByErrors(issues) => {
            res.status_code(StatusCode::CONFLICT);
            res.render(Json(EnableBlockedDto {
                error: "pipeline has validation errors and cannot be enabled",
                issues: issues.into_iter().map(ValidationIssueDto::from).collect(),
            }));
        }
        EnableOutcome::Applied => {
            state.refresh_pipelines().await?;
            let updated = state
                .pipeline_repo
                .get(id)
                .await?
                .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;
            res.render(Json(PipelineDto::from(updated)));
        }
    }
    Ok(())
}

/// POST /pipelines/:id/trigger
#[handler]
pub async fn trigger_pipeline(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: TriggerPipelineBody = parse_body(req).await?;

    state
        .trigger_evaluator
        .fire_manual(id, body.params)
        .map_err(ApiError::from)?;

    res.status_code(StatusCode::ACCEPTED);
    Ok(())
}

/// GET /pipelines/:id/validation
#[handler]
pub async fn get_pipeline_validation(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<ValidationIssueDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    let pipeline = state
        .pipeline_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("pipeline {id} not found")))?;

    let issues: Vec<ValidationIssue> = serde_json::from_value(pipeline.validation_issues)
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(
        issues.into_iter().map(ValidationIssueDto::from).collect(),
    ))
}

/// GET /pipelines/:id/runs
#[handler]
pub async fn list_runs(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<PipelineRunDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let limit = req.query::<u64>("limit").unwrap_or(50).clamp(1, 200);

    let runs = state
        .pipeline_run_repo
        .list_runs_for_pipeline(id, limit)
        .await?;

    Ok(Json(runs.into_iter().map(PipelineRunDto::from).collect()))
}

/// GET /pipelines/:id/runs/:run_id
#[handler]
pub async fn get_run(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<PipelineRunDetailDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let pipeline_id = parse_id(req)?;
    let run_id: Uuid = req
        .param::<String>("run_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid run_id: expected UUID"))?;

    let run = state
        .pipeline_run_repo
        .get_run(run_id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found")))?;

    if run.pipeline_id != pipeline_id {
        return Err(ApiError::not_found(format!("run {run_id} not found")));
    }

    let nodes = state
        .pipeline_run_repo
        .list_node_results_for_run(run_id)
        .await?;

    Ok(Json(PipelineRunDetailDto {
        run: PipelineRunDto::from(run),
        nodes: nodes.into_iter().map(NodeResultDto::from).collect(),
    }))
}
