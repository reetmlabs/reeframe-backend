use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::source::{self, SourceType},
    repos::source::{CreateSource, UpdateSource},
    repos::CREDENTIAL_FIELDS,
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

/// A trigger left with an unresolved reference by a source delete/disable.
#[derive(Serialize)]
struct AffectedTriggerDto {
    trigger_id: Uuid,
    pipeline_id: Uuid,
}

impl From<vms_core::pipeline::PipelineTrigger> for AffectedTriggerDto {
    fn from(t: vms_core::pipeline::PipelineTrigger) -> Self {
        Self {
            trigger_id: t.id,
            pipeline_id: t.pipeline_id,
        }
    }
}

// -- Credential masking --

fn mask_config(mut config: serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::Object(ref mut map) = config {
        for &key in CREDENTIAL_FIELDS {
            if map.contains_key(key) {
                map.insert(key.to_owned(), serde_json::Value::String("***".to_owned()));
            }
        }
    }
    config
}

// -- DTOs --

#[derive(Serialize)]
pub struct SourceDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub source_type: SourceType,
    /// Credential fields are replaced with `"***"`.
    pub config: serde_json::Value,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<source::Model> for SourceDto {
    fn from(m: source::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            source_type: m.source_type,
            config: mask_config(m.config),
            enabled: m.enabled,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

#[derive(Deserialize)]
pub struct CreateSourceBody {
    pub name: String,
    pub description: Option<String>,
    pub source_type: SourceType,
    /// Credential fields should be plaintext; the server encrypts before storage.
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

/// All fields are optional; only supplied fields are updated.
#[derive(Deserialize)]
pub struct UpdateSourceBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub source_type: Option<SourceType>,
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

// -- Handlers --

/// GET /sources
#[handler]
pub async fn list_sources(depot: &mut Depot) -> Result<Json<Vec<SourceDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let sources = state.source_repo.list().await?;
    Ok(Json(sources.into_iter().map(SourceDto::from).collect()))
}

/// POST /sources
#[handler]
pub async fn create_source(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<SourceDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateSourceBody = parse_body(req).await?;

    let input = CreateSource {
        name: body.name,
        description: body.description,
        source_type: body.source_type,
        config: body.config.unwrap_or_else(|| serde_json::json!({})),
        enabled: body.enabled.unwrap_or(true),
    };

    let source = state.source_repo.create(input).await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(SourceDto::from(source)))
}

/// GET /sources/{id}
#[handler]
pub async fn get_source(req: &mut Request, depot: &mut Depot) -> Result<Json<SourceDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let source = state
        .source_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("source {id} not found")))?;
    Ok(Json(SourceDto::from(source)))
}

/// Response for `PATCH /sources/{id}`: the updated source, plus any
/// triggers whose unresolved-reference status changed as a side effect of
/// this update (only non-empty when `enabled` was flipped).
#[derive(Serialize)]
struct UpdateSourceResponse {
    #[serde(flatten)]
    source: SourceDto,
    affected_triggers: Vec<AffectedTriggerDto>,
}

/// PATCH /sources/{id}
#[handler]
pub async fn update_source(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<UpdateSourceResponse>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateSourceBody = parse_body(req).await?;

    let was_enabled = state
        .source_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("source {id} not found")))?
        .enabled;

    let input = UpdateSource {
        name: body.name,
        description: body.description.map(Some),
        source_type: body.source_type,
        config: body.config,
        enabled: body.enabled,
    };

    let source = state.source_repo.update(id, input).await?;

    // Only an actual enabled/disabled transition marks or clears dependent
    // triggers and gates the source's resource. Other field changes leave
    // them alone.
    let affected = match (was_enabled, source.enabled) {
        (true, false) => {
            state.resource_manager.disable_source(id).await;
            state.pipeline_repo.mark_source_disabled(id).await?
        }
        (false, true) => {
            state.resource_manager.enable_source(id).await?;
            state.pipeline_repo.clear_source_unresolved(id).await?
        }
        _ => Vec::new(),
    };
    state.refresh_pipelines().await?;

    Ok(Json(UpdateSourceResponse {
        source: SourceDto::from(source),
        affected_triggers: affected.into_iter().map(AffectedTriggerDto::from).collect(),
    }))
}

/// DELETE /sources/{id}
#[handler]
pub async fn delete_source(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    // Triggers that pointed at this source are unlinked and marked
    // unresolved, so they neither block the delete nor get dropped.
    let affected = state.pipeline_repo.unlink_deleted_source(id).await?;
    state.source_repo.delete(id).await?;
    state.refresh_pipelines().await?;

    res.render(Json(
        affected
            .into_iter()
            .map(AffectedTriggerDto::from)
            .collect::<Vec<_>>(),
    ));
    Ok(())
}
