use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_db::{
    entities::destination::{self, DestinationType},
    repos::destination::{CreateDestination, UpdateDestination},
};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

/// A node left with an unresolved reference by a destination delete/disable.
#[derive(Serialize)]
struct AffectedNodeDto {
    node_id: Uuid,
    pipeline_id: Uuid,
}

impl From<vms_core::pipeline::PipelineNode> for AffectedNodeDto {
    fn from(n: vms_core::pipeline::PipelineNode) -> Self {
        Self {
            node_id: n.id,
            pipeline_id: n.pipeline_id,
        }
    }
}

// -- Credential masking --

const CREDENTIAL_FIELDS: &[&str] = &[
    "password",
    "token",
    "api_key",
    "bearer_token",
    "secret_access_key",
    "private_key",
    "auth_token",
    "access_token",
    "bot_token",
    "webhook_url",
    "shared_secret",
    "client_key",
    "client_cert",
];

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
pub struct DestinationDto {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub dest_type: DestinationType,
    /// Credential fields are replaced with `"***"`.
    pub config: serde_json::Value,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<destination::Model> for DestinationDto {
    fn from(m: destination::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            description: m.description,
            dest_type: m.dest_type,
            config: mask_config(m.config),
            enabled: m.enabled,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

#[derive(Deserialize)]
pub struct CreateDestinationBody {
    pub name: String,
    pub description: Option<String>,
    pub dest_type: DestinationType,
    /// Credential fields should be plaintext; the server encrypts before storage.
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

/// All fields optional — only supplied fields are updated.
#[derive(Deserialize)]
pub struct UpdateDestinationBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub dest_type: Option<DestinationType>,
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

// -- Handlers --

/// GET /destinations
#[handler]
pub async fn list_destinations(depot: &mut Depot) -> Result<Json<Vec<DestinationDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let dests = state.dest_repo.list().await?;
    Ok(Json(dests.into_iter().map(DestinationDto::from).collect()))
}

/// POST /destinations
#[handler]
pub async fn create_destination(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<DestinationDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateDestinationBody = parse_body(req).await?;

    let input = CreateDestination {
        name: body.name,
        description: body.description,
        dest_type: body.dest_type,
        config: body.config.unwrap_or_else(|| serde_json::json!({})),
        enabled: body.enabled.unwrap_or(true),
    };

    let dest = state.dest_repo.create(input).await?;
    res.status_code(StatusCode::CREATED);
    Ok(Json(DestinationDto::from(dest)))
}

/// GET /destinations/{id}
#[handler]
pub async fn get_destination(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<DestinationDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let dest = state
        .dest_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("destination {id} not found")))?;
    Ok(Json(DestinationDto::from(dest)))
}

/// Response for `PATCH /destinations/{id}`: the updated destination, plus
/// any nodes whose unresolved-reference status changed as a side effect of
/// this update (only non-empty when `enabled` was flipped).
#[derive(Serialize)]
struct UpdateDestinationResponse {
    #[serde(flatten)]
    destination: DestinationDto,
    affected_nodes: Vec<AffectedNodeDto>,
}

/// PATCH /destinations/{id}
#[handler]
pub async fn update_destination(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<UpdateDestinationResponse>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;
    let body: UpdateDestinationBody = parse_body(req).await?;

    let was_enabled = state
        .dest_repo
        .get(id)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("destination {id} not found")))?
        .enabled;

    let input = UpdateDestination {
        name: body.name,
        description: body.description.map(Some),
        dest_type: body.dest_type,
        config: body.config,
        enabled: body.enabled,
    };

    let dest = state.dest_repo.update(id, input).await?;

    // Only a real enabled/disabled transition marks or clears dependent
    // nodes — every other field change leaves them alone.
    let affected = match (was_enabled, dest.enabled) {
        (true, false) => state.pipeline_repo.mark_destination_disabled(id).await?,
        (false, true) => state.pipeline_repo.clear_destination_unresolved(id).await?,
        _ => Vec::new(),
    };

    Ok(Json(UpdateDestinationResponse {
        destination: DestinationDto::from(dest),
        affected_nodes: affected.into_iter().map(AffectedNodeDto::from).collect(),
    }))
}

/// DELETE /destinations/{id}
#[handler]
pub async fn delete_destination(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let id = parse_id(req)?;

    // Nodes that pointed at this destination aren't dropped or left
    // blocking the delete — they're unlinked and marked unresolved instead.
    let affected = state.pipeline_repo.unlink_deleted_destination(id).await?;
    state.dest_repo.delete(id).await?;

    res.render(Json(
        affected
            .into_iter()
            .map(AffectedNodeDto::from)
            .collect::<Vec<_>>(),
    ));
    Ok(())
}
