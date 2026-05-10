use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::VmsError;
use vms_db::{
    entities::destination::{self, DestinationType},
    repos::destination::{CreateDestination, UpdateDestination},
};

use crate::state::AppState;

// ── Credential masking ────────────────────────────────────────────────────────

const CREDENTIAL_FIELDS: &[&str] = &[
    "password",
    "token",
    "api_key",
    "bearer_token",
    "secret_access_key",
    "private_key",
    "auth_token",
    "bot_token",
    "webhook_url",
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

// ── DTOs ──────────────────────────────────────────────────────────────────────

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
    /// Adapter-specific config. Credential fields should be provided in plaintext;
    /// the server encrypts them before storage.
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

// ── Handlers ──────────────────────────────────────────────────────────────────

/// GET /destinations
#[handler]
pub async fn list_destinations(depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    match state.dest_repo.list().await {
        Ok(dests) => res.render(Json(dests.into_iter().map(DestinationDto::from).collect::<Vec<_>>())),
        Err(e) => err_internal(res, &e),
    }
}

/// POST /destinations
#[handler]
pub async fn create_destination(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateDestinationBody = match req.parse_json().await {
        Ok(b) => b,
        Err(e) => {
            err_bad_request(res, &e.to_string());
            return;
        }
    };

    let input = CreateDestination {
        name: body.name,
        description: body.description,
        dest_type: body.dest_type,
        config: body.config.unwrap_or_else(|| serde_json::json!({})),
        enabled: body.enabled.unwrap_or(true),
    };

    match state.dest_repo.create(input).await {
        Ok(dest) => {
            res.status_code(StatusCode::CREATED);
            res.render(Json(DestinationDto::from(dest)));
        }
        Err(e) => err_internal(res, &e),
    }
}

/// GET /destinations/:id
#[handler]
pub async fn get_destination(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    match state.dest_repo.get(id).await {
        Ok(Some(dest)) => res.render(Json(DestinationDto::from(dest))),
        Ok(None) => err_not_found(res, &format!("destination {id} not found")),
        Err(e) => err_internal(res, &e),
    }
}

/// PATCH /destinations/:id
#[handler]
pub async fn update_destination(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };
    let body: UpdateDestinationBody = match req.parse_json().await {
        Ok(b) => b,
        Err(e) => {
            err_bad_request(res, &e.to_string());
            return;
        }
    };

    let input = UpdateDestination {
        name: body.name,
        description: body.description.map(Some),
        dest_type: body.dest_type,
        config: body.config,
        enabled: body.enabled,
    };

    match state.dest_repo.update(id, input).await {
        Ok(dest) => res.render(Json(DestinationDto::from(dest))),
        Err(VmsError::DestinationNotFound(_)) => {
            err_not_found(res, &format!("destination {id} not found"))
        }
        Err(e) => err_internal(res, &e),
    }
}

/// DELETE /destinations/:id
#[handler]
pub async fn delete_destination(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    match state.dest_repo.delete(id).await {
        Ok(()) => {
            res.status_code(StatusCode::NO_CONTENT);
        }
        Err(VmsError::DestinationNotFound(_)) => {
            err_not_found(res, &format!("destination {id} not found"))
        }
        Err(e) => err_internal(res, &e),
    }
}
