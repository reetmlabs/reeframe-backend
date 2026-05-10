use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::VmsError;
use vms_db::{
    entities::source::{self, SourceType},
    repos::source::{CreateSource, UpdateSource},
};

use crate::state::AppState;

// ── Credential masking ────────────────────────────────────────────────────────

/// Keys whose values are masked with `"***"` in GET responses.
/// Must stay in sync with `vms_db::repos::CREDENTIAL_FIELDS`.
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
    /// Adapter-specific config. Credential fields should be provided in plaintext;
    /// the server encrypts them before storage.
    pub config: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

/// All fields optional — only supplied fields are updated.
#[derive(Deserialize)]
pub struct UpdateSourceBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub source_type: Option<SourceType>,
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

/// GET /sources
#[handler]
pub async fn list_sources(depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    match state.source_repo.list().await {
        Ok(sources) => res.render(Json(sources.into_iter().map(SourceDto::from).collect::<Vec<_>>())),
        Err(e) => err_internal(res, &e),
    }
}

/// POST /sources
#[handler]
pub async fn create_source(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: CreateSourceBody = match req.parse_json().await {
        Ok(b) => b,
        Err(e) => {
            err_bad_request(res, &e.to_string());
            return;
        }
    };

    let input = CreateSource {
        name: body.name,
        description: body.description,
        source_type: body.source_type,
        config: body.config.unwrap_or_else(|| serde_json::json!({})),
        enabled: body.enabled.unwrap_or(true),
    };

    match state.source_repo.create(input).await {
        Ok(source) => {
            res.status_code(StatusCode::CREATED);
            res.render(Json(SourceDto::from(source)));
        }
        Err(e) => err_internal(res, &e),
    }
}

/// GET /sources/:id
#[handler]
pub async fn get_source(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    match state.source_repo.get(id).await {
        Ok(Some(source)) => res.render(Json(SourceDto::from(source))),
        Ok(None) => err_not_found(res, &format!("source {id} not found")),
        Err(e) => err_internal(res, &e),
    }
}

/// PATCH /sources/:id
#[handler]
pub async fn update_source(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };
    let body: UpdateSourceBody = match req.parse_json().await {
        Ok(b) => b,
        Err(e) => {
            err_bad_request(res, &e.to_string());
            return;
        }
    };

    let input = UpdateSource {
        name: body.name,
        description: body.description.map(Some),
        source_type: body.source_type,
        config: body.config,
        enabled: body.enabled,
    };

    match state.source_repo.update(id, input).await {
        Ok(source) => res.render(Json(SourceDto::from(source))),
        Err(VmsError::SourceNotFound(_)) => err_not_found(res, &format!("source {id} not found")),
        Err(e) => err_internal(res, &e),
    }
}

/// DELETE /sources/:id
#[handler]
pub async fn delete_source(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let Some(id) = parse_id(req, res) else { return };

    match state.source_repo.delete(id).await {
        Ok(()) => {
            res.status_code(StatusCode::NO_CONTENT);
        }
        Err(VmsError::SourceNotFound(_)) => err_not_found(res, &format!("source {id} not found")),
        Err(e) => err_internal(res, &e),
    }
}
