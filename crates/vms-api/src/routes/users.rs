//! API key management, nested under `/users/{id}/api-keys`.
//!
//! There is no general `/users` CRUD yet — user creation is still only
//! possible via `POST /auth/setup` (step 9-2), and multi-user management is
//! Phase 2 work. Every handler here is self-service only: a caller may only
//! create, list, or revoke *their own* keys (`AuthClaims.user_id` must equal
//! the `{id}` path segment). There is no admin-manages-others path yet
//! either, since there's no second user or role-permission system to
//! exercise it — that's Phase 2's RBAC enforcement middleware, not this.

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::AuthClaims;
use vms_db::{entities::api_key, repos::api_key::CreateApiKey};

use crate::{
    error::{parse_body, parse_id, ApiError},
    state::AppState,
};

// -- DTOs --

#[derive(Serialize)]
pub struct ApiKeyDto {
    pub id: Uuid,
    pub name: String,
    pub last_used: Option<chrono::DateTime<chrono::FixedOffset>>,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<api_key::Model> for ApiKeyDto {
    fn from(m: api_key::Model) -> Self {
        Self {
            id: m.id,
            name: m.name,
            last_used: m.last_used,
            created_at: m.created_at,
        }
    }
}

/// Returned only from `create_api_key` — the one time the raw key is ever
/// available. Deliberately not `ApiKeyDto` + a bolted-on field, so it's
/// obvious at the type level that no other endpoint can produce this shape.
#[derive(Serialize)]
pub struct CreatedApiKeyDto {
    pub id: Uuid,
    pub name: String,
    pub key: String,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
}

#[derive(Deserialize)]
pub struct CreateApiKeyBody {
    pub name: String,
}

// -- Handlers --

/// POST /users/{id}/api-keys
#[handler]
pub async fn create_api_key(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<Json<CreatedApiKeyDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let user_id = parse_id(req)?;
    require_self(depot, user_id)?;
    let body: CreateApiKeyBody = parse_body(req).await?;

    let created = state
        .api_key_repo
        .create(CreateApiKey {
            user_id,
            name: body.name,
        })
        .await?;

    res.status_code(StatusCode::CREATED);
    Ok(Json(CreatedApiKeyDto {
        id: created.model.id,
        name: created.model.name,
        key: created.raw_key,
        created_at: created.model.created_at,
    }))
}

/// GET /users/{id}/api-keys
#[handler]
pub async fn list_api_keys(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<Vec<ApiKeyDto>>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let user_id = parse_id(req)?;
    require_self(depot, user_id)?;

    let keys = state.api_key_repo.list_for_user(user_id).await?;
    Ok(Json(keys.into_iter().map(ApiKeyDto::from).collect()))
}

/// DELETE /users/{id}/api-keys/{key_id}
#[handler]
pub async fn delete_api_key(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> Result<(), ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let user_id = parse_id(req)?;
    require_self(depot, user_id)?;
    let key_id: Uuid = req
        .param::<String>("key_id")
        .unwrap_or_default()
        .parse()
        .map_err(|_| ApiError::bad_request("invalid key_id: expected UUID"))?;

    state.api_key_repo.delete(user_id, key_id).await?;
    res.status_code(StatusCode::NO_CONTENT);
    Ok(())
}

/// Every handler above is self-service only — reject if the authenticated
/// caller isn't managing their own keys.
fn require_self(depot: &Depot, user_id: Uuid) -> Result<(), ApiError> {
    let claims = depot
        .obtain::<AuthClaims>()
        .expect("AuthClaims not in depot — auth middleware did not run");
    if claims.user_id != user_id {
        return Err(ApiError::forbidden("cannot manage another user's API keys"));
    }
    Ok(())
}
