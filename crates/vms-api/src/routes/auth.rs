//! Local authentication: first-run admin setup and username/password login.
//!
//! `POST /auth/setup` is the one intentionally unauthenticated *write*
//! endpoint in the whole API — by design, since there is no admin yet to
//! authenticate as. It refuses to run a second time once any user exists.

use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::AuthClaims;
use vms_db::{
    entities::user::{self, UserRole},
    repos::user::{verify_password, CreateUser},
};

use crate::{
    error::{parse_body, ApiError},
    state::AppState,
};

/// Passwords shorter than this are rejected at setup/creation time. Not a
/// full password policy — just a floor against trivially weak values.
const MIN_PASSWORD_LEN: usize = 8;

/// Generic message for every login failure mode (unknown username, wrong
/// password, disabled account) — distinguishing them in the response would
/// let a caller enumerate valid usernames or account state.
const INVALID_CREDENTIALS: &str = "invalid username or password";

// -- DTOs --

#[derive(Serialize)]
pub struct UserDto {
    pub id: Uuid,
    pub username: String,
    pub role: UserRole,
    pub enabled: bool,
    pub created_at: chrono::DateTime<chrono::FixedOffset>,
    pub updated_at: chrono::DateTime<chrono::FixedOffset>,
}

impl From<user::Model> for UserDto {
    fn from(m: user::Model) -> Self {
        Self {
            id: m.id,
            username: m.username,
            role: m.role,
            enabled: m.enabled,
            created_at: m.created_at,
            updated_at: m.updated_at,
        }
    }
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub user: UserDto,
}

#[derive(Deserialize)]
pub struct SetupBody {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct LoginBody {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct RefreshBody {
    pub refresh_token: String,
}

#[derive(Serialize)]
pub struct AccessTokenResponse {
    pub access_token: String,
}

// -- Handlers --

/// POST /auth/setup — create the first admin user. `409` once any user exists.
#[handler]
pub async fn setup(req: &mut Request, depot: &mut Depot) -> Result<Json<AuthResponse>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: SetupBody = parse_body(req).await?;

    if state.user_repo.count().await? > 0 {
        return Err(ApiError::conflict("setup has already been completed"));
    }
    if body.password.len() < MIN_PASSWORD_LEN {
        return Err(ApiError::bad_request(format!(
            "password must be at least {MIN_PASSWORD_LEN} characters"
        )));
    }

    let user = state
        .user_repo
        .create(CreateUser {
            username: body.username,
            password: body.password,
            role: UserRole::Admin,
        })
        .await?;

    issue_tokens(state, user)
}

/// POST /auth/login
#[handler]
pub async fn login(req: &mut Request, depot: &mut Depot) -> Result<Json<AuthResponse>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: LoginBody = parse_body(req).await?;

    let user = state
        .user_repo
        .get_by_username(&body.username)
        .await?
        .filter(|u| u.enabled)
        .filter(|u| verify_password(&body.password, &u.password_hash))
        .ok_or_else(|| ApiError::unauthorized(INVALID_CREDENTIALS))?;

    issue_tokens(state, user)
}

/// POST /auth/refresh — exchange a refresh token for a new access token.
/// Does **not** rotate the refresh token itself.
#[handler]
pub async fn refresh(
    req: &mut Request,
    depot: &mut Depot,
) -> Result<Json<AccessTokenResponse>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let body: RefreshBody = parse_body(req).await?;

    let claims = state
        .auth_provider
        .verify_refresh_token(&body.refresh_token)?;

    // Re-fetch the user rather than trusting the claims' role/enabled state —
    // both can have changed in the (potentially 30-day) window since the
    // refresh token was issued.
    let user = state
        .user_repo
        .get(claims.user_id)
        .await?
        .filter(|u| u.enabled)
        .ok_or_else(|| ApiError::unauthorized("account no longer exists or is disabled"))?;

    let access_token = state.auth_provider.issue_access_token(&user)?;
    Ok(Json(AccessTokenResponse { access_token }))
}

/// GET /auth/me — the caller's own identity, from the validated access token.
/// Requires the auth middleware to have already run (it injects the
/// [`AuthClaims`] this handler reads).
#[handler]
pub async fn me(depot: &mut Depot) -> Result<Json<UserDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let claims = depot
        .obtain::<AuthClaims>()
        .expect("AuthClaims not in depot — auth middleware did not run");

    let user = state
        .user_repo
        .get(claims.user_id)
        .await?
        .ok_or_else(|| ApiError::unauthorized("account no longer exists"))?;

    Ok(Json(UserDto::from(user)))
}

fn issue_tokens(state: &AppState, user: user::Model) -> Result<Json<AuthResponse>, ApiError> {
    let access_token = state.auth_provider.issue_access_token(&user)?;
    let refresh_token = state.auth_provider.issue_refresh_token(&user)?;
    Ok(Json(AuthResponse {
        access_token,
        refresh_token,
        user: UserDto::from(user),
    }))
}
