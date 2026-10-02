//! Local authentication: first-run admin setup and username/password login.
//!
//! `POST /auth/setup` is the only unauthenticated write endpoint in the API,
//! because there is no admin to authenticate as yet. It refuses to run once
//! any user exists.

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

/// Passwords shorter than this are rejected at setup/creation time. This is a
/// floor against trivially weak values, not a full password policy.
const MIN_PASSWORD_LEN: usize = 8;

/// Generic message for every login failure (unknown username, wrong password,
/// disabled account), so a caller can't enumerate usernames or account state.
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

/// POST /auth/setup
///
/// Creates the first admin user. Returns `409` once any user exists.
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

/// POST /auth/refresh
///
/// Exchanges a refresh token for a new access token. The refresh token itself
/// is not rotated.
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

    // Re-fetch the user instead of trusting the claims' role/enabled state,
    // since both can change in the (potentially 30-day) window since the
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

/// GET /auth/me
///
/// The caller's own identity, from the validated access token. Requires the
/// auth middleware to have run, since it injects the [`AuthClaims`] read here.
#[handler]
pub async fn me(depot: &mut Depot) -> Result<Json<UserDto>, ApiError> {
    let state = depot.obtain::<AppState>().expect("AppState not in depot");
    let claims = depot
        .obtain::<AuthClaims>()
        .expect("AuthClaims not in depot, auth middleware did not run");

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

// -- Tests --

#[cfg(test)]
mod tests {
    use sea_orm_migration::MigratorTrait;
    use vms_core::AuthProvider;
    use vms_db::{Migrator, UserRepo};

    use super::*;
    use crate::auth::LocalJwtAuthProvider;

    async fn test_user_repo() -> UserRepo {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        UserRepo::new(db)
    }

    /// The setup, login and refresh flow works end to end with only `UserRepo`
    /// and `LocalJwtAuthProvider`, with no `CoordinatorJwksAuthProvider` or
    /// `jwks_url`. `main.rs` always builds `AppState::auth_provider` regardless
    /// of `[auth] mode`, and these handlers only read `state.auth_provider`, so
    /// this is the same code path production runs without a Coordinator.
    #[tokio::test]
    async fn setup_then_login_then_refresh_never_touches_coordinator() {
        let user_repo = test_user_repo().await;
        let auth_provider = LocalJwtAuthProvider::new("test-secret", 900, 2_592_000);

        // -- POST /auth/setup --
        assert_eq!(user_repo.count().await.unwrap(), 0);
        let admin = user_repo
            .create(CreateUser {
                username: "admin".into(),
                password: "correct horse battery staple".into(),
                role: UserRole::Admin,
            })
            .await
            .unwrap();
        let setup_access = auth_provider.issue_access_token(&admin).unwrap();
        let setup_refresh = auth_provider.issue_refresh_token(&admin).unwrap();
        auth_provider.verify_token(&setup_access).await.unwrap();

        // -- POST /auth/login --
        let logged_in = user_repo
            .get_by_username("admin")
            .await
            .unwrap()
            .filter(|u| u.enabled)
            .filter(|u| verify_password("correct horse battery staple", &u.password_hash))
            .unwrap();
        let login_access = auth_provider.issue_access_token(&logged_in).unwrap();
        auth_provider.verify_token(&login_access).await.unwrap();

        // -- POST /auth/refresh --
        let refresh_claims = auth_provider.verify_refresh_token(&setup_refresh).unwrap();
        let refreshed_user = user_repo
            .get(refresh_claims.user_id)
            .await
            .unwrap()
            .unwrap();
        let new_access = auth_provider.issue_access_token(&refreshed_user).unwrap();
        auth_provider.verify_token(&new_access).await.unwrap();
    }
}
