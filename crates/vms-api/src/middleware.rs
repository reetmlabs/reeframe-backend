//! Request-level middleware (Salvo hoops).

use std::sync::Arc;

use salvo::http::header::AUTHORIZATION;
use salvo::prelude::*;
use vms_core::{AuthClaims, AuthProvider, VmsError};
use vms_engine::Metrics;

use crate::{error::ApiError, state::AppState};

/// Header carrying a long-lived API key, checked when no
/// `Authorization: Bearer` header is present.
const API_KEY_HEADER: &str = "x-api-key";

/// Gates every route it's applied to behind either a valid `Authorization:
/// Bearer <access_token>` header or an `X-API-Key` header (verified via
/// `AppState::api_key_repo`). On success, injects the resulting
/// [`AuthClaims`] into the [`Depot`] for downstream handlers (e.g. `GET
/// /auth/me`) to read.
///
/// A bearer token is checked against `AppState::auth_provider` (local JWT)
/// first, and only if that fails against `AppState::coordinator_auth_provider`
/// (Coordinator-issued JWT) when one is configured — the two are
/// independent, additive credential paths, not a replacement of one by the
/// other; local auth keeps working with zero Coordinator dependency.
///
/// Mounted on every route except `GET /health`, `POST /webhooks/{id}`
/// (external callers can't present either credential — the webhook
/// route does its own accept/reject check instead), and `POST
/// /auth/{setup,login,refresh}` (issuing/refreshing a token can't itself
/// require one).
pub struct AuthMiddleware;

#[async_trait]
impl Handler for AuthMiddleware {
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        ctrl: &mut FlowCtrl,
    ) {
        let state = depot.obtain::<AppState>().expect("AppState not in depot");

        let claims = if let Some(token) = extract_bearer_token(req) {
            verify_bearer_token(state, token).await
        } else if let Some(key) = extract_api_key(req) {
            verify_api_key(state, key).await
        } else {
            ApiError::unauthorized(
                "missing credentials: provide an Authorization: Bearer header or an X-API-Key header",
            )
            .write(req, depot, res)
            .await;
            ctrl.skip_rest();
            return;
        };

        match claims {
            Ok(claims) => {
                depot.inject(claims);
            }
            Err(e) => {
                ApiError::from(e).write(req, depot, res).await;
                ctrl.skip_rest();
            }
        }
    }
}

async fn verify_bearer_token(state: &AppState, token: &str) -> Result<AuthClaims, VmsError> {
    let coordinator = state
        .coordinator_auth_provider
        .as_deref()
        .map(|p| p as &dyn AuthProvider);
    verify_bearer_token_with(&state.auth_provider, coordinator, token).await
}

/// Tries `local` first; only falls back to `coordinator` (if configured)
/// when local verification fails — a local token and a Coordinator token are
/// never mistaken for each other (different signing algorithms), so this
/// never masks a genuine local-auth failure with a misleading Coordinator
/// error unless Coordinator trust is actually enabled. Generic over
/// `&dyn AuthProvider` (rather than taking `&AppState` directly) so the
/// routing logic is testable without constructing a full `AppState`.
async fn verify_bearer_token_with(
    local: &dyn AuthProvider,
    coordinator: Option<&dyn AuthProvider>,
    token: &str,
) -> Result<AuthClaims, VmsError> {
    match local.verify_token(token).await {
        Ok(claims) => Ok(claims),
        Err(local_err) => match coordinator {
            Some(c) => c.verify_token(token).await,
            None => Err(local_err),
        },
    }
}

async fn verify_api_key(state: &AppState, key: &str) -> Result<AuthClaims, VmsError> {
    let user = state
        .api_key_repo
        .verify_and_touch(key)
        .await?
        .filter(|u| u.enabled)
        .ok_or_else(|| VmsError::Unauthorized("invalid or revoked API key".into()))?;

    Ok(AuthClaims {
        user_id: user.id,
        username: user.username,
        roles: vec![user.role.as_str().to_string()],
        // API keys don't expire the way JWTs do — revocation is by deleting
        // the row (`DELETE /users/{id}/api-keys/{key_id}`), not by time.
        expires_at: i64::MAX,
    })
}

fn extract_bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn extract_api_key(req: &Request) -> Option<&str> {
    req.headers().get(API_KEY_HEADER)?.to_str().ok()
}

/// Records `http_requests_total{route, method, status}` for every request
/// that matches a route. Holds its own `Arc<Metrics>` (passed in at
/// construction, in `build_router`) rather than reading it from the
/// `Depot`, so it works whether it's mounted before or after the
/// `affix-state` hoop.
///
/// `req.matched_path()` (the `matched-path` Salvo feature) is already
/// populated by the time any hoop runs — Salvo resolves routing before
/// dispatching the hoop chain — so it's safe to read before *or* after
/// `ctrl.call_next()`. The status code is not: it's only final once the
/// downstream chain has actually run, so that read happens after.
pub struct MetricsMiddleware {
    metrics: Arc<Metrics>,
}

impl MetricsMiddleware {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

#[async_trait]
impl Handler for MetricsMiddleware {
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        ctrl: &mut FlowCtrl,
    ) {
        let method = req.method().as_str().to_owned();

        ctrl.call_next(req, depot, res).await;

        // Unset means no handler explicitly set one — Salvo defaults that to
        // 200 once every hoop has run, so mirror that here.
        let status = res.status_code.unwrap_or(StatusCode::OK);
        self.metrics
            .record_http_request(req.matched_path(), &method, status.as_u16());
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_auth_header(value: Option<&str>) -> Request {
        let mut req = Request::default();
        if let Some(v) = value {
            req.headers_mut().insert(AUTHORIZATION, v.parse().unwrap());
        }
        req
    }

    #[test]
    fn extracts_token_from_well_formed_bearer_header() {
        let req = request_with_auth_header(Some("Bearer abc.def.ghi"));
        assert_eq!(extract_bearer_token(&req), Some("abc.def.ghi"));
    }

    #[test]
    fn missing_header_returns_none() {
        let req = request_with_auth_header(None);
        assert_eq!(extract_bearer_token(&req), None);
    }

    #[test]
    fn non_bearer_scheme_returns_none() {
        let req = request_with_auth_header(Some("Basic dXNlcjpwYXNz"));
        assert_eq!(extract_bearer_token(&req), None);
    }

    #[test]
    fn bearer_prefix_without_a_token_returns_empty_str_not_none() {
        // "Bearer " with nothing after it still strips to an empty token —
        // that's rejected later by `verify_token`, not here.
        let req = request_with_auth_header(Some("Bearer "));
        assert_eq!(extract_bearer_token(&req), Some(""));
    }

    #[test]
    fn extracts_api_key_from_x_api_key_header() {
        let mut req = Request::default();
        req.headers_mut()
            .insert(API_KEY_HEADER, "rfk_abc123".parse().unwrap());
        assert_eq!(extract_api_key(&req), Some("rfk_abc123"));
    }

    #[test]
    fn missing_api_key_header_returns_none() {
        let req = Request::default();
        assert_eq!(extract_api_key(&req), None);
    }

    // -- Dual-issuer bearer token verification --
    //
    // Exercised with two `LocalJwtAuthProvider`s standing in for "local" and
    // "coordinator" — the routing logic in `verify_bearer_token_with` is
    // generic over any `AuthProvider`, so it doesn't need a real
    // `CoordinatorJwksAuthProvider` (already covered by its own tests) to
    // verify the fallback behavior itself.

    use chrono::{FixedOffset, Utc};
    use vms_db::entities::user::{self, UserRole};

    use crate::auth::LocalJwtAuthProvider;

    fn test_user() -> user::Model {
        user::Model {
            id: uuid::Uuid::new_v4(),
            username: "alice".into(),
            password_hash: "unused".into(),
            role: UserRole::Admin,
            enabled: true,
            created_at: Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()),
            updated_at: Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()),
        }
    }

    #[tokio::test]
    async fn a_valid_local_token_succeeds_with_no_coordinator_configured() {
        let local = LocalJwtAuthProvider::new("local-secret", 900, 2_592_000);
        let token = local.issue_access_token(&test_user()).unwrap();

        let claims = verify_bearer_token_with(&local, None, &token)
            .await
            .unwrap();

        assert_eq!(claims.username, "alice");
    }

    #[tokio::test]
    async fn a_valid_local_token_still_succeeds_when_coordinator_is_also_configured() {
        let local = LocalJwtAuthProvider::new("local-secret", 900, 2_592_000);
        let coordinator = LocalJwtAuthProvider::new("coordinator-secret", 900, 2_592_000);
        let token = local.issue_access_token(&test_user()).unwrap();

        let claims = verify_bearer_token_with(&local, Some(&coordinator), &token)
            .await
            .unwrap();

        assert_eq!(claims.username, "alice");
    }

    #[tokio::test]
    async fn a_token_only_the_coordinator_recognizes_falls_back_and_succeeds() {
        let local = LocalJwtAuthProvider::new("local-secret", 900, 2_592_000);
        let coordinator = LocalJwtAuthProvider::new("coordinator-secret", 900, 2_592_000);
        // Signed by "coordinator", so `local` (different secret) rejects it.
        let token = coordinator.issue_access_token(&test_user()).unwrap();

        let claims = verify_bearer_token_with(&local, Some(&coordinator), &token)
            .await
            .unwrap();

        assert_eq!(claims.username, "alice");
    }

    #[tokio::test]
    async fn a_token_neither_provider_recognizes_is_rejected() {
        let local = LocalJwtAuthProvider::new("local-secret", 900, 2_592_000);
        let coordinator = LocalJwtAuthProvider::new("coordinator-secret", 900, 2_592_000);
        let rogue = LocalJwtAuthProvider::new("rogue-secret", 900, 2_592_000);
        let token = rogue.issue_access_token(&test_user()).unwrap();

        let result = verify_bearer_token_with(&local, Some(&coordinator), &token).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_missing_credential_is_rejected_exactly_as_before_with_no_coordinator() {
        let local = LocalJwtAuthProvider::new("local-secret", 900, 2_592_000);

        let result = verify_bearer_token_with(&local, None, "not-a-real-token").await;

        assert!(result.is_err());
    }
}
