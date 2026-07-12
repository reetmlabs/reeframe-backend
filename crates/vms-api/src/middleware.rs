//! Request-level middleware (Salvo hoops).

use salvo::http::header::AUTHORIZATION;
use salvo::prelude::*;
use vms_core::AuthProvider;

use crate::{error::ApiError, state::AppState};

/// Gates every route it's applied to behind a valid `Authorization: Bearer
/// <access_token>` header, verified via `AppState::auth_provider`. On
/// success, injects the resulting [`vms_core::AuthClaims`] into the
/// [`Depot`] for downstream handlers (e.g. `GET /auth/me`) to read.
///
/// Mounted on every route except `GET /health`, `POST /webhooks/{id}`
/// (external callers can't present a JWT — step 8-4's webhook route does its
/// own accept/reject check instead), and `POST /auth/{setup,login,refresh}`
/// (issuing/refreshing a token can't itself require one).
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

        let Some(token) = extract_bearer_token(req) else {
            ApiError::unauthorized("missing or malformed Authorization header")
                .write(req, depot, res)
                .await;
            ctrl.skip_rest();
            return;
        };

        match state.auth_provider.verify_token(token).await {
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

fn extract_bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
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
}
