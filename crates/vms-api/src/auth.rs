//! Local JWT issuance and verification — the community `AuthProvider`.
//!
//! [`LocalJwtAuthProvider`] both issues tokens (`POST /auth/setup`,
//! `POST /auth/login`, `POST /auth/refresh`) and verifies them
//! (`AuthProvider::verify_token`, consumed by the auth middleware). Both
//! directions share the same signing secret, so one struct owns both rather
//! than splitting into an issuer and a separate verifier.

use chrono::Utc;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use vms_core::{AuthClaims, AuthProvider, VmsError};
use vms_db::entities::user::{self, UserRole};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TokenType {
    Access,
    Refresh,
}

/// JWT payload issued by this provider. Not part of the public API — callers
/// get an opaque signed string back, never this struct directly.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: Uuid,
    username: String,
    role: UserRole,
    token_type: TokenType,
    iat: i64,
    exp: i64,
}

/// The community `AuthProvider`: HS256 JWTs signed with a locally configured
/// secret. No external dependency — this is what makes `[auth] mode =
/// "local"` work with zero external services.
#[derive(Clone)]
pub struct LocalJwtAuthProvider {
    secret: String,
    access_ttl_secs: i64,
    refresh_ttl_secs: i64,
}

impl LocalJwtAuthProvider {
    pub fn new(secret: impl Into<String>, access_ttl_secs: i64, refresh_ttl_secs: i64) -> Self {
        Self {
            secret: secret.into(),
            access_ttl_secs,
            refresh_ttl_secs,
        }
    }

    pub fn issue_access_token(&self, user: &user::Model) -> Result<String, VmsError> {
        self.encode_for(user, TokenType::Access, self.access_ttl_secs)
    }

    pub fn issue_refresh_token(&self, user: &user::Model) -> Result<String, VmsError> {
        self.encode_for(user, TokenType::Refresh, self.refresh_ttl_secs)
    }

    /// Verify a refresh token specifically, rejecting an access token
    /// presented in its place. Not part of the generic [`AuthProvider`]
    /// trait — refresh-grant semantics are specific to local JWT issuance,
    /// not something every auth backend (SAML, OIDC) shares the same shape
    /// for.
    pub fn verify_refresh_token(&self, token: &str) -> Result<AuthClaims, VmsError> {
        let claims = self.decode_token(token)?;
        if claims.token_type != TokenType::Refresh {
            return Err(VmsError::Unauthorized("not a refresh token".into()));
        }
        Ok(claims_to_auth_claims(claims))
    }

    fn encode_for(
        &self,
        user: &user::Model,
        token_type: TokenType,
        ttl_secs: i64,
    ) -> Result<String, VmsError> {
        let now = Utc::now().timestamp();
        let claims = Claims {
            sub: user.id,
            username: user.username.clone(),
            role: user.role.clone(),
            token_type,
            iat: now,
            exp: now + ttl_secs,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| VmsError::Encryption(format!("jwt encode: {e}")))
    }

    fn decode_token(&self, token: &str) -> Result<Claims, VmsError> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::new(Algorithm::HS256),
        )
        .map(|data| data.claims)
        .map_err(|e| VmsError::Unauthorized(format!("invalid token: {e}")))
    }
}

fn claims_to_auth_claims(claims: Claims) -> AuthClaims {
    AuthClaims {
        user_id: claims.sub,
        username: claims.username,
        roles: vec![claims.role.as_str().to_string()],
        expires_at: claims.exp,
    }
}

#[async_trait::async_trait]
impl AuthProvider for LocalJwtAuthProvider {
    async fn verify_token(&self, token: &str) -> Result<AuthClaims, VmsError> {
        let claims = self.decode_token(token)?;
        if claims.token_type != TokenType::Access {
            return Err(VmsError::Unauthorized("not an access token".into()));
        }
        Ok(claims_to_auth_claims(claims))
    }

    fn provider_name(&self) -> &'static str {
        "local-jwt"
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use chrono::FixedOffset;

    use super::*;

    fn test_user() -> user::Model {
        user::Model {
            id: Uuid::new_v4(),
            username: "alice".into(),
            password_hash: "unused".into(),
            role: UserRole::Admin,
            enabled: true,
            created_at: Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()),
            updated_at: Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()),
        }
    }

    #[tokio::test]
    async fn access_token_round_trips_through_verify_token() {
        let provider = LocalJwtAuthProvider::new("test-secret", 900, 2_592_000);
        let user = test_user();

        let token = provider.issue_access_token(&user).unwrap();
        let claims = provider.verify_token(&token).await.unwrap();

        assert_eq!(claims.user_id, user.id);
        assert_eq!(claims.username, "alice");
        assert_eq!(claims.roles, vec!["admin".to_string()]);
    }

    #[tokio::test]
    async fn refresh_token_is_rejected_by_verify_token() {
        let provider = LocalJwtAuthProvider::new("test-secret", 900, 2_592_000);
        let user = test_user();

        let refresh = provider.issue_refresh_token(&user).unwrap();
        let result = provider.verify_token(&refresh).await;

        assert!(result.is_err());
    }

    #[test]
    fn access_token_is_rejected_by_verify_refresh_token() {
        let provider = LocalJwtAuthProvider::new("test-secret", 900, 2_592_000);
        let user = test_user();

        let access = provider.issue_access_token(&user).unwrap();
        let result = provider.verify_refresh_token(&access);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn token_signed_with_a_different_secret_is_rejected() {
        let issuer = LocalJwtAuthProvider::new("secret-a", 900, 2_592_000);
        let verifier = LocalJwtAuthProvider::new("secret-b", 900, 2_592_000);
        let token = issuer.issue_access_token(&test_user()).unwrap();

        assert!(verifier.verify_token(&token).await.is_err());
    }

    #[tokio::test]
    async fn expired_access_token_is_rejected() {
        // `jsonwebtoken`'s default `Validation` allows a 60s leeway on `exp`,
        // so the TTL needs to be well past that for this to actually exercise
        // expiry rejection rather than the leeway window.
        let provider = LocalJwtAuthProvider::new("test-secret", -120, 2_592_000);
        let token = provider.issue_access_token(&test_user()).unwrap();

        assert!(provider.verify_token(&token).await.is_err());
    }
}
