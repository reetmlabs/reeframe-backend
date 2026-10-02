//! Coordinator-issued JWT verification via JWKS.
//!
//! [`CoordinatorJwksAuthProvider`] is a second `AuthProvider` this server
//! trusts, alongside [`crate::auth::LocalJwtAuthProvider`]. It fetches the
//! Coordinator's `GET /.well-known/jwks.json` and verifies tokens locally
//! against the cached key set, so the request path only calls the Coordinator
//! when the cache is stale (see [`CoordinatorJwksAuthProvider::ensure_fresh`]).
//!
//! The claims match `coordinator-auth::token::SiteTokenClaims` in the
//! Coordinator repo (`sub`, `username`, `aud`, `role`, `exp`), since this
//! decodes tokens the Coordinator signs with its Ed25519 key. The Coordinator's
//! other token shape (`coordinator-auth::token::Claims`, with `is_admin`
//! instead of `aud`/`role`) is a general identity assertion. It has no `aud`
//! claim, so `set_audience` rejects it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::RwLock;
use uuid::Uuid;
use vms_core::{AuthClaims, AuthProvider, VmsError};

/// This server only distinguishes admin and non-admin
/// (`vms_db::entities::user::UserRole`), so the Coordinator's roles
/// (Admin/Auditor/Operator/Viewer) collapse to that. "Admin" (case-insensitive)
/// becomes `"admin"`; everything else becomes the least-privileged `"viewer"`.
fn map_coordinator_role(role: &str) -> &'static str {
    if role.eq_ignore_ascii_case("admin") {
        "admin"
    } else {
        "viewer"
    }
}

/// Fetches the current key set from the Coordinator's JWKS endpoint. A trait
/// so tests can substitute a fake key source for a real HTTP server.
#[async_trait::async_trait]
trait JwksFetcher: Send + Sync {
    async fn fetch(&self) -> Result<JwkSet, VmsError>;
}

struct HttpJwksFetcher {
    client: reqwest::Client,
    url: String,
}

#[async_trait::async_trait]
impl JwksFetcher for HttpJwksFetcher {
    async fn fetch(&self) -> Result<JwkSet, VmsError> {
        let resp =
            self.client.get(&self.url).send().await.map_err(|e| {
                VmsError::Unauthorized(format!("coordinator JWKS fetch failed: {e}"))
            })?;
        resp.json::<JwkSet>()
            .await
            .map_err(|e| VmsError::Unauthorized(format!("coordinator JWKS response invalid: {e}")))
    }
}

/// JWT payload issued by the Coordinator. Private: callers get [`AuthClaims`].
#[derive(Debug, Deserialize)]
struct CoordinatorClaims {
    sub: Uuid,
    username: String,
    // Never read. `Validation::set_audience` makes decode() reject a mismatch;
    // the field exists so serde requires it.
    #[allow(dead_code)]
    aud: Uuid,
    role: String,
    exp: i64,
}

struct Cache {
    keys: JwkSet,
    fetched_at: Instant,
}

/// Verifies JWTs signed by a Coordinator's Ed25519 key, fetched from
/// `[auth] jwks_url` and cached for `refresh_interval`. Only tokens whose `aud`
/// matches `be_id` (this server's identity in the Coordinator's `sites` table)
/// are accepted, so a token scoped to another server is rejected.
pub struct CoordinatorJwksAuthProvider {
    fetcher: Arc<dyn JwksFetcher>,
    refresh_interval: Duration,
    be_id: Uuid,
    cache: RwLock<Option<Cache>>,
}

impl CoordinatorJwksAuthProvider {
    pub fn new(jwks_url: impl Into<String>, refresh_interval: Duration, be_id: Uuid) -> Self {
        Self::with_fetcher(
            Arc::new(HttpJwksFetcher {
                client: reqwest::Client::new(),
                url: jwks_url.into(),
            }),
            refresh_interval,
            be_id,
        )
    }

    fn with_fetcher(
        fetcher: Arc<dyn JwksFetcher>,
        refresh_interval: Duration,
        be_id: Uuid,
    ) -> Self {
        Self {
            fetcher,
            refresh_interval,
            be_id,
            cache: RwLock::new(None),
        }
    }

    /// Fetches the Coordinator's JWKS and populates the cache. Called once at
    /// boot so a misconfigured `jwks_url` fails startup instead of every
    /// Coordinator-issued token being rejected at request time.
    pub async fn prefetch(&self) -> Result<(), VmsError> {
        self.force_refresh().await
    }

    /// Refetches and replaces the cache regardless of its age.
    async fn force_refresh(&self) -> Result<(), VmsError> {
        let keys = self.fetcher.fetch().await?;
        *self.cache.write().await = Some(Cache {
            keys,
            fetched_at: Instant::now(),
        });
        Ok(())
    }

    /// Refetches only if the cache is empty or older than `refresh_interval`.
    /// Refresh happens lazily during verification; there is no background timer.
    async fn ensure_fresh(&self) -> Result<(), VmsError> {
        {
            let cache = self.cache.read().await;
            if let Some(c) = cache.as_ref() {
                if c.fetched_at.elapsed() < self.refresh_interval {
                    return Ok(());
                }
            }
        }
        self.force_refresh().await
    }

    async fn find_key(&self, kid: &str) -> Option<Jwk> {
        self.cache
            .read()
            .await
            .as_ref()
            .and_then(|c| c.keys.find(kid).cloned())
    }
}

#[async_trait::async_trait]
impl AuthProvider for CoordinatorJwksAuthProvider {
    async fn verify_token(&self, token: &str) -> Result<AuthClaims, VmsError> {
        let header = decode_header(token)
            .map_err(|e| VmsError::Unauthorized(format!("invalid token header: {e}")))?;
        let kid = header
            .kid
            .ok_or_else(|| VmsError::Unauthorized("token has no kid".into()))?;

        self.ensure_fresh().await?;

        let mut jwk = self.find_key(&kid).await;
        if jwk.is_none() {
            // Not in the cached set, possibly a just-rotated key. Refresh once
            // and check again before rejecting.
            self.force_refresh().await?;
            jwk = self.find_key(&kid).await;
        }
        let jwk =
            jwk.ok_or_else(|| VmsError::Unauthorized(format!("unrecognized signing key: {kid}")))?;

        let decoding_key = DecodingKey::from_jwk(&jwk)
            .map_err(|e| VmsError::Unauthorized(format!("invalid JWKS key: {e}")))?;

        // `set_audience` makes jsonwebtoken reject a token whose `aud` isn't
        // this server's id, including the Coordinator's other token shape,
        // which has no `aud` claim.
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&[self.be_id.to_string()]);
        let claims = decode::<CoordinatorClaims>(token, &decoding_key, &validation)
            .map(|data| data.claims)
            .map_err(|e| VmsError::Unauthorized(format!("invalid token: {e}")))?;

        Ok(AuthClaims {
            user_id: claims.sub,
            username: claims.username,
            roles: vec![map_coordinator_role(&claims.role).to_string()],
            expires_at: claims.exp,
        })
    }

    fn provider_name(&self) -> &'static str {
        "coordinator-jwks"
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use chrono::Utc;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::SigningKey;
    use jsonwebtoken::jwk::{
        AlgorithmParameters, CommonParameters, EllipticCurve, KeyAlgorithm, OctetKeyPairParameters,
        OctetKeyPairType, PublicKeyUse,
    };
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rand::rngs::OsRng;
    use serde::Serialize;

    use super::*;

    /// A `JwksFetcher` that returns a fixed key set and counts calls, for
    /// testing the staleness-gated refresh without an HTTP server.
    struct FakeFetcher {
        keys: Mutex<JwkSet>,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl JwksFetcher for FakeFetcher {
        async fn fetch(&self) -> Result<JwkSet, VmsError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.keys.lock().unwrap().clone())
        }
    }

    #[derive(Serialize)]
    struct TestClaims {
        sub: Uuid,
        username: String,
        aud: Uuid,
        role: String,
        iat: i64,
        exp: i64,
    }

    /// Generates an Ed25519 keypair plus the JWK shape the Coordinator serves
    /// from `.well-known/jwks.json` (RFC 8037 OKP).
    fn generate_keypair(kid: &str) -> (SigningKey, Jwk) {
        let signing_key = SigningKey::generate(&mut OsRng);
        let x = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            signing_key.verifying_key().to_bytes(),
        );
        let jwk = Jwk {
            common: CommonParameters {
                public_key_use: Some(PublicKeyUse::Signature),
                key_algorithm: Some(KeyAlgorithm::EdDSA),
                key_id: Some(kid.to_string()),
                ..Default::default()
            },
            algorithm: AlgorithmParameters::OctetKeyPair(OctetKeyPairParameters {
                key_type: OctetKeyPairType::OctetKeyPair,
                curve: EllipticCurve::Ed25519,
                x,
            }),
        };
        (signing_key, jwk)
    }

    fn sign_token(signing_key: &SigningKey, kid: &str, claims: &TestClaims) -> String {
        let der = signing_key.to_pkcs8_der().unwrap();
        let encoding_key = EncodingKey::from_ed_der(der.as_bytes());
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &encoding_key).unwrap()
    }

    fn access_claims(sub: Uuid, aud: Uuid) -> TestClaims {
        let now = Utc::now().timestamp();
        TestClaims {
            sub,
            username: "alice".into(),
            aud,
            role: "Admin".into(),
            iat: now,
            exp: now + 900,
        }
    }

    fn provider_with_keys(
        keys: JwkSet,
        refresh_interval: Duration,
        be_id: Uuid,
    ) -> CoordinatorJwksAuthProvider {
        CoordinatorJwksAuthProvider::with_fetcher(
            Arc::new(FakeFetcher {
                keys: Mutex::new(keys),
                calls: AtomicUsize::new(0),
            }),
            refresh_interval,
            be_id,
        )
    }

    #[tokio::test]
    async fn token_signed_by_a_known_coordinator_key_and_matching_aud_verifies() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let be_id = Uuid::new_v4();
        let provider =
            provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300), be_id);
        let user_id = Uuid::new_v4();
        let token = sign_token(&signing_key, "kid-1", &access_claims(user_id, be_id));

        let claims = provider.verify_token(&token).await.unwrap();

        assert_eq!(claims.user_id, user_id);
        assert_eq!(claims.username, "alice");
        assert_eq!(claims.roles, vec!["admin".to_string()]);
    }

    #[tokio::test]
    async fn a_token_scoped_to_a_different_be_is_rejected() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let this_be_id = Uuid::new_v4();
        let other_be_id = Uuid::new_v4();
        let provider = provider_with_keys(
            JwkSet { keys: vec![jwk] },
            Duration::from_secs(300),
            this_be_id,
        );
        let token = sign_token(
            &signing_key,
            "kid-1",
            &access_claims(Uuid::new_v4(), other_be_id),
        );

        let result = provider.verify_token(&token).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_non_admin_coordinator_role_maps_to_viewer() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let be_id = Uuid::new_v4();
        let provider =
            provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300), be_id);
        let mut claims = access_claims(Uuid::new_v4(), be_id);
        claims.role = "Operator".into();
        let token = sign_token(&signing_key, "kid-1", &claims);

        let claims = provider.verify_token(&token).await.unwrap();

        assert_eq!(claims.roles, vec!["viewer".to_string()]);
    }

    #[tokio::test]
    async fn token_signed_by_an_unrecognized_key_is_rejected() {
        let (_signing_key, jwk) = generate_keypair("kid-1");
        let be_id = Uuid::new_v4();
        let provider =
            provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300), be_id);
        let (rogue_key, _rogue_jwk) = generate_keypair("kid-rogue");
        let token = sign_token(
            &rogue_key,
            "kid-rogue",
            &access_claims(Uuid::new_v4(), be_id),
        );

        let result = provider.verify_token(&token).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stale_cache_triggers_a_refresh_before_verification() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let be_id = Uuid::new_v4();
        let fetcher = Arc::new(FakeFetcher {
            keys: Mutex::new(JwkSet { keys: vec![jwk] }),
            calls: AtomicUsize::new(0),
        });
        let provider = CoordinatorJwksAuthProvider::with_fetcher(
            fetcher.clone(),
            Duration::from_millis(0), // always stale
            be_id,
        );
        let token = sign_token(&signing_key, "kid-1", &access_claims(Uuid::new_v4(), be_id));

        // The first call fetches into the empty cache. With a 0ms window the
        // cache is stale again by the second call, which must refetch.
        provider.verify_token(&token).await.unwrap();
        provider.verify_token(&token).await.unwrap();

        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_fresh_cache_is_not_refetched_on_every_verification() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let be_id = Uuid::new_v4();
        let fetcher = Arc::new(FakeFetcher {
            keys: Mutex::new(JwkSet { keys: vec![jwk] }),
            calls: AtomicUsize::new(0),
        });
        let provider = CoordinatorJwksAuthProvider::with_fetcher(
            fetcher.clone(),
            Duration::from_secs(300),
            be_id,
        );
        let token = sign_token(&signing_key, "kid-1", &access_claims(Uuid::new_v4(), be_id));

        provider.verify_token(&token).await.unwrap();
        provider.verify_token(&token).await.unwrap();
        provider.verify_token(&token).await.unwrap();

        // One fetch to populate the cache; the next two verifications reuse
        // it since the 300s refresh window hasn't elapsed.
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_expired_token_is_rejected() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let be_id = Uuid::new_v4();
        let provider =
            provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300), be_id);
        let now = Utc::now().timestamp();
        let claims = TestClaims {
            sub: Uuid::new_v4(),
            username: "alice".into(),
            aud: be_id,
            role: "Viewer".into(),
            iat: now - 1000,
            exp: now - 100,
        };
        let token = sign_token(&signing_key, "kid-1", &claims);

        let result = provider.verify_token(&token).await;

        assert!(result.is_err());
    }
}
