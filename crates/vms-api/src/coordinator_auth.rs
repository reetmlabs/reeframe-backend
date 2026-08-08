//! Coordinator-issued JWT verification via JWKS — Step 14a.
//!
//! [`CoordinatorJwksAuthProvider`] is the second, independent `AuthProvider`
//! this BE trusts, alongside [`crate::auth::LocalJwtAuthProvider`]. It fetches
//! Coordinator's `GET /.well-known/jwks.json` and caches the result;
//! verification of every token happens locally against that cache, so the BE
//! never makes a network call to Coordinator on the request path — only when
//! the cache is stale (see [`CoordinatorJwksAuthProvider::ensure_fresh`]).
//!
//! Claims shape mirrors `coordinator-auth::token::Claims` in the Coordinator
//! repo exactly (`sub`, `username`, `is_admin`, `exp`) — both sides must agree
//! since this decodes tokens Coordinator signs with its Ed25519 key.

use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::RwLock;
use uuid::Uuid;
use vms_core::{AuthClaims, AuthProvider, VmsError};

/// Fetches the current key set from Coordinator's JWKS endpoint. A trait
/// (rather than calling `reqwest` directly from the provider) so tests can
/// substitute a fake key source instead of standing up a real HTTP server.
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

/// JWT payload issued by Coordinator. Not part of the public API — callers
/// get an [`AuthClaims`] back, never this struct directly.
#[derive(Debug, Deserialize)]
struct CoordinatorClaims {
    sub: Uuid,
    username: String,
    is_admin: bool,
    exp: i64,
}

struct Cache {
    keys: JwkSet,
    fetched_at: Instant,
}

/// Verifies JWTs signed by a Coordinator instance's Ed25519 key, fetched
/// from `[auth] jwks_url` and cached locally for `refresh_interval`.
pub struct CoordinatorJwksAuthProvider {
    fetcher: Arc<dyn JwksFetcher>,
    refresh_interval: Duration,
    cache: RwLock<Option<Cache>>,
}

impl CoordinatorJwksAuthProvider {
    pub fn new(jwks_url: impl Into<String>, refresh_interval: Duration) -> Self {
        Self::with_fetcher(
            Arc::new(HttpJwksFetcher {
                client: reqwest::Client::new(),
                url: jwks_url.into(),
            }),
            refresh_interval,
        )
    }

    fn with_fetcher(fetcher: Arc<dyn JwksFetcher>, refresh_interval: Duration) -> Self {
        Self {
            fetcher,
            refresh_interval,
            cache: RwLock::new(None),
        }
    }

    /// Fetches Coordinator's JWKS now and populates the cache. Called once
    /// at boot (`main.rs`) so a misconfigured `jwks_url` fails startup
    /// loudly rather than silently rejecting every Coordinator-issued token
    /// at request time.
    pub async fn prefetch(&self) -> Result<(), VmsError> {
        self.force_refresh().await
    }

    /// Refetches unconditionally and replaces the cache, regardless of its
    /// current age.
    async fn force_refresh(&self) -> Result<(), VmsError> {
        let keys = self.fetcher.fetch().await?;
        *self.cache.write().await = Some(Cache {
            keys,
            fetched_at: Instant::now(),
        });
        Ok(())
    }

    /// Refetches only if the cache is empty or older than `refresh_interval`
    /// — "periodic" means "at most once per refresh window," not a
    /// background timer running independently of any verification attempt.
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
            // Not in the cached set — could be a just-rotated key. Force one
            // refresh and check again before rejecting.
            self.force_refresh().await?;
            jwk = self.find_key(&kid).await;
        }
        let jwk =
            jwk.ok_or_else(|| VmsError::Unauthorized(format!("unrecognized signing key: {kid}")))?;

        let decoding_key = DecodingKey::from_jwk(&jwk)
            .map_err(|e| VmsError::Unauthorized(format!("invalid JWKS key: {e}")))?;

        let claims =
            decode::<CoordinatorClaims>(token, &decoding_key, &Validation::new(Algorithm::EdDSA))
                .map(|data| data.claims)
                .map_err(|e| VmsError::Unauthorized(format!("invalid token: {e}")))?;

        Ok(AuthClaims {
            user_id: claims.sub,
            username: claims.username,
            roles: vec![if claims.is_admin { "admin" } else { "viewer" }.to_string()],
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

    /// A fake `JwksFetcher` that always returns whatever key set was handed
    /// to it, and counts how many times it was called — used to assert the
    /// staleness-gated refresh behaviour without a real HTTP server.
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
        is_admin: bool,
        token_type: &'static str,
        iat: i64,
        exp: i64,
    }

    /// Generates a fresh Ed25519 keypair plus the exact JWK shape
    /// Coordinator serves from `.well-known/jwks.json` (RFC 8037 OKP).
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

    fn access_claims(sub: Uuid) -> TestClaims {
        let now = Utc::now().timestamp();
        TestClaims {
            sub,
            username: "alice".into(),
            is_admin: true,
            token_type: "access",
            iat: now,
            exp: now + 900,
        }
    }

    fn provider_with_keys(keys: JwkSet, refresh_interval: Duration) -> CoordinatorJwksAuthProvider {
        CoordinatorJwksAuthProvider::with_fetcher(
            Arc::new(FakeFetcher {
                keys: Mutex::new(keys),
                calls: AtomicUsize::new(0),
            }),
            refresh_interval,
        )
    }

    #[tokio::test]
    async fn token_signed_by_a_known_coordinator_key_verifies() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let provider = provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300));
        let user_id = Uuid::new_v4();
        let token = sign_token(&signing_key, "kid-1", &access_claims(user_id));

        let claims = provider.verify_token(&token).await.unwrap();

        assert_eq!(claims.user_id, user_id);
        assert_eq!(claims.username, "alice");
        assert_eq!(claims.roles, vec!["admin".to_string()]);
    }

    #[tokio::test]
    async fn token_signed_by_an_unrecognized_key_is_rejected() {
        let (_signing_key, jwk) = generate_keypair("kid-1");
        let provider = provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300));
        let (rogue_key, _rogue_jwk) = generate_keypair("kid-rogue");
        let token = sign_token(&rogue_key, "kid-rogue", &access_claims(Uuid::new_v4()));

        let result = provider.verify_token(&token).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stale_cache_triggers_a_refresh_before_verification() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let fetcher = Arc::new(FakeFetcher {
            keys: Mutex::new(JwkSet { keys: vec![jwk] }),
            calls: AtomicUsize::new(0),
        });
        let provider = CoordinatorJwksAuthProvider::with_fetcher(
            fetcher.clone(),
            Duration::from_millis(0), // always stale
        );
        let token = sign_token(&signing_key, "kid-1", &access_claims(Uuid::new_v4()));

        // First call: cache is empty, forces a fetch. Second call: cache is
        // immediately stale again (0ms window), so this must refetch before
        // verifying rather than serving a verification off a stale cache.
        provider.verify_token(&token).await.unwrap();
        provider.verify_token(&token).await.unwrap();

        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_fresh_cache_is_not_refetched_on_every_verification() {
        let (signing_key, jwk) = generate_keypair("kid-1");
        let fetcher = Arc::new(FakeFetcher {
            keys: Mutex::new(JwkSet { keys: vec![jwk] }),
            calls: AtomicUsize::new(0),
        });
        let provider =
            CoordinatorJwksAuthProvider::with_fetcher(fetcher.clone(), Duration::from_secs(300));
        let token = sign_token(&signing_key, "kid-1", &access_claims(Uuid::new_v4()));

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
        let provider = provider_with_keys(JwkSet { keys: vec![jwk] }, Duration::from_secs(300));
        let now = Utc::now().timestamp();
        let claims = TestClaims {
            sub: Uuid::new_v4(),
            username: "alice".into(),
            is_admin: false,
            token_type: "access",
            iat: now - 1000,
            exp: now - 100,
        };
        let token = sign_token(&signing_key, "kid-1", &claims);

        let result = provider.verify_token(&token).await;

        assert!(result.is_err());
    }
}
