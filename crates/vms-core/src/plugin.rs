//! Async plugin trait definitions for the open-core extension seams.
//!
//! These four traits are the *only* places where the community (BSD-3-Clause)
//! core touches enterprise behaviour.  Enterprise crates provide concrete
//! implementations; the community edition ships no-op or minimal stubs.
//!
//! | Trait | Community impl | Enterprise impl |
//! |-------|----------------|-----------------|
//! | [`AuthProvider`] | Local JWT validation | SAML 2.0, OIDC, LDAP/AD, SCIM |
//! | [`AnalyticsProvider`] | ONNX Runtime object detection | Facial recognition, LPR, crowd analytics, custom models |
//! | [`AuditSink`] | Structured `tracing` output | Tamper-proof append-only store (GDPR/SOC2/HIPAA) |
//! | [`ClusterCoordinator`] | No-op (single-host) | Raft consensus, active-active failover, multi-site |
//!
//! All traits are `async` (via [`async_trait`]) and `dyn`-safe so they can be
//! held behind `Arc<dyn Trait>` in the engine's service registry.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::VmsError;

// -- Auth provider --

/// Verifies API tokens and manages authenticated sessions.
///
/// The community implementation validates HS256/RS256 JWTs signed by the local
/// key store.  Enterprise implementations can delegate to an external IdP via
/// SAML 2.0, OIDC, or LDAP.
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Verify `token` and return the claims it carries.
    ///
    /// # Errors
    ///
    /// Returns [`VmsError::Unauthorized`] if the token is missing, expired,
    /// or has an invalid signature.
    async fn verify_token(&self, token: &str) -> Result<AuthClaims, VmsError>;

    /// A short human-readable name for this provider (e.g. `"local-jwt"`).
    ///
    /// Used in logs and health-check responses to identify which backend is
    /// active.
    fn provider_name(&self) -> &'static str;
}

/// Claims extracted from a verified authentication token.
#[derive(Debug, Clone)]
pub struct AuthClaims {
    /// UUID of the authenticated user in the `users` table.
    pub user_id: Uuid,
    /// Login name of the authenticated user.
    pub username: String,
    /// Role strings granted to this user (e.g. `["admin", "viewer"]`).
    pub roles: Vec<String>,
    /// Unix timestamp (seconds since epoch) at which the token expires.
    pub expires_at: i64,
}

// -- Analytics provider --

/// Runs AI inference on camera frames and returns detections.
///
/// The community implementation uses ONNX Runtime for general object detection
/// (person, vehicle, animal).  Enterprise implementations can add facial
/// recognition, licence-plate reading, crowd analytics, and custom model
/// loading.
#[async_trait]
pub trait AnalyticsProvider: Send + Sync {
    /// Run inference on a raw `frame_data` byte slice from `camera_id`.
    ///
    /// `timestamp_ns` is the capture timestamp in nanoseconds since the Unix
    /// epoch, used to correlate detections with ring-buffer segments.
    ///
    /// # Errors
    ///
    /// Returns [`VmsError::Media`] if the frame cannot be decoded or
    /// [`VmsError::Config`] if the model is not loaded.
    async fn process_frame(
        &self,
        camera_id: Uuid,
        frame_data: &[u8],
        timestamp_ns: u64,
    ) -> Result<Vec<Detection>, VmsError>;

    /// A short human-readable name for this provider (e.g. `"onnx-object-detection"`).
    fn provider_name(&self) -> &'static str;

    /// The set of detection capabilities this provider supports.
    ///
    /// Called at startup so the engine can warn when a pipeline references a
    /// capability (e.g. [`AnalyticsCapability::FacialRecognition`]) that the
    /// active provider does not support.
    fn capabilities(&self) -> Vec<AnalyticsCapability>;
}

/// A single object detection returned by an [`AnalyticsProvider`].
#[derive(Debug, Clone)]
pub struct Detection {
    /// Class label, e.g. `"person"`, `"car"`, `"dog"`.
    pub label: String,
    /// Confidence score in the range `[0.0, 1.0]`.
    pub confidence: f32,
    /// Bounding box of the detected object in the frame.
    pub bbox: BoundingBox,
    /// Provider-specific metadata (e.g. face embedding vector, plate text).
    pub metadata: serde_json::Value,
}

/// Axis-aligned bounding box in normalised frame coordinates.
///
/// All values are in the range `[0.0, 1.0]` relative to the frame dimensions,
/// so the box is resolution-independent.
#[derive(Debug, Clone)]
pub struct BoundingBox {
    /// Normalised X coordinate of the top-left corner (`0.0` = left edge).
    pub x: f32,
    /// Normalised Y coordinate of the top-left corner (`0.0` = top edge).
    pub y: f32,
    /// Normalised width of the box.
    pub width: f32,
    /// Normalised height of the box.
    pub height: f32,
}

/// Detection capabilities that an [`AnalyticsProvider`] may advertise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalyticsCapability {
    /// General-purpose object detection (person, vehicle, animal, etc.).
    ObjectDetection,
    /// Pixel-level motion detection between consecutive frames.
    MotionDetection,
    /// Detection of abrupt scene changes (camera tamper, cut, etc.).
    SceneChange,
    /// Identification of individuals by face. *(Enterprise)*
    FacialRecognition,
    /// Reading of vehicle licence plates. *(Enterprise)*
    LicensePlateRecognition,
    /// Counting the number of people in a region. *(Enterprise)*
    PeopleCounting,
    /// A provider-specific capability not covered by the standard variants.
    Custom(String),
}

// -- Audit sink --

/// Records security-relevant events for compliance and forensics.
///
/// The community implementation emits structured [`tracing`] log lines at the
/// `INFO` level.  Enterprise implementations write to a tamper-proof,
/// append-only store with built-in GDPR/SOC2/HIPAA report generation.
#[async_trait]
pub trait AuditSink: Send + Sync {
    /// Persist one audit entry.
    ///
    /// # Errors
    ///
    /// Returns [`VmsError::Database`] (or equivalent) if the entry cannot be
    /// written to the backing store.
    async fn record(&self, entry: AuditEntry) -> Result<(), VmsError>;

    /// A short human-readable name for this sink (e.g. `"tracing-audit"`).
    fn sink_name(&self) -> &'static str;
}

/// A single audit record describing one security-relevant action.
#[derive(Debug, Clone)]
pub struct AuditEntry {
    /// Unique identifier for this audit record.
    pub id: Uuid,
    /// Wall-clock time at which the event occurred.
    pub occurred_at: DateTime<Utc>,
    /// UUID of the user or service account that performed the action, if known.
    pub actor_id: Option<Uuid>,
    /// Display name of the actor (denormalised for read performance).
    pub actor_name: Option<String>,
    /// Short verb describing what was done, e.g. `"pipeline.trigger"`.
    pub action: String,
    /// The kind of resource that was acted on, e.g. `"pipeline"`, `"camera"`.
    pub resource_type: String,
    /// The string-encoded ID of the resource that was acted on, if applicable.
    pub resource_id: Option<String>,
    /// Whether the action succeeded, failed, or was denied by policy.
    pub outcome: AuditOutcome,
    /// Additional context as a JSON object (request parameters, error details, etc.).
    pub metadata: serde_json::Value,
}

/// Outcome of an audited action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditOutcome {
    /// The action completed successfully.
    Success,
    /// The action was attempted but failed due to an internal error.
    Failure,
    /// The action was rejected by an authorization policy before execution.
    Denied,
}

// -- Cluster coordinator --

/// Manages distributed state and failover across VMS nodes.
///
/// The community implementation is a no-op that assumes single-host
/// deployment.  The enterprise implementation uses Raft consensus to provide
/// active-active camera failover and multi-site replication.
#[async_trait]
pub trait ClusterCoordinator: Send + Sync {
    /// Try to acquire exclusive ownership of a camera pipeline on this node.
    ///
    /// Returns `true` if the lease was granted (this node should process the
    /// camera), `false` if another node already holds the lease.
    ///
    /// # Errors
    ///
    /// Returns [`VmsError::Config`] if the coordinator is not reachable.
    async fn acquire_camera_lease(&self, camera_id: Uuid) -> Result<bool, VmsError>;

    /// Release the lease for `camera_id` so another node can acquire it.
    ///
    /// Should be called when the camera pipeline is shut down gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`VmsError::Config`] if the coordinator is not reachable.
    async fn release_camera_lease(&self, camera_id: Uuid) -> Result<(), VmsError>;

    /// Returns `true` if this node is the current Raft leader.
    ///
    /// In the community no-op implementation this always returns `true`.
    async fn is_leader(&self) -> bool;

    /// The stable identifier for this VMS node in the cluster.
    fn node_id(&self) -> Uuid;

    /// A short human-readable name for this coordinator (e.g. `"single-host-noop"`).
    fn coordinator_name(&self) -> &'static str;
}
