//! Error types for the VMS platform.
//!
//! [`VmsError`] is the single error enum used by every crate in the workspace.
//! All sub-errors (I/O, JSON, media, database, …) are folded into it so that
//! the `?` operator works uniformly across crate boundaries without needing
//! `Box<dyn Error>`.
//!
//! # `From` conversions
//!
//! Blanket `From` implementations are provided for the two most common
//! standard-library / ecosystem error types:
//! - [`std::io::Error`] -> [`VmsError::Io`]
//! - [`serde_json::Error`] -> [`VmsError::Serialization`]
//!
//! Other conversions (SeaORM, GStreamer, etc.) live in the crates that introduce
//! those dependencies, keeping `vms-core` dependency-free.

use uuid::Uuid;

/// The canonical error type for the VMS platform.
///
/// Every public function in the workspace that can fail returns
/// `Result<_, VmsError>`.  Downstream callers can match on specific variants
/// for structured error handling or call `.to_string()` to get a human-readable
/// message courtesy of [`thiserror`].
#[derive(Debug, thiserror::Error)]
pub enum VmsError {
    /// A pipeline with the given ID does not exist in the registry.
    #[error("pipeline {0} not found")]
    PipelineNotFound(Uuid),

    /// A camera with the given ID is not registered in the system.
    #[error("camera {0} not found")]
    CameraNotFound(Uuid),

    /// An external source with the given ID is not registered.
    #[error("source {0} not found")]
    SourceNotFound(Uuid),

    /// A user with the given ID is not registered in the system.
    #[error("user {0} not found")]
    UserNotFound(Uuid),

    /// An API key with the given ID does not exist, or does not belong to
    /// the user it was requested under.
    #[error("api key {0} not found")]
    ApiKeyNotFound(Uuid),

    /// A pipeline node with the given ID does not exist, or does not belong
    /// to the pipeline it was requested under.
    #[error("node {0} not found")]
    NodeNotFound(Uuid),

    /// A pipeline edge with the given ID does not exist, or does not belong
    /// to the pipeline it was requested under.
    #[error("edge {0} not found")]
    EdgeNotFound(Uuid),

    /// A pipeline trigger with the given ID does not exist, or does not
    /// belong to the pipeline it was requested under.
    #[error("trigger {0} not found")]
    TriggerNotFound(Uuid),

    /// A transport destination (S3 bucket, SMTP relay, webhook, …) was not found.
    #[error("destination {0} not found")]
    DestinationNotFound(Uuid),

    /// A contact (notification recipient) with the given ID does not exist.
    #[error("contact {0} not found")]
    ContactNotFound(Uuid),

    /// A contact list with the given ID does not exist.
    #[error("contact list {0} not found")]
    ContactListNotFound(Uuid),

    /// A recording (chunk) with the given ID does not exist, or does not
    /// belong to the camera it was requested under.
    #[error("recording {0} not found")]
    RecordingNotFound(Uuid),

    /// A tile-layout profile with the given ID does not exist.
    #[error("tile profile {0} not found")]
    TileProfileNotFound(Uuid),

    /// A tile formation with the given ID does not exist, or does not
    /// belong to the profile it was requested under.
    #[error("tile formation {0} not found")]
    TileFormationNotFound(Uuid),

    /// A structural rule was violated when compiling a pipeline DAG.
    ///
    /// The inner string describes the specific rule that failed (e.g. "pipeline
    /// contains a cycle" or "condition node must have exactly 2 outgoing edges").
    #[error("DAG validation failed: {0}")]
    DagValidation(String),

    /// An error occurred in the media pipeline (GStreamer, FFmpeg, etc.).
    #[error("media error: {0}")]
    Media(String),

    /// An external source adapter (MQTT, HTTP webhook, file watcher, etc.)
    /// failed to start, stop, or process an event.
    #[error("source error: {0}")]
    Source(String),

    /// A SeaORM / SQLx database operation failed.
    #[error("database error: {0}")]
    Database(String),

    /// Delivery to a destination failed.
    ///
    /// `dest_id` identifies the destination; `message` contains the underlying
    /// error from the transport adapter.
    #[error("transport error for destination {dest_id}: {message}")]
    Transport {
        /// UUID of the transport destination that failed.
        dest_id: Uuid,
        /// Human-readable description of the failure.
        message: String,
    },

    /// A minijinja template failed to render.
    #[error("template error: {0}")]
    Template(String),

    /// An `evalexpr` condition expression could not be evaluated.
    #[error("expression evaluation error: {0}")]
    ExpressionEval(String),

    /// AES-256-GCM encryption or decryption failed.
    #[error("encryption error: {0}")]
    Encryption(String),

    /// A required configuration key is absent or invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// An I/O operation on the filesystem failed.
    ///
    /// Automatically constructed by the [`From<std::io::Error>`] impl.
    #[error("I/O error: {0}")]
    Io(String),

    /// An Actix / Tokio actor's channel closed unexpectedly, indicating a crash.
    #[error("actor channel closed unexpectedly")]
    ActorDied,

    /// The caller does not have permission to perform the requested operation.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// A generic "resource not found" variant for cases not covered by the
    /// typed variants above.
    #[error("{0}")]
    NotFound(String),

    /// The requested change would violate a domain invariant (e.g. deleting
    /// or disabling the last remaining local admin) — the request is
    /// well-formed but rejected because of the system's current state.
    #[error("conflict: {0}")]
    Conflict(String),

    /// JSON serialization or deserialization failed.
    ///
    /// Automatically constructed by the [`From<serde_json::Error>`] impl.
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<std::io::Error> for VmsError {
    fn from(e: std::io::Error) -> Self {
        VmsError::Io(e.to_string())
    }
}

impl From<serde_json::Error> for VmsError {
    fn from(e: serde_json::Error) -> Self {
        VmsError::Serialization(e.to_string())
    }
}
