//! `vms-core` — shared domain types, error definitions, and pipeline DAG model.
//!
//! This crate is the dependency-free heart of the VMS workspace.  Every other
//! crate depends on it; it depends on nothing inside the workspace.  All
//! public types that need to cross crate boundaries live here so they can be
//! imported from a single location.
//!
//! # Module overview
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`error`] | [`VmsError`] — the single error type used across all crates |
//! | [`event`] | [`Event`] and [`TopicKey`] — the Event Bus message format |
//! | [`resource`] | Lifecycle tracking types for the Resource Manager |
//! | [`trigger`] | Trigger configuration and the [`TriggerContext`] injected at run-time |
//! | [`action`] | Action and transport node configuration enums |
//! | [`pipeline`] | [`PipelineDag`] — compile, validate, and walk the pipeline graph |
//! | [`node`] | [`NodeInput`] / [`NodeOutput`] — data passed between nodes at execution time |
//! | [`plugin`] | Async plugin traits (`AuthProvider`, `AnalyticsProvider`, `AuditSink`, `ClusterCoordinator`) |
//! | [`source`] | [`SourceType`] — discriminator for external source adapters |

pub mod action;
pub mod error;
pub mod event;
pub mod node;
pub mod pipeline;
pub mod plugin;
pub mod recording_event;
pub mod resource;
pub mod settings;
pub mod source;
pub mod trigger;

// -- Flat re-exports for ergonomic use in other crates --

pub use error::VmsError;

pub use event::{Event, TopicKey};

pub use recording_event::RecordingChunkEvent;

pub use settings::{setting_meta, SettingMeta, KNOWN_SETTINGS};

pub use resource::{ResourceEntry, ResourceId, ResourceState, RingBufferMode};

pub use trigger::{
    CompareOperator, ScheduleMode, StatMetric, SystemSignal, TriggerConfig, TriggerContext,
    TriggerType,
};

pub use action::{
    ActionConfig, ClipOrder, CompressConfig, CompressionAlgorithm, DelayConfig, EncryptConfig,
    EncryptionAlgorithm, ExtractClipConfig, GapFill, MergeClipsConfig, NotificationFormat,
    PtzCommand, PtzMoveConfig, RenderNotificationConfig, SetStreamQualityConfig, SnapshotConfig,
    StartRecordingConfig, StopRecordingConfig, TranscodeConfig, TransportConfig,
    TriggerAlarmOutputConfig, WatermarkConfig, WatermarkPosition,
};

pub use pipeline::{
    CompiledPipeline, EdgeType, NodeId, NodeType, PipelineCameraRef, PipelineDag, PipelineEdge,
    PipelineNode, PipelineTrigger,
};

pub use node::{NodeInput, NodeOutput, TransferProgress};

pub use plugin::{
    AnalyticsCapability, AnalyticsProvider, AuditEntry, AuditOutcome, AuditSink, AuthClaims,
    AuthProvider, BoundingBox, ClusterCoordinator, Detection,
};

pub use source::SourceType;
