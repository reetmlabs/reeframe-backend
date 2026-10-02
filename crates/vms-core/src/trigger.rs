//! Trigger configuration and the execution context injected at run-time.
//!
//! A *trigger* is the condition that causes a pipeline to run.  Each
//! [`PipelineTrigger`] row in the database carries a [`TriggerConfig`] variant
//! that describes *how* to fire:
//!
//! | Variant | When it fires |
//! |---------|---------------|
//! | [`TriggerConfig::Schedule`] | On a cron expression or fixed interval |
//! | [`TriggerConfig::Event`] | When an [`Event`] matches an `evalexpr` filter |
//! | [`TriggerConfig::System`] | On a VMS lifecycle signal (chunk finished, feed lost, …) |
//! | [`TriggerConfig::Manual`] | When the REST API `/pipelines/{id}/trigger` is called |
//! | [`TriggerConfig::Stat`] | When a system metric crosses a threshold |
//!
//! When a trigger fires, the Trigger Manager constructs a [`TriggerContext`]
//! and passes it to the Pipeline Executor, which seeds it into every node's
//! [`NodeInput`].
//!
//! [`Event`]: crate::event::Event
//! [`NodeInput`]: crate::node::NodeInput

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Discriminator that identifies which kind of trigger fired a pipeline run.
///
/// Stored on [`TriggerContext::trigger_type`] so downstream nodes can branch
/// on how the pipeline was invoked.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TriggerType {
    /// Fired by a cron or interval scheduler.
    Schedule,
    /// Fired by an event on the Event Bus that passed the filter expression.
    Event,
    /// Fired by a VMS lifecycle signal (e.g. chunk finished, feed disconnected).
    System,
    /// Fired by a direct API call to the manual-trigger endpoint.
    Manual,
    /// Fired when a system metric (disk, RAM, CPU, bitrate) crossed a threshold.
    Stat,
}

impl TriggerType {
    /// Snake_case form matching the serde wire representation, so error
    /// messages use the same spelling as the JSON the client sent.
    pub fn as_str(&self) -> &'static str {
        match self {
            TriggerType::Schedule => "schedule",
            TriggerType::Event => "event",
            TriggerType::System => "system",
            TriggerType::Manual => "manual",
            TriggerType::Stat => "stat",
        }
    }
}

// -- Schedule --

/// Defines the cadence for a [`TriggerConfig::Schedule`] trigger.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ScheduleMode {
    /// Fire according to a standard 5- or 6-field cron expression.
    ///
    /// The expression is evaluated in the timezone specified by
    /// [`TriggerConfig::Schedule::timezone`].
    Cron {
        /// A valid cron expression, e.g. `"0 2 * * *"` (daily at 02:00).
        expression: String,
    },
    /// Fire every N seconds, starting from when the trigger was enabled.
    Interval {
        /// Seconds between firings; must be ≥ 1.
        interval_secs: u64,
    },
}

// -- System signals --

/// VMS-internal lifecycle signals that can trigger a pipeline.
///
/// The recording engine emits these signals via the Event Bus whenever the
/// corresponding state transition occurs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SystemSignal {
    /// A continuous recording chunk has been finalised and flushed to disk.
    ChunkFinished,
    /// The RTSP (or other) feed for a camera was lost.
    FeedDisconnected,
    /// The RTSP feed was successfully re-established after a disconnect.
    FeedReconnected,
    /// A recording session was started on a camera.
    RecordingStarted,
    /// A recording session was stopped on a camera.
    RecordingStopped,
}

// -- Stat-based --

/// The system metric observed by a [`TriggerConfig::Stat`] trigger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StatMetric {
    /// Percentage of total disk space consumed (0-100).
    DiskUsagePercent,
    /// Percentage of physical RAM in use (0-100).
    RamUsagePercent,
    /// Aggregate CPU utilisation across all cores (0-100).
    CpuUsagePercent,
    /// Ingest bitrate for a camera feed, in kilobits per second.
    FeedBitrateKbps,
    /// Packet loss percentage for a camera feed (0-100).
    FeedPacketLossPercent,
}

/// Comparison operator used by [`TriggerConfig::Stat`] and condition node
/// expressions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompareOperator {
    /// `actual > threshold`
    GreaterThan,
    /// `actual < threshold`
    LessThan,
    /// `actual >= threshold`
    GreaterThanOrEqual,
    /// `actual <= threshold`
    LessThanOrEqual,
    /// `actual == threshold` (within floating-point epsilon)
    Equal,
    /// `actual != threshold` (outside floating-point epsilon)
    NotEqual,
}

impl CompareOperator {
    /// Evaluate the comparison between `actual` and `threshold`.
    ///
    /// Equality comparisons use [`f64::EPSILON`] to avoid floating-point
    /// representation issues.
    pub fn evaluate(&self, actual: f64, threshold: f64) -> bool {
        match self {
            CompareOperator::GreaterThan => actual > threshold,
            CompareOperator::LessThan => actual < threshold,
            CompareOperator::GreaterThanOrEqual => actual >= threshold,
            CompareOperator::LessThanOrEqual => actual <= threshold,
            CompareOperator::Equal => (actual - threshold).abs() < f64::EPSILON,
            CompareOperator::NotEqual => (actual - threshold).abs() >= f64::EPSILON,
        }
    }
}

// -- Unified trigger config --

/// Per-trigger configuration stored in `pipeline_triggers.config` (JSONB/JSON).
///
/// Serialized with `trigger_type` as the serde tag so the correct variant is
/// reconstructed when loading from the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "trigger_type", rename_all = "snake_case")]
pub enum TriggerConfig {
    /// Fires on a cron expression or fixed interval.
    Schedule {
        /// The schedule cadence: cron or interval.
        mode: ScheduleMode,
        /// IANA timezone name used to evaluate cron expressions.
        ///
        /// Defaults to `"UTC"` when absent in the database row.
        #[serde(default = "default_timezone")]
        timezone: String,
    },
    /// Fires when an [`Event`] matching the optional filter arrives on the bus.
    ///
    /// [`Event`]: crate::event::Event
    Event {
        /// `evalexpr` expression evaluated against the event payload.
        ///
        /// Example: `"event.label == 'person' AND event.confidence > 0.85"`.
        /// When `None`, every event on the subscribed topic fires the trigger.
        filter: Option<String>,
        /// Post-event capture window forwarded to `extract_clip` nodes.
        duration_secs: Option<u32>,
    },
    /// Fires when a specific VMS lifecycle signal occurs, optionally scoped to
    /// one camera.
    System {
        /// The lifecycle signal to listen for.
        signal: SystemSignal,
        /// When `Some`, only signals from this camera will fire the trigger.
        camera_id: Option<Uuid>,
    },
    /// Fires when the REST API caller POSTs to `/pipelines/{id}/trigger`.
    Manual {
        /// JSON Schema that validates the request body sent by the API caller.
        ///
        /// The validated object is placed in [`TriggerContext::manual_params`]
        /// and is available to minijinja templates inside the pipeline.
        parameter_schema: Option<serde_json::Value>,
    },
    /// Fires when a system metric crosses a threshold for a sustained period.
    Stat {
        /// The metric to observe.
        metric: StatMetric,
        /// Filesystem path for disk metrics (e.g. `"/"`); `None` for RAM/CPU.
        path: Option<String>,
        /// The comparison applied to `actual_value` vs `threshold`.
        operator: CompareOperator,
        /// The value `actual` is compared against.
        threshold: f64,
        /// The condition must hold continuously for this many seconds before
        /// the trigger fires.  `0` means fire immediately.
        #[serde(default)]
        sustained_secs: u32,
        /// Minimum seconds between consecutive firings of this trigger.
        /// `0` disables the cooldown.
        #[serde(default)]
        cooldown_secs: u32,
    },
}

fn default_timezone() -> String {
    "UTC".into()
}

// -- Context passed to pipeline execution --

/// Snapshot of the conditions under which a trigger fired.
///
/// Constructed by the Trigger Manager and injected into the Pipeline Executor.
/// It is propagated unchanged through every node as the immutable "why did this
/// run?" record.  Nodes use it to look up the relevant camera, resolve
/// templates, and scope their operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerContext {
    /// The `pipeline_runs` row ID assigned by the executor once a run record
    /// is persisted.  `None` during the brief window before the DB write.
    pub run_id: Option<Uuid>,
    /// The UUID of the `pipeline_triggers` row that fired.
    pub trigger_id: Uuid,
    /// The UUID of the pipeline this run belongs to.
    pub pipeline_id: Uuid,
    /// Wall-clock time at which the trigger fired.
    pub fired_at: DateTime<Utc>,
    /// Source adapter UUID, present for [`TriggerType::Event`] triggers sourced
    /// from a source adapter.
    pub source_id: Option<Uuid>,
    /// Camera UUID associated with this run (from the trigger definition or the
    /// event that fired it).
    pub camera_id: Option<Uuid>,
    /// Human-readable camera name, resolved at fire time for use in templates.
    pub camera_name: Option<String>,
    /// The raw event payload for [`TriggerType::Event`] triggers.
    ///
    /// Available in `evalexpr` condition expressions as `event.*`.
    pub event_payload: Option<serde_json::Value>,
    /// Parameters supplied by the API caller for [`TriggerType::Manual`] triggers.
    ///
    /// Available in minijinja templates as `manual.*`.
    pub manual_params: Option<serde_json::Value>,
    /// Post-event capture duration forwarded from [`TriggerConfig::Event::duration_secs`]
    /// to `extract_clip` action nodes.
    pub duration_secs: Option<u32>,
    /// The kind of trigger that produced this context.
    pub trigger_type: TriggerType,
}

impl TriggerContext {
    /// Create a minimal context for a [`TriggerType::Schedule`] firing.
    ///
    /// Most optional fields are `None`; downstream action nodes that need a
    /// `camera_id` must have it configured directly on their [`ActionConfig`].
    ///
    /// [`ActionConfig`]: crate::action::ActionConfig
    pub fn for_schedule(trigger_id: Uuid, pipeline_id: Uuid) -> Self {
        Self {
            run_id: None,
            trigger_id,
            pipeline_id,
            fired_at: Utc::now(),
            source_id: None,
            camera_id: None,
            camera_name: None,
            event_payload: None,
            manual_params: None,
            duration_secs: None,
            trigger_type: TriggerType::Schedule,
        }
    }

    /// Create a context for a [`TriggerType::Manual`] firing.
    ///
    /// `params` is the validated JSON body supplied by the API caller and is
    /// forwarded as [`TriggerContext::manual_params`].
    pub fn for_manual(
        trigger_id: Uuid,
        pipeline_id: Uuid,
        params: Option<serde_json::Value>,
    ) -> Self {
        Self {
            run_id: None,
            trigger_id,
            pipeline_id,
            fired_at: Utc::now(),
            source_id: None,
            camera_id: None,
            camera_name: None,
            event_payload: None,
            manual_params: params,
            duration_secs: None,
            trigger_type: TriggerType::Manual,
        }
    }
}
