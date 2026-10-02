//! Event Bus message types.
//!
//! The VMS Event Bus is a [`tokio::sync::broadcast`] channel (or equivalent)
//! that carries [`Event`] values between the recording/analytics engine and the
//! pipeline executor.  [`TopicKey`] identifies the logical channel an event
//! belongs to, while [`Event`] is the envelope that flows over the wire.
//!
//! # Publishing flow
//!
//! 1. A camera pipeline or source adapter publishes an [`Event`] via
//!    `EventBus::publish(key, event)`.
//! 2. The Trigger Manager's subscription loop receives the event and evaluates
//!    any `event`-typed trigger filters against [`Event::payload`].
//! 3. If the filter passes, the Trigger Manager forwards a [`TriggerContext`]
//!    to the Pipeline Executor.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifies which broadcast channel on the Event Bus an event belongs to.
///
/// Each variant maps to a distinct topic string used for subscription routing.
/// Callers that want all events for a specific camera subscribe to
/// `TopicKey::Camera(camera_id)`.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum TopicKey {
    /// `camera/{id}/event`: events produced by the recording/analytics engine for one camera.
    Camera(Uuid),
    /// `source/{id}/event`: events published by an external source adapter.
    Source(Uuid),
    /// `system/vms`: VMS-internal signals (feed_disconnected, startup, etc.).
    System,
    /// `stat/vms`: periodic system metric snapshots (disk, RAM, CPU).
    Stat,
}

impl TopicKey {
    /// Returns the canonical topic string for this key.
    ///
    /// The string is stored on [`Event::topic`] and used for log filtering.
    pub fn topic_string(&self) -> String {
        match self {
            TopicKey::Camera(id) => format!("camera/{id}/event"),
            TopicKey::Source(id) => format!("source/{id}/event"),
            TopicKey::System => "system/vms".into(),
            TopicKey::Stat => "stat/vms".into(),
        }
    }
}

/// A single event flowing through the Event Bus.
///
/// Events are serialized to JSON when persisted to the `pipeline_run_events`
/// table and deserialized back when replaying history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// The canonical topic string derived from the [`TopicKey`] at publish time.
    pub topic: String,
    /// The source adapter that produced this event, if applicable.
    pub source_id: Option<Uuid>,
    /// The camera this event relates to, if applicable.
    pub camera_id: Option<Uuid>,
    /// A short discriminator string such as `"motion_detected"` or
    /// `"object_detected"` used by trigger filters.
    pub event_type: String,
    /// Arbitrary JSON payload whose structure depends on `event_type`.
    ///
    /// Trigger filters written in `evalexpr` are evaluated against this value.
    pub payload: serde_json::Value,
    /// Wall-clock time at which the event was created.
    pub occurred_at: DateTime<Utc>,
}

impl Event {
    /// Construct a new event for the given topic key.
    ///
    /// `source_id` and `camera_id` are extracted from `key` automatically;
    /// `occurred_at` is set to [`Utc::now`].
    pub fn new(key: &TopicKey, event_type: impl Into<String>, payload: serde_json::Value) -> Self {
        let (source_id, camera_id) = match key {
            TopicKey::Source(id) => (Some(*id), None),
            TopicKey::Camera(id) => (None, Some(*id)),
            _ => (None, None),
        };
        Self {
            topic: key.topic_string(),
            source_id,
            camera_id,
            event_type: event_type.into(),
            payload,
            occurred_at: Utc::now(),
        }
    }
}
