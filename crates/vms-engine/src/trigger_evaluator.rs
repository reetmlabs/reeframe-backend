use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use evalexpr::{eval_boolean_with_context, ContextWithMutableVariables, HashMapContext, Value as EvalValue};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;
use vms_core::{
    Event, SystemSignal, TopicKey, TriggerConfig, TriggerContext, TriggerType, VmsError,
};

use crate::{EventBus, PipelineRegistry};

/// Evaluates pipeline triggers and dispatches pipeline runs.
///
/// Each trigger type is handled differently:
/// - `Manual`   — fired via [`fire_manual`] from the REST API handler.
/// - `Event`    — fired by the EventBus subscription loop ([`start_event_listener`]).
/// - `System`   — fired via the same EventBus loop on the system topic.
/// - `Schedule` — fired by the cron / interval scheduler.
/// - `Stat`     — fired when a metric event crosses a threshold.
///
/// [`fire_pipeline`] is currently a stub — it logs the context and returns.
/// The real Pipeline Executor is wired in future.
pub struct TriggerEvaluator {
    registry:  Arc<PipelineRegistry>,
    event_bus: Arc<EventBus>,
}

impl TriggerEvaluator {
    pub fn new(registry: Arc<PipelineRegistry>, event_bus: Arc<EventBus>) -> Arc<Self> {
        Arc::new(Self { registry, event_bus })
    }

    // ── Manual trigger ────────────────────────────────────────────────────────

    /// Fire the pipeline identified by `pipeline_id` as a manual trigger.
    ///
    /// Looks up the pipeline in the registry, verifies it is enabled, finds
    /// the first enabled `Manual` trigger, builds a [`TriggerContext`], and
    /// dispatches it to [`fire_pipeline`].
    ///
    /// # Errors
    /// - [`VmsError::PipelineNotFound`] — pipeline is not in the registry.
    /// - [`VmsError::NotFound`] — pipeline is disabled or has no enabled manual trigger.
    pub fn fire_manual(
        &self,
        pipeline_id: Uuid,
        params:      Option<serde_json::Value>,
    ) -> Result<(), VmsError> {
        let pipeline = self
            .registry
            .get(pipeline_id)
            .ok_or(VmsError::PipelineNotFound(pipeline_id))?;

        if !pipeline.enabled {
            return Err(VmsError::NotFound(format!(
                "pipeline {pipeline_id} is disabled"
            )));
        }

        let trigger = pipeline
            .triggers
            .iter()
            .find(|t| t.enabled && matches!(t.config, TriggerConfig::Manual { .. }))
            .ok_or_else(|| {
                VmsError::NotFound(format!(
                    "pipeline {pipeline_id} has no enabled manual trigger"
                ))
            })?;

        let ctx = TriggerContext::for_manual(trigger.id, pipeline_id, params);
        self.fire_pipeline(ctx);
        Ok(())
    }

    // ── Event listener ────────────────────────────────────────────────────────

    /// Subscribe to all EventBus topics referenced by enabled pipelines and
    /// spawn background listener tasks that call [`evaluate_event`] for each
    /// incoming event.
    ///
    /// One task is always spawned for `TopicKey::System`. Additional tasks are
    /// spawned for each unique `Camera` and `Source` topic needed. The topics
    /// are derived from the registry snapshot taken at call time — call again
    /// after a registry reload to pick up new cameras / sources.
    ///
    /// Safe to call from a non-async context (only spawns, does not await).
    pub fn start_event_listener(self: Arc<Self>) {
        let snapshot = self.registry.snapshot();

        let mut camera_ids: HashSet<Uuid> = HashSet::new();
        let mut source_ids: HashSet<Uuid> = HashSet::new();

        // What topics do we need? Those scoped to  a camera or source trigger.
        // or no scoped applied and it is applied to all cameras/sources the pipeline touches.
        for pipeline in snapshot.values() {
            if !pipeline.enabled {
                continue;
            }
            for trigger in &pipeline.triggers {
                if !trigger.enabled {
                    continue;
                }
                if let TriggerConfig::Event { .. } = &trigger.config {
                    // If the trigger is scoped to a specific source or camera subscribe
                    // only to that topic. Otherwise subscribe to every resource the
                    // pipeline touches.
                    if let Some(src_id) = trigger.source_id {
                        source_ids.insert(src_id);
                    } else if let Some(cam_id) = trigger.camera_id {
                        camera_ids.insert(cam_id);
                    } else {
                        for cam_ref in &pipeline.camera_refs {
                            camera_ids.insert(cam_ref.camera_id);
                        }
                        for &src_id in &pipeline.source_refs {
                            source_ids.insert(src_id);
                        }
                    }
                }
                // System triggers always arrive on TopicKey::System — handled below.
            }
        }

        // ── TopicKey::System ──────────────────────────────────────────────────
        {
            let mut rx = self.event_bus.subscribe(&TopicKey::System);
            let ev = self.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => ev.evaluate_event(&event),
                        Err(RecvError::Lagged(n)) => {
                            tracing::warn!(missed = n, topic = "system/vms", "event listener lagged");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                tracing::debug!(topic = "system/vms", "event listener task exited");
            });
        }

        // ── TopicKey::Camera(id) ──────────────────────────────────────────────
        for cam_id in camera_ids {
            let mut rx = self.event_bus.subscribe(&TopicKey::Camera(cam_id));
            let ev = self.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => ev.evaluate_event(&event),
                        Err(RecvError::Lagged(n)) => {
                            tracing::warn!(missed = n, %cam_id, "camera event listener lagged");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            });
        }

        // ── TopicKey::Source(id) ──────────────────────────────────────────────
        for src_id in source_ids {
            let mut rx = self.event_bus.subscribe(&TopicKey::Source(src_id));
            let ev = self.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => ev.evaluate_event(&event),
                        Err(RecvError::Lagged(n)) => {
                            tracing::warn!(missed = n, %src_id, "source event listener lagged");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            });
        }

        tracing::info!("Trigger evaluator event listeners started");
    }

    // ── Event evaluation ──────────────────────────────────────────────────────

    /// Evaluate all enabled pipeline triggers against `event`.
    ///
    /// Handles [`TriggerConfig::Event`] and [`TriggerConfig::System`].
    /// Schedule, Manual, and Stat triggers are skipped here.
    fn evaluate_event(&self, event: &Event) {
        let snapshot = self.registry.snapshot();

        for pipeline in snapshot.values() {
            if !pipeline.enabled {
                continue;
            }
            for trigger in &pipeline.triggers {
                if !trigger.enabled {
                    continue;
                }
                match &trigger.config {
                    TriggerConfig::Event { filter, duration_secs } => {
                        // Scope: if trigger is bound to a source or camera, verify the event matches.
                        if let Some(src_id) = trigger.source_id {
                            if event.source_id != Some(src_id) {
                                continue;
                            }
                        }
                        if let Some(cam_id) = trigger.camera_id {
                            if event.camera_id != Some(cam_id) {
                                continue;
                            }
                        }

                        // Apply the evalexpr filter if present.
                        if let Some(expr) = filter {
                            let ctx = build_event_context(event);
                            match eval_boolean_with_context(expr, &ctx) {
                                Ok(true) => {}
                                Ok(false) => continue,
                                Err(e) => {
                                    tracing::warn!(
                                        pipeline_id = %pipeline.id,
                                        trigger_id  = %trigger.id,
                                        error       = %e,
                                        "trigger filter expression failed — skipping"
                                    );
                                    continue;
                                }
                            }
                        }

                        self.fire_pipeline(TriggerContext {
                            run_id:        None,
                            trigger_id:    trigger.id,
                            pipeline_id:   pipeline.id,
                            fired_at:      Utc::now(),
                            source_id:     event.source_id,
                            camera_id:     event.camera_id,
                            camera_name:   None,
                            event_payload: Some(event.payload.clone()),
                            manual_params: None,
                            duration_secs: *duration_secs,
                            trigger_type:  TriggerType::Event,
                        });
                    }

                    TriggerConfig::System { signal, camera_id } => {
                        // The event_type field on system events encodes the signal name.
                        if event.event_type != signal_to_event_type(signal) {
                            continue;
                        }
                        // Optional camera scope filter.
                        if let Some(cam_id) = camera_id {
                            if event.camera_id != Some(*cam_id) {
                                continue;
                            }
                        }

                        self.fire_pipeline(TriggerContext {
                            run_id:        None,
                            trigger_id:    trigger.id,
                            pipeline_id:   pipeline.id,
                            fired_at:      Utc::now(),
                            source_id:     None,
                            camera_id:     event.camera_id,
                            camera_name:   None,
                            event_payload: Some(event.payload.clone()),
                            manual_params: None,
                            duration_secs: None,
                            trigger_type:  TriggerType::System,
                        });
                    }

                    // Not handled by the event loop.
                    TriggerConfig::Schedule { .. }
                    | TriggerConfig::Manual { .. }
                    | TriggerConfig::Stat { .. } => {}
                }
            }
        }
    }

    // ── Pipeline dispatch ─────────────────────────────────────────────────────

    /// Dispatch a pipeline run for the given trigger context.
    ///
    /// Stub — logs the firing event. Replaced by the real Pipeline Executor.
    pub(crate) fn fire_pipeline(&self, ctx: TriggerContext) {
        tracing::info!(
            pipeline_id  = %ctx.pipeline_id,
            trigger_id   = %ctx.trigger_id,
            trigger_type = ?ctx.trigger_type,
            fired_at     = %ctx.fired_at,
            "Pipeline trigger fired",
        );
    }
}

// ── Module-level helpers ──────────────────────────────────────────────────────

/// Build an evalexpr context from `event` for filter expression evaluation.
///
/// The event's payload fields are exposed as `event.<key>`.  String, float,
/// and boolean values are mapped; nested objects and arrays are silently
/// skipped.  The `event.type` variable is always set to `event.event_type`.
///
/// Example expression: `event.confidence > 0.85 && event.label == "person"`
fn build_event_context(event: &Event) -> HashMapContext {
    let mut ctx = HashMapContext::new();
    ctx.set_value(
        "event.type".into(),
        EvalValue::String(event.event_type.clone()),
    )
    .ok();

    if let serde_json::Value::Object(map) = &event.payload {
        for (k, v) in map {
            let key = format!("event.{k}");
            let eval_val = match v {
                serde_json::Value::String(s) => EvalValue::String(s.clone()),
                serde_json::Value::Number(n) => {
                    let Some(f) = n.as_f64() else { continue };
                    EvalValue::Float(f)
                }
                serde_json::Value::Bool(b) => EvalValue::Boolean(*b),
                _ => continue,
            };
            ctx.set_value(key, eval_val).ok();
        }
    }
    ctx
}

/// Map a [`SystemSignal`] to the `event_type` string published on the bus.
fn signal_to_event_type(signal: &SystemSignal) -> &'static str {
    match signal {
        SystemSignal::ChunkFinished    => "chunk_finished",
        SystemSignal::FeedDisconnected => "feed_disconnected",
        SystemSignal::FeedReconnected  => "feed_reconnected",
        SystemSignal::RecordingStarted => "recording_started",
        SystemSignal::RecordingStopped => "recording_stopped",
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(event_type: &str, payload: serde_json::Value) -> Event {
        Event {
            topic:      "camera/test/event".into(),
            source_id:  None,
            camera_id:  Some(Uuid::new_v4()),
            event_type: event_type.into(),
            payload,
            occurred_at: Utc::now(),
        }
    }

    // Build a context from a sample event and verify evalexpr can evaluate
    // a filter expression against it.
    #[test]
    fn event_context_filter_passes() {
        let event = make_event(
            "object_detected",
            json!({"label": "person", "confidence": 0.92}),
        );
        let ctx = build_event_context(&event);
        let result = eval_boolean_with_context(
            r#"event.confidence > 0.85 && event.label == "person""#,
            &ctx,
        );
        assert_eq!(result, Ok(true));
    }

    // A filter that does not match should return false, not an error.
    #[test]
    fn event_context_filter_does_not_match() {
        let event = make_event(
            "object_detected",
            json!({"label": "car", "confidence": 0.7}),
        );
        let ctx = build_event_context(&event);
        let result =
            eval_boolean_with_context(r#"event.label == "person""#, &ctx);
        assert_eq!(result, Ok(false));
    }

    // signal_to_event_type round-trips for every variant.
    #[test]
    fn signal_event_type_mapping_is_exhaustive() {
        use SystemSignal::*;
        for signal in [
            ChunkFinished,
            FeedDisconnected,
            FeedReconnected,
            RecordingStarted,
            RecordingStopped,
        ] {
            let s = signal_to_event_type(&signal);
            assert!(!s.is_empty());
        }
    }
}
