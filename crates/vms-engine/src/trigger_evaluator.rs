use std::sync::Arc;

use uuid::Uuid;
use vms_core::{TriggerConfig, TriggerContext, VmsError};

use crate::{EventBus, PipelineRegistry};

/// Evaluates pipeline triggers and dispatches pipeline runs.
///
/// Each trigger type is handled differently:
/// - `Manual`   — fired via [`fire_manual`] from the REST API handler.
/// - `Event`    — fired by the EventBus subscription loop (5e-2).
/// - `System`   — fired via the same EventBus loop on system topic events (5e-2).
/// - `Schedule` — fired by the cron / interval scheduler (5e-3).
/// - `Stat`     — fired when a metric event crosses a threshold (5e-4).
///
/// [`fire_pipeline`] is currently a stub — it logs the context and returns.
/// The real Pipeline Executor is wired in during 5g.
pub struct TriggerEvaluator {
    registry:  Arc<PipelineRegistry>,
    // Used by start_event_listener in 5e-2.
    event_bus: Arc<EventBus>,
}

impl TriggerEvaluator {
    pub fn new(registry: Arc<PipelineRegistry>, event_bus: Arc<EventBus>) -> Arc<Self> {
        Arc::new(Self { registry, event_bus })
    }

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

    /// Dispatch a pipeline run for the given trigger context.
    ///
    /// Stub — logs the firing event. Replaced by the real Pipeline Executor in 5g.
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
