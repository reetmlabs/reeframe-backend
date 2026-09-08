use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use evalexpr::{
    eval_boolean_with_context, ContextWithMutableVariables, HashMapContext, Value as EvalValue,
};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;
use vms_core::{
    pipeline::CompiledPipeline, Event, ScheduleMode, StatMetric, SystemSignal, TopicKey,
    TriggerConfig, TriggerContext, TriggerType, VmsError,
};
use vms_db::repos::PipelineRepo;

use crate::{
    pipeline_registry::RegistrySnapshot, time_helpers, EventBus, PipelineExecutor, PipelineRegistry,
};

/// Evaluates pipeline triggers and dispatches pipeline runs.
///
/// Each trigger type is handled differently:
/// - `Manual`   — fired via [`fire_manual`] from the REST API handler.
/// - `Event`    — fired by the EventBus subscription loop ([`start_event_listener`]).
/// - `System`   — fired via the same EventBus loop on the system topic.
/// - `Schedule` — fired by the cron / interval scheduler.
/// - `Stat`     — fired when a metric event crosses a threshold.
pub struct TriggerEvaluator {
    registry: Arc<PipelineRegistry>,
    event_bus: Arc<EventBus>,
    executor: Option<Arc<PipelineExecutor>>,
    /// `None` only in the test-only constructor — production always has one,
    /// used to persist [`build_event_context`]/filter-eval failures so a bad
    /// filter is visible via the trigger's own API representation instead of
    /// only a `tracing::warn!` line.
    pipeline_repo: Option<PipelineRepo>,
    /// Holds the cron scheduler after `start_schedulers` is called.
    cron_scheduler: Mutex<Option<JobScheduler>>,
    /// JoinHandles for all spawned interval trigger tasks.
    /// Stored so they can be aborted on reload or shutdown.
    interval_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Tracks the last time each stat trigger fired: (pipeline_id, trigger_id) -> Instant.
    stat_cooldowns: DashMap<(Uuid, Uuid), Instant>,
    /// Tracks when the stat condition was first observed as true: used to enforce sustained_secs.
    stat_sustained: DashMap<(Uuid, Uuid), Instant>,
    /// Cancelled by `stop_event_listener` to signal all event-loop tasks to exit.
    event_listener_token: tokio_util::sync::CancellationToken,
}

impl TriggerEvaluator {
    pub fn new(
        registry: Arc<PipelineRegistry>,
        event_bus: Arc<EventBus>,
        executor: Arc<PipelineExecutor>,
        pipeline_repo: PipelineRepo,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            event_bus,
            executor: Some(executor),
            pipeline_repo: Some(pipeline_repo),
            cron_scheduler: Mutex::new(None),
            interval_tasks: std::sync::Mutex::new(Vec::new()),
            stat_cooldowns: DashMap::new(),
            stat_sustained: DashMap::new(),
            event_listener_token: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Test-only constructor — no executor, `fire_pipeline` logs and returns.
    #[cfg(test)]
    pub(crate) fn new_without_executor(
        registry: Arc<PipelineRegistry>,
        event_bus: Arc<EventBus>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            event_bus,
            executor: None,
            pipeline_repo: None,
            cron_scheduler: Mutex::new(None),
            interval_tasks: std::sync::Mutex::new(Vec::new()),
            stat_cooldowns: DashMap::new(),
            stat_sustained: DashMap::new(),
            event_listener_token: tokio_util::sync::CancellationToken::new(),
        })
    }

    // -- Manual trigger --

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
        params: Option<serde_json::Value>,
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

    // -- Schedule triggers --

    /// Abort all running interval tasks and shut down the cron scheduler.
    ///
    /// Safe to call when no schedulers are running — both operations are no-ops
    /// in that case. Called at the top of [`start_schedulers`] to replace the
    /// current task set on reload, and by the daemon shutdown sequence.
    pub async fn stop_schedulers(&self) {
        let handles: Vec<_> = self.interval_tasks.lock().unwrap().drain(..).collect();
        for handle in handles {
            handle.abort();
        }

        if let Some(mut scheduler) = self.cron_scheduler.lock().await.take() {
            if let Err(e) = scheduler.shutdown().await {
                tracing::warn!(error = %e, "cron scheduler shutdown error");
            }
        }

        tracing::debug!("Trigger evaluator schedulers stopped");
    }

    /// Cancel the event listener tasks spawned by [`start_event_listener`].
    ///
    /// Each event loop task selects on the cancellation token and exits cleanly
    /// on the next iteration after this is called.
    pub fn stop_event_listener(&self) {
        self.event_listener_token.cancel();
        tracing::debug!("Trigger evaluator event listeners stopped");
    }

    /// Register schedule triggers (cron + interval) for all enabled pipelines
    /// and start the underlying cron scheduler.
    ///
    /// - `Interval` triggers spawn a `tokio::task` that sleeps for
    ///   `interval_secs` and fires [`fire_pipeline`] on each tick.
    /// - `Cron` triggers are registered with `tokio-cron-scheduler`, which
    ///   fires [`fire_pipeline`] on every matching tick. IANA timezone strings
    ///   are parsed via `chrono-tz`; an unknown timezone falls back to UTC with
    ///   a warning.
    ///
    /// Aborts any previously running interval tasks and shuts down the previous
    /// cron scheduler before registering new ones — safe to call on reload.
    pub async fn start_schedulers(self: Arc<Self>) -> Result<(), VmsError> {
        self.stop_schedulers().await;
        let snapshot = self.registry.snapshot();

        let scheduler = JobScheduler::new()
            .await
            .map_err(|e| VmsError::Config(format!("scheduler init: {e}")))?;

        let mut cron_job_count: usize = 0;

        for pipeline in snapshot.pipelines.values() {
            if !pipeline.enabled {
                continue;
            }
            for trigger in &pipeline.triggers {
                if !trigger.enabled {
                    continue;
                }
                let TriggerConfig::Schedule { mode, timezone } = &trigger.config else {
                    continue;
                };

                let pipeline_id = pipeline.id;
                let trigger_id = trigger.id;

                match mode {
                    // -- Interval --
                    ScheduleMode::Interval { interval_secs } => {
                        let secs = *interval_secs;
                        let ev = self.clone();
                        let handle = tokio::spawn(async move {
                            loop {
                                tokio::time::sleep(Duration::from_secs(secs)).await;
                                ev.fire_pipeline(TriggerContext::for_schedule(
                                    trigger_id,
                                    pipeline_id,
                                ));
                            }
                        });
                        self.interval_tasks.lock().unwrap().push(handle);
                        tracing::debug!(
                            %pipeline_id, %trigger_id, interval_secs,
                            "Interval trigger scheduled"
                        );
                    }

                    // -- Cron --
                    ScheduleMode::Cron { expression } => {
                        let tz = time_helpers::parse_iana_tz(timezone);
                        let expr = time_helpers::normalize_cron(expression);
                        let ev = self.clone();

                        let job = Job::new_async_tz(expr.clone(), tz, move |_id, _sched| {
                            let ev = ev.clone();
                            Box::pin(async move {
                                ev.fire_pipeline(TriggerContext::for_schedule(
                                    trigger_id,
                                    pipeline_id,
                                ));
                            })
                        })
                        .map_err(|e| {
                            VmsError::Config(format!(
                                "invalid cron expression '{expression}' for pipeline \
                                 {pipeline_id}: {e}"
                            ))
                        })?;

                        scheduler
                            .add(job)
                            .await
                            .map_err(|e| VmsError::Config(format!("add cron job: {e}")))?;

                        cron_job_count += 1;
                        tracing::debug!(
                            %pipeline_id, %trigger_id,
                            expression = %expr,
                            timezone   = %timezone,
                            "Cron trigger scheduled"
                        );
                    }
                }
            }
        }

        if cron_job_count > 0 {
            scheduler
                .start()
                .await
                .map_err(|e| VmsError::Config(format!("start cron scheduler: {e}")))?;
        }

        *self.cron_scheduler.lock().await = Some(scheduler);

        tracing::info!(
            cron_jobs = cron_job_count,
            "Trigger evaluator schedulers started"
        );
        Ok(())
    }

    // -- Event listener --

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
        for pipeline in snapshot.pipelines.values() {
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

        // -- TopicKey::System --
        {
            let mut rx = self.event_bus.subscribe(&TopicKey::System);
            let ev = self.clone();
            let token = self.event_listener_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = rx.recv() => match result {
                            Ok(event) => ev.evaluate_event(&TopicKey::System, &event).await,
                            Err(RecvError::Lagged(n)) => {
                                tracing::warn!(
                                    missed = n,
                                    topic = "system/vms",
                                    "event listener lagged"
                                );
                            }
                            Err(RecvError::Closed) => break,
                        },
                        _ = token.cancelled() => break,
                    }
                }
                tracing::debug!(topic = "system/vms", "event listener task exited");
            });
        }

        // -- TopicKey::Camera(id) --
        for cam_id in camera_ids {
            let mut rx = self.event_bus.subscribe(&TopicKey::Camera(cam_id));
            let ev = self.clone();
            let token = self.event_listener_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = rx.recv() => match result {
                            Ok(event) => ev.evaluate_event(&TopicKey::Camera(cam_id), &event).await,
                            Err(RecvError::Lagged(n)) => {
                                tracing::warn!(missed = n, %cam_id, "camera event listener lagged");
                            }
                            Err(RecvError::Closed) => break,
                        },
                        _ = token.cancelled() => break,
                    }
                }
            });
        }

        // -- TopicKey::Source(id) --
        for src_id in source_ids {
            let mut rx = self.event_bus.subscribe(&TopicKey::Source(src_id));
            let ev = self.clone();
            let token = self.event_listener_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = rx.recv() => match result {
                            Ok(event) => ev.evaluate_event(&TopicKey::Source(src_id), &event).await,
                            Err(RecvError::Lagged(n)) => {
                                tracing::warn!(missed = n, %src_id, "source event listener lagged");
                            }
                            Err(RecvError::Closed) => break,
                        },
                        _ = token.cancelled() => break,
                    }
                }
            });
        }

        tracing::info!("Trigger evaluator event listeners started");
    }

    // -- Event evaluation --

    /// Evaluate pipeline triggers for `event` arriving on `topic`.
    ///
    /// Uses the pre-built trigger index for O(1) dispatch — only pipelines with
    /// a matching `(topic, trigger_type)` entry are examined.
    async fn evaluate_event(&self, topic: &TopicKey, event: &Event) {
        let snapshot = self.registry.snapshot();

        let trigger_type = match topic {
            TopicKey::System => TriggerType::System,
            _ => TriggerType::Event,
        };

        let Some(entries) = snapshot.trigger_index.get(&(topic.clone(), trigger_type)) else {
            return;
        };

        for &(pipeline_id, trigger_id) in entries {
            let Some(pipeline) = snapshot.pipelines.get(&pipeline_id) else {
                continue;
            };
            if !pipeline.enabled {
                continue;
            }
            let Some(trigger) = pipeline.triggers.iter().find(|t| t.id == trigger_id) else {
                continue;
            };
            if !trigger.enabled {
                continue;
            }

            match &trigger.config {
                TriggerConfig::Event {
                    filter,
                    duration_secs,
                } => {
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

                    if let Some(expr) = filter {
                        let ctx = build_event_context(event);
                        match eval_boolean_with_context(expr, &ctx) {
                            Ok(matched) => {
                                if trigger.last_error.is_some() {
                                    self.clear_trigger_error(trigger_id).await;
                                }
                                tracing::info!(
                                    %pipeline_id, %trigger_id, matched,
                                    "Trigger condition evaluated",
                                );
                                if !matched {
                                    continue;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    pipeline_id = %pipeline_id,
                                    trigger_id  = %trigger_id,
                                    error       = %e,
                                    "trigger filter expression failed — skipping"
                                );
                                if trigger.last_error.as_deref() != Some(e.to_string().as_str()) {
                                    self.set_trigger_error(trigger_id, e.to_string()).await;
                                }
                                continue;
                            }
                        }
                    }

                    self.fire_pipeline(TriggerContext {
                        run_id: None,
                        trigger_id,
                        pipeline_id,
                        fired_at: Utc::now(),
                        source_id: event.source_id,
                        camera_id: event.camera_id,
                        camera_name: None,
                        event_payload: Some(event.payload.clone()),
                        manual_params: None,
                        duration_secs: *duration_secs,
                        trigger_type: TriggerType::Event,
                    });
                }

                TriggerConfig::System { signal, camera_id } => {
                    if event.event_type != signal_to_event_type(signal) {
                        continue;
                    }
                    if let Some(cam_id) = camera_id {
                        if event.camera_id != Some(*cam_id) {
                            continue;
                        }
                    }

                    self.fire_pipeline(TriggerContext {
                        run_id: None,
                        trigger_id,
                        pipeline_id,
                        fired_at: Utc::now(),
                        source_id: None,
                        camera_id: event.camera_id,
                        camera_name: None,
                        event_payload: Some(event.payload.clone()),
                        manual_params: None,
                        duration_secs: None,
                        trigger_type: TriggerType::System,
                    });
                }

                _ => {}
            }
        }
    }

    // -- Stat trigger evaluation --

    /// Called by the Stat Monitor with the latest reading for `metric`.
    ///
    /// Scans all enabled pipelines for `Stat` triggers whose metric, path, and
    /// camera scope match the supplied sample.  For each matching trigger:
    ///
    /// 1. If the condition (`operator(actual, threshold)`) is **false** the
    ///    sustained clock is reset so the next rising edge starts fresh.
    /// 2. If the condition is **true** and `sustained_secs > 0`, the trigger
    ///    waits until it has been continuously true for that many seconds.
    /// 3. Once sustained, the trigger is gated by `cooldown_secs` — it will
    ///    not fire again until at least that many seconds have elapsed since the
    ///    last firing.
    ///
    /// `path` applies only to disk metrics (e.g. `"/var/lib/vms"`).
    /// `camera_id` applies only to per-feed metrics (`FeedBitrateKbps`,
    /// `FeedPacketLossPercent`).
    pub fn evaluate_stat(
        &self,
        metric: &StatMetric,
        path: Option<&str>,
        camera_id: Option<Uuid>,
        actual: f64,
    ) {
        let snapshot = self.registry.snapshot();
        self.evaluate_stat_impl(&snapshot.pipelines, metric, path, camera_id, actual);
    }

    /// Same as [`evaluate_stat`] but uses a caller-provided snapshot.
    ///
    /// Call from `stat_monitor::poll` after taking a single snapshot for the
    /// whole poll cycle so all metrics in one pass see a consistent registry.
    pub(crate) fn evaluate_stat_impl(
        &self,
        snapshot: &HashMap<Uuid, Arc<CompiledPipeline>>,
        metric: &StatMetric,
        path: Option<&str>,
        camera_id: Option<Uuid>,
        actual: f64,
    ) {
        let now = Instant::now();

        for pipeline in snapshot.values() {
            if !pipeline.enabled {
                continue;
            }
            for trigger in &pipeline.triggers {
                if !trigger.enabled {
                    continue;
                }
                let TriggerConfig::Stat {
                    metric: t_metric,
                    path: t_path,
                    operator,
                    threshold,
                    sustained_secs,
                    cooldown_secs,
                } = &trigger.config
                else {
                    continue;
                };

                if t_metric != metric {
                    continue;
                }

                // Per-feed metrics are scoped to a camera; others are not.
                if matches!(
                    metric,
                    StatMetric::FeedBitrateKbps | StatMetric::FeedPacketLossPercent
                ) && trigger.camera_id != camera_id
                {
                    continue;
                }

                // Disk metrics may be scoped to a filesystem path.
                if let Some(tp) = t_path {
                    if path.map_or(true, |p| p != tp.as_str()) {
                        continue;
                    }
                }

                let key = (pipeline.id, trigger.id);

                if operator.evaluate(actual, *threshold) {
                    // Record the first instant the condition was observed true.
                    let observed_at = *self.stat_sustained.entry(key).or_insert(now);

                    // Wait until the condition has been sustained long enough.
                    if now.duration_since(observed_at).as_secs() as u32 >= *sustained_secs {
                        // Enforce cooldown.
                        let in_cooldown = *cooldown_secs > 0
                            && self.stat_cooldowns.get(&key).map_or(false, |last| {
                                (now.duration_since(*last).as_secs() as u32) < *cooldown_secs
                            });

                        if !in_cooldown {
                            self.stat_cooldowns.insert(key, now);
                            self.fire_pipeline(TriggerContext {
                                run_id: None,
                                trigger_id: trigger.id,
                                pipeline_id: pipeline.id,
                                fired_at: Utc::now(),
                                source_id: None,
                                camera_id,
                                camera_name: None,
                                event_payload: None,
                                manual_params: None,
                                duration_secs: None,
                                trigger_type: TriggerType::Stat,
                            });
                        }
                    }
                } else {
                    // Condition no longer met — reset sustained clock so the
                    // next rising edge requires a fresh sustained period.
                    self.stat_sustained.remove(&key);
                }
            }
        }
    }

    // -- Pipeline dispatch --

    /// Persist a filter-evaluation failure so it's visible via the
    /// trigger's own API representation instead of only a log line. A no-op
    /// in test builds, where no [`PipelineRepo`] is wired.
    async fn set_trigger_error(&self, trigger_id: Uuid, error: String) {
        let Some(repo) = &self.pipeline_repo else {
            return;
        };
        if let Err(e) = repo.set_trigger_error(trigger_id, Some(error)).await {
            tracing::warn!(%trigger_id, error = %e, "failed to persist trigger filter error");
        }
    }

    /// Clear a previously recorded filter-evaluation failure once the
    /// filter evaluates successfully again.
    async fn clear_trigger_error(&self, trigger_id: Uuid) {
        let Some(repo) = &self.pipeline_repo else {
            return;
        };
        if let Err(e) = repo.set_trigger_error(trigger_id, None).await {
            tracing::warn!(%trigger_id, error = %e, "failed to clear trigger filter error");
        }
    }

    /// Dispatch a pipeline run for the given trigger context.
    ///
    /// Looks up the pipeline in the registry and spawns an async task that
    /// calls [`PipelineExecutor::execute`].  If no executor is wired (test
    /// builds) the firing is only logged.
    pub(crate) fn fire_pipeline(&self, ctx: TriggerContext) {
        let Some(executor) = self.executor.clone() else {
            tracing::info!(
                pipeline_id  = %ctx.pipeline_id,
                trigger_id   = %ctx.trigger_id,
                trigger_type = ?ctx.trigger_type,
                fired_at     = %ctx.fired_at,
                "Pipeline trigger fired (no executor wired)",
            );
            return;
        };

        let Some(pipeline) = self.registry.get(ctx.pipeline_id) else {
            tracing::warn!(
                pipeline_id = %ctx.pipeline_id,
                "Pipeline not found in registry at fire time — skipping",
            );
            return;
        };

        tracing::info!(
            pipeline_id  = %ctx.pipeline_id,
            trigger_id   = %ctx.trigger_id,
            trigger_type = ?ctx.trigger_type,
            fired_at     = %ctx.fired_at,
            "Pipeline trigger fired",
        );

        tokio::spawn(async move {
            // execute() already logs its own run-scoped outcome (with
            // run_id) — no need for a second, less-correlated echo here.
            let _ = executor.execute(ctx, pipeline).await;
        });
    }
}

// -- Module-level helpers --

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
    ctx.set_value("event.topic".into(), EvalValue::String(event.topic.clone()))
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
        SystemSignal::ChunkFinished => "chunk_finished",
        SystemSignal::FeedDisconnected => "feed_disconnected",
        SystemSignal::FeedReconnected => "feed_reconnected",
        SystemSignal::RecordingStarted => "recording_started",
        SystemSignal::RecordingStopped => "recording_stopped",
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(event_type: &str, payload: serde_json::Value) -> Event {
        Event {
            topic: "camera/test/event".into(),
            source_id: None,
            camera_id: Some(Uuid::new_v4()),
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

    // A filter referencing event.topic must resolve, not error out with
    // VariableIdentifierNotFound (regression: topic was never added to the
    // eval context).
    #[test]
    fn event_context_filter_matches_on_topic() {
        let event = make_event("object_detected", json!({"label": "person"}));
        let ctx = build_event_context(&event);
        let result = eval_boolean_with_context(r#"event.topic == "camera/test/event""#, &ctx);
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
        let result = eval_boolean_with_context(r#"event.label == "person""#, &ctx);
        assert_eq!(result, Ok(false));
    }

    // -- Stat trigger helpers --

    fn make_stat_pipeline(
        pipeline_id: Uuid,
        metric: StatMetric,
        threshold: f64,
        sustained_secs: u32,
        cooldown_secs: u32,
    ) -> vms_core::pipeline::CompiledPipeline {
        use std::collections::HashMap;
        use vms_core::{
            pipeline::{CompiledPipeline, PipelineDag, PipelineTrigger},
            CompareOperator, TriggerConfig,
        };
        CompiledPipeline {
            id: pipeline_id,
            name: "test".into(),
            enabled: true,
            dag: PipelineDag {
                nodes: HashMap::new(),
                edges: vec![],
                topological_order: vec![],
                adjacency: HashMap::new(),
                parents: HashMap::new(),
                edge_types: HashMap::new(),
                root_id: Uuid::nil(),
            },
            triggers: vec![PipelineTrigger {
                id: Uuid::new_v4(),
                pipeline_id,
                trigger_type: vms_core::TriggerType::Stat,
                enabled: true,
                source_id: None,
                camera_id: None,
                config: TriggerConfig::Stat {
                    metric,
                    path: None,
                    operator: CompareOperator::GreaterThan,
                    threshold,
                    sustained_secs,
                    cooldown_secs,
                },
                last_error: None,
                last_error_at: None,
                unresolved_reference: false,
            }],
            camera_refs: vec![],
            source_refs: vec![],
        }
    }

    // Stat trigger fires when condition is met with sustained_secs == 0.
    #[test]
    fn stat_trigger_fires_immediately_when_sustained_zero() {
        use vms_core::StatMetric;
        let pipeline_id = Uuid::new_v4();
        let pipeline = make_stat_pipeline(pipeline_id, StatMetric::CpuUsagePercent, 80.0, 0, 0);
        let registry = PipelineRegistry::new_test(vec![pipeline]);
        let event_bus = EventBus::new(16);
        let ev = TriggerEvaluator::new_without_executor(registry, event_bus);

        // No panic; stat_cooldowns should have an entry after firing.
        ev.evaluate_stat(&StatMetric::CpuUsagePercent, None, None, 90.0);
        assert_eq!(ev.stat_cooldowns.len(), 1);
    }

    // Stat trigger does NOT fire when actual is below threshold.
    #[test]
    fn stat_trigger_does_not_fire_when_below_threshold() {
        use vms_core::StatMetric;
        let pipeline_id = Uuid::new_v4();
        let pipeline = make_stat_pipeline(pipeline_id, StatMetric::CpuUsagePercent, 80.0, 0, 0);
        let registry = PipelineRegistry::new_test(vec![pipeline]);
        let event_bus = EventBus::new(16);
        let ev = TriggerEvaluator::new_without_executor(registry, event_bus);

        ev.evaluate_stat(&StatMetric::CpuUsagePercent, None, None, 50.0);
        assert!(ev.stat_cooldowns.is_empty());
    }

    // Stat trigger does NOT fire on the first sample when sustained_secs > 0.
    #[test]
    fn stat_trigger_waits_for_sustained_duration() {
        use vms_core::StatMetric;
        let pipeline_id = Uuid::new_v4();
        // sustained_secs = 30 — won't fire on the first call.
        let pipeline = make_stat_pipeline(pipeline_id, StatMetric::RamUsagePercent, 70.0, 30, 0);
        let registry = PipelineRegistry::new_test(vec![pipeline]);
        let event_bus = EventBus::new(16);
        let ev = TriggerEvaluator::new_without_executor(registry, event_bus);

        ev.evaluate_stat(&StatMetric::RamUsagePercent, None, None, 85.0);
        // Condition is met, but not yet sustained for 30s — no firing.
        assert!(ev.stat_cooldowns.is_empty());
        // The sustained clock should have started.
        assert_eq!(ev.stat_sustained.len(), 1);
    }

    // Falling edge resets the sustained clock.
    #[test]
    fn stat_trigger_resets_sustained_on_falling_edge() {
        use vms_core::StatMetric;
        let pipeline_id = Uuid::new_v4();
        let pipeline = make_stat_pipeline(pipeline_id, StatMetric::CpuUsagePercent, 80.0, 10, 0);
        let registry = PipelineRegistry::new_test(vec![pipeline]);
        let event_bus = EventBus::new(16);
        let ev = TriggerEvaluator::new_without_executor(registry, event_bus);

        // Rising edge — sustained clock starts.
        ev.evaluate_stat(&StatMetric::CpuUsagePercent, None, None, 90.0);
        assert_eq!(ev.stat_sustained.len(), 1);

        // Falling edge — sustained clock clears.
        ev.evaluate_stat(&StatMetric::CpuUsagePercent, None, None, 50.0);
        assert!(ev.stat_sustained.is_empty());
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
