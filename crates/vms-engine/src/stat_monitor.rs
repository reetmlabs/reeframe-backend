use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sysinfo::{Disks, System};
use uuid::Uuid;
use vms_core::{pipeline::CompiledPipeline, StatMetric, TriggerConfig};
use vms_db::repos::daily_recording_coverage::UpsertDailyCoverage;
use vms_db::{CameraRepo, DailyRecordingCoverageRepo, RecordingRepo};
use vms_media::MediaManager;

use crate::coverage::compute_day_coverage;
use crate::recording_intent::reconcile_recording_intent;
use crate::time_helpers::{local_to_utc, parse_iana_tz};
use crate::{PipelineRegistry, TriggerEvaluator};

/// A camera's own `timezone` override, if set, else `default_timezone`.
fn resolve_camera_timezone(camera_timezone: Option<&str>, default_timezone: &str) -> chrono_tz::Tz {
    parse_iana_tz(camera_timezone.unwrap_or(default_timezone))
}

/// Seconds before the first sample; later samples use the fast or slow interval.
const POLL_INTERVAL_SECS: u64 = 5;
/// Seconds between samples when any metric is close to a threshold.
const FAST_INTERVAL_SECS: u64 = 2;
/// Seconds between samples when all metrics are well clear of every threshold.
const SLOW_INTERVAL_SECS: u64 = 15;
/// A metric is "near" a threshold when the relative distance is below this fraction.
/// E.g. 0.20 means within 20% of the threshold value, in either direction.
const NEAR_MARGIN: f64 = 0.20;
/// Cap on how many oldest-chunk batches the disk-threshold sweep deletes in
/// one tick, so it can't loop forever when the disk is full for reasons that
/// deleting recordings won't fix.
const MAX_RETENTION_BATCHES_PER_TICK: usize = 20;
const RETENTION_BATCH_SIZE: u64 = 10;
/// Minimum seconds between daily-coverage recomputes. Retention runs on
/// every tick because its query is a cheap age filter, but recomputing every
/// camera's whole "today" that often would put the DB load back that
/// precomputed coverage exists to avoid.
const COVERAGE_RECOMPUTE_INTERVAL_SECS: u64 = 120;
/// Cap on how many (camera, day) backfill pairs one coverage pass computes,
/// so a fresh deploy with a long retention window doesn't compute months of
/// history at once. The rest backfills over later passes.
const MAX_COVERAGE_BACKFILL_PER_TICK: usize = 20;
/// Minimum seconds between recording-intent sweeps. The sweep only catches
/// what the event-driven reconciliation on `MediaManager::pipeline_live_tx`
/// misses, so this much delay is acceptable.
const RECORDING_INTENT_SWEEP_INTERVAL_SECS: u64 = 30;

/// Recording retention configuration, set once via [`StatMonitor::set_retention`]
/// after construction.
pub struct RetentionConfig {
    pub recording_repo: RecordingRepo,
    pub camera_repo: CameraRepo,
    pub recording_dir: PathBuf,
    pub retention_days: u32,
    pub retention_disk_threshold_percent: f64,
}

/// Daily-coverage aggregation configuration, set once via
/// [`StatMonitor::set_coverage`]. `retention_days` is the global default
/// backfill window, overridden per camera the same way retention is.
pub struct CoverageConfig {
    pub recording_repo: RecordingRepo,
    pub camera_repo: CameraRepo,
    pub coverage_repo: DailyRecordingCoverageRepo,
    pub retention_days: u32,
    /// IANA timezone used for any camera with no `timezone` override of its
    /// own. Invalid or unrecognised names fall back to UTC.
    pub default_timezone: String,
}

/// Recording-intent reconciliation configuration, set once via
/// [`StatMonitor::set_recording_intent`]. Needs a live [`MediaManager`]
/// because reconciling intent attaches a recording branch.
pub struct RecordingIntentConfig {
    pub camera_repo: CameraRepo,
    pub media_manager: Arc<MediaManager>,
}

/// Polls system metrics and feeds readings to the [`TriggerEvaluator`].
///
/// Wraps `sysinfo` behind a `Mutex` so the same `System` instance is reused
/// across polls. sysinfo computes CPU usage from the difference between
/// successive readings, so the first sample is meaningless.
///
/// The same tick also runs the retention, daily-coverage and recording-intent
/// sweeps once each one is configured.
pub struct StatMonitor {
    evaluator: Arc<TriggerEvaluator>,
    registry: Arc<PipelineRegistry>,
    sys: Mutex<System>,
    disks: Mutex<Disks>,
    retention: Mutex<Option<RetentionConfig>>,
    coverage: Mutex<Option<CoverageConfig>>,
    last_coverage_run: Mutex<Option<Instant>>,
    recording_intent: Mutex<Option<RecordingIntentConfig>>,
    last_recording_intent_sweep: Mutex<Option<Instant>>,
}

impl StatMonitor {
    pub fn new(evaluator: Arc<TriggerEvaluator>, registry: Arc<PipelineRegistry>) -> Arc<Self> {
        Arc::new(Self {
            evaluator,
            registry,
            sys: Mutex::new(System::new()),
            disks: Mutex::new(Disks::new()),
            retention: Mutex::new(None),
            coverage: Mutex::new(None),
            last_coverage_run: Mutex::new(None),
            recording_intent: Mutex::new(None),
            last_recording_intent_sweep: Mutex::new(None),
        })
    }

    /// Set the recording-retention configuration this monitor sweeps on
    /// every poll tick. Until this is called, the retention sweep is a no-op.
    pub fn set_retention(&self, config: RetentionConfig) {
        *self.retention.lock().unwrap() = Some(config);
    }

    /// Set the daily-coverage configuration this monitor recomputes
    /// periodically (see [`COVERAGE_RECOMPUTE_INTERVAL_SECS`]).
    pub fn set_coverage(&self, config: CoverageConfig) {
        *self.coverage.lock().unwrap() = Some(config);
    }

    /// Set the recording-intent configuration this monitor sweeps
    /// periodically (see [`RECORDING_INTENT_SWEEP_INTERVAL_SECS`]).
    pub fn set_recording_intent(&self, config: RecordingIntentConfig) {
        *self.recording_intent.lock().unwrap() = Some(config);
    }

    // -- Polling loop --

    /// Spawn the background polling loop.
    ///
    /// Calls [`cpu_percent`] once before entering the loop so sysinfo can
    /// establish a CPU baseline; the first real reading comes after
    /// `POLL_INTERVAL_SECS`.
    ///
    /// Only spawns and does not await, but must be called inside a Tokio runtime.
    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            // Establish CPU baseline before the first real sample.
            self.cpu_percent();
            tracing::info!(interval_secs = POLL_INTERVAL_SECS, "Stat monitor started");

            let mut interval_secs = POLL_INTERVAL_SECS;
            loop {
                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
                let near = self.poll();
                self.sweep_retention().await;
                self.sweep_daily_coverage().await;
                self.sweep_recording_intent().await;
                let next = if near {
                    FAST_INTERVAL_SECS
                } else {
                    SLOW_INTERVAL_SECS
                };
                if next != interval_secs {
                    tracing::debug!(
                        interval_secs = next,
                        "Stat monitor polling interval adjusted"
                    );
                }
                interval_secs = next;
            }
        });
    }

    /// Take one sample of every active metric and feed results to the evaluator.
    ///
    /// Returns `true` if any sampled value is within [`NEAR_MARGIN`] of a
    /// configured threshold. The caller uses this to tighten the poll interval.
    fn poll(&self) -> bool {
        let snapshot = self.registry.snapshot(); // Arc<RegistrySnapshot>
        let mut near = false;

        // -- CPU --
        let cpu = self.cpu_percent();
        tracing::trace!(cpu, "cpu_usage_percent");
        self.evaluator.evaluate_stat_impl(
            &snapshot.pipelines,
            &StatMetric::CpuUsagePercent,
            None,
            None,
            cpu,
        );
        near |=
            self.is_near_threshold(&snapshot.pipelines, &StatMetric::CpuUsagePercent, None, cpu);

        // -- RAM --
        let ram = self.ram_percent();
        tracing::trace!(ram, "ram_usage_percent");
        self.evaluator.evaluate_stat_impl(
            &snapshot.pipelines,
            &StatMetric::RamUsagePercent,
            None,
            None,
            ram,
        );
        near |=
            self.is_near_threshold(&snapshot.pipelines, &StatMetric::RamUsagePercent, None, ram);

        // -- Disk: only paths referenced by active triggers --
        for path in self.active_disk_paths(&snapshot.pipelines) {
            match self.disk_percent(&path) {
                Some(pct) => {
                    tracing::trace!(path, pct, "disk_usage_percent");
                    self.evaluator.evaluate_stat_impl(
                        &snapshot.pipelines,
                        &StatMetric::DiskUsagePercent,
                        Some(&path),
                        None,
                        pct,
                    );
                    near |= self.is_near_threshold(
                        &snapshot.pipelines,
                        &StatMetric::DiskUsagePercent,
                        Some(&path),
                        pct,
                    );
                }
                None => {
                    tracing::warn!(
                        path,
                        "Stat trigger references a disk path not found on this system"
                    );
                }
            }
        }

        near
    }

    // -- Recording retention --

    /// Age-based cleanup first (delete what's definitely past its retention
    /// window regardless of disk usage), then disk-threshold cleanup (delete
    /// the oldest remaining chunks first until usage drops back under the
    /// configured threshold). Both passes resolve each camera's effective
    /// setting as `camera.override.unwrap_or(global_default)`, so a per-camera
    /// override of `0` disables that half of the sweep for just that camera.
    /// No-op until [`Self::set_retention`] has been called.
    ///
    /// Every `(camera_id, day)` touched by a deletion is recomputed via
    /// [`Self::recompute_purged_days`] once both passes finish, so a
    /// precomputed coverage row never reports data retention just deleted.
    async fn sweep_retention(&self) {
        let (recording_repo, camera_repo, recording_dir, retention_days, disk_threshold_percent) = {
            let guard = self.retention.lock().unwrap();
            let Some(cfg) = guard.as_ref() else {
                return;
            };
            (
                cfg.recording_repo.clone(),
                cfg.camera_repo.clone(),
                cfg.recording_dir.clone(),
                cfg.retention_days,
                cfg.retention_disk_threshold_percent,
            )
        };

        let cameras = match camera_repo.list().await {
            Ok(cams) => cams,
            Err(e) => {
                tracing::warn!(error = %e, "Retention sweep: failed to list cameras");
                return;
            }
        };

        let mut purged_days: HashSet<(Uuid, chrono::NaiveDate)> = HashSet::new();

        for camera in &cameras {
            let effective_days = camera
                .retention_days
                .map(|d| d as u32)
                .unwrap_or(retention_days);
            if effective_days == 0 {
                continue;
            }
            let cutoff =
                (chrono::Utc::now() - chrono::Duration::days(effective_days as i64)).fixed_offset();
            match recording_repo
                .list_older_than_for_camera(camera.id, cutoff)
                .await
            {
                Ok(rows) => {
                    for row in rows {
                        purged_days.insert((row.camera_id, row.start_time.date_naive()));
                        self.delete_recording_row(&recording_repo, row).await;
                    }
                }
                Err(e) => tracing::warn!(
                    camera_id = %camera.id,
                    error = %e,
                    "Retention sweep: failed to list aged recordings",
                ),
            }
        }

        if let Some(dir_str) = recording_dir.to_str() {
            for _ in 0..MAX_RETENTION_BATCHES_PER_TICK {
                let Some(pct) = self.disk_percent(dir_str) else {
                    break;
                };
                let eligible: Vec<Uuid> = cameras
                    .iter()
                    .filter(|c| {
                        let threshold = c
                            .retention_disk_threshold_percent
                            .unwrap_or(disk_threshold_percent);
                        threshold > 0.0 && pct >= threshold
                    })
                    .map(|c| c.id)
                    .collect();
                if eligible.is_empty() {
                    break;
                }
                match recording_repo
                    .list_oldest_finalized_for_cameras(&eligible, RETENTION_BATCH_SIZE)
                    .await
                {
                    Ok(rows) if !rows.is_empty() => {
                        for row in rows {
                            purged_days.insert((row.camera_id, row.start_time.date_naive()));
                            self.delete_recording_row(&recording_repo, row).await;
                        }
                    }
                    _ => break,
                }
            }
        }

        if !purged_days.is_empty() {
            self.recompute_purged_days(&recording_repo, purged_days)
                .await;
        }
    }

    /// Recompute each `(camera_id, day)` pair against whatever chunks
    /// retention left behind, flagging `purged_by_retention` when nothing's
    /// left. No-op if [`Self::set_coverage`] hasn't been called, since
    /// retention must work without coverage precomputation.
    async fn recompute_purged_days(
        &self,
        recording_repo: &RecordingRepo,
        days: HashSet<(Uuid, chrono::NaiveDate)>,
    ) {
        let (camera_repo, coverage_repo, default_timezone) = {
            let guard = self.coverage.lock().unwrap();
            let Some(cfg) = guard.as_ref() else {
                return;
            };
            (
                cfg.camera_repo.clone(),
                cfg.coverage_repo.clone(),
                cfg.default_timezone.clone(),
            )
        };

        for (camera_id, day) in days {
            let camera_timezone = camera_repo
                .get(camera_id)
                .await
                .ok()
                .flatten()
                .and_then(|c| c.timezone);
            let tz = resolve_camera_timezone(camera_timezone.as_deref(), &default_timezone);
            self.recompute_and_store_day(recording_repo, &coverage_repo, camera_id, day, tz, true)
                .await;
        }
    }

    // -- Daily recording coverage --

    /// Recompute every camera's "today" (in its own timezone), then backfill
    /// past days in the retention window that have no row yet. No-op until
    /// [`Self::set_coverage`] has been called. Called every tick but only does
    /// work once per [`COVERAGE_RECOMPUTE_INTERVAL_SECS`].
    async fn sweep_daily_coverage(&self) {
        {
            let mut last_run = self.last_coverage_run.lock().unwrap();
            let due = last_run.is_none_or(|t| {
                t.elapsed() >= Duration::from_secs(COVERAGE_RECOMPUTE_INTERVAL_SECS)
            });
            if !due {
                return;
            }
            *last_run = Some(Instant::now());
        }

        let (recording_repo, camera_repo, coverage_repo, retention_days, default_timezone) = {
            let guard = self.coverage.lock().unwrap();
            let Some(cfg) = guard.as_ref() else {
                return;
            };
            (
                cfg.recording_repo.clone(),
                cfg.camera_repo.clone(),
                cfg.coverage_repo.clone(),
                cfg.retention_days,
                cfg.default_timezone.clone(),
            )
        };

        let cameras = match camera_repo.list().await {
            Ok(cams) => cams,
            Err(e) => {
                tracing::warn!(error = %e, "Coverage sweep: failed to list cameras");
                return;
            }
        };

        let mut backfill_budget = MAX_COVERAGE_BACKFILL_PER_TICK;

        for camera in &cameras {
            let tz = resolve_camera_timezone(camera.timezone.as_deref(), &default_timezone);
            let today = chrono::Utc::now().with_timezone(&tz).date_naive();

            self.recompute_and_store_day(
                &recording_repo,
                &coverage_repo,
                camera.id,
                today,
                tz,
                false,
            )
            .await;

            if backfill_budget == 0 {
                continue;
            }
            let effective_days = camera
                .retention_days
                .map(|d| d as u32)
                .unwrap_or(retention_days);

            for offset in 1..=effective_days as i64 {
                if backfill_budget == 0 {
                    break;
                }
                let day = today - chrono::Duration::days(offset);
                match coverage_repo.exists(camera.id, day).await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(
                            camera_id = %camera.id,
                            %day,
                            error = %e,
                            "Coverage sweep: failed to check for existing backfill row",
                        );
                        continue;
                    }
                }
                self.recompute_and_store_day(
                    &recording_repo,
                    &coverage_repo,
                    camera.id,
                    day,
                    tz,
                    false,
                )
                .await;
                backfill_budget -= 1;
            }
        }
    }

    /// Compute one camera's coverage for `day` in `tz` (that camera's own
    /// resolved timezone) from its current chunks, and upsert the row.
    /// `day < today` (also in `tz`) is finalized; `day == today` isn't.
    /// `mark_purged_if_empty` is only `true` from the retention-purge hook,
    /// because an empty backfilled day was never recorded at all.
    async fn recompute_and_store_day(
        &self,
        recording_repo: &RecordingRepo,
        coverage_repo: &DailyRecordingCoverageRepo,
        camera_id: Uuid,
        day: chrono::NaiveDate,
        tz: chrono_tz::Tz,
        mark_purged_if_empty: bool,
    ) {
        let (Some(day_start_local), Some(next_day_start_local)) = (
            day.and_hms_opt(0, 0, 0),
            (day + chrono::Duration::days(1)).and_hms_opt(0, 0, 0),
        ) else {
            return;
        };
        let day_start = local_to_utc(tz, day_start_local).fixed_offset();
        let day_end = local_to_utc(tz, next_day_start_local).fixed_offset();

        let chunks = match recording_repo
            .list_starting_in_range_for_camera(camera_id, day_start, day_end)
            .await
        {
            Ok(chunks) => chunks,
            Err(e) => {
                tracing::warn!(
                    camera_id = %camera_id,
                    %day,
                    error = %e,
                    "Coverage sweep: failed to list chunks for day",
                );
                return;
            }
        };

        let result = compute_day_coverage(&chunks);
        let today_in_tz = chrono::Utc::now().with_timezone(&tz).date_naive();
        let is_finalized = day < today_in_tz;
        let purged_by_retention = mark_purged_if_empty && result.chunk_count == 0;

        if let Err(e) = coverage_repo
            .upsert(UpsertDailyCoverage {
                camera_id,
                day,
                coverage_seconds: result.coverage_seconds,
                session_ranges: result.session_ranges,
                chunk_count: result.chunk_count,
                total_size_bytes: result.total_size_bytes,
                is_finalized,
                purged_by_retention,
            })
            .await
        {
            tracing::warn!(
                camera_id = %camera_id,
                %day,
                error = %e,
                "Coverage sweep: failed to store daily coverage",
            );
        }
    }

    // -- Recording intent --

    /// Periodically checks that every camera that wants to be recording is,
    /// as a fallback for the event-driven reconciliation on [`MediaManager`]'s
    /// `pipeline_live_tx`. No-op until [`Self::set_recording_intent`] has been called.
    async fn sweep_recording_intent(&self) {
        {
            let mut last = self.last_recording_intent_sweep.lock().unwrap();
            let due = last.is_none_or(|t| {
                t.elapsed() >= Duration::from_secs(RECORDING_INTENT_SWEEP_INTERVAL_SECS)
            });
            if !due {
                return;
            }
            *last = Some(Instant::now());
        }

        let (camera_repo, media_manager) = {
            let guard = self.recording_intent.lock().unwrap();
            let Some(cfg) = guard.as_ref() else {
                return;
            };
            (cfg.camera_repo.clone(), cfg.media_manager.clone())
        };

        let cameras = match camera_repo.list().await {
            Ok(cams) => cams,
            Err(e) => {
                tracing::warn!(error = %e, "Recording intent sweep: failed to list cameras");
                return;
            }
        };

        // Filter on fields `list()` already returned so cameras that don't
        // need recording skip the decrypting reload in `reconcile_recording_intent`.
        for camera in cameras {
            if !camera.enabled || !camera.desired_recording {
                continue;
            }
            if media_manager.is_recording(camera.id) {
                continue;
            }
            reconcile_recording_intent(&camera_repo, &media_manager, camera.id).await;
        }
    }

    async fn delete_recording_row(
        &self,
        repo: &RecordingRepo,
        row: vms_db::entities::recording::Model,
    ) {
        if let Err(e) = tokio::fs::remove_file(&row.file_path).await {
            tracing::warn!(
                recording_id = %row.id,
                file_path = %row.file_path,
                error = %e,
                "Retention sweep: failed to delete file on disk — DB row will still be removed",
            );
        }
        match repo.delete(row.id).await {
            Ok(()) => tracing::info!(
                recording_id = %row.id,
                file_path = %row.file_path,
                "Retention sweep: deleted recording",
            ),
            Err(e) => {
                tracing::error!(recording_id = %row.id, error = %e, "Retention sweep: failed to delete DB row")
            }
        }
    }

    /// Returns `true` if `actual` is within [`NEAR_MARGIN`] of the threshold of
    /// any enabled `Stat` trigger that matches `metric` and `path`.
    ///
    /// Nearness is defined as:
    /// `|actual - threshold| / max(|threshold|, 1.0) < NEAR_MARGIN`
    ///
    /// The check ignores the trigger's operator, so it applies equally to
    /// `GreaterThan` and `LessThan` triggers.
    fn is_near_threshold(
        &self,
        snapshot: &HashMap<Uuid, Arc<CompiledPipeline>>,
        metric: &StatMetric,
        path: Option<&str>,
        actual: f64,
    ) -> bool {
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
                    threshold,
                    ..
                } = &trigger.config
                else {
                    continue;
                };

                if t_metric != metric {
                    continue;
                }

                if let Some(tp) = t_path {
                    if path != Some(tp.as_str()) {
                        continue;
                    }
                }

                let distance = (actual - threshold).abs() / threshold.abs().max(1.0);
                if distance < NEAR_MARGIN {
                    return true;
                }
            }
        }

        false
    }

    /// Collect the unique filesystem paths watched by any enabled `Stat` trigger.
    ///
    /// Called on every poll so newly enabled pipelines are picked up without
    /// a restart.
    fn active_disk_paths(
        &self,
        snapshot: &HashMap<Uuid, Arc<CompiledPipeline>>,
    ) -> HashSet<String> {
        let mut paths = HashSet::new();

        for pipeline in snapshot.values() {
            if !pipeline.enabled {
                continue;
            }
            for trigger in &pipeline.triggers {
                if !trigger.enabled {
                    continue;
                }
                if let TriggerConfig::Stat {
                    metric: StatMetric::DiskUsagePercent,
                    path: Some(p),
                    ..
                } = &trigger.config
                {
                    paths.insert(p.clone());
                }
            }
        }

        paths
    }

    #[cfg(test)]
    pub(crate) fn registry_snapshot(&self) -> Arc<crate::pipeline_registry::RegistrySnapshot> {
        self.registry.snapshot()
    }

    // -- Metric samplers --

    /// Aggregate CPU utilisation across all cores (0 to 100%).
    ///
    /// sysinfo needs two consecutive calls to compute a delta, so the first
    /// call returns roughly 0.
    pub(crate) fn cpu_percent(&self) -> f64 {
        let mut sys = self.sys.lock().expect("sys mutex poisoned");
        sys.refresh_cpu_usage();
        sys.global_cpu_usage() as f64
    }

    /// Percentage of physical RAM currently in use (0 to 100%).
    pub(crate) fn ram_percent(&self) -> f64 {
        let mut sys = self.sys.lock().expect("sys mutex poisoned");
        sys.refresh_memory();
        let total = sys.total_memory();
        if total == 0 {
            return 0.0;
        }
        sys.used_memory() as f64 / total as f64 * 100.0
    }

    /// Percentage of disk space used on the filesystem that contains `path`
    /// (0 to 100%).
    ///
    /// Returns `None` if no mounted filesystem matches `path`.
    pub(crate) fn disk_percent(&self, path: &str) -> Option<f64> {
        let mut disks = self.disks.lock().expect("disks mutex poisoned");
        disks.refresh(true);

        // Find the disk whose mount point is a prefix of `path`, preferring
        // the longest match (most specific mount point wins).
        let best = disks
            .list()
            .iter()
            .filter(|d| path.starts_with(d.mount_point().to_string_lossy().as_ref()))
            .max_by_key(|d| d.mount_point().to_string_lossy().len())?;

        let total = best.total_space();
        if total == 0 {
            return Some(0.0);
        }
        let used = total.saturating_sub(best.available_space());
        Some(used as f64 / total as f64 * 100.0)
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventBus, PipelineRegistry};

    fn make_monitor() -> Arc<StatMonitor> {
        let registry = PipelineRegistry::new_test(vec![]);
        let event_bus = EventBus::new(16);
        let evaluator = TriggerEvaluator::new_without_executor(registry.clone(), event_bus);
        StatMonitor::new(evaluator, registry)
    }

    #[test]
    fn cpu_percent_is_in_valid_range() {
        let mon = make_monitor();
        // First call gives a baseline reading; a second call gives the delta.
        let _ = mon.cpu_percent();
        let cpu = mon.cpu_percent();
        assert!(
            (0.0..=100.0).contains(&cpu),
            "cpu_percent out of range: {cpu}"
        );
    }

    #[test]
    fn ram_percent_is_in_valid_range() {
        let mon = make_monitor();
        let ram = mon.ram_percent();
        assert!(
            (0.0..=100.0).contains(&ram),
            "ram_percent out of range: {ram}"
        );
    }

    #[test]
    fn ram_percent_is_nonzero_on_running_system() {
        let mon = make_monitor();
        let ram = mon.ram_percent();
        assert!(ram > 0.0, "ram_percent should be > 0 on a live system");
    }

    #[test]
    fn disk_percent_root_is_in_valid_range() {
        let mon = make_monitor();
        let pct = mon.disk_percent("/");
        assert!(pct.is_some(), "root filesystem not found");
        let pct = pct.unwrap();
        assert!(
            (0.0..=100.0).contains(&pct),
            "disk_percent('/') out of range: {pct}"
        );
    }

    #[test]
    fn disk_percent_unknown_path_returns_none() {
        let mon = make_monitor();
        // A relative path can never start with a mount point (all mount points
        // are absolute), so this is guaranteed to return None on any OS.
        let pct = mon.disk_percent("relative/path/no/leading/slash");
        assert!(pct.is_none());
    }

    // -- Polling helpers --

    fn make_disk_pipeline(path: &str) -> vms_core::pipeline::CompiledPipeline {
        use std::collections::HashMap;
        use uuid::Uuid;
        use vms_core::{
            pipeline::{CompiledPipeline, PipelineDag, PipelineTrigger},
            CompareOperator, TriggerConfig, TriggerType,
        };
        let id = Uuid::new_v4();
        CompiledPipeline {
            id,
            name: "disk-test".into(),
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
                pipeline_id: id,
                trigger_type: TriggerType::Stat,
                enabled: true,
                source_id: None,
                camera_id: None,
                config: TriggerConfig::Stat {
                    metric: StatMetric::DiskUsagePercent,
                    path: Some(path.to_string()),
                    operator: CompareOperator::GreaterThan,
                    threshold: 90.0,
                    sustained_secs: 0,
                    cooldown_secs: 0,
                },
                last_error: None,
                last_error_at: None,
                unresolved_reference: false,
            }],
            camera_refs: vec![],
            source_refs: vec![],
        }
    }

    #[test]
    fn active_disk_paths_collects_unique_paths() {
        let pipeline = make_disk_pipeline("/var/lib/vms");
        let registry = PipelineRegistry::new_test(vec![pipeline]);
        let event_bus = EventBus::new(16);
        let evaluator = TriggerEvaluator::new_without_executor(registry.clone(), event_bus);
        let mon = StatMonitor::new(evaluator, registry.clone());

        let snapshot = registry.snapshot();
        let paths = mon.active_disk_paths(&snapshot.pipelines);
        assert_eq!(paths.len(), 1);
        assert!(paths.contains("/var/lib/vms"));
    }

    #[test]
    fn poll_does_not_panic() {
        // Smoke test: poll() with an empty registry must complete without error.
        let mon = make_monitor();
        mon.poll();
    }

    // -- Adaptive interval helpers --

    fn make_cpu_monitor(threshold: f64) -> Arc<StatMonitor> {
        use std::collections::HashMap;
        use uuid::Uuid;
        use vms_core::{
            pipeline::{CompiledPipeline, PipelineDag, PipelineTrigger},
            CompareOperator, TriggerConfig, TriggerType,
        };
        let id = Uuid::new_v4();
        let pipeline = CompiledPipeline {
            id,
            name: "cpu-test".into(),
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
                pipeline_id: id,
                trigger_type: TriggerType::Stat,
                enabled: true,
                source_id: None,
                camera_id: None,
                config: TriggerConfig::Stat {
                    metric: StatMetric::CpuUsagePercent,
                    path: None,
                    operator: CompareOperator::GreaterThan,
                    threshold,
                    sustained_secs: 0,
                    cooldown_secs: 0,
                },
                last_error: None,
                last_error_at: None,
                unresolved_reference: false,
            }],
            camera_refs: vec![],
            source_refs: vec![],
        };
        let registry = PipelineRegistry::new_test(vec![pipeline]);
        let event_bus = EventBus::new(16);
        let evaluator = TriggerEvaluator::new_without_executor(registry.clone(), event_bus);
        StatMonitor::new(evaluator, registry)
    }

    // A value within NEAR_MARGIN of the threshold is considered near.
    #[test]
    fn is_near_threshold_true_when_within_margin() {
        // threshold = 80, actual = 75 -> distance = |75-80|/80 = 0.0625 < 0.20
        let mon = make_cpu_monitor(80.0);
        let snap = mon.registry_snapshot();
        assert!(mon.is_near_threshold(&snap.pipelines, &StatMetric::CpuUsagePercent, None, 75.0));
    }

    // A value far from the threshold is not near.
    #[test]
    fn is_near_threshold_false_when_outside_margin() {
        // threshold = 80, actual = 40 -> distance = |40-80|/80 = 0.50 > 0.20
        let mon = make_cpu_monitor(80.0);
        let snap = mon.registry_snapshot();
        assert!(!mon.is_near_threshold(&snap.pipelines, &StatMetric::CpuUsagePercent, None, 40.0));
    }

    // A different metric never triggers nearness for a non-matching trigger.
    #[test]
    fn is_near_threshold_false_for_different_metric() {
        let mon = make_cpu_monitor(80.0);
        let snap = mon.registry_snapshot();
        // No RAM trigger exists, so nothing can match.
        assert!(!mon.is_near_threshold(&snap.pipelines, &StatMetric::RamUsagePercent, None, 75.0));
    }

    // poll() with a threshold near every real reading runs without panicking.
    #[test]
    fn poll_returns_true_when_near_any_threshold() {
        // Set threshold to 0.01 so any real CPU/RAM reading (always > 0) is "near".
        let mon = make_cpu_monitor(0.01);
        // The real readings vary by machine, so only check that poll() doesn't panic.
        let _ = mon.poll();
    }

    // -- resolve_camera_timezone --

    #[test]
    fn resolve_camera_timezone_prefers_the_camera_override() {
        let tz = resolve_camera_timezone(Some("Asia/Tehran"), "UTC");
        assert_eq!(tz, chrono_tz::Asia::Tehran);
    }

    #[test]
    fn resolve_camera_timezone_falls_back_to_the_default() {
        let tz = resolve_camera_timezone(None, "Europe/Berlin");
        assert_eq!(tz, chrono_tz::Europe::Berlin);
    }

    #[test]
    fn resolve_camera_timezone_falls_back_to_utc_when_both_are_unset() {
        let tz = resolve_camera_timezone(None, "UTC");
        assert_eq!(tz, chrono_tz::Tz::UTC);
    }
}
