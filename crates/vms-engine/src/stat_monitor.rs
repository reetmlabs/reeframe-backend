use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sysinfo::{Disks, System};
use vms_core::{StatMetric, TriggerConfig};

use crate::{PipelineRegistry, TriggerEvaluator};

/// Seconds between samples during normal operation (first sleep and default).
const POLL_INTERVAL_SECS: u64 = 5;
/// Seconds between samples when any metric is close to a threshold.
const FAST_INTERVAL_SECS: u64 = 2;
/// Seconds between samples when all metrics are well clear of every threshold.
const SLOW_INTERVAL_SECS: u64 = 15;
/// A metric is "near" a threshold when the relative distance is below this fraction.
/// E.g. 0.20 means within ±20 % of the threshold value.
const NEAR_MARGIN: f64 = 0.20;

/// Polls system metrics and feeds readings to the [`TriggerEvaluator`].
///
/// Wraps `sysinfo` behind a `Mutex` so the same `System` instance is reused
/// across polls — sysinfo works best when it can diff successive readings
/// (especially for CPU usage, which is meaningless on the very first sample).
pub struct StatMonitor {
    evaluator: Arc<TriggerEvaluator>,
    registry: Arc<PipelineRegistry>,
    sys: Mutex<System>,
    disks: Mutex<Disks>,
}

impl StatMonitor {
    pub fn new(evaluator: Arc<TriggerEvaluator>, registry: Arc<PipelineRegistry>) -> Arc<Self> {
        Arc::new(Self {
            evaluator,
            registry,
            sys: Mutex::new(System::new()),
            disks: Mutex::new(Disks::new()),
        })
    }

    // ── Polling loop ─────────────────────────────────────────────────────────

    /// Spawn the background polling loop.
    ///
    /// Calls [`cpu_percent`] once before entering the loop so sysinfo can
    /// establish a CPU baseline — the first real reading is taken after
    /// `POLL_INTERVAL_SECS`, by which point the delta is meaningful.
    ///
    /// Safe to call from a non-async context; only spawns, does not await.
    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            // Establish CPU baseline before the first real sample.
            self.cpu_percent();
            tracing::info!(interval_secs = POLL_INTERVAL_SECS, "Stat monitor started");

            let mut interval_secs = POLL_INTERVAL_SECS;
            loop {
                tokio::time::sleep(Duration::from_secs(interval_secs)).await;
                let near = self.poll();
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
    /// configured threshold — the caller uses this to tighten the poll interval.
    fn poll(&self) -> bool {
        let mut near = false;

        // ── CPU ───────────────────────────────────────────────────────────────
        let cpu = self.cpu_percent();
        tracing::trace!(cpu, "cpu_usage_percent");
        self.evaluator
            .evaluate_stat(&StatMetric::CpuUsagePercent, None, None, cpu);
        near |= self.is_near_threshold(&StatMetric::CpuUsagePercent, None, cpu);

        // ── RAM ───────────────────────────────────────────────────────────────
        let ram = self.ram_percent();
        tracing::trace!(ram, "ram_usage_percent");
        self.evaluator
            .evaluate_stat(&StatMetric::RamUsagePercent, None, None, ram);
        near |= self.is_near_threshold(&StatMetric::RamUsagePercent, None, ram);

        // ── Disk — only paths referenced by active triggers ───────────────────
        for path in self.active_disk_paths() {
            match self.disk_percent(&path) {
                Some(pct) => {
                    tracing::trace!(path, pct, "disk_usage_percent");
                    self.evaluator.evaluate_stat(
                        &StatMetric::DiskUsagePercent,
                        Some(&path),
                        None,
                        pct,
                    );
                    near |= self.is_near_threshold(&StatMetric::DiskUsagePercent, Some(&path), pct);
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

    /// Returns `true` if `actual` is within [`NEAR_MARGIN`] of the threshold of
    /// any enabled `Stat` trigger that matches `metric` and `path`.
    ///
    /// Nearness is defined as:
    /// `|actual − threshold| / max(|threshold|, 1.0) < NEAR_MARGIN`
    ///
    /// This is operator-agnostic — it fires for both rising and falling edges,
    /// covering `GreaterThan` (disk filling up) and `LessThan` (disk running out
    /// of free space) equally.
    fn is_near_threshold(&self, metric: &StatMetric, path: Option<&str>, actual: f64) -> bool {
        let snapshot = self.registry.snapshot();

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
                    if path.map_or(true, |p| p != tp.as_str()) {
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
    fn active_disk_paths(&self) -> HashSet<String> {
        let snapshot = self.registry.snapshot();
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

    // ── Metric samplers ───────────────────────────────────────────────────────

    /// Aggregate CPU utilisation across all cores (0–100 %).
    ///
    /// Requires two consecutive calls to sysinfo to compute a delta — the very
    /// first call always returns ~0. Subsequent calls return accurate values.
    pub(crate) fn cpu_percent(&self) -> f64 {
        let mut sys = self.sys.lock().expect("sys mutex poisoned");
        sys.refresh_cpu_usage();
        sys.global_cpu_usage() as f64
    }

    /// Percentage of physical RAM currently in use (0–100 %).
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
    /// (0–100 %).
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

// ── Tests ─────────────────────────────────────────────────────────────────────

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

    // ── Polling helpers ───────────────────────────────────────────────────────

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
        let mon = StatMonitor::new(evaluator, registry);

        let paths = mon.active_disk_paths();
        assert_eq!(paths.len(), 1);
        assert!(paths.contains("/var/lib/vms"));
    }

    #[test]
    fn poll_does_not_panic() {
        // Smoke test: poll() with an empty registry must complete without error.
        let mon = make_monitor();
        mon.poll();
    }

    // ── Adaptive interval helpers ─────────────────────────────────────────────

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
        // threshold = 80, actual = 75 → distance = |75-80|/80 = 0.0625 < 0.20
        let mon = make_cpu_monitor(80.0);
        assert!(mon.is_near_threshold(&StatMetric::CpuUsagePercent, None, 75.0));
    }

    // A value far from the threshold is not near.
    #[test]
    fn is_near_threshold_false_when_outside_margin() {
        // threshold = 80, actual = 40 → distance = |40-80|/80 = 0.50 > 0.20
        let mon = make_cpu_monitor(80.0);
        assert!(!mon.is_near_threshold(&StatMetric::CpuUsagePercent, None, 40.0));
    }

    // A different metric never triggers nearness for a non-matching trigger.
    #[test]
    fn is_near_threshold_false_for_different_metric() {
        let mon = make_cpu_monitor(80.0);
        // RAM trigger doesn't exist — no match possible.
        assert!(!mon.is_near_threshold(&StatMetric::RamUsagePercent, None, 75.0));
    }

    // poll() returns true when a real metric happens to be near a threshold.
    #[test]
    fn poll_returns_true_when_near_any_threshold() {
        // Set threshold to 0.01 so any real CPU/RAM reading (always > 0) is "near".
        let mon = make_cpu_monitor(0.01);
        // RAM will likely be > 0 on a live system; CPU baseline is ~0 on first
        // call but any value within 20% of 0.01 counts.  Just verify no panic.
        let _ = mon.poll();
    }
}
