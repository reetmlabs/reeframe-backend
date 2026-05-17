use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sysinfo::{Disks, System};
use vms_core::{StatMetric, TriggerConfig};

use crate::{PipelineRegistry, TriggerEvaluator};

/// Seconds between metric samples during normal operation.
const POLL_INTERVAL_SECS: u64 = 5;

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

            loop {
                tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
                self.poll();
            }
        });
    }

    /// Take one sample of every active metric and feed results to the evaluator.
    fn poll(&self) {
        // ── CPU ───────────────────────────────────────────────────────────────
        let cpu = self.cpu_percent();
        tracing::trace!(cpu, "cpu_usage_percent");
        self.evaluator
            .evaluate_stat(&StatMetric::CpuUsagePercent, None, None, cpu);

        // ── RAM ───────────────────────────────────────────────────────────────
        let ram = self.ram_percent();
        tracing::trace!(ram, "ram_usage_percent");
        self.evaluator
            .evaluate_stat(&StatMetric::RamUsagePercent, None, None, ram);

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
                }
                None => {
                    tracing::warn!(
                        path,
                        "Stat trigger references a disk path not found on this system"
                    );
                }
            }
        }
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
        let evaluator = TriggerEvaluator::new(registry.clone(), event_bus);
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
        let evaluator = TriggerEvaluator::new(registry.clone(), event_bus);
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
}
