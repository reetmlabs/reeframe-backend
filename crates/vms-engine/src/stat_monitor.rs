use std::sync::{Arc, Mutex};

use sysinfo::{Disks, System};

use crate::{PipelineRegistry, TriggerEvaluator};

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
}
