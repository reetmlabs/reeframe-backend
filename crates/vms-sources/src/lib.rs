//! `vms-sources` — external source adapters (MQTT, webhook, HA WS, poller, file watcher).
//!
//! [`SourceManager`] owns the lazy-start / eager-stop lifecycle for every
//! adapter; the Resource Manager (`vms-engine`) drives it the same way it
//! drives camera pipelines.

mod file_watcher;
mod manager;

pub use manager::SourceManager;
