//! External source adapters: MQTT, webhook, Home Assistant WebSocket, API poller and file watcher.
//!
//! [`SourceManager`] owns the lazy-start / eager-stop lifecycle for every
//! adapter. The Resource Manager in `vms-engine` drives it the same way it
//! drives camera pipelines.

mod api_poll;
mod file_watcher;
mod ha_websocket;
mod manager;
mod mqtt;

pub use manager::SourceManager;
