//! `vms-media` — GStreamer camera stream management and continuous recording.
//!
//! # Module overview
//!
//! | Module | Purpose |
//! |---|---|
//! | [`manager`] | [`MediaManager`] — start/stop per-camera streams, graceful shutdown |
//! | [`camera_stream`] | GStreamer pipeline builder and reconnect monitor task |
//!
//! # GStreamer element graph (per camera)
//!
//! ```text
//! rtspsrc ──(pad-added)──► [rtph264depay|rtph265depay] ──► [h264parse|h265parse]
//!                                                                      │
//!                                                                      ▼
//!                                                      tee ──► queue ──► splitmuxsink (MP4)
//! ```
//!
//! The codec is detected at runtime from the camera's SDP (`encoding-name`
//! field), so H.264 and H.265 cameras work without any configuration change.
//! The `tee` is the fan-out point for future branches (ring buffer, analytics,
//! live view) added in later steps.

pub mod manager;
pub(crate) mod camera_stream;

pub use manager::{MediaConfig, MediaManager};
