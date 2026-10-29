//! `vms-media`: GStreamer camera stream management, live view, and recording.
//!
//! # Module overview
//!
//! | Module | Purpose |
//! |---|---|
//! | [`manager`] | [`MediaManager`]: start/stop per-camera live pipelines and recording, graceful shutdown |
//! | [`camera_stream`] | GStreamer pipeline builder, recording-branch attach/detach, reconnect monitor task |
//!
//! # GStreamer element graph (per camera)
//!
//! ```text
//! rtspsrc --(pad-added)---> [rtph264depay|rtph265depay] ---> [h264parse|h265parse] ---> tee
//! ```
//!
//! The codec is detected at runtime from the camera's SDP (`encoding-name`
//! field), so H.264 and H.265 cameras work without configuration.
//! The `tee` feeds every branch this crate attaches: recording, the RTSP
//! restream, ring buffer, motion detection and thumbnail capture. Recording is
//! one of those branches and is not part of the base pipeline.
//! [`MediaManager::hold_live`] keeps the pipeline up without writing to
//! disk, and [`MediaManager::start_recording`] attaches the recording branch
//! on top, starting the live pipeline first if needed. Live view never
//! implies recording, and stopping recording never tears down live view. The
//! pipeline closes once nothing needs it.

pub(crate) mod camera_stream;
pub mod export;
pub mod manager;
pub mod motion;
pub(crate) mod motion_branch;
pub mod onvif;
pub mod recording_path;
pub mod restream;
pub(crate) mod restream_bridge;
pub mod ring_buffer;
pub(crate) mod ring_buffer_branch;
pub(crate) mod sub_stream;
pub(crate) mod thumbnail_branch;

pub use export::{export_range, ExportChunk};
pub use manager::{MediaConfig, MediaManager};
pub use motion::{MotionAnalyzer, MotionSignal};
pub use onvif::{discover, resolve_streams, DiscoveredDevice, ResolvedStreams};
pub use recording_path::resolve_recording_file;
pub use restream::{RestreamServer, StreamQuality};
pub use ring_buffer::{RingBuffer, RingBufferManager, TimestampedFrame};
