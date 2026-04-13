//! Per-camera GStreamer recording pipeline.
//!
//! [`CameraStream`] owns the high-resolution GStreamer pipeline for a single camera.
//! Its sole responsibility is recording: the pipeline connects to the camera's high-res
//! RTSP stream, maintains a pre-alarm rolling buffer in RAM, and writes MP4 chunks to
//! disk on demand via a `valve` element.
//!
//! The **live view** RTSP stream is served separately by [`super::rtsp_server::BackendRtspServer`],
//! which runs its own `rtspsrc` connections directly.  This clean separation keeps the
//! recording pipeline independent of the serving layer.
//!
//! # Pipeline layout
//!
//! ```text
//! rtspsrc → rtph264depay → h264parse
//!   → queue(leaky=upstream, max-size-time=pre_alarm_ns)   ← rolling pre-alarm buffer
//!   → valve(drop=true)                                    ← recording gate
//!   → splitmuxsink(location=..., max-size-time=chunk_ns)  ← chunked MP4 output
//! ```
//!
//! The `leaky=upstream` queue behaves as a circular buffer: once it fills to
//! `pre_alarm_ns` of footage, older frames are dropped silently.  When the valve opens,
//! the queued frames (up to `pre_alarm_ns` of footage before the trigger) are flushed to
//! the file first, providing the pre-alarm effect.

use anyhow::{anyhow, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use tokio::sync::mpsc;
use tracing::info;

use crate::entities::{feed, settings};
use super::recording::{RecordingController, SegmentEvent};

/// The quality of a camera stream.  Used when switching the live view quality.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamQuality {
    /// Low-resolution stream (served by default for live view).
    Low,
    /// High-resolution stream (served on demand for live view; always used for recording).
    High,
}

/// Manages the high-resolution recording pipeline for one connected camera feed.
///
/// Create via [`CameraStream::new`].  The pipeline starts immediately and continues
/// running until [`CameraStream::stop`] is called.
pub struct CameraStream {
    /// The GStreamer pipeline (rtspsrc → h264parse → pre-alarm queue → valve → splitmuxsink).
    pipeline: gst::Pipeline,
    /// Exposes recording control (valve toggling, timed event recordings, DB indexing).
    pub recording: RecordingController,
}

impl CameraStream {
    /// Build the high-res recording pipeline and start it.
    ///
    /// # Arguments
    /// * `feed`     — Camera feed configuration.  The high-res URL
    ///                (`feed.rtsp_url_high`) is used when available; otherwise the
    ///                low-res URL (`feed.rtsp_url`) is used as a fallback for
    ///                single-stream cameras.
    /// * `settings` — Global settings (latency, pre-alarm duration, chunk duration,
    ///                storage path).
    /// * `db_tx`    — Sender to the shared DB indexer task.  Fragment open/close events
    ///                are forwarded here so that `recording_segments` rows are kept in
    ///                sync with the files on disk.
    ///
    /// # Errors
    /// Returns an error if the GStreamer pipeline string cannot be parsed or the pipeline
    /// fails to transition to the `Playing` state.
    pub fn new(
        feed: &feed::Model,
        settings: &settings::Model,
        db_tx: mpsc::Sender<SegmentEvent>,
    ) -> Result<Self> {
        gst::init()?;

        // Use the high-res URL for recording if configured; fall back to low-res.
        let rec_url = feed.rtsp_url_high.as_deref().unwrap_or(feed.rtsp_url.as_str());

        let id = feed.id;
        let latency = settings.gst_latency_ms;
        let storage_path = &settings.storage_path;

        // Convert configured durations to nanoseconds for GStreamer properties.
        let pre_alarm_ns = settings.pre_event_cache_duration_secs as u64 * 1_000_000_000;
        let chunk_ns = settings.recording_chunk_duration_mins as u64 * 60 * 1_000_000_000;

        // Build the pipeline as a single parse-launch string.
        //
        // Key design choices:
        // * `rtph264depay ! h264parse` — passthrough: no decode or re-encode.
        // * `queue leaky=upstream max-size-time=…` — circular pre-alarm buffer.  When
        //   full, the oldest frames are silently dropped (upstream leaky), so the queue
        //   always holds the most recent `pre_alarm_ns` nanoseconds of video.
        // * `valve drop=true` — gate closed by default; opened by RecordingController.
        // * `splitmuxsink` — writes H.264 into sequentially numbered MP4 chunks.
        let pipeline_str = format!(
            "rtspsrc location={url} latency={lat} protocols=tcp name=rec_src_{id} \
             ! rtph264depay ! h264parse \
             ! queue max-size-buffers=0 max-size-time={pre_alarm} max-size-bytes=0 leaky=upstream \
             ! valve name=rec_valve_{id} drop=true \
             ! splitmuxsink name=mux_{id} location={storage}/{id}_%05d.mp4 max-size-time={chunk}",
            url = rec_url,
            lat = latency,
            id = id,
            pre_alarm = pre_alarm_ns,
            storage = storage_path,
            chunk = chunk_ns,
        );

        let pipeline = gst::parse::launch(&pipeline_str)?
            .dynamic_cast::<gst::Pipeline>()
            .map_err(|_| anyhow!("Recording pipeline is not a gst::Pipeline for feed {}", id))?;

        // Extract the valve and muxer so RecordingController can operate on them.
        let valve = pipeline
            .by_name(&format!("rec_valve_{}", id))
            .ok_or_else(|| anyhow!("rec_valve_{} not found in recording pipeline", id))?;

        let mux = pipeline
            .by_name(&format!("mux_{}", id))
            .ok_or_else(|| anyhow!("mux_{} not found in recording pipeline", id))?;

        // Wire up the recording controller (connects splitmuxsink signals for DB indexing).
        let recording = RecordingController::new(id, valve, mux, db_tx)?;

        // Start the pipeline — it will connect to the camera and begin buffering.
        pipeline.set_state(gst::State::Playing)?;
        info!("feed {}: recording pipeline started (rec_url={})", id, rec_url);

        Ok(Self { pipeline, recording })
    }

    /// Stop the GStreamer recording pipeline.
    ///
    /// Sends an EOS event so the muxer can finalize the current MP4 chunk before the
    /// pipeline transitions to `Null`.  Any in-progress `splitmuxsink` chunk will be
    /// closed cleanly.
    ///
    /// # Errors
    /// Returns an error if the state transition fails.
    pub fn stop(&self) -> Result<()> {
        // Send EOS so the muxer closes the current chunk gracefully.
        let _ = self.pipeline.send_event(gst::event::Eos::new());
        self.pipeline.set_state(gst::State::Null)?;
        Ok(())
    }
}
