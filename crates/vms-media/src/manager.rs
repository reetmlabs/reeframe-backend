use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use gstreamer::prelude::*;

use uuid::Uuid;
use vms_core::VmsError;

use crate::camera_stream::{build_camera_stream, spawn_monitor};
use crate::relay::RelayServer;
use crate::ring_buffer::RingBuffer;
use crate::ring_buffer_branch;

// -- Config --

/// Configuration for the Media Manager.
pub struct MediaConfig {
    /// Directory where MP4 chunk files are written.
    pub recording_dir: PathBuf,
    /// Duration of each recording chunk in seconds (default: 300 = 5 minutes).
    pub chunk_duration_secs: u64,
    /// Address and port for the RTSP relay server (e.g. "0.0.0.0:8554").
    pub rtsp_bind: String,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            recording_dir: PathBuf::from("/var/lib/reeframe/recordings"),
            chunk_duration_secs: 300,
            rtsp_bind: "0.0.0.0:8554".into(),
        }
    }
}

// -- Internal per-camera handle --

struct CameraHandle {
    /// Keeps the pipeline alive alongside the monitor task.
    #[allow(dead_code)]
    pipeline: gstreamer::Pipeline,
    /// Send `()` to ask the monitor task to shut down cleanly.
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Join handle for the bus-monitor / reconnect task.
    task: tokio::task::JoinHandle<()>,
}

// -- MediaManager --

/// Manages per-camera GStreamer pipelines.
///
/// Each started camera gets one pipeline:
/// `rtspsrc -> rtph264depay -> h264parse -> tee -> queue -> splitmuxsink`
///
/// A background tokio task monitors the GStreamer bus for errors and EOS events
/// and automatically reconnects with exponential backoff (2 s -> 60 s).
pub struct MediaManager {
    config: MediaConfig,
    cameras: Mutex<HashMap<Uuid, CameraHandle>>,
    relay: Arc<RelayServer>,
}

impl MediaManager {
    /// Create a new `MediaManager` and initialise GStreamer.
    ///
    /// `gstreamer::init()` is idempotent — safe to call multiple times.
    pub fn new(config: MediaConfig) -> Result<Self, VmsError> {
        gstreamer::init().map_err(|e| VmsError::Media(format!("GStreamer init failed: {e}")))?;
        std::fs::create_dir_all(&config.recording_dir)?;
        let relay = Arc::new(RelayServer::new(&config.rtsp_bind)?);
        Ok(Self {
            config,
            cameras: Mutex::new(HashMap::new()),
            relay,
        })
    }

    /// Start continuous recording for a camera. No-op if already running.
    pub async fn start_camera(&self, camera_id: Uuid, rtsp_url: &str) -> Result<(), VmsError> {
        {
            let cameras = self.cameras.lock().unwrap();
            if cameras.contains_key(&camera_id) {
                return Ok(());
            }
        }

        let pipeline = build_camera_stream(
            camera_id,
            rtsp_url,
            &self.config.recording_dir,
            self.config.chunk_duration_secs,
        )?;

        pipeline
            .set_state(gstreamer::State::Playing)
            .map_err(|e| VmsError::Media(format!("start pipeline {camera_id}: {e}")))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = spawn_monitor(
            camera_id,
            pipeline.clone(),
            self.config.recording_dir.clone(),
            shutdown_rx,
        );

        self.cameras.lock().unwrap().insert(
            camera_id,
            CameraHandle { pipeline, shutdown_tx, task },
        );

        tracing::info!(camera_id = %camera_id, rtsp_url, "Recording pipeline started");
        Ok(())
    }

    /// Start the RTSP relay for a camera.
    ///
    /// When `cached_codec` is `Some`, the probe step is skipped (instant start).
    /// When `None`, the camera is probed — takes up to 10 s on a first start.
    /// Returns the codec in use (cached or freshly detected) so the caller can
    /// persist it to the DB for future daemon restarts.
    pub async fn start_relay(
        &self,
        camera_id: Uuid,
        source_url: &str,
        cached_codec: Option<&str>,
    ) -> Result<String, VmsError> {
        if self.relay.is_relaying(camera_id) {
            return Ok(self.relay.codec(camera_id).unwrap_or_default());
        }
        let codec = match cached_codec {
            Some(c) => c.to_owned(),
            None => crate::relay::probe_codec(source_url).await?,
        };
        self.relay.start_relay(camera_id, source_url, &codec)?;
        Ok(codec)
    }

    /// Stop the RTSP relay for a camera. No-op if not relaying.
    pub fn stop_relay(&self, camera_id: Uuid) {
        self.relay.stop_relay(camera_id);
    }

    /// Stop the recording pipeline for a camera. Does not affect the relay.
    pub async fn stop_camera(&self, camera_id: Uuid) -> Result<(), VmsError> {
        let handle = self.cameras.lock().unwrap().remove(&camera_id);

        if let Some(h) = handle {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
            tracing::info!(camera_id = %camera_id, "Recording pipeline stopped");
        }

        Ok(())
    }

    /// Stop all recording pipelines and all relays, wait for monitor tasks to exit.
    pub async fn shutdown(&self) -> Result<(), VmsError> {
        let handles: Vec<(Uuid, CameraHandle)> = {
            let mut cameras = self.cameras.lock().unwrap();
            cameras.drain().collect()
        };

        for (id, h) in handles {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
            self.relay.stop_relay(id);
        }

        tracing::info!("MediaManager shutdown complete");
        Ok(())
    }

    /// Return the relay URL for a camera if recording (and relay) is active.
    pub fn relay_url(&self, camera_id: Uuid) -> Option<String> {
        self.relay.relay_url(camera_id)
    }

    /// Attach a ring-buffer appsink branch to a running camera pipeline.
    ///
    /// Returns `VmsError::Media` if the camera is not currently running or if
    /// GStreamer fails to link the new branch.
    pub fn attach_ring_buffer(
        &self,
        camera_id: Uuid,
        ring_buffer: Arc<Mutex<RingBuffer>>,
    ) -> Result<(), VmsError> {
        let cameras = self.cameras.lock().unwrap();
        let handle = cameras.get(&camera_id).ok_or_else(|| {
            VmsError::Media(format!(
                "camera {camera_id} is not running — cannot attach ring buffer"
            ))
        })?;
        ring_buffer_branch::attach(&handle.pipeline, camera_id, ring_buffer)
    }

    /// Detach the ring-buffer branch from a running camera pipeline.
    ///
    /// No-op if the camera is not running or has no ring-buffer branch.
    /// Cleanup is asynchronous — see [`ring_buffer_branch::detach`].
    pub fn detach_ring_buffer(&self, camera_id: Uuid) -> Result<(), VmsError> {
        let cameras = self.cameras.lock().unwrap();
        let Some(handle) = cameras.get(&camera_id) else {
            return Ok(());
        };
        ring_buffer_branch::detach(&handle.pipeline, camera_id)
    }

    /// Return `true` if a camera pipeline is currently running.
    pub fn is_running(&self, camera_id: Uuid) -> bool {
        self.cameras.lock().unwrap().contains_key(&camera_id)
    }

    /// Return the list of currently running camera IDs.
    pub fn running_cameras(&self) -> Vec<Uuid> {
        self.cameras.lock().unwrap().keys().copied().collect()
    }

    /// Capture a single still frame from a running camera and write it to `output_dir`.
    ///
    /// Taps the camera's live GStreamer tee — no second RTSP connection is opened.
    /// A temporary decode branch (`decodebin -> videoconvert -> jpegenc/pngenc -> appsink`)
    /// is attached to the tee, one decoded frame is captured, then the branch is
    /// detached. The wait is bounded by the camera's keyframe interval (typically
    /// under 2 seconds for surveillance cameras).
    ///
    /// Returns the path of the written file on success.
    pub async fn capture_snapshot(
        &self,
        camera_id: Uuid,
        format: &str,
        quality: u8,
        output_dir: &Path,
    ) -> Result<PathBuf, VmsError> {
        let pipeline = {
            let cameras = self.cameras.lock().unwrap();
            cameras
                .get(&camera_id)
                .ok_or_else(|| VmsError::Media(format!("camera {camera_id} is not running")))?
                .pipeline
                .clone()
        };

        std::fs::create_dir_all(output_dir)
            .map_err(|e| VmsError::Media(format!("create snapshot dir: {e}")))?;

        let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let ext = if format == "png" { "png" } else { "jpg" };
        let filename = format!("snap_{}_{}.{}", camera_id.as_simple(), ts, ext);
        let output_path = output_dir.join(&filename);

        let out = output_path.clone();
        let fmt = format.to_string();

        tokio::task::spawn_blocking(move || {
            snapshot_from_tee(&pipeline, camera_id, &fmt, quality, &out)
        })
        .await
        .map_err(|e| VmsError::Media(format!("snapshot task: {e}")))??;

        tracing::info!(
            camera_id = %camera_id,
            path = %output_path.display(),
            "Snapshot captured"
        );
        Ok(output_path)
    }
}

// -- Snapshot helper --

/// Attach a one-shot decode branch to the live camera tee, pull one frame, detach.
///
/// Branch: `tee -> queue -> decodebin -> videoconvert -> [jpegenc|pngenc] -> appsink`
///
/// `decodebin` handles codec detection automatically (H.264, H.265, MJPEG, AV1).
/// The `pad-added` callback links its decoded video src pad to `videoconvert`.
/// The appsink callback sends the encoded frame bytes over a sync channel; this
/// thread waits on the channel with a 5 s timeout (one GOP interval on most cameras).
fn snapshot_from_tee(
    pipeline: &gstreamer::Pipeline,
    camera_id: Uuid,
    format: &str,
    quality: u8,
    output: &Path,
) -> Result<(), VmsError> {
    let id = camera_id.as_simple().to_string();
    let tee_el_name = format!("cam_{id}_tee");

    let tee = pipeline
        .by_name(&tee_el_name)
        .ok_or_else(|| VmsError::Media(format!("tee not found for camera {camera_id}")))?;

    // -- Build branch elements --
    let queue = gstreamer::ElementFactory::make("queue")
        .name(format!("cam_{id}_snapqueue"))
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()
        .map_err(|e| VmsError::Media(format!("snapshot queue: {e}")))?;

    let decodebin = gstreamer::ElementFactory::make("decodebin")
        .name(format!("cam_{id}_snapdecode"))
        .build()
        .map_err(|e| VmsError::Media(format!("snapshot decodebin: {e}")))?;

    let convert = gstreamer::ElementFactory::make("videoconvert")
        .name(format!("cam_{id}_snapconvert"))
        .build()
        .map_err(|e| VmsError::Media(format!("snapshot videoconvert: {e}")))?;

    let encoder_name = if format == "png" { "pngenc" } else { "jpegenc" };
    let encoder = {
        let mut b = gstreamer::ElementFactory::make(encoder_name)
            .name(format!("cam_{id}_snapenc"));
        if format != "png" {
            b = b.property("quality", quality as i32);
        }
        b.build()
            .map_err(|e| VmsError::Media(format!("{encoder_name}: {e}")))?
    };

    let appsink = gstreamer_app::AppSink::builder()
        .name(format!("cam_{id}_snapsink"))
        .max_buffers(1u32)
        .drop(false)
        .sync(false)
        .build();

    // -- Add to pipeline --
    pipeline
        .add_many([
            &queue,
            &decodebin,
            &convert,
            &encoder,
            appsink.upcast_ref::<gstreamer::Element>(),
        ])
        .map_err(|e| VmsError::Media(format!("add snapshot branch: {e}")))?;

    // Link static chain: convert -> encoder -> appsink
    gstreamer::Element::link_many([
        &convert,
        &encoder,
        appsink.upcast_ref::<gstreamer::Element>(),
    ])
    .map_err(|e| VmsError::Media(format!("link snapshot chain: {e}")))?;

    // Link queue -> decodebin (encoded video in, decoded video out via pad-added)
    queue
        .link(&decodebin)
        .map_err(|e| VmsError::Media(format!("link queue->decodebin: {e}")))?;

    // Link tee -> queue
    let tee_src = tee
        .request_pad_simple("src_%u")
        .ok_or_else(|| VmsError::Media("tee: no src pad for snapshot".into()))?;
    let queue_sink = queue
        .static_pad("sink")
        .ok_or_else(|| VmsError::Media("snapshot queue has no sink pad".into()))?;
    tee_src
        .link(&queue_sink)
        .map_err(|e| VmsError::Media(format!("link tee->snapshot queue: {e}")))?;

    // Wire decodebin's dynamic video src pad to videoconvert
    let convert_weak = convert.downgrade();
    decodebin.connect_pad_added(move |_, src_pad| {
        let caps = match src_pad.current_caps() { Some(c) => c, None => return };
        let s = match caps.structure(0) { Some(s) => s, None => return };
        if !s.name().starts_with("video/") { return; }
        let Some(convert) = convert_weak.upgrade() else { return };
        let Some(sink_pad) = convert.static_pad("sink") else { return };
        if !sink_pad.is_linked() {
            src_pad.link(&sink_pad).ok();
        }
    });

    // Appsink callback: send the first encoded frame over a sync channel
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
    let tx = Arc::new(Mutex::new(Some(tx)));
    appsink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                let map = buffer.map_readable().map_err(|_| gstreamer::FlowError::Error)?;
                if let Some(sender) = tx.lock().ok().and_then(|mut g| g.take()) {
                    let _ = sender.send(map.to_vec());
                }
                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );

    // Bring branch elements up to the pipeline's current state
    for el in [
        &queue,
        &decodebin,
        &convert,
        &encoder,
        appsink.upcast_ref::<gstreamer::Element>(),
    ] {
        el.sync_state_with_parent()
            .map_err(|e| VmsError::Media(format!("sync snapshot element: {e}")))?;
    }

    // Wait for one decoded+encoded frame (bounded by camera's keyframe interval)
    let frame_data = rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .map_err(|_| {
            VmsError::Media(format!(
                "snapshot timeout for camera {camera_id} — no frame within 5 s"
            ))
        })?;

    std::fs::write(output, &frame_data)
        .map_err(|e| VmsError::Media(format!("write snapshot: {e}")))?;

    // Detach the branch asynchronously (same probe pattern as ring_buffer_branch::detach)
    detach_snapshot_branch(pipeline, camera_id, &tee, &tee_src, &queue_sink);

    Ok(())
}

/// Detach the snapshot branch from the tee using a blocking pad probe.
///
/// Follows the same pattern as `ring_buffer_branch::detach`: block the tee src pad,
/// unlink + remove elements inside the probe, then release the tee request pad from
/// a short-lived std thread once the probe has fired.
fn detach_snapshot_branch(
    pipeline: &gstreamer::Pipeline,
    camera_id: Uuid,
    tee: &gstreamer::Element,
    tee_src: &gstreamer::Pad,
    queue_sink: &gstreamer::Pad,
) {
    let id = camera_id.as_simple().to_string();
    let names = [
        format!("cam_{id}_snapqueue"),
        format!("cam_{id}_snapdecode"),
        format!("cam_{id}_snapconvert"),
        format!("cam_{id}_snapenc"),
        format!("cam_{id}_snapsink"),
    ];
    let elements: Vec<gstreamer::Element> = names
        .iter()
        .filter_map(|n| pipeline.by_name(n))
        .collect();

    let (done_tx, done_rx) = std::sync::mpsc::sync_channel::<()>(1);
    let pipeline_clone = pipeline.clone();
    let tee_clone = tee.clone();
    let tee_src_clone = tee_src.clone();
    let queue_sink_clone = queue_sink.clone();

    tee_src.add_probe(gstreamer::PadProbeType::BLOCK_DOWNSTREAM, move |pad, _| {
        pad.unlink(&queue_sink_clone).ok();
        for el in &elements {
            el.set_state(gstreamer::State::Null).ok();
            pipeline_clone.remove(el).ok();
        }
        let _ = done_tx.send(());
        gstreamer::PadProbeReturn::Remove
    });

    std::thread::spawn(move || {
        match done_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(()) => tee_clone.release_request_pad(&tee_src_clone),
            Err(_) => tracing::warn!(
                camera_id = %camera_id,
                "snapshot branch detach timed out"
            ),
        }
    });
}
