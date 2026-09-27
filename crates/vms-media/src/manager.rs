use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use gstreamer::prelude::*;

use tokio::sync::mpsc;
use uuid::Uuid;
use vms_core::event::Event;
use vms_core::{RecordingChunkEvent, VmsError};

use crate::camera_stream;
use crate::camera_stream::{build_camera_stream, spawn_monitor, ChunkNaming};
use crate::motion_branch::{self, MotionHandle};
use crate::relay::{RelayQuality, RelayServer};
use crate::ring_buffer::RingBuffer;
use crate::ring_buffer_branch;
use crate::sub_stream::{build_sub_stream, spawn_sub_monitor};
use crate::thumbnail_branch::{self, ThumbnailHandle};

// -- Config --

/// Configuration for the Media Manager.
pub struct MediaConfig {
    /// Directory where MP4 chunk files are written.
    pub recording_dir: PathBuf,
    /// Duration of each recording chunk in seconds (default: 300 = 5 minutes).
    pub chunk_duration_secs: u64,
    /// Address and port for the RTSP relay server (e.g. "0.0.0.0:8554").
    pub rtsp_bind: String,
    /// Minimum spacing between timeline thumbnail captures, in seconds
    /// (default: 10).
    pub thumbnail_interval_secs: u64,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            recording_dir: PathBuf::from("/var/lib/reeframe/recordings"),
            chunk_duration_secs: 300,
            rtsp_bind: "0.0.0.0:8554".into(),
            thumbnail_interval_secs: 10,
        }
    }
}

// -- Internal per-camera handle --

struct CameraHandle {
    /// Keeps the pipeline alive alongside the monitor task.
    #[allow(dead_code)]
    pipeline: gstreamer::Pipeline,
    /// The RTSP source URL this camera was started with.
    rtsp_url: String,
    /// Send `()` to ask the monitor task to shut down cleanly.
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Join handle for the bus-monitor / reconnect task.
    task: tokio::task::JoinHandle<()>,
    /// Shared with the monitor task — needed here too so
    /// `start_recording`/`stop_recording` can attach/detach the recording
    /// branch against the same `ChunkNaming` state the monitor refreshes on
    /// reconnect. See `camera_stream::ChunkNaming`.
    naming: Arc<ChunkNaming>,
}

/// A camera's optional sub-stream pipeline — `rtspsrc -> [depay|parse] -> tee`,
/// no recording branch. See `sub_stream.rs`.
struct SubStreamHandle {
    pipeline: gstreamer::Pipeline,
    /// The RTSP source URL this sub-stream was started with — needed if a
    /// sub-quality relay later needs to probe the codec.
    sub_rtsp_url: String,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

/// Name of the main pipeline's tee element for a camera.
fn main_tee_name(camera_id: Uuid) -> String {
    format!("cam_{}_tee", camera_id.as_simple())
}

/// Name of the sub-stream pipeline's tee element for a camera.
fn sub_tee_name(camera_id: Uuid) -> String {
    format!("cam_{}_subtee", camera_id.as_simple())
}

/// Motion runs when the camera's switch is on (unset counts as on) or a
/// pipeline requires its events.
fn motion_wanted(enabled: Option<bool>, required: bool) -> bool {
    enabled.unwrap_or(true) || required
}

/// Remove leftover `*.faststart.tmp` files from a previous process
/// lifetime. `remux_faststart` always writes to a fresh tmp path per
/// attempt and renames it into place on success, so any tmp file still
/// present at startup is a dead partial write — most commonly from a
/// remux that was still running when the process was hard-killed. Runs
/// once at `MediaManager::new()`, non-recursively (recordings are stored
/// flat in `recording_dir`); best-effort, since a leftover file is
/// harmless clutter, not a correctness problem.
fn cleanup_stale_faststart_tmp_files(recording_dir: &Path) {
    let entries = match std::fs::read_dir(recording_dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %e, dir = %recording_dir.display(), "Failed to scan recording dir for stale faststart tmp files");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.to_string_lossy().ends_with(".faststart.tmp") {
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    tracing::info!(path = %path.display(), "Removed stale faststart tmp file")
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "Failed to remove stale faststart tmp file")
                }
            }
        }
    }
}

// -- MediaManager --

/// Manages per-camera GStreamer pipelines.
///
/// Each started camera gets one **live** pipeline:
/// `rtspsrc -> rtph264depay -> h264parse -> tee`
///
/// Recording is not part of that pipeline by default — it is an
/// attach/detach branch (`queue -> splitmuxsink`) on the tee, started
/// explicitly via [`start_recording`](Self::start_recording) and stopped via
/// [`stop_recording`](Self::stop_recording), the same way the relay, ring
/// buffer, motion detection, and thumbnail capture all tap the same tee.
/// Connecting a relay (or otherwise going live) never implies recording.
///
/// A background tokio task monitors the GStreamer bus for errors and EOS events
/// and automatically reconnects with exponential backoff (2 s -> 60 s).
pub struct MediaManager {
    config: MediaConfig,
    cameras: Mutex<HashMap<Uuid, CameraHandle>>,
    /// Optional per-camera sub-stream pipeline — exists only while the
    /// camera has a configured sub-stream and something needs it (motion
    /// detection by default; a future sub-quality relay tap will share it).
    sub_streams: Mutex<HashMap<Uuid, SubStreamHandle>>,
    relay: Arc<RelayServer>,
    /// Sender used to publish `Event`s (motion, scene-change, tamper,
    /// signal-loss) produced by [`motion_branch`] onto the daemon's
    /// event-bus bridge.
    event_tx: mpsc::UnboundedSender<Event>,
    /// Sender used to publish `RecordingChunkEvent`s as `splitmuxsink`
    /// opens/closes each chunk, for whichever task actually has DB access
    /// to turn them into `recordings` rows.
    chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
    /// Fires the camera's ID every time its main pipeline reaches `Playing`
    /// — a fresh [`start_live`](Self::start_live) *and* every successful
    /// reconnect inside the monitor task. Lets whichever task actually has
    /// DB access reconcile persisted recording intent against reality the
    /// moment a pipeline comes back, not just at daemon boot — this crate
    /// has no `vms-db` dependency, so it can only announce the event, not
    /// act on it.
    pipeline_live_tx: mpsc::UnboundedSender<Uuid>,
    motion: Mutex<HashMap<Uuid, MotionHandle>>,
    /// Each camera's `motion_detection_enabled`. A camera missing here counts
    /// as enabled, matching the column default.
    motion_enabled: Mutex<HashMap<Uuid, bool>>,
    /// Cameras a pipeline needs motion events from, which keeps motion
    /// running even when the camera's own switch is off.
    motion_required: Mutex<HashSet<Uuid>>,
    thumbnails: Mutex<HashMap<Uuid, ThumbnailHandle>>,
}

impl MediaManager {
    /// Create a new `MediaManager` and initialise GStreamer.
    ///
    /// `gstreamer::init()` is idempotent — safe to call multiple times.
    ///
    /// `event_tx` is where motion/scene-change/tamper events get sent —
    /// the same channel `vms-sources` adapters publish onto, bridged to the
    /// `EventBus` in `main.rs`. `chunk_event_tx` is the equivalent channel
    /// for recording-chunk lifecycle bookkeeping. `pipeline_live_tx` is the
    /// equivalent for "a camera's pipeline just came up" — see the field
    /// doc comment. All three keep this crate free of a `vms-db`/
    /// `vms-engine` dependency.
    pub fn new(
        config: MediaConfig,
        event_tx: mpsc::UnboundedSender<Event>,
        chunk_event_tx: mpsc::UnboundedSender<RecordingChunkEvent>,
        pipeline_live_tx: mpsc::UnboundedSender<Uuid>,
    ) -> Result<Self, VmsError> {
        gstreamer::init().map_err(|e| VmsError::Media(format!("GStreamer init failed: {e}")))?;
        std::fs::create_dir_all(&config.recording_dir)?;
        cleanup_stale_faststart_tmp_files(&config.recording_dir);
        let relay = Arc::new(RelayServer::new(&config.rtsp_bind)?);
        Ok(Self {
            config,
            cameras: Mutex::new(HashMap::new()),
            sub_streams: Mutex::new(HashMap::new()),
            relay,
            event_tx,
            chunk_event_tx,
            pipeline_live_tx,
            motion: Mutex::new(HashMap::new()),
            motion_enabled: Mutex::new(HashMap::new()),
            motion_required: Mutex::new(HashSet::new()),
            thumbnails: Mutex::new(HashMap::new()),
        })
    }

    /// Start the camera's live pipeline (`rtspsrc -> tee`, no recording),
    /// its optional sub-stream pipeline, and motion/thumbnail detection —
    /// all tied to this one call, since they're only ever torn down together
    /// (see [`stop_live`](Self::stop_live)). No-op if the live pipeline is
    /// already running. Does **not** start recording — see
    /// [`start_recording`](Self::start_recording) for that.
    ///
    /// `sub_rtsp_url` is the camera's dedicated low-resolution sub-stream,
    /// if it has one (the caller resolves credentials into the URL, same as
    /// `rtsp_url`). A camera typically supports only two concurrent RTSP
    /// sessions, already spoken for by this daemon: one for the main stream
    /// (recording when attached, full-screen relay, ring buffer), one for
    /// the sub-stream (tile relay, analytics) — so motion detection never
    /// opens a connection of its own. It taps the sub-stream pipeline's tee
    /// by default (starting that pipeline here if it isn't already
    /// running), falling back to the main pipeline's tee when the camera
    /// has no sub-stream configured, or if starting the sub-stream pipeline
    /// fails. Both the sub-stream and motion detection are best-effort: a
    /// failure in either is logged but never fails the live pipeline that
    /// just started successfully.
    pub async fn start_live(
        &self,
        camera_id: Uuid,
        rtsp_url: &str,
        sub_rtsp_url: Option<&str>,
    ) -> Result<(), VmsError> {
        {
            let cameras = self.cameras.lock().unwrap();
            if cameras.contains_key(&camera_id) {
                return Ok(());
            }
        }

        let (pipeline, naming) = build_camera_stream(camera_id, rtsp_url)?;

        pipeline
            .set_state(gstreamer::State::Playing)
            .map_err(|e| VmsError::Media(format!("start pipeline {camera_id}: {e}")))?;
        let _ = self.pipeline_live_tx.send(camera_id);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = spawn_monitor(
            camera_id,
            pipeline.clone(),
            naming.clone(),
            self.config.recording_dir.clone(),
            self.config.chunk_duration_secs,
            self.chunk_event_tx.clone(),
            self.pipeline_live_tx.clone(),
            shutdown_rx,
        );

        self.cameras.lock().unwrap().insert(
            camera_id,
            CameraHandle {
                pipeline: pipeline.clone(),
                rtsp_url: rtsp_url.to_owned(),
                shutdown_tx,
                task,
                naming,
            },
        );

        tracing::info!(camera_id = %camera_id, rtsp_url, "Live pipeline started");

        // Prefer the sub-stream for motion detection and thumbnail capture
        // (cheaper decode, and neither needs main-resolution frames);
        // fall back to main.
        let (low_res_pipeline, low_res_tee) = match sub_rtsp_url {
            Some(sub_url) => match self.start_sub_stream(camera_id, sub_url) {
                Ok(sub_pipeline) => (sub_pipeline, sub_tee_name(camera_id)),
                Err(e) => {
                    tracing::warn!(
                        camera_id = %camera_id,
                        error = %e,
                        "Failed to start sub-stream — motion detection and thumbnails will use the main stream instead",
                    );
                    (pipeline, main_tee_name(camera_id))
                }
            },
            None => (pipeline, main_tee_name(camera_id)),
        };

        self.attach_analytics_branches(camera_id, &low_res_pipeline, &low_res_tee);

        Ok(())
    }

    /// Attach motion detection and thumbnail capture to `tee_name`'s tee on
    /// `pipeline` — both tied to "live" (start as soon as any pipeline for
    /// this camera exists, independent of recording). No-ops individually if
    /// already attached; failures are logged, never propagated, since
    /// neither is essential to having a working live pipeline.
    fn attach_analytics_branches(
        &self,
        camera_id: Uuid,
        pipeline: &gstreamer::Pipeline,
        tee_name: &str,
    ) {
        if self.motion_wanted(camera_id) {
            if let Err(e) = self.start_motion_detection(camera_id, pipeline, tee_name) {
                tracing::warn!(camera_id = %camera_id, error = %e, "Failed to start motion detection");
            }
        }
        if let Err(e) = self.start_thumbnail_capture(camera_id, pipeline, tee_name) {
            tracing::warn!(camera_id = %camera_id, error = %e, "Failed to start thumbnail capture");
        }
    }

    /// Start recording for a camera: ensures the live pipeline is running
    /// (starting it via [`start_live`](Self::start_live) if not), then
    /// attaches the recording branch (`queue -> splitmuxsink`) to its tee.
    /// No-op if already recording.
    pub async fn start_recording(
        &self,
        camera_id: Uuid,
        rtsp_url: &str,
        sub_rtsp_url: Option<&str>,
    ) -> Result<(), VmsError> {
        if !self.is_running(camera_id) {
            self.start_live(camera_id, rtsp_url, sub_rtsp_url).await?;
        }

        let (pipeline, naming) = {
            let cameras = self.cameras.lock().unwrap();
            let h = cameras
                .get(&camera_id)
                .ok_or_else(|| VmsError::Media(format!("camera {camera_id} is not running")))?;
            (h.pipeline.clone(), h.naming.clone())
        };

        // Give the video codec a chance to wire into `tee` first — attaching
        // the recording branch before that finishes lets `splitmuxsink` link
        // in ahead of it, which can permanently fail the video link. See
        // `camera_stream::wait_for_codec_wired`.
        camera_stream::wait_for_codec_wired(&pipeline, camera_id).await;

        camera_stream::attach_recording_branch(
            camera_id,
            &pipeline,
            &naming,
            &self.chunk_event_tx,
            &self.config.recording_dir,
            self.config.chunk_duration_secs,
        )?;

        tracing::info!(camera_id = %camera_id, "Recording started");
        Ok(())
    }

    /// Stop recording for a camera — detaches the recording branch from the
    /// tee, leaving the live pipeline (and relay/motion/thumbnails/ring
    /// buffer) running untouched. No-op if not recording, or if the camera
    /// is not even live.
    pub fn stop_recording(&self, camera_id: Uuid) -> Result<(), VmsError> {
        let Some((pipeline, naming)) = self
            .cameras
            .lock()
            .unwrap()
            .get(&camera_id)
            .map(|h| (h.pipeline.clone(), h.naming.clone()))
        else {
            return Ok(());
        };
        camera_stream::detach_recording_branch(camera_id, &pipeline, &naming)?;
        tracing::info!(camera_id = %camera_id, "Recording stopped");
        Ok(())
    }

    /// Return `true` if a recording branch is currently attached for `camera_id`.
    pub fn is_recording(&self, camera_id: Uuid) -> bool {
        self.cameras
            .lock()
            .unwrap()
            .get(&camera_id)
            .map(|h| camera_stream::is_recording_attached(camera_id, &h.pipeline))
            .unwrap_or(false)
    }

    /// Start an RTSP relay mount for a camera, bridged from whichever
    /// pipeline `quality` selects — the main pipeline for
    /// [`RelayQuality::Main`], the sub-stream pipeline for
    /// [`RelayQuality::Sub`]. Neither pipeline needs to already be running —
    /// live view never implies recording, so this starts whichever pipeline
    /// `quality` needs on demand (no-op if already up), same as
    /// [`start_live`](Self::start_live)/[`start_recording`](Self::start_recording)
    /// would. Relaying itself never opens a second connection to the camera
    /// beyond that one live pipeline.
    ///
    /// `rtsp_url`/`sub_rtsp_url` are only used if the relevant pipeline needs
    /// starting — ignored (may be empty/`None`) if it's already running.
    /// `RelayQuality::Sub` requires `sub_rtsp_url` to be `Some` the first
    /// time it's requested for a camera.
    ///
    /// When `cached_codec` is `Some`, the probe step is skipped (instant
    /// start). When `None`, a brief separate connection probes the relevant
    /// stream's codec — takes up to 10 s on a first start. Returns the
    /// codec in use (cached or freshly detected) so the caller can persist
    /// it to the DB for future daemon restarts. Main and sub streams are
    /// assumed to share one encoding, same as the camera's single cached
    /// `codec` DB column — real cameras use the same encoder for both.
    pub async fn start_relay(
        &self,
        camera_id: Uuid,
        quality: RelayQuality,
        rtsp_url: &str,
        sub_rtsp_url: Option<&str>,
        cached_codec: Option<&str>,
    ) -> Result<String, VmsError> {
        if self.relay.is_relaying(camera_id, quality) {
            return Ok(self.relay.codec(camera_id, quality).unwrap_or_default());
        }

        let (pipeline, tee_name, probe_url) = match quality {
            RelayQuality::Main => {
                if !self.is_running(camera_id) {
                    self.start_live(camera_id, rtsp_url, sub_rtsp_url).await?;
                }
                let cameras = self.cameras.lock().unwrap();
                let h = cameras
                    .get(&camera_id)
                    .expect("start_live just inserted this camera");
                (
                    h.pipeline.clone(),
                    main_tee_name(camera_id),
                    h.rtsp_url.clone(),
                )
            }
            RelayQuality::Sub => {
                if !self.sub_streams.lock().unwrap().contains_key(&camera_id) {
                    let sub_url = sub_rtsp_url.ok_or_else(|| {
                        VmsError::Media(format!(
                            "camera {camera_id} has no sub-stream configured — cannot start sub relay"
                        ))
                    })?;
                    let sub_pipeline = self.start_sub_stream(camera_id, sub_url)?;
                    // First live activity for this camera if the main
                    // pipeline isn't up either — attach motion/thumbnails
                    // here so they aren't skipped just because the viewer
                    // only ever asked for the sub-quality tile relay.
                    if !self.is_running(camera_id) {
                        self.attach_analytics_branches(
                            camera_id,
                            &sub_pipeline,
                            &sub_tee_name(camera_id),
                        );
                    }
                }
                let subs = self.sub_streams.lock().unwrap();
                let h = subs
                    .get(&camera_id)
                    .expect("start_sub_stream just inserted this camera");
                (
                    h.pipeline.clone(),
                    sub_tee_name(camera_id),
                    h.sub_rtsp_url.clone(),
                )
            }
        };

        let codec = match cached_codec {
            Some(c) => c.to_owned(),
            None => crate::relay::probe_codec(&probe_url).await?,
        };

        self.relay
            .start_relay(camera_id, quality, &pipeline, &tee_name, &codec)?;
        Ok(codec)
    }

    /// Stop an RTSP relay mount for a camera/quality. No-op if not relaying.
    pub fn stop_relay(&self, camera_id: Uuid, quality: RelayQuality) {
        let pipeline = match quality {
            RelayQuality::Main => self
                .cameras
                .lock()
                .unwrap()
                .get(&camera_id)
                .map(|h| h.pipeline.clone()),
            RelayQuality::Sub => self
                .sub_streams
                .lock()
                .unwrap()
                .get(&camera_id)
                .map(|h| h.pipeline.clone()),
        };
        let Some(pipeline) = pipeline else {
            return;
        };
        self.relay.stop_relay(camera_id, quality, &pipeline);
    }

    /// Stop the live pipeline for a camera — its recording branch if one is
    /// attached, its sub-stream pipeline if one is running, motion
    /// detection, thumbnail capture, and any relay mounts bridged from
    /// either pipeline.
    ///
    /// Order matters: motion detection and both relay qualities are
    /// detached first (while both pipelines are still `Playing`, so their
    /// blocking-pad-probe detaches can complete cleanly), then the
    /// sub-stream pipeline is torn down, then the main one — the recording
    /// branch dies with it, no separate detach needed since the whole
    /// pipeline is going to `Null` anyway.
    pub async fn stop_live(&self, camera_id: Uuid) -> Result<(), VmsError> {
        self.stop_motion_detection(camera_id).await;
        self.stop_thumbnail_capture(camera_id).await;
        self.stop_relay(camera_id, RelayQuality::Main);
        self.stop_relay(camera_id, RelayQuality::Sub);
        self.stop_sub_stream(camera_id).await;

        let handle = self.cameras.lock().unwrap().remove(&camera_id);
        if let Some(h) = handle {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
            tracing::info!(camera_id = %camera_id, "Live pipeline stopped");
        }

        Ok(())
    }

    /// Stop all live pipelines, all sub-stream pipelines, all
    /// motion-detection and thumbnail-capture branches, and all relay
    /// mounts, wait for monitor/analyzer tasks to exit.
    pub async fn shutdown(&self) -> Result<(), VmsError> {
        let motion_handles: Vec<MotionHandle> = {
            let mut motion = self.motion.lock().unwrap();
            motion.drain().map(|(_, h)| h).collect()
        };
        for h in motion_handles {
            h.stop().await;
        }

        let thumbnail_handles: Vec<ThumbnailHandle> = {
            let mut thumbnails = self.thumbnails.lock().unwrap();
            thumbnails.drain().map(|(_, h)| h).collect()
        };
        for h in thumbnail_handles {
            h.stop().await;
        }

        // Relay mounts must be torn down before the pipelines they're
        // bridged from.
        let camera_ids: Vec<Uuid> = self.cameras.lock().unwrap().keys().copied().collect();
        for id in camera_ids {
            self.stop_relay(id, RelayQuality::Main);
            self.stop_relay(id, RelayQuality::Sub);
        }

        let sub_handles: Vec<(Uuid, SubStreamHandle)> = {
            let mut subs = self.sub_streams.lock().unwrap();
            subs.drain().collect()
        };
        for (_, h) in sub_handles {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
        }

        let handles: Vec<(Uuid, CameraHandle)> = {
            let mut cameras = self.cameras.lock().unwrap();
            cameras.drain().collect()
        };
        for (_id, h) in handles {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
        }

        tracing::info!("MediaManager shutdown complete");
        Ok(())
    }

    /// Start the camera's sub-stream pipeline if not already running, and
    /// return a clone of its `Pipeline` (for the caller to attach a branch
    /// to). No-op — just returns the existing pipeline — if already running.
    fn start_sub_stream(
        &self,
        camera_id: Uuid,
        sub_rtsp_url: &str,
    ) -> Result<gstreamer::Pipeline, VmsError> {
        {
            let subs = self.sub_streams.lock().unwrap();
            if let Some(h) = subs.get(&camera_id) {
                return Ok(h.pipeline.clone());
            }
        }

        let pipeline = build_sub_stream(camera_id, sub_rtsp_url)?;
        pipeline
            .set_state(gstreamer::State::Playing)
            .map_err(|e| VmsError::Media(format!("start sub-stream {camera_id}: {e}")))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = spawn_sub_monitor(camera_id, pipeline.clone(), shutdown_rx);

        self.sub_streams.lock().unwrap().insert(
            camera_id,
            SubStreamHandle {
                pipeline: pipeline.clone(),
                sub_rtsp_url: sub_rtsp_url.to_owned(),
                shutdown_tx,
                task,
            },
        );

        tracing::info!(camera_id = %camera_id, sub_rtsp_url, "Sub-stream pipeline started");
        Ok(pipeline)
    }

    /// Stop the camera's sub-stream pipeline. No-op if none is running.
    async fn stop_sub_stream(&self, camera_id: Uuid) {
        let handle = self.sub_streams.lock().unwrap().remove(&camera_id);
        if let Some(h) = handle {
            let _ = h.shutdown_tx.send(());
            h.task.await.ok();
            tracing::info!(camera_id = %camera_id, "Sub-stream pipeline stopped");
        }
    }

    /// Apply a camera's `motion_detection_enabled` setting, attaching or
    /// detaching its motion branch right away if the camera is live.
    pub async fn set_motion_detection_enabled(&self, camera_id: Uuid, enabled: bool) {
        self.motion_enabled
            .lock()
            .unwrap()
            .insert(camera_id, enabled);
        self.reconcile_motion(camera_id).await;
    }

    /// Keep motion detection running for `camera_id` while a pipeline
    /// listens for its events, regardless of the camera's own setting.
    pub async fn require_motion(&self, camera_id: Uuid) {
        self.motion_required.lock().unwrap().insert(camera_id);
        self.reconcile_motion(camera_id).await;
    }

    /// Undo [`require_motion`](Self::require_motion).
    pub async fn release_motion(&self, camera_id: Uuid) {
        self.motion_required.lock().unwrap().remove(&camera_id);
        self.reconcile_motion(camera_id).await;
    }

    fn motion_wanted(&self, camera_id: Uuid) -> bool {
        motion_wanted(
            self.motion_enabled.lock().unwrap().get(&camera_id).copied(),
            self.motion_required.lock().unwrap().contains(&camera_id),
        )
    }

    /// Attach or detach the camera's motion branch to match
    /// [`motion_wanted`](Self::motion_wanted). Does nothing to a camera that
    /// isn't live; `start_live` checks the same rule when it comes up.
    async fn reconcile_motion(&self, camera_id: Uuid) {
        if !self.motion_wanted(camera_id) {
            self.stop_motion_detection(camera_id).await;
            return;
        }
        let Some((pipeline, tee_name)) = self.analytics_tee(camera_id) else {
            return;
        };
        if let Err(e) = self.start_motion_detection(camera_id, &pipeline, &tee_name) {
            tracing::warn!(camera_id = %camera_id, error = %e, "Failed to start motion detection");
        }
    }

    /// The tee analytics branches attach to: the sub-stream's when it's
    /// running, otherwise the main pipeline's.
    fn analytics_tee(&self, camera_id: Uuid) -> Option<(gstreamer::Pipeline, String)> {
        if let Some(h) = self.sub_streams.lock().unwrap().get(&camera_id) {
            return Some((h.pipeline.clone(), sub_tee_name(camera_id)));
        }
        self.cameras
            .lock()
            .unwrap()
            .get(&camera_id)
            .map(|h| (h.pipeline.clone(), main_tee_name(camera_id)))
    }

    /// Attach the motion/scene-change/tamper detection branch to
    /// `tee_name`'s tee on `pipeline`. No-op if one is already running for
    /// this camera.
    fn start_motion_detection(
        &self,
        camera_id: Uuid,
        pipeline: &gstreamer::Pipeline,
        tee_name: &str,
    ) -> Result<(), VmsError> {
        {
            let motion = self.motion.lock().unwrap();
            if motion.contains_key(&camera_id) {
                return Ok(());
            }
        }
        let handle = motion_branch::attach(pipeline, tee_name, camera_id, self.event_tx.clone())?;
        self.motion.lock().unwrap().insert(camera_id, handle);
        Ok(())
    }

    /// Stop the motion-detection branch for a camera. No-op if none is running.
    async fn stop_motion_detection(&self, camera_id: Uuid) {
        let handle = self.motion.lock().unwrap().remove(&camera_id);
        if let Some(h) = handle {
            h.stop().await;
            tracing::info!(camera_id = %camera_id, "Motion detection branch stopped");
        }
    }

    /// Attach the periodic thumbnail-capture branch to `tee_name`'s tee on
    /// `pipeline`. No-op if one is already running for this camera.
    fn start_thumbnail_capture(
        &self,
        camera_id: Uuid,
        pipeline: &gstreamer::Pipeline,
        tee_name: &str,
    ) -> Result<(), VmsError> {
        {
            let thumbnails = self.thumbnails.lock().unwrap();
            if thumbnails.contains_key(&camera_id) {
                return Ok(());
            }
        }
        let handle = thumbnail_branch::attach(
            pipeline,
            tee_name,
            camera_id,
            self.config.recording_dir.join("thumbnails"),
            std::time::Duration::from_secs(self.config.thumbnail_interval_secs),
        )?;
        self.thumbnails.lock().unwrap().insert(camera_id, handle);
        Ok(())
    }

    /// Stop the thumbnail-capture branch for a camera. No-op if none is running.
    async fn stop_thumbnail_capture(&self, camera_id: Uuid) {
        let handle = self.thumbnails.lock().unwrap().remove(&camera_id);
        if let Some(h) = handle {
            h.stop().await;
            tracing::info!(camera_id = %camera_id, "Thumbnail capture branch stopped");
        }
    }

    /// Return the relay URL for a camera/quality if that relay mount is active.
    pub fn relay_url(&self, camera_id: Uuid, quality: RelayQuality) -> Option<String> {
        self.relay.relay_url(camera_id, quality)
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
        let pipeline = {
            let cameras = self.cameras.lock().unwrap();
            cameras
                .get(&camera_id)
                .ok_or_else(|| {
                    VmsError::Media(format!(
                        "camera {camera_id} is not running — cannot attach ring buffer"
                    ))
                })?
                .pipeline
                .clone()
        }; // lock dropped here — GStreamer call runs without holding the mutex
        ring_buffer_branch::attach(&pipeline, camera_id, ring_buffer)
    }

    /// Detach the ring-buffer branch from a running camera pipeline.
    ///
    /// No-op if the camera is not running or has no ring-buffer branch.
    /// Cleanup is asynchronous — see [`ring_buffer_branch::detach`].
    pub fn detach_ring_buffer(&self, camera_id: Uuid) -> Result<(), VmsError> {
        let pipeline = {
            let cameras = self.cameras.lock().unwrap();
            match cameras.get(&camera_id) {
                Some(h) => h.pipeline.clone(),
                None => return Ok(()),
            }
        }; // lock dropped here
        ring_buffer_branch::detach(&pipeline, camera_id)
    }

    /// Return the codec currently in use for a camera/quality's relay, if running.
    pub fn relay_codec(&self, camera_id: Uuid, quality: RelayQuality) -> Option<String> {
        self.relay.codec(camera_id, quality)
    }

    /// Return `true` if a camera's live pipeline is currently running
    /// (independent of whether it's also recording — see
    /// [`is_recording`](Self::is_recording)).
    pub fn is_running(&self, camera_id: Uuid) -> bool {
        self.cameras.lock().unwrap().contains_key(&camera_id)
    }

    /// Return the list of currently running camera IDs.
    pub fn running_cameras(&self) -> Vec<Uuid> {
        self.cameras.lock().unwrap().keys().copied().collect()
    }

    /// Return a snapshot of RTSP URLs for all currently running cameras.
    pub fn rtsp_urls(&self) -> std::collections::HashMap<Uuid, String> {
        self.cameras
            .lock()
            .unwrap()
            .iter()
            .map(|(&id, h)| (id, h.rtsp_url.clone()))
            .collect()
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
        let mut b = gstreamer::ElementFactory::make(encoder_name).name(format!("cam_{id}_snapenc"));
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
        let caps = match src_pad.current_caps() {
            Some(c) => c,
            None => return,
        };
        let s = match caps.structure(0) {
            Some(s) => s,
            None => return,
        };
        if !s.name().starts_with("video/") {
            return;
        }
        let Some(convert) = convert_weak.upgrade() else {
            return;
        };
        let Some(sink_pad) = convert.static_pad("sink") else {
            return;
        };
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
                let sample = sink
                    .pull_sample()
                    .map_err(|_| gstreamer::FlowError::Error)?;
                let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                let map = buffer
                    .map_readable()
                    .map_err(|_| gstreamer::FlowError::Error)?;
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
    let elements: Vec<gstreamer::Element> =
        names.iter().filter_map(|n| pipeline.by_name(n)).collect();

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
            Ok(()) => tracing::info!(camera_id = %camera_id, "Snapshot branch detached"),
            Err(_) => tracing::warn!(
                camera_id = %camera_id,
                "snapshot detach probe timed out — releasing tee pad anyway",
            ),
        }
        tee_clone.release_request_pad(&tee_src_clone);
    });
}

#[cfg(test)]
mod tests {
    use super::cleanup_stale_faststart_tmp_files;
    use uuid::Uuid;

    /// A scratch dir under the system temp dir, unique per test run, cleaned
    /// up on drop.
    struct ScratchDir(std::path::PathBuf);

    impl ScratchDir {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("vms-media-test-{name}-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[test]
    fn removes_only_faststart_tmp_files() {
        let dir = ScratchDir::new("removes-only-faststart-tmp");
        let stale = dir.0.join("cam_abc_20260101_chunk00000.mp4.faststart.tmp");
        let chunk = dir.0.join("cam_abc_20260101_chunk00000.mp4");
        std::fs::write(&stale, b"").unwrap();
        std::fs::write(&chunk, b"").unwrap();

        cleanup_stale_faststart_tmp_files(&dir.0);

        assert!(!stale.exists());
        assert!(chunk.exists());
    }

    #[test]
    fn missing_dir_does_not_panic() {
        cleanup_stale_faststart_tmp_files(std::path::Path::new("/nonexistent/does/not/exist"));
    }
}
