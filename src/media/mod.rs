//! Camera management subsystem.
//!
//! This module is the public API surface for the recording and streaming subsystem.
//! [`CameraManager`] is the single object injected into Salvo's application state; HTTP
//! handlers call its methods to connect cameras, switch quality, start/stop recording, and
//! trigger event-based recordings.
//!
//! # Sub-modules
//! * [`camera_stream`] — High-res GStreamer recording pipeline per camera with pre-alarm
//!                        rolling buffer.
//! * [`recording`]     — Valve control, timed event recording, and DB segment indexing
//!                        via a background task.
//! * [`rtsp_server`]   — Backend GStreamer RTSP server (live + playback, port 8554 by
//!                        default).
//!
//! # Separation of concerns
//! The **live RTSP view** is served by [`rtsp_server::BackendRtspServer`], which runs its
//! own `rtspsrc` connections directly and uses an `input-selector` for quality switching.
//! The **recording pipeline** in [`camera_stream::CameraStream`] has a separate `rtspsrc`
//! connection to the camera's high-res stream and is purely concerned with writing MP4
//! chunks to disk.
//!
//! # Thread safety
//! All state is wrapped in `Arc<Mutex<…>>` so that `CameraManager` can be cloned cheaply
//! and used from multiple Salvo handlers concurrently.

pub mod camera_stream;
pub mod recording;
pub mod rtsp_server;

pub use camera_stream::StreamQuality;
pub use recording::TriggerType;

use anyhow::{anyhow, Result};
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

use crate::entities::{feed, settings};
use camera_stream::CameraStream;
use recording::{recording_segment_indexer, SegmentEvent};
use rtsp_server::BackendRtspServer;

/// Manages all connected camera feeds, the backend RTSP server, and the DB indexer.
///
/// Clone is cheap: all mutable state is behind `Arc<Mutex<…>>`.
#[derive(Clone)]
pub struct CameraManager {
    /// Live recording camera streams, keyed by feed ID.
    streams: Arc<Mutex<HashMap<i32, CameraStream>>>,
    /// The shared GStreamer RTSP server instance (live view + playback).
    rtsp_server: BackendRtspServer,
    /// Sender side of the DB indexer channel.  Cloned into each
    /// [`recording::RecordingController`] so all feeds share one indexer task.
    db_tx: mpsc::Sender<SegmentEvent>,
}

impl CameraManager {
    /// Create a new `CameraManager`, start the RTSP server, and launch the DB indexer.
    ///
    /// The RTSP server binds to `rtsp_port` (typically `8554`).  The DB indexer is a
    /// Tokio task that receives [`recording::SegmentEvent`]s from recording pipelines and
    /// inserts rows into the `recording_segments` table.
    ///
    /// # Arguments
    /// * `rtsp_port` — TCP port for the backend RTSP server (e.g. `8554`).
    /// * `db`        — SeaORM database connection passed to the indexer task.
    ///
    /// # Errors
    /// Returns an error if the RTSP server fails to start (e.g. port already in use or
    /// GStreamer initialisation failure).
    pub fn new(rtsp_port: u16, db: DatabaseConnection) -> Result<Self> {
        let rtsp_server = BackendRtspServer::new(rtsp_port)?;

        // Channel buffer: 256 events handles bursts from many simultaneous cameras.
        let (db_tx, db_rx) = mpsc::channel::<SegmentEvent>(256);

        // Spawn the background DB indexer task.
        tokio::spawn(recording_segment_indexer(db, db_rx));

        info!("CameraManager initialised (RTSP port {})", rtsp_port);

        Ok(Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            rtsp_server,
            db_tx,
        })
    }

    /// Connect to a camera feed: start the recording pipeline and register RTSP mount points.
    ///
    /// After a successful connect:
    /// * Live stream is available at `rtsp://<host>:<rtsp_port>/live/feed_{id}` (low-res
    ///   by default; switch quality with [`switch_quality`](Self::switch_quality)).
    /// * Playback of recordings is available at
    ///   `rtsp://<host>:<rtsp_port>/playback/feed_{id}`.
    ///
    /// If the feed is already connected this is a no-op and returns `Ok(())`.
    ///
    /// # Arguments
    /// * `feed`     — Camera feed model from the database.
    /// * `settings` — Global settings model from the database.
    ///
    /// # Errors
    /// Returns an error if either the recording pipeline or the RTSP mount points cannot
    /// be created.
    pub fn connect(&self, feed: &feed::Model, settings: &settings::Model) -> Result<()> {
        let mut streams = self.streams.lock().unwrap();
        if streams.contains_key(&feed.id) {
            info!("feed {}: already connected, skipping", feed.id);
            return Ok(());
        }

        // Register live RTSP mount (uses its own rtspsrc connections with input-selector).
        let high_url = feed.rtsp_url_high.as_deref().unwrap_or(feed.rtsp_url.as_str());
        self.rtsp_server
            .add_live_feed(feed.id, &feed.rtsp_url, high_url)?;

        // Register playback RTSP mount (splitmuxsrc reads recorded MP4 chunks).
        self.rtsp_server
            .add_playback_feed(feed.id, &settings.storage_path)?;

        // Build the high-res recording pipeline.
        let stream = CameraStream::new(feed, settings, self.db_tx.clone())?;
        streams.insert(feed.id, stream);

        info!("feed {}: connected", feed.id);
        Ok(())
    }

    /// Disconnect a camera feed: stop the recording pipeline and remove RTSP mount points.
    ///
    /// Any in-progress recording chunk is finalised gracefully (EOS sent to splitmuxsink).
    /// Any RTSP clients currently viewing the live or playback stream will be disconnected.
    ///
    /// # Arguments
    /// * `feed_id` — Database ID of the feed to disconnect.
    ///
    /// # Errors
    /// Returns an error if the feed is not connected or the pipeline fails to stop.
    pub fn disconnect(&self, feed_id: i32) -> Result<()> {
        let mut streams = self.streams.lock().unwrap();
        if let Some(stream) = streams.remove(&feed_id) {
            stream.stop()?;
            self.rtsp_server.remove_feed(feed_id);
            info!("feed {}: disconnected", feed_id);
            Ok(())
        } else {
            Err(anyhow!("feed {}: not connected", feed_id))
        }
    }

    /// Switch the quality of the live RTSP stream served to the frontend.
    ///
    /// Adjusts the `active-pad` of the `input-selector` inside the live media pipeline.
    /// The switch is seamless — the frontend does not need to reconnect.  If no client
    /// has yet connected to the live stream the switch is noted and takes effect when
    /// the selector becomes available.
    ///
    /// # Arguments
    /// * `feed_id` — Database ID of the feed.
    /// * `quality` — Target quality (`Low` or `High`).
    ///
    /// # Errors
    /// Returns an error if the feed is not connected.
    pub fn switch_quality(&self, feed_id: i32, quality: StreamQuality) -> Result<()> {
        // Verify the feed is connected (streams map is the source of truth).
        if !self.streams.lock().unwrap().contains_key(&feed_id) {
            return Err(anyhow!("feed {}: not connected", feed_id));
        }
        self.rtsp_server
            .switch_live_quality(feed_id, quality == StreamQuality::High);
        Ok(())
    }

    /// Start a user-commanded (manual) recording on the feed's high-res stream.
    ///
    /// The recording continues until [`stop_recording`](Self::stop_recording) is called.
    /// If an event recording (AI / hardware / schedule) is already running, both continue
    /// in parallel — the valve remains open until all active recordings end.
    ///
    /// If the feed is not yet connected, the backend connects it automatically.
    ///
    /// # Arguments
    /// * `feed`     — Camera feed model (used to connect if necessary).
    /// * `settings` — Global settings model (used to create the pipeline if necessary).
    ///
    /// # Errors
    /// Returns an error if the feed cannot be connected.
    pub fn start_recording(
        &self,
        feed: &feed::Model,
        settings: &settings::Model,
    ) -> Result<()> {
        self.ensure_connected(feed, settings)?;
        let streams = self.streams.lock().unwrap();
        streams
            .get(&feed.id)
            .ok_or_else(|| anyhow!("feed {}: not connected", feed.id))?
            .recording
            .start_user_recording();
        Ok(())
    }

    /// Stop the user-commanded recording.
    ///
    /// Closes the recording valve only if no event recording (AI / hardware / schedule)
    /// is also currently active.
    ///
    /// # Arguments
    /// * `feed_id` — Database ID of the feed.
    ///
    /// # Errors
    /// Returns an error if the feed is not connected.
    pub fn stop_recording(&self, feed_id: i32) -> Result<()> {
        let streams = self.streams.lock().unwrap();
        streams
            .get(&feed_id)
            .ok_or_else(|| anyhow!("feed {}: not connected", feed_id))?
            .recording
            .stop_user_recording();
        Ok(())
    }

    /// Trigger a timed event recording on the feed's high-res stream.
    ///
    /// If an event recording is already active, its deadline is extended when `duration`
    /// would push the end time further into the future.  This avoids restarting the
    /// pipeline (which would lose the pre-alarm buffer) when rapid successive events fire.
    ///
    /// If the feed is not yet connected, the backend connects it automatically.
    ///
    /// # Arguments
    /// * `feed`     — Camera feed model.
    /// * `settings` — Global settings model.
    /// * `trigger`  — What caused this recording (`Ai`, `Hardware`, or `Schedule`).
    /// * `duration` — How long to record after the trigger fires.
    ///
    /// # Errors
    /// Returns an error if the feed cannot be connected.
    pub fn trigger_event(
        &self,
        feed: &feed::Model,
        settings: &settings::Model,
        trigger: TriggerType,
        duration: Duration,
    ) -> Result<()> {
        self.ensure_connected(feed, settings)?;
        let streams = self.streams.lock().unwrap();
        streams
            .get(&feed.id)
            .ok_or_else(|| anyhow!("feed {}: not connected", feed.id))?
            .recording
            .trigger_event_recording(trigger, duration);
        Ok(())
    }

    /// Connect the feed if it is not already connected.
    ///
    /// Used internally by [`start_recording`](Self::start_recording) and
    /// [`trigger_event`](Self::trigger_event) to avoid requiring a separate `/connect`
    /// call before recording.
    fn ensure_connected(&self, feed: &feed::Model, settings: &settings::Model) -> Result<()> {
        if !self.streams.lock().unwrap().contains_key(&feed.id) {
            self.connect(feed, settings)?;
        }
        Ok(())
    }
}
