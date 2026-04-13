//! Backend RTSP server for frontend consumption.
//!
//! [`BackendRtspServer`] wraps the GStreamer RTSP server (`gst-rtsp-server`) and exposes
//! two types of mount points per camera feed:
//!
//! * **Live** — `rtsp://<host>:8554/live/feed_{id}`
//!   A single shared pipeline connects to both camera streams and uses an
//!   `input-selector` element for instant, seamless quality switching without requiring
//!   the frontend to reconnect.
//!
//! * **Playback** — `rtsp://<host>:8554/playback/feed_{id}`
//!   Reads recorded MP4 chunks via `splitmuxsrc`.  Because `splitmuxsrc` supports
//!   GStreamer seek events, the RTSP server honours `PLAY Range: npt=…` requests,
//!   enabling the frontend's timeline slider.
//!
//! The GLib main loop required by the RTSP server runs in a dedicated OS thread so that
//! it does not interfere with Tokio's async executor.

use anyhow::{anyhow, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_rtsp_server as gst_rtsp;
use gstreamer_rtsp_server::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::info;

/// Manages the GStreamer RTSP server and all its mount points.
///
/// Clone is cheap — all state is behind `Arc<Mutex<…>>`.
#[derive(Clone)]
pub struct BackendRtspServer {
    /// The GLib-level RTSP server.  Kept alive here; dropping it would stop the server.
    #[allow(dead_code)]
    server: gst_rtsp::RTSPServer,
    /// The mount-points registry where factories are registered.
    mounts: gst_rtsp::RTSPMountPoints,
    /// Maps `feed_id` → the `input-selector` element inside the live media pipeline.
    /// Populated when the first client connects and `media-configure` fires.
    live_selectors: Arc<Mutex<HashMap<i32, gst::Element>>>,
}

impl BackendRtspServer {
    /// Create and start the RTSP server on the given TCP port.
    ///
    /// Spawns a dedicated OS thread that runs the GLib main loop.  This thread is
    /// required by the RTSP server and is separate from Tokio's executor.
    ///
    /// # Arguments
    /// * `port` — TCP port number (e.g. `8554`).
    ///
    /// # Errors
    /// Returns an error if GStreamer initialisation fails or the server cannot attach to
    /// the GLib main context.
    pub fn new(port: u16) -> Result<Self> {
        gst::init()?;

        let server = gst_rtsp::RTSPServer::new();
        server.set_service(&port.to_string());

        let mounts = server
            .mount_points()
            .ok_or_else(|| anyhow!("Failed to get RTSP server mount points"))?;

        // Attach the server to the default GLib main context and run the loop in its
        // own thread so we don't block Tokio.
        server.attach(None)?;

        std::thread::spawn(|| {
            let main_loop = glib::MainLoop::new(None, false);
            main_loop.run();
        });

        info!("RTSP server listening on port {}", port);

        Ok(Self {
            server,
            mounts,
            live_selectors: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Register a **live** mount point for the given feed.
    ///
    /// The factory pipeline connects to both camera streams via two `rtspsrc` elements
    /// and uses an `input-selector` to switch between them.  Quality switching is
    /// therefore seamless — the frontend does not need to reconnect.
    ///
    /// Pipeline layout:
    /// ```text
    /// rtspsrc location={low_url}  ! rtph264depay ! h264parse ! input-selector name=sel_{id}
    /// rtspsrc location={high_url} ! rtph264depay ! h264parse ! sel_{id}.
    /// sel_{id}. ! rtph264pay name=pay0 pt=96
    /// ```
    ///
    /// The `input-selector` element is captured via the `media-configure` signal (fires
    /// when the first client connects) and stored so
    /// [`switch_live_quality`](Self::switch_live_quality) can change `active-pad`.
    ///
    /// # Arguments
    /// * `feed_id`  — The feed's database primary key.
    /// * `low_url`  — RTSP URL of the low-resolution camera stream (default quality).
    /// * `high_url` — RTSP URL of the high-resolution camera stream.
    ///
    /// # Errors
    /// Returns an error if the factory cannot be registered.
    pub fn add_live_feed(
        &self,
        feed_id: i32,
        low_url: &str,
        high_url: &str,
    ) -> Result<()> {
        let launch_str = format!(
            "( rtspsrc location={low} latency=100 protocols=tcp name=low_{id} \
                   ! rtph264depay ! h264parse ! input-selector name=sel_{id} \
               rtspsrc location={high} latency=100 protocols=tcp name=high_{id} \
                   ! rtph264depay ! h264parse ! sel_{id}. \
               sel_{id}. ! rtph264pay name=pay0 pt=96 )",
            low = low_url,
            high = high_url,
            id = feed_id,
        );

        let factory = gst_rtsp::RTSPMediaFactory::new();
        factory.set_launch(&launch_str);
        // Shared: one pipeline instance is used for all connected clients.
        factory.set_shared(true);

        // `media-configure` fires once when the shared pipeline is first created (i.e.
        // when the first client connects to this mount point).  We use it to extract the
        // `input-selector` element and store it for quality switching.
        let selectors = Arc::clone(&self.live_selectors);
        factory.connect_media_configure(move |_factory, media| {
            // `RTSPMedia::element()` returns the root bin of the media pipeline.
            let root: gst::Element = media.element();

            // The root element can be upcast to a Bin (Pipelines are Bins).
            let bin: gst::Bin = match root.downcast() {
                Ok(b) => b,
                Err(_) => {
                    // Not a bin — unexpected, but handle gracefully.
                    return;
                }
            };

            if let Some(sel) = bin.by_name(&format!("sel_{}", feed_id)) {
                info!("feed {}: input-selector captured from RTSP media", feed_id);
                selectors.lock().unwrap().insert(feed_id, sel);
            }
        });

        let mount_path = format!("/live/feed_{}", feed_id);
        self.mounts.add_factory(&mount_path, factory);

        info!("Registered live RTSP mount: /live/feed_{}", feed_id);
        Ok(())
    }

    /// Register a **playback** mount point for the given feed.
    ///
    /// The factory pipeline reads all recorded MP4 chunks via `splitmuxsrc`, which
    /// supports GStreamer seek events.  The RTSP server will automatically translate
    /// `PLAY Range: npt=…` requests into seek events on this pipeline.
    ///
    /// # Arguments
    /// * `feed_id`      — The feed's database primary key.
    /// * `storage_path` — Directory that contains the recorded MP4 files.
    ///
    /// # Errors
    /// Returns an error if the factory cannot be registered (currently infallible).
    pub fn add_playback_feed(&self, feed_id: i32, storage_path: &str) -> Result<()> {
        // splitmuxsrc with a glob pattern reads all chunks in recording order.
        let launch_str = format!(
            "( splitmuxsrc location={storage}/{id}_*.mp4 ! h264parse ! \
               rtph264pay name=pay0 pt=96 )",
            storage = storage_path,
            id = feed_id,
        );

        let factory = gst_rtsp::RTSPMediaFactory::new();
        factory.set_launch(&launch_str);
        // Each client gets its own independent seekable pipeline instance.
        factory.set_shared(false);

        let mount_path = format!("/playback/feed_{}", feed_id);
        self.mounts.add_factory(&mount_path, factory);

        info!("Registered playback RTSP mount: /playback/feed_{}", feed_id);
        Ok(())
    }

    /// Switch the quality of the live stream between low-res and high-res.
    ///
    /// Sets the `active-pad` of the `input-selector` inside the live media pipeline.
    /// The change takes effect on the next buffer boundary without requiring the frontend
    /// to reconnect.
    ///
    /// This is a no-op if the `input-selector` is not yet available (i.e. no client has
    /// connected to the live stream yet, so the pipeline has not been created).
    ///
    /// # Arguments
    /// * `feed_id`   — The feed's database primary key.
    /// * `use_high`  — `true` to switch to the high-res stream; `false` for low-res.
    pub fn switch_live_quality(&self, feed_id: i32, use_high: bool) {
        let selectors = self.live_selectors.lock().unwrap();
        if let Some(selector) = selectors.get(&feed_id) {
            // The input-selector has sink_0 (low-res, added first) and sink_1 (high-res).
            let pad_name = if use_high { "sink_1" } else { "sink_0" };
            if let Some(pad) = selector.static_pad(pad_name) {
                selector.set_property("active-pad", &pad);
                info!(
                    "feed {}: live quality switched to {}",
                    feed_id,
                    if use_high { "high" } else { "low" }
                );
            }
        } else {
            info!(
                "feed {}: input-selector not yet available (no client connected yet)",
                feed_id
            );
        }
    }

    /// Remove both the live and playback mount points for the given feed.
    ///
    /// Any currently connected RTSP clients will be dropped.  Should be called when the
    /// frontend disconnects the camera or the feed is deleted from the database.
    ///
    /// # Arguments
    /// * `feed_id` — The feed's database primary key.
    pub fn remove_feed(&self, feed_id: i32) {
        self.mounts
            .remove_factory(&format!("/live/feed_{}", feed_id));
        self.mounts
            .remove_factory(&format!("/playback/feed_{}", feed_id));
        self.live_selectors.lock().unwrap().remove(&feed_id);
        info!("Removed RTSP mount points for feed {}", feed_id);
    }
}
