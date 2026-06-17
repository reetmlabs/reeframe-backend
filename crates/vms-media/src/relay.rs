use std::{collections::HashMap, sync::Mutex};

use gstreamer::prelude::*;
use gstreamer_rtsp_server::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

// -- RelayServer --

/// Per-camera RTSP relay server powered by `gstreamer-rtsp-server`.
///
/// Each camera is served at `rtsp://{bind_host}:{bind_port}/{camera_id}`.
/// The relay opens its own RTSP connection to the source camera (sub_rtsp_url
/// when available, otherwise rtsp_url) so the recording pipeline is unaffected.
///
/// A dedicated GLib main loop runs on a background thread to drive I/O for the
/// RTSP server without interfering with GStreamer's own main context.
pub struct RelayServer {
    mounts: gstreamer_rtsp_server::RTSPMountPoints,
    bind: String,
    relays: Mutex<HashMap<Uuid, ()>>,
    _loop_thread: std::thread::JoinHandle<()>,
}

impl RelayServer {
    pub fn new(bind: &str) -> Result<Self, VmsError> {
        let main_context = glib::MainContext::new();
        let main_loop = glib::MainLoop::new(Some(&main_context), false);

        let server = gstreamer_rtsp_server::RTSPServer::new();

        let (address, service) = split_bind(bind)
            .ok_or_else(|| VmsError::Media(format!("invalid RTSP bind address: {bind}")))?;
        server.set_address(&address);
        server.set_service(&service);

        let mounts = server
            .mount_points()
            .ok_or_else(|| VmsError::Media("RTSP server returned no mount points".into()))?;

        server
            .attach(Some(&main_context))
            .map_err(|e| VmsError::Media(format!("RTSP server attach failed: {e}")))?;

        let loop_thread = std::thread::spawn(move || main_loop.run());

        tracing::info!(bind, "RTSP relay server listening");

        Ok(Self {
            mounts,
            bind: bind.to_owned(),
            relays: Mutex::new(HashMap::new()),
            _loop_thread: loop_thread,
        })
    }

    /// Register an RTSP relay for a camera.
    ///
    /// `source_url` is the RTSP URL the relay will connect to (sub_rtsp_url if
    /// available, otherwise rtsp_url). `codec` is the RTP encoding name as
    /// reported by the camera's SDP (e.g. "H264", "H265", "JPEG").
    ///
    /// The relay is served at `rtsp://{bind_host}:{port}/{camera_id}`.
    pub fn start_relay(
        &self,
        camera_id: Uuid,
        source_url: &str,
        codec: &str,
    ) -> Result<(), VmsError> {
        let launch = relay_launch_str(source_url, codec).ok_or_else(|| {
            VmsError::Media(format!("RTSP relay: unsupported codec '{codec}'"))
        })?;

        let factory = gstreamer_rtsp_server::RTSPMediaFactory::new();
        factory.set_launch(&launch);
        factory.set_shared(true);

        let path = relay_path(camera_id);
        self.mounts.add_factory(&path, factory);
        self.relays.lock().unwrap().insert(camera_id, ());

        tracing::info!(
            camera_id = %camera_id,
            codec,
            source_url,
            relay = %self.relay_url(camera_id).unwrap_or_default(),
            "Relay registered",
        );
        Ok(())
    }

    /// Remove the relay for a camera. No-op if not registered.
    pub fn stop_relay(&self, camera_id: Uuid) {
        self.mounts.remove_factory(&relay_path(camera_id));
        self.relays.lock().unwrap().remove(&camera_id);
        tracing::info!(camera_id = %camera_id, "Relay removed");
    }

    /// Return the relay URL for a camera if a relay is currently registered.
    ///
    /// The host is taken from the configured bind address; `0.0.0.0` is kept
    /// as-is (the client substitutes the actual server address).
    pub fn relay_url(&self, camera_id: Uuid) -> Option<String> {
        if !self.relays.lock().unwrap().contains_key(&camera_id) {
            return None;
        }
        let (host, port) = split_bind(&self.bind)?;
        Some(format!("rtsp://{}:{}/{}", host, port, camera_id.as_simple()))
    }

    pub fn is_relaying(&self, camera_id: Uuid) -> bool {
        self.relays.lock().unwrap().contains_key(&camera_id)
    }
}

// -- Codec probe --

/// Connect briefly to an RTSP source and return the RTP encoding name
/// ("H264", "H265", "JPEG", "AV1"). Runs the GStreamer probe on a
/// `spawn_blocking` thread so it does not block the async runtime.
pub async fn probe_codec(url: &str) -> Result<String, VmsError> {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || probe_codec_blocking(&url))
        .await
        .map_err(|e| VmsError::Media(format!("codec probe task panicked: {e}")))?
}

fn probe_codec_blocking(url: &str) -> Result<String, VmsError> {
    let pipeline = gstreamer::Pipeline::new();

    let src = gstreamer::ElementFactory::make("rtspsrc")
        .property("location", url)
        .property("latency", 200u32)
        .build()
        .map_err(|e| VmsError::Media(format!("probe rtspsrc: {e}")))?;

    let fakesink = gstreamer::ElementFactory::make("fakesink")
        .property("sync", false)
        .build()
        .map_err(|e| VmsError::Media(format!("probe fakesink: {e}")))?;

    pipeline
        .add_many([&src, &fakesink])
        .map_err(|e| VmsError::Media(format!("probe add_many: {e}")))?;

    let (tx, rx) = std::sync::mpsc::sync_channel::<String>(1);

    src.connect_pad_added(move |_, pad| {
        let caps = match pad.current_caps() {
            Some(c) => c,
            None => return,
        };
        let s = match caps.structure(0) {
            Some(s) => s,
            None => return,
        };
        if s.get::<&str>("media").ok() == Some("audio") {
            return;
        }
        if !s.name().starts_with("application/x-rtp") {
            return;
        }
        if let Ok(enc) = s.get::<&str>("encoding-name") {
            let _ = tx.send(enc.to_owned());
        }
    });

    pipeline
        .set_state(gstreamer::State::Playing)
        .map_err(|e| VmsError::Media(format!("probe pipeline start: {e}")))?;

    let result = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| VmsError::Media("codec probe timed out — camera did not respond in 10 s".into()));

    pipeline.set_state(gstreamer::State::Null).ok();

    result
}

// -- Helpers --

fn relay_path(camera_id: Uuid) -> String {
    format!("/{}", camera_id.as_simple())
}

/// Build the gst-launch pipeline string for the relay factory.
///
/// The pipeline uses passthrough (depay -> parse -> pay) — no transcode.
/// The `set_shared(true)` flag on the factory means a single upstream
/// connection is shared across all clients watching the same camera.
fn relay_launch_str(url: &str, codec: &str) -> Option<String> {
    let s = match codec.to_uppercase().as_str() {
        "H264" => format!(
            "( rtspsrc location={url} latency=100 protocols=tcp \
             ! rtph264depay ! h264parse config-interval=-1 \
             ! rtph264pay name=pay0 pt=96 )"
        ),
        "H265" | "HEVC" => format!(
            "( rtspsrc location={url} latency=100 protocols=tcp \
             ! rtph265depay ! h265parse config-interval=-1 \
             ! rtph265pay name=pay0 pt=96 )"
        ),
        "JPEG" => format!(
            "( rtspsrc location={url} latency=100 protocols=tcp \
             ! rtpjpegdepay ! jpegparse \
             ! rtpjpegpay name=pay0 pt=26 )"
        ),
        "AV1" => format!(
            "( rtspsrc location={url} latency=100 protocols=tcp \
             ! rtpav1depay ! av1parse \
             ! rtpav1pay name=pay0 pt=96 )"
        ),
        _ => return None,
    };
    Some(s)
}

/// Split "host:port" into ("host", "port"). Returns None for malformed input.
fn split_bind(bind: &str) -> Option<(String, String)> {
    let mut parts = bind.rsplitn(2, ':');
    let port = parts.next()?.to_owned();
    let host = parts.next()?.to_owned();
    Some((host, port))
}
