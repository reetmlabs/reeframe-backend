use std::{collections::HashMap, sync::Mutex};

use gstreamer::prelude::*;
use gstreamer_rtsp_server::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

use crate::relay_bridge::{self, RelayBridgeHandle};

// -- RelayQuality --

/// Which of a camera's two persistent pipelines a relay mount is bridged from.
///
/// Cameras typically allow only two concurrent RTSP sessions, used by the main
/// (live/recording) pipeline and the optional sub-stream pipeline (see
/// `sub_stream.rs`). Relays therefore tap one of those pipelines' tees instead
/// of opening their own connections. Starting a relay brings up the pipeline
/// it needs on demand; recording does not have to be active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelayQuality {
    /// Full resolution, on the same connection recording uses. Meant for
    /// full-screen live view.
    Main,
    /// The camera's low-resolution sub-stream, when configured. Meant for
    /// tile/grid live view.
    Sub,
}

impl RelayQuality {
    fn path_suffix(self) -> &'static str {
        match self {
            RelayQuality::Main => "",
            RelayQuality::Sub => "/sub",
        }
    }

    fn branch_suffix(self) -> &'static str {
        match self {
            RelayQuality::Main => "relay",
            RelayQuality::Sub => "subrelay",
        }
    }
}

// -- RelayServer --

struct RelayEntry {
    codec: String,
    bridge: RelayBridgeHandle,
}

/// Per-camera, per-quality RTSP relay server powered by `gstreamer-rtsp-server`.
///
/// Each camera can be served at up to two mounts:
/// `rtsp://{bind_host}:{bind_port}/{camera_id}` (main) and
/// `.../{camera_id}/sub` (sub). Both are bridged from a pipeline's tee (see
/// `relay_bridge.rs`), which `MediaManager::start_relay` starts on demand if
/// needed. The relay never opens its own connection to the camera and never
/// requires recording to be active.
///
/// A dedicated GLib main loop runs on a background thread to drive I/O for
/// the RTSP server without interfering with GStreamer's own main context.
pub struct RelayServer {
    mounts: gstreamer_rtsp_server::RTSPMountPoints,
    bind: String,
    public_host: Option<String>,
    public_port: Option<u16>,
    relays: Mutex<HashMap<(Uuid, RelayQuality), RelayEntry>>,
    _loop_thread: std::thread::JoinHandle<()>,
}

impl RelayServer {
    /// `public_host`/`public_port` override the host and port advertised in
    /// relay URLs, for when the bind address isn't what clients reach (Docker).
    pub fn new(
        bind: &str,
        public_host: Option<String>,
        public_port: Option<u16>,
    ) -> Result<Self, VmsError> {
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

        relay_bridge::warmup();

        tracing::info!(bind, "RTSP relay server listening");

        Ok(Self {
            mounts,
            bind: bind.to_owned(),
            public_host,
            public_port,
            relays: Mutex::new(HashMap::new()),
            _loop_thread: loop_thread,
        })
    }

    /// Register an RTSP relay mount for a camera, bridged from `tee_name`'s
    /// tee on `pipeline` (the main pipeline for [`RelayQuality::Main`], the
    /// sub-stream pipeline for [`RelayQuality::Sub`]). No-op if already
    /// registered for this `(camera_id, quality)`.
    ///
    /// `codec` is the RTP encoding name as reported by the camera's SDP
    /// (e.g. "H264", "H265", "JPEG").
    pub fn start_relay(
        &self,
        camera_id: Uuid,
        quality: RelayQuality,
        pipeline: &gstreamer::Pipeline,
        tee_name: &str,
        codec: &str,
    ) -> Result<(), VmsError> {
        if self
            .relays
            .lock()
            .unwrap()
            .contains_key(&(camera_id, quality))
        {
            return Ok(());
        }

        let path = relay_path(camera_id, quality);
        let bridge = relay_bridge::attach(
            pipeline,
            tee_name,
            camera_id,
            quality.branch_suffix(),
            &self.mounts,
            &path,
            codec,
        )?;

        self.relays.lock().unwrap().insert(
            (camera_id, quality),
            RelayEntry {
                codec: codec.to_owned(),
                bridge,
            },
        );

        tracing::info!(
            camera_id = %camera_id,
            quality = ?quality,
            codec,
            relay = %self.relay_url(camera_id, quality).unwrap_or_default(),
            "Relay registered",
        );
        Ok(())
    }

    /// Remove the relay mount for a camera/quality and detach its tee-tap
    /// from `pipeline`. No-op if not registered.
    pub fn stop_relay(
        &self,
        camera_id: Uuid,
        quality: RelayQuality,
        pipeline: &gstreamer::Pipeline,
    ) {
        let entry = self.relays.lock().unwrap().remove(&(camera_id, quality));
        if let Some(entry) = entry {
            if let Err(e) = relay_bridge::detach(pipeline, &self.mounts, camera_id, &entry.bridge) {
                tracing::warn!(
                    camera_id = %camera_id,
                    quality = ?quality,
                    error = %e,
                    "Failed to detach relay bridge",
                );
            }
        }
        tracing::info!(camera_id = %camera_id, quality = ?quality, "Relay removed");
    }

    /// Return the relay URL for a camera/quality if currently registered.
    pub fn relay_url(&self, camera_id: Uuid, quality: RelayQuality) -> Option<String> {
        if !self
            .relays
            .lock()
            .unwrap()
            .contains_key(&(camera_id, quality))
        {
            return None;
        }
        let (host, port) = advertised_host_port(
            &self.bind,
            self.public_host.as_deref(),
            self.public_port,
            primary_ip,
        )?;
        Some(format!(
            "rtsp://{}:{}{}",
            host,
            port,
            relay_path(camera_id, quality)
        ))
    }

    pub fn is_relaying(&self, camera_id: Uuid, quality: RelayQuality) -> bool {
        self.relays
            .lock()
            .unwrap()
            .contains_key(&(camera_id, quality))
    }

    /// Return the cached codec for a running relay, if any.
    pub fn codec(&self, camera_id: Uuid, quality: RelayQuality) -> Option<String> {
        self.relays
            .lock()
            .unwrap()
            .get(&(camera_id, quality))
            .map(|e| e.codec.clone())
    }
}

// -- Codec probe --

/// Connect briefly to an RTSP source and return the RTP encoding name
/// ("H264", "H265", "JPEG", "AV1"). Runs the GStreamer probe on a
/// `spawn_blocking` thread so it does not block the async runtime.
///
/// Used only for one-time codec detection, with the result cached on the
/// camera's DB row. The connection is short-lived, so it does not hold one of
/// the camera's session slots used by the main and sub pipelines.
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
        .map_err(|_| {
            VmsError::Media("codec probe timed out, camera did not respond in 10 s".into())
        });

    pipeline.set_state(gstreamer::State::Null).ok();

    result
}

// -- Helpers --

fn relay_path(camera_id: Uuid, quality: RelayQuality) -> String {
    format!("/{}{}", camera_id.as_simple(), quality.path_suffix())
}

/// Split "host:port" into ("host", "port"). Returns None for malformed input.
fn split_bind(bind: &str) -> Option<(String, String)> {
    let mut parts = bind.rsplitn(2, ':');
    let port = parts.next()?.to_owned();
    let host = parts.next()?.to_owned();
    Some((host, port))
}

/// Resolve the host and port clients should use. A configured public host or
/// port wins; otherwise a wildcard bind host (`0.0.0.0`/`::`) is replaced by
/// `detect_ip()` so the URL is routable on the LAN.
fn advertised_host_port(
    bind: &str,
    public_host: Option<&str>,
    public_port: Option<u16>,
    detect_ip: impl FnOnce() -> Option<String>,
) -> Option<(String, String)> {
    let (bind_host, bind_port) = split_bind(bind)?;
    let host = match public_host.filter(|h| !h.is_empty()) {
        Some(h) => h.to_owned(),
        None if bind_host == "0.0.0.0" || bind_host == "::" => detect_ip().unwrap_or(bind_host),
        None => bind_host,
    };
    let port = public_port.map_or(bind_port, |p| p.to_string());
    Some((host, port))
}

/// Detect the machine's primary outbound IP by "connecting" a UDP socket to a
/// public address (no packets are actually sent). Returns the local address the
/// OS selected, which is the IP that LAN clients should use to reach this host.
fn primary_ip() -> Option<String> {
    use std::net::UdpSocket;
    let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("8.8.8.8:80").ok()?;
    let addr = sock.local_addr().ok()?;
    Some(addr.ip().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detected() -> Option<String> {
        Some("192.168.1.5".into())
    }

    #[test]
    fn wildcard_bind_uses_detected_ip() {
        assert_eq!(
            advertised_host_port("0.0.0.0:8554", None, None, detected),
            Some(("192.168.1.5".into(), "8554".into()))
        );
    }

    #[test]
    fn specific_bind_host_is_kept() {
        assert_eq!(
            advertised_host_port("10.0.0.2:8554", None, None, detected),
            Some(("10.0.0.2".into(), "8554".into()))
        );
    }

    #[test]
    fn public_host_and_port_override_bind() {
        assert_eq!(
            advertised_host_port("0.0.0.0:8554", Some("nvr.lan"), Some(18554), detected),
            Some(("nvr.lan".into(), "18554".into()))
        );
    }

    #[test]
    fn empty_public_host_falls_back() {
        assert_eq!(
            advertised_host_port("0.0.0.0:8554", Some(""), None, detected),
            Some(("192.168.1.5".into(), "8554".into()))
        );
    }
}
