//! Recording, the restream, motion detection and snapshots with the OpenH264
//! plugin removed from GStreamer's registry, as on a system without
//! libopenh264. This file is its own test process, so removing the plugin
//! does not affect other tests. The test camera's encoder is created before
//! the plugin is removed.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gstreamer::prelude::*;
use gstreamer_rtsp_server::prelude::*;
use uuid::Uuid;
use vms_core::RecordingChunkEvent;
use vms_media::{MediaConfig, MediaManager, StreamQuality};

fn hide_openh264() {
    let registry = gstreamer::Registry::get();
    if let Some(plugin) = registry.find_plugin("openh264") {
        registry.remove_plugin(&plugin);
    }
    assert!(gstreamer::ElementFactory::find("openh264enc").is_none());
    assert!(gstreamer::ElementFactory::find("openh264dec").is_none());
}

/// Serves live 320x240 H.264 at 10 fps over RTSP, a moving ball with a
/// keyframe per second, and returns the URL. The media is shared, so its
/// encoder is created by the first client and reused by later ones.
fn serve() -> String {
    let context = gstreamer::glib::MainContext::new();
    let server = gstreamer_rtsp_server::RTSPServer::new();
    server.set_address("127.0.0.1");
    server.set_service("0");
    let factory = gstreamer_rtsp_server::RTSPMediaFactory::new();
    factory.set_launch(
        "( videotestsrc is-live=true pattern=ball \
         ! video/x-raw,width=320,height=240,framerate=10/1 \
         ! openh264enc complexity=low gop-size=10 ! h264parse \
         ! rtph264pay name=pay0 pt=96 config-interval=-1 )",
    );
    factory.set_shared(true);
    server.mount_points().unwrap().add_factory("/cam", factory);
    server.attach(Some(&context)).unwrap();
    let port = server.bound_port();
    std::thread::spawn(move || {
        let _server = server;
        gstreamer::glib::MainLoop::new(Some(&context), false).run();
    });
    format!("rtsp://127.0.0.1:{port}/cam")
}

/// A client that keeps reading `url` until it is dropped.
fn connect(url: &str) -> gstreamer::Element {
    let pipeline = gstreamer::parse::launch(&format!(
        "rtspsrc location={url} protocols=tcp ! fakesink sync=false"
    ))
    .unwrap();
    pipeline.set_state(gstreamer::State::Playing).unwrap();
    let (_, state, _) = pipeline.state(gstreamer::ClockTime::from_seconds(10));
    assert_eq!(state, gstreamer::State::Playing);
    pipeline
}

/// A TCP port that was free a moment ago. Restream URLs carry the bind port,
/// so the restream server cannot bind port 0.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Bytes a restream viewer receives in `duration`.
fn watch(url: &str, duration: Duration) -> u64 {
    let pipeline = gstreamer::parse::launch(&format!(
        "rtspsrc location={url} protocols=tcp ! fakesink name=sink sync=false"
    ))
    .unwrap()
    .downcast::<gstreamer::Pipeline>()
    .unwrap();
    let bytes = Arc::new(AtomicU64::new(0));
    let counted = bytes.clone();
    pipeline
        .by_name("sink")
        .unwrap()
        .static_pad("sink")
        .unwrap()
        .add_probe(gstreamer::PadProbeType::BUFFER, move |_, info| {
            let size = info.buffer().map_or(0, |b| b.size() as u64);
            counted.fetch_add(size, Ordering::SeqCst);
            gstreamer::PadProbeReturn::Ok
        });
    pipeline.set_state(gstreamer::State::Playing).unwrap();
    std::thread::sleep(duration);
    pipeline.set_state(gstreamer::State::Null).unwrap();
    bytes.load(Ordering::SeqCst)
}

#[tokio::test(flavor = "multi_thread")]
async fn recording_restream_motion_and_snapshots_work_without_openh264() {
    gstreamer::init().unwrap();
    let dir: PathBuf = std::env::temp_dir().join(format!("without-openh264-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    // The camera's encoder is created before the plugin is removed, as if
    // the camera were another device.
    let url = serve();
    let camera_client = connect(&url);
    hide_openh264();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::unbounded_channel();
    let (live_tx, _live_rx) = tokio::sync::mpsc::unbounded_channel();
    let config = MediaConfig {
        recording_dir: dir.join("recordings"),
        rtsp_bind: format!("127.0.0.1:{}", free_port()),
        chunk_duration_secs: 2,
        ..Default::default()
    };
    let media = MediaManager::new(config, event_tx, chunk_tx, live_tx).unwrap();
    let cam = Uuid::new_v4();

    media.start_recording(cam, &url, None).await.unwrap();
    media
        .start_restream(cam, StreamQuality::Main, &url, None, Some("H264"))
        .await
        .unwrap();
    media.require_motion(cam).await;

    let snapshot = media
        .capture_snapshot(cam, "jpeg", 80, &dir.join("snapshots"))
        .await
        .unwrap();
    assert!(std::fs::metadata(&snapshot).unwrap().len() > 0);

    let restream_url = media.restream_url(cam, StreamQuality::Main).unwrap();
    let watched = tokio::task::spawn_blocking(move || watch(&restream_url, Duration::from_secs(3)))
        .await
        .unwrap();
    assert!(watched > 0, "the restream viewer got no data");

    let closed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match chunk_rx.recv().await {
                Some(RecordingChunkEvent::Closed { size_bytes, .. }) => return size_bytes,
                Some(_) => {}
                None => panic!("the chunk channel closed"),
            }
        }
    })
    .await
    .expect("no recording chunk was closed");
    assert!(closed > 0);

    let motion = tokio::time::timeout(Duration::from_secs(20), event_rx.recv()).await;
    assert!(
        matches!(motion, Ok(Some(_))),
        "no motion event from the moving ball"
    );

    media.shutdown().await.unwrap();
    camera_client.set_state(gstreamer::State::Null).unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
