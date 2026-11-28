//! Encoding actions with the OpenH264 plugin removed from GStreamer's
//! registry, as on a system without libopenh264. This file is its own test
//! process, so removing the plugin does not affect other tests.

use std::path::{Path, PathBuf};

use uuid::Uuid;
use vms_actions::{ActionContext, ActionDispatcher};
use vms_core::action::{
    ActionConfig, ClipOrder, GapFill, MergeClipsConfig, TranscodeConfig, WatermarkConfig,
    WatermarkPosition,
};
use vms_core::{NodeInput, NodeOutput, TriggerContext};

fn hide_openh264() {
    gstreamer::init().unwrap();
    let registry = gstreamer::Registry::get();
    if let Some(plugin) = registry.find_plugin("openh264") {
        registry.remove_plugin(&plugin);
    }
    assert!(gstreamer::ElementFactory::find("openh264enc").is_none());
}

fn clip() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/h265_4000x3000.mp4")
}

async fn run(config: ActionConfig, artifacts: usize, dir: &Path) -> NodeOutput {
    let node_id = Uuid::new_v4();
    let input = NodeInput {
        parent_outputs: (0..artifacts)
            .map(|_| NodeOutput::success(Uuid::new_v4()).with_artifact(clip()))
            .collect(),
        trigger_ctx: TriggerContext::for_schedule(Uuid::new_v4(), Uuid::new_v4()),
    };
    let ctx = ActionContext {
        recording_dir: dir.to_path_buf(),
        ..Default::default()
    };
    ActionDispatcher::dispatch(node_id, &config, &input, &ctx).await
}

fn transcode(codec: &str, output_format: &str) -> ActionConfig {
    ActionConfig::Transcode(TranscodeConfig {
        codec: codec.into(),
        bitrate_kbps: 500,
        resolution: Some("320x240".into()),
        preset: "fast".into(),
        output_format: output_format.into(),
    })
}

fn assert_needs_openh264(output: &NodeOutput) {
    assert!(!output.success);
    let error = output.error.as_deref().unwrap_or_default();
    assert!(error.contains("openh264enc"), "{error}");
}

#[tokio::test]
async fn only_h264_encoding_fails_without_openh264() {
    hide_openh264();
    let dir = std::env::temp_dir().join(format!("without-openh264-{}", Uuid::new_v4()));

    assert_needs_openh264(&run(transcode("h264", "mp4"), 1, &dir).await);
    let watermark = ActionConfig::Watermark(WatermarkConfig {
        text_template: "cam".into(),
        position: WatermarkPosition::TopLeft,
        font_size: 12,
        opacity: 1.0,
        bitrate_kbps: None,
    });
    assert_needs_openh264(&run(watermark, 1, &dir).await);
    let merge = ActionConfig::MergeClips(MergeClipsConfig {
        order: ClipOrder::Chronological,
        gap_fill: GapFill::Skip,
        output_format: "mp4".into(),
        bitrate_kbps: None,
    });
    assert_needs_openh264(&run(merge, 2, &dir).await);

    let vp9 = run(transcode("vp9", "webm"), 1, &dir).await;
    assert!(vp9.success, "{:?}", vp9.error);

    std::fs::remove_dir_all(&dir).ok();
}
