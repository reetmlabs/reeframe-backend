//! Action and transport node configuration types.
//!
//! Every non-root, non-condition, non-fork node in a pipeline DAG is either an
//! *action node* or a *transport node*.
//!
//! - **Action nodes** transform media, control devices, or produce rendered
//!   text.  Their configuration is a concrete `*Config` struct wrapped in the
//!   [`ActionConfig`] discriminated enum.
//! - **Transport nodes** deliver artifacts or messages to an external
//!   destination.  They carry a [`TransportConfig`] for path/filename/body
//!   templates plus a `destination_id` foreign key.
//!
//! # Design: enum of named structs
//!
//! Each action type has its own dedicated config struct (e.g. [`TranscodeConfig`],
//! [`ExtractClipConfig`]).  [`ActionConfig`] is a tagged enum that wraps them.
//! This means:
//!
//! - Executor handlers receive the concrete struct, not the whole enum.
//! - API request bodies deserialize directly into the concrete struct.
//! - Each struct gets its own rustdoc page.
//!
//! Serde serializes the enum with an internal `action_type` tag, producing a
//! flat JSON object:
//!
//! ```json
//! { "action_type": "transcode", "codec": "h265", "bitrate_kbps": 4000 }
//! ```

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// -- Supporting enums --

/// Order in which clips are sorted when merging.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClipOrder {
    /// Oldest clip first.
    Chronological,
    /// Newest clip first.
    ReverseChronological,
}

/// Strategy for filling time gaps between concatenated clips.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GapFill {
    /// Insert black frames for the duration of the gap.
    BlackFrame,
    /// Freeze the last frame of the preceding clip.
    Freeze,
    /// Skip the gap entirely (clips are joined seamlessly).
    Skip,
}

/// Lossless compression algorithm used by the `compress` action.
///
/// Rarely used — only for non-video artifacts such as JSON detection logs,
/// CSV exports, and audit traces.  Video files are already entropy-coded by
/// their codec; applying these algorithms yields no size reduction.  Use the
/// `transcode` action (H.265 / AV1) to reduce video file size.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionAlgorithm {
    /// Zstandard — best ratio/speed trade-off; recommended default.
    Zstd,
    /// Gzip — widely compatible; slower than Zstd at equivalent ratios.
    Gzip,
    /// LZ4 — fastest decompression; lower ratio than Zstd.
    Lz4,
}

/// Encryption algorithm used by the `encrypt` action.
///
/// Mostly used when footage must be stored in third-party storage (S3, SFTP,
/// etc.) that cannot be fully trusted, or for forensic evidence export where
/// only the recipient holds the decryption key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EncryptionAlgorithm {
    /// AES-256 in Galois/Counter Mode — authenticated encryption.
    Aes256Gcm,
}

/// Where the watermark text is rendered on the frame.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkPosition {
    /// Top-left corner.
    TopLeft,
    /// Top-right corner.
    TopRight,
    /// Bottom-left corner.
    BottomLeft,
    /// Bottom-right corner.
    BottomRight,
    /// Centred on the frame.
    Center,
}

/// Output format for the `render_notification` action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotificationFormat {
    /// Plain text.
    Text,
    /// HTML — suitable for email transports.
    Html,
    /// Markdown — suitable for Slack / Matrix / Teams transports.
    Markdown,
}

// -- PTZ command --

/// A PTZ movement command dispatched to a camera via the `ptz_move` action.
///
/// Serialized with a `mode` tag to allow the three movement modes to share the
/// same JSON envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PtzCommand {
    /// Move the camera to a preset position stored in the camera's firmware.
    ///
    /// The VMS sends only the preset slot number; the camera recalls its own
    /// saved pan/tilt/zoom coordinates.  Preset positions are configured
    /// directly on the camera, not stored in the VMS.
    Preset {
        /// Preset slot number (1-indexed, camera-specific).
        ///
        /// Mapped to an ONVIF `PresetToken` string at dispatch time.
        preset_id: u32,
    },
    /// Move to an absolute pan/tilt/zoom position.
    Absolute {
        /// Absolute pan angle in degrees (camera-specific range).
        pan: f32,
        /// Absolute tilt angle in degrees (camera-specific range).
        tilt: f32,
        /// Absolute zoom level (camera-specific range).
        zoom: f32,
    },
    /// Move by a delta relative to the current position.
    Relative {
        /// Pan delta in degrees.
        pan: f32,
        /// Tilt delta in degrees.
        tilt: f32,
        /// Zoom delta.
        zoom: f32,
    },
}

// -- Per-action configuration structs --

/// Configuration for the `transcode` action.
///
/// Re-encodes the upstream artifact to a different codec, bitrate, or
/// resolution using FFmpeg.  This is the correct action for reducing video
/// file size (not `compress`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscodeConfig {
    /// Target codec name (e.g. `"h264"`, `"h265"`, `"vp9"`, `"av1"`).
    pub codec: String,
    /// Target bitrate in kilobits per second.
    pub bitrate_kbps: u32,
    /// Optional resolution override (e.g. `"1920x1080"`); `None` keeps the source resolution.
    pub resolution: Option<String>,
    /// Encoder preset (e.g. `"fast"`, `"slow"`) — passed directly to FFmpeg `-preset`.
    pub preset: String,
    /// Container format for the output file (e.g. `"mp4"`, `"mkv"`).
    pub output_format: String,
}

/// Configuration for the `extract_clip` action.
///
/// Cuts a time-bounded segment from the camera's ring-buffer centred on the
/// event timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractClipConfig {
    /// Seconds of footage to include *before* the event timestamp.
    pub pre_event_secs: u32,
    /// Seconds of footage to include *after* the event timestamp.
    pub post_event_secs: u32,
    /// Container format for the extracted clip (e.g. `"mp4"`).
    pub format: String,
    /// Camera to extract from.  `None` inherits `camera_id` from the [`TriggerContext`].
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub camera_id: Option<Uuid>,
    /// When `true`, use explicit `start`/`end` timestamps from
    /// [`TriggerContext::manual_params`] instead of deriving them from the
    /// event timestamp.  Used for manual "export a specific range" pipelines.
    ///
    /// [`TriggerContext::manual_params`]: crate::trigger::TriggerContext::manual_params
    #[serde(default)]
    pub use_manual_range: bool,
}

/// Configuration for the `snapshot` action.
///
/// Captures a single still frame from a camera.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    /// Image format (e.g. `"jpeg"`, `"png"`).
    pub format: String,
    /// JPEG quality 1–100 (ignored for lossless formats such as PNG).
    pub quality: u8,
    /// Camera to snapshot.  `None` inherits `camera_id` from the [`TriggerContext`].
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub camera_id: Option<Uuid>,
}

/// Configuration for the `merge_clips` action.
///
/// Concatenates multiple clip artifacts from parent nodes into a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeClipsConfig {
    /// Sort order applied to the input clips before concatenation.
    pub order: ClipOrder,
    /// How to handle time gaps between clips.
    pub gap_fill: GapFill,
    /// Container format for the merged output (e.g. `"mp4"`).
    pub output_format: String,
}

/// Configuration for the `compress` action.
///
/// Applies lossless compression to the upstream artifact.  Only useful for
/// non-video artifacts (JSON, CSV, logs).  See [`CompressionAlgorithm`] for
/// why this action should not be applied to video files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressConfig {
    /// Compression algorithm to apply.
    pub algorithm: CompressionAlgorithm,
    /// Compression level (algorithm-specific, e.g. 1–22 for Zstd).
    pub level: u8,
}

/// Configuration for the `encrypt` action.
///
/// Encrypts the upstream artifact using authenticated encryption.  The output
/// file can only be decrypted by a party that has access to the named key.
///
/// # Example pipeline
///
/// ```text
/// ExtractClip -> Watermark -> Encrypt -> Transport(S3)
/// ```
///
/// The clip lands on S3 already encrypted; a bucket breach cannot expose
/// footage without the key from the secrets backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptConfig {
    /// Encryption algorithm to use.
    pub algorithm: EncryptionAlgorithm,
    /// Logical name of the key in the key store / Vault.
    ///
    /// The actual key material is never stored in the pipeline config.  The
    /// executor resolves this name at run-time via the secrets backend
    /// (HashiCorp Vault, AWS KMS, or a local key file for the community edition).
    pub key_ref: String,
}

/// Configuration for the `watermark` action.
///
/// Burns a text watermark onto every frame of a video or onto an image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkConfig {
    /// minijinja template evaluated at run-time with [`TriggerContext`] in scope.
    ///
    /// Example: `"{{ camera_name }} — {{ fired_at }}"`.
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub text_template: String,
    /// Where on the frame to render the watermark.
    pub position: WatermarkPosition,
    /// Font size in points.
    pub font_size: u32,
    /// Watermark opacity: `0.0` = fully transparent, `1.0` = fully opaque.
    pub opacity: f32,
}

/// Configuration for the `render_notification` action.
///
/// Renders a minijinja template into a [`NodeOutput::text`] string that
/// downstream transport nodes use as the message body.
///
/// [`NodeOutput::text`]: crate::node::NodeOutput::text
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderNotificationConfig {
    /// minijinja template rendered with [`TriggerContext`] variables in scope.
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub template: String,
    /// Format of the rendered output — determines which transport types can
    /// consume it directly.
    pub format: NotificationFormat,
}

/// Configuration for the `delay` action.
///
/// Pauses the pipeline branch for a fixed duration.  Useful for rate-limiting
/// or implementing a "wait N seconds before sending" pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelayConfig {
    /// Number of seconds to sleep before the next node executes.
    pub duration_secs: u64,
}

/// Configuration for the `ptz_move` action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PtzMoveConfig {
    /// Target camera.  `None` inherits `camera_id` from the [`TriggerContext`].
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub camera_id: Option<Uuid>,
    /// The movement to perform.
    pub command: PtzCommand,
}

/// Configuration for the `start_recording` action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRecordingConfig {
    /// Target camera.  `None` inherits `camera_id` from the [`TriggerContext`].
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub camera_id: Option<Uuid>,
    /// Maximum recording duration in seconds.  `0` means record indefinitely
    /// until a `stop_recording` action fires.
    pub duration_secs: u32,
    /// Quality profile name (camera-specific, e.g. `"high"`, `"low"`).
    pub quality: String,
}

/// Configuration for the `stop_recording` action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopRecordingConfig {
    /// Target camera.  `None` inherits `camera_id` from the [`TriggerContext`].
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub camera_id: Option<Uuid>,
}

/// Configuration for the `set_stream_quality` action.
///
/// Switches a camera to a different streaming quality profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetStreamQualityConfig {
    /// Target camera.  `None` inherits `camera_id` from the [`TriggerContext`].
    ///
    /// [`TriggerContext`]: crate::trigger::TriggerContext
    pub camera_id: Option<Uuid>,
    /// Quality profile name to activate (camera-specific, e.g. `"main"`, `"sub"`).
    pub profile: String,
}

/// Configuration for the `trigger_alarm_output` action.
///
/// Pulses an alarm output relay on a camera or NVR for a fixed duration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerAlarmOutputConfig {
    /// Identifier of the alarm output channel on the camera or NVR.
    pub output_id: String,
    /// How many seconds to hold the relay closed.
    pub duration_secs: u32,
}

// -- ActionConfig enum --

/// Discriminated union of all action node configurations.
///
/// Stored as JSONB/JSON in `pipeline_nodes.config` with `action_type` as the
/// internal serde tag.  The Action Executor matches on this enum to dispatch to
/// the correct handler and destructures the inner struct to obtain typed
/// parameters:
///
/// ```rust,ignore
/// match &node.action_config {
///     Some(ActionConfig::Transcode(cfg))   => transcode_handler(input, cfg).await,
///     Some(ActionConfig::ExtractClip(cfg)) => extract_clip_handler(input, cfg).await,
///     // …
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action_type", rename_all = "snake_case")]
pub enum ActionConfig {
    /// Re-encode media to a different codec / bitrate / resolution.
    Transcode(TranscodeConfig),
    /// Extract a time-bounded clip from the ring-buffer.
    ExtractClip(ExtractClipConfig),
    /// Capture a single still frame.
    Snapshot(SnapshotConfig),
    /// Concatenate multiple clips into one file.
    MergeClips(MergeClipsConfig),
    /// Losslessly compress a non-video artifact.
    Compress(CompressConfig),
    /// Encrypt an artifact using AES-256-GCM.
    Encrypt(EncryptConfig),
    /// Burn a text watermark onto a video or image.
    Watermark(WatermarkConfig),
    /// Render a minijinja template to a notification text string.
    RenderNotification(RenderNotificationConfig),
    /// Pause pipeline execution for a fixed duration.
    Delay(DelayConfig),
    /// Send a PTZ move command to a camera.
    PtzMove(PtzMoveConfig),
    /// Start a recording session on a camera.
    StartRecording(StartRecordingConfig),
    /// Stop the active recording session on a camera.
    StopRecording(StopRecordingConfig),
    /// Switch a camera to a different streaming quality profile.
    SetStreamQuality(SetStreamQualityConfig),
    /// Pulse an alarm output relay on a camera or NVR.
    TriggerAlarmOutput(TriggerAlarmOutputConfig),
}

impl ActionConfig {
    /// Returns the `snake_case` type name of this action.
    ///
    /// Useful for logging, metrics, and API responses where only the
    /// discriminator string is needed without re-serializing the full config.
    pub fn action_type_str(&self) -> &'static str {
        match self {
            Self::Transcode(_) => "transcode",
            Self::ExtractClip(_) => "extract_clip",
            Self::Snapshot(_) => "snapshot",
            Self::MergeClips(_) => "merge_clips",
            Self::Compress(_) => "compress",
            Self::Encrypt(_) => "encrypt",
            Self::Watermark(_) => "watermark",
            Self::RenderNotification(_) => "render_notification",
            Self::Delay(_) => "delay",
            Self::PtzMove(_) => "ptz_move",
            Self::StartRecording(_) => "start_recording",
            Self::StopRecording(_) => "stop_recording",
            Self::SetStreamQuality(_) => "set_stream_quality",
            Self::TriggerAlarmOutput(_) => "trigger_alarm_output",
        }
    }
}

// -- Transport node config --

/// Optional template overrides for a transport node.
///
/// Stored in `pipeline_nodes.config` alongside the `destination_id` foreign
/// key.  All fields are minijinja templates evaluated with [`TriggerContext`]
/// variables in scope.  When a field is `None`, the transport adapter falls
/// back to its destination-level defaults.
///
/// [`TriggerContext`]: crate::trigger::TriggerContext
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TransportConfig {
    /// minijinja template for the remote directory path.
    ///
    /// Example: `"recordings/{{ camera_name }}/{{ fired_at | date }}"`.
    pub path_template: Option<String>,
    /// minijinja template for the remote filename.
    ///
    /// Example: `"{{ fired_at | timestamp }}_{{ camera_name }}.mp4"`.
    pub filename_template: Option<String>,
    /// minijinja template for the message body.
    ///
    /// When `None` and the upstream node produced a [`NodeOutput::text`] (e.g.
    /// via `render_notification`), that text is used verbatim.
    ///
    /// [`NodeOutput::text`]: crate::node::NodeOutput::text
    pub message_template: Option<String>,
}
