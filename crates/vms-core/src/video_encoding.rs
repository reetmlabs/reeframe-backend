//! Rules for the video that the `transcode`, `watermark` and `merge_clips`
//! actions encode. They are checked when a pipeline is validated and again
//! when a job runs.

use crate::action::TranscodeConfig;

/// Largest frame, in pixels, that the H.264 encoder (OpenH264) accepts, with
/// both sides rounded up to a multiple of 16. It is 4096x2304, the H.264 level
/// 5.2 limit.
pub const MAX_H264_FRAME_PIXELS: u64 = 9_437_184;

/// Whether the H.264 encoder accepts a `width`x`height` frame. It codes whole
/// 16x16 macroblocks, so the limit applies to the size rounded up to them.
pub fn fits_h264(width: u32, height: u32) -> bool {
    let blocks = |v: u32| u64::from(v).div_ceil(16) * 16;
    blocks(width) * blocks(height) <= MAX_H264_FRAME_PIXELS
}

/// Output codec of a `transcode` job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscodeCodec {
    H264,
    Vp9,
    Av1,
}

impl TranscodeCodec {
    /// Parses a `transcode` codec setting. H.265 and any unknown value are
    /// rejected rather than replaced by another codec.
    pub fn parse(codec: &str) -> Result<Self, String> {
        match codec {
            "h264" => Ok(Self::H264),
            "vp9" => Ok(Self::Vp9),
            "av1" => Ok(Self::Av1),
            "h265" | "hevc" => Err(format!(
                "codec '{codec}' is not supported: H.265 output is not available, \
                 use h264, vp9 or av1"
            )),
            other => Err(format!(
                "unknown codec '{other}', expected h264, vp9 or av1"
            )),
        }
    }
}

/// OpenH264 encoder complexity, from fastest to slowest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum H264Complexity {
    Low,
    Medium,
    High,
}

impl H264Complexity {
    /// Maps an x264-style speed preset to the nearest complexity. An empty
    /// preset means medium; an unknown one is rejected.
    pub fn from_preset(preset: &str) -> Result<Self, String> {
        match preset {
            "ultrafast" | "superfast" | "veryfast" => Ok(Self::Low),
            "" | "faster" | "fast" | "medium" => Ok(Self::Medium),
            "slow" | "slower" | "veryslow" | "placebo" => Ok(Self::High),
            other => Err(format!(
                "unknown preset '{other}', expected one of ultrafast, superfast, veryfast, \
                 faster, fast, medium, slow, slower, veryslow or placebo"
            )),
        }
    }

    /// Value of `openh264enc`'s `complexity` property.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Parses a `WxH` resolution such as `1920x1080`.
pub fn parse_resolution(res: &str) -> Result<(u32, u32), String> {
    let invalid = || format!("invalid resolution '{res}', expected WxH (e.g. 1920x1080)");
    let (w, h) = res.split_once('x').ok_or_else(invalid)?;
    let w: u32 = w.trim().parse().map_err(|_| invalid())?;
    let h: u32 = h.trim().parse().map_err(|_| invalid())?;
    if w == 0 || h == 0 {
        return Err(invalid());
    }
    Ok((w, h))
}

/// The size an H.264 frame of `width`x`height` is scaled to so that the
/// encoder accepts it (see [`fits_h264`]), keeping the aspect ratio with even
/// dimensions. `None` if it already fits.
pub fn fit_h264_frame(width: u32, height: u32) -> Option<(u32, u32)> {
    if fits_h264(width, height) {
        return None;
    }
    let pixels = f64::from(width) * f64::from(height);
    let scale = (MAX_H264_FRAME_PIXELS as f64 / pixels).sqrt();
    let mut w = ((f64::from(width) * scale) as u32 & !1).max(2);
    loop {
        let h = ((f64::from(w) * f64::from(height) / f64::from(width)).round() as u32 & !1).max(2);
        if fits_h264(w, h) || w == 2 {
            return Some((w, h));
        }
        w -= 2;
    }
}

/// Checks a `transcode` configuration: the codec, for H.264 the preset, and
/// the resolution, which for H.264 must fit in [`MAX_H264_FRAME_PIXELS`].
pub fn check_transcode(cfg: &TranscodeConfig) -> Result<(), String> {
    let codec = TranscodeCodec::parse(&cfg.codec)?;
    if codec == TranscodeCodec::H264 {
        H264Complexity::from_preset(&cfg.preset)?;
    }
    if let Some(res) = &cfg.resolution {
        let (w, h) = parse_resolution(res)?;
        if codec == TranscodeCodec::H264 && !fits_h264(w, h) {
            return Err(format!(
                "resolution {w}x{h} is too large for H.264: at most \
                 {MAX_H264_FRAME_PIXELS} pixels (4096x2304) are supported, \
                 counted with both sides rounded up to a multiple of 16"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transcode(codec: &str, preset: &str, resolution: Option<&str>) -> TranscodeConfig {
        TranscodeConfig {
            codec: codec.into(),
            bitrate_kbps: 2000,
            resolution: resolution.map(Into::into),
            preset: preset.into(),
            output_format: "mp4".into(),
        }
    }

    #[test]
    fn h264_vp9_and_av1_are_accepted() {
        for codec in ["h264", "vp9", "av1"] {
            assert_eq!(check_transcode(&transcode(codec, "fast", None)), Ok(()));
        }
    }

    #[test]
    fn h265_output_is_rejected() {
        for codec in ["h265", "hevc"] {
            let err = check_transcode(&transcode(codec, "fast", None)).unwrap_err();
            assert!(err.contains("H.265 output is not available"), "{err}");
        }
    }

    #[test]
    fn encoder_names_and_unknown_codecs_are_rejected() {
        for codec in ["x264enc", "x265enc", "mpeg2", ""] {
            let err = check_transcode(&transcode(codec, "fast", None)).unwrap_err();
            assert!(err.starts_with("unknown codec"), "{err}");
        }
    }

    #[test]
    fn presets_map_to_the_nearest_complexity() {
        let complexity = |p| H264Complexity::from_preset(p).unwrap();
        assert_eq!(complexity("ultrafast"), H264Complexity::Low);
        assert_eq!(complexity("veryfast"), H264Complexity::Low);
        assert_eq!(complexity(""), H264Complexity::Medium);
        assert_eq!(complexity("fast"), H264Complexity::Medium);
        assert_eq!(complexity("medium"), H264Complexity::Medium);
        assert_eq!(complexity("slow"), H264Complexity::High);
        assert_eq!(complexity("placebo"), H264Complexity::High);
    }

    #[test]
    fn an_unknown_h264_preset_is_rejected() {
        let err = check_transcode(&transcode("h264", "turbo", None)).unwrap_err();
        assert!(err.contains("unknown preset 'turbo'"), "{err}");
        assert_eq!(check_transcode(&transcode("vp9", "turbo", None)), Ok(()));
    }

    #[test]
    fn an_h264_resolution_above_the_limit_is_rejected() {
        assert_eq!(
            check_transcode(&transcode("h264", "fast", Some("4096x2304"))),
            Ok(())
        );
        let err = check_transcode(&transcode("h264", "fast", Some("4000x3000"))).unwrap_err();
        assert!(err.contains("too large for H.264"), "{err}");
        assert_eq!(
            check_transcode(&transcode("av1", "", Some("4000x3000"))),
            Ok(())
        );
    }

    #[test]
    fn malformed_resolutions_are_rejected() {
        for res in ["bad", "1920", "0x1080", "1920x", "-1x2"] {
            assert!(parse_resolution(res).is_err(), "{res}");
        }
        assert_eq!(parse_resolution("1280 x 720"), Ok((1280, 720)));
    }

    #[test]
    fn large_frames_are_scaled_to_fit_with_even_sizes() {
        assert_eq!(fit_h264_frame(3840, 2160), None);
        assert_eq!(fit_h264_frame(4096, 2304), None);
        for (w, h) in [(4000, 3000), (7680, 4320), (8192, 4320), (5001, 3001)] {
            let (fw, fh) = fit_h264_frame(w, h).unwrap();
            assert!(fits_h264(fw, fh), "{fw}x{fh}");
            assert_eq!((fw % 2, fh % 2), (0, 0));
            let ratio = f64::from(w) / f64::from(h);
            assert!((f64::from(fw) / f64::from(fh) - ratio).abs() < 0.01);
        }
        assert_eq!(fit_h264_frame(7680, 4320), Some((4096, 2304)));
    }

    #[test]
    fn the_limit_counts_whole_macroblocks() {
        assert!(fits_h264(4096, 2304));
        assert!(fits_h264(4090, 2300));
        // 9,435,000 pixels, but 3008x3152 once rounded up to macroblocks.
        assert!(!fits_h264(3000, 3145));
        let err = check_transcode(&transcode("h264", "", Some("3000x3145"))).unwrap_err();
        assert!(err.contains("too large for H.264"), "{err}");
    }
}
