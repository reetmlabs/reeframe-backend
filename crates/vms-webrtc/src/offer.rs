//! Reading the client's SDP offer.

use gst_sdp::SDPMessage;

/// A parsed offer and the H.264 payload type to send video with.
pub(crate) struct Offer {
    pub sdp: SDPMessage,
    pub h264_payload_type: u32,
}

/// Parses `text` and picks the offered H.264 format. The answer must use the
/// client's payload type, so it comes from the offer rather than being fixed.
pub(crate) fn parse(text: &str) -> Result<Offer, String> {
    let sdp = SDPMessage::parse_buffer(text.as_bytes()).map_err(|_| "not a valid SDP")?;
    let video = sdp
        .medias()
        .find(|media| media.media() == Some("video"))
        .ok_or("the offer has no video section")?;

    let mut formats: Vec<H264Format> = Vec::new();
    for i in 0.. {
        let Some(rtpmap) = video.attribute_val_n("rtpmap", i) else {
            break;
        };
        let Some((pt, encoding)) = rtpmap.split_once(' ') else {
            continue;
        };
        if !encoding.to_ascii_uppercase().starts_with("H264/") {
            continue;
        }
        let Ok(pt) = pt.parse::<u32>() else {
            continue;
        };
        let fmtp = fmtp_of(video, pt).unwrap_or_default();
        formats.push(H264Format { pt, fmtp });
    }

    let best = formats
        .iter()
        .max_by_key(|format| format.score())
        .ok_or("the offer has no H.264 video")?;
    let h264_payload_type = best.pt;
    Ok(Offer {
        sdp,
        h264_payload_type,
    })
}

struct H264Format {
    pt: u32,
    fmtp: String,
}

impl H264Format {
    /// The encoder produces constrained baseline with packetization mode 1,
    /// which every browser decodes, so the closest offered format wins.
    fn score(&self) -> u8 {
        let fmtp = self.fmtp.to_ascii_lowercase();
        let mode_1 = fmtp.contains("packetization-mode=1");
        let baseline =
            fmtp.contains("profile-level-id=42e0") || fmtp.contains("profile-level-id=4200");
        u8::from(mode_1) * 2 + u8::from(baseline)
    }
}

fn fmtp_of(media: &gst_sdp::SDPMediaRef, pt: u32) -> Option<String> {
    let prefix = format!("{pt} ");
    (0..)
        .map_while(|i| media.attribute_val_n("fmtp", i))
        .find_map(|fmtp| fmtp.strip_prefix(&prefix).map(str::to_owned))
}
