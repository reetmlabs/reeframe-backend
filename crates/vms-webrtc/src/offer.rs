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

#[cfg(test)]
mod tests {
    use super::*;

    fn offer_with(video_formats: &str) -> String {
        format!(
            "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n\
             m=video 9 UDP/TLS/RTP/SAVPF 96 102 127\r\nc=IN IP4 0.0.0.0\r\n\
             a=mid:0\r\na=recvonly\r\n{video_formats}"
        )
    }

    fn init() {
        gst::init().unwrap();
    }

    #[test]
    fn the_baseline_packetization_mode_1_format_is_picked() {
        init();
        let text = offer_with(
            "a=rtpmap:96 VP8/90000\r\n\
             a=rtpmap:102 H264/90000\r\n\
             a=fmtp:102 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f\r\n\
             a=rtpmap:127 H264/90000\r\n\
             a=fmtp:127 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f\r\n",
        );
        assert_eq!(parse(&text).unwrap().h264_payload_type, 127);
    }

    #[test]
    fn garbage_and_offers_without_h264_video_are_rejected() {
        init();
        assert!(parse("not sdp at all").is_err());
        assert!(parse(&offer_with("a=rtpmap:96 VP8/90000\r\n")).is_err());
        let audio_only = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n\
                          m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n";
        assert!(parse(audio_only).is_err());
    }
}
