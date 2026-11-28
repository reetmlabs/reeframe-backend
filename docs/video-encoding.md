# Video encoding

The `transcode`, `watermark` and `merge_clips` actions decode their input and encode it again. Everything else (recording, live view and the restream, motion detection, thumbnails and snapshots) never encodes video.

## Encoders

| Output | GStreamer element | Package |
|---|---|---|
| H.264 | `openh264enc` (OpenH264) | gstreamer1.0-plugins-bad, which depends on libopenh264 |
| VP9 (`transcode` only) | `vp9enc` (libvpx) | gstreamer1.0-plugins-good |
| AV1 | `av1enc` (libaom) in `transcode`, `svtav1enc` (SVT-AV1) in `watermark` and `merge_clips` | gstreamer1.0-plugins-bad |

There is no H.265 encoder. H.265 input is decoded and encoded to H.264.

If `openh264enc` is not available, only the jobs that encode H.264 fail, with an error that names the missing element. Recording, live view, motion detection, thumbnails and snapshots still work.

## transcode

| Setting | Values |
|---|---|
| `codec` | `h264`, `vp9` or `av1`. `h265`, `hevc` and any other value are rejected. |
| `bitrate_kbps` | Target bitrate in kilobits per second. |
| `preset` | For H.264, an x264 speed preset mapped to OpenH264's complexity: `ultrafast`, `superfast` and `veryfast` to low; `faster`, `fast`, `medium` or empty to medium; `slow`, `slower`, `veryslow` and `placebo` to high. Other values are rejected. Ignored for VP9 and AV1. |
| `resolution` | `WxH`, or unset to keep the input size. For H.264 at most 9,437,184 pixels (see below). |

A pipeline with an unsupported setting has a validation error and cannot be enabled. A job that still runs with one fails without encoding anything.

## watermark and merge_clips

The output is AV1 for AV1 input and H.264 for everything else, at 2048 kbps. Neither action has a codec or bitrate setting.

## H.264 limits

- **Frame size:** OpenH264 accepts at most 9,437,184 pixels (4096x2304, the H.264 level 5.2 limit), counted with both sides rounded up to a multiple of 16. Larger video, such as 4000x3000 from a 12 MP camera, is scaled down to fit with the same aspect ratio and even sizes, and the daemon logs the new size. In `transcode`, an explicit `resolution` above the limit is rejected.
- **Odd sizes:** OpenH264 crops an odd width or height to the next even number, so 641x361 comes out as 640x360.
- **Bitrate:** the encoder does not skip frames, so very noisy video, such as a dark scene with heavy sensor noise, can come out well above `bitrate_kbps`.
- **Profile:** the output is Constrained Baseline with no B-frames, which every browser plays.
