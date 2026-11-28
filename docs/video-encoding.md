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

The output is AV1 for AV1 input and H.264 for everything else. Neither action has a codec setting.

| Setting | Values |
|---|---|
| `bitrate_kbps` | Optional H.264 bitrate in kilobits per second, from 100 to 100000. Other values are rejected. Ignored for AV1 output, which uses the encoder's own quality mode. |

Without `bitrate_kbps`, the H.264 bitrate follows the output frame size (after any scaling, see below): 4000 kbps at 1920x1080 and 12000 kbps at 3840x2160, growing with the pixel count to the power log4(3), about 0.79, and rounded to 100 kbps. It stays between 300 and 16000 kbps.

| Output | Default bitrate |
|---|---|
| 1280x720 | 2100 kbps |
| 1920x1080 | 4000 kbps |
| 2560x1440 | 6300 kbps |
| 3840x2160 | 12000 kbps |
| 4096x2304 | 13300 kbps |

If the input size can't be read, the 1920x1080 default is used.

## H.264 limits

- **Frame size:** OpenH264 accepts at most 9,437,184 pixels (4096x2304, the H.264 level 5.2 limit), counted with both sides rounded up to a multiple of 16. Larger video, such as 4000x3000 from a 12 MP camera, is scaled down to fit with the same aspect ratio and even sizes, and the daemon logs the new size. In `transcode`, an explicit `resolution` above the limit is rejected.
- **Odd sizes:** OpenH264 crops an odd width or height to the next even number, so 641x361 comes out as 640x360.
- **Bitrate:** the encoder does not skip frames, so very noisy video, such as a dark scene with heavy sensor noise, can come out well above the target bitrate.
- **Profile:** the output is Constrained Baseline with no B-frames, which every browser plays.
