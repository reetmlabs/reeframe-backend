## Reeframe backend (unreleased)

### Breaking: relay renamed to restream

The RTSP relay is now called the restream, in the API, the code and the docs. Update clients with the frontend release that matches this one; the old routes and fields are gone.

| Before | After |
|---|---|
| `POST /cameras/{id}/relay/start` | `POST /cameras/{id}/restream/start` |
| `POST /cameras/{id}/relay/stop` | `POST /cameras/{id}/restream/stop` |
| `relay_url` in the start response | `restream_url` |
| `relay_url` on a camera (main stream) | `main_restream_url` |
| `sub_relay_url` on a camera | `sub_restream_url` |

The `rtsp.*` settings keep their names.

### Breaking: H.264 encoding uses OpenH264, no H.265 output

`transcode`, `watermark` and `merge_clips` encode H.264 with OpenH264 (`openh264enc` from gstreamer1.0-plugins-bad) instead of x264, and gstreamer1.0-plugins-ugly is no longer needed.

- `transcode` accepts only `h264`, `vp9` and `av1` as `codec`. Pipelines that use `h265`, `hevc` or another value get a validation error and cannot be enabled until the node is changed; old settings are not converted. H.265 input is still accepted and comes out as H.264.
- The H.264 `preset` must be an x264 preset name (or empty) and maps to OpenH264's complexity.
- H.264 output is limited to 4096x2304 pixels. Larger video is scaled down to fit, and a larger `transcode` `resolution` is rejected.
- H.264 output is Constrained Baseline, and odd sizes are cropped to even ones.
- If OpenH264 is missing, only these jobs fail; recording, live view, motion detection and snapshots still work.

See [docs/video-encoding.md](../docs/video-encoding.md).

### What's new

- `rtsp.public_host` and `rtsp.public_port` set the host and port in restream URLs, so they work from the LAN when the daemon runs in Docker.
- `rtsp.client_threads` sets how many threads handle restream clients (default 4).

### Changed

- New recordings are written to a folder per camera, `{recording_dir}/cam_<id>/`, and exports to `{recording_dir}/exports/`. Recordings made before this release stay where they are and still play back, export and expire as before.

- Recordings are written with their index at the front, so closing a chunk no longer rewrites the whole file. A chunk cut off by a crash plays up to about 10 seconds before the crash.

### Fixed

- A faststart remux or export trim that fails no longer leaves its temp file next to the chunk. Temp files left by a crash are now also cleaned up from camera folders at startup.
- A camera that reconnects is no longer disconnected again by an error left over from its previous session.
- Transcoding to VP9 no longer fails: the bitrate was passed to the encoder with the wrong type.
- Motion detection no longer crashes the daemon on an H.264 stream without timing information in its headers.
