# Configuration

## Where settings come from

The daemon merges these sources in order. Later ones win.

1. Built-in defaults
2. `/etc/reeframe/config.toml`
3. `./reeframe.toml`, relative to the working directory
4. Environment variables starting with `VMS_`

A missing file is skipped. In environment variables, `__` separates a section from its key, so `[media] chunk_duration_secs` becomes `VMS_MEDIA__CHUNK_DURATION_SECS`.

```toml
# reeframe.toml
encryption_key = "…"

[auth]
jwt_secret = "…"

[media]
recording_dir = "/srv/reeframe/recordings"

[recordings]
retention_days = 14
timezone = "Europe/Berlin"
```

## Required

| Key | Environment variable | |
|---|---|---|
| `encryption_key` | `VMS_ENCRYPTION_KEY` | Base64, 32 bytes. Encrypts camera credentials at rest (AES-256-GCM). Changing it makes stored credentials unreadable. |
| `auth.jwt_secret` | `VMS_AUTH__JWT_SECRET` | Base64, 32 bytes. Signs access and refresh tokens. Changing it logs everyone out. |

Generate each with `openssl rand -base64 32`.

## All settings

| Key | Default | |
|---|---|---|
| `database.url` | `sqlite://./reeframe.db?mode=rwc` | SQLite, MySQL (`mysql://user:pass@host/db`) or PostgreSQL (`postgres://user:pass@host/db`). For SQLite, keep `?mode=rwc` so the file is created on first start. |
| `media.recording_dir` | `/var/lib/reeframe/recordings` | Where MP4 chunks and thumbnails are written. Chunks go in a `cam_<id>` folder per camera, exports in `exports`. |
| `media.chunk_duration_secs` | `300` | Length of each recording chunk |
| `media.thumbnail_interval_secs` | `30` | Spacing between thumbnails, for cameras that have them turned on |
| `recordings.retention_days` | `30` | Delete recordings older than this. `0` keeps them. |
| `recordings.retention_disk_threshold_percent` | `90` | Above this disk usage, delete the oldest recordings first. `0` turns it off. |
| `recordings.timezone` | `UTC` | IANA time zone for daily coverage summaries, for cameras without their own |
| `api.bind` | `0.0.0.0:8080` | REST API address |
| `rtsp.bind` | `0.0.0.0:8554` | RTSP restream address |
| `rtsp.public_host` | unset | Host put in `restream_url`. Unset, a `0.0.0.0` bind advertises the machine's outbound IP, which in Docker is the container's address. Set it to the host's LAN address or name. |
| `rtsp.public_port` | unset | Port put in `restream_url`, when the published port differs from the bind port |
| `rtsp.client_threads` | `4` | Threads that handle RTSP requests and send media to restream clients, 1 to 64. Any number of clients share them, so this limits neither viewers nor cameras. A client that connects holds its thread until the camera's next keyframe, so raise this when many clients connect at once. Any other value stops startup. |
| `auth.mode` | `local` | `local` issues and checks its own tokens. `oidc` also accepts tokens issued by a Reeframe Coordinator. |
| `auth.access_token_ttl_secs` | `900` | 15 minutes |
| `auth.refresh_token_ttl_secs` | `2592000` | 30 days |
| `auth.jwks_url` | unset | The Coordinator's `/.well-known/jwks.json`. Required for `oidc`. |
| `auth.jwks_refresh_interval_secs` | `300` | How long fetched Coordinator keys are trusted |
| `coordinator.be_id` | unset | This backend's site ID from the Coordinator, for setups without `vms-daemon enroll`. An enrolled site identity takes precedence, and a different value here stops startup. `auth.mode = "oidc"` needs one of the two. |
| `coordinator.control_addr` | empty | `host:port` of the Coordinator's control channel (QUIC, UDP). Empty uses the host of the enrolled Coordinator URL on port 443. Only used once the backend is enrolled. The WebSocket fallback always goes to the Coordinator URL. |
| `coordinator.ca_file` | empty | PEM file with extra CA certificates to trust for the Coordinator, for a Coordinator whose certificate comes from a private or company CA. They are added to the public roots, so public certificates keep working. Every connection to the Coordinator uses the same roots: enrollment, the control channel and fetching its token signing keys (`auth.jwks_url`). A file that cannot be read or holds no certificate stops startup. |
| `identity.path` | `site_identity.json` | Site identity written by `vms-daemon enroll`: key, site ID and Coordinator URL, readable only by its owner. Relative to the working directory. The Coordinator URL must start with `https://`; `vms-daemon enroll` refuses anything else, and the daemon does not start with an identity that holds another scheme. |
| `network.outbound_proxy_url` | empty | HTTP proxy in the site own network that outgoing connections must go through (HTTP CONNECT). Leave empty for direct access. This is not the reverse proxy in front of the Coordinator. Format `http://[user:pass@]host:port`, only `http://` is accepted. Used for the control channel WebSocket fallback; QUIC (UDP) does not go through it. |
| `webrtc.max_sessions` | `16` | Most WebRTC sessions the Coordinator may open at once. Each runs its own encoder. WebRTC needs the `gstreamer1.0-nice` package next to `gstreamer1.0-plugins-bad`. Sessions only run on an enrolled backend, and their site tokens are checked against `auth.jwks_url`, or the enrolled Coordinator URL plus `/.well-known/jwks.json` when it is unset. |
| `log_level` | `info` | Stderr log filter. Takes a level or any `RUST_LOG`-style directive, for example `info,vms_media=debug`. A non-empty `RUST_LOG` overrides it. Changes apply on restart. |

Setting `OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces or logs variant) turns on OpenTelemetry export. Prometheus metrics are always available at `/metrics`.

## Changing settings at runtime

`GET /system/settings` lists the settings that can be changed through the API. `PATCH /system/settings` takes a `{"key": value}` map:

```bash
curl -X PATCH localhost:8080/system/settings -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"recordings.retention_days": 14}'
```

The two retention settings take effect immediately. Every other setting is stored and applied on the next start, and the response marks it `pending_restart`. `auth.jwt_secret` can be written but is never returned. `database.url` and `encryption_key` can only be set in a file or the environment.

`POST /system/config-file` replaces `./reeframe.toml` with an uploaded TOML file, after backing up the old one. The upload is rejected unchanged if it doesn't parse.

## Per-camera settings

These are fields on the camera itself, set with `POST /cameras` or `PATCH /cameras/{id}`.

| Field | Default | |
|---|---|---|
| `motion_detection_enabled` | `false` | Run motion, scene-change and tamper detection whenever the camera is live. A pipeline with an event trigger on the camera turns it on regardless. |
| `motion_detection_mode` | `keyframes` | `keyframes` decodes only the stream's keyframes, `full` decodes every frame and analyzes up to 2 per second. See below. |
| `thumbnails_enabled` | `false` | Capture scrub-preview thumbnails while the camera records |
| `live_view_stream` | `sub` | `sub` restreams the sub stream, falling back to main when there is none. `main` always restreams the main stream. |
| `sub_rtsp_url` | unset | Low-resolution stream used for live view and analytics. Recording always uses `rtsp_url`. |

Changes apply right away to a running camera. Changing `rtsp_url`, `sub_rtsp_url` or the credentials reconnects it.

### Motion detection mode

`keyframes` decodes one frame per keyframe interval instead of every frame, so it needs far less CPU. It reacts within one or two keyframe intervals and can miss motion shorter than one interval. For faster detection, use `full`, ideally with the camera's sub stream set to 2 to 5 fps so decoding every frame stays cheap.

While a camera's keyframes are more than 4 s apart, `keyframes` decodes every frame instead and logs a warning. The read-only `effective_motion_mode` field on the camera shows the mode in use, or `null` while motion detection is not running. Changing the mode rebuilds only the motion detection branch; the camera's streams keep running.

Retention and time zone can be overridden per camera with `PATCH /cameras/{id}/retention-policy` and `PATCH /cameras/{id}/timezone`.
