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
| `media.recording_dir` | `/var/lib/reeframe/recordings` | Where MP4 chunks and thumbnails are written |
| `media.chunk_duration_secs` | `300` | Length of each recording chunk |
| `media.thumbnail_interval_secs` | `30` | Spacing between thumbnails, for cameras that have them turned on |
| `recordings.retention_days` | `30` | Delete recordings older than this. `0` keeps them. |
| `recordings.retention_disk_threshold_percent` | `90` | Above this disk usage, delete the oldest recordings first. `0` turns it off. |
| `recordings.timezone` | `UTC` | IANA time zone for daily coverage summaries, for cameras without their own |
| `api.bind` | `0.0.0.0:8080` | REST API address |
| `rtsp.bind` | `0.0.0.0:8554` | RTSP relay address |
| `auth.mode` | `local` | `local` issues and checks its own tokens. `oidc` also accepts tokens issued by a Reeframe Coordinator. |
| `auth.access_token_ttl_secs` | `900` | 15 minutes |
| `auth.refresh_token_ttl_secs` | `2592000` | 30 days |
| `auth.jwks_url` | unset | The Coordinator's `/.well-known/jwks.json`. Required for `oidc`. |
| `auth.jwks_refresh_interval_secs` | `300` | How long fetched Coordinator keys are trusted |
| `gateway.url` | unset | `host:port` of a Reeframe gateway for remote pairing |
| `gateway.be_id` | unset | This backend's site ID from the Coordinator. Required with `gateway.url` or `auth.mode = "oidc"`. |
| `log_level` | `info` | Stored and reported, but not applied yet. Set the log filter with `RUST_LOG` instead, for example `RUST_LOG=info`. |

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
| `thumbnails_enabled` | `false` | Capture scrub-preview thumbnails while the camera records |
| `live_view_stream` | `sub` | `sub` relays the sub stream, falling back to main when there is none. `main` always relays the main stream. |
| `sub_rtsp_url` | unset | Low-resolution stream used for live view and analytics. Recording always uses `rtsp_url`. |

Changes apply right away to a running camera. Changing `rtsp_url`, `sub_rtsp_url` or the credentials reconnects it.

Retention and time zone can be overridden per camera with `PATCH /cameras/{id}/retention-policy` and `PATCH /cameras/{id}/timezone`.
