<div align="center">
  <img src="assets/icon.svg" width="96" height="96" alt="OneWard VMS"/>
  <h1>OneWard VMS</h1>
  <p><strong>A Rust-native, pipeline-driven Video Management System.</strong><br/>
  Maximum automation flexibility. Zero custom code.</p>

  ![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)
  ![Version](https://img.shields.io/badge/version-v0.1.0-informational)
  ![Docker](https://img.shields.io/badge/docker-ghcr.io-0ea5e9)
  ![Rust](https://img.shields.io/badge/built%20with-Rust-orange)
</div>

---

## What is OneWard?

OneWard is an open-source VMS built around one idea: **everything the system does is a pipeline**.

Recording footage, sending a Telegram clip when someone crosses a line, running a nightly S3 backup, moving a PTZ camera when a door sensor fires — these are not separate features with separate configuration dialogs. They are all pipelines: a trigger connected to a sequence of action and transport nodes in a directed acyclic graph (DAG).

That single abstraction gives you the composability, auditability, and extensibility that purpose-built feature modules never can.

---

## The Pipeline Framework

> *Maximum flexibility with minimum code — wire any trigger to any action to any destination, without writing a single line of code.*

Every automated behavior in OneWard is expressed as a **Pipeline DAG**: a directed graph of typed nodes that the engine evaluates whenever the trigger fires.

```
   ┌─────────────┐
   │   TRIGGER   │  — schedule, event, MQTT, webhook, manual, stat threshold
   └──────┬──────┘
          │
   ┌──────▼──────┐
   │   ACTION    │  — transcode, extract_clip, snapshot, watermark, delay, condition …
   └──────┬──────┘
          │ (fork: run branches concurrently)
    ┌─────┴──────┐
    │            │
┌───▼───┐   ┌───▼────┐
│TRANSP.│   │TRANSP. │  — S3, SFTP, Telegram, Email, Slack, SMS, webhook …
└───────┘   └────────┘
```

### Why this matters

| Traditional VMS | OneWard |
|---|---|
| "Motion alert" is a fixed feature | Any trigger can activate any set of actions |
| Adding a new workflow requires a plugin or config wizard | Connect nodes via API — no code required |
| Resources (RTSP connections, ring buffers) are always on | Resources start only when a pipeline that needs them is enabled |
| Audit trail is whatever the vendor exposes | Every pipeline run, every node result, every timing is stored |

### Node types

| Category | Nodes |
|---|---|
| **Triggers** | `schedule`, `event` (camera or source), `system`, `manual`, `stat` (disk / RAM / CPU thresholds) |
| **Actions** | `transcode`, `extract_clip`, `snapshot`, `merge_clips`, `compress`, `encrypt`, `watermark`, `render_notification`, `delay`, `condition`, `fork`, `start_recording`, `stop_recording`, `ptz_move`, `set_stream_quality` |
| **Transports** | `local`, `s3`, `sftp`, `smb`, `telegram`, `email`, `slack`, `sms`, `webhook` |

### Example: motion-triggered Telegram clip

```jsonc
{
  "name": "Front door — motion clip to Telegram",
  "trigger": { "type": "event", "camera_id": "...", "event": "motion_detected" },
  "nodes": [
    { "id": "clip",   "type": "extract_clip",  "config": { "pre_secs": 5, "post_secs": 10 } },
    { "id": "notify", "type": "telegram",       "config": { "destination_id": "..." } }
  ],
  "edges": [
    { "from": "clip", "to": "notify" }
  ]
}
```

That is the entire definition. No code. One API call.

---

## Quick Start

### Docker (recommended)

```bash
# Generate a persistent encryption key
export VMS_ENCRYPTION_KEY=$(openssl rand -base64 32)

# Start the daemon
docker run -d \
  -e VMS_ENCRYPTION_KEY="$VMS_ENCRYPTION_KEY" \
  -p 8080:8080 \
  -v onward-data:/var/lib/onward \
  ghcr.io/yourorg/onward-vms-be:latest
```

Or with `docker compose`:

```bash
echo "VMS_ENCRYPTION_KEY=$(openssl rand -base64 32)" > .env
docker compose up -d
```

### Debian / Ubuntu

```bash
sudo dpkg -i onward-vms_0.1.0_amd64.deb
sudo sh -c 'echo VMS_ENCRYPTION_KEY=$(openssl rand -base64 32) >> /etc/onward/env'
sudo systemctl start onward.service
```

### Verify

```bash
curl http://localhost:8080/health
# → {"status":"ok"}
```

### Add a camera and start recording

```bash
# Register a camera
curl -s -X POST http://localhost:8080/cameras \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Front door",
    "rtsp_url": "rtsp://192.168.1.100/stream",
    "username": "admin",
    "password": "secret"
  }' | tee /tmp/cam.json

CAM_ID=$(jq -r .id /tmp/cam.json)

# Start continuous recording
curl -X POST "http://localhost:8080/cameras/$CAM_ID/recording/start"
```

MP4 chunks appear in `/var/lib/onward/recordings/`.

---

## Features — v0.1.0

### Recording & media
- Continuous chunked MP4 recording via GStreamer `splitmuxsink`
- H.264, H.265, MJPEG, and AV1 codec support
- Automatic RTSP reconnection with exponential backoff (2 s → 60 s cap)
- Per-camera start / stop via REST API

### Security
- AES-256-GCM credential encryption at rest (RTSP passwords, API keys, S3 secrets)
- 12-byte random nonce per field per write — same plaintext never produces the same ciphertext
- Systemd unit with `NoNewPrivileges`, `ProtectSystem=strict`, capability bounding set cleared

### Database
- SQLite out of the box; MySQL and PostgreSQL supported via the same SeaORM migrations
- Auto-migration on startup — no manual `migrate` command needed

### API
- Full REST API over Salvo (Rust)
- Camera, source, and destination CRUD
- `POST /cameras/{id}/recording/start|stop`
- `GET /health`

### Deployment
- Single static binary
- Multi-stage Docker image (GStreamer runtime, non-root user, healthcheck)
- `.deb` package with systemd integration and postinst hardening
- `.tar.gz` archive for manual installs

---

## Configuration

All settings have sane defaults. Override via environment variable or config file.

| Env var | Default | Description |
|---|---|---|
| `VMS_ENCRYPTION_KEY` | *(required)* | Base64-encoded 32-byte AES-256-GCM key. Generate: `openssl rand -base64 32` |
| `VMS_DATABASE__URL` | `sqlite:///var/lib/onward/onward.db` | SeaORM connection URL (`sqlite://`, `mysql://`, `postgres://`) |
| `VMS_MEDIA__RECORDING_DIR` | `/var/lib/onward/recordings` | Directory for MP4 chunk output |
| `VMS_MEDIA__CHUNK_DURATION_SECS` | `300` | Seconds per recording chunk |
| `VMS_API__BIND` | `0.0.0.0:8080` | HTTP server bind address |
| `RUST_LOG` | `info` | Log level (`trace`, `debug`, `info`, `warn`, `error`) |

Config files (lowest → highest priority, all optional):

```
/etc/onward/config.toml   — system-wide
./onward.toml             — local dev override
VMS_* env vars            — highest priority
```

---

## Roadmap

| Release | What ships                                                                           |
|---|--------------------------------------------------------------------------------------|
| **v0.1.0** ✓ | Camera CRUD, continuous RTSP recording, REST API, Docker, .deb                       |
| **v0.2.0** | Pipeline DAG engine — schedule & manual triggers, ring buffer, executor              |
| **v0.3.0** | All 14 action nodes, all 10 transport adapters                                       |
| **v0.4.0** | External event sources — MQTT, Home Assistant, HTTP webhook, file watcher            |
| **v0.5.0** | Full authenticated API, Prometheus metrics, OTLP tracing/logs, ONNX object detection |
| **v1.0.0** | ONVIF discovery, arm64 builds, community documentation                               |

Enterprise features (SSO, RBAC, facial recognition, HA clustering, tiered storage) are built on top of the same community engine in a separate private repository. The pipeline DAG, all action nodes, and all transport adapters stay open source forever.

---

## Codec support

| Codec | Elements | Notes |
|---|---|---|
| H.264 | `rtph264depay` + `h264parse` | Most cameras; `gst-plugins-good` |
| H.265 / HEVC | `rtph265depay` + `h265parse` | Higher-efficiency cameras; `gst-plugins-bad` |
| MJPEG | `rtpjpegdepay` + `jpegparse` | Older / cheap cameras; larger files |
| AV1 | `rtpav1depay` + `av1parse` | Newer high-end cameras; `gst-plugins-rs` |

---

## Building from source

```bash
# Install GStreamer dev libraries (Debian / Ubuntu)
sudo apt-get install -y \
  pkg-config \
  libgstreamer1.0-dev \
  libgstreamer-plugins-base1.0-dev \
  libssl-dev

# Build
cargo build --release --bin vms-daemon

# Run
VMS_ENCRYPTION_KEY=$(openssl rand -base64 32) \
  ./target/release/vms-daemon
```

---

## Contributing

OneWard is in active development. Contributions are welcome.

1. Fork the repo and create a feature branch.
2. Follow the existing commit style (`feat(scope): description`).
3. Run `cargo check --workspace` and `cargo test` before opening a PR.
4. Open a pull request against `main`.

---

## License

Community edition — [BSD 3-Clause](LICENSE)

Enterprise edition — commercial license (private repository)
