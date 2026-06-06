# syntax=docker/dockerfile:1
#
# Multi-stage build for vms-daemon.
#
# cargo-chef is used for workspace-aware dependency caching: the compiled
# dependency layer is only invalidated when Cargo.lock or a Cargo.toml
# changes, not on every source edit.

# ── Stage 1: chef base ────────────────────────────────────────────────────────
# Rust toolchain + GStreamer dev libraries + cargo-chef.
# Shared by both the planner and builder stages so apt installs are cached once.
FROM rust:1-bookworm AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked

WORKDIR /build

# ── Stage 2: planner ──────────────────────────────────────────────────────────
# Computes the dependency recipe for the workspace.
# Runs fast — no compilation happens here.
FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: builder ──────────────────────────────────────────────────────────
FROM chef AS builder

# Cook workspace dependencies first.
# This layer is only re-run when recipe.json changes (i.e. Cargo.lock /
# Cargo.toml changed) — source-only edits skip straight to the next RUN.
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --bin vms-daemon --recipe-path recipe.json

# Build the real binary.
COPY . .
RUN cargo build --release --bin vms-daemon

# ── Stage 4: runtime ──────────────────────────────────────────────────────────
# Minimal Debian image with only the GStreamer runtime plugins needed to
# receive RTSP streams and write chunked MP4 recordings.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        # GStreamer core runtime
        libgstreamer1.0-0 \
        libgstreamer-plugins-base1.0-0 \
        # Plugin sets required for RTSP receive + MP4 recording
        # good:  rtspsrc, rtph26{4,5}depay, rtpjpegdepay, splitmuxsink, mp4mux
        gstreamer1.0-plugins-good \
        # bad:   h264parse, h265parse, rtpav1depay, av1parse
        gstreamer1.0-plugins-bad \
        # libav: broad codec decoding fallback (avdec_*)
        gstreamer1.0-libav \
        # TLS + CA trust store (RTSPS, outbound HTTPS transports)
        libssl3 \
        ca-certificates \
        # SQLite shared library (sqlx-sqlite may bundle its own; kept as fallback)
        libsqlite3-0 \
        # Used by the HEALTHCHECK instruction below
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root service account
RUN groupadd --system reeframe \
    && useradd --system --gid reeframe --no-create-home --shell /usr/sbin/nologin reeframe

# Persistent data directory (recordings + SQLite DB)
RUN mkdir -p /var/lib/reeframe/recordings \
    && chown -R reeframe:reeframe /var/lib/reeframe

COPY --from=builder /build/target/release/vms-daemon /usr/local/bin/vms-daemon

USER reeframe

# Default runtime configuration — all values can be overridden via environment
# variables or a mounted config file at /etc/reeframe/config.toml.
ENV VMS_DATABASE__URL="sqlite:///var/lib/reeframe/reeframe.db" \
    VMS_API__BIND="0.0.0.0:8080" \
    RUST_LOG="info"

# /var/lib/reeframe holds both the SQLite database and the recordings directory.
# Mount a named volume here so data survives container restarts.
VOLUME ["/var/lib/reeframe"]

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/vms-daemon"]

# Docker / container orchestrators poll this to determine whether the daemon
# is alive and serving traffic.  start_period gives the process time to run
# migrations and bind the socket before the first check fires.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
