# syntax=docker/dockerfile:1

# ============================================================
# Stage 1: builder
# ============================================================
FROM rust:1-bookworm AS builder

# GStreamer dev packages + pkg-config needed by the gstreamer-* crates
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        libgstreamer-plugins-bad1.0-dev \
        libgstreamer-rtsp-server-1.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ------------------------------------------------------------------
# Dependency-caching layer
# Copy manifests first, build a stub binary, then replace with real
# source.  This layer is only invalidated when Cargo.toml / Cargo.lock
# change, not on every source edit.
# ------------------------------------------------------------------
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -f target/release/deps/OneWardBackend* target/release/OneWardBackend

# Build the real binary
COPY src ./src
RUN cargo build --release

# ============================================================
# Stage 2: runtime
# ============================================================
FROM debian:bookworm-slim AS runtime

# GStreamer runtime + codec plugins
RUN apt-get update && apt-get install -y --no-install-recommends \
        # GStreamer core
        libgstreamer1.0-0 \
        libgstreamer-plugins-base1.0-0 \
        libgstreamer-plugins-bad1.0-0 \
        libgstreamer-rtsp-server-1.0-0 \
        # Codec plugins required for RTSP re-streaming & recording
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        # TLS + CA certs (SeaORM rustls, outbound RTSP over TLS)
        libssl3 \
        ca-certificates \
        # SQLite runtime library
        libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/OneWardBackend ./oneward-backend

RUN mkdir -p recordings

# HTTP API
EXPOSE 5800
# RTSP server
EXPOSE 8554

CMD ["./oneward-backend"]
