#!/bin/bash
# Builds the gst-libav plugin that the Ubuntu 24.04 package ships, against the
# distro FFmpeg and GStreamer. Run as root on a clean Ubuntu 24.04; it writes
# libgstlibav.so to the directory given as the first argument.
set -euo pipefail

VERSION=1.24.2
SHA256=962838648e14ce8a78322ab16f84c7d84d86a6d7bebb6574ac05dea84a7c7c9b
OUT=$(realpath -m "${1:?usage: build-gst-libav.sh OUTPUT_DIR}")

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends ca-certificates curl xz-utils \
    build-essential meson ninja-build pkg-config \
    libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
    libavcodec-dev libavformat-dev libavutil-dev libavfilter-dev >/dev/null

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"
curl -sSfLo gst-libav.tar.xz "https://gstreamer.freedesktop.org/src/gst-libav/gst-libav-${VERSION}.tar.xz"
echo "${SHA256}  gst-libav.tar.xz" | sha256sum -c --quiet
tar xJf gst-libav.tar.xz
cd "gst-libav-${VERSION}"
meson setup build --buildtype=release -Ddoc=disabled -Dtests=disabled >/dev/null
ninja -C build >/dev/null
install -D -m 0644 build/ext/libav/libgstlibav.so "$OUT/libgstlibav.so"
