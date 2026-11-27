# CI runner requirements

Pushes to main and pull requests from branches in this repo run the `lint` and
`test` jobs of `.github/workflows/ci.yml` on a self-hosted runner with the
labels `self-hosted`, `linux`, `x64` and `wsl`. Pull requests from forks run on
GitHub-hosted `ubuntu-24.04` runners instead.

The runner user has no sudo, so CI skips every setup step that needs root on
the self-hosted runner. The runner image must already contain everything below.
When the workflow changes one of these, update this file in the same pull
request.

## Operating system

Ubuntu 24.04 (x86_64). The gst-libav plugin path below assumes the Debian
multiarch layout of that release.

## Apt packages

```sh
apt-get update
apt-get install -y --no-install-recommends \
  git curl ca-certificates xz-utils build-essential \
  pkg-config \
  libgstreamer1.0-dev \
  libgstreamer-plugins-base1.0-dev \
  libgstreamer-plugins-bad1.0-dev \
  libgstrtspserver-1.0-dev \
  gstreamer1.0-plugins-good \
  gstreamer1.0-x \
  gstreamer1.0-plugins-bad \
  gstreamer1.0-plugins-ugly \
  gstreamer1.0-libav \
  gstreamer1.0-nice \
  gstreamer1.0-tools \
  meson ninja-build \
  libavcodec-dev libavformat-dev libavutil-dev libavfilter-dev
```

The first line (git, curl, a C toolchain) is what a GitHub-hosted image has by
default; the rest matches the workflow.

## gst-libav 1.24.2

Ubuntu 24.04 ships gst-libav 1.24.1, which leaks memory per decoded frame and
fails the daemon's startup check. Build 1.24.2 and put it in place of the
packaged plugin, as root:

```sh
curl -sSfL https://gstreamer.freedesktop.org/src/gst-libav/gst-libav-1.24.2.tar.xz | tar xJ -C /tmp
cd /tmp/gst-libav-1.24.2
meson setup build --buildtype=release -Ddoc=disabled -Dtests=disabled
ninja -C build

PLUGINS=/usr/lib/x86_64-linux-gnu/gstreamer-1.0
dpkg-divert --divert $PLUGINS/libgstlibav.so.distrib --rename --add $PLUGINS/libgstlibav.so
install -m644 build/ext/libav/libgstlibav.so $PLUGINS/
```

Check it as the runner user (clear its GStreamer registry cache first):

```sh
rm -rf ~/.cache/gstreamer-1.0
gst-inspect-1.0 libav | grep -E 'Version +1\.24\.2$'
```

## Rust

Install rustup for the runner user (no root needed). CI calls
`dtolnay/rust-toolchain@stable` on every run, which updates to the latest
stable toolchain through rustup, with these components:

- rustfmt
- clippy

## cargo-deny

cargo-deny 0.22.2, on the runner user's `PATH`. The version is pinned as
`CARGO_DENY_VERSION` at the top of `ci.yml`; keep the two the same.

```sh
cargo install --locked cargo-deny --version 0.22.2
```

## Other

- Network access to github.com, crates.io, static.rust-lang.org and the GitHub
  Actions cache service (used by `Swatinem/rust-cache`).
- Docker is not needed.
