# Benchmark

How the resource numbers in the README were measured, and how to reproduce them.

## What is measured

Only the `vms-daemon` process, for a given number of cameras and features:

- **CPU:** user plus system time of the daemon over a 30 s window, as a share of one core.
- **Memory:** resident set size (RSS) at the end of the window.
- **Camera connections:** open TCP connections from the daemon to the cameras.

The fake cameras and the live-view clients run as separate processes and aren't counted.

## Test setup

- Intel Core i7-10750H (6 cores, 12 threads), 31 GB RAM
- Ubuntu 24.04.3 LTS, kernel 7.0, GStreamer 1.24.2, Rust 1.94
- Release build (`cargo build --release`)
- Each camera serves two H.264 streams over RTSP from `camsrv.py`: a 1920×1080 main stream at 4 Mbps and a 640×360 sub stream at 500 kbps. Both run at 25 fps with constant bitrate and a keyframe every 2 s.

The streams are video noise. Noise can't be compressed, so the encoder really sends the target bitrate; smoother test patterns compress far below it and understate the cost of moving the data. Noise is also the hardest content to decode, so features that decode video (thumbnails, motion detection) cost more here than on real footage.

## Steps

Each run starts a fresh daemon with an empty database. The daemon's API and RTSP relay listen on ports 28080 and 28554 and the fake cameras on 28556, so a daemon already running on the default ports isn't disturbed.

**Live view mode** steps through these states and lets each one settle before sampling:

1. Idle, with no cameras.
2. Live view: the relay started on every camera (the sub stream, by default), with one viewer each. The relay only sends video while someone watches, so without viewers live view would cost almost nothing.
3. Recording started on every camera.
4. Thumbnails turned on.
5. Motion detection turned on.

**Recording mode** samples idle, then recording on every camera, with no live view.

Run each mode for 1 camera and for 4:

```bash
cargo build --release
for mode in live rec; do
  for n in 1 4; do
    BIN=target/release/vms-daemon MODE=$mode ./bench.sh $n
  done
done
```

The first camera's cost is its 1-camera result minus idle. Each extra camera costs (4-camera result − 1-camera result) / 3.

Besides the build dependencies in the README, the scripts need `jq`, `bc`, and Python with GStreamer's RTSP server bindings (`python3-gi` and `gir1.2-gst-rtsp-server-1.0` on Debian and Ubuntu). If your default `python3` doesn't have them, point `PYTHON` at one that does, for example `PYTHON=/usr/bin/python3`.

## Results

| Mode | 1 camera | 4 cameras | Each extra camera |
|---|---|---|---|
| Idle, no cameras | 35 MB, 0% | 34 MB, 0% | |
| Recording | 44 MB, 1.9% | 54 MB, 7.5% | +3.4 MB, +1.9% |
| Live view | 34 MB, 1.0% | 45 MB, 4.4% | +3.7 MB, +1.1% |
| Live view and recording | 42 MB, 2.1% | 59 MB, 10.4% | +5.8 MB, +2.8% |
| + thumbnails | 112 MB, 2.1% | 150 MB, 10.2% | +12.5 MB, +2.7% |
| + motion detection | 146 MB, 3.2% | 198 MB, 19.4% | +17 MB, +5.4% |

Every camera recorded in every run, and no run logged an error. Each camera held one connection until thumbnails or motion detection needed its sub stream. Recording wrote about 3.5 Mbps per camera, which confirms the streams arrived at the expected bitrate.

Most of the jump when the first camera turns on thumbnails or motion detection is the decoders loading once. Expect about ±0.5% CPU variation between runs.

## Scripts

`camsrv.py`, the fake cameras:

```python
#!/usr/bin/env python3
"""Fake RTSP cameras: N x (1080p main + 360p sub), H.264 4:2:0, constant bitrate.

Usage: camsrv.py N [port]   ->  rtsp://HOST:PORT/high1, /low1, ... /highN, /lowN
"""
import sys

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstRtspServer", "1.0")
from gi.repository import GLib, Gst, GstRtspServer

Gst.init(None)
n = int(sys.argv[1])
port = sys.argv[2] if len(sys.argv) > 2 else "8556"

# Video noise can't be compressed, so CBR actually reaches the target bitrate.
def launch(width, height, kbps):
    return (
        f"( videotestsrc is-live=true pattern=snow "
        f"! video/x-raw,width={width},height={height},framerate=25/1,format=I420 "
        f"! x264enc tune=zerolatency speed-preset=ultrafast pass=cbr bitrate={kbps} "
        f"vbv-buf-capacity=1000 key-int-max=50 bframes=0 "
        f"! video/x-h264,profile=main ! rtph264pay name=pay0 pt=96 config-interval=-1 )"
    )

server = GstRtspServer.RTSPServer()
server.set_service(port)
mounts = server.get_mount_points()
for i in range(1, n + 1):
    for name, (w, h, kbps) in {"high": (1920, 1080, 4000), "low": (640, 360, 500)}.items():
        factory = GstRtspServer.RTSPMediaFactory()
        factory.set_launch(launch(w, h, kbps))
        factory.set_shared(True)
        mounts.add_factory(f"/{name}{i}", factory)
server.attach(None)
print(f"serving {n} cameras on :{port}", flush=True)
GLib.MainLoop().run()
```

`bench.sh`, the measurement. Put it next to `camsrv.py`:

```bash
#!/usr/bin/env bash
# Measure vms-daemon CPU and memory for N cameras.
#   MODE=live: idle, live view, + recording, + thumbnails, + motion
#   MODE=rec:  idle, recording only
# Usage: BIN=target/release/vms-daemon MODE=live ./bench.sh 4
set -u
BIN=${BIN:?path to vms-daemon}
MODE=${MODE:-live}
N=${1:?camera count}
WINDOW=${WINDOW:-30}                   # seconds each sample averages over
PYTHON=${PYTHON:-python3}               # needs PyGObject (python3-gi) and GstRtspServer
API=http://127.0.0.1:28080
CAMS=127.0.0.1:28556
DIR=$(mktemp -d)
TICK=$(getconf CLK_TCK)
VIEWERS=()

cleanup() { kill "${VIEWERS[@]}" "$CAM_PID" "$DAEMON_PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$DIR"; }
trap cleanup EXIT

if ss -ltn | grep -qE ":(28080|28554|28556) "; then echo "a benchmark port is already in use"; exit 1; fi
"$PYTHON" "$(dirname "$0")/camsrv.py" "$N" 28556 > "$DIR/camsrv.log" 2>&1 &
CAM_PID=$!
sleep 2
kill -0 "$CAM_PID" 2>/dev/null || { cat "$DIR/camsrv.log"; exit 1; }
for i in $(seq 1 "$N"); do
  gst-discoverer-1.0 -t 8 "rtsp://$CAMS/high$i" | grep -q "video #" || { echo "camera $i not serving"; exit 1; }
done

VMS_ENCRYPTION_KEY=$(openssl rand -base64 32) \
VMS_AUTH__JWT_SECRET=$(openssl rand -base64 32) \
VMS_DATABASE__URL="sqlite://$DIR/bench.db?mode=rwc" \
VMS_MEDIA__RECORDING_DIR="$DIR/rec" \
VMS_API__BIND=127.0.0.1:28080 \
VMS_RTSP__BIND=127.0.0.1:28554 \
RUST_LOG=warn "$BIN" > "$DIR/daemon.log" 2>&1 &
DAEMON_PID=$!
until curl -sf "$API/health" > /dev/null; do sleep 0.5; done
sleep 5

# CPU is utime + stime of the daemon over the window, as a share of one core.
sample() {
  local t0 t1
  t0=$(awk '{print $14 + $15}' /proc/$DAEMON_PID/stat); sleep "$WINDOW"
  t1=$(awk '{print $14 + $15}' /proc/$DAEMON_PID/stat)
  printf "%-20s RSS %4d MB  CPU %5.1f%%  camera connections %s\n" "$1" \
    $(( $(awk '/VmRSS/{print $2}' /proc/$DAEMON_PID/status) / 1024 )) \
    "$(echo "($t1 - $t0) * 100 / $TICK / $WINDOW" | bc -l)" \
    "$(ss -tnp | grep "pid=$DAEMON_PID," | grep -c ":28556 ")"
}
api() { curl -sf -X "$1" "$API$2" -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' ${3:+-d "$3"}; }
each() { for id in "${IDS[@]}"; do api "$1" "${2//ID/$id}" ${3:+"$3"} > /dev/null; done; }

sample "idle"

curl -sf -X POST "$API/auth/setup" -H 'content-type: application/json' -d '{"username":"bench","password":"benchbench"}' > /dev/null
TOKEN=$(curl -sf -X POST "$API/auth/login" -H 'content-type: application/json' \
  -d '{"username":"bench","password":"benchbench"}' | jq -r .access_token)
IDS=()
for i in $(seq 1 "$N"); do
  IDS+=("$(api POST /cameras "{\"name\":\"cam$i\",\"rtsp_url\":\"rtsp://$CAMS/high$i\",\"sub_rtsp_url\":\"rtsp://$CAMS/low$i\"}" | jq -r .id)")
done

if [ "$MODE" = rec ]; then
  each POST /cameras/ID/recording/start; sleep 20; sample "recording"
  exit
fi

# Live view only costs anything while someone watches, so attach one viewer per camera.
for id in "${IDS[@]}"; do
  url=$(api POST "/cameras/$id/relay/start" | jq -r .relay_url)
  gst-launch-1.0 -q rtspsrc location="$url" latency=200 ! fakesink sync=false > /dev/null 2>&1 &
  VIEWERS+=($!)
done
sleep 15; sample "live view"
each POST /cameras/ID/recording/start;                            sleep 20; sample "+ recording"
each PATCH /cameras/ID '{"thumbnails_enabled":true}';             sleep 35; sample "+ thumbnails"
each PATCH /cameras/ID '{"motion_detection_enabled":true}';       sleep 15; sample "+ motion"
```
