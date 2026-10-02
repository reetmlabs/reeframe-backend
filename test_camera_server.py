#!/usr/bin/env python3
"""
test_camera_server.py: Configurable multi-stream RTSP test server.

Two operating modes:

  FILE / V4L2 MODE : transcode a local file or USB camera at multiple
                      resolutions and serve N feeds per resolution.

  IP CAMERA MODE   : connect to an IP camera's own RTSP streams and
                      re-serve each as N independent feeds.  The camera
                      already provides the desired resolutions; this server
                      just forwards (decode → re-encode) them.

Usage:
  # File / V4L2 mode (SOURCE required)
  python3 test_camera_server.py [--port PORT] [--profile NAME:WxH:KBPS[:N]]... SOURCE

  # IP camera mode (no SOURCE: use --stream instead)
  python3 test_camera_server.py [--port PORT] --stream NAME:RTSP_URL[:KBPS[:N]]...

SOURCE:
  A video file path (MP4, MKV, …) or a V4L2 device (e.g. /dev/video0).

OPTIONS:
  --port PORT
      RTSP server port (default: 8556).

  --profile NAME:WxH:KBPS[:N]
      (File / V4L2 mode) Named encode profile.  Repeatable.
        NAME: mount-point prefix           e.g. "low", "high"
        WxH : output resolution            e.g. 640x360
        KBPS: x264 target bitrate (kbps)   e.g. 1000
        N   : number of feeds (default 1)
      N>1 → /NAME1 … /NAMEN.  N=1 → /NAME.
      Default: low:640x360:1000  high:1920x1080:4000

  --stream NAME:RTSP_URL[:KBPS[:N]]
      (IP camera mode) Re-stream an IP camera's own RTSP URL.  Repeatable.
        NAME    : mount-point prefix           e.g. "low", "high"
        RTSP_URL: source URL from the camera   e.g. rtsp://192.168.1.10/stream2
        KBPS    : re-encode bitrate (default 4000)
        N       : number of feeds (default 1)
      The camera provides the resolution; no scaling is applied.

Examples:
  # Default dual-stream from a file
  python3 test_camera_server.py sample.mp4

  # 3 low + 3 high feeds from a file
  python3 test_camera_server.py \\
      --profile low:640x360:1000:3 --profile high:1920x1080:4000:3 sample.mp4

  # V4L2 USB camera, one 720p feed
  python3 test_camera_server.py --profile hd:1280x720:2000 /dev/video0

  # IP camera: re-serve its high & low streams, 3 feeds each
  python3 test_camera_server.py \\
      --stream high:rtsp://192.168.1.10/stream1:4000:3 \\
      --stream low:rtsp://192.168.1.10/stream2:1000:3

Dependencies (Ubuntu/Debian):
  sudo apt install python3-gi gir1.2-gst-rtsp-server-1.0 \\
       gstreamer1.0-plugins-good gstreamer1.0-plugins-bad \\
       gstreamer1.0-plugins-ugly gstreamer1.0-libav
"""

import os
import sys
import argparse

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstRtspServer", "1.0")

from gi.repository import GLib, Gst, GstRtspServer  # noqa: E402


# ---------------------------------------------------------------------------
# Argument parsing helpers
# ---------------------------------------------------------------------------

DEFAULT_PROFILES = [
    ("low",  640,  360, 1000, 1),
    ("high", 1920, 1080, 4000, 1),
]


def _parse_profile(s: str) -> tuple:
    """Parse ``NAME:WxH:KBPS[:N]`` → ``(name, width, height, bitrate, count)``."""
    parts = s.split(":")
    if len(parts) not in (3, 4):
        raise argparse.ArgumentTypeError(
            f"invalid profile {s!r}: expected NAME:WxH:KBPS[:N]"
        )
    name = parts[0]
    try:
        w_str, h_str = parts[1].lower().split("x")
        width, height = int(w_str), int(h_str)
    except (ValueError, AttributeError):
        raise argparse.ArgumentTypeError(
            f"invalid resolution in {s!r}: expected WIDTHxHEIGHT (e.g. 640x360)"
        )
    try:
        bitrate = int(parts[2])
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid bitrate in {s!r}")
    count = 1
    if len(parts) == 4:
        try:
            count = int(parts[3])
            if count < 1:
                raise ValueError
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"invalid count in {s!r}: must be a positive integer"
            )
    return (name, width, height, bitrate, count)


def _parse_stream(s: str) -> tuple:
    """
    Parse ``NAME:RTSP_URL[:KBPS[:N]]`` → ``(name, url, bitrate, count)``.

    The RTSP URL itself contains colons (``rtsp://host:port/path``), so plain
    splitting is unreliable.  Instead we strip integer-only tokens from the
    *right* of the colon-split list (count and bitrate) and
    reassemble what remains as the URL.
    """
    colon = s.index(":")          # guaranteed to exist (argparse validates)
    name = s[:colon]
    parts = s[colon + 1:].split(":")

    # Strip trailing integer tokens: optional N, then optional KBPS.
    count = 1
    bitrate = 4000
    if parts and parts[-1].isdigit():
        count = int(parts.pop())
    if parts and parts[-1].isdigit():
        bitrate = int(parts.pop())

    url = ":".join(parts)
    if not url.startswith(("rtsp://", "rtsps://")):
        raise argparse.ArgumentTypeError(
            f"invalid stream {s!r}: URL must start with rtsp:// or rtsps://"
        )
    if count < 1:
        raise argparse.ArgumentTypeError(
            f"invalid count in {s!r}: must be a positive integer"
        )
    return (name, url, bitrate, count)


# ---------------------------------------------------------------------------
# GStreamer pipeline builders
# ---------------------------------------------------------------------------

def _launch_from_file_or_v4l2(
    source: str, width: int, height: int, bitrate: int
) -> str:
    """
    Pipeline: local file or V4L2 device → scale to WxH → x264enc → rtph264pay.
    """
    if source.startswith("/dev/"):
        src = f"v4l2src device={source} ! videoconvert ! videoscale"
    else:
        src = (
            f"filesrc location={source} ! decodebin name=d "
            f"d. ! queue ! videoconvert ! videoscale"
        )
    return (
        f"( {src} "
        f"! video/x-raw,width={width},height={height} "
        f"! x264enc tune=zerolatency bitrate={bitrate} speed-preset=ultrafast "
        f"! rtph264pay name=pay0 pt=96 )"
    )


def _launch_from_ip_camera(url: str, bitrate: int) -> str:
    """
    Pipeline: IP camera RTSP URL → decode → x264enc → rtph264pay.

    No scaling: the camera provides the stream at the desired resolution.
    latency=100 gives rtspsrc a small jitter buffer without adding noticeable
    delay. The media=video filter is needed because decodebin has only one
    sink pad: without it, a camera's audio pad loses the auto-link race and
    ends up unlinked, which GStreamer treats as a fatal error that kills
    the whole pipeline.
    """
    return (
        f"( rtspsrc location={url} latency=100 name=src "
        f"src. ! application/x-rtp,media=video ! decodebin name=d "
        f"d. ! queue ! videoconvert "
        f"! x264enc tune=zerolatency bitrate={bitrate} speed-preset=ultrafast "
        f"! rtph264pay name=pay0 pt=96 )"
    )


def _make_factory(
    launch_str: str,
    loop_on_eos: bool = False,
    eos_label: str = "",
) -> GstRtspServer.RTSPMediaFactory:
    """
    Create a shared RTSP media factory from a gst-launch pipeline string.

    ``loop_on_eos=True`` installs a bus handler that seeks back to position 0
    when the pipeline signals EOS: used for looping file sources.
    """
    factory = GstRtspServer.RTSPMediaFactory()
    factory.set_launch(launch_str)
    factory.set_shared(True)

    if loop_on_eos:
        def on_media_configure(
            _factory: GstRtspServer.RTSPMediaFactory,
            media: GstRtspServer.RTSPMedia,
        ) -> None:
            pipeline: Gst.Element = media.get_element()
            bus: Gst.Bus = pipeline.get_bus()
            bus.add_signal_watch()

            def on_bus_message(_bus: Gst.Bus, message: Gst.Message) -> None:
                if message.type == Gst.MessageType.EOS:
                    print(f"[loop] EOS on {eos_label}: seeking to start")
                    pipeline.seek_simple(
                        Gst.Format.TIME,
                        Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT,
                        0,
                    )

            bus.connect("message", on_bus_message)

        factory.connect("media-configure", on_media_configure)

    return factory


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="test_camera_server.py",
        description="Configurable multi-stream RTSP test server (file, V4L2, or IP camera).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # file, default profiles\n"
            "  python3 test_camera_server.py sample.mp4\n\n"
            "  # file, 3 low + 3 high feeds\n"
            "  python3 test_camera_server.py \\\n"
            "      --profile low:640x360:1000:3 --profile high:1920x1080:4000:3 sample.mp4\n\n"
            "  # V4L2 camera, 720p\n"
            "  python3 test_camera_server.py --profile hd:1280x720:2000 /dev/video0\n\n"
            "  # IP camera, re-serve its own high + low streams, 3 feeds each\n"
            "  python3 test_camera_server.py \\\n"
            "      --stream high:rtsp://192.168.1.10/stream1:4000:3 \\\n"
            "      --stream low:rtsp://192.168.1.10/stream2:1000:3\n"
        ),
    )
    parser.add_argument(
        "source", nargs="?",
        help="Video file (MP4/MKV/…) or V4L2 device (/dev/video0). Not used with --stream.",
    )
    parser.add_argument(
        "--port", default="8556",
        help="RTSP server port (default: 8556)",
    )
    parser.add_argument(
        "--profile",
        metavar="NAME:WxH:KBPS[:N]",
        type=_parse_profile,
        action="append",
        dest="profiles",
        help="Encode profile for file/V4L2 mode. Repeatable.",
    )
    parser.add_argument(
        "--stream",
        metavar="NAME:RTSP_URL[:KBPS[:N]]",
        type=_parse_stream,
        action="append",
        dest="streams",
        help="IP camera re-stream. Repeatable.",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Validate mode
    # ------------------------------------------------------------------
    if args.streams and args.source:
        parser.error("--stream and SOURCE are mutually exclusive")

    if not args.streams and not args.source:
        parser.error("provide SOURCE (file / V4L2) or at least one --stream (IP camera)")

    Gst.init(None)

    server = GstRtspServer.RTSPServer()
    server.set_service(args.port)
    mounts: GstRtspServer.RTSPMountPoints = server.get_mount_points()

    # (path, description): collected for the info banner
    registered: list[tuple[str, str]] = []

    # ------------------------------------------------------------------
    # IP camera mode
    # ------------------------------------------------------------------
    if args.streams:
        for name, url, bitrate, count in args.streams:
            for i in range(1, count + 1):
                path = f"/{name}" if count == 1 else f"/{name}{i}"
                launch = _launch_from_ip_camera(url, bitrate)
                mounts.add_factory(path, _make_factory(launch))
                registered.append((path, f"→ {url}  {bitrate} kbps"))

    # ------------------------------------------------------------------
    # File / V4L2 mode
    # ------------------------------------------------------------------
    else:
        source = args.source
        is_v4l2 = source.startswith("/dev/")
        if is_v4l2:
            if not os.path.exists(source):
                print(f"Error: device not found: {source}", file=sys.stderr)
                sys.exit(1)
        else:
            source = os.path.abspath(source)
            if not os.path.isfile(source):
                print(f"Error: file not found: {source}", file=sys.stderr)
                sys.exit(1)

        profiles = args.profiles or DEFAULT_PROFILES
        for name, width, height, bitrate, count in profiles:
            for i in range(1, count + 1):
                path = f"/{name}" if count == 1 else f"/{name}{i}"
                launch = _launch_from_file_or_v4l2(source, width, height, bitrate)
                loop = not is_v4l2
                factory = _make_factory(
                    launch,
                    loop_on_eos=loop,
                    eos_label=os.path.basename(source),
                )
                mounts.add_factory(path, factory)
                registered.append((path, f"{width}×{height}  {bitrate} kbps"))

    server.attach(None)

    # ------------------------------------------------------------------
    # Info banner
    # ------------------------------------------------------------------
    host = "127.0.0.1"
    print("=" * 60)
    print("  Test camera RTSP server started")
    print("=" * 60)
    if args.streams:
        print("  Mode   : IP camera re-stream")
    else:
        src_label = args.source if args.source.startswith("/dev/") else os.path.abspath(args.source)
        print(f"  Source : {src_label}")
    print(f"  Port   : {args.port}")
    print()
    print("  RTSP streams:")
    for path, desc in registered:
        print(f"    rtsp://{host}:{args.port}{path:<12}  {desc}")
    print()
    if registered:
        sample = f"rtsp://{host}:{args.port}{registered[0][0]}"
        print("  Quick-play check (run in another terminal):")
        print(f"    gst-launch-1.0 rtspsrc location={sample}")
        print( "                   ! decodebin ! autovideosink")
        print()
    print("  Press Ctrl-C to stop.")
    print("=" * 60)

    loop = GLib.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        print("\nStopping test camera server.")
        loop.quit()


if __name__ == "__main__":
    main()
