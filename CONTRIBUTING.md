# Contributing

Thanks for helping. Bug reports, fixes and small focused features are all welcome.

## Setup

Install the build dependencies listed in the README, then:

```bash
cargo build
cargo test --all-targets
```

The GStreamer tests need the `good`, `bad`, `ugly` and `libav` plugin sets installed.

To try the daemon without real cameras, `test_camera_server.py` serves test RTSP streams from a video file:

```bash
python3 test_camera_server.py sample.mp4    # rtsp://localhost:8556/high and /low
```

## Before opening a pull request

CI runs these, and all of them must pass:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

- Keep a pull request to one change. Put a fix and its regression test in separate commits.
- Commit messages are one imperative line, for example `Retry a failed codec link on first connect`.
- Add a test that would have caught the bug when the existing test setup makes that practical.
- Comment the non-obvious "why", not what the code already says.

## Reporting bugs

Open an issue with what you expected, what happened, and the relevant daemon log lines (`RUST_LOG=debug` helps). Include the camera make and model for stream problems.

## License

By contributing you agree that your contributions are licensed under the project's [BSD-3-Clause license](LICENSE).
