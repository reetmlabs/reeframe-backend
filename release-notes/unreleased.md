## Reeframe backend (unreleased)

### Breaking: relay renamed to restream

The RTSP relay is now called the restream, in the API, the code and the docs. Update clients with the frontend release that matches this one; the old routes and fields are gone.

| Before | After |
|---|---|
| `POST /cameras/{id}/relay/start` | `POST /cameras/{id}/restream/start` |
| `POST /cameras/{id}/relay/stop` | `POST /cameras/{id}/restream/stop` |
| `relay_url` in the start response | `restream_url` |
| `relay_url` on a camera (main stream) | `main_restream_url` |
| `sub_relay_url` on a camera | `sub_restream_url` |

The `rtsp.*` settings keep their names.

### What's new

- `rtsp.public_host` and `rtsp.public_port` set the host and port in restream URLs, so they work from the LAN when the daemon runs in Docker.
