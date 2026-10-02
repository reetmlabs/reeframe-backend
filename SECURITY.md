# Security

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Don't open a public issue for a security problem.

Include what you found, how to reproduce it, and the version or commit you tested. Reports are reviewed and fixed on the latest release.

## Supported versions

Reeframe is pre-1.0. Security fixes go into the latest release only.

## Deploying safely

Some defaults assume a trusted local network. Before exposing a Reeframe backend beyond it:

- **The API is plain HTTP.** It has no TLS of its own. For access from outside your network, put it behind a reverse proxy that terminates TLS, or a VPN.
- **The RTSP relay has no authentication.** Anyone who can reach port 8554 and knows a camera's ID can watch it. Camera IDs are random UUIDs but are not secrets: they appear in API responses and logs. Keep 8554 on your local network or behind a firewall or VPN.
- **`/health`, `/health/ready` and `/metrics` need no login.** Metrics reveal operational details such as camera and pipeline counts. Restrict them at the proxy if that matters to you.
- **Inbound webhooks (`POST /webhooks/{id}`) are open by default.** Set a shared secret on the webhook source; callers must then send it in the `X-Webhook-Secret` header.
- **Protect the two secrets.** `VMS_ENCRYPTION_KEY` decrypts stored camera passwords and `VMS_AUTH__JWT_SECRET` signs login tokens. Keep them in a file only root can read (the Debian package uses `/etc/reeframe/env` with mode 600), and never commit them.
- **The first user becomes the admin.** `POST /auth/setup` works only until the first account exists, so create it right after installing.
