//! The control channel client: one QUIC connection to the Coordinator,
//! authenticated with the site key, kept alive with pings and opened again
//! whenever it is lost.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::SigningKey;
use futures::stream::{FuturesUnordered, StreamExt};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, ConnectionError, Endpoint};
use rand::seq::SliceRandom;
use reeframe_protocol::{read_message, write_message, CloseReason, Message, ALPN};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;
use tokio::time::{interval_at, sleep, sleep_until, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::reconnect::{jittered, Backoff};

/// Where the control channel connects to. `host` is also the TLS server name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub host: String,
    pub port: u16,
}

impl Target {
    /// Parses `control_addr` (`host:port`). An empty one means the host of
    /// `coordinator_url` on port 443.
    pub(crate) fn resolve(control_addr: &str, coordinator_url: &str) -> anyhow::Result<Self> {
        if control_addr.is_empty() {
            let url = reqwest::Url::parse(coordinator_url)
                .with_context(|| format!("coordinator URL {coordinator_url:?} is not a URL"))?;
            let host = url
                .host_str()
                .with_context(|| format!("coordinator URL {coordinator_url:?} has no host"))?;
            return Ok(Self {
                host: unbracket(host).to_owned(),
                port: 443,
            });
        }
        let (host, port) = control_addr
            .rsplit_once(':')
            .with_context(|| format!("control_addr {control_addr:?} is not host:port"))?;
        let port = port
            .parse()
            .with_context(|| format!("control_addr {control_addr:?} has an invalid port"))?;
        anyhow::ensure!(
            !host.is_empty(),
            "control_addr {control_addr:?} has no host"
        );
        Ok(Self {
            host: unbracket(host).to_owned(),
            port,
        })
    }
}

/// IPv6 literals are written `[::1]` in URLs and addresses, but resolved and
/// used as TLS server names without the brackets.
fn unbracket(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Timing of the client. The Coordinator closes a connection after 45 s
/// without a message, so pings go out well within that.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timings {
    pub ping_interval: Duration,
    /// Silence after which the connection counts as dead.
    pub silence_timeout: Duration,
    /// Gap between connection attempts to the next address of the host.
    pub race_stagger: Duration,
    /// Limit for resolving and connecting to all addresses.
    pub connect_timeout: Duration,
    pub retry_base: Duration,
    pub retry_max: Duration,
    /// Uptime after which a lost connection counts as recovered, so the
    /// backoff starts again at `retry_base`.
    pub stable_uptime: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(15),
            silence_timeout: Duration::from_secs(45),
            race_stagger: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(10),
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            stable_uptime: Duration::from_secs(30),
        }
    }
}

/// The trusted roots for the Coordinator's certificate: the Mozilla set, so
/// the result does not depend on the host's CA store.
pub(crate) fn web_pki_roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    roots
}

/// A control channel client, ready to [`run`](Self::run).
pub(crate) struct ControlClient {
    endpoint: Endpoint,
    target: Target,
    timings: Timings,
}

impl ControlClient {
    pub(crate) fn new(
        key: &SigningKey,
        target: Target,
        roots: RootCertStore,
        timings: Timings,
    ) -> anyhow::Result<Self> {
        let mut endpoint = client_endpoint()?;
        endpoint.set_default_client_config(client_config(key, roots)?);
        Ok(Self {
            endpoint,
            target,
            timings,
        })
    }

    /// Keeps the control channel up until `shutdown` is cancelled, then
    /// closes it.
    pub(crate) async fn run(self, shutdown: CancellationToken) {
        let mut backoff = Backoff::new(
            self.timings.retry_base,
            self.timings.retry_max,
            self.timings.stable_uptime,
        );
        loop {
            let connected = tokio::select! {
                _ = shutdown.cancelled() => break,
                connected = self.connect() => connected,
            };
            let end = match connected {
                Ok((connection, addr)) => {
                    backoff.record_connected();
                    tracing::info!(host = %self.target.host, %addr, "Control channel connected");
                    session(&connection, &self.timings, &shutdown).await
                }
                Err(e) => SessionEnd::ConnectFailed(e),
            };
            if matches!(end, SessionEnd::Shutdown) {
                break;
            }
            let delay = retry_delay(&end, &mut backoff, self.timings.retry_max);
            end.log(delay);
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(delay) => {}
            }
        }
        self.endpoint
            .close(CloseReason::Closed.code().into(), b"shutdown");
        // Gives the close frame a moment to reach the Coordinator.
        let _ = timeout(Duration::from_secs(1), self.endpoint.wait_idle()).await;
        tracing::info!("Control channel stopped");
    }

    async fn connect(&self) -> anyhow::Result<(Connection, SocketAddr)> {
        let attempt = async {
            let mut addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((self.target.host.as_str(), self.target.port))
                    .await
                    .with_context(|| format!("cannot resolve {}", self.target.host))?
                    .collect();
            addrs.shuffle(&mut rand::thread_rng());
            race(
                &self.endpoint,
                addrs,
                &self.target.host,
                self.timings.race_stagger,
            )
            .await
        };
        timeout(self.timings.connect_timeout, attempt)
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "no address of {} answered in time",
                    self.target.host
                ))
            })
    }
}

/// A UDP endpoint for both address families where the host has IPv6, so
/// racing can try every resolved address.
fn client_endpoint() -> anyhow::Result<Endpoint> {
    Endpoint::client((std::net::Ipv6Addr::UNSPECIFIED, 0).into())
        .or_else(|_| Endpoint::client((std::net::Ipv4Addr::UNSPECIFIED, 0).into()))
        .context("cannot open a UDP socket for the control channel")
}

/// TLS 1.3 with the site key as client certificate. The Coordinator only
/// looks at the key, so the certificate is self-signed and its names do not
/// matter.
fn client_config(key: &SigningKey, roots: RootCertStore) -> anyhow::Result<quinn::ClientConfig> {
    let pkcs8 = key.to_pkcs8_der()?;
    let key_pair = rcgen::KeyPair::try_from(pkcs8.as_bytes())?;
    let cert =
        rcgen::CertificateParams::new(vec!["reeframe-backend".into()])?.self_signed(&key_pair)?;
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8.as_bytes().to_vec()));

    // Chosen explicitly because the dependency tree also enables aws-lc-rs,
    // and rustls refuses to guess between two installed providers.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![CertificateDer::from(cert.der().to_vec())], private_key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.enable_early_data = false;
    Ok(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)?,
    )))
}

/// Connects to the first address that completes a handshake. Attempts start
/// `stagger` apart, or at once when the previous one failed, so an address
/// that never answers costs only `stagger`. Losing attempts are dropped,
/// which closes them.
async fn race(
    endpoint: &Endpoint,
    addrs: Vec<SocketAddr>,
    server_name: &str,
    stagger: Duration,
) -> anyhow::Result<(Connection, SocketAddr)> {
    let attempt = |addr: SocketAddr| async move {
        let connecting = endpoint
            .connect(addr, server_name)
            .with_context(|| format!("cannot connect to {addr}"))?;
        let connection = connecting
            .await
            .with_context(|| format!("handshake with {addr} failed"))?;
        anyhow::Ok((connection, addr))
    };

    let mut addrs = addrs.into_iter();
    let mut attempts = FuturesUnordered::new();
    let mut last_error = anyhow::anyhow!("{server_name} resolved to no address");
    loop {
        if attempts.is_empty() {
            match addrs.next() {
                Some(addr) => attempts.push(attempt(addr)),
                None => return Err(last_error),
            }
        }
        let more = addrs.len() > 0;
        tokio::select! {
            result = attempts.next() => match result {
                Some(Ok(won)) => return Ok(won),
                Some(Err(e)) => last_error = e,
                None => {}
            },
            _ = sleep(stagger), if more => {
                if let Some(addr) = addrs.next() {
                    attempts.push(attempt(addr));
                }
            }
        }
    }
}

/// Why a connection attempt or session ended.
#[derive(Debug)]
enum SessionEnd {
    Shutdown,
    ConnectFailed(anyhow::Error),
    /// The Coordinator closed the connection with this application code.
    ClosedByServer(u32),
    /// The connection broke for any other reason.
    Lost(String),
    /// Nothing arrived within the silence timeout.
    Silent,
}

impl SessionEnd {
    fn log(&self, delay: Duration) {
        let retry_in_secs = delay.as_secs_f64();
        match self {
            Self::ClosedByServer(code) => match CloseReason::from_code(*code) {
                Some(
                    reason @ (CloseReason::UnknownKey
                    | CloseReason::Replaced
                    | CloseReason::Revoked),
                ) => tracing::error!(
                    code,
                    reason = reason.text(),
                    retry_in_secs,
                    "Coordinator refused the control channel"
                ),
                reason => tracing::warn!(
                    code,
                    reason = reason.map(CloseReason::text),
                    retry_in_secs,
                    "Coordinator closed the control channel"
                ),
            },
            Self::ConnectFailed(e) => tracing::warn!(
                error = format!("{e:#}"),
                retry_in_secs,
                "Cannot connect the control channel"
            ),
            Self::Lost(e) => tracing::warn!(error = %e, retry_in_secs, "Control channel lost"),
            Self::Silent => tracing::warn!(
                retry_in_secs,
                "Control channel silent for too long, reconnecting"
            ),
            Self::Shutdown => {}
        }
    }
}

/// How long to wait before the next attempt. A Coordinator that refused the
/// key, or another backend holding the same key, is not fixed by retrying
/// quickly, so those wait `retry_max`.
fn retry_delay(end: &SessionEnd, backoff: &mut Backoff, retry_max: Duration) -> Duration {
    let refused = matches!(
        end,
        SessionEnd::ClosedByServer(code)
            if matches!(
                CloseReason::from_code(*code),
                Some(CloseReason::UnknownKey | CloseReason::Replaced | CloseReason::Revoked)
            )
    );
    if refused {
        retry_max
    } else {
        jittered(backoff.next_delay())
    }
}

/// Runs one connection: opens the control stream, pings, and watches for
/// silence until the connection ends or `shutdown` is cancelled.
async fn session(
    connection: &Connection,
    timings: &Timings,
    shutdown: &CancellationToken,
) -> SessionEnd {
    let (mut send, recv) = match connection.open_bi().await {
        Ok(streams) => streams,
        Err(e) => return connection_end(connection, e.to_string()).await,
    };
    // The Coordinator only sees the stream once data is sent on it, so the
    // first ping goes out right away.
    if let Err(e) = write_message(&mut send, &Message::Ping).await {
        return connection_end(connection, e.to_string()).await;
    }

    // A stream keeps a partly read frame across `select!` iterations, unlike a
    // `read_message` future that a firing timer would drop.
    let messages = futures::stream::unfold(recv, |mut recv| async move {
        let message = read_message(&mut recv).await.transpose()?;
        Some((message, recv))
    });
    tokio::pin!(messages);

    let mut ping = interval_at(
        Instant::now() + timings.ping_interval,
        timings.ping_interval,
    );
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut deadline = Instant::now() + timings.silence_timeout;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                connection.close(CloseReason::Closed.code().into(), CloseReason::Closed.text().as_bytes());
                return SessionEnd::Shutdown;
            }
            _ = ping.tick() => {
                if let Err(e) = write_message(&mut send, &Message::Ping).await {
                    return connection_end(connection, e.to_string()).await;
                }
            }
            message = messages.next() => match message {
                Some(Ok(message)) => {
                    deadline = Instant::now() + timings.silence_timeout;
                    tracing::trace!(?message, "Control message received");
                }
                Some(Err(e)) => return connection_end(connection, e.to_string()).await,
                None => return connection_end(connection, "stream finished".into()).await,
            },
            _ = sleep_until(deadline) => {
                connection.close(CloseReason::Timeout.code().into(), CloseReason::Timeout.text().as_bytes());
                return SessionEnd::Silent;
            }
        }
    }
}

/// Turns a failed read or write into the reason the connection ended. A
/// stream error usually arrives just before the close itself, so this waits
/// briefly for the close code.
async fn connection_end(connection: &Connection, error: String) -> SessionEnd {
    let closed = timeout(Duration::from_secs(1), connection.closed()).await;
    match closed {
        Ok(ConnectionError::ApplicationClosed(close)) => {
            match u32::try_from(close.error_code.into_inner()) {
                Ok(code) => SessionEnd::ClosedByServer(code),
                Err(_) => SessionEnd::Lost(format!("closed with code {}", close.error_code)),
            }
        }
        Ok(other) => SessionEnd::Lost(other.to_string()),
        Err(_) => {
            connection.close(CloseReason::Protocol.code().into(), error.as_bytes());
            SessionEnd::Lost(error)
        }
    }
}

#[cfg(test)]
mod tests;
