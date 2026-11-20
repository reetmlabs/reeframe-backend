//! The control channel client: one connection to the Coordinator, over QUIC
//! or, where UDP is blocked, over a WebSocket. Authenticated with the site
//! key, kept alive with pings and opened again whenever it is lost.

mod quic;
mod wss;

use std::time::Duration;

use anyhow::Context;
use ed25519_dalek::SigningKey;
use reeframe_protocol::{CloseReason, Message, NONCE_TTL};
use rustls::RootCertStore;
use tokio::time::{interval_at, sleep, sleep_until, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::proxy::OutboundProxy;
use crate::reconnect::{jittered, Backoff};

/// A host and port to connect to. `host` is also the TLS server name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    pub host: String,
    pub port: u16,
}

impl Target {
    /// The QUIC target: `control_addr` (`host:port`), or when it is empty
    /// the host of `coordinator_url` on port 443.
    pub(crate) fn quic(control_addr: &str, coordinator_url: &str) -> anyhow::Result<Self> {
        if control_addr.is_empty() {
            let (host, _) = url_host_port(coordinator_url)?;
            return Ok(Self { host, port: 443 });
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

    /// The WebSocket target: host and port of `coordinator_url`, port 443
    /// when it has none. `control_addr` does not apply, since the WebSocket
    /// goes through the same HTTPS front as the rest of the Coordinator.
    pub(crate) fn wss(coordinator_url: &str) -> anyhow::Result<Self> {
        let (host, port) = url_host_port(coordinator_url)?;
        Ok(Self {
            host,
            port: port.unwrap_or(443),
        })
    }
}

fn url_host_port(coordinator_url: &str) -> anyhow::Result<(String, Option<u16>)> {
    let url = reqwest::Url::parse(coordinator_url)
        .with_context(|| format!("coordinator URL {coordinator_url:?} is not a URL"))?;
    let host = url
        .host_str()
        .with_context(|| format!("coordinator URL {coordinator_url:?} has no host"))?;
    Ok((unbracket(host).to_owned(), url.port()))
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
    /// Gap between QUIC attempts to the next address of the host.
    pub race_stagger: Duration,
    /// Time QUIC gets before the WebSocket is tried instead.
    pub quic_fallback_after: Duration,
    /// Limit for resolving and connecting, per transport.
    pub connect_timeout: Duration,
    /// Limit for each step of the WebSocket login.
    pub login_timeout: Duration,
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
            quic_fallback_after: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(10),
            login_timeout: NONCE_TTL,
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            stable_uptime: Duration::from_secs(30),
        }
    }
}

/// A control channel client, ready to [`run`](Self::run).
pub(crate) struct ControlClient {
    quic: quic::Transport,
    wss: wss::Transport,
    timings: Timings,
}

impl ControlClient {
    pub(crate) fn new(
        key: &SigningKey,
        quic_target: Target,
        wss_target: Target,
        proxy: Option<OutboundProxy>,
        roots: RootCertStore,
        timings: Timings,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            quic: quic::Transport::new(key, quic_target, roots.clone())?,
            wss: wss::Transport::new(key, wss_target, proxy, roots)?,
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
                Ok(mut channel) => {
                    backoff.record_connected();
                    tracing::info!(
                        transport = channel.transport(),
                        peer = %channel.peer(),
                        "Control channel connected"
                    );
                    session(&mut channel, &self.timings, &shutdown).await
                }
                Err(end) => end,
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
        self.quic.shutdown().await;
        tracing::info!("Control channel stopped");
    }

    /// QUIC first on every attempt; the WebSocket only when no QUIC handshake
    /// completes within `quic_fallback_after`.
    async fn connect(&self) -> Result<Channel, SessionEnd> {
        let quic = timeout(
            self.timings.quic_fallback_after,
            self.quic.connect(&self.timings),
        )
        .await;
        let quic_error = match quic {
            Ok(Ok(channel)) => return Ok(Channel::Quic(channel)),
            Ok(Err(SessionEnd::ConnectFailed(e))) => format!("{e:#}"),
            Ok(Err(end)) => return Err(end),
            Err(_) => "no handshake in time".to_owned(),
        };
        tracing::debug!(error = quic_error, "QUIC unavailable, trying the WebSocket");
        self.wss
            .connect(&self.timings)
            .await
            .map(|channel| Channel::Wss(Box::new(channel)))
    }
}

/// An open control channel on either transport.
enum Channel {
    Quic(quic::Channel),
    // Boxed because the WebSocket and TLS state make it much larger.
    Wss(Box<wss::Channel>),
}

/// What a channel received.
enum Received {
    Message(Message),
    /// Traffic that carries no message, like a WebSocket ping. It still
    /// shows the connection is alive.
    Other,
    /// The channel ended; the session asks the transport why.
    Ended(String),
}

impl Channel {
    fn transport(&self) -> &'static str {
        match self {
            Self::Quic(_) => "quic",
            Self::Wss(_) => "wss",
        }
    }

    fn peer(&self) -> String {
        match self {
            Self::Quic(channel) => channel.peer(),
            Self::Wss(channel) => channel.peer(),
        }
    }

    async fn send(&mut self, message: &Message) -> Result<(), String> {
        match self {
            Self::Quic(channel) => channel.send(message).await,
            Self::Wss(channel) => channel.send(message).await,
        }
    }

    /// Cancel safe, so it can sit in a `select!` next to timers.
    async fn recv(&mut self) -> Received {
        match self {
            Self::Quic(channel) => channel.recv().await,
            Self::Wss(channel) => channel.recv().await,
        }
    }

    async fn close(&mut self, reason: CloseReason) {
        match self {
            Self::Quic(channel) => channel.close(reason),
            Self::Wss(channel) => channel.close(reason).await,
        }
    }

    /// Why the channel ended after a failed send or receive.
    async fn end(&mut self, error: String) -> SessionEnd {
        match self {
            Self::Quic(channel) => channel.end(error).await,
            Self::Wss(channel) => channel.end(error),
        }
    }
}

/// Why a connection attempt or session ended.
#[derive(Debug)]
enum SessionEnd {
    Shutdown,
    ConnectFailed(anyhow::Error),
    /// The Coordinator closed the connection. `code` is the code on the
    /// wire, `reason` its meaning if this version knows it.
    ClosedByServer {
        code: u32,
        reason: Option<CloseReason>,
    },
    /// The connection broke for any other reason.
    Lost(String),
    /// Nothing arrived within the silence timeout.
    Silent,
}

impl SessionEnd {
    fn quic_close(code: u32) -> Self {
        Self::ClosedByServer {
            code,
            reason: CloseReason::from_code(code),
        }
    }

    fn ws_close(code: u16) -> Self {
        Self::ClosedByServer {
            code: code.into(),
            reason: CloseReason::from_ws_code(code),
        }
    }

    fn log(&self, delay: Duration) {
        let retry_in_secs = delay.as_secs_f64();
        match self {
            Self::ClosedByServer { code, reason } => match reason {
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
        SessionEnd::ClosedByServer {
            reason: Some(CloseReason::UnknownKey | CloseReason::Replaced | CloseReason::Revoked),
            ..
        }
    );
    if refused {
        retry_max
    } else {
        jittered(backoff.next_delay())
    }
}

/// Runs one open channel: pings, and watches for silence until the channel
/// ends or `shutdown` is cancelled.
async fn session(
    channel: &mut Channel,
    timings: &Timings,
    shutdown: &CancellationToken,
) -> SessionEnd {
    let mut ping = interval_at(
        Instant::now() + timings.ping_interval,
        timings.ping_interval,
    );
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut deadline = Instant::now() + timings.silence_timeout;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                channel.close(CloseReason::Closed).await;
                return SessionEnd::Shutdown;
            }
            _ = ping.tick() => {
                if let Err(e) = channel.send(&Message::Ping).await {
                    return channel.end(e).await;
                }
            }
            received = channel.recv() => match received {
                Received::Message(message) => {
                    deadline = Instant::now() + timings.silence_timeout;
                    tracing::trace!(?message, "Control message received");
                }
                Received::Other => deadline = Instant::now() + timings.silence_timeout,
                Received::Ended(e) => return channel.end(e).await,
            },
            _ = sleep_until(deadline) => {
                channel.close(CloseReason::Timeout).await;
                return SessionEnd::Silent;
            }
        }
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_wss;
