//! Persistent outbound connection to a Relay/gateway server for WAPP
//! pairing, plus the registration handshake sent once each time it connects.
//! The wire format below is provisional — nothing on the other end of this
//! connection exists yet to confirm it against.
//!
//! Only runs at all when `[gateway] url` is configured — this BE stays
//! fully autonomous with zero dependency on any Relay/WAPP being reachable
//! otherwise, the same guarantee Coordinator trust already has.

use std::time::Duration;

use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use uuid::Uuid;

/// Sent once, immediately after connecting, so the Relay knows which site
/// this connection belongs to and how to route requests to it. `be_id` is
/// the same identifier Coordinator's `sites` table registers this BE under
/// — trusting that identity is what the JWKS work already established.
///
/// Newline-delimited JSON: simplest thing that's both human-debuggable and
/// trivially frameable over a raw stream, and easy to replace once a real
/// protocol is designed against an actual Relay implementation.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RegistrationMessage {
    be_id: Uuid,
    /// This daemon's own version — the most concrete "capability info" that
    /// actually exists today. Extend with real capability flags once the
    /// Relay side defines what it needs to route on.
    version: String,
}

impl RegistrationMessage {
    fn for_this_daemon(be_id: Uuid) -> Self {
        Self {
            be_id,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// NDJSON framing: compact JSON plus a trailing `\n`. Serializing a
    /// `Uuid` and a `String` can't fail, so this returns the bytes
    /// directly rather than a `Result` — there is no "malformed" case once
    /// you already have a `be_id: Uuid` in hand.
    fn encode(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(self).expect("RegistrationMessage always serializes");
        bytes.push(b'\n');
        bytes
    }
}

/// Rejects a missing `be_id` before any connection is even attempted — a
/// static configuration gap (`[gateway] url` set without `[gateway]
/// be_id`), not a transient failure backoff/retry could ever fix. A
/// malformed `be_id` in the config file is rejected even earlier, by
/// `Uuid`'s own `Deserialize` impl when the config is first loaded.
pub(crate) fn require_be_id(be_id: Option<Uuid>) -> Result<Uuid, String> {
    be_id.ok_or_else(|| "[gateway] be_id must be set when [gateway] url is configured".into())
}

/// Exponential backoff with jitter, capped at `max`. Mirrors
/// `vms_media::camera_stream::ReconnectPolicy`'s "a connection must stay up
/// for a while before the *next* failure counts as real recovery" design —
/// without that, a connection that flaps every few seconds would hot-loop
/// reconnects at the base delay forever instead of ever escalating.
struct Backoff {
    base: Duration,
    max: Duration,
    /// A connection must stay up at least this long before the next failure
    /// is treated as the start of a fresh failure streak rather than a
    /// continuation of the current one.
    stable_uptime: Duration,
    connected_at: Instant,
    delay: Duration,
}

impl Backoff {
    fn new(base: Duration, max: Duration, stable_uptime: Duration) -> Self {
        Self {
            base,
            max,
            stable_uptime,
            connected_at: Instant::now(),
            delay: base,
        }
    }

    /// Call once per failed/dropped connection to get the delay before the
    /// next attempt. Deterministic — jitter is applied separately by the
    /// caller, at the point the delay is actually used, so this stays
    /// exactly testable like `ReconnectPolicy::on_failure`.
    fn next_delay(&mut self) -> Duration {
        if self.connected_at.elapsed() >= self.stable_uptime {
            self.delay = self.base;
        }
        let delay = self.delay;
        self.delay = (self.delay * 2).min(self.max);
        delay
    }

    /// Call right after a connection attempt succeeds — starts the uptime
    /// clock `next_delay` checks against. Deliberately does not reset
    /// `delay` itself; only a *stable* connection (checked in `next_delay`)
    /// counts as recovery.
    fn record_connected(&mut self) {
        self.connected_at = Instant::now();
    }
}

/// Applies up to ±20% jitter to `delay` — so many BEs reconnecting to the
/// same gateway after a shared outage don't all retry in lockstep.
fn jittered(delay: Duration) -> Duration {
    let jitter_range = delay.as_secs_f64() * 0.2;
    let jitter = rand::thread_rng().gen_range(-jitter_range..=jitter_range);
    Duration::from_secs_f64((delay.as_secs_f64() + jitter).max(0.0))
}

/// Runs until `shutdown` resolves. Reconnects on any connect failure,
/// registration-send failure, or dropped connection, with exponential
/// backoff. A single long-lived task drives the whole lifecycle — no task
/// or socket is ever spawned per attempt, so nothing accumulates across
/// however many reconnect cycles happen.
///
/// `be_id` is required, not `Option<Uuid>` — see `require_be_id`'s doc
/// comment for why a missing one is rejected before this is ever called.
pub async fn run(addr: String, be_id: Uuid, shutdown: tokio::sync::oneshot::Receiver<()>) {
    run_with_backoff(
        addr,
        be_id,
        shutdown,
        Backoff::new(
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ),
    )
    .await
}

async fn run_with_backoff(
    addr: String,
    be_id: Uuid,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    mut backoff: Backoff,
) {
    let registration = RegistrationMessage::for_this_daemon(be_id).encode();

    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            result = connect_and_register(&addr, &registration) => {
                match result {
                    Ok(stream) => {
                        tracing::info!(%addr, %be_id, "Gateway connection established and registered");
                        backoff.record_connected();
                        wait_for_disconnect(stream, &mut shutdown).await;
                        tracing::warn!(%addr, "Gateway connection lost");
                    }
                    Err(error) => {
                        tracing::warn!(%addr, %error, "Gateway connection/registration attempt failed");
                    }
                }
            }
        }

        let delay = jittered(backoff.next_delay());
        tokio::select! {
            _ = &mut shutdown => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn connect_and_register(addr: &str, registration: &[u8]) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(registration).await?;
    Ok(stream)
}

/// Blocks until the connection drops (EOF or a read error) or `shutdown`
/// resolves. No wire protocol exists yet — any bytes received are
/// discarded; only the connection's liveness matters here.
async fn wait_for_disconnect(
    mut stream: TcpStream,
    shutdown: &mut tokio::sync::oneshot::Receiver<()>,
) {
    let mut buf = [0u8; 256];
    loop {
        tokio::select! {
            _ = &mut *shutdown => return,
            result = stream.read(&mut buf) => {
                match result {
                    Ok(0) | Err(_) => return,
                    Ok(_) => continue,
                }
            }
        }
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backoff() -> Backoff {
        Backoff::new(
            Duration::from_millis(10),
            Duration::from_millis(80),
            Duration::from_millis(200),
        )
    }

    // -- Backoff --

    #[tokio::test(start_paused = true)]
    async fn first_failure_uses_base_delay() {
        let mut backoff = test_backoff();
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_quick_failures_escalate_and_cap() {
        let mut backoff = test_backoff();
        let expected_ms = [10u64, 20, 40, 80, 80, 80];
        for expected in expected_ms {
            assert_eq!(backoff.next_delay().as_millis() as u64, expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stable_uptime_resets_backoff_to_base() {
        let mut backoff = test_backoff();
        backoff.next_delay();
        backoff.next_delay();
        backoff.next_delay(); // escalated past base by now

        backoff.record_connected();
        tokio::time::advance(Duration::from_millis(201)).await;

        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
    }

    #[tokio::test(start_paused = true)]
    async fn short_uptime_does_not_reset_backoff() {
        let mut backoff = test_backoff();
        backoff.next_delay(); // returns 10ms, escalates internal state to 20ms

        backoff.record_connected();
        tokio::time::advance(Duration::from_millis(100)).await; // < stable_uptime

        assert_eq!(backoff.next_delay().as_millis() as u64, 20);
    }

    #[test]
    fn jitter_stays_within_twenty_percent_and_never_negative() {
        let base = Duration::from_secs(10);
        for _ in 0..1000 {
            let d = jittered(base);
            assert!(d.as_secs_f64() >= 8.0 && d.as_secs_f64() <= 12.0);
        }
    }

    // -- Connection lifecycle against a local mock acceptor --

    /// Accepts connections on an ephemeral local port and immediately drops
    /// each one — simulating a Relay server that keeps bouncing the BE,
    /// forcing repeated reconnects. Returns the bound address and a counter
    /// of how many connections it has accepted so far.
    async fn spawn_flaky_acceptor() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = count.clone();
        tokio::spawn(async move {
            loop {
                let Ok((_socket, _)) = listener.accept().await else {
                    return;
                };
                count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Dropping `_socket` immediately closes the connection —
                // the client's next read returns EOF.
            }
        });
        (addr, count)
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_repeatedly_against_a_flaky_acceptor_then_shuts_down_cleanly() {
        let (addr, accepted) = spawn_flaky_acceptor().await;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let run_handle = tokio::spawn(run_with_backoff(
            addr,
            Uuid::new_v4(),
            shutdown_rx,
            test_backoff(),
        ));

        // Let several reconnect cycles happen. Each cycle is: real (fast,
        // local) TCP connect + immediate drop, then a virtual-time sleep
        // for the backoff delay — advancing time drives that sleep without
        // any real wall-clock wait.
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }

        assert!(
            accepted.load(std::sync::atomic::Ordering::SeqCst) >= 5,
            "expected multiple reconnect attempts against the flaky acceptor"
        );

        // A single task drives the whole lifecycle — shutting it down once
        // must actually end it, not leave it (or anything it spawned)
        // running in the background.
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), run_handle)
            .await
            .expect("run task did not exit promptly after shutdown")
            .unwrap();
    }

    // -- Registration handshake --

    #[test]
    fn missing_be_id_is_rejected_before_any_connection_is_attempted() {
        assert!(require_be_id(None).is_err());
    }

    #[test]
    fn present_be_id_is_accepted() {
        let be_id = Uuid::new_v4();
        assert_eq!(require_be_id(Some(be_id)).unwrap(), be_id);
    }

    /// Accepts one connection, reads and parses exactly one NDJSON-framed
    /// [`RegistrationMessage`] from it, and reports the result back.
    async fn spawn_registration_acceptor() -> (
        String,
        tokio::sync::oneshot::Receiver<std::io::Result<RegistrationMessage>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(socket);
            let mut line = String::new();
            let result = match tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut line).await {
                Ok(_) => serde_json::from_str::<RegistrationMessage>(line.trim_end())
                    .map_err(std::io::Error::other),
                Err(e) => Err(e),
            };
            let _ = tx.send(result);
        });
        (addr, rx)
    }

    #[tokio::test]
    async fn a_mock_acceptor_can_parse_the_be_id_and_version_from_the_registration_message() {
        let (addr, result_rx) = spawn_registration_acceptor().await;
        let be_id = Uuid::new_v4();

        let stream =
            connect_and_register(&addr, &RegistrationMessage::for_this_daemon(be_id).encode())
                .await
                .unwrap();
        drop(stream);

        let parsed = tokio::time::timeout(Duration::from_secs(1), result_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(parsed.be_id, be_id);
        assert_eq!(parsed.version, env!("CARGO_PKG_VERSION"));
    }
}
