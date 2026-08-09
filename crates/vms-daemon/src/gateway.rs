//! Persistent outbound connection to a Relay/gateway server for WAPP
//! pairing — connection lifecycle only, no registration handshake or wire
//! protocol yet.
//!
//! Only runs at all when `[gateway] url` is configured — this BE stays
//! fully autonomous with zero dependency on any Relay/WAPP being reachable
//! otherwise, the same guarantee Coordinator trust already has.

use std::time::Duration;

use rand::Rng;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::Instant;

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

/// Runs until `shutdown` resolves. Reconnects on any connect failure or
/// dropped connection, with exponential backoff. A single long-lived task
/// drives the whole lifecycle — no task or socket is ever spawned per
/// attempt, so nothing accumulates across however many reconnect cycles
/// happen.
pub async fn run(addr: String, shutdown: tokio::sync::oneshot::Receiver<()>) {
    run_with_backoff(
        addr,
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
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    mut backoff: Backoff,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            result = TcpStream::connect(&addr) => {
                match result {
                    Ok(stream) => {
                        tracing::info!(%addr, "Gateway connection established");
                        backoff.record_connected();
                        wait_for_disconnect(stream, &mut shutdown).await;
                        tracing::warn!(%addr, "Gateway connection lost");
                    }
                    Err(error) => {
                        tracing::warn!(%addr, %error, "Gateway connection attempt failed");
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

        let run_handle = tokio::spawn(run_with_backoff(addr, shutdown_rx, test_backoff()));

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
}
