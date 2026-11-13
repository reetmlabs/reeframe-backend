//! Reconnect timing for long-lived outbound connections: exponential
//! backoff that resets only after a stable connection, plus jitter.

use std::time::Duration;

use rand::Rng;
use tokio::time::Instant;

/// Exponential backoff capped at `max` (jitter is added by [`jittered`]).
/// Like `vms_media::camera_stream::ReconnectPolicy`, the delay only resets
/// after a connection has stayed up for a while, so a flapping connection
/// keeps backing off instead of hot-looping.
pub(crate) struct Backoff {
    base: Duration,
    max: Duration,
    /// How long a connection must stay up before the next failure starts a
    /// fresh failure streak.
    stable_uptime: Duration,
    connected_at: Instant,
    delay: Duration,
}

impl Backoff {
    pub(crate) fn new(base: Duration, max: Duration, stable_uptime: Duration) -> Self {
        Self {
            base,
            max,
            stable_uptime,
            connected_at: Instant::now(),
            delay: base,
        }
    }

    /// Call once per failed or dropped connection to get the next delay.
    /// Deterministic so it can be tested exactly; the caller adds jitter.
    pub(crate) fn next_delay(&mut self) -> Duration {
        if self.connected_at.elapsed() >= self.stable_uptime {
            self.delay = self.base;
        }
        let delay = self.delay;
        self.delay = (self.delay * 2).min(self.max);
        delay
    }

    /// Starts the uptime clock that `next_delay` checks. It doesn't reset
    /// `delay`, because only a stable connection counts as recovery.
    pub(crate) fn record_connected(&mut self) {
        self.connected_at = Instant::now();
    }
}

/// Applies up to ±20% jitter to `delay` so BEs reconnecting to the same
/// server after a shared outage don't retry in lockstep.
pub(crate) fn jittered(delay: Duration) -> Duration {
    let jitter_range = delay.as_secs_f64() * 0.2;
    let jitter = rand::thread_rng().gen_range(-jitter_range..=jitter_range);
    Duration::from_secs_f64((delay.as_secs_f64() + jitter).max(0.0))
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
}
