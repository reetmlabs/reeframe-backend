use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use vms_core::event::{Event, TopicKey};

use crate::metrics::Metrics;

/// Capacity used when none is specified.
pub const DEFAULT_CAPACITY: usize = 512;

/// Central publish / subscribe hub for the VMS pipeline engine.
///
/// Internally a [`DashMap`] that maps every [`TopicKey`] to the
/// [`broadcast::Sender`] half of a Tokio broadcast channel. Channels are
/// created lazily on the first [`subscribe`](EventBus::subscribe) call for
/// a given key.
///
/// The daemon creates one bus and shares it through `Arc` clones.
pub struct EventBus {
    channels: DashMap<TopicKey, broadcast::Sender<Event>>,
    capacity: usize,
    metrics: Option<Arc<Metrics>>,
}

impl EventBus {
    /// Create a new bus and return it behind an `Arc`.
    ///
    /// `capacity` is the per-topic ring buffer size passed to
    /// [`broadcast::channel`]. Receivers that fall more than `capacity`
    /// messages behind receive [`broadcast::error::RecvError::Lagged`] on
    /// their next [`recv`](broadcast::Receiver::recv) call.
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            channels: DashMap::new(),
            capacity,
            metrics: None,
        })
    }

    /// Same as [`new`](EventBus::new), but counts every delivered publish in
    /// `metrics` (`event_bus_published_total`, by topic kind). A separate
    /// constructor keeps tests that call `EventBus::new(capacity)` free of a
    /// `Metrics` handle.
    pub fn new_with_metrics(capacity: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            channels: DashMap::new(),
            capacity,
            metrics: Some(metrics),
        })
    }

    /// Subscribe to a topic, returning a live [`broadcast::Receiver<Event>`].
    ///
    /// If no channel exists for `key` yet, one is created now (lazy init).
    /// The receiver only delivers events published **after** this call;
    /// there is no history replay.
    ///
    /// Multiple callers subscribing to the same key each get an independent
    /// receiver; all of them receive every subsequent event on that topic.
    pub fn subscribe(&self, key: &TopicKey) -> broadcast::Receiver<Event> {
        self.channels
            .entry(key.clone())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(self.capacity);
                tx
            })
            .subscribe()
    }

    /// Publish `event` to every active subscriber on `key`.
    ///
    /// If no channel exists for `key` (nobody has called [`subscribe`] yet)
    /// the event is dropped silently.
    ///
    /// If the channel exists but all receivers have been dropped, the event is
    /// dropped and the dead channel is removed from the map. This is logged at
    /// TRACE and not treated as an error.
    ///
    /// [`subscribe`]: EventBus::subscribe
    pub fn publish(&self, key: &TopicKey, event: Event) {
        let Some(sender) = self.channels.get(key) else {
            return;
        };
        let result = sender.send(event);
        drop(sender); // release shard read lock before potential write below
        match result {
            Ok(n) => {
                tracing::trace!(topic = %key.topic_string(), receivers = n, "event published");
                if let Some(metrics) = &self.metrics {
                    metrics.record_event_published(topic_kind(key));
                }
            }
            Err(_) => {
                self.channels.remove(key);
                tracing::trace!(topic = %key.topic_string(), "event dropped, no active receivers, channel pruned");
            }
        }
    }
}

/// Maps a [`TopicKey`] to its *kind*, a small fixed vocabulary safe to use as
/// a Prometheus label. [`TopicKey::topic_string`] embeds a UUID for
/// `Camera`/`Source` and would blow up label cardinality.
fn topic_kind(key: &TopicKey) -> &'static str {
    match key {
        TopicKey::Camera(_) => "camera",
        TopicKey::Source(_) => "source",
        TopicKey::System => "system",
        TopicKey::Stat => "stat",
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    fn make_event(key: &TopicKey) -> Event {
        Event::new(key, "test_event", json!({"x": 1}))
    }

    // A subscriber receives an event published after it subscribed.
    #[tokio::test]
    async fn single_subscriber_receives_published_event() {
        let bus = EventBus::new(16);
        let key = TopicKey::System;

        let mut rx = bus.subscribe(&key);
        bus.publish(&key, make_event(&key));

        let got = rx.recv().await.unwrap();
        assert_eq!(got.event_type, "test_event");
    }

    // Two subscribers on the same topic both receive the same event; the first
    // reader does not consume it.
    #[tokio::test]
    async fn two_subscribers_both_receive_same_event() {
        let bus = EventBus::new(16);
        let key = TopicKey::Stat;

        let mut rx1 = bus.subscribe(&key);
        let mut rx2 = bus.subscribe(&key);
        bus.publish(&key, make_event(&key));

        assert_eq!(rx1.recv().await.unwrap().event_type, "test_event");
        assert_eq!(rx2.recv().await.unwrap().event_type, "test_event");
    }

    // Events published to Camera(A) are not seen by subscribers on Camera(B)
    // or System.
    #[tokio::test]
    async fn different_topics_do_not_cross_pollute() {
        let bus = EventBus::new(16);
        let cam_a = TopicKey::Camera(Uuid::new_v4());
        let cam_b = TopicKey::Camera(Uuid::new_v4());
        let sys = TopicKey::System;

        let mut rx_a = bus.subscribe(&cam_a);
        let mut rx_b = bus.subscribe(&cam_b);
        let mut rx_s = bus.subscribe(&sys);

        bus.publish(&cam_a, make_event(&cam_a));

        // cam_a subscriber got it
        assert!(rx_a.try_recv().is_ok());
        // cam_b and system subscribers did not
        assert!(rx_b.try_recv().is_err());
        assert!(rx_s.try_recv().is_err());
    }

    // When a receiver falls behind the ring buffer capacity, it receives
    // RecvError::Lagged(n) telling it how many messages it missed, then
    // continues receiving normally on the next call.
    #[tokio::test]
    async fn lagged_receiver_reports_lag_then_recovers() {
        use tokio::sync::broadcast::error::RecvError;

        let bus = EventBus::new(2); // overflows after 2 unread events
        let key = TopicKey::Stat;

        let mut rx = bus.subscribe(&key);

        // Publish 4 events without consuming, overflowing the buffer by 2
        for _ in 0..4 {
            bus.publish(&key, make_event(&key));
        }

        // First recv tells us we lagged
        match rx.recv().await {
            Err(RecvError::Lagged(n)) => assert!(n > 0),
            other => panic!("expected Lagged, got {other:?}"),
        }

        // After the lag the receiver is still usable and gets the next event
        assert!(rx.recv().await.is_ok());
    }

    // When the last receiver for a topic is dropped, the next publish removes
    // the dead channel entry so the map does not grow without bound.
    #[tokio::test]
    async fn dead_channel_is_pruned_on_publish() {
        let bus = EventBus::new(16);
        let key = TopicKey::Camera(Uuid::new_v4());

        let rx = bus.subscribe(&key);
        assert!(bus.channels.contains_key(&key));

        drop(rx); // all receivers gone

        bus.publish(&key, make_event(&key));

        assert!(
            !bus.channels.contains_key(&key),
            "dead channel must be pruned after failed publish"
        );
    }

    // Publishing to a topic with no subscribers is a silent no-op and must not
    // create a channel entry; only subscribe() creates channels.
    #[tokio::test]
    async fn publish_before_subscribe_is_noop_and_creates_no_channel() {
        let bus = EventBus::new(16);
        let key = TopicKey::Camera(Uuid::new_v4());

        bus.publish(&key, make_event(&key)); // no subscriber yet

        assert!(
            !bus.channels.contains_key(&key),
            "publish must not create a channel"
        );
    }
}
