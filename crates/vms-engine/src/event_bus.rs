use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use vms_core::event::{Event, TopicKey};

/// Capacity used when none is specified.
pub const DEFAULT_CAPACITY: usize = 512;

/// Central publish / subscribe hub for the VMS pipeline engine.
///
/// Internally a [`DashMap`] that maps every [`TopicKey`] to the
/// [`broadcast::Sender`] half of a Tokio broadcast channel. Channels are
/// created lazily on the first [`subscribe`](EventBus::subscribe) call for
/// a given key.
///
/// All clones of `Arc<EventBus>` share the same registry — there is exactly
/// one bus per daemon process.
pub struct EventBus {
    channels: DashMap<TopicKey, broadcast::Sender<Event>>,
    capacity: usize,
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
        })
    }
}
