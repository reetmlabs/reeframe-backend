//! MQTT subscriber source adapter.
//!
//! Subscribes to a topic (or topic filter) on a broker and publishes an
//! [`Event`] for every message received. `rumqttc`'s event loop reconnects
//! by itself as long as `poll()` keeps being called, so the loop below keeps
//! going through network errors and only stops on cancellation.

use std::time::Duration;

use rumqttc::{AsyncClient, Event as MqttEvent, Incoming, MqttOptions, QoS};
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use vms_core::{
    event::{Event, TopicKey},
    VmsError,
};

/// Configuration for the [`vms_core::SourceType::Mqtt`] adapter.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttSourceConfig {
    pub host: String,
    pub port: u16,
    /// Topic or topic filter (e.g. `"sensors/+/motion"`) to subscribe to.
    pub topic: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: Option<String>,
    /// `0` = at most once, `1` = at least once, `2` = exactly once. Defaults to `0`.
    #[serde(default)]
    pub qos: u8,
}

/// Delay before retrying `poll()` after a connection error, so a broker
/// that's unreachable doesn't turn this into a hot spin loop.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

fn qos_from_u8(qos: u8) -> Result<QoS, VmsError> {
    match qos {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        other => Err(VmsError::Source(format!(
            "mqtt: invalid qos {other}, expected 0, 1, or 2"
        ))),
    }
}

/// Connect to the broker in `config` and subscribe to its topic.
///
/// A bad broker address can't be detected here, because `AsyncClient::new`
/// only builds local state and doesn't touch the network until the event loop
/// is polled. Connection errors show up inside the loop and are logged, since
/// `rumqttc` reconnects by being polled again. Only config errors such as an
/// invalid QoS are returned.
pub fn spawn(
    source_id: Uuid,
    config: serde_json::Value,
    event_tx: UnboundedSender<Event>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, VmsError> {
    let cfg: MqttSourceConfig = serde_json::from_value(config)?;
    let qos = qos_from_u8(cfg.qos)?;

    let client_id = cfg
        .client_id
        .clone()
        .unwrap_or_else(|| format!("reeframe-{source_id}"));
    let mut opts = MqttOptions::new(client_id, cfg.host.clone(), cfg.port);
    opts.set_keep_alive(Duration::from_secs(30));
    if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
        opts.set_credentials(user.clone(), pass.clone());
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 32);
    let topic = cfg.topic.clone();

    let task = tokio::spawn(async move {
        if let Err(e) = client.subscribe(&topic, qos).await {
            tracing::error!(source_id = %source_id, error = %e, "mqtt: failed to queue subscribe");
            return;
        }

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                result = eventloop.poll() => match result {
                    Ok(MqttEvent::Incoming(Incoming::Publish(publish))) => {
                        let payload = serde_json::json!({
                            "topic": publish.topic,
                            "payload": String::from_utf8_lossy(&publish.payload),
                        });
                        let event =
                            Event::new(&TopicKey::Source(source_id), "mqtt_message", payload);
                        if event_tx.send(event).is_err() {
                            return; // The Event Bus forwarder is gone, so nobody is listening.
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(source_id = %source_id, error = %e, "mqtt: connection error, retrying");
                        tokio::time::sleep(RECONNECT_DELAY).await;
                    }
                },
            }
        }
    });

    Ok(task)
}

// -- Tests --

#[cfg(test)]
mod tests {
    use tokio::time::timeout;

    use super::*;

    // Invalid QoS is rejected synchronously, before any connection attempt.
    #[test]
    fn invalid_qos_returns_error() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let config = serde_json::json!({
            "host": "127.0.0.1", "port": 1883, "topic": "t", "qos": 9
        });
        let result = spawn(Uuid::new_v4(), config, tx, CancellationToken::new());
        assert!(result.is_err());
    }

    // Cancellation stops the task promptly even while the connection attempt
    // to an unreachable broker is pending, because `select!` races it against
    // the cancel token.
    #[tokio::test]
    async fn cancel_stops_task_without_a_reachable_broker() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let config = serde_json::json!({
            "host": "127.0.0.1", "port": 1, "topic": "vms-sources-test/topic"
        });
        let task = spawn(Uuid::new_v4(), config, tx, cancel.clone()).unwrap();

        cancel.cancel();
        timeout(Duration::from_secs(5), task)
            .await
            .expect("task did not stop promptly after cancellation")
            .unwrap();
    }
}
