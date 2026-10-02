//! External source adapter types.
//!
//! Mirrors `vms_db::entities::source::SourceType` at the domain level, the
//! same way [`crate::trigger::TriggerType`] mirrors the DB-level trigger
//! enum. The persistence layer keeps its own `DeriveActiveEnum` type and the
//! repo maps between the two, so `vms-core` needs no SeaORM dependency.

use serde::{Deserialize, Serialize};

/// Discriminates which adapter a [`crate::event::TopicKey::Source`] event
/// came from and how its `config` JSON should be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Mqtt,
    Webhook,
    ApiPoll,
    HaWebsocket,
    FileWatcher,
}
