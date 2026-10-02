//! `vms-actions`: action and device-control node handlers.
//!
//! Every handler receives a [`NodeInput`] and returns a [`NodeOutput`].
//! Handlers are dispatched through [`ActionDispatcher::dispatch`].
//! The device-control handlers (`ptz_move`, `set_stream_quality`,
//! `trigger_alarm_output`) are not implemented and always fail.

pub mod dispatcher;
mod handlers;

pub use dispatcher::{ActionContext, ActionDispatcher};
