//! `vms-actions` — action and device-control node handlers.
//!
//! Every handler receives a [`NodeInput`] and returns a [`NodeOutput`].
//! Handlers are dispatched through [`ActionDispatcher::dispatch`].
//!
//! # Sub-step status
//!
//! | Sub-step | Handlers implemented                                     |
//! |----------|----------------------------------------------------------|
//! | 6-1      | `delay`, `render_notification`                           |
//! | 6-2      | `extract_clip`, `snapshot`                               |
//! | 6-3      | `transcode`, `watermark`, `merge_clips`                  |
//! | 6-4      | `compress`, `encrypt`                                    |
//! | 6-5      | `start_recording`, `stop_recording`                      |
//! | 6-6      | `ptz_move`, `set_stream_quality`, `trigger_alarm_output` |

pub mod dispatcher;
mod handlers;

pub use dispatcher::{ActionContext, ActionDispatcher};
