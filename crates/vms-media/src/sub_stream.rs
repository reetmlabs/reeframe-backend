//! Per-camera sub-stream GStreamer pipeline.
//!
//! Uses the same codec detection and reconnect monitor as the main pipeline in
//! `camera_stream.rs`, without a recording branch, so nothing here is written
//! to disk. It exists only when a camera has a `sub_rtsp_url`. Its `tee` feeds
//! the sub-quality restream and, by default, motion detection, so all those
//! consumers share one low-resolution camera connection.

use gstreamer::prelude::*;
use uuid::Uuid;
use vms_core::VmsError;

use crate::camera_stream::{
    codec_for, stall_check_period, take_queued_messages, FlowWatchdog, ReconnectPolicy, WaitPlan,
};

fn tee_name(camera_id: Uuid) -> String {
    format!("cam_{}_subtee", camera_id.as_simple())
}

/// Build a per-camera sub-stream pipeline: `rtspsrc -> [depay|parse] -> tee`.
///
/// As in `camera_stream::build_camera_stream`, the depayloader and parser are
/// created once the SDP reveals the encoding and are re-linked on reconnect
/// instead of being re-created.
pub(crate) fn build_sub_stream(
    camera_id: Uuid,
    sub_rtsp_url: &str,
) -> Result<gstreamer::Pipeline, VmsError> {
    let gst_pipeline = gstreamer::Pipeline::new();
    let id = camera_id.as_simple().to_string();

    let src = gstreamer::ElementFactory::make("rtspsrc")
        .name(format!("cam_{id}_subsrc"))
        .property("location", sub_rtsp_url)
        .property("latency", 200u32)
        .build()
        .map_err(|e| VmsError::Media(format!("sub-stream rtspsrc: {e}")))?;

    // Consumers attach at runtime, so the tee must tolerate having none.
    let tee = gstreamer::ElementFactory::make("tee")
        .name(tee_name(camera_id))
        .property("allow-not-linked", true)
        .build()
        .map_err(|e| VmsError::Media(format!("sub-stream tee: {e}")))?;

    gst_pipeline
        .add_many([&src, &tee])
        .map_err(|e| VmsError::Media(format!("sub-stream add_many: {e}")))?;

    let pipeline_weak = gst_pipeline.downgrade();
    let tee_weak = tee.downgrade();
    let cam_id = camera_id;

    src.connect_pad_added(move |_src, src_pad| {
        let Some(caps) = src_pad.current_caps() else {
            return;
        };
        let Some(structure) = caps.structure(0) else {
            return;
        };
        if !structure.name().starts_with("application/x-rtp") {
            return;
        }
        if structure.get::<&str>("media").ok() == Some("audio") {
            return;
        }
        let Ok(encoding) = structure.get::<&str>("encoding-name") else {
            return;
        };
        let encoding = encoding.to_owned();

        let Some(gst_pipeline) = pipeline_weak.upgrade() else {
            return;
        };
        let Some(tee) = tee_weak.upgrade() else {
            return;
        };

        let depay_name = format!("cam_{}_subdepay", cam_id.as_simple());
        let parse_name = format!("cam_{}_subparse", cam_id.as_simple());

        // -- Reconnect path: elements exist, just re-link the src pad --
        if let Some(depay) = gst_pipeline.by_name(&depay_name) {
            let Some(sink) = depay.static_pad("sink") else {
                return;
            };
            if !sink.is_linked() {
                if let Err(e) = src_pad.link(&sink) {
                    tracing::error!(camera_id = %cam_id, "sub-stream re-link rtspsrc->depay: {e}");
                }
            }
            return;
        }

        // -- First connection: create depay + parse for the negotiated codec --
        let Some(codec) = codec_for(&encoding) else {
            tracing::warn!(camera_id = %cam_id, encoding, "sub-stream: unsupported RTP encoding, camera feed ignored");
            return;
        };

        let depay = match gstreamer::ElementFactory::make(codec.depay_factory)
            .name(&depay_name)
            .build()
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(camera_id = %cam_id, "sub-stream create {}: {e}", codec.depay_factory);
                return;
            }
        };
        let parse = match gstreamer::ElementFactory::make(codec.parse_factory)
            .name(&parse_name)
            .build()
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(camera_id = %cam_id, "sub-stream create {}: {e}", codec.parse_factory);
                return;
            }
        };

        if let Err(e) = gst_pipeline.add_many([&depay, &parse]) {
            tracing::error!(camera_id = %cam_id, "sub-stream add depay+parse: {e}");
            return;
        }

        if let Err(e) = gstreamer::Element::link_many([&depay, &parse, &tee]) {
            tracing::error!(camera_id = %cam_id, "sub-stream link depay->parse->tee: {e}");
            // Remove the half-wired elements so the next reconnect retries
            // the link instead of assuming they are already wired.
            for el in [&depay, &parse] {
                el.set_state(gstreamer::State::Null).ok();
                gst_pipeline.remove(el).ok();
            }
            return;
        }

        for el in [&depay, &parse] {
            el.sync_state_with_parent().ok();
        }

        let Some(sink) = depay.static_pad("sink") else {
            return;
        };
        if let Err(e) = src_pad.link(&sink) {
            tracing::error!(camera_id = %cam_id, encoding, "sub-stream rtspsrc->depay link: {e}");
            return;
        }

        tracing::info!(camera_id = %cam_id, encoding, "Sub-stream codec wired");
    });

    Ok(gst_pipeline)
}

/// Reconnect monitor for the sub-stream pipeline.
///
/// Uses the same backoff and circuit breaker as `camera_stream::spawn_monitor`
/// (see `ReconnectPolicy`), including the restart after `stall_timeout`
/// without data. There is no splitmuxsink here, so a reconnect is a plain
/// `Null` to `Playing` cycle.
pub(crate) fn spawn_sub_monitor(
    camera_id: Uuid,
    gst_pipeline: gstreamer::Pipeline,
    stall_timeout: std::time::Duration,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use futures::StreamExt as _;
        use gstreamer::MessageView;

        let bus = gst_pipeline.bus().expect("pipeline bus missing");
        let mut bus_stream = bus.stream();
        let mut policy = ReconnectPolicy::new();
        let watchdog = FlowWatchdog::install(&gst_pipeline, &tee_name(camera_id));
        let mut stall_check = tokio::time::interval(stall_check_period(stall_timeout));

        'outer: loop {
            let needs_reconnect = 'watch: loop {
                tokio::select! {
                    maybe_msg = bus_stream.next() => {
                        let Some(msg) = maybe_msg else { break 'watch false };
                        match msg.view() {
                            MessageView::Error(err) => {
                                tracing::error!(
                                    camera_id = %camera_id,
                                    error = %err.error(),
                                    debug = ?err.debug(),
                                    "Sub-stream GStreamer error, will reconnect",
                                );
                                break 'watch true;
                            }
                            MessageView::Eos(_) => {
                                tracing::warn!(camera_id = %camera_id, "Sub-stream RTSP EOS, will reconnect");
                                break 'watch true;
                            }
                            MessageView::Warning(w) => {
                                tracing::warn!(
                                    camera_id = %camera_id,
                                    warning = %w.error(),
                                    "Sub-stream GStreamer warning",
                                );
                            }
                            _ => {}
                        }
                    }
                    _ = stall_check.tick() => {
                        let idle = watchdog.as_ref().map_or(std::time::Duration::ZERO, FlowWatchdog::idle);
                        if idle >= stall_timeout {
                            tracing::warn!(
                                camera_id = %camera_id,
                                idle_secs = idle.as_secs(),
                                "No data from the camera's sub stream, will reconnect",
                            );
                            break 'watch true;
                        }
                    }
                    _ = &mut shutdown_rx => break 'outer,
                }
            };

            if !needs_reconnect {
                break;
            }

            gst_pipeline.set_state(gstreamer::State::Null).ok();

            'reconnect: loop {
                let wait_plan = policy.on_failure();
                match wait_plan {
                    WaitPlan::Backoff(d) => {
                        tracing::info!(
                            camera_id = %camera_id,
                            backoff_secs = d.as_secs(),
                            consecutive_failures = policy.consecutive_failures(),
                            "Sub-stream reconnecting",
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(d) => {}
                            _ = &mut shutdown_rx => break 'outer,
                        }
                    }
                    WaitPlan::CircuitOpen(cooldown) => {
                        tracing::error!(
                            camera_id = %camera_id,
                            consecutive_failures = policy.consecutive_failures(),
                            cooldown_secs = cooldown.as_secs(),
                            "Sub-stream circuit breaker open, too many reconnect failures \
                             in a row, cooling down before the next attempt",
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(cooldown) => {}
                            _ = &mut shutdown_rx => break 'outer,
                        }
                    }
                }

                // An error or EOS of the stopped session must not end the new one.
                take_queued_messages(&mut bus_stream);

                match gst_pipeline.set_state(gstreamer::State::Playing) {
                    Ok(_) => {
                        tracing::info!(camera_id = %camera_id, "Sub-stream restarted");
                        policy.record_attempt();
                        if let Some(w) = &watchdog {
                            w.reset();
                        }
                        break 'reconnect;
                    }
                    Err(e) => {
                        tracing::error!(camera_id = %camera_id, "Sub-stream restart failed: {e}");
                        continue 'reconnect;
                    }
                }
            }
        }

        gst_pipeline.set_state(gstreamer::State::Null).ok();
        tracing::info!(camera_id = %camera_id, "Sub-stream monitor exited");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// On a current-thread runtime the monitor cannot run between the two
    /// errors, so the second one is still queued when it stops the pipeline.
    #[tokio::test]
    async fn an_error_of_the_stopped_session_does_not_end_the_next_one() {
        gstreamer::init().unwrap();
        let pipeline = gstreamer::parse::launch("fakesrc is-live=true ! fakesink name=sink")
            .unwrap()
            .downcast::<gstreamer::Pipeline>()
            .unwrap();
        // Each start of the pipeline sends one segment to the sink.
        let starts = Arc::new(AtomicU32::new(0));
        let counted = starts.clone();
        let sink = pipeline.by_name("sink").unwrap();
        sink.static_pad("sink").unwrap().add_probe(
            gstreamer::PadProbeType::EVENT_DOWNSTREAM,
            move |_, info| {
                if info
                    .event()
                    .is_some_and(|e| e.type_() == gstreamer::EventType::Segment)
                {
                    counted.fetch_add(1, Ordering::SeqCst);
                }
                gstreamer::PadProbeReturn::Ok
            },
        );
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let monitor = spawn_sub_monitor(
            Uuid::new_v4(),
            pipeline.clone(),
            Duration::from_secs(60),
            shutdown_rx,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(starts.load(Ordering::SeqCst), 1);

        for text in ["first", "second"] {
            gstreamer::element_error!(pipeline, gstreamer::ResourceError::Write, ("{}", text));
        }
        // The first backoff is 2 s and a second one would last until 6 s.
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(
            pipeline.current_state(),
            gstreamer::State::Playing,
            "the restarted pipeline was stopped again"
        );
        assert_eq!(starts.load(Ordering::SeqCst), 2);

        shutdown_tx.send(()).unwrap();
        monitor.await.unwrap();
    }
}
