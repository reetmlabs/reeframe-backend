use gstreamer as gst;
use gst::prelude::*;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use anyhow::{anyhow, Result};
use std::time::{Duration, Instant};
use tracing::info;
use crate::entities::{feed, settings};

pub struct Recorder {
    pipeline: gst::Pipeline,
    user_recording_valve: gst::Element,
    ai_recording_valve: gst::Element,
    ai_stop_time: Arc<Mutex<Option<Instant>>>,
}

impl Recorder {
    pub fn new(feed_model: &feed::Model, global_settings: &settings::Model) -> Result<Self> {
        gst::init()?;

        let rtsp_url = &feed_model.rtsp_url;
        let storage_path = &global_settings.storage_path;
        let chunk_duration_secs = global_settings.recording_chunk_duration_mins * 60;
        let pre_event_cache_duration = global_settings.pre_event_cache_duration_secs;

        // Quality configuration
        let encoder_settings = match feed_model.recording_quality.as_deref() {
            Some("high") => "bitrate=4000 speed-preset=ultrafast",
            Some("medium") => "bitrate=2000 speed-preset=ultrafast",
            Some("low") => "bitrate=1000 speed-preset=ultrafast",
            _ => "bitrate=2000 speed-preset=ultrafast",
        };

        // GStreamer Pipeline Construction
        // We use tee to split the source into multiple branches:
        // 1. User-commanded recording branch (with valve)
        // 2. AI-triggered recording branch (with valve and queue for caching)
        // 3. Restreaming branch (optional)

        let mut pipeline_str = format!(
            "rtspsrc location={} latency={} name=src_{} ! decodebin ! videoconvert ! x264enc {} ! tee name=t_{} ",
            rtsp_url,
            global_settings.gst_latency_ms,
            feed_model.id,
            encoder_settings,
            feed_model.id
        );

        // 1. User recording branch
        pipeline_str.push_str(&format!(
            "t_{}. ! queue ! valve name=u_valve_{} ! splitmuxsink location={}/feed_{}_user_%05d.mp4 max-size-time={} ",
            feed_model.id,
            feed_model.id,
            storage_path,
            feed_model.id,
            chunk_duration_secs as u64 * 1_000_000_000
        ));

        // 2. AI recording branch
        // The queue here provides the pre-event caching.
        pipeline_str.push_str(&format!(
            "t_{}. ! queue max-size-buffers=0 max-size-time={} max-size-bytes=0 ! valve name=ai_valve_{} ! splitmuxsink location={}/feed_{}_ai_%05d.mp4 max-size-time={} ",
            feed_model.id,
            pre_event_cache_duration as u64 * 1_000_000_000,
            feed_model.id,
            storage_path,
            feed_model.id,
            chunk_duration_secs as u64 * 1_000_000_000
        ));

        // 3. Restreaming branch
        if feed_model.restream_enabled {
            if let Some(port) = feed_model.restream_port {
                pipeline_str.push_str(&format!(
                    "t_{}. ! queue ! mpegtsmux ! udpsink host=127.0.0.1 port={} ",
                    feed_model.id,
                    port
                ));
            }
        }

        let pipeline = gst::parse::launch(&pipeline_str)?
            .dynamic_cast::<gst::Pipeline>()
            .map_err(|_| anyhow!("Failed to cast to pipeline"))?;

        let user_recording_valve = pipeline
            .by_name(&format!("u_valve_{}", feed_model.id))
            .ok_or_else(|| anyhow!("Failed to find u_valve for feed {}", feed_model.id))?;
        
        let ai_recording_valve = pipeline
            .by_name(&format!("ai_valve_{}", feed_model.id))
            .ok_or_else(|| anyhow!("Failed to find ai_valve for feed {}", feed_model.id))?;

        // Initialize valves to drop (not recording)
        user_recording_valve.set_property("drop", true);
        ai_recording_valve.set_property("drop", true);

        let ai_stop_time = Arc::new(Mutex::new(None));

        let recorder = Self {
            pipeline,
            user_recording_valve,
            ai_recording_valve,
            ai_stop_time,
        };

        recorder.start_pipeline()?;

        Ok(recorder)
    }

    fn start_pipeline(&self) -> Result<()> {
        self.pipeline.set_state(gst::State::Playing)?;
        Ok(())
    }

    pub fn stop_pipeline(&self) -> Result<()> {
        self.pipeline.set_state(gst::State::Null)?;
        Ok(())
    }

    pub fn start_user_recording(&self) -> Result<()> {
        info!("Starting user-commanded recording");
        self.user_recording_valve.set_property("drop", false);
        Ok(())
    }

    pub fn stop_user_recording(&self) -> Result<()> {
        info!("Stopping user-commanded recording");
        self.user_recording_valve.set_property("drop", true);
        Ok(())
    }

    pub fn trigger_ai_recording(&self, duration: Duration) -> Result<()> {
        info!("Triggering AI recording for {:?}", duration);
        let mut stop_time_lock = self.ai_stop_time.lock().unwrap();
        let new_stop_time = Instant::now() + duration;
        
        if let Some(current_stop_time) = *stop_time_lock {
            if new_stop_time > current_stop_time {
                *stop_time_lock = Some(new_stop_time);
            }
        } else {
            *stop_time_lock = Some(new_stop_time);
            self.ai_recording_valve.set_property("drop", false);
            
            // Spawn a thread or task to stop AI recording after duration
            let ai_valve = self.ai_recording_valve.clone();
            let stop_time_shared = Arc::clone(&self.ai_stop_time);
            
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_millis(500));
                    let mut lock = stop_time_shared.lock().unwrap();
                    if let Some(stop_at) = *lock {
                        if Instant::now() >= stop_at {
                            info!("AI recording duration reached, stopping");
                            ai_valve.set_property("drop", true);
                            *lock = None;
                            break;
                        }
                    } else {
                        break;
                    }
                }
            });
        }
        
        Ok(())
    }
}

pub struct RecorderManager {
    recorders: Arc<Mutex<HashMap<i32, Arc<Recorder>>>>,
}

impl RecorderManager {
    pub fn new() -> Self {
        Self {
            recorders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_or_create_recorder(&self, feed_model: &feed::Model, settings_model: &settings::Model) -> Result<Arc<Recorder>> {
        let mut recorders = self.recorders.lock().unwrap();
        if let Some(recorder) = recorders.get(&feed_model.id) {
            Ok(Arc::clone(recorder))
        } else {
            let recorder = Arc::new(Recorder::new(feed_model, settings_model)?);
            recorders.insert(feed_model.id, Arc::clone(&recorder));
            Ok(recorder)
        }
    }

    pub fn start_recording(&self, feed_model: &feed::Model, settings_model: &settings::Model) -> Result<()> {
        let recorder = self.get_or_create_recorder(feed_model, settings_model)?;
        recorder.start_user_recording()
    }

    pub fn stop_recording(&self, feed_id: i32) -> Result<()> {
        let recorders = self.recorders.lock().unwrap();
        if let Some(recorder) = recorders.get(&feed_id) {
            recorder.stop_user_recording()?;
            Ok(())
        } else {
            Err(anyhow!("No recorder found for feed {}", feed_id))
        }
    }

    pub fn trigger_ai_recording(&self, feed_model: &feed::Model, settings_model: &settings::Model, duration: Duration) -> Result<()> {
        let recorder = self.get_or_create_recorder(feed_model, settings_model)?;
        recorder.trigger_ai_recording(duration)
    }

    pub fn ensure_connected(&self, feed_model: &feed::Model, settings_model: &settings::Model) -> Result<()> {
        self.get_or_create_recorder(feed_model, settings_model)?;
        Ok(())
    }
}

impl Clone for RecorderManager {
    fn clone(&self) -> Self {
        Self {
            recorders: Arc::clone(&self.recorders),
        }
    }
}
