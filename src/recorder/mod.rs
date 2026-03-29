use gstreamer as gst;
use gst::prelude::*;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use anyhow::{anyhow, Result};

pub struct Recorder {
    pipeline: gst::Pipeline,
}

impl Recorder {
    pub fn new(rtsp_url: &str, output_path: &str) -> Result<Self> {
        gst::init()?;

        // Example pipeline: rtspsrc -> decodebin -> videoconvert -> x264enc -> mp4mux -> filesink
        // Note: For simplicity, we'll use a string-based pipeline description.
        // In a production app, you'd probably want to build it element by element or handle caps more carefully.
        let pipeline_str = format!(
            "rtspsrc location={} latency=100 ! decodebin ! videoconvert ! x264enc ! mp4mux ! filesink location={}",
            rtsp_url, output_path
        );

        let pipeline = gst::parse::launch(&pipeline_str)?
            .dynamic_cast::<gst::Pipeline>()
            .map_err(|_| anyhow!("Failed to cast to pipeline"))?;

        Ok(Self { pipeline })
    }

    pub fn start(&self) -> Result<()> {
        self.pipeline.set_state(gst::State::Playing)?;
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.pipeline.set_state(gst::State::Null)?;
        Ok(())
    }
}

pub struct RecorderManager {
    recorders: Arc<Mutex<HashMap<i32, Recorder>>>,
}

impl RecorderManager {
    pub fn new() -> Self {
        Self {
            recorders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start_recording(&self, feed_id: i32, rtsp_url: &str) -> Result<()> {
        let mut recorders = self.recorders.lock().unwrap();
        if recorders.contains_key(&feed_id) {
            return Err(anyhow!("Recording already in progress for feed {}", feed_id));
        }

        let output_path = format!("recording_feed_{}.mp4", feed_id);
        let recorder = Recorder::new(rtsp_url, &output_path)?;
        recorder.start()?;
        recorders.insert(feed_id, recorder);
        Ok(())
    }

    pub fn stop_recording(&self, feed_id: i32) -> Result<()> {
        let mut recorders = self.recorders.lock().unwrap();
        if let Some(recorder) = recorders.remove(&feed_id) {
            recorder.stop()?;
            Ok(())
        } else {
            Err(anyhow!("No recording found for feed {}", feed_id))
        }
    }
}

impl Clone for RecorderManager {
    fn clone(&self) -> Self {
        Self {
            recorders: Arc::clone(&self.recorders),
        }
    }
}
