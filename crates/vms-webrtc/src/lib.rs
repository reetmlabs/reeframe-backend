//! WebRTC sessions requested by the Coordinator over the control channel.
//!
//! The Coordinator relays a client's offer as `session_offer`. The
//! [`SessionManager`] checks the site token, builds a webrtcbin pipeline,
//! answers with `session_answer`, applies the client's trickled candidates
//! and tears the session down on `session_close`, ICE failure or shutdown.
//! Sessions do not depend on the control connection, so a reconnect leaves
//! them running.

extern crate gstreamer as gst;
extern crate gstreamer_sdp as gst_sdp;
extern crate gstreamer_webrtc as gst_webrtc;

mod ice;
mod offer;
mod session;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reeframe_protocol::{session_error, IceServer, Message};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use vms_core::AuthProvider;

use crate::session::{Failure, Media};

/// Settings of the session manager.
#[derive(Debug, Clone)]
pub struct Config {
    /// Sessions beyond this many are refused with `too_many_sessions`.
    pub max_sessions: usize,
    /// How long the answer waits for ICE gathering before it is sent with
    /// the candidates found so far.
    pub gathering_limit: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_sessions: 16,
            gathering_limit: Duration::from_secs(2),
        }
    }
}

/// Runs the WebRTC sessions of this backend. Cheap to clone.
#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<Inner>,
}

struct Inner {
    config: Config,
    /// Verifies `site_token`; the Coordinator JWKS provider in production.
    verifier: Arc<dyn AuthProvider>,
    /// Messages for the Coordinator, sent over whatever control connection
    /// is up.
    outbound: mpsc::UnboundedSender<Message>,
    sessions: Mutex<HashMap<String, Entry>>,
    failures: mpsc::UnboundedSender<Failure>,
    failures_rx: Mutex<Option<mpsc::UnboundedReceiver<Failure>>>,
}

/// A session from the moment its offer is accepted. `media` is set once the
/// pipeline exists, and candidates wait in `pending` until the offer is
/// applied.
#[derive(Default)]
struct Entry {
    media: Option<Arc<Media>>,
    remote_set: bool,
    pending: Vec<(u32, String)>,
}

/// Why setting up a session stopped.
enum SetupError {
    /// The session was closed while it was being set up.
    Closed,
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for SetupError {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

impl SessionManager {
    pub fn new(
        config: Config,
        verifier: Arc<dyn AuthProvider>,
        outbound: mpsc::UnboundedSender<Message>,
    ) -> anyhow::Result<Self> {
        gst::init()?;
        let (failures, failures_rx) = mpsc::unbounded_channel();
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                verifier,
                outbound,
                sessions: Mutex::new(HashMap::new()),
                failures,
                failures_rx: Mutex::new(Some(failures_rx)),
            }),
        })
    }

    /// Number of sessions, including those still being set up.
    pub fn session_count(&self) -> usize {
        self.inner.sessions.lock().unwrap().len()
    }

    /// Handles session messages from `inbound` until `shutdown` is
    /// cancelled, then stops every session.
    pub async fn run(
        self,
        mut inbound: mpsc::UnboundedReceiver<Message>,
        shutdown: CancellationToken,
    ) {
        let mut failures = self
            .inner
            .failures_rx
            .lock()
            .unwrap()
            .take()
            .expect("run is called once");
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                Some(message) = inbound.recv() => self.handle(message),
                Some(failure) = failures.recv() => self.fail(failure),
            }
        }
        let ids: Vec<String> = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let stops: Vec<_> = ids.iter().filter_map(|id| self.remove(id)).collect();
        for stop in stops {
            let _ = stop.await;
        }
        tracing::info!("WebRTC sessions stopped");
    }

    /// Handles one message from the Coordinator. Offers are set up in the
    /// background, so candidates and closes for other sessions are not held up.
    pub fn handle(&self, message: Message) {
        match message {
            Message::SessionOffer {
                session_id,
                sdp,
                ice_servers,
                site_token,
            } => {
                tokio::spawn(self.clone().start(session_id, sdp, ice_servers, site_token));
            }
            Message::SessionCandidate {
                session_id,
                candidate,
                sdp_mid: _,
                sdp_mline_index,
            } => self.add_candidate(&session_id, sdp_mline_index, candidate),
            Message::SessionClose { session_id, reason } => {
                if self.remove(&session_id).is_some() {
                    tracing::info!(
                        session_id,
                        reason,
                        "WebRTC session closed by the Coordinator"
                    );
                }
            }
            _ => {}
        }
    }

    async fn start(
        self,
        session_id: String,
        sdp: String,
        ice_servers: Vec<IceServer>,
        site_token: String,
    ) {
        if let Err(e) = self.inner.verifier.verify_token(&site_token).await {
            tracing::warn!(session_id, error = %e, "Refusing a session with an invalid site token");
            return self.send_error(
                session_id,
                session_error::INVALID_TOKEN,
                "invalid site token",
            );
        }
        let offer = match offer::parse(&sdp) {
            Ok(offer) => offer,
            Err(e) => {
                tracing::warn!(session_id, error = e, "Refusing an unusable offer");
                return self.send_error(session_id, session_error::INVALID_OFFER, e);
            }
        };
        {
            let mut sessions = self.inner.sessions.lock().unwrap();
            if sessions.contains_key(&session_id) {
                drop(sessions);
                return self.send_error(
                    session_id,
                    session_error::INVALID_OFFER,
                    "a session with this id already exists",
                );
            }
            if sessions.len() >= self.inner.config.max_sessions {
                drop(sessions);
                tracing::warn!(
                    session_id,
                    "Refusing a session, the session limit is reached"
                );
                return self.send_error(
                    session_id,
                    session_error::TOO_MANY_SESSIONS,
                    "this backend runs as many sessions as it allows",
                );
            }
            sessions.insert(session_id.clone(), Entry::default());
        }

        match self.set_up(&session_id, offer, &ice_servers).await {
            Ok(sdp) => {
                tracing::info!(session_id, "WebRTC session answered");
                let _ = self
                    .inner
                    .outbound
                    .send(Message::SessionAnswer { session_id, sdp });
            }
            Err(SetupError::Closed) => {}
            Err(SetupError::Failed(e)) => {
                tracing::warn!(
                    session_id,
                    error = format!("{e:#}"),
                    "WebRTC session setup failed"
                );
                if self.remove(&session_id).is_some() {
                    self.send_close(session_id, format!("setup failed: {e:#}"));
                }
            }
        }
    }

    async fn set_up(
        &self,
        session_id: &str,
        offer: offer::Offer,
        ice_servers: &[IceServer],
    ) -> Result<String, SetupError> {
        let media = Arc::new(Media::build(
            session_id,
            offer.h264_payload_type,
            ice_servers,
            self.inner.failures.clone(),
        )?);
        {
            let mut sessions = self.inner.sessions.lock().unwrap();
            let Some(entry) = sessions.get_mut(session_id) else {
                drop(sessions);
                stop_in_background(media);
                return Err(SetupError::Closed);
            };
            entry.media = Some(media.clone());
        }
        media.start()?;
        media.set_remote_offer(offer).await?;
        {
            let mut sessions = self.inner.sessions.lock().unwrap();
            let entry = sessions.get_mut(session_id).ok_or(SetupError::Closed)?;
            entry.remote_set = true;
            for (mline_index, candidate) in entry.pending.drain(..) {
                media.add_candidate(mline_index, &candidate);
            }
        }
        let answer = media.answer(self.inner.config.gathering_limit).await?;
        if !self.inner.sessions.lock().unwrap().contains_key(session_id) {
            return Err(SetupError::Closed);
        }
        Ok(answer)
    }

    fn add_candidate(&self, session_id: &str, mline_index: Option<u32>, candidate: String) {
        // An empty candidate only marks the end of the client's candidates.
        if candidate.is_empty() {
            return;
        }
        // With max-bundle every m-line shares one transport, so the first
        // one is right when the client leaves the index out.
        let mline_index = mline_index.unwrap_or(0);
        let mut sessions = self.inner.sessions.lock().unwrap();
        let Some(entry) = sessions.get_mut(session_id) else {
            tracing::debug!(session_id, "Candidate for an unknown session");
            return;
        };
        match (&entry.media, entry.remote_set) {
            (Some(media), true) => media.add_candidate(mline_index, &candidate),
            _ => entry.pending.push((mline_index, candidate)),
        }
    }

    fn fail(&self, failure: Failure) {
        if self.remove(&failure.session_id).is_some() {
            tracing::warn!(
                session_id = failure.session_id,
                reason = failure.reason,
                "WebRTC session failed"
            );
            self.send_close(failure.session_id, failure.reason);
        }
    }

    /// Forgets a session and stops its pipeline in the background. Returns
    /// `None` if there was no such session.
    fn remove(&self, session_id: &str) -> Option<tokio::task::JoinHandle<()>> {
        let entry = self.inner.sessions.lock().unwrap().remove(session_id)?;
        Some(match entry.media {
            Some(media) => stop_in_background(media),
            None => tokio::spawn(async {}),
        })
    }

    fn send_error(&self, session_id: String, code: &str, message: impl Into<String>) {
        let _ = self.inner.outbound.send(Message::SessionError {
            session_id,
            code: code.to_owned(),
            message: message.into(),
        });
    }

    fn send_close(&self, session_id: String, reason: String) {
        let _ = self
            .inner
            .outbound
            .send(Message::SessionClose { session_id, reason });
    }
}

/// Stopping a pipeline blocks until GStreamer releases it.
fn stop_in_background(media: Arc<Media>) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || media.stop())
}
