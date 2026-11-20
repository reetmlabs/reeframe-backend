//! The WebSocket (WSS) transport of the control channel, for networks that
//! block UDP. TLS here only authenticates the Coordinator; the backend proves
//! its key by signing a challenge after the WebSocket opens.

use std::sync::Arc;

use anyhow::Context;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use futures::{SinkExt, StreamExt};
use reeframe_protocol::{auth_payload, CloseReason, Message, WS_PATH};
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::net::TcpStream;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::WebSocketStream;

use super::{Received, SessionEnd, Target, Timings};
use crate::proxy::OutboundProxy;

type Socket = WebSocketStream<TlsStream<TcpStream>>;

pub(super) struct Transport {
    key: SigningKey,
    target: Target,
    proxy: Option<OutboundProxy>,
    tls: TlsConnector,
}

impl Transport {
    pub(super) fn new(
        key: &SigningKey,
        target: Target,
        proxy: Option<OutboundProxy>,
        roots: RootCertStore,
    ) -> anyhow::Result<Self> {
        // Same explicit provider as QUIC. TLS 1.2 stays allowed because the
        // WebSocket may end at a reverse proxy in front of the Coordinator.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            key: key.clone(),
            target,
            proxy,
            tls: TlsConnector::from(Arc::new(tls)),
        })
    }

    /// Opens the WebSocket and logs in.
    pub(super) async fn connect(&self, timings: &Timings) -> Result<Channel, SessionEnd> {
        let mut socket = timeout(timings.connect_timeout, self.open())
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "no WebSocket to {} in time",
                    self.target.host
                ))
            })
            .map_err(SessionEnd::ConnectFailed)?;
        self.login(&mut socket, timings).await?;
        Ok(Channel {
            socket,
            peer: self.url(),
            close_code: None,
        })
    }

    fn url(&self) -> String {
        let host = if self.target.host.contains(':') {
            format!("[{}]", self.target.host)
        } else {
            self.target.host.clone()
        };
        match self.target.port {
            443 => format!("wss://{host}{WS_PATH}"),
            port => format!("wss://{host}:{port}{WS_PATH}"),
        }
    }

    async fn open(&self) -> anyhow::Result<Socket> {
        let (host, port) = (self.target.host.as_str(), self.target.port);
        let tcp = match &self.proxy {
            Some(proxy) => proxy.connect(host, port).await?,
            None => TcpStream::connect((host, port))
                .await
                .with_context(|| format!("cannot connect to {host}:{port}"))?,
        };
        let server_name = ServerName::try_from(host.to_owned())
            .with_context(|| format!("{host} is not a valid TLS server name"))?;
        let tls = self
            .tls
            .connect(server_name, tcp)
            .await
            .with_context(|| format!("TLS handshake with {host} failed"))?;
        let (socket, _) = tokio_tungstenite::client_async(self.url(), tls)
            .await
            .with_context(|| format!("WebSocket upgrade at {} failed", self.url()))?;
        Ok(socket)
    }

    /// Answers the Coordinator's challenge with a signature over
    /// `auth_payload` and waits for `auth_ok`.
    async fn login(&self, socket: &mut Socket, timings: &Timings) -> Result<(), SessionEnd> {
        let deadline = Instant::now() + timings.login_timeout;
        let nonce = loop {
            match next_message(socket, deadline, "challenge").await? {
                Message::Challenge { nonce } => break nonce,
                _ => continue,
            }
        };

        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let signature = self.key.sign(&auth_payload(&nonce, &self.target.host));
        let auth = Message::Auth {
            public_key: b64.encode(self.key.verifying_key().as_bytes()),
            signature: b64.encode(signature.to_bytes()),
        };
        socket
            .send(Frame::Text(auth.to_json()))
            .await
            .map_err(|e| SessionEnd::Lost(e.to_string()))?;

        let deadline = Instant::now() + timings.login_timeout;
        loop {
            if next_message(socket, deadline, "auth_ok").await? == Message::AuthOk {
                return Ok(());
            }
        }
    }
}

/// The next control message during login. A close frame ends the attempt
/// with its code, so a refused key gets the same retry policy as on QUIC.
async fn next_message(
    socket: &mut Socket,
    deadline: Instant,
    expected: &str,
) -> Result<Message, SessionEnd> {
    loop {
        let frame = match timeout_at(deadline, socket.next()).await {
            Err(_) => {
                return Err(SessionEnd::ConnectFailed(anyhow::anyhow!(
                    "no {expected} from the Coordinator in time"
                )))
            }
            Ok(None) => return Err(SessionEnd::Lost(format!("closed before {expected}"))),
            Ok(Some(Err(e))) => return Err(SessionEnd::Lost(e.to_string())),
            Ok(Some(Ok(frame))) => frame,
        };
        match frame {
            Frame::Text(text) => {
                return Message::from_json(&text).map_err(|e| {
                    SessionEnd::ConnectFailed(anyhow::anyhow!("invalid message during login: {e}"))
                })
            }
            Frame::Close(Some(close)) => return Err(SessionEnd::ws_close(close.code.into())),
            Frame::Close(None) => {
                return Err(SessionEnd::Lost(format!("closed before {expected}")))
            }
            _ => continue,
        }
    }
}

pub(super) struct Channel {
    socket: Socket,
    peer: String,
    /// Code of the close frame the Coordinator sent, once it did.
    close_code: Option<u16>,
}

impl Channel {
    pub(super) fn peer(&self) -> String {
        self.peer.clone()
    }

    pub(super) async fn send(&mut self, message: &Message) -> Result<(), String> {
        self.socket
            .send(Frame::Text(message.to_json()))
            .await
            .map_err(|e| e.to_string())
    }

    pub(super) async fn recv(&mut self) -> Received {
        match self.socket.next().await {
            Some(Ok(Frame::Text(text))) => match Message::from_json(&text) {
                Ok(message) => Received::Message(message),
                Err(e) => Received::Ended(format!("invalid message: {e}")),
            },
            Some(Ok(Frame::Close(close))) => {
                self.close_code = close.map(|close| close.code.into());
                Received::Ended("closed by the Coordinator".into())
            }
            Some(Ok(_)) => Received::Other,
            Some(Err(e)) => Received::Ended(e.to_string()),
            None => Received::Ended("connection closed".into()),
        }
    }

    pub(super) async fn close(&mut self, reason: CloseReason) {
        let frame = CloseFrame {
            code: CloseCode::from(reason.ws_code()),
            reason: reason.text().into(),
        };
        // Bounded, because a dead connection would never confirm the close.
        let _ = timeout(
            std::time::Duration::from_secs(1),
            self.socket.close(Some(frame)),
        )
        .await;
    }

    pub(super) fn end(&self, error: String) -> SessionEnd {
        match self.close_code {
            Some(code) => SessionEnd::ws_close(code),
            None => SessionEnd::Lost(error),
        }
    }
}
