//! The QUIC transport of the control channel, authenticated with a client
//! certificate made from the site key.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::SigningKey;
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, ConnectionError, Endpoint, SendStream};
use rand::seq::SliceRandom;
use reeframe_protocol::{read_message, write_message, CloseReason, Message, ALPN};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;
use tokio::time::{sleep, timeout};

use super::{Received, SessionEnd, Target, Timings};

pub(super) struct Transport {
    endpoint: Endpoint,
    target: Target,
}

impl Transport {
    pub(super) fn new(
        key: &SigningKey,
        target: Target,
        roots: RootCertStore,
    ) -> anyhow::Result<Self> {
        let mut endpoint = client_endpoint()?;
        endpoint.set_default_client_config(client_config(key, roots)?);
        Ok(Self { endpoint, target })
    }

    /// Connects and opens the control stream.
    pub(super) async fn connect(&self, timings: &Timings) -> Result<Channel, SessionEnd> {
        let (connection, addr) = self
            .handshake(timings)
            .await
            .map_err(SessionEnd::ConnectFailed)?;
        let (mut send, recv) = match connection.open_bi().await {
            Ok(streams) => streams,
            Err(e) => return Err(connection_end(&connection, e.to_string()).await),
        };
        // The Coordinator only sees the stream once data is sent on it, so the
        // first ping goes out right away.
        if let Err(e) = write_message(&mut send, &Message::Ping).await {
            return Err(connection_end(&connection, e.to_string()).await);
        }
        // A stream keeps a partly read frame across `select!` iterations,
        // unlike a `read_message` future that a firing timer would drop.
        let messages = futures::stream::unfold(recv, |mut recv| async move {
            let message = read_message(&mut recv).await.transpose()?;
            Some((message, recv))
        });
        Ok(Channel {
            connection,
            addr,
            send,
            messages: Box::pin(messages),
        })
    }

    async fn handshake(&self, timings: &Timings) -> anyhow::Result<(Connection, SocketAddr)> {
        let attempt = async {
            let mut addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((self.target.host.as_str(), self.target.port))
                    .await
                    .with_context(|| format!("cannot resolve {}", self.target.host))?
                    .collect();
            addrs.shuffle(&mut rand::thread_rng());
            race(
                &self.endpoint,
                addrs,
                &self.target.host,
                timings.race_stagger,
            )
            .await
        };
        timeout(timings.connect_timeout, attempt)
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "no address of {} answered in time",
                    self.target.host
                ))
            })
    }

    pub(super) async fn shutdown(&self) {
        self.endpoint
            .close(CloseReason::Closed.code().into(), b"shutdown");
        // Gives the close frame a moment to reach the Coordinator.
        let _ = timeout(Duration::from_secs(1), self.endpoint.wait_idle()).await;
    }
}

/// A UDP endpoint for both address families where the host has IPv6, so
/// racing can try every resolved address.
pub(super) fn client_endpoint() -> anyhow::Result<Endpoint> {
    Endpoint::client((std::net::Ipv6Addr::UNSPECIFIED, 0).into())
        .or_else(|_| Endpoint::client((std::net::Ipv4Addr::UNSPECIFIED, 0).into()))
        .context("cannot open a UDP socket for the control channel")
}

/// TLS 1.3 with the site key as client certificate. The Coordinator only
/// looks at the key, so the certificate is self-signed and its names do not
/// matter.
pub(super) fn client_config(
    key: &SigningKey,
    roots: RootCertStore,
) -> anyhow::Result<quinn::ClientConfig> {
    let pkcs8 = key.to_pkcs8_der()?;
    let key_pair = rcgen::KeyPair::try_from(pkcs8.as_bytes())?;
    let cert =
        rcgen::CertificateParams::new(vec!["reeframe-backend".into()])?.self_signed(&key_pair)?;
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8.as_bytes().to_vec()));

    // Chosen explicitly because the dependency tree also enables aws-lc-rs,
    // and rustls refuses to guess between two installed providers.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![CertificateDer::from(cert.der().to_vec())], private_key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.enable_early_data = false;
    Ok(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)?,
    )))
}

/// Connects to the first address that completes a handshake. Attempts start
/// `stagger` apart, or at once when the previous one failed, so an address
/// that never answers costs only `stagger`. Losing attempts are dropped,
/// which closes them.
pub(super) async fn race(
    endpoint: &Endpoint,
    addrs: Vec<SocketAddr>,
    server_name: &str,
    stagger: Duration,
) -> anyhow::Result<(Connection, SocketAddr)> {
    let attempt = |addr: SocketAddr| async move {
        let connecting = endpoint
            .connect(addr, server_name)
            .with_context(|| format!("cannot connect to {addr}"))?;
        let connection = connecting
            .await
            .with_context(|| format!("handshake with {addr} failed"))?;
        anyhow::Ok((connection, addr))
    };

    let mut addrs = addrs.into_iter();
    let mut attempts = FuturesUnordered::new();
    let mut last_error = anyhow::anyhow!("{server_name} resolved to no address");
    loop {
        if attempts.is_empty() {
            match addrs.next() {
                Some(addr) => attempts.push(attempt(addr)),
                None => return Err(last_error),
            }
        }
        let more = addrs.len() > 0;
        tokio::select! {
            result = attempts.next() => match result {
                Some(Ok(won)) => return Ok(won),
                Some(Err(e)) => last_error = e,
                None => {}
            },
            _ = sleep(stagger), if more => {
                if let Some(addr) = addrs.next() {
                    attempts.push(attempt(addr));
                }
            }
        }
    }
}

type Messages = Pin<Box<dyn Stream<Item = std::io::Result<Message>> + Send>>;

pub(super) struct Channel {
    connection: Connection,
    addr: SocketAddr,
    send: SendStream,
    messages: Messages,
}

impl Channel {
    pub(super) fn peer(&self) -> String {
        self.addr.to_string()
    }

    pub(super) async fn send(&mut self, message: &Message) -> Result<(), String> {
        write_message(&mut self.send, message)
            .await
            .map_err(|e| e.to_string())
    }

    pub(super) async fn recv(&mut self) -> Received {
        match self.messages.next().await {
            Some(Ok(message)) => Received::Message(message),
            Some(Err(e)) => Received::Ended(e.to_string()),
            None => Received::Ended("stream finished".into()),
        }
    }

    pub(super) fn close(&self, reason: CloseReason) {
        self.connection
            .close(reason.code().into(), reason.text().as_bytes());
    }

    pub(super) async fn end(&mut self, error: String) -> SessionEnd {
        connection_end(&self.connection, error).await
    }
}

/// Turns a failed read or write into the reason the connection ended. A
/// stream error usually arrives just before the close itself, so this waits
/// briefly for the close code.
async fn connection_end(connection: &Connection, error: String) -> SessionEnd {
    let closed = timeout(Duration::from_secs(1), connection.closed()).await;
    match closed {
        Ok(ConnectionError::ApplicationClosed(close)) => {
            match u32::try_from(close.error_code.into_inner()) {
                Ok(code) => SessionEnd::quic_close(code),
                Err(_) => SessionEnd::Lost(format!("closed with code {}", close.error_code)),
            }
        }
        Ok(other) => SessionEnd::Lost(other.to_string()),
        Err(_) => {
            connection.close(CloseReason::Protocol.code().into(), error.as_bytes());
            SessionEnd::Lost(error)
        }
    }
}
