use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use base64::Engine;
use ed25519_dalek::{Signature, VerifyingKey};
use futures::{SinkExt, StreamExt};
use rand::rngs::OsRng;
use rand::RngCore;
use reeframe_protocol::{auth_payload, WS_PATH};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::WebSocketStream;

use super::tests::{fast_timings, wait_until};
use super::*;

/// What the stand-in Coordinator does after the WebSocket opens.
#[derive(Clone, Copy)]
enum Behavior {
    /// Logs in and answers pings.
    Pong,
    /// Never sends a challenge.
    NoChallenge,
    /// Logs in, then closes with this WebSocket code.
    CloseAfterLogin(u16),
}

#[derive(Default)]
struct Stats {
    logins: AtomicUsize,
    pings: AtomicUsize,
    rejected: AtomicBool,
    wrong_path: AtomicBool,
    /// Close code the client sent, plus one, so zero means none yet.
    client_close: AtomicU32,
}

struct WsServer {
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    stats: Arc<Stats>,
}

impl WsServer {
    async fn start(behavior: Behavior, site_key: VerifyingKey) -> Self {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = generated.cert.der().clone();
        let key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
        Self::start_with(behavior, site_key, cert, key).await
    }

    async fn start_with(
        behavior: Behavior,
        site_key: VerifyingKey,
        cert: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(tls));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::default());
        tokio::spawn({
            let stats = stats.clone();
            async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    let acceptor = acceptor.clone();
                    let stats = stats.clone();
                    tokio::spawn(async move {
                        let Ok(tls) = acceptor.accept(tcp).await else {
                            return;
                        };
                        // The error type is fixed by tungstenite's callback signature.
                        #[allow(clippy::result_large_err)]
                        let check_path = |request: &Request, response: Response| {
                            if request.uri().path() != WS_PATH {
                                stats.wrong_path.store(true, Ordering::SeqCst);
                            }
                            Ok(response)
                        };
                        let Ok(socket) = tokio_tungstenite::accept_hdr_async(tls, check_path).await
                        else {
                            return;
                        };
                        serve(socket, behavior, site_key, &stats).await;
                    });
                }
            }
        });
        Self { addr, cert, stats }
    }

    fn target(&self) -> Target {
        Target {
            host: "localhost".into(),
            port: self.addr.port(),
        }
    }

    fn roots(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.clone()).unwrap();
        roots
    }

    fn logins(&self) -> usize {
        self.stats.logins.load(Ordering::SeqCst)
    }
}

async fn serve<S>(
    mut socket: WebSocketStream<S>,
    behavior: Behavior,
    site_key: VerifyingKey,
    stats: &Stats,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    if let Behavior::NoChallenge = behavior {
        while socket.next().await.is_some() {}
        return;
    }

    let mut nonce = [0u8; 32];
    OsRng.fill_bytes(&mut nonce);
    let nonce = b64.encode(nonce);
    let challenge = Message::Challenge {
        nonce: nonce.clone(),
    };
    socket.send(Frame::Text(challenge.to_json())).await.unwrap();

    let Some(Ok(Frame::Text(text))) = socket.next().await else {
        return;
    };
    let Ok(Message::Auth {
        public_key,
        signature,
    }) = Message::from_json(&text)
    else {
        return;
    };
    let signature = b64
        .decode(signature)
        .ok()
        .and_then(|bytes| Signature::from_slice(&bytes).ok());
    let valid = public_key == b64.encode(site_key.as_bytes())
        && signature.is_some_and(|signature| {
            site_key
                .verify_strict(&auth_payload(&nonce, "localhost"), &signature)
                .is_ok()
        });
    if !valid {
        stats.rejected.store(true, Ordering::SeqCst);
        close(&mut socket, CloseReason::UnknownKey.ws_code()).await;
        return;
    }
    socket
        .send(Frame::Text(Message::AuthOk.to_json()))
        .await
        .unwrap();
    stats.logins.fetch_add(1, Ordering::SeqCst);

    if let Behavior::CloseAfterLogin(code) = behavior {
        close(&mut socket, code).await;
        return;
    }
    while let Some(Ok(frame)) = socket.next().await {
        match frame {
            Frame::Text(text) if Message::from_json(&text).ok() == Some(Message::Ping) => {
                stats.pings.fetch_add(1, Ordering::SeqCst);
                if socket
                    .send(Frame::Text(Message::Pong.to_json()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Frame::Close(Some(close)) => {
                let code: u16 = close.code.into();
                stats
                    .client_close
                    .store(u32::from(code) + 1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

async fn close<S>(socket: &mut WebSocketStream<S>, code: u16)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = CloseFrame {
        code: CloseCode::from(code),
        reason: "test".into(),
    };
    let _ = socket.close(Some(frame)).await;
}

fn transport(server: &WsServer, key: &SigningKey, proxy: Option<OutboundProxy>) -> wss::Transport {
    wss::Transport::new(key, server.target(), proxy, server.roots()).unwrap()
}

/// A QUIC client whose only address never answers, so every round falls
/// back to the WebSocket.
fn client_without_quic(
    server: &WsServer,
    key: &SigningKey,
    timings: Timings,
) -> (ControlClient, UdpSocket) {
    let black_hole = UdpSocket::bind("127.0.0.1:0").unwrap();
    let quic_target = Target {
        host: "127.0.0.1".into(),
        port: black_hole.local_addr().unwrap().port(),
    };
    let client = ControlClient::new(
        key,
        quic_target,
        server.target(),
        None,
        server.roots(),
        timings,
    )
    .unwrap();
    (client, black_hole)
}

#[tokio::test]
async fn the_client_falls_back_to_the_websocket_when_quic_never_answers() {
    let key = SigningKey::generate(&mut OsRng);
    let server = WsServer::start(Behavior::Pong, key.verifying_key()).await;
    let timings = Timings {
        quic_fallback_after: Timings::default().quic_fallback_after,
        connect_timeout: Timings::default().connect_timeout,
        ..fast_timings()
    };
    let (client, _black_hole) = client_without_quic(&server, &key, timings);
    let shutdown = CancellationToken::new();

    let started = Instant::now();
    let task = tokio::spawn(client.run(shutdown.clone()));
    wait_until("a WebSocket login", || server.logins() == 1).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(5),
        "{elapsed:?}"
    );
    shutdown.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_websocket_session_logs_in_keeps_alive_and_closes_cleanly() {
    let key = SigningKey::generate(&mut OsRng);
    let server = WsServer::start(Behavior::Pong, key.verifying_key()).await;
    let (client, _black_hole) = client_without_quic(&server, &key, fast_timings());
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(client.run(shutdown.clone()));

    // Several silence timeouts long, so without pongs the client would have
    // logged in again by now.
    wait_until("pings", || server.stats.pings.load(Ordering::SeqCst) >= 12).await;
    assert_eq!(server.logins(), 1);
    assert!(!server.stats.rejected.load(Ordering::SeqCst));
    assert!(!server.stats.wrong_path.load(Ordering::SeqCst));

    shutdown.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    wait_until("the client close", || {
        server.stats.client_close.load(Ordering::SeqCst) != 0
    })
    .await;
    assert_eq!(
        server.stats.client_close.load(Ordering::SeqCst),
        u32::from(CloseReason::Closed.ws_code()) + 1
    );
}

#[tokio::test]
async fn a_close_code_from_the_coordinator_maps_to_its_reason() {
    let key = SigningKey::generate(&mut OsRng);
    let server = WsServer::start(
        Behavior::CloseAfterLogin(CloseReason::Replaced.ws_code()),
        key.verifying_key(),
    )
    .await;

    let Ok(channel) = transport(&server, &key, None)
        .connect(&fast_timings())
        .await
    else {
        panic!("login failed");
    };
    let mut channel = Channel::Wss(Box::new(channel));
    let end = session(&mut channel, &fast_timings(), &CancellationToken::new()).await;

    assert!(
        matches!(
            end,
            SessionEnd::ClosedByServer {
                code: 4002,
                reason: Some(CloseReason::Replaced)
            }
        ),
        "{end:?}"
    );
}

#[tokio::test]
async fn a_key_the_coordinator_rejects_ends_the_login_with_unknown_key() {
    let key = SigningKey::generate(&mut OsRng);
    let enrolled = SigningKey::generate(&mut OsRng);
    let server = WsServer::start(Behavior::Pong, enrolled.verifying_key()).await;

    let result = transport(&server, &key, None)
        .connect(&fast_timings())
        .await;

    assert!(
        matches!(
            result,
            Err(SessionEnd::ClosedByServer {
                reason: Some(CloseReason::UnknownKey),
                ..
            })
        ),
        "login did not end with unknown key"
    );
    assert!(server.stats.rejected.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_missing_challenge_fails_the_attempt_after_the_login_timeout() {
    let key = SigningKey::generate(&mut OsRng);
    let server = WsServer::start(Behavior::NoChallenge, key.verifying_key()).await;

    let started = Instant::now();
    let result = transport(&server, &key, None)
        .connect(&fast_timings())
        .await;

    let Err(SessionEnd::ConnectFailed(error)) = result else {
        panic!("expected a failed connect");
    };
    assert!(error.to_string().contains("challenge"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn the_websocket_goes_through_the_outbound_proxy() {
    let key = SigningKey::generate(&mut OsRng);
    let server = WsServer::start(Behavior::Pong, key.verifying_key()).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let upstream = server.addr;
    let tunnels = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let tunnels = tunnels.clone();
        async move {
            let (client, _) = listener.accept().await.unwrap();
            let mut client = BufReader::new(client);
            let mut request_line = String::new();
            client.read_line(&mut request_line).await.unwrap();
            assert!(request_line.starts_with(&format!("CONNECT localhost:{} ", upstream.port())));
            loop {
                let mut line = String::new();
                client.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            let mut client = client.into_inner();
            client
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            tunnels.fetch_add(1, Ordering::SeqCst);
            let mut server = TcpStream::connect(upstream).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
        }
    });
    let proxy = OutboundProxy::parse(&format!("http://{proxy_addr}"))
        .unwrap()
        .unwrap();

    let result = transport(&server, &key, Some(proxy))
        .connect(&fast_timings())
        .await;

    assert!(result.is_ok(), "login through the proxy failed");
    assert_eq!(tunnels.load(Ordering::SeqCst), 1);
    assert_eq!(server.logins(), 1);
}

#[tokio::test]
async fn the_websocket_trusts_a_coordinator_signed_by_a_ca_from_the_ca_file() {
    let ca = crate::test_ca::TestCa::new();
    let (cert, cert_key) = ca.leaf("localhost");
    let key = SigningKey::generate(&mut OsRng);
    let server = WsServer::start_with(Behavior::Pong, key.verifying_key(), cert, cert_key).await;
    let ca_file = ca.pem_file();

    let with_ca = crate::coordinator::trusted_roots(ca_file.to_str().unwrap()).unwrap();
    let connected = wss::Transport::new(&key, server.target(), None, with_ca)
        .unwrap()
        .connect(&fast_timings())
        .await;
    assert!(connected.is_ok(), "rejected although the CA is in ca_file");

    let public_only = crate::coordinator::trusted_roots("").unwrap();
    let connected = wss::Transport::new(&key, server.target(), None, public_only)
        .unwrap()
        .connect(&fast_timings())
        .await;
    assert!(
        matches!(connected, Err(SessionEnd::ConnectFailed(_))),
        "accepted a certificate from an untrusted CA"
    );
    assert_eq!(server.logins(), 1);
}
