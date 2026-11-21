use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use ed25519_dalek::VerifyingKey;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, ConnectionError, Endpoint};
use rand::rngs::OsRng;
use reeframe_protocol::{read_message, write_message, ALPN};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::UnixTime;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};
use tokio::task::JoinHandle;

use super::*;

// -- Target --

#[test]
fn an_empty_control_addr_uses_the_coordinator_host_on_443() {
    let target = Target::quic("", "https://coordinator.example:8443/api").unwrap();
    assert_eq!(
        target,
        Target {
            host: "coordinator.example".into(),
            port: 443
        }
    );

    let target = Target::quic("", "https://[2001:db8::1]/").unwrap();
    assert_eq!(target.host, "2001:db8::1");
    assert_eq!(target.port, 443);

    assert!(Target::quic("", "not a url").is_err());
}

#[test]
fn a_set_control_addr_wins_over_the_coordinator_url() {
    let target = Target::quic("control.example:4433", "https://coordinator.example").unwrap();
    assert_eq!(
        target,
        Target {
            host: "control.example".into(),
            port: 4433
        }
    );

    let target = Target::quic("[::1]:4433", "https://coordinator.example").unwrap();
    assert_eq!(target.host, "::1");

    assert!(Target::quic("control.example", "https://c.example").is_err());
    assert!(Target::quic("control.example:http", "https://c.example").is_err());
    assert!(Target::quic(":4433", "https://c.example").is_err());
}

#[test]
fn the_websocket_goes_to_the_coordinator_url_and_ignores_control_addr() {
    let target = Target::wss("https://coordinator.example/api").unwrap();
    assert_eq!(
        target,
        Target {
            host: "coordinator.example".into(),
            port: 443
        }
    );
    assert_eq!(
        Target::wss("https://coordinator.example:8443")
            .unwrap()
            .port,
        8443
    );
    assert!(Target::wss("not a url").is_err());
    let error = Target::wss("http://coordinator.example").unwrap_err();
    assert!(error.to_string().contains("https://"), "{error}");
}

// -- Retry delay --

#[tokio::test(start_paused = true)]
async fn refusals_retry_at_the_maximum_and_everything_else_backs_off_normally() {
    let max = Duration::from_secs(60);
    let backoff = || Backoff::new(Duration::from_secs(1), max, Duration::from_secs(30));

    for code in [1, 2, 4] {
        let delay = retry_delay(&SessionEnd::quic_close(code), &mut backoff(), max);
        assert_eq!(delay, max, "QUIC close code {code}");
    }
    for code in [4001, 4002, 4004] {
        let delay = retry_delay(&SessionEnd::ws_close(code), &mut backoff(), max);
        assert_eq!(delay, max, "WebSocket close code {code}");
    }

    let others = [
        SessionEnd::quic_close(0),
        SessionEnd::quic_close(3),
        SessionEnd::quic_close(5),
        SessionEnd::quic_close(99),
        SessionEnd::ws_close(1000),
        SessionEnd::ws_close(1006),
        SessionEnd::ws_close(4000),
        SessionEnd::ws_close(4003),
        SessionEnd::ws_close(4005),
        SessionEnd::ws_close(4099),
        // QUIC codes arriving as WebSocket codes must not count as refusals.
        SessionEnd::ws_close(1),
        SessionEnd::Lost("reset".into()),
        SessionEnd::Silent,
        SessionEnd::ConnectFailed(anyhow::anyhow!("unreachable")),
    ];
    for end in others {
        let mut backoff = backoff();
        let first = retry_delay(&end, &mut backoff, max);
        let second = retry_delay(&end, &mut backoff, max);
        assert!(
            (0.8..=1.2).contains(&first.as_secs_f64()),
            "{end:?}: {first:?}"
        );
        assert!(
            (1.6..=2.4).contains(&second.as_secs_f64()),
            "{end:?}: {second:?}"
        );
    }
}

// -- Against a local Coordinator stand-in --

pub(super) fn fast_timings() -> Timings {
    Timings {
        ping_interval: Duration::from_millis(100),
        silence_timeout: Duration::from_millis(400),
        race_stagger: Duration::from_millis(50),
        quic_fallback_after: Duration::from_millis(500),
        connect_timeout: Duration::from_secs(2),
        login_timeout: Duration::from_millis(300),
        retry_base: Duration::from_millis(50),
        retry_max: Duration::from_millis(200),
        stable_uptime: Duration::from_secs(1),
    }
}

/// How the stand-in answers pings.
#[derive(Clone, Copy)]
enum Behavior {
    Pong,
    Silent,
    /// Closes the first connection with this code at its first ping.
    CloseFirstWith(u32),
}

#[derive(Default)]
struct Stats {
    connections: AtomicUsize,
    pings: AtomicUsize,
    foreign_key: AtomicBool,
    /// Close code the client sent, plus one, so zero means none yet.
    client_close: AtomicU32,
}

struct TestServer {
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    stats: Arc<Stats>,
}

impl TestServer {
    fn start(behavior: Behavior, client_key: VerifyingKey) -> Self {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = generated.cert.der().clone();
        let key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
        Self::start_with(behavior, client_key, cert, key)
    }

    fn start_with(
        behavior: Behavior,
        client_key: VerifyingKey,
        cert: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut tls = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(Arc::new(AnyClientCert(provider)))
            .with_single_cert(vec![cert.clone()], key)
            .unwrap();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let config =
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap()));
        let endpoint = Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap();

        let stats = Arc::new(Stats::default());
        let spki = ed25519_spki(&client_key);
        tokio::spawn({
            let stats = stats.clone();
            async move {
                while let Some(incoming) = endpoint.accept().await {
                    let Ok(connection) = incoming.await else {
                        continue;
                    };
                    let n = stats.connections.fetch_add(1, Ordering::SeqCst) + 1;
                    tokio::spawn(serve_connection(
                        connection,
                        n,
                        behavior,
                        spki.clone(),
                        stats.clone(),
                    ));
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

    fn connections(&self) -> usize {
        self.stats.connections.load(Ordering::SeqCst)
    }

    fn pings(&self) -> usize {
        self.stats.pings.load(Ordering::SeqCst)
    }
}

async fn serve_connection(
    connection: Connection,
    n: usize,
    behavior: Behavior,
    spki: Vec<u8>,
    stats: Arc<Stats>,
) {
    let certs = connection
        .peer_identity()
        .and_then(|identity| identity.downcast::<Vec<CertificateDer<'static>>>().ok())
        .unwrap();
    if !certs[0].windows(spki.len()).any(|window| window == spki) {
        stats.foreign_key.store(true, Ordering::SeqCst);
    }

    if let Ok((mut send, mut recv)) = connection.accept_bi().await {
        while let Ok(Some(message)) = read_message(&mut recv).await {
            if message != Message::Ping {
                continue;
            }
            stats.pings.fetch_add(1, Ordering::SeqCst);
            match behavior {
                Behavior::CloseFirstWith(code) if n == 1 => {
                    connection.close(code.into(), b"test");
                    return;
                }
                Behavior::Silent => {}
                _ => {
                    if write_message(&mut send, &Message::Pong).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
    if let ConnectionError::ApplicationClosed(close) = connection.closed().await {
        let code = u32::try_from(close.error_code.into_inner()).unwrap();
        stats.client_close.store(code + 1, Ordering::SeqCst);
    }
}

/// DER of an Ed25519 SubjectPublicKeyInfo: a fixed 12-byte header and the key.
fn ed25519_spki(key: &VerifyingKey) -> Vec<u8> {
    let mut spki = vec![
        0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ];
    spki.extend_from_slice(key.as_bytes());
    spki
}

/// Takes any client certificate that proves its key, like the Coordinator,
/// which checks the key against enrolled sites afterwards.
#[derive(Debug)]
struct AnyClientCert(Arc<CryptoProvider>);

impl ClientCertVerifier for AnyClientCert {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn start_client(server: &TestServer, key: &SigningKey) -> (CancellationToken, JoinHandle<()>) {
    let client = ControlClient::new(
        key,
        server.target(),
        unreachable_wss_target(),
        None,
        server.roots(),
        fast_timings(),
    )
    .unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(client.run(shutdown.clone()));
    (shutdown, task)
}

/// A TCP port nothing listens on, so a WebSocket fallback fails at once.
pub(super) fn unreachable_wss_target() -> Target {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    Target {
        host: "127.0.0.1".into(),
        port,
    }
}

pub(super) async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn pongs_keep_one_connection_up_and_shutdown_closes_it() {
    let key = SigningKey::generate(&mut OsRng);
    let server = TestServer::start(Behavior::Pong, key.verifying_key());
    let (shutdown, task) = start_client(&server, &key);

    // Several silence timeouts long, so without pongs the client would have
    // dropped and reopened the connection by now.
    wait_until("pings", || server.pings() >= 12).await;
    assert_eq!(server.connections(), 1);
    assert!(!server.stats.foreign_key.load(Ordering::SeqCst));

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
        CloseReason::Closed.code() + 1
    );
}

#[tokio::test]
async fn a_server_that_stops_answering_is_left_for_a_new_connection() {
    let key = SigningKey::generate(&mut OsRng);
    let server = TestServer::start(Behavior::Silent, key.verifying_key());
    let (shutdown, task) = start_client(&server, &key);

    wait_until("a second connection", || server.connections() >= 2).await;

    shutdown.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_client_reconnects_after_the_server_closes_the_connection() {
    let key = SigningKey::generate(&mut OsRng);
    let server = TestServer::start(
        Behavior::CloseFirstWith(CloseReason::Timeout.code()),
        key.verifying_key(),
    );
    let (shutdown, task) = start_client(&server, &key);

    wait_until("a second connection", || server.connections() >= 2).await;
    wait_until("a ping on it", || server.pings() >= 2).await;

    shutdown.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn racing_skips_an_address_that_never_answers() {
    let key = SigningKey::generate(&mut OsRng);
    let first = TestServer::start(Behavior::Pong, key.verifying_key());
    let second = TestServer::start(Behavior::Pong, key.verifying_key());
    // Bound but never read, so handshakes to it get no answer at all.
    let black_hole = UdpSocket::bind("127.0.0.1:0").unwrap();

    let mut roots = first.roots();
    roots.add(second.cert.clone()).unwrap();
    let mut endpoint = quic::client_endpoint().unwrap();
    endpoint.set_default_client_config(quic::client_config(&key, roots).unwrap());
    let addrs = vec![black_hole.local_addr().unwrap(), first.addr, second.addr];

    let started = Instant::now();
    let (_connection, addr) = quic::race(&endpoint, addrs, "localhost", Duration::from_millis(250))
        .await
        .unwrap();

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_ne!(addr, black_hole.local_addr().unwrap());
}

#[tokio::test]
async fn quic_trusts_a_coordinator_signed_by_a_ca_from_the_ca_file() {
    let ca = crate::test_ca::TestCa::new();
    let (cert, cert_key) = ca.leaf("localhost");
    let key = SigningKey::generate(&mut OsRng);
    let server = TestServer::start_with(Behavior::Pong, key.verifying_key(), cert, cert_key);
    let ca_file = ca.pem_file();

    let with_ca = crate::coordinator::trusted_roots(ca_file.to_str().unwrap()).unwrap();
    let connected = quic::Transport::new(&key, server.target(), with_ca)
        .unwrap()
        .connect(&fast_timings())
        .await;
    assert!(connected.is_ok(), "rejected although the CA is in ca_file");

    let public_only = crate::coordinator::trusted_roots("").unwrap();
    let connected = quic::Transport::new(&key, server.target(), public_only)
        .unwrap()
        .connect(&fast_timings())
        .await;
    assert!(
        matches!(connected, Err(SessionEnd::ConnectFailed(_))),
        "accepted a certificate from an untrusted CA"
    );
}
