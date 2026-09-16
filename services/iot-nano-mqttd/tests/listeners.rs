use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use iot_nano_mqttd::{
    ListenerConfiguration, MuxSettings, ProtocolBackends, load_tls_acceptor, management_router,
    serve_plaintext_mux, serve_plaintext_mux_with_settings, serve_tls_mux,
    serve_tls_mux_with_settings, start_broker,
};
use rustls_pemfile::certs;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
        client::{
            WebPkiServerVerifier,
            danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        },
        pki_types::{CertificateDer, ServerName, UnixTime},
    },
};
use tower::ServiceExt;

const CONNECT_V311: &[u8] = &[
    0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 4, 0x02, 0x00, 0x3c, 0x00, 0x02, b'i', b'd',
];
const CONNECT_V5: &[u8] = &[
    0x10, 0x0f, 0x00, 0x04, b'M', b'Q', b'T', b'T', 5, 0x02, 0x00, 0x3c, 0x00, 0x00, 0x02, b'i',
    b'd',
];
const CONNECT_V5_TOKEN: &[u8] = &[
    0x10, 0x1b, 0x00, 0x04, b'M', b'Q', b'T', b'T', 5, 0x82, 0x00, 0x3c, 0x00, 0x00, 0x02, b'i',
    b'd', 0x00, 0x0a, b'i', b'o', b't', b'd', b'_', b't', b'o', b'k', b'e', b'n',
];
const FIXTURE_CERTIFICATE_VALID_TIME: u64 = 1_790_000_000;

#[derive(Debug)]
struct FixtureCertificateVerifier {
    inner: Arc<dyn ServerCertVerifier>,
}

impl ServerCertVerifier for FixtureCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            UnixTime::since_unix_epoch(Duration::from_secs(FIXTURE_CERTIFICATE_VALID_TIME)),
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[test]
fn fixture_certificate_is_valid_at_fixed_verifier_time() {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let certificate_path = fixture("server.crt");
    let certificate = std::fs::File::open(certificate_path).unwrap();
    let mut certificate = std::io::BufReader::new(certificate);
    let certificate = certs(&mut certificate).next().unwrap().unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();

    verifier
        .verify_server_cert(
            &certificate,
            &[],
            &ServerName::try_from("localhost").unwrap(),
            &[],
            UnixTime::since_unix_epoch(Duration::from_secs(FIXTURE_CERTIFICATE_VALID_TIME)),
        )
        .unwrap();
}

#[tokio::test]
async fn public_listeners_route_real_mqtt_versions_and_require_tls() {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let ports = reserve_ports().await;
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::copy(fixture("server.crt"), &certificate).unwrap();
    std::fs::copy(fixture("server.key"), &key).unwrap();

    let _broker = start_broker(ListenerConfiguration {
        plaintext_address: ports.plain,
        tls_address: ports.tls,
        v311_backend_address: ports.v311,
        v5_backend_address: ports.v5,
        tls_cert_path: certificate.clone(),
        tls_key_path: key.clone(),
        websocket_address: None,
        websocket_tls: false,
        bridge: None,
        max_connections: 100,
        max_payload_size: 1024,
        max_inflight_count: 10,
        auth_handler: None,
        authorization_handler: None,
    })
    .await
    .unwrap();

    let plaintext_listener = TcpListener::bind(ports.plain).await.unwrap();
    let tls_listener = TcpListener::bind(ports.tls).await.unwrap();
    let backends = ProtocolBackends {
        v311: ports.v311,
        v5: ports.v5,
        device_v311: None,
        device_v5: None,
    };
    let plaintext_task = tokio::spawn(serve_plaintext_mux(plaintext_listener, backends));
    let tls_task = tokio::spawn(serve_tls_mux(
        tls_listener,
        load_tls_acceptor(&certificate, &key).unwrap(),
        backends,
    ));

    assert_connack(ports.plain, CONNECT_V311, &[0x20, 0x02, 0x00, 0x00]).await;
    assert_connack(ports.plain, CONNECT_V5, &[0x20, 0x06, 0x00, 0x00, 0x03]).await;

    let mut tls_stream = tls_connect(ports.tls, &certificate).await;
    tls_stream.write_all(CONNECT_V311).await.unwrap();
    let mut connack = [0_u8; 4];
    tokio::time::timeout(Duration::from_secs(3), tls_stream.read_exact(&mut connack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

    let mut tls_v5_stream = tls_connect(ports.tls, &certificate).await;
    tls_v5_stream.write_all(CONNECT_V5).await.unwrap();
    let mut v5_connack = [0_u8; 5];
    tokio::time::timeout(
        Duration::from_secs(3),
        tls_v5_stream.read_exact(&mut v5_connack),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(v5_connack, [0x20, 0x06, 0x00, 0x00, 0x03]);

    let mut plaintext_client = TcpStream::connect(ports.tls).await.unwrap();
    plaintext_client.write_all(CONNECT_V311).await.unwrap();
    let mut response = Vec::new();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        plaintext_client.read_to_end(&mut response),
    )
    .await;
    match result {
        Ok(Ok(_)) => assert!(response.is_empty() || response[0] != 0x20),
        Ok(Err(_)) => {}
        Err(_) => panic!("plaintext MQTT CONNECT must be closed by the TLS listener"),
    }

    plaintext_task.abort();
    tls_task.abort();
}

#[tokio::test]
async fn public_plaintext_mux_closes_partial_and_oversized_connects_within_limits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_plaintext_mux_with_settings(
        listener,
        ProtocolBackends {
            v311: "127.0.0.1:1".parse().unwrap(),
            v5: "127.0.0.1:1".parse().unwrap(),
            device_v311: None,
            device_v5: None,
        },
        MuxSettings {
            max_preamble_size: 8,
            preamble_timeout: Duration::from_millis(100),
        },
    ));

    for payload in [
        &[0x10, 0x80][..],
        &[0x10, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80][..],
    ] {
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(payload).await.unwrap();
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut received))
            .await
            .expect("mux must close a stalled or oversized CONNECT")
            .unwrap();
        assert!(received.is_empty());
    }

    task.abort();
}

#[tokio::test]
async fn public_tls_mux_closes_stalled_handshake_and_preamble() {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::copy(fixture("server.crt"), &certificate).unwrap();
    std::fs::copy(fixture("server.key"), &key).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_tls_mux_with_settings(
        listener,
        load_tls_acceptor(&certificate, &key).unwrap(),
        ProtocolBackends {
            v311: "127.0.0.1:1".parse().unwrap(),
            v5: "127.0.0.1:1".parse().unwrap(),
            device_v311: None,
            device_v5: None,
        },
        MuxSettings {
            max_preamble_size: 1024,
            preamble_timeout: Duration::from_millis(100),
        },
    ));

    let mut stalled_handshake = TcpStream::connect(address).await.unwrap();
    let mut received = Vec::new();
    match tokio::time::timeout(
        Duration::from_secs(1),
        stalled_handshake.read_to_end(&mut received),
    )
    .await
    {
        Ok(Ok(_)) | Ok(Err(_)) => {}
        Err(_) => panic!("TLS handshake timeout must close the socket"),
    }

    let mut stalled_preamble = tls_connect(address, &certificate).await;
    let mut received = Vec::new();
    match tokio::time::timeout(
        Duration::from_secs(1),
        stalled_preamble.read_to_end(&mut received),
    )
    .await
    {
        Ok(Ok(_)) | Ok(Err(_)) => {}
        Err(_) => panic!("TLS CONNECT preamble timeout must close the socket"),
    }

    task.abort();
}

#[tokio::test]
async fn tls_mux_routes_mqtt5_device_tokens_to_the_native_backend() {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("server.crt");
    let key = directory.path().join("server.key");
    std::fs::copy(fixture("server.crt"), &certificate).unwrap();
    std::fs::copy(fixture("server.key"), &key).unwrap();

    let generic_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let device_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let generic_address = generic_listener.local_addr().unwrap();
    let device_address = device_listener.local_addr().unwrap();
    let public_address = public_listener.local_addr().unwrap();
    let generic_task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_millis(300), generic_listener.accept()).await
    });
    let device_task = tokio::spawn(async move {
        let (mut stream, _) = device_listener.accept().await.unwrap();
        let mut packet = vec![0_u8; CONNECT_V5_TOKEN.len()];
        stream.read_exact(&mut packet).await.unwrap();
        stream.write_all(b"device-v5").await.unwrap();
    });
    let mux_task = tokio::spawn(serve_tls_mux(
        public_listener,
        load_tls_acceptor(&certificate, &key).unwrap(),
        ProtocolBackends {
            v311: generic_address,
            v5: generic_address,
            device_v311: None,
            device_v5: Some(device_address),
        },
    ));

    let mut client = tls_connect(public_address, &certificate).await;
    client.write_all(CONNECT_V5_TOKEN).await.unwrap();
    let mut received = [0_u8; 9];
    tokio::time::timeout(Duration::from_secs(3), client.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received, b"device-v5");
    assert!(generic_task.await.unwrap().is_err());
    device_task.await.unwrap();
    mux_task.abort();
}

#[tokio::test]
async fn management_router_exposes_health_and_truthful_metrics() {
    let router = management_router();

    let health = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), axum::http::StatusCode::OK);

    let metrics = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(metrics.into_body(), 1024)
        .await
        .unwrap();
    assert_eq!(&body[..], b"iot_mqttd_up 1\n");
}

async fn assert_connack(address: SocketAddr, connect: &[u8], expected: &[u8]) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(connect).await.unwrap();
    let mut response = vec![0_u8; expected.len()];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, expected);
}

async fn tls_connect(
    address: SocketAddr,
    certificate: &PathBuf,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = RootCertStore::empty();
    let certificate = std::fs::File::open(certificate).unwrap();
    let mut certificate = std::io::BufReader::new(certificate);
    for certificate in certs(&mut certificate) {
        roots.add(certificate.unwrap()).unwrap();
    }
    let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(FixtureCertificateVerifier { inner: verifier }))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TcpStream::connect(address).await.unwrap();
    connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap()
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

struct Ports {
    plain: SocketAddr,
    tls: SocketAddr,
    v311: SocketAddr,
    v5: SocketAddr,
}

async fn reserve_ports() -> Ports {
    let mut addresses = Vec::new();
    for _ in 0..4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap());
    }
    Ports {
        plain: addresses[0],
        tls: addresses[1],
        v311: addresses[2],
        v5: addresses[3],
    }
}
