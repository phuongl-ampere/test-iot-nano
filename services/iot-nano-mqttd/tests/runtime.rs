use std::{
    future::Future,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, BrokerStorage, CacheEntry, CacheError, CachePort,
    CommandResponseError, CommandResponsePort, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, MemoryStorage, MqttListenerConfig, MqttRuntime, MqttRuntimeConfig,
    MqttRuntimeStartError, SqliteStorage, TransportAuthRequest, TransportRpcResponse,
};
use iot_nano_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment, PartitionId,
    StreamError, StreamMessage, StreamPort,
};
use rustls_pemfile::certs;
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
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
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";
const FIXTURE_CERTIFICATE_VALID_TIME: u64 = 1_789_000_000;

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

#[derive(Clone)]
struct TestAuthorization {
    allow: bool,
}

impl TestAuthorization {
    fn allowing() -> Self {
        Self { allow: true }
    }

    fn denying() -> Self {
        Self { allow: false }
    }
}

impl DeviceAuthorizationPort for TestAuthorization {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        let allow = self.allow;
        Box::pin(async move {
            if !allow
                || request.username != DEVICE_TOKEN_USERNAME
                || request.password != "valid-token"
            {
                return Err(AuthorizationError::Denied);
            }
            Ok(AuthenticatedDevice {
                token_id: Uuid::parse_str("018f68d1-cc91-7000-8000-000000000001").unwrap(),
                device_id: "meter-a".to_owned(),
                is_gateway: false,
            })
        })
    }

    fn authorize_session(
        &self,
        _device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        let allow = self.allow;
        Box::pin(async move {
            if allow {
                Ok(())
            } else {
                Err(AuthorizationError::Denied)
            }
        })
    }

    fn authorize_gateway_uplink(
        &self,
        _request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async { Err(AuthorizationError::Denied) })
    }
}

struct TestCache;

impl CachePort for TestCache {
    fn get(
        &self,
        _key: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, CacheError>> + Send + '_>> {
        Box::pin(async { Ok(None) })
    }

    fn put(
        &self,
        _entry: CacheEntry,
    ) -> Pin<Box<dyn Future<Output = Result<(), CacheError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

struct TestResponses;

impl CommandResponsePort for TestResponses {
    fn record_response(
        &self,
        _response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandResponseError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

struct TestStream;

impl StreamPort for TestStream {
    fn append(
        &self,
        _message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        Box::pin(async {
            Ok(AppendReceipt {
                partition: PartitionId::new(0),
                offset: 0,
            })
        })
    }

    fn claim(
        &self,
        _request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn acknowledge(
        &self,
        _request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn heartbeat(
        &self,
        _request: iot_nano_stream::HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        Box::pin(async {
            Ok(GroupAssignment {
                generation: 1,
                partitions: vec![PartitionId::new(0)],
            })
        })
    }

    fn drain(
        &self,
        _deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn runtime_binds_public_tcp_and_tls_only_after_typed_port_startup() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let storage: Arc<dyn BrokerStorage> = Arc::new(MemoryStorage::new());
    let mut runtime = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        storage,
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();

    assert!(runtime.is_accepting());
    drop(TcpStream::connect(plaintext_address).await.unwrap());
    drop(TcpStream::connect(tls_address).await.unwrap());

    shutdown(&mut runtime).await;
}

#[tokio::test]
async fn runtime_denies_device_token_before_accepting_a_session() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let storage: Arc<dyn BrokerStorage> = Arc::new(MemoryStorage::new());
    let mut runtime = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        storage,
        Arc::new(TestAuthorization::denying()),
    )
    .await
    .unwrap();
    let mut stream = TcpStream::connect(plaintext_address).await.unwrap();
    stream
        .write_all(&v311_connect(
            "meter-a",
            DEVICE_TOKEN_USERNAME,
            "invalid-token",
        ))
        .await
        .unwrap();
    let mut connack = [0_u8; 4];
    timeout(Duration::from_secs(2), stream.read_exact(&mut connack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x05]);

    drop(stream);
    shutdown(&mut runtime).await;
}

#[tokio::test]
async fn stop_accepting_keeps_an_active_device_connection_alive_for_drain() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let storage: Arc<dyn BrokerStorage> = Arc::new(MemoryStorage::new());
    let mut runtime = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        storage,
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();
    let mut stream = TcpStream::connect(plaintext_address).await.unwrap();
    stream
        .write_all(&v311_connect(
            "meter-a",
            DEVICE_TOKEN_USERNAME,
            "valid-token",
        ))
        .await
        .unwrap();
    let mut connack = [0_u8; 4];
    stream.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

    runtime.stop_accepting().await.unwrap();
    stream
        .write_all(&v311_qos_one_publish(
            "v1/devices/me/telemetry",
            &serde_json::to_vec(&json!({
                "schema_version": 1,
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1,
                "event_at": "2026-09-10T08:00:00Z",
                "measurements": {"temperature_c": 26.4},
            }))
            .unwrap(),
            7,
        ))
        .await
        .unwrap();
    let mut puback = [0_u8; 4];
    timeout(Duration::from_secs(2), stream.read_exact(&mut puback))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(puback, [0x40, 0x02, 0x00, 0x07]);

    drop(stream);
    runtime
        .drain(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn expired_drain_closes_listeners_and_joins_runtime_work() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let storage: Arc<dyn BrokerStorage> = Arc::new(MemoryStorage::new());
    let mut runtime = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        storage,
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();
    let mut stream = TcpStream::connect(plaintext_address).await.unwrap();
    stream
        .write_all(&v311_connect(
            "meter-a",
            DEVICE_TOKEN_USERNAME,
            "valid-token",
        ))
        .await
        .unwrap();
    let mut connack = [0_u8; 4];
    stream.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);

    assert!(matches!(
        runtime.drain(Instant::now()).await,
        Err(iot_nano_mqttd::MqttRuntimeError::DeadlineElapsed)
    ));
    drop(stream);
    drop(runtime);
    assert_bindable(plaintext_address).await;
    assert_bindable(tls_address).await;
}

#[tokio::test]
async fn parent_cancellation_releases_configured_public_listener_addresses() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let cancellation = CancellationToken::new();
    let mut config = runtime_config(
        directory.path(),
        plaintext_address,
        tls_address,
        Arc::new(MemoryStorage::new()),
        Arc::new(TestAuthorization::allowing()),
        fixture_certificate(),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/server.key"),
    );
    config.cancellation = cancellation.clone();
    let mut runtime = MqttRuntime::start(config).await.unwrap();

    cancellation.cancel();

    assert_public_addresses_rebindable(plaintext_address, tls_address).await;
    shutdown(&mut runtime).await;
}

#[tokio::test]
async fn runtime_rejects_missing_and_non_device_mqtt311_and_mqtt5_before_broker_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let storage: Arc<dyn BrokerStorage> = Arc::new(MemoryStorage::new());
    let mut runtime = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        storage,
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();

    for (connect, publish) in [
        (
            v311_connect_without_credentials("missing-v311"),
            v311_qos_one_publish("test/missing-v311", b"blocked", 1),
        ),
        (
            v311_connect("non-device-v311", "ordinary", "password"),
            v311_qos_one_publish("test/non-device-v311", b"blocked", 2),
        ),
        (
            v5_connect_without_credentials("missing-v5"),
            v5_qos_one_publish("test/missing-v5", b"blocked", 3),
        ),
        (
            v5_connect("non-device-v5", "ordinary", "password"),
            v5_qos_one_publish("test/non-device-v5", b"blocked", 4),
        ),
    ] {
        let stream = TcpStream::connect(plaintext_address).await.unwrap();
        assert_rejected_before_broker_dispatch(stream, &connect, &publish).await;
    }

    shutdown(&mut runtime).await;
}

#[tokio::test]
async fn runtime_rejects_non_device_tls_mqtt_connection_before_broker_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let storage: Arc<dyn BrokerStorage> = Arc::new(MemoryStorage::new());
    let mut runtime = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        storage,
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();

    let stream = tls_connect(tls_address, &fixture_certificate()).await;
    assert_rejected_before_broker_dispatch(
        stream,
        &v311_connect("non-device-tls", "ordinary", "password"),
        &v311_qos_one_publish("test/non-device-tls", b"blocked", 5),
    )
    .await;

    shutdown(&mut runtime).await;
}

#[tokio::test]
async fn tls_failure_leaves_configured_public_addresses_unbound() {
    let directory = tempfile::tempdir().unwrap();
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let result = MqttRuntime::start(runtime_config(
        directory.path(),
        plaintext_address,
        tls_address,
        Arc::new(MemoryStorage::new()),
        Arc::new(TestAuthorization::allowing()),
        directory.path().join("missing.crt"),
        directory.path().join("missing.key"),
    ))
    .await;

    assert!(matches!(result, Err(MqttRuntimeStartError::Tls(_))));
    assert_bindable(plaintext_address).await;
    assert_bindable(tls_address).await;
}

#[tokio::test]
async fn runtime_reopens_mqttd_sqlite_after_drain() {
    let directory = tempfile::tempdir().unwrap();
    let storage_path = directory.path().join("mqttd.sqlite");
    let plaintext_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let mut first = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        Arc::new(SqliteStorage::open(&storage_path).unwrap()),
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();
    shutdown(&mut first).await;
    drop(first);

    let mut second = start_runtime(
        directory.path(),
        plaintext_address,
        tls_address,
        Arc::new(SqliteStorage::open(&storage_path).unwrap()),
        Arc::new(TestAuthorization::allowing()),
    )
    .await
    .unwrap();
    assert!(second.is_accepting());
    shutdown(&mut second).await;
}

#[test]
fn runtime_path_contains_no_internal_http_or_service_secret_boundary() {
    let source =
        std::fs::read_to_string(format!("{}/src/runtime.rs", env!("CARGO_MANIFEST_DIR"))).unwrap();
    for forbidden in [
        "reqwest",
        "axum",
        "/internal/",
        "http://",
        "https://",
        "secret",
    ] {
        assert!(
            !source.contains(forbidden),
            "runtime implementation contains forbidden boundary {forbidden:?}"
        );
    }
}

async fn start_runtime(
    root: &Path,
    plaintext_address: SocketAddr,
    tls_address: SocketAddr,
    storage: Arc<dyn BrokerStorage>,
    authorization: Arc<dyn DeviceAuthorizationPort>,
) -> Result<MqttRuntime, MqttRuntimeStartError> {
    let fixture_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    MqttRuntime::start(runtime_config(
        root,
        plaintext_address,
        tls_address,
        storage,
        authorization,
        fixture_directory.join("server.crt"),
        fixture_directory.join("server.key"),
    ))
    .await
}

fn runtime_config(
    _root: &Path,
    plaintext_address: SocketAddr,
    tls_address: SocketAddr,
    storage: Arc<dyn BrokerStorage>,
    authorization: Arc<dyn DeviceAuthorizationPort>,
    tls_cert_path: PathBuf,
    tls_key_path: PathBuf,
) -> MqttRuntimeConfig {
    MqttRuntimeConfig {
        listeners: MqttListenerConfig {
            plaintext_address,
            tls_address,
            tls_cert_path,
            tls_key_path,
            max_connections: 32,
            max_payload_size: 1024 * 1024,
            max_inflight_count: 16,
        },
        storage,
        authorization,
        stream: Arc::new(TestStream),
        command_responses: Arc::new(TestResponses),
        cache: Arc::new(TestCache),
        session_router: Default::default(),
        cancellation: CancellationToken::new(),
    }
}

async fn shutdown(runtime: &mut MqttRuntime) {
    runtime.stop_accepting().await.unwrap();
    runtime
        .drain(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

async fn assert_bindable(address: SocketAddr) {
    let listener = TcpListener::bind(address).await.unwrap();
    drop(listener);
}

async fn assert_public_addresses_rebindable(plaintext_address: SocketAddr, tls_address: SocketAddr) {
    timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(plaintext_listener) = TcpListener::bind(plaintext_address).await {
                if let Ok(tls_listener) = TcpListener::bind(tls_address).await {
                    drop((plaintext_listener, tls_listener));
                    return;
                }
                drop(plaintext_listener);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("parent cancellation did not release both public listener addresses");
}

fn v311_connect(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let remaining = 10 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        0xc2,
        0x00,
        0x3c,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet.extend_from_slice(&(password.len() as u16).to_be_bytes());
    packet.extend_from_slice(password.as_bytes());
    packet
}

fn v311_connect_without_credentials(client_id: &str) -> Vec<u8> {
    let remaining = 10 + 2 + client_id.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        0x02,
        0x00,
        0x3c,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet
}

fn v5_connect_without_credentials(client_id: &str) -> Vec<u8> {
    let remaining = 11 + 2 + client_id.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        5,
        0x02,
        0x00,
        0x3c,
        0x00,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet
}

fn v5_connect(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let remaining = 11 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
    let mut packet = vec![
        0x10,
        remaining as u8,
        0x00,
        0x04,
        b'M',
        b'Q',
        b'T',
        b'T',
        5,
        0xc2,
        0x00,
        0x3c,
        0x00,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ];
    packet.extend_from_slice(client_id.as_bytes());
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet.extend_from_slice(&(password.len() as u16).to_be_bytes());
    packet.extend_from_slice(password.as_bytes());
    packet
}

fn v311_qos_one_publish(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    let remaining = 2 + topic.len() + 2 + payload.len();
    let mut packet = vec![0x32];
    encode_remaining_length(remaining, &mut packet);
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(payload);
    packet
}

fn v5_qos_one_publish(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    let remaining = 2 + topic.len() + 2 + 1 + payload.len();
    let mut packet = vec![0x32];
    encode_remaining_length(remaining, &mut packet);
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.push(0x00);
    packet.extend_from_slice(payload);
    packet
}

fn encode_remaining_length(mut value: usize, packet: &mut Vec<u8>) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if value == 0 {
            return;
        }
    }
}

async fn assert_rejected_before_broker_dispatch<S>(mut stream: S, connect: &[u8], publish: &[u8])
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(connect).await.unwrap();
    let _ = stream.write_all(publish).await;
    let mut response = Vec::new();
    match timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await {
        Ok(Ok(_)) | Ok(Err(_)) => {
            assert!(
                response.is_empty(),
                "unexpected MQTT broker response: {response:?}"
            );
        }
        Err(_) => panic!("non-device MQTT connection remained open"),
    }
}

async fn tls_connect(
    address: SocketAddr,
    certificate_path: &PathBuf,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let certificate = std::fs::File::open(certificate_path).unwrap();
    let mut certificate = BufReader::new(certificate);
    let mut roots = RootCertStore::empty();
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
    let stream = TcpStream::connect(address).await.unwrap();
    TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
        .unwrap()
}

fn fixture_certificate() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("server.crt")
}
