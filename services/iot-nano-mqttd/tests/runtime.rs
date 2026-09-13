use std::{
    future::Future,
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
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";

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
