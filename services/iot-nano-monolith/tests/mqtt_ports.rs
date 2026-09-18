use std::{
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{Duration as ChronoDuration, Utc};
use iot_core::RpcMode;
use iot_nano_core::{CommandTransport, CommandTransportError, TransportRpcPublishRequest};
use iot_nano_monolith::{PersistentCache, PlatformCommandTransport};
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, CachePort, CommandResponseError, CommandResponsePort,
    DeviceAuthorizationPort, GatewayAuthorization, GatewayAuthorizationRequest, MqttListenerConfig,
    MqttRuntime, MqttRuntimeConfig, SqliteStorage, TransportAuthRequest, TransportRpcResponse,
};
use iot_nano_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment, PartitionId,
    StreamError, StreamMessage, StreamPort,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::timeout,
};
use uuid::Uuid;

const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";
const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

struct MonolithMqttFixture {
    _directory: tempfile::TempDir,
    runtime: MqttRuntime,
    stream: Arc<BlockingStream>,
    authorization: Arc<AllowingAuthorization>,
    responses: Arc<RecordingResponses>,
    plaintext_address: SocketAddr,
}

impl MonolithMqttFixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let plaintext_address = reserve_address().await;
        let tls_address = reserve_address().await;
        let certificate_directory =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        let authorization = Arc::new(AllowingAuthorization::default());
        let stream = Arc::new(BlockingStream::default());
        let responses = Arc::new(RecordingResponses::default());
        let storage = Arc::new(SqliteStorage::open(root.join("mqttd.sqlite")).unwrap());
        let cache: Arc<dyn CachePort> = Arc::new(
            PersistentCache::open(root.join("cache.sqlite"))
                .await
                .unwrap(),
        );
        let runtime = MqttRuntime::start(MqttRuntimeConfig {
            listeners: MqttListenerConfig {
                plaintext_address,
                tls_address,
                tls_cert_path: certificate_directory.join("server.crt"),
                tls_key_path: certificate_directory.join("server.key"),
                max_connections: 32,
                max_payload_size: 1024 * 1024,
                max_inflight_count: 16,
            },
            storage,
            authorization: authorization.clone(),
            stream: stream.clone(),
            command_responses: responses.clone(),
            cache,
            session_router: Default::default(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();

        Self {
            _directory: directory,
            runtime,
            stream,
            authorization,
            responses,
            plaintext_address,
        }
    }

    async fn connect_device(&self) -> TcpStream {
        let mut stream = TcpStream::connect(self.plaintext_address).await.unwrap();
        stream
            .write_all(&v311_connect(
                "meter-a",
                DEVICE_TOKEN_USERNAME,
                "valid-token",
            ))
            .await
            .unwrap();
        assert_eq!(
            read_mqtt_packet(&mut stream).await,
            vec![0x20, 0x02, 0x00, 0x00]
        );
        stream
    }

    async fn shutdown(mut self) {
        self.runtime.stop_accepting().await.unwrap();
        self.runtime
            .drain(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }
}

#[derive(Default)]
struct AllowingAuthorization {
    authenticate_calls: AtomicUsize,
}

impl DeviceAuthorizationPort for AllowingAuthorization {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async move {
            self.authenticate_calls.fetch_add(1, Ordering::SeqCst);
            if request.username != DEVICE_TOKEN_USERNAME || request.password != "valid-token" {
                return Err(AuthorizationError::Denied);
            }
            Ok(AuthenticatedDevice {
                token_id: Uuid::parse_str("018f68d1-cc91-7000-8000-000000000001").unwrap(),
                tenant_id: TEST_TENANT_ID,
                device_id: "meter-a".to_owned(),
                is_gateway: false,
            })
        })
    }

    fn authorize_session(
        &self,
        _device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn authorize_gateway_uplink(
        &self,
        _request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        Box::pin(async { Err(AuthorizationError::Denied) })
    }
}

#[derive(Default)]
struct RecordingResponses {
    values: Mutex<Vec<TransportRpcResponse>>,
}

impl CommandResponsePort for RecordingResponses {
    fn record_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandResponseError>> + Send + '_>> {
        self.values.lock().unwrap().push(response);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct BlockingStream {
    append_started: Notify,
    release_append: Notify,
    appended: AtomicUsize,
}

impl StreamPort for BlockingStream {
    fn append(
        &self,
        _message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        let append_started = &self.append_started;
        let release_append = &self.release_append;
        let appended = &self.appended;
        Box::pin(async move {
            append_started.notify_one();
            release_append.notified().await;
            appended.fetch_add(1, Ordering::SeqCst);
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
async fn qos_one_ack_follows_stream_commit() {
    let fixture = MonolithMqttFixture::new().await;
    let mut device = fixture.connect_device().await;
    device
        .write_all(&v311_qos_one_publish(
            "v1/devices/me/telemetry",
            &telemetry_payload(),
            7,
        ))
        .await
        .unwrap();

    fixture.stream.append_started.notified().await;
    assert!(
        timeout(Duration::from_millis(100), read_mqtt_packet(&mut device))
            .await
            .is_err()
    );

    fixture.stream.release_append.notify_one();
    assert_eq!(
        read_mqtt_packet(&mut device).await,
        vec![0x40, 0x02, 0x00, 0x07]
    );
    assert_eq!(fixture.stream.appended.load(Ordering::SeqCst), 1);
    drop(device);
    fixture.shutdown().await;
}

#[tokio::test]
async fn successful_token_authentication_is_reused_from_the_cache() {
    let fixture = MonolithMqttFixture::new().await;
    let first = fixture.connect_device().await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(25)).await;
    let second = fixture.connect_device().await;
    drop(second);

    assert_eq!(
        fixture
            .authorization
            .authenticate_calls
            .load(Ordering::SeqCst),
        1
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn shared_router_waits_for_puback_records_response_and_honors_revocation() {
    let fixture = MonolithMqttFixture::new().await;
    let mut device = fixture.connect_device().await;
    device
        .write_all(&v311_subscribe("v1/devices/me/rpc/request/+", 3))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut device).await,
        vec![0x90, 0x03, 0x00, 0x03, 0x01]
    );

    let router = fixture.runtime.session_router();
    let command_transport =
        PlatformCommandTransport::new(router.clone(), fixture.authorization.clone());
    let issued_at = Utc::now();
    let command_id = Uuid::now_v7();
    let publish = tokio::spawn(async move {
        command_transport
            .publish(TransportRpcPublishRequest {
                tenant_id: TEST_TENANT_ID,
                device_id: "meter-a".to_owned(),
                id: command_id,
                method: "sample_now".to_owned(),
                params: serde_json::json!({"channel": "temperature"}),
                mode: RpcMode::TwoWay,
                issued_at,
                expires_at: issued_at + ChronoDuration::seconds(30),
            })
            .await
    });

    let command = read_mqtt_packet(&mut device).await;
    assert_eq!(
        mqtt_publish_topic(&command),
        format!("v1/devices/me/rpc/request/{command_id}")
    );
    assert!(!publish.is_finished());
    device
        .write_all(&v311_puback(mqtt_publish_packet_id(&command)))
        .await
        .unwrap();
    assert_eq!(publish.await.unwrap(), Ok(()));

    let response = serde_json::json!({"status": "ok"}).to_string();
    device
        .write_all(&v311_qos_one_publish(
            &format!("v1/devices/me/rpc/response/{command_id}"),
            response.as_bytes(),
            11,
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut device).await,
        vec![0x40, 0x02, 0x00, 0x0b]
    );
    let recorded = fixture.responses.values.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].command_id, command_id);
    assert_eq!(recorded[0].device_id, "meter-a");
    drop(recorded);

    assert!(
        router
            .revoke_session(
                "meter-a",
                Uuid::parse_str("018f68d1-cc91-7000-8000-000000000001").unwrap(),
            )
            .await
    );
    let now = Utc::now();
    let revoked_transport = PlatformCommandTransport::new(router, fixture.authorization.clone());
    let revoked = revoked_transport.publish(TransportRpcPublishRequest {
        tenant_id: TEST_TENANT_ID,
        device_id: "meter-a".to_owned(),
        id: Uuid::now_v7(),
        method: "sample_now".to_owned(),
        params: serde_json::json!({}),
        mode: RpcMode::OneWay,
        issued_at: now,
        expires_at: now + ChronoDuration::seconds(30),
    });
    assert_eq!(revoked.await, Err(CommandTransportError::NoActiveSession));
    drop(device);
    fixture.shutdown().await;
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn telemetry_payload() -> Vec<u8> {
    serde_json::json!({
        "schema_version": 1,
        "boot_id": "018f68d1-cc91-7000-8000-000000000002",
        "sequence": 1,
        "event_at": Utc::now(),
        "measurements": { "temperature": 22.5 }
    })
    .to_string()
    .into_bytes()
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

fn v311_subscribe(topic: &str, packet_id: u16) -> Vec<u8> {
    let remaining = 2 + 2 + topic.len() + 1;
    let mut packet = vec![0x82];
    encode_remaining_length(remaining, &mut packet);
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.push(1);
    packet
}

fn v311_puback(packet_id: u16) -> Vec<u8> {
    vec![0x40, 0x02, (packet_id >> 8) as u8, packet_id as u8]
}

fn mqtt_publish_topic(packet: &[u8]) -> String {
    let body = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    String::from_utf8(packet[body + 2..body + 2 + topic_length].to_vec()).unwrap()
}

fn mqtt_publish_packet_id(packet: &[u8]) -> u16 {
    let body = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    let packet_id_start = body + 2 + topic_length;
    u16::from_be_bytes([packet[packet_id_start], packet[packet_id_start + 1]])
}

fn mqtt_packet_body_offset(packet: &[u8]) -> usize {
    let mut index = 1;
    while packet[index] & 0x80 != 0 {
        index += 1;
    }
    index + 1
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

async fn read_mqtt_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut first = [0_u8; 2];
    timeout(Duration::from_secs(2), stream.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    let mut packet = first.to_vec();
    let mut encoded = first[1];
    let mut multiplier = 1_usize;
    let mut remaining = usize::from(first[1] & 0x7f);
    while encoded & 0x80 != 0 {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        packet.push(byte[0]);
        multiplier *= 128;
        remaining += usize::from(byte[0] & 0x7f) * multiplier;
        encoded = byte[0];
    }
    let mut body = vec![0_u8; remaining];
    stream.read_exact(&mut body).await.unwrap();
    packet.extend(body);
    packet
}
