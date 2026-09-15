use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use iot_core::{
    DatabaseStorage, StorageConfiguration, TelemetryEvent, device_token_prefix,
    generate_device_token, hash_device_token,
};
use iot_nano_monolith::{CacheEntry, MonolithConfig, MonolithRuntime, ShutdownError, StartupError};
use iot_nano_mqttd::{
    BrokerLifecycleHandle, BrokerStorage, ListenerConfiguration, MqttRuntimeStartError,
    MuxSettings, PreboundBackendListeners, ProtocolBackends, SqliteStorage,
    start_broker_with_prebound_listeners,
};
use iot_nano_stream::{
    AcknowledgeRequest, ClaimRequest, GroupStart, StreamMessage, TelemetryMessage,
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use uuid::Uuid;

const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
    reserved_listeners: Option<Vec<TcpListener>>,
}

impl Fixture {
    async fn sqlite() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mqtt_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");

        let mut fixture = Self {
            config: MonolithConfig {
                storage: StorageConfiguration {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(root.join("platform.sqlite")),
                    sqlite_busy_timeout_ms: 5_000,
                },
                device_token_vault_key: "durable-recovery-device-token-key-material-0001"
                    .to_owned(),
                internal_dir: root.join("internal"),
                public_http: SocketAddr::from(([127, 0, 0, 1], 0)),
                management_http: SocketAddr::from(([127, 0, 0, 1], 0)),
                mqtt_tcp: SocketAddr::from(([127, 0, 0, 1], 0)),
                mqtt_tls: SocketAddr::from(([127, 0, 0, 1], 0)),
                tls_cert_path: mqtt_fixtures.join("server.crt"),
                tls_key_path: mqtt_fixtures.join("server.key"),
                shutdown_deadline: Duration::from_secs(1),
            },
            _directory: directory,
            reserved_listeners: None,
        };
        fixture.reserve_new_addresses().await;
        fixture
    }

    fn platform_path(&self) -> &std::path::Path {
        self.config.storage.sqlite_path.as_deref().unwrap()
    }

    fn state_path(&self, name: &str) -> PathBuf {
        self.config.internal_dir.join(name)
    }

    async fn start(&mut self) -> MonolithRuntime {
        const START_ATTEMPTS: usize = 4;

        for attempt in 0..START_ATTEMPTS {
            if self.reserved_listeners.is_none()
                && self.reserve_configured_addresses().await.is_err()
            {
                self.reserve_new_addresses().await;
            }
            self.release_reserved_addresses();

            match MonolithRuntime::start(self.config.clone()).await {
                Ok(runtime) => return runtime,
                Err(error)
                    if is_retryable_listener_error(&error) && attempt + 1 < START_ATTEMPTS =>
                {
                    self.reserve_new_addresses().await;
                }
                Err(error) => panic!("monolith runtime failed to start: {error}"),
            }
        }

        unreachable!("listener startup retry loop must return or panic")
    }

    async fn reserve_configured_addresses(&mut self) -> std::io::Result<()> {
        let public_listener = TcpListener::bind(self.config.public_http).await?;
        let management_listener = TcpListener::bind(self.config.management_http).await?;
        let mqtt_tcp_listener = TcpListener::bind(self.config.mqtt_tcp).await?;
        let mqtt_tls_listener = TcpListener::bind(self.config.mqtt_tls).await?;
        self.reserved_listeners = Some(vec![
            public_listener,
            management_listener,
            mqtt_tcp_listener,
            mqtt_tls_listener,
        ]);
        Ok(())
    }

    async fn reserve_new_addresses(&mut self) {
        self.release_reserved_addresses();
        let (public_http, public_listener) = reserve_address().await;
        let (management_http, management_listener) = reserve_address().await;
        let (mqtt_tcp, mqtt_tcp_listener) = reserve_address().await;
        let (mqtt_tls, mqtt_tls_listener) = reserve_address().await;
        self.config.public_http = public_http;
        self.config.management_http = management_http;
        self.config.mqtt_tcp = mqtt_tcp;
        self.config.mqtt_tls = mqtt_tls;
        self.reserved_listeners = Some(vec![
            public_listener,
            management_listener,
            mqtt_tcp_listener,
            mqtt_tls_listener,
        ]);
    }

    fn release_reserved_addresses(&mut self) {
        self.reserved_listeners.take();
    }
}

fn is_retryable_listener_error(error: &StartupError) -> bool {
    matches!(
        error,
        StartupError::PublicHttpBind(_)
            | StartupError::ManagementHttpBind(_)
            | StartupError::MqttRuntime(
                MqttRuntimeStartError::PrivateBackendBind(_)
                    | MqttRuntimeStartError::DeviceBackendBind(_)
                    | MqttRuntimeStartError::PlaintextListenerBind(_)
                    | MqttRuntimeStartError::TlsListenerBind(_)
            )
    )
}

struct ProtocolBroker {
    address: SocketAddr,
    handle: Option<BrokerLifecycleHandle>,
}

impl ProtocolBroker {
    async fn start(database: &Path) -> Self {
        let public_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = public_listener.local_addr().unwrap();
        let v311_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let v311_address = v311_listener.local_addr().unwrap();
        let v5_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let v5_address = v5_listener.local_addr().unwrap();
        let fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        let storage: Arc<dyn BrokerStorage> = Arc::new(SqliteStorage::open(database).unwrap());
        let handle = start_broker_with_prebound_listeners(
            ListenerConfiguration {
                plaintext_address: address,
                tls_address: SocketAddr::from(([127, 0, 0, 1], 0)),
                v311_backend_address: v311_address,
                v5_backend_address: v5_address,
                tls_cert_path: fixtures.join("server.crt"),
                tls_key_path: fixtures.join("server.key"),
                websocket_address: None,
                websocket_tls: false,
                bridge: None,
                max_connections: 32,
                max_payload_size: 1024 * 1024,
                max_inflight_count: 16,
                token_authenticator: None,
                auth_handler: None,
                authorization_handler: None,
            },
            PreboundBackendListeners {
                v311: v311_listener,
                v5: v5_listener,
            },
            storage,
        )
        .await
        .unwrap();
        handle
            .spawn_public_plaintext_mux(
                public_listener,
                ProtocolBackends {
                    v311: v311_address,
                    v5: v5_address,
                    device_v311: None,
                    device_v5: None,
                },
                MuxSettings::default(),
            )
            .unwrap();

        Self {
            address,
            handle: Some(handle),
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn shutdown(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown();
            handle.join().unwrap();
        }
    }
}

impl Drop for ProtocolBroker {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown();
            let _ = handle.join();
        }
    }
}

#[tokio::test]
async fn instance_lock_rejects_a_second_runtime_and_restored_platform_backup_is_bootable() {
    let mut fixture = Fixture::sqlite().await;
    let mut first = fixture.start().await;
    first
        .platform()
        .unwrap()
        .register_device("in-backup")
        .await
        .unwrap();
    let backup = first.platform().unwrap().backup_sqlite().await.unwrap();

    first
        .platform()
        .unwrap()
        .register_device("after-backup")
        .await
        .unwrap();

    let error = match MonolithRuntime::start(fixture.config.clone()).await {
        Ok(_) => panic!("a second monolith instance unexpectedly shared internal state"),
        Err(error) => error,
    };
    assert!(matches!(error, StartupError::InstanceLocked { .. }));

    first
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(first);
    replace_sqlite_database(&backup, fixture.platform_path());

    let mut restored = fixture.start().await;
    let pool = restored.platform().unwrap().sqlite_pool().unwrap();
    let devices: Vec<String> =
        sqlx::query_scalar("SELECT device_id FROM devices ORDER BY device_id")
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(devices, vec!["in-backup"]);
    restored
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn unacknowledged_stream_work_replays_after_a_full_runtime_restart() {
    let mut fixture = Fixture::sqlite().await;
    let mut runtime = fixture.start().await;
    let stream = runtime.stream().unwrap().clone();
    stream.append(telemetry_message(1)).await.unwrap();
    let interrupted_claim = stream
        .claim(claim("recovery", "interrupted"))
        .await
        .unwrap();
    assert_eq!(interrupted_claim.len(), 1);
    let shutdown_error = runtime.shutdown(Instant::now()).await.unwrap_err();
    assert!(matches!(shutdown_error, ShutdownError::DeadlineElapsed));
    drop(stream);
    drop(runtime);

    let mut restarted = fixture.start().await;
    let replay = restarted
        .stream()
        .unwrap()
        .claim(claim("recovery", "interrupted"))
        .await
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].offset, interrupted_claim[0].offset);
    restarted
        .stream()
        .unwrap()
        .acknowledge(AcknowledgeRequest::from_claims(
            "recovery",
            "interrupted",
            &replay,
        ))
        .await
        .unwrap();
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn retained_publish_is_delivered_after_a_real_broker_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("mqttd.sqlite");
    let first = ProtocolBroker::start(&database).await;
    let mut publisher =
        connect_mqtt(first.address(), "durable-retained-publisher", true, false).await;
    publisher
        .write_all(&mqtt_qos_one_retained_publish(
            "durable/retained",
            b"retained payload",
            17,
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x40, 0x02, 0x00, 0x11]
    );
    drop(publisher);
    first.shutdown();

    let second = ProtocolBroker::start(&database).await;
    let mut reader = connect_mqtt(second.address(), "durable-retained-reader", true, false).await;
    subscribe(&mut reader, "durable/retained", 23).await;
    let delivery = read_mqtt_packet(&mut reader).await;
    assert_eq!(delivery[0] & 0xf0, 0x30);
    assert_eq!(delivery[0] & 0x06, 0x02);
    assert_eq!(mqtt_publish_topic(&delivery), "durable/retained");
    assert_eq!(mqtt_publish_payload(&delivery), b"retained payload");
    assert_ne!(
        delivery[0] & 0x01,
        0,
        "retained delivery must retain its flag"
    );
    reader
        .write_all(&mqtt_puback(mqtt_publish_packet_id(&delivery)))
        .await
        .unwrap();
    drop(reader);
    second.shutdown();
}

#[tokio::test]
async fn broker_completes_restarted_qos2_pubrel_once() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("mqttd.sqlite");
    let first = ProtocolBroker::start(&database).await;
    let mut publisher = connect_mqtt(first.address(), "durable-qos2-publisher", false, false).await;
    publisher
        .write_all(&mqtt_qos_two_publish("durable/qos2", b"qos2 payload", 29))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x50, 0x02, 0x00, 0x1d]
    );
    drop(publisher);
    first.shutdown();

    let second = ProtocolBroker::start(&database).await;
    let mut reader = connect_mqtt(second.address(), "durable-qos2-reader", true, false).await;
    subscribe(&mut reader, "durable/qos2", 23).await;
    let mut publisher = connect_mqtt(second.address(), "durable-qos2-publisher", false, true).await;
    publisher.write_all(&mqtt_pubrel(29)).await.unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x70, 0x02, 0x00, 0x1d]
    );
    let delivery = read_mqtt_packet(&mut reader).await;
    assert_eq!(delivery[0] & 0xf0, 0x30);
    assert_eq!(delivery[0] & 0x06, 0x02);
    assert_eq!(mqtt_publish_topic(&delivery), "durable/qos2");
    assert_eq!(mqtt_publish_payload(&delivery), b"qos2 payload");
    reader
        .write_all(&mqtt_puback(mqtt_publish_packet_id(&delivery)))
        .await
        .unwrap();

    publisher.write_all(&mqtt_pubrel(29)).await.unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x70, 0x02, 0x00, 0x1d]
    );
    assert!(
        timeout(Duration::from_millis(300), read_mqtt_packet(&mut reader))
            .await
            .is_err(),
        "duplicate PUBREL must not create a second broker delivery"
    );
    drop(publisher);
    drop(reader);
    second.shutdown();
}

#[tokio::test]
async fn mqtt_runtime_appends_replayed_qos_one_telemetry_once_after_restart() {
    let mut fixture = Fixture::sqlite().await;
    let mut first = fixture.start().await;
    let token = provision_device_token(&first, "durable-device").await;
    let payload = telemetry_payload(42);
    let mut publisher =
        connect_device(fixture.config.mqtt_tcp, "durable-qos1-publisher", &token).await;
    publisher
        .write_all(&mqtt_qos_one_publish(
            "v1/devices/me/telemetry",
            &payload,
            29,
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x40, 0x02, 0x00, 0x1d]
    );
    drop(publisher);
    first
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(first);

    let mut restarted = fixture.start().await;
    let mut publisher =
        connect_device(fixture.config.mqtt_tcp, "durable-qos1-publisher", &token).await;
    publisher
        .write_all(&mqtt_qos_one_publish(
            "v1/devices/me/telemetry",
            &payload,
            30,
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x40, 0x02, 0x00, 0x1e]
    );
    drop(publisher);

    let records = restarted
        .stream()
        .unwrap()
        .claim(claim("qos1-recovery", "verifier"))
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        1,
        "replayed telemetry must append exactly one stream record"
    );
    let StreamMessage::Telemetry(message) = &records[0].message else {
        panic!("expected the replayed telemetry message in the durable stream");
    };
    assert_eq!(message.event.sequence, 42);
    restarted
        .stream()
        .unwrap()
        .acknowledge(AcknowledgeRequest::from_claims(
            "qos1-recovery",
            "verifier",
            &records,
        ))
        .await
        .unwrap();
    assert!(
        restarted
            .stream()
            .unwrap()
            .claim(claim("qos1-recovery", "verifier"))
            .await
            .unwrap()
            .is_empty()
    );
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn cache_expiry_is_pruned_when_the_monolith_reopens_internal_state() {
    let mut fixture = Fixture::sqlite().await;
    let mut runtime = fixture.start().await;
    runtime
        .cache()
        .unwrap()
        .put(CacheEntry {
            key: "device:expired-after-stop".to_owned(),
            value: b"stale".to_vec(),
            expires_at_ms: now_ms() + 60_000,
        })
        .await
        .unwrap();
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(runtime);

    let connection = rusqlite::Connection::open(fixture.state_path("cache.sqlite")).unwrap();
    connection
        .execute(
            "UPDATE cache_entries SET expires_at_ms = 0 WHERE key = 'device:expired-after-stop'",
            [],
        )
        .unwrap();
    drop(connection);

    let mut restarted = fixture.start().await;
    let connection = rusqlite::Connection::open(fixture.state_path("cache.sqlite")).unwrap();
    let persisted_entries: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM cache_entries WHERE key = 'device:expired-after-stop'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        persisted_entries, 0,
        "cache startup must prune expired rows before serving gets"
    );
    drop(connection);
    assert_eq!(
        restarted
            .cache()
            .unwrap()
            .get("device:expired-after-stop")
            .await
            .unwrap(),
        None
    );
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

fn replace_sqlite_database(backup: &std::path::Path, platform: &std::path::Path) {
    for suffix in ["", "-shm", "-wal"] {
        let path = PathBuf::from(format!("{}{}", platform.display(), suffix));
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    std::fs::copy(backup, platform).unwrap();
}

fn telemetry_message(sequence: u64) -> StreamMessage {
    let now = Utc::now();
    StreamMessage::Telemetry(TelemetryMessage {
        topic: "iot/v1/devices/durable-device/telemetry".to_owned(),
        payload: format!(r#"{{"sequence":{sequence}}}"#).into_bytes(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: "durable-device".to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence,
            event_at: now,
            measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(22.5))]),
            gateway_device_id: None,
        },
        received_at: now,
    })
}

fn claim(group: &str, member_id: &str) -> ClaimRequest {
    ClaimRequest {
        group: group.to_owned(),
        member_id: member_id.to_owned(),
        start: GroupStart::Earliest,
        limit: 10,
    }
}

async fn provision_device_token(runtime: &MonolithRuntime, device_id: &str) -> String {
    runtime
        .platform()
        .unwrap()
        .register_device(device_id)
        .await
        .unwrap();
    let token = generate_device_token();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, ?, ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(device_id)
    .bind(device_token_prefix(&token).unwrap())
    .bind(hash_device_token(&token).unwrap())
    .execute(runtime.platform().unwrap().sqlite_pool().unwrap())
    .await
    .unwrap();
    token
}

async fn connect_mqtt(
    address: SocketAddr,
    client_id: &str,
    clean_session: bool,
    expected_session_present: bool,
) -> TcpStream {
    connect_with_credentials(
        address,
        client_id,
        clean_session,
        None,
        expected_session_present,
    )
    .await
}

async fn connect_device(address: SocketAddr, client_id: &str, token: &str) -> TcpStream {
    connect_with_credentials(
        address,
        client_id,
        false,
        Some((DEVICE_TOKEN_USERNAME, token)),
        false,
    )
    .await
}

async fn connect_with_credentials(
    address: SocketAddr,
    client_id: &str,
    clean_session: bool,
    credentials: Option<(&str, &str)>,
    expected_session_present: bool,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&mqtt_connect(client_id, clean_session, credentials))
        .await
        .unwrap();
    let connack = read_mqtt_packet(&mut stream).await;
    assert_eq!(&connack[..2], [0x20, 0x02]);
    assert_eq!(connack[2], u8::from(expected_session_present));
    assert_eq!(connack[3], 0x00);
    stream
}

async fn subscribe(stream: &mut TcpStream, topic: &str, packet_id: u16) {
    stream
        .write_all(&mqtt_subscribe(topic, packet_id))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(stream).await,
        vec![0x90, 0x03, (packet_id >> 8) as u8, packet_id as u8, 0x01,]
    );
}

fn mqtt_connect(
    client_id: &str,
    clean_session: bool,
    credentials: Option<(&str, &str)>,
) -> Vec<u8> {
    let mut flags = if clean_session { 0x02 } else { 0x00 };
    if credentials.is_some() {
        flags |= 0xc0;
    }

    let mut variable_header = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, flags, 0x00, 0x3c];
    let mut payload = Vec::new();
    append_mqtt_string(&mut payload, client_id);
    if let Some((username, password)) = credentials {
        append_mqtt_string(&mut payload, username);
        append_mqtt_string(&mut payload, password);
    }

    let mut packet = vec![0x10];
    encode_remaining_length(variable_header.len() + payload.len(), &mut packet);
    packet.append(&mut variable_header);
    packet.extend(payload);
    packet
}

fn mqtt_qos_one_retained_publish(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    mqtt_publish(0x33, topic, payload, Some(packet_id))
}

fn mqtt_qos_one_publish(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    mqtt_publish(0x32, topic, payload, Some(packet_id))
}

fn mqtt_qos_two_publish(topic: &str, payload: &[u8], packet_id: u16) -> Vec<u8> {
    mqtt_publish(0x34, topic, payload, Some(packet_id))
}

fn mqtt_publish(header: u8, topic: &str, payload: &[u8], packet_id: Option<u16>) -> Vec<u8> {
    let mut variable_header = Vec::new();
    append_mqtt_string(&mut variable_header, topic);
    if let Some(packet_id) = packet_id {
        variable_header.extend_from_slice(&packet_id.to_be_bytes());
    }
    let mut packet = vec![header];
    encode_remaining_length(variable_header.len() + payload.len(), &mut packet);
    packet.extend(variable_header);
    packet.extend_from_slice(payload);
    packet
}

fn mqtt_subscribe(topic: &str, packet_id: u16) -> Vec<u8> {
    let mut payload = Vec::new();
    append_mqtt_string(&mut payload, topic);
    payload.push(0x01);
    let mut packet = vec![0x82];
    encode_remaining_length(2 + payload.len(), &mut packet);
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend(payload);
    packet
}

fn mqtt_puback(packet_id: u16) -> Vec<u8> {
    vec![0x40, 0x02, (packet_id >> 8) as u8, packet_id as u8]
}

fn mqtt_pubrel(packet_id: u16) -> Vec<u8> {
    vec![0x62, 0x02, (packet_id >> 8) as u8, packet_id as u8]
}

fn mqtt_publish_topic(packet: &[u8]) -> String {
    let body_offset = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([
        packet[body_offset],
        packet[body_offset + 1],
    ]));
    String::from_utf8(packet[body_offset + 2..body_offset + 2 + topic_length].to_vec()).unwrap()
}

fn mqtt_publish_packet_id(packet: &[u8]) -> u16 {
    let body_offset = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([
        packet[body_offset],
        packet[body_offset + 1],
    ]));
    let packet_id_offset = body_offset + 2 + topic_length;
    u16::from_be_bytes([packet[packet_id_offset], packet[packet_id_offset + 1]])
}

fn mqtt_publish_payload(packet: &[u8]) -> Vec<u8> {
    let body_offset = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([
        packet[body_offset],
        packet[body_offset + 1],
    ]));
    let packet_id_length = if packet[0] & 0x06 == 0 { 0 } else { 2 };
    packet[body_offset + 2 + topic_length + packet_id_length..].to_vec()
}

fn mqtt_packet_body_offset(packet: &[u8]) -> usize {
    let mut offset = 1;
    loop {
        let encoded = packet[offset];
        offset += 1;
        if encoded & 0x80 == 0 {
            return offset;
        }
    }
}

async fn read_mqtt_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut first = [0_u8; 1];
    timeout(Duration::from_secs(3), stream.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    let mut packet = first.to_vec();
    let mut remaining = 0_usize;
    let mut multiplier = 1_usize;
    loop {
        let mut encoded = [0_u8; 1];
        stream.read_exact(&mut encoded).await.unwrap();
        packet.push(encoded[0]);
        remaining += usize::from(encoded[0] & 0x7f) * multiplier;
        if encoded[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    let mut body = vec![0_u8; remaining];
    stream.read_exact(&mut body).await.unwrap();
    packet.extend(body);
    packet
}

fn encode_remaining_length(mut value: usize, packet: &mut Vec<u8>) {
    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value != 0 {
            encoded |= 0x80;
        }
        packet.push(encoded);
        if value == 0 {
            break;
        }
    }
}

fn append_mqtt_string(packet: &mut Vec<u8>, value: &str) {
    let length = u16::try_from(value.len()).unwrap();
    packet.extend_from_slice(&length.to_be_bytes());
    packet.extend_from_slice(value.as_bytes());
}

fn telemetry_payload(sequence: u64) -> Vec<u8> {
    json!({
        "schema_version": 1,
        "boot_id": "018f68d1-cc91-7000-8000-000000000002",
        "sequence": sequence,
        "event_at": Utc::now(),
        "measurements": { "temperature": 22.5 }
    })
    .to_string()
    .into_bytes()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn reserve_address() -> (SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (address, listener)
}
