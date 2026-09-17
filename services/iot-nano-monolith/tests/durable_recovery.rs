use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use iot_api::{TokenVault, create_platform_device_token};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_monolith::{CacheEntry, MonolithConfig, MonolithRuntime, ShutdownError, StartupError};
use iot_nano_mqttd::{
    BrokerLifecycleHandle, BrokerStorage, ListenerConfiguration, MuxSettings,
    PreboundBackendListeners, ProtocolBackends, SqliteStorage,
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
const MAX_MQTT_PACKET_BODY_SIZE: usize = 1024 * 1024;

#[tokio::test]
async fn mqtt_packet_reader_rejects_a_fifth_remaining_length_byte() {
    let panic = panic_from_mqtt_packet_reader(vec![0x10, 0x80, 0x80, 0x80, 0x80, 0x00]).await;

    assert!(
        panic.contains("four bytes"),
        "unexpected MQTT parser panic: {panic}"
    );
}

#[tokio::test]
async fn mqtt_packet_reader_rejects_a_body_larger_than_the_test_limit() {
    let oversized_body = vec![
        0x10, 0x80, 0x80, 0x41, // more than 1 MiB remaining length
    ]
    .into_iter()
    .chain(std::iter::repeat(0_u8).take(MAX_MQTT_PACKET_BODY_SIZE))
    .collect();
    let panic = panic_from_mqtt_packet_reader(oversized_body).await;

    assert!(
        panic.contains("exceeds"),
        "unexpected MQTT parser panic: {panic}"
    );
}

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
}

impl Fixture {
    async fn sqlite() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mqtt_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");

        Self {
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
        }
    }

    fn platform_path(&self) -> &std::path::Path {
        self.config.storage.sqlite_path.as_deref().unwrap()
    }

    async fn start(&self) -> (MonolithRuntime, SocketAddr) {
        let runtime = MonolithRuntime::start(self.config.clone()).await.unwrap();
        let mqtt_tcp = runtime.mqtt_tcp_address().unwrap();
        (runtime, mqtt_tcp)
    }
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
    let fixture = Fixture::sqlite().await;
    let (mut first, _) = fixture.start().await;
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

    let (mut restored, _) = fixture.start().await;
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
    let fixture = Fixture::sqlite().await;
    let (mut runtime, _) = fixture.start().await;
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

    let (mut restarted, _) = fixture.start().await;
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
    publisher
        .write_all(&mqtt_qos_one_publish(
            "durable/qos2",
            b"post-pubrel marker",
            30,
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mqtt_packet(&mut publisher).await,
        vec![0x40, 0x02, 0x00, 0x1e]
    );
    let marker = read_mqtt_packet(&mut reader).await;
    assert_eq!(
        mqtt_publish_payload(&marker),
        b"post-pubrel marker",
        "the second delivery after the duplicate PUBREL must be the ordered marker"
    );
    reader
        .write_all(&mqtt_puback(mqtt_publish_packet_id(&marker)))
        .await
        .unwrap();
    drop(publisher);
    drop(reader);
    second.shutdown();
}

#[tokio::test]
async fn mqtt_runtime_appends_replayed_qos_one_telemetry_once_after_restart() {
    let fixture = Fixture::sqlite().await;
    let (mut first, first_mqtt_tcp) = fixture.start().await;
    let token = provision_device_token(&first, "durable-device").await;
    let payload = telemetry_payload(42);
    let mut publisher = connect_device(first_mqtt_tcp, "durable-qos1-publisher", &token).await;
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

    let (mut restarted, restarted_mqtt_tcp) = fixture.start().await;
    let mut publisher = connect_device(restarted_mqtt_tcp, "durable-qos1-publisher", &token).await;
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
async fn expired_cache_entries_remain_hidden_after_a_monolith_restart() {
    let fixture = Fixture::sqlite().await;
    let (mut runtime, _) = fixture.start().await;
    let expires_at_ms = now_ms() + 50;
    runtime
        .cache()
        .unwrap()
        .put(CacheEntry {
            key: "device:expired-after-stop".to_owned(),
            value: b"stale".to_vec(),
            expires_at_ms,
        })
        .await
        .unwrap();
    assert_eq!(
        runtime
            .cache()
            .unwrap()
            .get("device:expired-after-stop")
            .await
            .unwrap(),
        Some(b"stale".to_vec())
    );
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(runtime);
    timeout(Duration::from_secs(1), async {
        while now_ms() <= expires_at_ms {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cache entry did not reach its public expiration deadline");

    let (mut restarted, _) = fixture.start().await;
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
    let store = runtime.platform().unwrap();
    let tenant_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, ?, 'active')")
        .bind(tenant_id.to_string())
        .bind(format!("durable-recovery-{tenant_id}"))
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES (?, ?)")
        .bind(device_id)
        .bind(tenant_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    create_platform_device_token(
        store,
        &TokenVault::from_key_material("durable-recovery-device-token-key-material-0001"),
        tenant_id,
        device_id,
    )
    .await
    .unwrap()
    .token
    .unwrap()
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
    timeout(Duration::from_secs(3), async {
        let mut first = [0_u8; 1];
        stream.read_exact(&mut first).await.unwrap();
        let mut packet = first.to_vec();
        let mut remaining = 0_usize;
        let mut multiplier = 1_usize;
        for _ in 0..4 {
            let mut encoded = [0_u8; 1];
            stream.read_exact(&mut encoded).await.unwrap();
            packet.push(encoded[0]);
            let value = usize::from(encoded[0] & 0x7f)
                .checked_mul(multiplier)
                .expect("MQTT remaining length arithmetic overflowed");
            remaining = remaining
                .checked_add(value)
                .expect("MQTT remaining length arithmetic overflowed");
            if encoded[0] & 0x80 == 0 {
                assert!(
                    remaining <= MAX_MQTT_PACKET_BODY_SIZE,
                    "MQTT packet body exceeds {MAX_MQTT_PACKET_BODY_SIZE} bytes"
                );
                let mut body = vec![0_u8; remaining];
                stream.read_exact(&mut body).await.unwrap();
                packet.extend(body);
                return packet;
            }
            multiplier = multiplier
                .checked_mul(128)
                .expect("MQTT remaining length arithmetic overflowed");
        }
        panic!("MQTT remaining length uses more than four bytes");
    })
    .await
    .expect("MQTT packet read timed out")
}

async fn panic_from_mqtt_packet_reader(packet: Vec<u8>) -> String {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let writer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = stream.write_all(&packet).await;
    });
    let mut reader = TcpStream::connect(address).await.unwrap();
    let panic = tokio::spawn(async move { read_mqtt_packet(&mut reader).await })
        .await
        .expect_err("malformed MQTT packet was accepted")
        .into_panic();
    writer.await.unwrap();

    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_else(|| "unknown panic payload".to_owned())
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
