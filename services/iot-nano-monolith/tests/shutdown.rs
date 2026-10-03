use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::{Duration as ChronoDuration, Utc};
use iot_api::{TokenVault, provision_platform_device_token};
use iot_nano_foundation::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_nano_monolith::{MonolithConfig, MonolithRuntime, ShutdownError};
use iot_storage::{NewCommandOutboxEntry, PlatformStore};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

const DEVICE_NAME: &str = "shutdown-meter";
const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_PACKET_BYTES: usize = 1_048_576;

fn provisioning_tenant_id() -> Uuid {
    Uuid::from_u128(10_007)
}

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        Self {
            config: MonolithConfig {
                storage: StorageConfiguration {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(root.join("platform.sqlite")),
                    sqlite_busy_timeout_ms: 5_000,
                },
                device_token_vault_key: vault_key().to_owned(),
                internal_dir: root.join("internal"),
                http: reserve_address().await,
                mqtt_tcp: reserve_address().await,
                mqtt_tls: reserve_address().await,
                web_https_enabled: false,
                tls_cert_path: fixtures.join("server.crt"),
                tls_key_path: fixtures.join("server.key"),
                shutdown_deadline: SHUTDOWN_TIMEOUT,
            },
            _directory: directory,
        }
    }

    async fn wait_for_listener_release(&self, addresses: &[SocketAddr]) {
        timeout(IO_TIMEOUT, async {
            loop {
                let mut listeners = Vec::new();
                for address in addresses {
                    match TcpListener::bind(*address).await {
                        Ok(listener) => listeners.push(listener),
                        Err(_) => break,
                    }
                }
                if listeners.len() == addresses.len() {
                    drop(listeners);
                    return;
                }
                drop(listeners);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown did not release the requested listener addresses");
    }
}

#[tokio::test]
async fn shutdown_handoff_finishes_an_in_flight_core_command_after_listener_closure() {
    let fixture = Fixture::new().await;
    let runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    let store = PlatformStore::open(&fixture.config.storage).await.unwrap();
    let (mut device, device_id) = provision_and_connect(&runtime, fixture.config.mqtt_tcp).await;
    let command_id = Uuid::now_v7();

    enqueue_command(&store, command_id, &device_id).await;
    let published = read_packet(&mut device).await;
    assert_eq!(
        packet_topic(&published),
        format!("v1/devices/me/rpc/request/{command_id}")
    );
    assert_eq!(command_state(&store, command_id).await, "leased");

    let mut shutdown = start_shutdown(runtime);
    fixture
        .wait_for_listener_release(&[fixture.config.http])
        .await;
    assert!(
        timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "shutdown must preserve the in-flight claim until the device PUBACK"
    );

    device
        .write_all(&puback(packet_id(&published)))
        .await
        .unwrap();
    wait_for_state(&store, command_id, "published_to_broker").await;
    drop(device);

    assert!(
        timeout(SHUTDOWN_TIMEOUT, shutdown)
            .await
            .expect("shutdown exceeded its test deadline")
            .expect("shutdown task panicked")
            .is_ok()
    );
    fixture
        .wait_for_listener_release(&[
            fixture.config.http,
            fixture.config.mqtt_tcp,
            fixture.config.mqtt_tls,
        ])
        .await;
}

fn start_shutdown(mut runtime: MonolithRuntime) -> JoinHandle<Result<(), ShutdownError>> {
    tokio::spawn(async move { runtime.shutdown(Instant::now() + SHUTDOWN_TIMEOUT).await })
}

async fn provision_and_connect(
    runtime: &MonolithRuntime,
    address: SocketAddr,
) -> (TcpStream, String) {
    let vault = TokenVault::from_key_material(vault_key());
    let store = runtime.platform().unwrap();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'shutdown-provisioning', 'active')",
    )
    .bind(provisioning_tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let provisioned =
        provision_platform_device_token(store, &vault, provisioning_tenant_id(), DEVICE_NAME)
            .await
            .unwrap();
    let token = provisioned.token.unwrap();
    let device_id = provisioned.device_id;
    let mut device = TcpStream::connect(address).await.unwrap();
    device
        .write_all(&connect(&device_id, DEVICE_TOKEN_USERNAME, &token))
        .await
        .unwrap();
    assert_eq!(read_packet(&mut device).await, vec![0x20, 0x02, 0x00, 0x00]);
    device
        .write_all(&subscribe("v1/devices/me/rpc/request/+", 1))
        .await
        .unwrap();
    assert_eq!(
        read_packet(&mut device).await,
        vec![0x90, 0x03, 0x00, 0x01, 0x01]
    );
    (device, device_id)
}

async fn enqueue_command(store: &PlatformStore, command_id: Uuid, device_id: &str) {
    let now = Utc::now();
    store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            tenant_id: provisioning_tenant_id(),
            device_id: device_id.to_owned(),
            method: "sample_now".to_owned(),
            params: r#"{"source":"shutdown"}"#.to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + ChronoDuration::minutes(5),
            next_attempt_at: now,
        })
        .await
        .unwrap();
}

async fn command_state(store: &PlatformStore, command_id: Uuid) -> String {
    sqlx::query_scalar("SELECT state FROM command_outbox WHERE id = ?")
        .bind(command_id.to_string())
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
}

async fn wait_for_state(store: &PlatformStore, command_id: Uuid, expected: &str) {
    timeout(IO_TIMEOUT, async {
        loop {
            if command_state(store, command_id).await == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("command {command_id} did not reach {expected}"));
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn vault_key() -> &'static str {
    "shutdown-test-device-token-vault-key-material-0001"
}

fn connect(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let remaining = 10 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
    let mut packet = vec![0x10];
    encode_remaining_length(remaining, &mut packet);
    packet.extend_from_slice(&[
        0,
        4,
        b'M',
        b'Q',
        b'T',
        b'T',
        4,
        0xc2,
        0,
        0x3c,
        (client_id.len() >> 8) as u8,
        client_id.len() as u8,
    ]);
    packet.extend_from_slice(client_id.as_bytes());
    packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
    packet.extend_from_slice(username.as_bytes());
    packet.extend_from_slice(&(password.len() as u16).to_be_bytes());
    packet.extend_from_slice(password.as_bytes());
    packet
}

fn subscribe(topic: &str, id: u16) -> Vec<u8> {
    let mut packet = vec![0x82];
    encode_remaining_length(5 + topic.len(), &mut packet);
    packet.extend_from_slice(&id.to_be_bytes());
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.push(1);
    packet
}

fn puback(id: u16) -> [u8; 4] {
    [0x40, 0x02, (id >> 8) as u8, id as u8]
}

fn packet_topic(packet: &[u8]) -> String {
    let body = packet_body_offset(packet);
    let length = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    String::from_utf8(packet[body + 2..body + 2 + length].to_vec()).unwrap()
}

fn packet_id(packet: &[u8]) -> u16 {
    let body = packet_body_offset(packet);
    let length = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    u16::from_be_bytes([packet[body + 2 + length], packet[body + 3 + length]])
}

fn packet_body_offset(packet: &[u8]) -> usize {
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

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut first_byte = [0; 1];
    read_exact(stream, &mut first_byte).await;
    let mut packet = first_byte.to_vec();
    let mut remaining = 0_usize;
    let mut multiplier = 1_usize;
    for _ in 0..4 {
        let mut encoded = [0; 1];
        read_exact(stream, &mut encoded).await;
        packet.push(encoded[0]);
        remaining += usize::from(encoded[0] & 0x7f) * multiplier;
        if encoded[0] & 0x80 == 0 {
            assert!(
                remaining <= MAX_PACKET_BYTES,
                "MQTT packet exceeds test read limit"
            );
            let mut body = vec![0; remaining];
            read_exact(stream, &mut body).await;
            packet.extend(body);
            return packet;
        }
        multiplier *= 128;
    }
    panic!("MQTT remaining length uses more than four bytes");
}

async fn read_exact(stream: &mut TcpStream, buffer: &mut [u8]) {
    timeout(IO_TIMEOUT, stream.read_exact(buffer))
        .await
        .expect("packet read timed out")
        .expect("packet read failed");
}
