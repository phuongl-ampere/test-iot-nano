use std::{path::PathBuf, process::Stdio, time::Duration};

use iot_nano_mqttd::{BrokerFileConfig, StorageConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    time::sleep,
};

const STANDALONE_CONFIG: &str = include_str!("../config/standalone.toml");
const STANDALONE_SERVICE: &str =
    include_str!("../../../infra/systemd/iot-nano-mqttd-standalone.service");
const STANDALONE_INSTALLER: &str = include_str!("../../../scripts/install-mqttd-standalone.sh");

#[test]
fn standalone_example_disables_platform_transport_and_enables_static_acl() {
    let config = BrokerFileConfig::from_toml(STANDALONE_CONFIG).unwrap();

    assert!(!config.device_transport.enabled);
    assert!(matches!(config.storage, StorageConfig::Sqlite { .. }));
    assert!(config.static_acl.as_ref().is_some_and(|acl| acl.enabled));
    assert!(config.http_authorization.is_none());
    assert!(config.listeners.tcp.address.ip().is_loopback());
}

#[tokio::test]
async fn standalone_binary_starts_without_platform_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let mqtt_address = reserve_address().await;
    let tls_address = reserve_address().await;
    let management_address = reserve_address().await;
    let v311_address = reserve_address().await;
    let v5_address = reserve_address().await;
    let certificate = fixture("server.crt");
    let key = fixture("server.key");
    let database = directory.path().join("broker.sqlite");
    let configuration = directory.path().join("broker.toml");
    std::fs::write(
        &configuration,
        standalone_config(
            mqtt_address,
            tls_address,
            management_address,
            v311_address,
            v5_address,
            &certificate,
            &key,
            &database,
        ),
    )
    .unwrap();

    let mut child = TestChild {
        child: Command::new(env!("CARGO_BIN_EXE_iot-nano-mqttd"))
            .arg("--config")
            .arg(&configuration)
            .env_remove("IOT_MQTTD_API_BASE_URL")
            .env_remove("IOT_NANO_MQTTD_API_SECRET")
            .env_remove("IOT_NANO_API_MQTTD_SECRET")
            .env_remove("IOT_NANO_CORE_MQTTD_SECRET")
            .env_remove("IOT_NANO_STREAM_URL")
            .env_remove("IOT_NANO_MQTTD_STREAM_SECRET")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    };

    wait_for_health(management_address).await;

    let status = reqwest::Client::new()
        .get(format!("http://{management_address}/api/v1/broker/status"))
        .basic_auth("admin", Some("management-password"))
        .send()
        .await
        .unwrap();
    assert!(status.status().is_success());
    let status: serde_json::Value = status.json().await.unwrap();
    let capabilities = status["capabilities"].as_array().unwrap();
    assert!(
        capabilities
            .iter()
            .any(|capability| capability == "sqlite_persistence")
    );
    assert!(
        capabilities
            .iter()
            .any(|capability| capability == "static_acl")
    );
    assert!(
        capabilities
            .iter()
            .any(|capability| capability == "mqtt5_topic_alias")
    );
    assert!(
        !capabilities
            .iter()
            .any(|capability| capability == "mqtt311_native_device_transport")
    );

    let mut mqtt = TcpStream::connect(mqtt_address).await.unwrap();
    mqtt.write_all(&mqtt5_connect(
        "standalone-client",
        "device-operator",
        "device-password",
    ))
    .await
    .unwrap();
    let mut connack = [0_u8; 8];
    mqtt.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack, [0x20, 0x06, 0x00, 0x00, 0x03, 0x22, 0x10, 0x00]);

    mqtt.write_all(&mqtt5_qos1_publish(
        "sensors/unit-1/telemetry",
        b"{\"temperature_c\":24.5}",
        1,
        1,
    ))
    .await
    .unwrap();
    let mut puback = [0_u8; 4];
    mqtt.read_exact(&mut puback).await.unwrap();
    assert_eq!(puback, [0x40, 0x02, 0x00, 0x01]);

    mqtt.write_all(&mqtt5_qos1_publish("", b"{\"temperature_c\":25.0}", 2, 1))
        .await
        .unwrap();
    mqtt.read_exact(&mut puback).await.unwrap();
    assert_eq!(puback, [0x40, 0x02, 0x00, 0x02]);

    child.shutdown().await;
}

#[test]
fn standalone_package_installs_with_the_required_privileges_and_no_platform_dependencies() {
    assert!(STANDALONE_SERVICE.contains("After=network-online.target"));
    assert!(STANDALONE_SERVICE.contains(
        "ExecStart=/opt/rush-iot-nano/iot-nano-mqttd --config /etc/rush-iot-nano/iot-nano-mqttd.toml"
    ));
    assert!(STANDALONE_SERVICE.contains("AmbientCapabilities=CAP_NET_BIND_SERVICE"));
    assert!(STANDALONE_SERVICE.contains("CapabilityBoundingSet=CAP_NET_BIND_SERVICE"));
    assert!(!STANDALONE_SERVICE.contains("iot-nano-api.service"));
    assert!(!STANDALONE_SERVICE.contains("iot-nano-stream.service"));
    assert!(
        STANDALONE_INSTALLER.contains("\"$cargo_bin\" build --release --package iot-nano-mqttd")
    );
    assert!(
        STANDALONE_INSTALLER.contains("\"$install_bin\" -d --owner iot --group iot --mode 0700")
    );
    assert!(STANDALONE_INSTALLER.contains("iot-nano-mqttd-standalone.service"));
    assert!(STANDALONE_INSTALLER.contains("if [ ! -e \"$config_path\" ]; then"));
}

async fn reserve_address() -> std::net::SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn wait_for_health(address: std::net::SocketAddr) {
    let client = reqwest::Client::new();
    for _ in 0..400 {
        if client
            .get(format!("http://{address}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("standalone broker did not become healthy");
}

struct TestChild {
    child: Child,
}

impl TestChild {
    async fn shutdown(&mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

impl Drop for TestChild {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn standalone_config(
    mqtt_address: std::net::SocketAddr,
    tls_address: std::net::SocketAddr,
    management_address: std::net::SocketAddr,
    v311_address: std::net::SocketAddr,
    v5_address: std::net::SocketAddr,
    certificate: &PathBuf,
    key: &PathBuf,
    database: &PathBuf,
) -> String {
    format!(
        r#"
version = 1

[listeners]
v311_backend_address = "{v311_address}"
v5_backend_address = "{v5_address}"

[listeners.tcp]
address = "{mqtt_address}"

[listeners.tls]
address = "{tls_address}"
certificate_path = "{}"
key_path = "{}"

[storage]
kind = "sqlite"
path = "{}"

[management]
address = "{management_address}"
username = "admin"
password = "management-password"

[device_transport]
enabled = false

[static_acl]
enabled = true

[[static_acl.users]]
username = "device-operator"
password = "device-password"

[[static_acl.rules]]
identity = "device-operator"
topic = "sensors/+/telemetry"
publish = true
"#,
        certificate.display(),
        key.display(),
        database.display(),
    )
}

fn mqtt5_connect(client_id: &str, username: &str, password: &str) -> Vec<u8> {
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

fn mqtt5_qos1_publish(topic: &str, payload: &[u8], packet_id: u16, topic_alias: u16) -> Vec<u8> {
    let remaining = 2 + topic.len() + 2 + 1 + 3 + payload.len();
    let mut packet = vec![0x32, remaining as u8];
    packet.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(&[0x03, 0x23]);
    packet.extend_from_slice(&topic_alias.to_be_bytes());
    packet.extend_from_slice(payload);
    packet
}
