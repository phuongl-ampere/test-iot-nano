use std::{net::SocketAddr, path::PathBuf, process::Stdio, time::Duration};

use chrono::Utc;
use reqwest::{
    Client, StatusCode,
    header::{COOKIE, SET_COOKIE},
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    time::{sleep, timeout},
};

struct Fixture {
    _directory: TempDir,
    platform_path: PathBuf,
    internal_dir: PathBuf,
    public_address: SocketAddr,
    management_address: SocketAddr,
    mqtt_tcp_address: SocketAddr,
    mqtt_tls_address: SocketAddr,
    tls_cert_path: PathBuf,
    tls_key_path: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let tls_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        Self {
            platform_path: root.join("platform.sqlite"),
            internal_dir: root.join("internal"),
            public_address: reserve_address().await,
            management_address: reserve_address().await,
            mqtt_tcp_address: reserve_address().await,
            mqtt_tls_address: reserve_address().await,
            tls_cert_path: tls_fixtures.join("server.crt"),
            tls_key_path: tls_fixtures.join("server.key"),
            _directory: directory,
        }
    }

    fn configure(&self, command: &mut Command) {
        command
            .env_clear()
            .env("IOT_NANO_STORAGE", "sqlite")
            .env("IOT_NANO_SQLITE_PATH", &self.platform_path)
            .env("IOT_NANO_INTERNAL_DIR", &self.internal_dir)
            .env("IOT_NANO_TLS_CERT_PATH", &self.tls_cert_path)
            .env("IOT_NANO_TLS_KEY_PATH", &self.tls_key_path)
            .env(
                "IOT_DEVICE_TOKEN_VAULT_KEY",
                "e2e-device-token-vault-key-material-0001",
            )
            .env(
                "IOT_NANO_PUBLIC_HTTP_ADDRESS",
                self.public_address.to_string(),
            )
            .env(
                "IOT_NANO_MANAGEMENT_ADDRESS",
                self.management_address.to_string(),
            )
            .env(
                "IOT_NANO_MQTT_TCP_ADDRESS",
                self.mqtt_tcp_address.to_string(),
            )
            .env(
                "IOT_NANO_MQTT_TLS_ADDRESS",
                self.mqtt_tls_address.to_string(),
            )
            .env("IOT_NANO_SHUTDOWN_DEADLINE_SECONDS", "10");
    }
}

#[tokio::test]
async fn sqlite_monolith_process_runs_management_oauth_public_api_and_graceful_shutdown() {
    let fixture = Fixture::new().await;
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");
    let mut bootstrap = Command::new(binary);
    fixture.configure(&mut bootstrap);
    let output = bootstrap
        .arg("--bootstrap-admin")
        .env("IOT_NANO_BOOTSTRAP_ADMIN_USERNAME", "e2e-admin")
        .env(
            "IOT_NANO_BOOTSTRAP_ADMIN_PASSWORD",
            "E2eBootstrapAdmin@2026",
        )
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let mut command = Command::new(binary);
    fixture.configure(&mut command);
    let mut child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let result = run_e2e_flow(&fixture, &mut child).await;
    if child.try_wait().unwrap().is_none() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    result.unwrap();
}

async fn run_e2e_flow(
    fixture: &Fixture,
    child: &mut tokio::process::Child,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    wait_ready(&client, fixture.public_address).await?;

    let login = client
        .post(format!(
            "http://{}/api/auth/login",
            fixture.management_address
        ))
        .json(&json!({ "username": "e2e-admin", "password": "E2eBootstrapAdmin@2026" }))
        .send()
        .await?;
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login
        .headers()
        .get(SET_COOKIE)
        .expect("management login did not issue a session cookie")
        .to_str()?
        .split(';')
        .next()
        .expect("session cookie was empty")
        .to_owned();

    let registered = client
        .post(format!(
            "http://{}/api/management/applications",
            fixture.management_address
        ))
        .header(COOKIE, &cookie)
        .json(&json!({
            "app_id": "e2e-app",
            "kind": "full_stack",
            "launch_url": "https://client.example.test",
            "client_id": "e2e-client",
            "redirect_uris": ["https://client.example.test/callback"],
            "allowed_scopes": ["devices:read", "devices:write", "telemetry:read"],
            "enabled": true,
            "client_secret": "E2eClientSecret@2026"
        }))
        .send()
        .await?;
    assert_eq!(registered.status(), StatusCode::CREATED);

    let token: serde_json::Value = client
        .post(format!("http://{}/oauth/token", fixture.public_address))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", "e2e-client"),
            ("client_secret", "E2eClientSecret@2026"),
            ("scope", "devices:read devices:write telemetry:read"),
        ])
        .send()
        .await?
        .json()
        .await?;
    let access_token = token["access_token"].as_str().unwrap();

    let created = client
        .post(format!("http://{}/api/v1/devices", fixture.public_address))
        .bearer_auth(access_token)
        .json(&json!({
            "device_id": "e2e-device",
            "display_name": "E2E Device",
            "metadata": { "source": "e2e" }
        }))
        .send()
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);

    let devices: serde_json::Value = client
        .get(format!("http://{}/api/v1/devices", fixture.public_address))
        .bearer_auth(access_token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(devices["items"][0]["device_id"], "e2e-device");

    let detail: serde_json::Value = client
        .get(format!(
            "http://{}/api/v1/devices/e2e-device",
            fixture.public_address
        ))
        .bearer_auth(access_token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(detail["metadata"]["source"], "e2e");

    let device_token: serde_json::Value = client
        .post(format!(
            "http://{}/api/management/devices/e2e-device/tokens",
            fixture.management_address
        ))
        .header(COOKIE, &cookie)
        .send()
        .await?
        .json()
        .await?;
    let device_token = device_token["token"]
        .as_str()
        .expect("management device token response omitted the plaintext token");

    publish_device_telemetry(fixture.mqtt_tcp_address, "e2e-device", device_token).await?;
    let telemetry = wait_for_telemetry(&client, fixture.public_address, access_token).await?;
    assert_eq!(telemetry["items"][0]["device_id"], "e2e-device");
    assert_eq!(telemetry["items"][0]["measurements"]["temperature_c"], 22.5);
    let stream = rusqlite::Connection::open(fixture.internal_dir.join("stream.sqlite"))?;
    let records: i64 =
        stream.query_row("SELECT COUNT(*) FROM stream_records", [], |row| row.get(0))?;
    assert_eq!(records, 1);

    TcpStream::connect(fixture.mqtt_tcp_address).await?;
    TcpStream::connect(fixture.mqtt_tls_address).await?;

    let pid = child.id().expect("monolith process has no PID");
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    assert!(status.success());
    let exit = timeout(Duration::from_secs(15), child.wait()).await??;
    assert!(exit.success(), "monolith exited with {exit}");
    for address in [
        fixture.public_address,
        fixture.management_address,
        fixture.mqtt_tcp_address,
        fixture.mqtt_tls_address,
    ] {
        let listener = TcpListener::bind(address).await?;
        drop(listener);
    }
    Ok(())
}

async fn wait_ready(
    client: &Client,
    address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..100 {
        if client
            .get(format!("http://{address}/healthz"))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK)
        {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err("monolith did not become ready".into())
}

async fn publish_device_telemetry(
    address: SocketAddr,
    device_id: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut device = TcpStream::connect(address).await?;
    device
        .write_all(&v311_connect(device_id, "iotd_device_token", token))
        .await?;
    assert_eq!(
        read_mqtt_packet(&mut device).await?,
        vec![0x20, 0x02, 0x00, 0x00]
    );
    device
        .write_all(&v311_qos_one_publish(
            "v1/devices/me/telemetry",
            &telemetry_payload(),
            7,
        ))
        .await?;
    assert_eq!(
        read_mqtt_packet(&mut device).await?,
        vec![0x40, 0x02, 0x00, 0x07]
    );
    Ok(())
}

async fn wait_for_telemetry(
    client: &Client,
    address: SocketAddr,
    access_token: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    for _ in 0..100 {
        let response = client
            .get(format!("http://{address}/api/v1/telemetry/e2e-device"))
            .bearer_auth(access_token)
            .send()
            .await?;
        if response.status() == StatusCode::OK {
            let telemetry: serde_json::Value = response.json().await?;
            if telemetry["items"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
            {
                return Ok(telemetry);
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err("telemetry did not reach the public API".into())
}

fn telemetry_payload() -> Vec<u8> {
    json!({
        "schema_version": 1,
        "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
        "sequence": 1,
        "event_at": Utc::now(),
        "measurements": { "temperature_c": 22.5 }
    })
    .to_string()
    .into_bytes()
}

fn v311_connect(client_id: &str, username: &str, password: &str) -> Vec<u8> {
    let remaining = 10 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
    let mut packet = vec![
        0x10,
        0x00,
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
    let mut encoded_remaining = Vec::new();
    encode_remaining_length(remaining, &mut encoded_remaining);
    packet.splice(1..2, encoded_remaining);
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

async fn read_mqtt_packet(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut first = [0_u8; 2];
    timeout(Duration::from_secs(2), stream.read_exact(&mut first)).await??;
    let mut packet = first.to_vec();
    let mut encoded = first[1];
    let mut multiplier = 1_usize;
    let mut remaining = usize::from(first[1] & 0x7f);
    while encoded & 0x80 != 0 {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await?;
        packet.push(byte[0]);
        multiplier *= 128;
        remaining += usize::from(byte[0] & 0x7f) * multiplier;
        encoded = byte[0];
    }
    let mut body = vec![0_u8; remaining];
    stream.read_exact(&mut body).await?;
    packet.extend(body);
    Ok(packet)
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}
