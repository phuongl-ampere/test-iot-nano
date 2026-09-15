use std::{
    env, error::Error, io, net::SocketAddr, path::PathBuf, process::Stdio, str::FromStr,
    time::Duration,
};

use chrono::Utc;
use reqwest::{
    Client, Response, StatusCode,
    header::{COOKIE, SET_COOKIE},
};
use serde_json::json;
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStderr, Command},
    task::JoinHandle,
    time::{sleep, timeout},
};
use uuid::Uuid;

const TEST_DATABASE_PREFIX: &str = "iot_nano_test_";
const DEVICE_TOKEN_VAULT_KEY: &str = "e2e-timescale-device-token-vault-key-material-0001";
const START_ATTEMPTS: usize = 3;
const READY_ATTEMPTS: usize = 200;
const MAX_CHILD_DIAGNOSTIC_BYTES: usize = 4 * 1024;
const TRUNCATED_DIAGNOSTIC_SUFFIX: &str = " [truncated]";

type E2eResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct InitialDatabaseState {
    had_timescaledb: bool,
    had_uuid_ossp: bool,
}

struct ManagedChild {
    child: Child,
    stderr: Option<JoinHandle<io::Result<String>>>,
}

struct Fixture {
    _directory: TempDir,
    database_url: String,
    platform_path: PathBuf,
    internal_dir: PathBuf,
    public_address: SocketAddr,
    management_address: SocketAddr,
    mqtt_tcp_address: SocketAddr,
    mqtt_tls_address: SocketAddr,
    reserved_listeners: Option<Vec<TcpListener>>,
    tls_cert_path: PathBuf,
    tls_key_path: PathBuf,
}

impl Fixture {
    async fn from_environment() -> E2eResult<Self> {
        let database_url = validated_test_database_url(
            &env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap_or_default(),
        )
        .map_err(test_error)?;
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let tls_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        let (public_address, public_listener) = reserve_address().await?;
        let (management_address, management_listener) = reserve_address().await?;
        let (mqtt_tcp_address, mqtt_tcp_listener) = reserve_address().await?;
        let (mqtt_tls_address, mqtt_tls_listener) = reserve_address().await?;

        Ok(Self {
            _directory: directory,
            database_url,
            platform_path: root.join("platform.sqlite"),
            internal_dir: root.join("internal"),
            public_address,
            management_address,
            mqtt_tcp_address,
            mqtt_tls_address,
            reserved_listeners: Some(vec![
                public_listener,
                management_listener,
                mqtt_tcp_listener,
                mqtt_tls_listener,
            ]),
            tls_cert_path: tls_fixtures.join("server.crt"),
            tls_key_path: tls_fixtures.join("server.key"),
        })
    }

    async fn rotate_reserved_addresses(&mut self) -> E2eResult {
        let (public_address, public_listener) = reserve_address().await?;
        let (management_address, management_listener) = reserve_address().await?;
        let (mqtt_tcp_address, mqtt_tcp_listener) = reserve_address().await?;
        let (mqtt_tls_address, mqtt_tls_listener) = reserve_address().await?;
        self.public_address = public_address;
        self.management_address = management_address;
        self.mqtt_tcp_address = mqtt_tcp_address;
        self.mqtt_tls_address = mqtt_tls_address;
        self.reserved_listeners = Some(vec![
            public_listener,
            management_listener,
            mqtt_tcp_listener,
            mqtt_tls_listener,
        ]);
        Ok(())
    }

    fn release_reserved_addresses(&mut self) {
        self.reserved_listeners.take();
    }

    fn configure(&self, command: &mut Command) {
        command
            .env_clear()
            .env("IOT_NANO_STORAGE", "timescale")
            .env("DATABASE_URL", &self.database_url)
            .env("IOT_NANO_INTERNAL_DIR", &self.internal_dir)
            .env("IOT_NANO_TLS_CERT_PATH", &self.tls_cert_path)
            .env("IOT_NANO_TLS_KEY_PATH", &self.tls_key_path)
            .env("IOT_DEVICE_TOKEN_VAULT_KEY", DEVICE_TOKEN_VAULT_KEY)
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

    fn child_redactions(&self, additional_values: &[&str]) -> Vec<String> {
        let mut values = vec![
            self.database_url.clone(),
            DEVICE_TOKEN_VAULT_KEY.to_owned(),
            self.tls_key_path.display().to_string(),
        ];
        values.extend(
            additional_values
                .iter()
                .filter(|value| !value.is_empty())
                .map(|value| (*value).to_owned()),
        );
        values
    }
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL naming an unused disposable test database"]
async fn timescale_monolith_process_exercises_management_mqtt_and_public_telemetry() -> E2eResult {
    let fixture = Fixture::from_environment().await?;
    let database_url = fixture.database_url.clone();
    let initial_database_state = assert_pristine_test_database(&database_url).await?;

    // A task boundary lets the outer test clean the schema even if the process flow panics.
    let result = tokio::spawn(async move { run_process_e2e(fixture).await }).await;
    let cleanup = cleanup_test_schema(&database_url, &initial_database_state).await;
    match (result, cleanup) {
        (Ok(Ok(())), Ok(())) => Ok(()),
        (Ok(Err(error)), Ok(())) => Err(error),
        (Err(error), Ok(())) => Err(test_error(format!("E2E task panicked: {error}"))),
        (Ok(Ok(())), Err(cleanup)) => Err(cleanup),
        (Ok(Err(error)), Err(cleanup)) => Err(test_error(format!(
            "E2E flow failed: {error}; schema cleanup also failed: {cleanup}"
        ))),
        (Err(error), Err(cleanup)) => Err(test_error(format!(
            "E2E task panicked: {error}; schema cleanup also failed: {cleanup}"
        ))),
    }
}

async fn run_process_e2e(mut fixture: Fixture) -> E2eResult {
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");
    prove_migration_lock_serialization(&fixture, binary).await?;
    assert_telemetry_is_hypertable(&fixture.database_url).await?;

    let admin_username = format!("e2e-admin-{}", Uuid::now_v7());
    let admin_password = "E2eTimescaleBootstrapAdmin@2026";
    let mut bootstrap = Command::new(binary);
    fixture.configure(&mut bootstrap);
    bootstrap
        .arg("--bootstrap-admin")
        .env("IOT_NANO_BOOTSTRAP_ADMIN_USERNAME", &admin_username)
        .env("IOT_NANO_BOOTSTRAP_ADMIN_PASSWORD", admin_password);
    let mut bootstrap = spawn_child(
        &mut bootstrap,
        fixture.child_redactions(&[&admin_username, admin_password]),
    )?;
    let bootstrap_exit = bootstrap.child.wait().await?;
    let bootstrap_diagnostic = bootstrap.diagnostic().await?;
    require_success(
        bootstrap_exit.success(),
        "bootstrap admin",
        &bootstrap_diagnostic,
    )?;

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut child = start_monolith(&mut fixture, binary, &client).await?;
    let flow = run_e2e_flow(&fixture, &client, &admin_username, admin_password).await;
    let shutdown = stop_child(&mut child).await;
    let shutdown = match shutdown {
        Ok(()) => assert_all_addresses_rebind(&fixture).await,
        Err(error) => Err(error),
    };
    combine_e2e_outcomes(flow, shutdown)?;

    if !fixture.internal_dir.join("stream.sqlite").is_file()
        || !fixture.internal_dir.join("mqttd.sqlite").is_file()
        || !fixture.internal_dir.join("cache.sqlite").is_file()
    {
        return Err(test_error(
            "monolith did not create all durable internal stores",
        ));
    }
    if fixture.platform_path.exists() {
        return Err(test_error(
            "Timescale fixture unexpectedly created platform.sqlite",
        ));
    }
    Ok(())
}

async fn prove_migration_lock_serialization(fixture: &Fixture, binary: &str) -> E2eResult {
    let mut lock_holder = PgConnection::connect(&fixture.database_url).await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut lock_holder)
        .await?;

    let migration_application_name = format!("iot-nano-e2e-migrate-{}", Uuid::now_v7());
    let migration_database_url =
        migration_database_url(&fixture.database_url, &migration_application_name);
    let mut migrate = Command::new(binary);
    fixture.configure(&mut migrate);
    migrate
        .arg("--migrate-only")
        .env("DATABASE_URL", &migration_database_url)
        .env("PGAPPNAME", &migration_application_name);
    let mut migration = spawn_child(
        &mut migrate,
        fixture.child_redactions(&[&migration_database_url]),
    )?;

    let observation =
        wait_for_migration_lock(&fixture.database_url, &migration_application_name).await;
    let unlock = sqlx::query("SELECT pg_advisory_unlock(hashtext('iot_nano:migrate'))")
        .execute(&mut lock_holder)
        .await;
    observation?;
    unlock?;

    let exit = match timeout(Duration::from_secs(30), migration.child.wait()).await {
        Ok(exit) => exit?,
        Err(_) => {
            let cleanup = force_stop_child(&mut migration).await;
            return combine_outcomes(
                "migration did not finish after advisory-lock release",
                Err(test_error(
                    "migration did not finish within 30 seconds after advisory-lock release",
                )),
                "forced migration cleanup",
                cleanup,
            );
        }
    };
    let diagnostic = migration.diagnostic().await?;
    require_success(
        exit.success(),
        "migration after advisory-lock release",
        &diagnostic,
    )
}

async fn wait_for_migration_lock(database_url: &str, application_name: &str) -> E2eResult {
    let mut observer = PgConnection::connect(database_url).await?;
    for _ in 0..READY_ATTEMPTS {
        let waiting: bool = sqlx::query_scalar(migration_lock_query())
            .bind(application_name)
            .fetch_one(&mut observer)
            .await?;
        if waiting {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(test_error(
        "migration did not block on the iot_nano:migrate advisory lock",
    ))
}

fn migration_lock_query() -> &'static str {
    "SELECT EXISTS (
        SELECT 1
        FROM pg_locks AS waiting
        JOIN pg_stat_activity AS activity ON waiting.pid = activity.pid
        WHERE waiting.locktype = 'advisory'
          AND waiting.classid = 0
          AND waiting.objid::integer = hashtext('iot_nano:migrate')
          AND waiting.granted = FALSE
          AND activity.application_name = $1
          AND activity.datname = current_database()
    )"
}

fn migration_database_url(database_url: &str, application_name: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}application_name={application_name}")
}

async fn assert_telemetry_is_hypertable(database_url: &str) -> E2eResult {
    let mut connection = PgConnection::connect(database_url).await?;
    let hypertable_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM timescaledb_information.hypertables
         WHERE hypertable_schema = 'iot_nano' AND hypertable_name = 'telemetry'",
    )
    .fetch_one(&mut connection)
    .await?;
    if hypertable_count != 1 {
        return Err(test_error(format!(
            "expected iot_nano.telemetry to be a Timescale hypertable, found {hypertable_count}"
        )));
    }
    Ok(())
}

async fn start_monolith(
    fixture: &mut Fixture,
    binary: &str,
    client: &Client,
) -> E2eResult<ManagedChild> {
    for attempt in 1..=START_ATTEMPTS {
        fixture.release_reserved_addresses();
        let mut command = Command::new(binary);
        fixture.configure(&mut command);
        let mut child = spawn_child(&mut command, fixture.child_redactions(&[]))?;

        match wait_ready(client, fixture.public_address, &mut child.child).await {
            Ok(()) => return Ok(child),
            Err(error) => {
                let exited_before_cleanup = child.child.try_wait()?.is_some();
                let cleanup = force_stop_child(&mut child).await;
                let diagnostic = match child.child.try_wait() {
                    Ok(Some(_)) => child.diagnostic().await.unwrap_or_else(|capture_error| {
                        format!("stderr capture failed: {capture_error}")
                    }),
                    Ok(None) => "child remained running after failed forced cleanup".to_owned(),
                    Err(error) => format!("could not inspect child after forced cleanup: {error}"),
                };
                let bind_conflict = exited_before_cleanup && is_bind_conflict(&diagnostic);
                let cleanup_succeeded = cleanup.is_ok();
                let startup = Err(test_error(child_failure_message(
                    "monolith failed before becoming ready",
                    error,
                    &diagnostic,
                )));
                let outcome = combine_outcomes(
                    "monolith startup",
                    startup,
                    "forced startup cleanup",
                    cleanup,
                );
                if bind_conflict && cleanup_succeeded && attempt < START_ATTEMPTS {
                    fixture.rotate_reserved_addresses().await?;
                    continue;
                }
                return outcome.map(|()| unreachable!());
            }
        }
    }
    Err(test_error("monolith did not start"))
}

async fn run_e2e_flow(
    fixture: &Fixture,
    client: &Client,
    admin_username: &str,
    admin_password: &str,
) -> E2eResult {
    let login = client
        .post(format!(
            "http://{}/api/auth/login",
            fixture.management_address
        ))
        .json(&json!({ "username": admin_username, "password": admin_password }))
        .send()
        .await?;
    require_status(&login, StatusCode::OK, "management login")?;
    let cookie = login
        .headers()
        .get(SET_COOKIE)
        .ok_or_else(|| test_error("management login did not issue a session cookie"))?
        .to_str()?
        .split(';')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| test_error("management session cookie was empty"))?
        .to_owned();

    let suffix = Uuid::now_v7().to_string();
    let app_id = format!("e2e-app-{suffix}");
    let client_id = format!("e2e-client-{suffix}");
    let client_secret = format!("E2eClientSecret-{suffix}@2026");
    let device_id = format!("e2e-device-{suffix}");
    let registered = client
        .post(format!(
            "http://{}/api/management/applications",
            fixture.management_address
        ))
        .header(COOKIE, &cookie)
        .json(&json!({
            "app_id": app_id,
            "kind": "full_stack",
            "launch_url": "https://client.example.test",
            "client_id": client_id,
            "redirect_uris": ["https://client.example.test/callback"],
            "allowed_scopes": ["devices:read", "devices:write", "telemetry:read"],
            "enabled": true,
            "client_secret": client_secret
        }))
        .send()
        .await?;
    require_status(&registered, StatusCode::CREATED, "application registration")?;

    let oauth = client
        .post(format!("http://{}/oauth/token", fixture.public_address))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("scope", "devices:read devices:write telemetry:read"),
        ])
        .send()
        .await?;
    require_status(&oauth, StatusCode::OK, "OAuth client credentials")?;
    let token: serde_json::Value = oauth.json().await?;
    let access_token = token["access_token"]
        .as_str()
        .ok_or_else(|| test_error("OAuth response omitted access_token"))?;

    let created = client
        .post(format!("http://{}/api/v1/devices", fixture.public_address))
        .bearer_auth(access_token)
        .json(&json!({
            "device_id": device_id,
            "display_name": "Timescale E2E Device",
            "metadata": { "source": "timescale-e2e" }
        }))
        .send()
        .await?;
    require_status(&created, StatusCode::CREATED, "public device creation")?;

    let devices_response = client
        .get(format!("http://{}/api/v1/devices", fixture.public_address))
        .bearer_auth(access_token)
        .send()
        .await?;
    require_status(&devices_response, StatusCode::OK, "public device listing")?;
    let devices: serde_json::Value = devices_response.json().await?;
    if devices["items"][0]["device_id"] != device_id {
        return Err(test_error(
            "public device listing omitted the created device",
        ));
    }

    let detail_response = client
        .get(format!(
            "http://{}/api/v1/devices/{device_id}",
            fixture.public_address
        ))
        .bearer_auth(access_token)
        .send()
        .await?;
    require_status(&detail_response, StatusCode::OK, "public device detail")?;
    let detail: serde_json::Value = detail_response.json().await?;
    if detail["metadata"]["source"] != "timescale-e2e" {
        return Err(test_error(
            "public device detail lost Timescale E2E metadata",
        ));
    }

    let device_token_response = client
        .post(format!(
            "http://{}/api/management/devices/{device_id}/tokens",
            fixture.management_address
        ))
        .header(COOKIE, &cookie)
        .send()
        .await?;
    require_status(
        &device_token_response,
        StatusCode::CREATED,
        "management MQTT token creation",
    )?;
    let device_token: serde_json::Value = device_token_response.json().await?;
    let device_token = device_token["token"]
        .as_str()
        .ok_or_else(|| test_error("management device-token response omitted plaintext token"))?;

    publish_device_telemetry(fixture.mqtt_tcp_address, &device_id, device_token).await?;
    let telemetry =
        wait_for_telemetry(client, fixture.public_address, access_token, &device_id).await?;
    if telemetry["items"][0]["device_id"] != device_id
        || telemetry["items"][0]["measurements"]["temperature_c"] != 22.5
    {
        return Err(test_error(
            "public telemetry did not contain the MQTT event",
        ));
    }
    assert_durable_stream_record(&fixture.internal_dir, &device_id)?;

    TcpStream::connect(fixture.mqtt_tcp_address).await?;
    TcpStream::connect(fixture.mqtt_tls_address).await?;
    Ok(())
}

async fn wait_for_telemetry(
    client: &Client,
    address: SocketAddr,
    access_token: &str,
    device_id: &str,
) -> E2eResult<serde_json::Value> {
    for _ in 0..READY_ATTEMPTS {
        let response = client
            .get(format!("http://{address}/api/v1/telemetry/{device_id}"))
            .bearer_auth(access_token)
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => {
                let telemetry: serde_json::Value = response.json().await?;
                if telemetry["items"]
                    .as_array()
                    .is_some_and(|items| !items.is_empty())
                {
                    return Ok(telemetry);
                }
            }
            StatusCode::NOT_FOUND => {}
            status => {
                return Err(test_error(format!(
                    "public telemetry request returned unexpected status {status}"
                )));
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(test_error("telemetry did not reach the public API"))
}

fn assert_durable_stream_record(internal_dir: &std::path::Path, device_id: &str) -> E2eResult {
    let stream = rusqlite::Connection::open(internal_dir.join("stream.sqlite"))?;
    let records: i64 = stream.query_row(
        "SELECT COUNT(*) FROM stream_records WHERE payload_json LIKE ?1",
        [format!("%{device_id}%")],
        |row| row.get(0),
    )?;
    if records < 1 {
        return Err(test_error(
            "MQTT telemetry was not durably written to stream.sqlite",
        ));
    }
    Ok(())
}

async fn publish_device_telemetry(address: SocketAddr, device_id: &str, token: &str) -> E2eResult {
    let mut device = TcpStream::connect(address).await?;
    device
        .write_all(&v311_connect(device_id, "iotd_device_token", token))
        .await?;
    let connected = read_mqtt_packet(&mut device).await?;
    if connected != [0x20, 0x02, 0x00, 0x00] {
        return Err(test_error(format!(
            "MQTT 3.1.1 CONNECT did not receive CONNACK: {connected:?}"
        )));
    }
    device
        .write_all(&v311_qos_one_publish(
            "v1/devices/me/telemetry",
            &telemetry_payload(),
            7,
        ))
        .await?;
    let puback = read_mqtt_packet(&mut device).await?;
    if puback != [0x40, 0x02, 0x00, 0x07] {
        return Err(test_error(format!(
            "MQTT QoS 1 telemetry did not receive PUBACK: {puback:?}"
        )));
    }
    Ok(())
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

async fn read_mqtt_packet(stream: &mut TcpStream) -> E2eResult<Vec<u8>> {
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

async fn wait_ready(client: &Client, address: SocketAddr, child: &mut Child) -> E2eResult {
    for _ in 0..READY_ATTEMPTS {
        if client
            .get(format!("http://{address}/readyz"))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK)
        {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(test_error(format!(
                "monolith exited before becoming ready with {status}"
            )));
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(test_error("monolith did not become ready"))
}

fn is_bind_conflict(diagnostic: &str) -> bool {
    diagnostic
        .to_ascii_lowercase()
        .contains("address already in use")
}

async fn stop_child(child: &mut ManagedChild) -> E2eResult {
    if let Some(status) = child.child.try_wait()? {
        let diagnostic = child.diagnostic().await?;
        return require_success(
            status.success(),
            "monolith exited unexpectedly",
            &diagnostic,
        );
    }

    let pid = child
        .child
        .id()
        .ok_or_else(|| test_error("monolith process has no PID"))?;
    let signal = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await;
    match signal {
        Ok(status) if status.success() => {}
        Ok(status) => {
            return combine_outcomes(
                "graceful shutdown signal",
                Err(test_error(format!("SIGTERM exited with {status}"))),
                "forced shutdown cleanup",
                force_stop_child(child).await,
            );
        }
        Err(error) => {
            return combine_outcomes(
                "graceful shutdown signal",
                Err(error.into()),
                "forced shutdown cleanup",
                force_stop_child(child).await,
            );
        }
    }

    let exit = match timeout(Duration::from_secs(15), child.child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            return combine_outcomes(
                "graceful shutdown wait",
                Err(error.into()),
                "forced shutdown cleanup",
                force_stop_child(child).await,
            );
        }
        Err(_) => {
            return combine_outcomes(
                "graceful shutdown wait",
                Err(test_error(
                    "monolith did not exit within 15 seconds of SIGTERM",
                )),
                "forced shutdown cleanup",
                force_stop_child(child).await,
            );
        }
    };
    let diagnostic = child.diagnostic().await?;
    require_success(exit.success(), "monolith graceful shutdown", &diagnostic)
}

async fn force_stop_child(child: &mut ManagedChild) -> E2eResult {
    if child.child.try_wait()?.is_some() {
        return Ok(());
    }
    child.child.start_kill()?;
    match timeout(Duration::from_secs(5), child.child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(test_error(
            "forced child kill did not complete within 5 seconds",
        )),
    }
}

async fn assert_all_addresses_rebind(fixture: &Fixture) -> E2eResult {
    let mut failures = Vec::new();
    for address in [
        fixture.public_address,
        fixture.management_address,
        fixture.mqtt_tcp_address,
        fixture.mqtt_tls_address,
    ] {
        match TcpListener::bind(address).await {
            Ok(listener) => drop(listener),
            Err(error) => failures.push(format!("{address}: {error}")),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(test_error(format!(
            "monolith graceful shutdown did not release all addresses: {}",
            failures.join("; ")
        )))
    }
}

async fn reserve_address() -> E2eResult<(SocketAddr, TcpListener)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok((listener.local_addr()?, listener))
}

async fn assert_pristine_test_database(database_url: &str) -> E2eResult<InitialDatabaseState> {
    let mut connection = PgConnection::connect(database_url).await?;
    let schema_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'iot_nano')")
            .fetch_one(&mut connection)
            .await?;
    let object_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM pg_class AS object
         JOIN pg_namespace AS schema ON schema.oid = object.relnamespace
         WHERE schema.nspname = 'iot_nano'",
    )
    .fetch_one(&mut connection)
    .await?;
    if schema_exists || object_count != 0 {
        return Err(test_error(
            "refusing Timescale E2E URL: iot_nano must be absent and contain no objects before mutation",
        ));
    }
    let had_timescaledb: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')",
    )
    .fetch_one(&mut connection)
    .await?;
    let had_uuid_ossp: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp')",
    )
    .fetch_one(&mut connection)
    .await?;
    Ok(InitialDatabaseState {
        had_timescaledb,
        had_uuid_ossp,
    })
}

async fn cleanup_test_schema(
    database_url: &str,
    initial_database_state: &InitialDatabaseState,
) -> E2eResult {
    let mut connection = PgConnection::connect(database_url).await?;
    let mut transaction = connection.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut *transaction)
        .await?;
    if !initial_database_state.had_uuid_ossp {
        sqlx::query("DROP EXTENSION IF EXISTS \"uuid-ossp\"")
            .execute(&mut *transaction)
            .await?;
    }
    if !initial_database_state.had_timescaledb {
        sqlx::query("DROP EXTENSION IF EXISTS timescaledb")
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn require_status(response: &Response, expected: StatusCode, context: &str) -> E2eResult {
    if response.status() == expected {
        Ok(())
    } else {
        Err(test_error(format!(
            "{context} returned {}, expected {expected}",
            response.status()
        )))
    }
}

fn require_success(success: bool, context: &str, diagnostic: &str) -> E2eResult {
    if success {
        Ok(())
    } else if diagnostic.is_empty() {
        Err(test_error(format!("{context} failed")))
    } else {
        Err(test_error(format!("{context} failed: {diagnostic}")))
    }
}

fn child_failure_message(
    context: &str,
    error: Box<dyn Error + Send + Sync>,
    diagnostic: &str,
) -> String {
    if diagnostic.is_empty() {
        format!("{context}: {error}")
    } else {
        format!("{context}: {error}; child stderr: {diagnostic}")
    }
}

fn combine_e2e_outcomes(flow: E2eResult, shutdown: E2eResult) -> E2eResult {
    combine_outcomes("E2E flow", flow, "shutdown", shutdown)
}

fn combine_outcomes(
    primary_context: &str,
    primary: E2eResult,
    secondary_context: &str,
    secondary: E2eResult,
) -> E2eResult {
    match (primary, secondary) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(secondary)) => Err(test_error(format!(
            "{primary_context} failed: {primary}; {secondary_context} also failed: {secondary}"
        ))),
    }
}

fn spawn_child(command: &mut Command, redactions: Vec<String>) -> E2eResult<ManagedChild> {
    let mut child = command
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| test_error("child stderr pipe was unavailable"))?;
    Ok(ManagedChild {
        child,
        stderr: Some(tokio::spawn(collect_redacted_stderr(stderr, redactions))),
    })
}

impl ManagedChild {
    async fn diagnostic(&mut self) -> E2eResult<String> {
        let task = self
            .stderr
            .take()
            .ok_or_else(|| test_error("child stderr was already collected"))?;
        task.await
            .map_err(|error| test_error(format!("child stderr capture task failed: {error}")))?
            .map_err(Into::into)
    }
}

async fn collect_redacted_stderr(
    mut stderr: ChildStderr,
    redactions: Vec<String>,
) -> io::Result<String> {
    let mut redactor = ChildDiagnosticRedactor::new(redactions.iter().map(String::as_str));
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stderr.read(&mut chunk).await?;
        if read == 0 {
            return Ok(redactor.finish());
        }
        redactor.push(&chunk[..read]);
    }
}

struct ChildDiagnosticRedactor {
    redactions: Vec<Vec<u8>>,
    pending: Vec<u8>,
    diagnostic: String,
    truncated: bool,
}

impl ChildDiagnosticRedactor {
    fn new<'a>(redactions: impl IntoIterator<Item = &'a str>) -> Self {
        let mut redactions: Vec<Vec<u8>> = redactions
            .into_iter()
            .filter(|value| !value.is_empty())
            .map(|value| value.as_bytes().to_vec())
            .collect();
        redactions.sort_by_key(|value| std::cmp::Reverse(value.len()));
        Self {
            redactions,
            pending: Vec::new(),
            diagnostic: String::new(),
            truncated: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
        self.drain(false);
    }

    fn finish(mut self) -> String {
        self.drain(true);
        if self.truncated {
            self.diagnostic.push_str(TRUNCATED_DIAGNOSTIC_SUFFIX);
        }
        self.diagnostic
    }

    fn drain(&mut self, final_chunk: bool) {
        let longest_redaction = self
            .redactions
            .first()
            .map_or(1, |value| value.len().max(1));
        let safe_end = if final_chunk {
            self.pending.len()
        } else {
            self.pending
                .len()
                .saturating_sub(longest_redaction.saturating_sub(1))
        };
        let mut index = 0;
        while index < safe_end {
            if let Some(redaction_length) = self
                .redactions
                .iter()
                .find(|value| self.pending[index..].starts_with(value))
                .map(Vec::len)
            {
                self.append(b"[REDACTED]");
                index += redaction_length;
                continue;
            }
            if final_chunk
                && self.redactions.iter().any(|value| {
                    self.pending[index..].len() < value.len()
                        && value.starts_with(&self.pending[index..])
                })
            {
                self.append(b"[REDACTED]");
                index = self.pending.len();
                break;
            }

            let start = index;
            index += 1;
            while index < safe_end
                && !self
                    .redactions
                    .iter()
                    .any(|value| self.pending[index..].starts_with(value))
            {
                index += 1;
            }
            let bytes = self.pending[start..index].to_vec();
            self.append(&bytes);
        }
        self.pending.drain(..index);
    }

    fn append(&mut self, bytes: &[u8]) {
        let limit = MAX_CHILD_DIAGNOSTIC_BYTES - TRUNCATED_DIAGNOSTIC_SUFFIX.len();
        if self.diagnostic.len() >= limit {
            self.truncated = true;
            return;
        }
        let text = String::from_utf8_lossy(bytes);
        let remaining = limit - self.diagnostic.len();
        if text.len() <= remaining {
            self.diagnostic.push_str(&text);
        } else {
            let mut end = remaining;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.diagnostic.push_str(&text[..end]);
            self.truncated = true;
        }
    }
}

fn redact_child_diagnostic(stderr: &[u8], was_truncated: bool, redactions: &[&str]) -> String {
    let mut redactor = ChildDiagnosticRedactor::new(redactions.iter().copied());
    redactor.push(stderr);
    redactor.truncated |= was_truncated;
    redactor.finish()
}

fn test_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    io::Error::other(message.into()).into()
}

fn validated_test_database_url(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("IOT_NANO_TIMESCALE_TEST_URL must be explicitly declared".to_owned());
    }
    if test_database_name(value).is_none() {
        return Err(format!(
            "IOT_NANO_TIMESCALE_TEST_URL must name a disposable {TEST_DATABASE_PREFIX}<uuid> database"
        ));
    }
    Ok(value.to_owned())
}

fn test_database_name(database_url: &str) -> Option<String> {
    let options = PgConnectOptions::from_str(database_url).ok()?;
    let database = options.get_database()?;
    database
        .strip_prefix(TEST_DATABASE_PREFIX)
        .filter(|suffix| Uuid::parse_str(suffix).is_ok())
        .map(|_| database.to_owned())
}

#[test]
fn timescale_test_url_must_name_an_isolated_test_database() {
    assert_eq!(
        test_database_name(
            "postgres://iot:secret@localhost/iot_nano_test_018f4e40-5d2c-7d19-9d6f-6f996de6f722"
        ),
        Some("iot_nano_test_018f4e40-5d2c-7d19-9d6f-6f996de6f722".to_owned())
    );
    assert_eq!(
        test_database_name("postgres://iot:secret@localhost/iot"),
        None
    );
    assert_eq!(
        test_database_name("postgres://iot:secret@localhost/iot_nano_test_"),
        None
    );
}

#[test]
fn timescale_test_url_requires_an_explicit_opt_in_value() {
    assert!(validated_test_database_url("").is_err());
}

#[test]
fn child_diagnostics_are_bounded_and_redact_child_secrets() {
    let database_url = "postgres://iot:database-password@localhost/iot_nano_test_018f4e40-5d2c-7d19-9d6f-6f996de6f722";
    let vault_key = "e2e-timescale-device-token-vault-key-material-0001";
    let bootstrap_password = "E2eTimescaleBootstrapAdmin@2026";
    let stderr = format!(
        "{database_url} {vault_key} {bootstrap_password}{}",
        "x".repeat(MAX_CHILD_DIAGNOSTIC_BYTES - 2)
    );

    let diagnostic = redact_child_diagnostic(
        stderr.as_bytes(),
        false,
        &[database_url, vault_key, bootstrap_password],
    );

    assert!(diagnostic.len() <= MAX_CHILD_DIAGNOSTIC_BYTES);
    assert!(diagnostic.contains("[REDACTED]"));
    assert!(!diagnostic.contains(database_url));
    assert!(!diagnostic.contains(vault_key));
    assert!(!diagnostic.contains(bootstrap_password));
}

#[test]
fn combined_e2e_outcomes_preserve_flow_and_shutdown_failures() {
    let error = combine_e2e_outcomes(
        Err(test_error("flow failure")),
        Err(test_error("shutdown failure")),
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("flow failure"));
    assert!(error.contains("shutdown failure"));
}

#[test]
fn migration_lock_query_identifies_the_marked_child_on_the_target_database() {
    let query = migration_lock_query();

    assert!(query.contains("pg_stat_activity"));
    assert!(query.contains("waiting.pid = activity.pid"));
    assert!(query.contains("activity.application_name = $1"));
    assert!(query.contains("activity.datname = current_database()"));
}

#[test]
fn migration_database_url_overrides_an_existing_application_name() {
    assert_eq!(
        migration_database_url(
            "postgres://iot:secret@localhost/iot_nano_test_018f4e40-5d2c-7d19-9d6f-6f996de6f722?application_name=unrelated&sslmode=require",
            "iot-nano-e2e-migrate-marker",
        ),
        "postgres://iot:secret@localhost/iot_nano_test_018f4e40-5d2c-7d19-9d6f-6f996de6f722?application_name=unrelated&sslmode=require&application_name=iot-nano-e2e-migrate-marker"
    );
}

#[test]
fn only_unambiguous_bind_conflicts_are_retried() {
    assert!(is_bind_conflict(
        "listener bind failed: Address already in use"
    ));
    assert!(!is_bind_conflict("listener bind failed: permission denied"));
    assert!(!is_bind_conflict("database connection failed"));
}
