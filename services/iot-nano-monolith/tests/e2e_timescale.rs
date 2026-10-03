use std::{
    env,
    error::Error,
    io::{self, Read},
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Stdio},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use chrono::Utc;
use reqwest::{
    Client, Response, StatusCode,
    header::{COOKIE, LOCATION, SET_COOKIE},
};
use serde_json::json;
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
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
    child: Option<Child>,
    stderr: Option<JoinHandle<io::Result<String>>>,
    process_group: rustix::process::Pid,
    reaper: Option<SyncSender<Child>>,
    reaped: Arc<AtomicBool>,
}

struct FlowCredentials {
    system_username: String,
    system_password: String,
    tenant_slug: String,
    tenant_account_password: String,
    client_id: String,
    client_secret: String,
}

impl FlowCredentials {
    fn generate() -> Self {
        let suffix = Uuid::now_v7();
        Self {
            system_username: format!("e2e-system-{suffix}"),
            system_password: format!("E2eTimescaleBootstrapSystem-{suffix}@2026"),
            tenant_slug: format!("e2e-tenant-{suffix}"),
            tenant_account_password: format!("E2eTimescaleTenantAccount-{suffix}@2026"),
            client_id: format!("e2e-client-{suffix}"),
            client_secret: format!("E2eClientSecret-{suffix}@2026"),
        }
    }

    fn child_redactions(&self) -> [&str; 3] {
        [
            &self.system_password,
            &self.tenant_account_password,
            &self.client_secret,
        ]
    }
}

struct Fixture {
    _directory: TempDir,
    database_url: String,
    platform_path: PathBuf,
    internal_dir: PathBuf,
    http_address: SocketAddr,
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
        let (http_address, http_listener) = reserve_address().await?;
        let (mqtt_tcp_address, mqtt_tcp_listener) = reserve_address().await?;
        let (mqtt_tls_address, mqtt_tls_listener) = reserve_address().await?;

        Ok(Self {
            _directory: directory,
            database_url,
            platform_path: root.join("platform.sqlite"),
            internal_dir: root.join("internal"),
            http_address,
            mqtt_tcp_address,
            mqtt_tls_address,
            reserved_listeners: Some(vec![
                http_listener,
                mqtt_tcp_listener,
                mqtt_tls_listener,
            ]),
            tls_cert_path: tls_fixtures.join("server.crt"),
            tls_key_path: tls_fixtures.join("server.key"),
        })
    }

    async fn rotate_reserved_addresses(&mut self) -> E2eResult {
        let (http_address, http_listener) = reserve_address().await?;
        let (mqtt_tcp_address, mqtt_tcp_listener) = reserve_address().await?;
        let (mqtt_tls_address, mqtt_tls_listener) = reserve_address().await?;
        self.http_address = http_address;
        self.mqtt_tcp_address = mqtt_tcp_address;
        self.mqtt_tls_address = mqtt_tls_address;
        self.reserved_listeners = Some(vec![
            http_listener,
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
            .env("IOT_NANO_HTTP_ADDRESS", self.http_address.to_string())
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

    let credentials = FlowCredentials::generate();
    let mut bootstrap = Command::new(binary);
    fixture.configure(&mut bootstrap);
    bootstrap
        .arg("--bootstrap-system")
        .env(
            "IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME",
            &credentials.system_username,
        )
        .env(
            "IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD",
            &credentials.system_password,
        );
    let mut bootstrap = spawn_child(
        &mut bootstrap,
        fixture.child_redactions(&credentials.child_redactions()),
    )
    .await?;
    wait_for_child_success(&mut bootstrap, "bootstrap admin").await?;

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut child = start_monolith(&mut fixture, binary, &client, &credentials).await?;
    let flow = run_e2e_flow(&fixture, &client, &credentials).await;
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
    )
    .await?;

    let observation =
        wait_for_migration_lock(&fixture.database_url, &migration_application_name).await;
    let unlock = sqlx::query("SELECT pg_advisory_unlock(hashtext('iot_nano:migrate'))")
        .execute(&mut lock_holder)
        .await;
    let lock_phase = combine_outcomes(
        "migration lock observation",
        observation,
        "migration advisory unlock",
        unlock.map(|_| ()).map_err(Into::into),
    );
    match lock_phase {
        Ok(()) => {
            wait_for_child_success_with_timeout(
                &mut migration,
                "migration after advisory-lock release",
                Duration::from_secs(30),
            )
            .await
        }
        Err(error) => {
            force_stop_after_failure(&mut migration, "migration lock phase", Err(error)).await
        }
    }
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
          AND ((waiting.classid::integer::bigint << 32) + waiting.objid::bigint)
              = hashtext('iot_nano:migrate')::bigint
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
    credentials: &FlowCredentials,
) -> E2eResult<ManagedChild> {
    for attempt in 1..=START_ATTEMPTS {
        fixture.release_reserved_addresses();
        let mut command = Command::new(binary);
        fixture.configure(&mut command);
        let mut child = spawn_child(
            &mut command,
            fixture.child_redactions(&credentials.child_redactions()),
        )
        .await?;

        match wait_ready(client, fixture.http_address, child.child_mut()?).await {
            Ok(()) => return Ok(child),
            Err(error) => {
                let exited_before_cleanup = matches!(child.try_wait(), Ok(Some(_)));
                let cleanup = force_stop_child(&mut child).await;
                let diagnostic = match child.try_wait() {
                    Ok(Some(_)) => child.diagnostic().await.unwrap_or_else(|capture_error| {
                        format!("stderr capture failed: {capture_error}")
                    }),
                    Ok(None) => "child remained running after failed forced cleanup".to_owned(),
                    Err(error) => format!("could not inspect child after forced cleanup: {error}"),
                };
                let bind_conflict = exited_before_cleanup && is_bind_conflict(&diagnostic);
                let cleanup_succeeded = cleanup.is_ok();
                let outcome = combine_failure_with_cleanup(
                    "monolith startup",
                    Err(test_error(error.to_string())),
                    cleanup,
                )
                .map_err(|cleanup_error| {
                    test_error(child_failure_message(
                        "monolith failed before becoming ready",
                        cleanup_error,
                        &diagnostic,
                    ))
                });
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
    credentials: &FlowCredentials,
) -> E2eResult {
    let system_login = client
        .post(format!(
            "http://{}/api/v1/system/auth/login",
            fixture.http_address
        ))
        .json(&json!({
            "username": credentials.system_username,
            "password": credentials.system_password
        }))
        .send()
        .await?;
    require_status(&system_login, StatusCode::OK, "system login")?;
    let system_cookie = system_login
        .headers()
        .get(SET_COOKIE)
        .ok_or_else(|| test_error("system login did not issue a session cookie"))?
        .to_str()?
        .split(';')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| test_error("system session cookie was empty"))?
        .to_owned();

    let tenant = client
        .post(format!(
            "http://{}/api/v1/system/tenants",
            fixture.http_address
        ))
        .header(COOKIE, &system_cookie)
        .json(&json!({
            "slug": credentials.tenant_slug,
            "metadata": { "source": "timescale-e2e" },
            "tenant_account_password": credentials.tenant_account_password,
        }))
        .send()
        .await?;
    require_status(&tenant, StatusCode::CREATED, "tenant creation")?;

    let tenant_login = client
        .post(format!(
            "http://{}/api/v1/tenant/auth/login",
            fixture.http_address
        ))
        .json(&json!({
            "tenant_slug": credentials.tenant_slug,
            "password": credentials.tenant_account_password,
        }))
        .send()
        .await?;
    require_status(&tenant_login, StatusCode::OK, "tenant login")?;
    let tenant_cookie = tenant_login
        .headers()
        .get(SET_COOKIE)
        .ok_or_else(|| test_error("tenant login did not issue a session cookie"))?
        .to_str()?
        .split(';')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| test_error("tenant session cookie was empty"))?
        .to_owned();

    let suffix = Uuid::now_v7().to_string();
    let app_id = format!("e2e-app-{suffix}");
    let device_id = format!("e2e-device-{suffix}");
    let username = format!("e2e-user-{suffix}");
    let password = format!("E2eTimescaleUser-{suffix}@2026");
    let registered = client
        .post(format!(
            "http://{}/api/v1/management/applications",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
        .json(&json!({
            "app_id": app_id,
            "kind": "full_stack",
            "launch_url": "https://client.example.test",
            "client_id": credentials.client_id,
            "redirect_uris": ["https://client.example.test/callback"],
            "allowed_scopes": ["devices:read", "devices:write", "telemetry:read"],
            "enabled": true,
            "client_secret": credentials.client_secret
        }))
        .send()
        .await?;
    require_status(&registered, StatusCode::CREATED, "application registration")?;

    let user = client
        .post(format!(
            "http://{}/api/v1/management/users",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
        .json(&json!({
            "username": username,
            "password": password
        }))
        .send()
        .await?;
    require_status(&user, StatusCode::CREATED, "tenant user creation")?;

    let user_login = client
        .post(format!(
            "http://{}/api/v1/user/auth/login",
            fixture.http_address
        ))
        .json(&json!({
            "tenant_slug": credentials.tenant_slug,
            "username": username,
            "password": password,
        }))
        .send()
        .await?;
    require_status(&user_login, StatusCode::OK, "tenant user login")?;
    let user_cookie = user_login
        .headers()
        .get(SET_COOKIE)
        .ok_or_else(|| test_error("tenant user login did not issue a session cookie"))?
        .to_str()?
        .split(';')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| test_error("tenant user session cookie was empty"))?
        .to_owned();

    let application_oauth = client
        .post(format!("http://{}/oauth/token", fixture.http_address))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", credentials.client_id.as_str()),
            ("client_secret", credentials.client_secret.as_str()),
            ("scope", "devices:read devices:write telemetry:read"),
        ])
        .send()
        .await?;
    require_status(
        &application_oauth,
        StatusCode::OK,
        "OAuth client credentials",
    )?;
    let application_token: serde_json::Value = application_oauth.json().await?;
    let application_access_token = application_token["access_token"]
        .as_str()
        .ok_or_else(|| test_error("client credentials response omitted access_token"))?;

    let application_devices = client
        .get(format!("http://{}/api/v1/devices", fixture.http_address))
        .bearer_auth(application_access_token)
        .send()
        .await?;
    require_status(
        &application_devices,
        StatusCode::FORBIDDEN,
        "application-only public device listing denial",
    )?;

    let verifier = "e2e-timescale-pkce-verifier-with-at-least-forty-three-characters";
    let authorize = client
        .get(format!("http://{}/oauth/authorize", fixture.http_address))
        .query(&[
            ("response_type", "code"),
            ("client_id", credentials.client_id.as_str()),
            ("redirect_uri", "https://client.example.test/callback"),
            ("scope", "devices:read devices:write telemetry:read"),
            ("state", "e2e-state"),
            (
                "code_challenge",
                "7fDBxwoy5XU0TMbGAkXF93CrHStWpR-noQW7sVSQ5fc",
            ),
            ("code_challenge_method", "S256"),
        ])
        .header(COOKIE, &user_cookie)
        .send()
        .await?;
    require_status(
        &authorize,
        StatusCode::FOUND,
        "OAuth authorization redirect",
    )?;
    let authorization_redirect = authorize
        .headers()
        .get(LOCATION)
        .ok_or_else(|| test_error("authorization endpoint did not redirect to callback"))?
        .to_str()?;
    let authorization_code = authorization_redirect
        .split('?')
        .nth(1)
        .and_then(|query| {
            query
                .split('&')
                .find_map(|parameter| parameter.strip_prefix("code="))
        })
        .ok_or_else(|| test_error("authorization redirect omitted authorization code"))?;

    let oauth = client
        .post(format!("http://{}/oauth/token", fixture.http_address))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", authorization_code),
            ("redirect_uri", "https://client.example.test/callback"),
            ("client_id", credentials.client_id.as_str()),
            ("client_secret", credentials.client_secret.as_str()),
            ("code_verifier", verifier),
        ])
        .send()
        .await?;
    require_status(&oauth, StatusCode::OK, "OAuth authorization code")?;
    let token: serde_json::Value = oauth.json().await?;
    let access_token = token["access_token"]
        .as_str()
        .ok_or_else(|| test_error("authorization-code response omitted access_token"))?;

    let created = client
        .post(format!("http://{}/api/v1/devices", fixture.http_address))
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
        .get(format!("http://{}/api/v1/devices", fixture.http_address))
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
            fixture.http_address
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
            "http://{}/api/v1/management/devices/{device_id}/tokens",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
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
        wait_for_telemetry(client, fixture.http_address, access_token, &device_id).await?;
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

async fn wait_for_child_success(child: &mut ManagedChild, context: &str) -> E2eResult {
    let exit = match child.wait().await {
        Ok(exit) => exit,
        Err(error) => {
            return force_stop_after_failure(child, context, Err(error.into())).await;
        }
    };
    let diagnostic = child.diagnostic().await?;
    require_success(exit.success(), context, &diagnostic)
}

async fn wait_for_child_success_with_timeout(
    child: &mut ManagedChild,
    context: &str,
    wait_timeout: Duration,
) -> E2eResult {
    let exit = match timeout(wait_timeout, child.wait()).await {
        Ok(Ok(exit)) => exit,
        Ok(Err(error)) => {
            return force_stop_after_failure(child, context, Err(error.into())).await;
        }
        Err(_) => {
            return force_stop_after_failure(
                child,
                context,
                Err(test_error(format!(
                    "{context} did not finish within {} seconds",
                    wait_timeout.as_secs()
                ))),
            )
            .await;
        }
    };
    let diagnostic = child.diagnostic().await?;
    require_success(exit.success(), context, &diagnostic)
}

async fn stop_child(child: &mut ManagedChild) -> E2eResult {
    match child.try_wait() {
        Ok(Some(status)) => {
            let diagnostic = child.diagnostic().await?;
            return require_success(
                status.success(),
                "monolith exited unexpectedly",
                &diagnostic,
            );
        }
        Ok(None) => {}
        Err(error) => {
            return force_stop_after_failure(
                child,
                "monolith shutdown status inspection",
                Err(error.into()),
            )
            .await;
        }
    }

    let pid = match child.id() {
        Some(pid) => pid,
        None => {
            return force_stop_after_failure(
                child,
                "monolith shutdown PID lookup",
                Err(test_error("monolith process has no PID")),
            )
            .await;
        }
    };
    let signal = child.signal(rustix::process::Signal::TERM);
    match signal {
        Ok(()) => {}
        Err(error) => {
            return force_stop_after_failure(
                child,
                "graceful shutdown signal",
                Err(test_error(format!("SIGTERM failed for {pid}: {error}"))),
            )
            .await;
        }
    }

    let exit = match timeout(Duration::from_secs(15), child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            return force_stop_after_failure(child, "graceful shutdown wait", Err(error.into()))
                .await;
        }
        Err(_) => {
            return force_stop_after_failure(
                child,
                "graceful shutdown wait",
                Err(test_error(
                    "monolith did not exit within 15 seconds of SIGTERM",
                )),
            )
            .await;
        }
    };
    let diagnostic = child.diagnostic().await?;
    require_success(exit.success(), "monolith graceful shutdown", &diagnostic)
}

async fn force_stop_child(child: &mut ManagedChild) -> E2eResult {
    kill_and_wait_for_child(child).await
}

async fn kill_and_wait_for_child(child: &mut ManagedChild) -> E2eResult {
    let kill = child
        .signal(rustix::process::Signal::KILL)
        .or_else(ignore_missing_process_group);
    let wait = match timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(test_error(
            "forced child kill did not complete within 5 seconds",
        )),
    };
    match (kill, wait) {
        (_, Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(error),
        (Err(kill), Err(wait)) => combine_outcomes(
            "forced child kill",
            Err(kill.into()),
            "forced child wait",
            Err(wait),
        ),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn managed_child_drop_during_task_abort_kills_its_process_group() {
    let directory = tempfile::tempdir().unwrap();
    let descendant_pid_path = directory.path().join("descendant.pid");
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg("sleep 30 & echo $! > \"$1\"; wait")
        .arg("shutdown-test")
        .arg(&descendant_pid_path);
    let child = spawn_child(&mut command, Vec::new()).await.unwrap();
    let reaped = Arc::clone(&child.reaped);
    let descendant_pid = wait_for_pid(&descendant_pid_path).await;

    let task = tokio::spawn(async move {
        let _child = child;
        panic!("abort the managed child fixture");
    });
    assert!(task.await.unwrap_err().is_panic());

    wait_until_process_is_gone(descendant_pid).await;
    wait_until_reaped(reaped).await;
}

async fn assert_all_addresses_rebind(fixture: &Fixture) -> E2eResult {
    let mut failures = Vec::new();
    for address in [
        fixture.http_address,
        fixture.http_address,
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

#[cfg(unix)]
async fn wait_for_pid(path: &std::path::Path) -> u32 {
    timeout(Duration::from_secs(1), async {
        loop {
            if let Some(pid) = std::fs::read_to_string(path)
                .ok()
                .and_then(|value| value.trim().parse().ok())
            {
                return pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("process-group descendant did not report its PID")
}

#[cfg(unix)]
async fn wait_until_process_is_gone(pid: u32) {
    let pid = rustix::process::Pid::from_raw(pid as i32).unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            if rustix::process::test_kill_process(pid).is_err() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("process-group descendant remained alive after fixture cleanup");
}

#[cfg(unix)]
async fn wait_until_reaped(reaped: Arc<AtomicBool>) {
    timeout(Duration::from_secs(1), async {
        while !reaped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed child remained unreaped after fixture cleanup");
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

async fn force_stop_after_failure(
    child: &mut ManagedChild,
    primary_context: &str,
    primary: E2eResult,
) -> E2eResult {
    combine_failure_with_cleanup(primary_context, primary, force_stop_child(child).await)
}

fn combine_failure_with_cleanup(
    primary_context: &str,
    primary: E2eResult,
    cleanup: E2eResult,
) -> E2eResult {
    combine_outcomes(primary_context, primary, "forced child cleanup", cleanup)
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

async fn spawn_child(command: &mut Command, redactions: Vec<String>) -> E2eResult<ManagedChild> {
    let reaped = Arc::new(AtomicBool::new(false));
    let reaper = spawn_child_reaper(Arc::clone(&reaped))?;
    command.process_group(0);
    let mut child = command
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .as_std_mut()
        .spawn()?;
    let process_group = match rustix::process::Pid::from_raw(child.id() as i32) {
        Some(process_group) => process_group,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(test_error("spawned child has no process group"));
        }
    };
    let mut managed = ManagedChild {
        child: Some(child),
        stderr: None,
        process_group,
        reaper: Some(reaper),
        reaped,
    };
    let stderr = match managed.child_mut()?.stderr.take() {
        Some(stderr) => stderr,
        None => {
            return force_stop_after_failure(
                &mut managed,
                "child stderr setup",
                Err(test_error("child stderr pipe was unavailable")),
            )
            .await
            .map(|()| unreachable!());
        }
    };
    match Builder::new()
        .name("iot-nano-e2e-stderr".to_owned())
        .spawn(move || collect_redacted_stderr(stderr, redactions))
    {
        Ok(stderr) => {
            managed.stderr = Some(stderr);
            Ok(managed)
        }
        Err(error) => force_stop_after_failure(
            &mut managed,
            "child stderr capture setup",
            Err(error.into()),
        )
        .await
        .map(|()| unreachable!()),
    }
}

impl ManagedChild {
    fn child_mut(&mut self) -> E2eResult<&mut Child> {
        self.child
            .as_mut()
            .ok_or_else(|| test_error("child was handed to its reaper"))
    }

    fn id(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        let status = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("child was handed to its reaper"))?
            .try_wait()?;
        if status.is_some() {
            self.reaped.store(true, Ordering::Release);
        }
        Ok(status)
    }

    async fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn signal(&self, signal: rustix::process::Signal) -> rustix::io::Result<()> {
        rustix::process::kill_process_group(self.process_group, signal)
    }

    async fn diagnostic(&mut self) -> E2eResult<String> {
        let task = self
            .stderr
            .take()
            .ok_or_else(|| test_error("child stderr was already collected"))?;
        task.join()
            .map_err(|_| test_error("child stderr capture thread panicked"))?
            .map_err(Into::into)
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let _ = self
            .signal(rustix::process::Signal::KILL)
            .or_else(ignore_missing_process_group);
        let Some(child) = self.child.take() else {
            return;
        };
        let Some(reaper) = self.reaper.take() else {
            return;
        };
        if let Err(error) = reaper.send(child) {
            let mut child = error.0;
            if child.wait().is_ok() {
                self.reaped.store(true, Ordering::Release);
            }
        }
    }
}

fn spawn_child_reaper(reaped: Arc<AtomicBool>) -> io::Result<SyncSender<Child>> {
    let (sender, receiver) = sync_channel::<Child>(1);
    // Provision before spawning so Drop can always transfer the direct child to a synchronous wait.
    Builder::new()
        .name("iot-nano-e2e-reaper".to_owned())
        .spawn(move || {
            if let Ok(mut child) = receiver.recv() {
                if child.wait().is_ok() {
                    reaped.store(true, Ordering::Release);
                }
            }
        })?;
    Ok(sender)
}

fn ignore_missing_process_group(error: rustix::io::Errno) -> rustix::io::Result<()> {
    if error == rustix::io::Errno::SRCH {
        Ok(())
    } else {
        Err(error)
    }
}

fn collect_redacted_stderr(
    mut stderr: std::process::ChildStderr,
    redactions: Vec<String>,
) -> io::Result<String> {
    let mut redactor = ChildDiagnosticRedactor::new(redactions.iter().map(String::as_str));
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stderr.read(&mut chunk)?;
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
        test_database_name("postgres://iot:secret@localhost/iot_nano_test_local"),
        None
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

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL naming an unused disposable test database"]
async fn migration_lock_query_detects_waiting_signed_hashtext_lock() -> E2eResult {
    let database_url =
        validated_test_database_url(&env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap_or_default())
            .map_err(test_error)?;
    let mut lock_holder = PgConnection::connect(&database_url).await?;
    let hash: i32 = sqlx::query_scalar("SELECT hashtext('iot_nano:migrate')")
        .fetch_one(&mut lock_holder)
        .await?;
    if hash >= 0 {
        return Err(test_error(format!(
            "expected hashtext('iot_nano:migrate') to be negative, found {hash}"
        )));
    }
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut lock_holder)
        .await?;

    let application_name = format!("iot-nano-e2e-lock-{}", Uuid::now_v7());
    let waiting_database_url = migration_database_url(&database_url, &application_name);
    let mut waiter = PgConnection::connect(&waiting_database_url).await?;
    let waiter = tokio::spawn(async move {
        sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:migrate'))")
            .execute(&mut waiter)
            .await
            .map(|_| ())
    });

    let observation = wait_for_migration_lock(&database_url, &application_name).await;
    let unlock = sqlx::query("SELECT pg_advisory_unlock(hashtext('iot_nano:migrate'))")
        .execute(&mut lock_holder)
        .await
        .map(|_| ())
        .map_err(Into::into);
    let waiter = match timeout(Duration::from_secs(30), waiter).await {
        Ok(Ok(result)) => result.map_err(Into::into),
        Ok(Err(error)) => Err(test_error(format!("waiting lock task failed: {error}"))),
        Err(error) => Err(error.into()),
    };

    combine_outcomes(
        "migration lock observation",
        observation,
        "advisory lock cleanup",
        combine_outcomes("advisory unlock", unlock, "waiting lock completion", waiter),
    )
}

#[test]
fn child_diagnostics_are_bounded_and_redact_child_secrets() {
    let database_url = "postgres://iot:database-password@localhost/iot_nano_test_018f4e40-5d2c-7d19-9d6f-6f996de6f722";
    let vault_key = "e2e-timescale-device-token-vault-key-material-0001";
    let bootstrap_password = "E2eTimescaleBootstrapSystem@2026";
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
fn monolith_child_redactor_covers_generated_flow_secrets() {
    let credentials = FlowCredentials::generate();
    let stderr = format!(
        "bootstrap password={} client secret={}",
        credentials.system_password, credentials.client_secret
    );
    let redactions = credentials.child_redactions();

    let diagnostic = redact_child_diagnostic(stderr.as_bytes(), false, &redactions);

    assert!(diagnostic.contains("[REDACTED]"));
    assert!(!diagnostic.contains(&credentials.system_password));
    assert!(!diagnostic.contains(&credentials.client_secret));
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
fn migration_lock_observation_failure_preserves_forced_cleanup_failure() {
    let error = combine_failure_with_cleanup(
        "migration lock observation",
        Err(test_error("lock observation failure")),
        Err(test_error("forced cleanup failure")),
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("lock observation failure"));
    assert!(error.contains("forced cleanup failure"));
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
