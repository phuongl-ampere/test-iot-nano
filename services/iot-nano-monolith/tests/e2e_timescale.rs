use std::{env, net::SocketAddr, path::PathBuf, process::Stdio, str::FromStr, time::Duration};

use reqwest::{
    Client, StatusCode,
    header::{COOKIE, SET_COOKIE},
};
use serde_json::json;
use sqlx::postgres::PgConnectOptions;
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    time::{sleep, timeout},
};
use uuid::Uuid;

const TEST_DATABASE_PREFIX: &str = "iot_nano_test_";

struct Fixture {
    _directory: TempDir,
    database_url: String,
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
    async fn from_environment() -> Option<Self> {
        let database_url = env::var("IOT_NANO_TIMESCALE_TEST_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        assert!(
            test_database_name(&database_url).is_some(),
            "IOT_NANO_TIMESCALE_TEST_URL must name a disposable {TEST_DATABASE_PREFIX}<uuid> database"
        );

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let tls_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        Some(Self {
            platform_path: root.join("platform.sqlite"),
            internal_dir: root.join("internal"),
            public_address: reserve_address().await,
            management_address: reserve_address().await,
            mqtt_tcp_address: reserve_address().await,
            mqtt_tls_address: reserve_address().await,
            tls_cert_path: tls_fixtures.join("server.crt"),
            tls_key_path: tls_fixtures.join("server.key"),
            database_url,
            _directory: directory,
        })
    }

    fn configure(&self, command: &mut Command) {
        command
            .env_clear()
            .env("IOT_NANO_STORAGE", "timescale")
            .env("DATABASE_URL", &self.database_url)
            .env("IOT_NANO_INTERNAL_DIR", &self.internal_dir)
            .env("IOT_NANO_TLS_CERT_PATH", &self.tls_cert_path)
            .env("IOT_NANO_TLS_KEY_PATH", &self.tls_key_path)
            .env(
                "IOT_DEVICE_TOKEN_VAULT_KEY",
                "e2e-timescale-device-token-vault-key-material-0001",
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
async fn timescale_monolith_process_runs_when_a_disposable_test_url_is_declared() {
    let Some(fixture) = Fixture::from_environment().await else {
        return;
    };
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");

    let mut migrate = Command::new(binary);
    fixture.configure(&mut migrate);
    let migration = migrate.arg("--migrate-only").output().await.unwrap();
    assert!(migration.status.success(), "{migration:?}");

    let admin_username = format!("e2e-admin-{}", Uuid::now_v7());
    let admin_password = "E2eTimescaleBootstrapAdmin@2026";
    let mut bootstrap = Command::new(binary);
    fixture.configure(&mut bootstrap);
    let output = bootstrap
        .arg("--bootstrap-admin")
        .env("IOT_NANO_BOOTSTRAP_ADMIN_USERNAME", &admin_username)
        .env("IOT_NANO_BOOTSTRAP_ADMIN_PASSWORD", admin_password)
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
    let result = run_e2e_flow(&fixture, &mut child, &admin_username, admin_password).await;
    if child.try_wait().unwrap().is_none() {
        stop_child(&mut child).await.unwrap();
    }
    result.unwrap();

    assert!(fixture.internal_dir.join("stream.sqlite").is_file());
    assert!(fixture.internal_dir.join("mqttd.sqlite").is_file());
    assert!(fixture.internal_dir.join("cache.sqlite").is_file());
    assert!(!fixture.platform_path.exists());
}

async fn run_e2e_flow(
    fixture: &Fixture,
    child: &mut Child,
    admin_username: &str,
    admin_password: &str,
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
        .json(&json!({ "username": admin_username, "password": admin_password }))
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
            "allowed_scopes": ["devices:read", "devices:write"],
            "enabled": true,
            "client_secret": client_secret
        }))
        .send()
        .await?;
    assert_eq!(registered.status(), StatusCode::CREATED);

    let token: serde_json::Value = client
        .post(format!("http://{}/oauth/token", fixture.public_address))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("scope", "devices:read devices:write"),
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
            "device_id": device_id,
            "display_name": "Timescale E2E Device",
            "metadata": { "source": "timescale-e2e" }
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
    assert_eq!(devices["items"][0]["device_id"], device_id);

    let detail: serde_json::Value = client
        .get(format!(
            "http://{}/api/v1/devices/{device_id}",
            fixture.public_address
        ))
        .bearer_auth(access_token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(detail["metadata"]["source"], "timescale-e2e");

    TcpStream::connect(fixture.mqtt_tcp_address).await?;
    TcpStream::connect(fixture.mqtt_tls_address).await?;

    stop_child(child).await?;
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
    for _ in 0..200 {
        if client
            .get(format!("http://{address}/readyz"))
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

async fn stop_child(child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
    let pid = child.id().expect("monolith process has no PID");
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    assert!(status.success());
    let exit = timeout(Duration::from_secs(15), child.wait()).await??;
    assert!(exit.success(), "monolith exited with {exit}");
    Ok(())
}

fn test_database_name(database_url: &str) -> Option<String> {
    let options = PgConnectOptions::from_str(database_url).ok()?;
    let database = options.get_database()?;
    database
        .strip_prefix(TEST_DATABASE_PREFIX)
        .filter(|suffix| Uuid::parse_str(suffix).is_ok())
        .map(|_| database.to_owned())
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
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
