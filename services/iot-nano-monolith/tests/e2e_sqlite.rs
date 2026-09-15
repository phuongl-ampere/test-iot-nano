use std::{net::SocketAddr, path::PathBuf, process::Stdio, time::Duration};

use reqwest::{
    Client, StatusCode,
    header::{COOKIE, SET_COOKIE},
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{
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
            "allowed_scopes": ["devices:read", "devices:write"],
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

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}
