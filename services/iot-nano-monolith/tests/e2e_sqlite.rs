use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
    thread::Builder,
    time::Duration,
};

use reqwest::{
    Client, StatusCode,
    header::{COOKIE, LOCATION, SET_COOKIE},
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
    http_address: SocketAddr,
    mqtt_tcp_address: SocketAddr,
    mqtt_tls_address: SocketAddr,
    tls_cert_path: PathBuf,
    tls_key_path: PathBuf,
}

struct ManagedChild {
    child: Option<Child>,
    process_group: rustix::process::Pid,
    reaper: Option<SyncSender<Child>>,
    reaped: Arc<AtomicBool>,
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
            http_address: reserve_address().await,
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
}

#[tokio::test]
#[ignore = "release-only: starts a SQLite monolith process"]
async fn sqlite_monolith_process_runs_management_oauth_public_api_and_graceful_shutdown() {
    let fixture = Fixture::new().await;
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");
    let mut bootstrap = Command::new(binary);
    fixture.configure(&mut bootstrap);
    let output = bootstrap
        .arg("--bootstrap-system")
        .env("IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME", "e2e-system")
        .env(
            "IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD",
            "E2eBootstrapSystem@2026",
        )
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let mut command = Command::new(binary);
    fixture.configure(&mut command);
    let mut child = spawn_child(&mut command).unwrap();
    let result = run_e2e_flow(&fixture, &mut child).await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "release-only: starts a SQLite monolith process"]
async fn sqlite_monolith_device_pairing_claim_and_share_require_recipient_acceptance() {
    run_device_pairing_claim_and_share_e2e().await.unwrap();
}

#[tokio::test]
#[ignore = "release-only: starts a SQLite monolith process"]
async fn sqlite_monolith_tenant_capabilities_gate_user_resource_creation() {
    run_user_capability_e2e().await.unwrap();
}

#[tokio::test]
#[ignore = "release-only: starts a SQLite monolith process"]
async fn sqlite_monolith_telemetry_opens_acknowledges_and_archives_alerts() {
    run_alert_lifecycle_e2e().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn managed_child_drop_during_task_panic_kills_and_reaps_its_process_group() {
    let directory = tempfile::tempdir().unwrap();
    let descendant_pid_path = directory.path().join("descendant.pid");
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg("sleep 30 & echo $! > \"$1\"; wait")
        .arg("shutdown-test")
        .arg(&descendant_pid_path);
    let child = spawn_child(&mut command).unwrap();
    let reaped = Arc::clone(&child.reaped);
    let descendant_pid = wait_for_pid(&descendant_pid_path).await;

    let task = tokio::spawn(async move {
        let _child = child;
        panic!("panic the managed child fixture");
    });
    assert!(task.await.unwrap_err().is_panic());

    wait_until_process_is_gone(descendant_pid).await;
    wait_until_reaped(reaped).await;
}

#[cfg(unix)]
#[tokio::test]
async fn managed_child_drop_during_task_error_kills_and_reaps_its_process_group() {
    let directory = tempfile::tempdir().unwrap();
    let descendant_pid_path = directory.path().join("descendant.pid");
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg("sleep 30 & echo $! > \"$1\"; wait")
        .arg("shutdown-test")
        .arg(&descendant_pid_path);
    let child = spawn_child(&mut command).unwrap();
    let reaped = Arc::clone(&child.reaped);
    let descendant_pid = wait_for_pid(&descendant_pid_path).await;

    let task = tokio::spawn(async move {
        let _child = child;
        Err::<(), _>("return an error from the managed child fixture")
    });
    assert_eq!(
        task.await.unwrap(),
        Err("return an error from the managed child fixture")
    );

    wait_until_process_is_gone(descendant_pid).await;
    wait_until_reaped(reaped).await;
}

async fn run_e2e_flow(
    fixture: &Fixture,
    child: &mut ManagedChild,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    wait_ready(&client, fixture.http_address).await?;

    let system_login = client
        .post(format!(
            "http://{}/api/v1/system/auth/login",
            fixture.http_address
        ))
        .json(&json!({ "username": "e2e-system", "password": "E2eBootstrapSystem@2026" }))
        .send()
        .await?;
    assert_eq!(system_login.status(), StatusCode::OK);
    let system_cookie = system_login
        .headers()
        .get(SET_COOKIE)
        .expect("system login did not issue a session cookie")
        .to_str()?
        .split(';')
        .next()
        .expect("session cookie was empty")
        .to_owned();

    let tenant = client
        .post(format!(
            "http://{}/api/v1/system/tenants",
            fixture.http_address
        ))
        .header(COOKIE, &system_cookie)
        .json(&json!({
            "slug": "e2e-tenant",
            "metadata": { "source": "sqlite-e2e" },
            "tenant_account_password": "E2eTenantAccount@2026"
        }))
        .send()
        .await?;
    assert_eq!(tenant.status(), StatusCode::CREATED);

    let tenant_login = client
        .post(format!(
            "http://{}/api/v1/tenant/auth/login",
            fixture.http_address
        ))
        .json(&json!({
            "tenant_slug": "e2e-tenant",
            "password": "E2eTenantAccount@2026"
        }))
        .send()
        .await?;
    assert_eq!(tenant_login.status(), StatusCode::OK);
    let tenant_cookie = tenant_login
        .headers()
        .get(SET_COOKIE)
        .expect("tenant login did not issue a session cookie")
        .to_str()?
        .split(';')
        .next()
        .expect("tenant session cookie was empty")
        .to_owned();

    let registered = client
        .post(format!(
            "http://{}/api/v1/management/applications",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
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

    let user = client
        .post(format!(
            "http://{}/api/v1/management/users",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
        .json(&json!({
            "username": "e2e-user",
            "password": "E2eUserPassword@2026"
        }))
        .send()
        .await?;
    assert_eq!(user.status(), StatusCode::CREATED);

    let capabilities = client
        .put(format!(
            "http://{}/api/v1/management/users/e2e-user/capabilities",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
        .json(&json!({
            "capabilities": ["create_devices"]
        }))
        .send()
        .await?;
    assert_eq!(capabilities.status(), StatusCode::OK);

    let user_login = client
        .post(format!(
            "http://{}/api/v1/user/auth/login",
            fixture.http_address
        ))
        .json(&json!({
            "tenant_slug": "e2e-tenant",
            "username": "e2e-user",
            "password": "E2eUserPassword@2026"
        }))
        .send()
        .await?;
    assert_eq!(user_login.status(), StatusCode::OK);
    let user_cookie = user_login
        .headers()
        .get(SET_COOKIE)
        .expect("user login did not issue a session cookie")
        .to_str()?
        .split(';')
        .next()
        .expect("user session cookie was empty")
        .to_owned();

    let application_token: serde_json::Value = client
        .post(format!("http://{}/oauth/token", fixture.http_address))
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
    let application_access_token = application_token["access_token"]
        .as_str()
        .expect("client credentials response omitted an access token");
    assert_eq!(
        client
            .get(format!("http://{}/api/v1/devices", fixture.http_address))
            .bearer_auth(application_access_token)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );

    let verifier = "e2e-sqlite-pkce-verifier-with-at-least-forty-three-characters";
    let authorize = client
        .get(format!("http://{}/oauth/authorize", fixture.http_address))
        .query(&[
            ("response_type", "code"),
            ("client_id", "e2e-client"),
            ("redirect_uri", "https://client.example.test/callback"),
            ("scope", "devices:read devices:write telemetry:read"),
            ("state", "e2e-state"),
            (
                "code_challenge",
                "_I6nwrptyTxPi7QlVmOQ-wn6M_zoyYwmqc67KorJAwI",
            ),
            ("code_challenge_method", "S256"),
        ])
        .header(COOKIE, &user_cookie)
        .send()
        .await?;
    assert_eq!(authorize.status(), StatusCode::FOUND);
    let authorization_redirect = authorize
        .headers()
        .get(LOCATION)
        .expect("authorization endpoint did not redirect to the registered callback")
        .to_str()?;
    let authorization_code = authorization_redirect
        .split('?')
        .nth(1)
        .and_then(|query| {
            query
                .split('&')
                .find_map(|parameter| parameter.strip_prefix("code="))
        })
        .expect("authorization redirect omitted the authorization code");

    let token = client
        .post(format!("http://{}/oauth/token", fixture.http_address))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", authorization_code),
            ("redirect_uri", "https://client.example.test/callback"),
            ("client_id", "e2e-client"),
            ("client_secret", "E2eClientSecret@2026"),
            ("code_verifier", verifier),
        ])
        .send()
        .await?;
    assert_eq!(token.status(), StatusCode::OK);
    let token: serde_json::Value = token.json().await?;
    let access_token = token["access_token"]
        .as_str()
        .expect("authorization-code response omitted an access token");

    let created = client
        .post(format!("http://{}/api/v1/devices", fixture.http_address))
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
        .get(format!("http://{}/api/v1/devices", fixture.http_address))
        .bearer_auth(access_token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(devices["items"][0]["device_id"], "e2e-device");

    let detail: serde_json::Value = client
        .get(format!(
            "http://{}/api/v1/devices/e2e-device",
            fixture.http_address
        ))
        .bearer_auth(access_token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(detail["metadata"]["source"], "e2e");

    let device_token: serde_json::Value = client
        .post(format!(
            "http://{}/api/v1/management/devices/e2e-device/tokens",
            fixture.http_address
        ))
        .header(COOKIE, &tenant_cookie)
        .send()
        .await?
        .json()
        .await?;
    let device_token = device_token["token"]
        .as_str()
        .expect("management device token response omitted the plaintext token");

    publish_device_telemetry(fixture.mqtt_tcp_address, "e2e-device", device_token).await?;
    let telemetry = wait_for_telemetry(&client, fixture.http_address, access_token).await?;
    assert_eq!(telemetry["items"][0]["device_id"], "e2e-device");
    assert_eq!(telemetry["items"][0]["measurements"]["temperature_c"], 22.5);
    let stream = rusqlite::Connection::open(fixture.internal_dir.join("stream.sqlite"))?;
    let records: i64 =
        stream.query_row("SELECT COUNT(*) FROM stream_records", [], |row| row.get(0))?;
    assert_eq!(records, 1);

    TcpStream::connect(fixture.mqtt_tcp_address).await?;
    TcpStream::connect(fixture.mqtt_tls_address).await?;

    child.signal(rustix::process::Signal::TERM)?;
    let exit = timeout(Duration::from_secs(15), child.wait()).await??;
    assert!(exit.success(), "monolith exited with {exit}");
    for address in [
        fixture.http_address,
        fixture.http_address,
        fixture.mqtt_tcp_address,
        fixture.mqtt_tls_address,
    ] {
        let listener = TcpListener::bind(address).await?;
        drop(listener);
    }
    Ok(())
}

struct E2eSession {
    fixture: Fixture,
    client: Client,
    child: ManagedChild,
    tenant_cookie: String,
}

impl E2eSession {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let fixture = Fixture::new().await;
        let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");
        let mut bootstrap = Command::new(binary);
        fixture.configure(&mut bootstrap);
        let output = bootstrap
            .arg("--bootstrap-system")
            .env("IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME", "e2e-system")
            .env(
                "IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD",
                "E2eBootstrapSystem@2026",
            )
            .output()
            .await?;
        assert!(output.status.success(), "{output:?}");

        let mut command = Command::new(binary);
        fixture.configure(&mut command);
        command.stderr(Stdio::inherit());
        let child = spawn_child(&mut command)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        wait_ready(&client, fixture.http_address).await?;

        let system_login = client
            .post(format!(
                "http://{}/api/v1/system/auth/login",
                fixture.http_address
            ))
            .json(&json!({
                "username": "e2e-system",
                "password": "E2eBootstrapSystem@2026"
            }))
            .send()
            .await?;
        assert_eq!(system_login.status(), StatusCode::OK);
        let system_cookie = session_cookie(&system_login, "system login");

        let tenant = client
            .post(format!(
                "http://{}/api/v1/system/tenants",
                fixture.http_address
            ))
            .header(COOKIE, system_cookie)
            .json(&json!({
                "slug": "e2e-tenant",
                "metadata": { "source": "sqlite-e2e" },
                "tenant_account_password": "E2eTenantAccount@2026"
            }))
            .send()
            .await?;
        assert_eq!(tenant.status(), StatusCode::CREATED);

        let tenant_login = client
            .post(format!(
                "http://{}/api/v1/tenant/auth/login",
                fixture.http_address
            ))
            .json(&json!({
                "tenant_slug": "e2e-tenant",
                "password": "E2eTenantAccount@2026"
            }))
            .send()
            .await?;
        assert_eq!(tenant_login.status(), StatusCode::OK);

        Ok(Self {
            fixture,
            client,
            child,
            tenant_cookie: session_cookie(&tenant_login, "tenant login"),
        })
    }

    async fn stop(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.child.signal(rustix::process::Signal::TERM)?;
        let exit = timeout(Duration::from_secs(15), self.child.wait()).await??;
        if !exit.success() {
            return Err(io::Error::other(format!("monolith exited with {exit}")).into());
        }
        for address in [
            self.fixture.http_address,
            self.fixture.http_address,
            self.fixture.mqtt_tcp_address,
            self.fixture.mqtt_tls_address,
        ] {
            let listener = TcpListener::bind(address).await?;
            drop(listener);
        }
        Ok(())
    }
}

async fn run_device_pairing_claim_and_share_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let session = E2eSession::start().await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        create_management_user(&session, "owner", "OwnerPassword@2026").await?;
        create_management_user(&session, "recipient", "RecipientPassword@2026").await?;
        enable_claim_policy(&session).await?;

        let device_id = provision_device(&session, "Pairing Device").await?;
        let token = create_device_token(&session, &device_id).await?;
        let code =
            request_pairing_code(session.fixture.mqtt_tcp_address, &device_id, &token).await?;

        let owner_cookie = user_cookie(&session, "owner", "OwnerPassword@2026").await?;
        let claim = session
            .client
            .post(format!(
                "http://{}/app/devices/claim",
                session.fixture.http_address
            ))
            .header(COOKIE, &owner_cookie)
            .form(&[("device_id", device_id.as_str()), ("code", code.as_str())])
            .send()
            .await?;
        assert_eq!(claim.status(), StatusCode::SEE_OTHER);
        assert_eq!(claim.headers()[LOCATION], "/app?notice=device-claimed");

        let invite = session
            .client
            .post(format!(
                "http://{}/app/devices/{device_id}/permissions",
                session.fixture.http_address
            ))
            .header(COOKIE, &owner_cookie)
            .form(&[("username", "recipient"), ("permission", "view")])
            .send()
            .await?;
        assert_eq!(invite.status(), StatusCode::SEE_OTHER);

        let recipient_cookie = user_cookie(&session, "recipient", "RecipientPassword@2026").await?;
        let workspace_before = session
            .client
            .get(format!("http://{}/app", session.fixture.http_address))
            .header(COOKIE, &recipient_cookie)
            .send()
            .await?
            .text()
            .await?;
        assert!(!workspace_before.contains("Pairing Device"));
        assert!(workspace_before.contains("Invitations (1)"));

        let invitation_page = session
            .client
            .get(format!(
                "http://{}/app/invitations",
                session.fixture.http_address
            ))
            .header(COOKIE, &recipient_cookie)
            .send()
            .await?
            .text()
            .await?;
        let invitation_id = invitation_page
            .split("/app/invitations/")
            .nth(1)
            .and_then(|value| value.split('/').next())
            .expect("invitation page omitted the accept route");

        let accepted = session
            .client
            .post(format!(
                "http://{}/app/invitations/{invitation_id}/accept",
                session.fixture.http_address
            ))
            .header(COOKIE, &recipient_cookie)
            .send()
            .await?;
        assert_eq!(accepted.status(), StatusCode::SEE_OTHER);

        let workspace_after = session
            .client
            .get(format!("http://{}/app", session.fixture.http_address))
            .header(COOKIE, &recipient_cookie)
            .send()
            .await?
            .text()
            .await?;
        assert!(workspace_after.contains("Pairing Device"));
        Ok(())
    }
    .await;
    let shutdown = session.stop().await;
    result?;
    shutdown
}

async fn run_user_capability_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let session = E2eSession::start().await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        create_management_user(&session, "contributor", "ContributorPassword@2026").await?;
        let contributor_cookie =
            user_cookie(&session, "contributor", "ContributorPassword@2026").await?;

        let denied_device = session
            .client
            .post(format!(
                "http://{}/app/devices",
                session.fixture.http_address
            ))
            .header(COOKIE, &contributor_cookie)
            .form(&[("display_name", "Denied Device")])
            .send()
            .await?;
        assert_eq!(denied_device.status(), StatusCode::FORBIDDEN);

        let default_asset = session
            .client
            .post(format!(
                "http://{}/app/assets",
                session.fixture.http_address
            ))
            .header(COOKIE, &contributor_cookie)
            .form(&[("name", "Default Asset")])
            .send()
            .await?;
        assert_eq!(default_asset.status(), StatusCode::SEE_OTHER);

        let capabilities = session
            .client
            .put(format!(
                "http://{}/api/v1/management/users/contributor/capabilities",
                session.fixture.http_address
            ))
            .header(COOKIE, &session.tenant_cookie)
            .json(&json!({
                "capabilities": [
                    "claim_devices",
                    "create_assets",
                    "create_devices",
                    "control_devices",
                    "edit_resources",
                    "share_owned_resources"
                ]
            }))
            .send()
            .await?;
        assert_eq!(capabilities.status(), StatusCode::OK);

        let created_device = session
            .client
            .post(format!(
                "http://{}/app/devices",
                session.fixture.http_address
            ))
            .header(COOKIE, &contributor_cookie)
            .form(&[("display_name", "Allowed Device")])
            .send()
            .await?;
        assert_eq!(created_device.status(), StatusCode::SEE_OTHER);
        let detail = created_device
            .headers()
            .get(LOCATION)
            .expect("created device response omitted its detail URL")
            .to_str()?;
        let device_detail = session
            .client
            .get(format!(
                "http://{}{}",
                session.fixture.http_address, detail
            ))
            .header(COOKIE, &contributor_cookie)
            .send()
            .await?
            .text()
            .await?;
        assert!(device_detail.contains("Allowed Device"));
        Ok(())
    }
    .await;
    let shutdown = session.stop().await;
    result?;
    shutdown
}

async fn run_alert_lifecycle_e2e() -> Result<(), Box<dyn std::error::Error>> {
    let session = E2eSession::start().await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let device_id = provision_device(&session, "Alert Device").await?;
        let token = create_device_token(&session, &device_id).await?;
        let rule = session
            .client
            .post(format!(
                "http://{}/api/v1/management/alert-rules",
                session.fixture.http_address
            ))
            .header(COOKIE, &session.tenant_cookie)
            .json(&json!({
                "name": "High Power",
                "enabled": true,
                "device_id": device_id,
                "metric_key": "power_w",
                "rule_type": "event_threshold",
                "comparison": "gt",
                "threshold": 500.0,
                "severity": "warning"
            }))
            .send()
            .await?;
        assert_eq!(rule.status(), StatusCode::CREATED);
        let rule: serde_json::Value = rule.json().await?;
        let rule_id = rule["id"].as_str().expect("alert rule omitted its ID");

        publish_device_measurements(
            session.fixture.mqtt_tcp_address,
            &device_id,
            &token,
            json!({ "power_w": 750.0 }),
        )
        .await?;
        let incident = wait_for_alert_incident(&session, rule_id).await?;
        let incident_id = incident["id"]
            .as_str()
            .expect("alert incident omitted its ID");
        assert_eq!(incident["status"], "open");
        assert_eq!(incident["last_value"], 750.0);

        let acknowledged = session
            .client
            .post(format!(
                "http://{}/api/v1/management/alert-incidents/{incident_id}/acknowledge",
                session.fixture.http_address
            ))
            .header(COOKIE, &session.tenant_cookie)
            .send()
            .await?;
        assert_eq!(acknowledged.status(), StatusCode::OK);
        let acknowledged: serde_json::Value = acknowledged.json().await?;
        assert!(acknowledged["acknowledged_at"].is_string());

        let archived = session
            .client
            .post(format!(
                "http://{}/api/v1/management/alert-rules/{rule_id}/archive",
                session.fixture.http_address
            ))
            .header(COOKIE, &session.tenant_cookie)
            .send()
            .await?;
        assert_eq!(archived.status(), StatusCode::NO_CONTENT);
        Ok(())
    }
    .await;
    let shutdown = session.stop().await;
    result?;
    shutdown
}

fn session_cookie(response: &reqwest::Response, context: &str) -> String {
    response
        .headers()
        .get(SET_COOKIE)
        .unwrap_or_else(|| panic!("{context} did not issue a session cookie"))
        .to_str()
        .expect("session cookie was not valid ASCII")
        .split(';')
        .next()
        .expect("session cookie was empty")
        .to_owned()
}

async fn create_management_user(
    session: &E2eSession,
    username: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = session
        .client
        .post(format!(
            "http://{}/api/v1/management/users",
            session.fixture.http_address
        ))
        .header(COOKIE, &session.tenant_cookie)
        .json(&json!({ "username": username, "password": password }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

async fn user_cookie(
    session: &E2eSession,
    username: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = session
        .client
        .post(format!(
            "http://{}/api/v1/user/auth/login",
            session.fixture.http_address
        ))
        .json(&json!({
            "tenant_slug": "e2e-tenant",
            "username": username,
            "password": password
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(session_cookie(&response, "user login"))
}

async fn enable_claim_policy(session: &E2eSession) -> Result<(), Box<dyn std::error::Error>> {
    let response = session
        .client
        .post(format!(
            "http://{}/tenant/devices/claim-policy",
            session.fixture.http_address
        ))
        .header(COOKIE, &session.tenant_cookie)
        .form(&[
            ("enabled", "on"),
            ("ttl_seconds", "900"),
            ("code_length", "12"),
            ("max_failed_attempts", "5"),
            ("request_cooldown_seconds", "10"),
        ])
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    Ok(())
}

async fn provision_device(
    session: &E2eSession,
    display_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = session
        .client
        .post(format!(
            "http://{}/api/v1/management/devices",
            session.fixture.http_address
        ))
        .header(COOKIE, &session.tenant_cookie)
        .json(&json!({ "display_name": display_name }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let device: serde_json::Value = response.json().await?;
    Ok(device["device_id"]
        .as_str()
        .expect("provisioned device omitted its ID")
        .to_owned())
}

async fn create_device_token(
    session: &E2eSession,
    device_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = session
        .client
        .post(format!(
            "http://{}/api/v1/management/devices/{device_id}/tokens",
            session.fixture.http_address
        ))
        .header(COOKIE, &session.tenant_cookie)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let token: serde_json::Value = response.json().await?;
    Ok(token["token"]
        .as_str()
        .expect("device-token response omitted plaintext token")
        .to_owned())
}

async fn request_pairing_code(
    address: SocketAddr,
    device_id: &str,
    token: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut device = TcpStream::connect(address).await?;
    device
        .write_all(&v311_connect(device_id, "iotd_device_token", token))
        .await?;
    assert_eq!(
        read_mqtt_packet(&mut device).await?,
        vec![0x20, 0x02, 0x00, 0x00]
    );
    device
        .write_all(&v311_subscribe("v1/devices/me/pairing/response/+", 11))
        .await?;
    assert_eq!(
        read_mqtt_packet(&mut device).await?,
        vec![0x90, 0x03, 0x00, 0x0b, 0x01]
    );

    let request_id = uuid::Uuid::now_v7();
    device
        .write_all(&v311_qos_one_publish(
            "v1/devices/me/pairing/request",
            json!({ "request_id": request_id }).to_string().as_bytes(),
            12,
        ))
        .await?;
    assert_eq!(
        read_mqtt_packet(&mut device).await?,
        vec![0x40, 0x02, 0x00, 0x0c]
    );
    let response = read_mqtt_packet(&mut device).await?;
    assert_eq!(
        mqtt_publish_topic(&response),
        format!("v1/devices/me/pairing/response/{request_id}")
    );
    device
        .write_all(&v311_puback(mqtt_publish_packet_id(&response)))
        .await?;
    let response: serde_json::Value = serde_json::from_slice(&mqtt_publish_payload(&response))?;
    assert_eq!(response["status"], "issued");
    assert_eq!(response["device_id"], device_id);
    Ok(response["code"]
        .as_str()
        .expect("pairing response omitted the raw claim code")
        .to_owned())
}

async fn publish_device_measurements(
    address: SocketAddr,
    device_id: &str,
    token: &str,
    measurements: serde_json::Value,
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
            measurements.to_string().as_bytes(),
            13,
        ))
        .await?;
    assert_eq!(
        read_mqtt_packet(&mut device).await?,
        vec![0x40, 0x02, 0x00, 0x0d]
    );
    Ok(())
}

async fn wait_for_alert_incident(
    session: &E2eSession,
    rule_id: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    for _ in 0..120 {
        let response = session
            .client
            .get(format!(
                "http://{}/api/v1/management/alert-incidents",
                session.fixture.http_address
            ))
            .header(COOKIE, &session.tenant_cookie)
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let incidents: Vec<serde_json::Value> = response.json().await?;
        if let Some(incident) = incidents
            .into_iter()
            .find(|incident| incident["rule_id"].as_str() == Some(rule_id))
        {
            return Ok(incident);
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err("alert worker did not create an incident".into())
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
        "temperature_c": 22.5
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
    String::from_utf8(packet[body + 2..body + 2 + topic_length].to_vec())
        .expect("MQTT publish topic was not valid UTF-8")
}

fn mqtt_publish_packet_id(packet: &[u8]) -> u16 {
    let body = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    let packet_id_start = body + 2 + topic_length;
    u16::from_be_bytes([packet[packet_id_start], packet[packet_id_start + 1]])
}

fn mqtt_publish_payload(packet: &[u8]) -> Vec<u8> {
    let body = mqtt_packet_body_offset(packet);
    let topic_length = usize::from(u16::from_be_bytes([packet[body], packet[body + 1]]));
    let packet_id_length = if packet[0] & 0x06 == 0 { 0 } else { 2 };
    packet[body + 2 + topic_length + packet_id_length..].to_vec()
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

fn spawn_child(command: &mut Command) -> io::Result<ManagedChild> {
    command.process_group(0);
    let mut child = command
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .as_std_mut()
        .spawn()?;
    let Some(process_group) = rustix::process::Pid::from_raw(child.id() as i32) else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::Error::other("spawned child has no process group"));
    };
    let reaped = Arc::new(AtomicBool::new(false));
    let reaper = match spawn_child_reaper(Arc::clone(&reaped)) {
        Ok(reaper) => reaper,
        Err(error) => {
            let _ =
                rustix::process::kill_process_group(process_group, rustix::process::Signal::KILL);
            let _ = child.wait();
            return Err(error);
        }
    };
    Ok(ManagedChild {
        child: Some(child),
        process_group,
        reaper: Some(reaper),
        reaped,
    })
}

impl ManagedChild {
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
