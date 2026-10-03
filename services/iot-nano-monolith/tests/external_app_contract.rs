use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs,
    future::Future,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    time::Duration,
};

use reqwest::{
    Client, Response, StatusCode,
    header::{COOKIE, LOCATION, SET_COOKIE},
};
use serde_json::json;
use tempfile::TempDir;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    process::{Child, ChildStderr, Command},
    task::JoinHandle,
    time::sleep,
};

const SYSTEM_USERNAME: &str = "external-contract-system";
const SYSTEM_PASSWORD: &str = "ExternalContractSystem@2026";
const TENANT_SLUG: &str = "external-contract-tenant";
const TENANT_ACCOUNT_PASSWORD: &str = "ExternalContractTenantAccount@2026";
const USERNAME: &str = "external-contract-user";
const USER_PASSWORD: &str = "ExternalContractUser@2026";
const CLIENT_SECRET: &str = "ExternalContractClientSecret@2026";
const INVALID_CLIENT_SECRET: &str = "InvalidExternalContractClientSecret@2026";
const SESSION_SECRET: &str = "external-contract-session-secret-material-0001";
const START_ATTEMPTS: usize = 5;

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
    platform_path: PathBuf,
    internal_dir: PathBuf,
    http_address: SocketAddr,
    mqtt_tcp_address: SocketAddr,
    mqtt_tls_address: SocketAddr,
    reserved_monolith_addresses: Option<Vec<TcpListener>>,
    tls_cert_path: PathBuf,
    tls_key_path: PathBuf,
}

impl Fixture {
    async fn new() -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let tls_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        let (addresses, reserved_monolith_addresses) = reserve_monolith_addresses().await?;
        Ok(Self {
            root: root.clone(),
            platform_path: root.join("platform.sqlite"),
            internal_dir: root.join("internal"),
            http_address: addresses[0],
            mqtt_tcp_address: addresses[1],
            mqtt_tls_address: addresses[2],
            reserved_monolith_addresses: Some(reserved_monolith_addresses),
            tls_cert_path: tls_fixtures.join("server.crt"),
            tls_key_path: tls_fixtures.join("server.key"),
            _directory: directory,
        })
    }

    fn configure_monolith(&self, command: &mut Command) {
        command
            .env_clear()
            .env("IOT_NANO_STORAGE", "sqlite")
            .env("IOT_NANO_SQLITE_PATH", &self.platform_path)
            .env("IOT_NANO_INTERNAL_DIR", &self.internal_dir)
            .env("IOT_NANO_TLS_CERT_PATH", &self.tls_cert_path)
            .env("IOT_NANO_TLS_KEY_PATH", &self.tls_key_path)
            .env(
                "IOT_DEVICE_TOKEN_VAULT_KEY",
                "external-contract-device-token-vault-key-material-0001",
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

    fn public_url(&self) -> String {
        format!("http://{}", self.http_address)
    }

    fn release_monolith_addresses(&mut self) {
        self.reserved_monolith_addresses.take();
    }

    async fn rebind_monolith_addresses(&mut self) -> Result<(), Box<dyn Error>> {
        let (addresses, listeners) = reserve_monolith_addresses().await?;
        self.http_address = addresses[0];
        self.mqtt_tcp_address = addresses[1];
        self.mqtt_tls_address = addresses[2];
        self.reserved_monolith_addresses = Some(listeners);
        Ok(())
    }
}

struct CapturedChild {
    child: Child,
    pgid: Option<u32>,
    stdout: JoinHandle<io::Result<Vec<u8>>>,
    stderr: JoinHandle<io::Result<Vec<u8>>>,
}

impl CapturedChild {
    async fn stop(mut self) -> Result<String, Box<dyn Error>> {
        let mut errors = Vec::new();
        record_cleanup_result(
            &mut errors,
            "PowerMonitor process",
            terminate_process_group(&mut self.child, self.pgid).await,
        );

        let mut output = Vec::new();
        match self.stdout.await {
            Ok(Ok(value)) => output.extend(value),
            Ok(Err(error)) => errors.push(format!("PowerMonitor stdout capture failed: {error}")),
            Err(error) => errors.push(format!("PowerMonitor stdout task failed: {error}")),
        }
        match self.stderr.await {
            Ok(Ok(value)) => output.extend(value),
            Ok(Err(error)) => errors.push(format!("PowerMonitor stderr capture failed: {error}")),
            Err(error) => errors.push(format!("PowerMonitor stderr task failed: {error}")),
        }

        if errors.is_empty() {
            Ok(String::from_utf8_lossy(&output).into_owned())
        } else {
            Err(io::Error::other(errors.join("; ")).into())
        }
    }
}

struct MonolithChild {
    child: Child,
    pgid: Option<u32>,
}

struct ManagedPowerMonitor {
    child: CapturedChild,
    secrets: Vec<&'static str>,
}

#[derive(Default)]
struct FixtureProcesses {
    monolith: Option<MonolithChild>,
    powermonitors: Vec<ManagedPowerMonitor>,
}

type CleanupFuture = Pin<Box<dyn Future<Output = Result<(), String>>>>;

impl FixtureProcesses {
    fn register_monolith(&mut self, child: MonolithChild) {
        self.monolith = Some(child);
    }

    fn register_powermonitor(&mut self, child: CapturedChild, secrets: Vec<&'static str>) {
        self.powermonitors
            .push(ManagedPowerMonitor { child, secrets });
    }

    async fn cleanup(mut self) -> Result<(), Box<dyn Error>> {
        let mut actions: Vec<CleanupFuture> = Vec::new();
        for power_monitor in self.powermonitors.drain(..) {
            actions.push(Box::pin(async move {
                let output = power_monitor
                    .child
                    .stop()
                    .await
                    .map_err(|error| format!("PowerMonitor cleanup failed: {error}"))?;
                if power_monitor
                    .secrets
                    .iter()
                    .any(|secret| output.contains(secret))
                {
                    return Err(
                        "PowerMonitor stdout/stderr exposed a confidential client secret"
                            .to_owned(),
                    );
                }
                Ok(())
            }));
        }
        if let Some(mut monolith) = self.monolith.take() {
            actions.push(Box::pin(async move {
                terminate_process_group(&mut monolith.child, monolith.pgid)
                    .await
                    .map_err(|error| format!("monolith cleanup failed: {error}"))
            }));
        }
        cleanup_all(actions).await
    }
}

impl Drop for FixtureProcesses {
    fn drop(&mut self) {
        for power_monitor in &mut self.powermonitors {
            let _ = kill_process_group(power_monitor.child.pgid);
            let _ = power_monitor.child.child.start_kill();
        }
        if let Some(monolith) = &mut self.monolith {
            let _ = kill_process_group(monolith.pgid);
            let _ = monolith.child.start_kill();
        }
    }
}

struct PreparedPowerMonitor {
    directory: PathBuf,
    home: PathBuf,
    npm: PathBuf,
    path: String,
    audit_path: PathBuf,
}

#[tokio::test]
#[ignore = "release-only: installs and builds PowerMonitor with npm"]
async fn powermonitor_bff_uses_only_public_oauth_and_v1_platform_contracts() {
    let npm = required_npm().await.expect(
        "external PowerMonitor contract prerequisite failed: npm is required; install Node.js and npm",
    );
    run_external_app_contract(npm).await.unwrap();
}

async fn run_external_app_contract(npm: PathBuf) -> Result<(), Box<dyn Error>> {
    let mut fixture = Fixture::new().await?;
    let prepared_powermonitor = prepare_powermonitor(&fixture, npm).await?;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");

    let mut bootstrap = Command::new(binary);
    fixture.configure_monolith(&mut bootstrap);
    let bootstrap_output = bootstrap
        .arg("--bootstrap-system")
        .env("IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME", SYSTEM_USERNAME)
        .env("IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD", SYSTEM_PASSWORD)
        .output()
        .await?;
    assert!(
        bootstrap_output.status.success(),
        "monolith bootstrap failed: {bootstrap_output:?}"
    );

    let mut processes = FixtureProcesses::default();

    let result = async {
        processes
            .register_monolith(start_monolith_with_retry(&mut fixture, binary, &client).await?);
        let system_cookie = system_login(&client, fixture.http_address).await?;
        create_tenant(&client, fixture.http_address, &system_cookie).await?;
        let tenant_cookie = tenant_login(&client, fixture.http_address).await?;
        let (child, powermonitor_address) =
            start_powermonitor_with_retry(&fixture, &prepared_powermonitor, &client, CLIENT_SECRET)
                .await?;
        let powermonitor_url = format!("http://{powermonitor_address}");
        processes.register_powermonitor(child, vec![CLIENT_SECRET]);
        assert_powermonitor_environment(&prepared_powermonitor.audit_path)?;
        register_application(
            &client,
            fixture.http_address,
            &tenant_cookie,
            "powermonitor-external",
            "powermonitor-external-client",
            &format!("{powermonitor_url}/api/v1/auth/callback"),
            true,
            &["devices:read"],
            Some(CLIENT_SECRET),
        )
        .await?;
        register_application(
            &client,
            fixture.http_address,
            &tenant_cookie,
            "disabled-external",
            "disabled-external-client",
            "https://disabled.example.test/callback",
            false,
            &["devices:read"],
            None,
        )
        .await?;

        create_user(&client, fixture.http_address, &tenant_cookie).await?;
        let user_cookie = user_login(&client, fixture.http_address).await?;
        assert_management_mutation_denied_to_user(
            &client,
            fixture.http_address,
            &user_cookie,
        )
        .await?;

        let login = client
            .get(format!("{powermonitor_url}/api/v1/auth/login"))
            .send()
            .await?;
        assert_eq!(login.status(), StatusCode::TEMPORARY_REDIRECT);
        let state_cookie = cookie(&login, "powermonitor_oauth_state")?;
        let authorize_url = login
            .headers()
            .get(LOCATION)
            .expect("PowerMonitor login did not redirect to public authorization")
            .to_str()?
            .to_owned();
        assert!(authorize_url.starts_with(&format!("{}/oauth/authorize", fixture.public_url())));

        let authorize = client
            .get(authorize_url)
            .header(COOKIE, &user_cookie)
            .send()
            .await?;
        assert_eq!(authorize.status(), StatusCode::FOUND);
        let callback_url = authorize
            .headers()
            .get(LOCATION)
            .expect("public authorization did not redirect to the PowerMonitor callback")
            .to_str()?
            .to_owned();
        assert!(callback_url.starts_with(&format!("{powermonitor_url}/api/v1/auth/callback")));

        let callback = client
            .get(callback_url)
            .header(COOKIE, state_cookie)
            .send()
            .await?;
        if callback.status() != StatusCode::TEMPORARY_REDIRECT {
            return Err(io::Error::other(format!(
                "PowerMonitor callback returned {}: {}",
                callback.status(),
                callback.text().await?
            ))
            .into());
        }
        let session_cookie = cookie(&callback, "powermonitor_session")?;
        assert_response_hides_client_secrets(callback, &[CLIENT_SECRET]).await?;

        let scoped_request = client
            .get(format!("{powermonitor_url}/api/v1/devices"))
            .header(COOKIE, &session_cookie)
            .send()
            .await?;
        assert_eq!(scoped_request.status(), StatusCode::OK);
        assert_response_hides_client_secrets(scoped_request, &[CLIENT_SECRET]).await?;

        let denied_scope = client
            .get(format!("{powermonitor_url}/api/v1/alerts"))
            .header(COOKIE, &session_cookie)
            .send()
            .await?;
        assert_eq!(denied_scope.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            denied_scope.json::<serde_json::Value>().await?["code"],
            "forbidden"
        );

        let (invalid_child, invalid_powermonitor_address) = start_powermonitor_with_retry(
            &fixture,
            &prepared_powermonitor,
            &client,
            INVALID_CLIENT_SECRET,
        )
        .await?;
        let invalid_powermonitor_url = format!("http://{invalid_powermonitor_address}");
        processes.register_powermonitor(invalid_child, vec![CLIENT_SECRET, INVALID_CLIENT_SECRET]);
        register_application(
            &client,
            fixture.http_address,
            &tenant_cookie,
            "powermonitor-external",
            "powermonitor-external-client",
            &format!("{invalid_powermonitor_url}/api/v1/auth/callback"),
            true,
            &["devices:read"],
            None,
        )
        .await?;
        assert_powermonitor_environment(&prepared_powermonitor.audit_path)?;

        let invalid_login = client
            .get(format!("{invalid_powermonitor_url}/api/v1/auth/login"))
            .send()
            .await?;
        assert_eq!(invalid_login.status(), StatusCode::TEMPORARY_REDIRECT);
        let invalid_state_cookie = cookie(&invalid_login, "powermonitor_oauth_state")?;
        let invalid_authorize_url = invalid_login
            .headers()
            .get(LOCATION)
            .expect("PowerMonitor login did not redirect to public authorization")
            .to_str()?
            .to_owned();
        let invalid_authorize = client
            .get(invalid_authorize_url)
            .header(COOKIE, &user_cookie)
            .send()
            .await?;
        assert_eq!(invalid_authorize.status(), StatusCode::FOUND);
        let invalid_callback_url = invalid_authorize
            .headers()
            .get(LOCATION)
            .expect("public authorization did not redirect to the PowerMonitor callback")
            .to_str()?
            .to_owned();
        let invalid_callback = client
            .get(invalid_callback_url)
            .header(COOKIE, invalid_state_cookie)
            .send()
            .await?;
        assert_eq!(invalid_callback.status(), StatusCode::BAD_GATEWAY);
        assert!(
            invalid_callback
                .headers()
                .get_all(SET_COOKIE)
                .iter()
                .all(|value| {
                    value
                        .to_str()
                        .map_or(true, |value| !value.starts_with("powermonitor_session="))
                }),
            "invalid confidential BFF exchange created a session"
        );
        assert_response_hides_client_secrets(
            invalid_callback,
            &[CLIENT_SECRET, INVALID_CLIENT_SECRET],
        )
        .await?;

        let confidential_token = client
            .post(format!("{}/oauth/token", fixture.public_url()))
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", "powermonitor-external-client"),
                ("client_secret", CLIENT_SECRET),
                ("scope", "devices:read"),
            ])
            .send()
            .await?;
        assert_eq!(confidential_token.status(), StatusCode::OK);
        let confidential_token =
            confidential_token.json::<serde_json::Value>().await?["access_token"]
                .as_str()
                .expect("client credentials response omitted an access token")
                .to_owned();
        assert_eq!(
            client
                .get(format!("{}/api/v1/devices", fixture.public_url()))
                .bearer_auth(confidential_token)
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );

        let disabled_authorize = client
            .get(format!("{}/oauth/authorize", fixture.public_url()))
            .query(&[
                ("response_type", "code"),
                ("client_id", "disabled-external-client"),
                ("redirect_uri", "https://disabled.example.test/callback"),
                ("scope", "devices:read"),
                ("state", "disabled-state"),
                (
                    "code_challenge",
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                ),
                ("code_challenge_method", "S256"),
            ])
            .header(COOKIE, &user_cookie)
            .send()
            .await?;
        assert_eq!(disabled_authorize.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            disabled_authorize.json::<serde_json::Value>().await?["error"],
            "unauthorized_client"
        );

        for url in [
            format!("{}/internal/commands", fixture.public_url()),
            format!("{powermonitor_url}/internal/commands"),
        ] {
            assert_eq!(
                client.get(url).send().await?.status(),
                StatusCode::NOT_FOUND
            );
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    combine_result_and_cleanup(result, processes.cleanup().await)
}

async fn system_login(
    client: &Client,
    http_address: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{http_address}/api/v1/system/auth/login"))
        .json(&json!({
            "username": SYSTEM_USERNAME,
            "password": SYSTEM_PASSWORD,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    cookie(&response, "iot_nano_session")
}

async fn create_tenant(
    client: &Client,
    http_address: SocketAddr,
    system_cookie: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{http_address}/api/v1/system/tenants"))
        .header(COOKIE, system_cookie)
        .json(&json!({
            "slug": TENANT_SLUG,
            "metadata": { "source": "external-app-contract" },
            "tenant_account_password": TENANT_ACCOUNT_PASSWORD,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

async fn tenant_login(
    client: &Client,
    http_address: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{http_address}/api/v1/tenant/auth/login"))
        .json(&json!({
            "tenant_slug": TENANT_SLUG,
            "password": TENANT_ACCOUNT_PASSWORD,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    cookie(&response, "iot_nano_session")
}

async fn create_user(
    client: &Client,
    http_address: SocketAddr,
    tenant_cookie: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{http_address}/api/v1/management/users"))
        .header(COOKIE, tenant_cookie)
        .json(&json!({
            "username": USERNAME,
            "password": USER_PASSWORD,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

async fn user_login(
    client: &Client,
    http_address: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{http_address}/api/v1/user/auth/login"))
        .json(&json!({
            "tenant_slug": TENANT_SLUG,
            "username": USERNAME,
            "password": USER_PASSWORD,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    cookie(&response, "iot_nano_session")
}

async fn assert_management_mutation_denied_to_user(
    client: &Client,
    http_address: SocketAddr,
    user_cookie: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .post(format!(
            "http://{http_address}/api/v1/management/applications"
        ))
        .header(COOKIE, user_cookie)
        .json(&json!({
            "app_id": "user-session-denied",
            "kind": "full_stack",
            "launch_url": "https://denied.example.test",
            "client_id": "user-session-denied-client",
            "redirect_uris": ["https://denied.example.test/callback"],
            "allowed_scopes": ["devices:read"],
            "enabled": true,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    Ok(())
}

async fn register_application(
    client: &Client,
    http_address: SocketAddr,
    tenant_cookie: &str,
    app_id: &str,
    client_id: &str,
    redirect_uri: &str,
    enabled: bool,
    scopes: &[&str],
    client_secret: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .post(format!(
            "http://{http_address}/api/v1/management/applications"
        ))
        .header(COOKIE, tenant_cookie)
        .json(&json!({
            "app_id": app_id,
            "kind": "full_stack",
            "launch_url": redirect_uri,
            "client_id": client_id,
            "redirect_uris": [redirect_uri],
            "allowed_scopes": scopes,
            "enabled": enabled,
            "client_secret": client_secret,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

fn powermonitor_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/powermonitor")
}

async fn prepare_powermonitor(
    fixture: &Fixture,
    npm: PathBuf,
) -> Result<PreparedPowerMonitor, Box<dyn Error>> {
    let source = powermonitor_directory();
    let directory = fixture.root.join("powermonitor");
    copy_powermonitor_source(&source, &directory)?;
    assert!(
        !directory.join(".next").exists(),
        "external app fixture copied an artifact from apps/powermonitor/.next"
    );
    let home = fixture.root.join("powermonitor-home");
    fs::create_dir(&home)?;
    let path = node_path()?;
    run_npm(
        &npm,
        &path,
        &home,
        &directory,
        &["ci", "--no-audit", "--no-fund"],
    )
    .await?;
    run_npm(&npm, &path, &home, &directory, &["run", "build"]).await?;
    assert!(
        directory.join(".next").is_dir(),
        "external app contract did not build PowerMonitor in its isolated fixture"
    );

    let audit_path = fixture.root.join("powermonitor-start-environment.txt");
    Ok(PreparedPowerMonitor {
        directory,
        home,
        npm,
        path,
        audit_path,
    })
}

async fn run_npm(
    npm: &Path,
    path: &str,
    home: &Path,
    directory: &Path,
    args: &[&str],
) -> Result<(), Box<dyn Error>> {
    let output = Command::new(npm)
        .args(args)
        .current_dir(directory)
        .env_clear()
        .env("HOME", home)
        .env("NO_COLOR", "1")
        .env("PATH", path)
        .output()
        .await?;
    assert!(
        output.status.success(),
        "PowerMonitor npm {} failed:\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

async fn start_monolith_with_retry(
    fixture: &mut Fixture,
    binary: &str,
    client: &Client,
) -> Result<MonolithChild, Box<dyn Error>> {
    let mut failures = Vec::new();
    for attempt in 1..=START_ATTEMPTS {
        fixture.release_monolith_addresses();
        let mut command = Command::new(binary);
        fixture.configure_monolith(&mut command);
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                failures.push(format!(
                    "attempt {attempt}: failed to spawn monolith: {error}"
                ));
                if attempt < START_ATTEMPTS {
                    fixture.rebind_monolith_addresses().await?;
                    continue;
                }
                break;
            }
        };
        let pgid = child.id();
        match wait_ready(client, &fixture.public_url()).await {
            Ok(()) => return Ok(MonolithChild { child, pgid }),
            Err(error) => {
                if let Err(cleanup_error) = terminate_process_group(&mut child, pgid).await {
                    return Err(io::Error::other(format!(
                        "attempt {attempt}: monolith did not become ready: {error}; cleanup failed: {cleanup_error}"
                    ))
                    .into());
                }
                failures.push(format!(
                    "attempt {attempt}: monolith did not become ready: {error}"
                ));
                if attempt < START_ATTEMPTS {
                    fixture.rebind_monolith_addresses().await?;
                }
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AddrInUse,
        format!(
            "monolith could not bind ready test ports after {START_ATTEMPTS} attempts: {}",
            failures.join("\n")
        ),
    )
    .into())
}

async fn start_powermonitor(
    fixture: &Fixture,
    prepared: &PreparedPowerMonitor,
    address: SocketAddr,
    client_secret: &str,
) -> Result<CapturedChild, Box<dyn Error>> {
    let wrapper = create_npm_audit_wrapper(&prepared.npm, &prepared.audit_path)?;
    let port = address.port().to_string();
    let mut command = Command::new(wrapper);
    command
        .args([
            "run",
            "start",
            "--",
            "--hostname",
            "127.0.0.1",
            "--port",
            &port,
        ])
        .current_dir(&prepared.directory)
        .env_clear()
        .env("HOME", &prepared.home)
        .env("NO_COLOR", "1")
        .env("PATH", &prepared.path)
        .env("PLATFORM_BASE_URL", fixture.public_url())
        .env("PLATFORM_AUTH_BASE_URL", fixture.public_url())
        .env("OAUTH_CLIENT_ID", "powermonitor-external-client")
        .env("OAUTH_CLIENT_SECRET", client_secret)
        .env(
            "OAUTH_REDIRECT_URI",
            format!("http://{address}/api/v1/auth/callback"),
        )
        .env("OAUTH_SCOPE", "devices:read")
        .env("SESSION_SECRET", SESSION_SECRET)
        .env("NEXT_TELEMETRY_DISABLED", "1")
        .env("NODE_ENV", "production")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn()?;
    let pgid = child.id();
    let stdout = capture_output(child.stdout.take().expect("PowerMonitor stdout"));
    let stderr = capture_stderr(child.stderr.take().expect("PowerMonitor stderr"));
    Ok(CapturedChild {
        child,
        pgid,
        stdout,
        stderr,
    })
}

async fn start_powermonitor_with_retry(
    fixture: &Fixture,
    prepared: &PreparedPowerMonitor,
    client: &Client,
    client_secret: &str,
) -> Result<(CapturedChild, SocketAddr), Box<dyn Error>> {
    let mut failures = Vec::new();
    for attempt in 1..=START_ATTEMPTS {
        let (address, listener) = reserve_address().await?;
        drop(listener);
        let child = start_powermonitor(fixture, prepared, address, client_secret).await?;
        let url = format!("http://{address}");
        if wait_powermonitor(client, &url).await.is_ok() {
            return Ok((child, address));
        }
        let output = child.stop().await.map_err(|error| {
            io::Error::other(format!(
                "attempt {attempt}: PowerMonitor cleanup failed: {error}"
            ))
        })?;
        failures.push(format!("attempt {attempt}: {output}"));
    }
    Err(io::Error::new(
        io::ErrorKind::AddrInUse,
        format!(
            "PowerMonitor could not bind a ready test port after {START_ATTEMPTS} attempts: {}",
            failures.join("\n")
        ),
    )
    .into())
}

async fn required_npm() -> Result<PathBuf, Box<dyn Error>> {
    let npm = executable_in_path("npm").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "external PowerMonitor contract prerequisite missing: npm executable was not found on PATH",
        )
    })?;
    let node = executable_in_path("node").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "external PowerMonitor contract prerequisite missing: node executable was not found on PATH",
        )
    })?;
    let output = Command::new(&npm)
        .arg("--version")
        .env_clear()
        .env("PATH", node.parent().expect("node parent"))
        .output()
        .await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "external PowerMonitor contract prerequisite missing: npm --version failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }
    Ok(npm)
}

fn node_path() -> Result<String, Box<dyn Error>> {
    let node = executable_in_path("node").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "external PowerMonitor contract prerequisite missing: node executable was not found on PATH",
        )
    })?;
    let node_parent = node.parent().expect("node parent");
    let path = env::join_paths([node_parent, Path::new("/usr/bin"), Path::new("/bin")])?;
    Ok(path.to_string_lossy().into_owned())
}

fn executable_in_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|path| {
        env::split_paths(&path)
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn copy_powermonitor_source(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        if matches!(name.to_str(), Some("node_modules" | ".next")) {
            continue;
        }
        let target = destination.join(&name);
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_powermonitor_source(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn create_npm_audit_wrapper(npm: &Path, audit_path: &Path) -> io::Result<PathBuf> {
    let wrapper = audit_path.with_extension("npm");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n/usr/bin/env > {}\nexec {} \"$@\"\n",
            shell_quote(audit_path),
            shell_quote(npm)
        ),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700))?;
    }
    Ok(wrapper)
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn capture_output<R>(mut reader: R) -> JoinHandle<io::Result<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await?;
        Ok(output)
    })
}

fn capture_stderr(stderr: ChildStderr) -> JoinHandle<io::Result<Vec<u8>>> {
    capture_output(stderr)
}

fn assert_powermonitor_environment(audit_path: &Path) -> Result<(), Box<dyn Error>> {
    let values = fs::read_to_string(audit_path)?;
    let names = values
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name))
        .collect::<BTreeSet<_>>();
    let allowed = BTreeSet::from([
        "HOME",
        "NEXT_TELEMETRY_DISABLED",
        "NODE_ENV",
        "NO_COLOR",
        "OAUTH_CLIENT_ID",
        "OAUTH_CLIENT_SECRET",
        "OAUTH_REDIRECT_URI",
        "OAUTH_SCOPE",
        "PATH",
        "PLATFORM_BASE_URL",
        "PLATFORM_AUTH_BASE_URL",
        "PWD",
        "SESSION_SECRET",
        "SHLVL",
        "_",
    ]);
    assert!(
        names.is_subset(&allowed),
        "PowerMonitor inherited unexpected environment variables: {names:?}"
    );
    for name in [
        "DATABASE_URL",
        "IOT_NANO_INTERNAL_DIR",
        "IOT_NANO_SQLITE_PATH",
        "PLATFORM_DB_URL",
        "PLATFORM_DATABASE_URL",
        "INTERNAL_STATE_PATH",
        "IOT_NANO_DATABASE_URL",
    ] {
        assert!(
            !names.contains(name),
            "PowerMonitor received forbidden platform state variable {name}"
        );
    }
    Ok(())
}

async fn assert_response_hides_client_secrets(
    response: Response,
    secrets: &[&str],
) -> Result<(), Box<dyn Error>> {
    for value in response.headers().values() {
        for secret in secrets {
            assert!(
                !value
                    .as_bytes()
                    .windows(secret.len())
                    .any(|part| part == secret.as_bytes()),
                "PowerMonitor response headers exposed a confidential client secret"
            );
        }
    }
    let body = response.text().await?;
    for secret in secrets {
        assert!(
            !body.contains(secret),
            "PowerMonitor response body exposed a confidential client secret"
        );
    }
    Ok(())
}

async fn wait_ready(client: &Client, public_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..100 {
        if client
            .get(format!("{public_url}/healthz"))
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

async fn wait_powermonitor(
    client: &Client,
    powermonitor_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..200 {
        if client
            .get(format!("{powermonitor_url}/api/v1/auth/login"))
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::TEMPORARY_REDIRECT)
        {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err("PowerMonitor did not become ready".into())
}

fn cookie(response: &Response, name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let prefix = format!("{name}=");
    response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .find(|value| value.starts_with(&prefix))
        .map(str::to_owned)
        .ok_or_else(|| format!("response did not set {name}").into())
}

async fn terminate_process_group(child: &mut Child, pgid: Option<u32>) -> Result<(), String> {
    let mut errors = Vec::new();
    let should_stop = match child.try_wait() {
        Ok(None) => true,
        Ok(Some(_)) => false,
        Err(error) => {
            errors.push(format!("could not inspect process state: {error}"));
            true
        }
    };
    if let Err(error) = kill_process_group(pgid) {
        errors.push(format!("could not kill process group: {error}"));
    }
    if should_stop {
        if let Err(error) = child.start_kill() {
            errors.push(format!("could not kill process: {error}"));
        }
    }
    if let Err(error) = child.wait().await {
        errors.push(format!("could not reap process: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn kill_process_group(pgid: Option<u32>) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) = test_hooks::intercept_kill_process_group(pgid) {
        return result;
    }

    #[cfg(unix)]
    if let Some(raw_pgid) = pgid {
        let Some(pgid) = rustix::process::Pid::from_raw(raw_pgid as i32) else {
            return Err(format!("invalid process group id {raw_pgid}"));
        };
        return match rustix::process::kill_process_group(pgid, rustix::process::Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(error) => Err(error.to_string()),
        };
    }
    Ok(())
}

async fn reserve_address() -> io::Result<(SocketAddr, TcpListener)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    Ok((address, listener))
}

async fn reserve_monolith_addresses() -> io::Result<([SocketAddr; 3], Vec<TcpListener>)> {
    let mut addresses = Vec::with_capacity(3);
    let mut listeners = Vec::with_capacity(3);
    for _ in 0..3 {
        let (address, listener) = reserve_address().await?;
        addresses.push(address);
        listeners.push(listener);
    }
    let addresses = addresses
        .try_into()
        .expect("three reserved monolith addresses");
    Ok((addresses, listeners))
}

async fn cleanup_all(actions: Vec<CleanupFuture>) -> Result<(), Box<dyn Error>> {
    let mut errors = Vec::new();
    for action in actions {
        if let Err(error) = action.await {
            errors.push(error);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(errors.join("; ")).into())
    }
}

fn record_cleanup_result(errors: &mut Vec<String>, context: &str, result: Result<(), String>) {
    if let Err(error) = result {
        errors.push(format!("{context}: {error}"));
    }
}

fn combine_result_and_cleanup(
    result: Result<(), Box<dyn Error>>,
    cleanup: Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(result), Err(cleanup)) => Err(io::Error::other(format!(
            "external app contract failed: {result}; fixture cleanup also failed: {cleanup}"
        ))
        .into()),
    }
}

#[tokio::test]
async fn cleanup_failure_does_not_prevent_later_actions() {
    use std::{cell::RefCell, rc::Rc};

    let actions = Rc::new(RefCell::new(Vec::new()));
    let first = Rc::clone(&actions);
    let second = Rc::clone(&actions);
    let error = cleanup_all(vec![
        Box::pin(async move {
            first.borrow_mut().push("first");
            Err("first cleanup failed".to_owned())
        }),
        Box::pin(async move {
            second.borrow_mut().push("second");
            Ok(())
        }),
    ])
    .await
    .expect_err("the first cleanup action must fail");

    assert_eq!(actions.borrow().as_slice(), ["first", "second"]);
    assert!(error.to_string().contains("first cleanup failed"));
}

#[cfg(unix)]
#[tokio::test]
async fn process_group_cleanup_reports_errors_and_kills_saved_group_after_leader_exit() {
    let mut failed_child = Command::new("sh")
        .args(["-c", "exit 0"])
        .process_group(0)
        .spawn()
        .unwrap();
    let failed_pgid = failed_child.id();
    failed_child.wait().await.unwrap();

    let recorded =
        test_hooks::install_kill_process_group_hook(Some("injected cleanup failure"), true);
    let cleanup_error = terminate_process_group(&mut failed_child, failed_pgid)
        .await
        .expect_err("a non-ESRCH group-kill error must fail cleanup");
    test_hooks::clear_kill_process_group_hook();
    assert!(cleanup_error.contains("injected cleanup failure"));
    assert_eq!(
        recorded.lock().unwrap().as_slice(),
        &[failed_pgid],
        "cleanup must use the PGID captured before the leader exited"
    );

    let directory = tempfile::tempdir().unwrap();
    let descendant_path = directory.path().join("descendant.pid");
    let script = format!(
        "sleep 30 & echo $! > {} ; wait",
        shell_quote(&descendant_path)
    );
    let mut leader = Command::new("sh")
        .args(["-c", &script])
        .process_group(0)
        .spawn()
        .unwrap();
    let saved_pgid = leader.id().expect("group leader must have a PID");
    let descendant_pid = loop {
        if let Some(pid) = fs::read_to_string(&descendant_path)
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
        {
            break pid;
        }
        sleep(Duration::from_millis(10)).await;
    };
    leader.start_kill().unwrap();
    leader.wait().await.unwrap();

    let recorded = test_hooks::install_kill_process_group_hook(None, false);
    terminate_process_group(&mut leader, Some(saved_pgid))
        .await
        .expect("ESRCH from a group that disappeared should be suppressed");
    test_hooks::clear_kill_process_group_hook();
    assert_eq!(
        recorded.lock().unwrap().as_slice(),
        &[Some(saved_pgid)],
        "an exited leader must still trigger termination of its saved group"
    );

    let descendant = rustix::process::Pid::from_raw(descendant_pid as i32).unwrap();
    for _ in 0..100 {
        if rustix::process::test_kill_process(descendant).is_err() {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("process-group descendant remained alive after saved-group cleanup");
}

#[cfg(test)]
mod test_hooks {
    use std::sync::{Arc, Mutex, OnceLock};

    struct KillProcessGroupHook {
        error: Option<String>,
        intercept: bool,
        recorded: Arc<Mutex<Vec<Option<u32>>>>,
    }

    static KILL_PROCESS_GROUP_HOOK: OnceLock<Mutex<Option<KillProcessGroupHook>>> = OnceLock::new();

    pub(super) fn install_kill_process_group_hook(
        error: Option<&str>,
        intercept: bool,
    ) -> Arc<Mutex<Vec<Option<u32>>>> {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        *KILL_PROCESS_GROUP_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = Some(KillProcessGroupHook {
            error: error.map(str::to_owned),
            intercept,
            recorded: Arc::clone(&recorded),
        });
        recorded
    }

    pub(super) fn clear_kill_process_group_hook() {
        *KILL_PROCESS_GROUP_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap() = None;
    }

    pub(super) fn intercept_kill_process_group(pgid: Option<u32>) -> Option<Result<(), String>> {
        let hook = KILL_PROCESS_GROUP_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap();
        let hook = hook.as_ref()?;
        hook.recorded.lock().unwrap().push(pgid);
        if !hook.intercept {
            return None;
        }
        Some(match &hook.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        })
    }
}
