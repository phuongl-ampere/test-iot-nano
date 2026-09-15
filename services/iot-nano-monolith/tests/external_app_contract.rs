use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
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

const ADMIN_PASSWORD: &str = "ExternalContractAdmin@2026";
const CLIENT_SECRET: &str = "ExternalContractClientSecret@2026";
const INVALID_CLIENT_SECRET: &str = "InvalidExternalContractClientSecret@2026";
const SESSION_SECRET: &str = "external-contract-session-secret-material-0001";

struct Fixture {
    _directory: TempDir,
    root: PathBuf,
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
            root: root.clone(),
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

    fn public_url(&self) -> String {
        format!("http://{}", self.public_address)
    }
}

struct CapturedChild {
    child: Child,
    stdout: JoinHandle<io::Result<Vec<u8>>>,
    stderr: JoinHandle<io::Result<Vec<u8>>>,
}

impl CapturedChild {
    async fn stop(mut self) -> Result<String, Box<dyn Error>> {
        if self.child.try_wait()?.is_none() {
            #[cfg(unix)]
            if let Some(pid) = self
                .child
                .id()
                .and_then(|pid| rustix::process::Pid::from_raw(pid as i32))
            {
                let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
            }
            self.child.start_kill()?;
        }
        let _ = self.child.wait().await?;
        let mut output = self.stdout.await??;
        output.extend(self.stderr.await??);
        Ok(String::from_utf8_lossy(&output).into_owned())
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
async fn powermonitor_bff_uses_only_public_oauth_and_v1_platform_contracts() {
    let npm = required_npm().await.expect(
        "external PowerMonitor contract prerequisite failed: npm is required; install Node.js and npm",
    );
    run_external_app_contract(npm).await.unwrap();
}

async fn run_external_app_contract(npm: PathBuf) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let binary = env!("CARGO_BIN_EXE_iot-nano-monolith");

    let mut bootstrap = Command::new(binary);
    fixture.configure_monolith(&mut bootstrap);
    let bootstrap_output = bootstrap
        .arg("--bootstrap-admin")
        .env(
            "IOT_NANO_BOOTSTRAP_ADMIN_USERNAME",
            "external-contract-admin",
        )
        .env("IOT_NANO_BOOTSTRAP_ADMIN_PASSWORD", ADMIN_PASSWORD)
        .output()
        .await?;
    assert!(
        bootstrap_output.status.success(),
        "monolith bootstrap failed: {bootstrap_output:?}"
    );

    let mut monolith_command = Command::new(binary);
    fixture.configure_monolith(&mut monolith_command);
    let mut monolith = monolith_command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut powermonitor = None;
    let mut invalid_powermonitor = None;
    let prepared_powermonitor = prepare_powermonitor(&fixture, npm).await?;

    let result = async {
        wait_ready(&client, &fixture.public_url()).await?;
        let management_cookie = management_login(&client, fixture.management_address).await?;
        let (child, powermonitor_address) =
            start_powermonitor_with_retry(&fixture, &prepared_powermonitor, &client, CLIENT_SECRET)
                .await?;
        let powermonitor_url = format!("http://{powermonitor_address}");
        assert_powermonitor_environment(&prepared_powermonitor.audit_path)?;
        powermonitor = Some(child);
        register_application(
            &client,
            fixture.management_address,
            &management_cookie,
            "powermonitor-external",
            "powermonitor-external-client",
            &format!("{powermonitor_url}/api/auth/callback"),
            true,
            &["devices:read"],
            Some(CLIENT_SECRET),
        )
        .await?;
        register_application(
            &client,
            fixture.management_address,
            &management_cookie,
            "disabled-external",
            "disabled-external-client",
            "https://disabled.example.test/callback",
            false,
            &["devices:read"],
            None,
        )
        .await?;

        let login = client
            .get(format!("{powermonitor_url}/api/auth/login"))
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
            .header(COOKIE, &management_cookie)
            .send()
            .await?;
        assert_eq!(authorize.status(), StatusCode::FOUND);
        let callback_url = authorize
            .headers()
            .get(LOCATION)
            .expect("public authorization did not redirect to the PowerMonitor callback")
            .to_str()?
            .to_owned();
        assert!(callback_url.starts_with(&format!("{powermonitor_url}/api/auth/callback")));

        let callback = client
            .get(callback_url)
            .header(COOKIE, state_cookie)
            .send()
            .await?;
        assert_eq!(callback.status(), StatusCode::TEMPORARY_REDIRECT);
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
        register_application(
            &client,
            fixture.management_address,
            &management_cookie,
            "powermonitor-external",
            "powermonitor-external-client",
            &format!("{invalid_powermonitor_url}/api/auth/callback"),
            true,
            &["devices:read"],
            None,
        )
        .await?;
        assert_powermonitor_environment(&prepared_powermonitor.audit_path)?;

        let invalid_login = client
            .get(format!("{invalid_powermonitor_url}/api/auth/login"))
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
            .header(COOKIE, &management_cookie)
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
        invalid_powermonitor = Some(invalid_child);

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
            StatusCode::OK
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
            .header(COOKIE, &management_cookie)
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

    if let Some(child) = powermonitor {
        let output = child.stop().await?;
        assert!(
            !output.contains(CLIENT_SECRET),
            "PowerMonitor stdout/stderr exposed the confidential client secret"
        );
    }
    if let Some(child) = invalid_powermonitor {
        let output = child.stop().await?;
        assert!(
            !output.contains(CLIENT_SECRET) && !output.contains(INVALID_CLIENT_SECRET),
            "PowerMonitor stdout/stderr exposed a confidential client secret"
        );
    }
    stop(&mut monolith).await;
    result
}

async fn management_login(
    client: &Client,
    management_address: SocketAddr,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = client
        .post(format!("http://{management_address}/api/auth/login"))
        .json(&json!({
            "username": "external-contract-admin",
            "password": ADMIN_PASSWORD,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    cookie(&response, "iot_nano_session")
}

async fn register_application(
    client: &Client,
    management_address: SocketAddr,
    management_cookie: &str,
    app_id: &str,
    client_id: &str,
    redirect_uri: &str,
    enabled: bool,
    scopes: &[&str],
    client_secret: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .post(format!(
            "http://{management_address}/api/management/applications"
        ))
        .header(COOKIE, management_cookie)
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
    assert!(
        !source.join(".next").exists(),
        "external app contract must not use artifacts in apps/powermonitor/.next"
    );

    let directory = fixture.root.join("powermonitor");
    copy_powermonitor_source(&source, &directory)?;
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
        !source.join(".next").exists(),
        "external app contract left a generated .next directory in apps/powermonitor"
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
        .env("OAUTH_CLIENT_ID", "powermonitor-external-client")
        .env("OAUTH_CLIENT_SECRET", client_secret)
        .env(
            "OAUTH_REDIRECT_URI",
            format!("http://{address}/api/auth/callback"),
        )
        .env("OAUTH_SCOPE", "devices:read")
        .env("SESSION_SECRET", SESSION_SECRET)
        .env("NEXT_TELEMETRY_DISABLED", "1")
        .env("NODE_ENV", "production")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn()?;
    let stdout = capture_output(child.stdout.take().expect("PowerMonitor stdout"));
    let stderr = capture_stderr(child.stderr.take().expect("PowerMonitor stderr"));
    Ok(CapturedChild {
        child,
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
    for _ in 0..5 {
        let address = reserve_address().await;
        let child = start_powermonitor(fixture, prepared, address, client_secret).await?;
        let url = format!("http://{address}");
        if wait_powermonitor(client, &url).await.is_ok() {
            return Ok((child, address));
        }
        failures.push(child.stop().await?);
    }
    Err(io::Error::new(
        io::ErrorKind::AddrInUse,
        format!(
            "PowerMonitor could not bind a ready test port after five attempts: {}",
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
            .get(format!("{powermonitor_url}/api/auth/login"))
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

async fn stop(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}
