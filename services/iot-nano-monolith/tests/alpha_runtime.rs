use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use iot_api::hash_password;
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{MonolithConfig, MonolithRuntime, bootstrap_system};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewTenant, NewTenantAccount,
    TenantIdentityRepository,
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mqtt_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        Self {
            config: MonolithConfig {
                storage: StorageConfiguration {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(root.join("platform.sqlite")),
                    sqlite_busy_timeout_ms: 5_000,
                },
                device_token_vault_key: "test-device-token-vault-key-material-0001".to_owned(),
                internal_dir: root.join("internal"),
                http: reserve_address().await,
                mqtt_tcp: reserve_address().await,
                mqtt_tls: reserve_address().await,
                web_https_enabled: false,
                tls_cert_path: mqtt_fixtures.join("server.crt"),
                tls_key_path: mqtt_fixtures.join("server.key"),
                shutdown_deadline: Duration::from_secs(2),
            },
            _directory: directory,
        }
    }
}

#[tokio::test]
async fn alpha_runtime_binds_health_and_mqtt_after_recovery() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert!(runtime.readiness().is_ready());
    assert_health(fixture.config.http).await;
    assert_health(fixture.config.http).await;
    let mqtt = TcpStream::connect(fixture.config.mqtt_tcp).await.unwrap();
    drop(mqtt);
    let mqtt_tls = TcpStream::connect(fixture.config.mqtt_tls).await.unwrap();
    drop(mqtt_tls);

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    for address in [
        fixture.config.http,
        fixture.config.http,
        fixture.config.mqtt_tcp,
        fixture.config.mqtt_tls,
    ] {
        let listener = TcpListener::bind(address).await.unwrap();
        drop(listener);
    }
}

#[tokio::test]
async fn alpha_runtime_serves_oauth_public_api_and_management_api_on_one_listener() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert_http_status(fixture.config.http, "/api/v1/devices", 401).await;
    assert_http_status(fixture.config.http, "/api/v1/auth/me", 401).await;
    assert_http_status(fixture.config.http, "/api/auth/me", 404).await;
    assert_form_post_status(fixture.config.http, "/oauth/token", "", 400).await;

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn alpha_runtime_renders_system_infrastructure_from_started_runtime_without_secrets() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    bootstrap_system(runtime.platform().unwrap(), "system", "SystemAccount@2026")
        .await
        .unwrap();

    let login_body = r#"{"username":"system","password":"SystemAccount@2026"}"#;
    let login = send_http(
        fixture.config.http,
        format!(
            "POST /api/v1/system/auth/login HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{login_body}",
            login_body.len(),
        ),
    )
    .await;
    assert!(
        login.starts_with(b"HTTP/1.1 200"),
        "unexpected system login response: {}",
        String::from_utf8_lossy(&login)
    );
    let cookie = String::from_utf8_lossy(&login)
        .lines()
        .find_map(|line| line.strip_prefix("set-cookie: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let infrastructure = send_http(
        fixture.config.http,
        format!(
            "GET /system/infrastructure HTTP/1.1\r\nHost: localhost\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(
        infrastructure.starts_with(b"HTTP/1.1 200"),
        "unexpected infrastructure response: {}",
        String::from_utf8_lossy(&infrastructure)
    );
    let infrastructure = String::from_utf8(infrastructure).unwrap();
    assert!(infrastructure.contains("Runtime health"));
    assert!(infrastructure.contains("Ready"));
    assert!(infrastructure.contains(&format!("Listening on {}", fixture.config.http)));
    assert!(infrastructure.contains(&format!("Listening on {}", fixture.config.http)));
    assert!(infrastructure.contains(&format!("Listening on {}", fixture.config.mqtt_tcp)));
    assert!(infrastructure.contains("Listening (TLS endpoint bound)"));
    assert!(infrastructure.contains("Completed at startup"));
    assert!(infrastructure.contains("SQLite connected"));
    assert!(infrastructure.contains("Loaded for MQTT TLS"));
    assert!(!infrastructure.contains("Not reported"));
    assert!(
        !infrastructure.contains(
            &fixture
                .config
                .storage
                .sqlite_path
                .as_ref()
                .unwrap()
                .display()
                .to_string(),
        )
    );
    assert!(!infrastructure.contains(&fixture.config.tls_cert_path.display().to_string()));
    assert!(!infrastructure.contains(&fixture.config.tls_key_path.display().to_string()));
    assert!(!infrastructure.contains(&fixture.config.device_token_vault_key));
    assert!(!infrastructure.contains("SystemAccount@2026"));

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn alpha_runtime_infrastructure_page_stops_presenting_components_as_healthy_after_failure() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    bootstrap_system(runtime.platform().unwrap(), "system", "SystemAccount@2026")
        .await
        .unwrap();

    let login_body = r#"{"username":"system","password":"SystemAccount@2026"}"#;
    let login = send_http(
        fixture.config.http,
        format!(
            "POST /api/v1/system/auth/login HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{login_body}",
            login_body.len(),
        ),
    )
    .await;
    assert!(
        login.starts_with(b"HTTP/1.1 200"),
        "unexpected system login response: {}",
        String::from_utf8_lossy(&login)
    );
    let cookie = String::from_utf8_lossy(&login)
        .lines()
        .find_map(|line| line.strip_prefix("set-cookie: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let started = send_http(
        fixture.config.http,
        format!(
            "GET /system/infrastructure HTTP/1.1\r\nHost: localhost\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    let started = String::from_utf8(started).unwrap();
    assert!(started.contains("Listening on"));
    assert!(started.contains("SQLite connected"));

    runtime.cancellation_token().cancel();
    timeout(Duration::from_secs(1), async {
        while runtime.readiness().is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime readiness remained healthy after parent cancellation");

    let failed = send_http(
        fixture.config.http,
        format!(
            "GET /system/infrastructure HTTP/1.1\r\nHost: localhost\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(
        failed.starts_with(b"HTTP/1.1 200"),
        "unexpected infrastructure response after failure: {}",
        String::from_utf8_lossy(&failed)
    );
    let failed = String::from_utf8(failed).unwrap();
    assert!(failed.contains("Not ready"));
    for healthy_status in [
        "Listening on",
        "Listening (TLS endpoint bound)",
        "Completed at startup",
        "SQLite connected",
        "Loaded for MQTT TLS",
    ] {
        assert!(
            !failed.contains(healthy_status),
            "stale healthy status {healthy_status}: {failed}"
        );
    }

    let _ = timeout(
        Duration::from_secs(3),
        runtime.shutdown(Instant::now() + Duration::from_secs(2)),
    )
    .await
    .expect("runtime shutdown hung after parent cancellation");
}

#[tokio::test]
async fn alpha_runtime_marks_not_ready_after_parent_cancellation() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    runtime.cancellation_token().cancel();

    timeout(Duration::from_secs(1), async {
        while runtime.readiness().is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("runtime readiness remained healthy after parent cancellation");
    assert_health_status(fixture.config.http, 503).await;

    let _ = timeout(
        Duration::from_secs(3),
        runtime.shutdown(Instant::now() + Duration::from_secs(2)),
    )
    .await
    .expect("runtime shutdown hung after parent cancellation");
}

#[tokio::test]
async fn alpha_runtime_mounts_generic_public_api_and_requires_bearer_token() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert_http_status(fixture.config.http, "/api/v1/assets", 401).await;
    assert_http_status(fixture.config.http, "/api/v1/devices", 401).await;
    assert_http_status(
        fixture.config.http,
        "/api/v1/devices/missing-device",
        401,
    )
    .await;

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn alpha_runtime_mounts_public_oauth_token_endpoint() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert_form_post_status(fixture.config.http, "/oauth/token", "", 400).await;

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn alpha_runtime_mounts_management_login_endpoint() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert_json_post_status(
        fixture.config.http,
        "/api/v1/auth/login",
        r#"{"username":"missing","password":"wrong"}"#,
        401,
    )
    .await;
    assert_json_post_status(
        fixture.config.http,
        "/api/auth/login",
        r#"{"username":"missing","password":"wrong"}"#,
        404,
    )
    .await;

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn alpha_runtime_exposes_openapi_and_swagger_on_the_single_http_listener() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert_http_status(
        fixture.config.http,
        "/api-docs/openapi.json",
        200,
    )
    .await;
    assert_http_status(fixture.config.http, "/docs/", 200).await;

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn alpha_runtime_uses_a_management_session_to_issue_a_public_pkce_code() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    let store = runtime.platform().unwrap();
    let (tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store,
        NewTenant {
            slug: "test".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("TenantAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'admin', ?, 'admin', 'admin')",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(tenant.id.to_string())
    .bind(hash_password("NanoAdmin@1234").unwrap())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    ApplicationRepository::upsert_application(
        store,
        NewApplication {
            app_id: "alpha-pkce-app".parse().unwrap(),
            tenant_id: tenant.id,
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: "alpha-pkce-client".parse().unwrap(),
            redirect_uris: vec!["https://client.example.test/callback".parse().unwrap()],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();

    let login = send_http(
        fixture.config.http,
        "POST /api/v1/auth/login HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 48\r\n\r\n{\"username\":\"admin\",\"password\":\"NanoAdmin@1234\"}".to_owned(),
    )
    .await;
    assert!(login.starts_with(b"HTTP/1.1 200"));
    let cookie = String::from_utf8_lossy(&login)
        .lines()
        .find_map(|line| line.strip_prefix("set-cookie: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let challenge = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let authorize = send_http(
        fixture.config.http,
        format!(
            "GET /oauth/authorize?response_type=code&client_id=alpha-pkce-client&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256 HTTP/1.1\r\nHost: localhost\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(
        authorize.starts_with(b"HTTP/1.1 302"),
        "unexpected authorization response: {}",
        String::from_utf8_lossy(&authorize)
    );

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

async fn assert_health(address: SocketAddr) {
    assert_health_status(address, 200).await;
}

async fn assert_health_status(address: SocketAddr, status: u16) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(format!("HTTP/1.1 {status}").as_bytes()),
        "unexpected health response: {}",
        String::from_utf8_lossy(&response)
    );
}

async fn assert_http_status(address: SocketAddr, path: &str, status: u16) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(format!("HTTP/1.1 {status}").as_bytes()),
        "unexpected HTTP response: {}",
        String::from_utf8_lossy(&response)
    );
}

async fn assert_form_post_status(address: SocketAddr, path: &str, body: &str, status: u16) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!(
                "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(format!("HTTP/1.1 {status}").as_bytes()),
        "unexpected HTTP response: {}",
        String::from_utf8_lossy(&response)
    );
}

async fn assert_json_post_status(address: SocketAddr, path: &str, body: &str, status: u16) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!(
                "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(format!("HTTP/1.1 {status}").as_bytes()),
        "unexpected HTTP response: {}",
        String::from_utf8_lossy(&response)
    );
}

async fn send_http(address: SocketAddr, request: String) -> Vec<u8> {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    response
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}
