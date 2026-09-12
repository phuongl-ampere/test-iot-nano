use std::{
    env,
    fs::{File, OpenOptions},
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use fs2::FileExt;
use iot_api::{
    ApiState, MqttdDeviceTransportSessionRevocation, MqttdDeviceTransportSessionRevoker,
    MqttdDeviceTransportSessionRevokerError, Role, SystemConfigurationService,
    SystemConfigurationServiceError, bootstrap_users, connect_api_database, migrate_api, router,
    validate_password,
};
use iot_core::{
    IngestTuning, MqttConfiguration, SmtpConfiguration, SmtpConfigurationUpdate,
    SystemConfiguration, SystemConfigurationUpdate,
};
use serde_json::json;
use sqlx::{PgPool, Row, query};
use tower::ServiceExt;
use uuid::Uuid;

// Every test targets the same TimescaleDB database and resets shared tables.
static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const ADMIN_PASSWORD: &str = "NanoAdmin@1234";
const VIEWER_PASSWORD: &str = "NanoView@1234";
const ADMIN_SESSION: &str = "session_test_admin";
const VIEWER_SESSION: &str = "session_test_viewer";
const MQTTD_API_SECRET: &str = "test-mqttd-api-secret-must-have-32";

#[derive(Clone, Default)]
struct RecordingSessionRevoker {
    calls: Arc<Mutex<Vec<MqttdDeviceTransportSessionRevocation>>>,
    fail: bool,
}

impl RecordingSessionRevoker {
    fn failing() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            fail: true,
        }
    }

    fn calls(&self) -> Vec<MqttdDeviceTransportSessionRevocation> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl MqttdDeviceTransportSessionRevoker for RecordingSessionRevoker {
    fn revoke_session(
        &self,
        revocation: MqttdDeviceTransportSessionRevocation,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), MqttdDeviceTransportSessionRevokerError>> + Send + '_>,
    > {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(revocation);
        let fail = self.fail;
        Box::pin(async move {
            if fail {
                Err(MqttdDeviceTransportSessionRevokerError::Unavailable)
            } else {
                Ok(())
            }
        })
    }
}

#[derive(Clone)]
struct TestSystemConfigurationService {
    configuration: Arc<Mutex<SystemConfiguration>>,
}

impl TestSystemConfigurationService {
    fn new() -> Self {
        Self {
            configuration: Arc::new(Mutex::new(SystemConfiguration {
                mqtt: MqttConfiguration {
                    host: "127.0.0.1".to_owned(),
                    port: 1883,
                },
                smtp: SmtpConfiguration {
                    enabled: true,
                    host: Some("smtp.example.test".to_owned()),
                    port: 465,
                    username: Some("alerts@example.test".to_owned()),
                    password_configured: true,
                    from: Some("alerts@example.test".to_owned()),
                    to: Some("ops@example.test".to_owned()),
                    timeout_seconds: 15,
                },
                tuning: IngestTuning::default(),
            })),
        }
    }
}

impl SystemConfigurationService for TestSystemConfigurationService {
    fn read(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SystemConfiguration, SystemConfigurationServiceError>>
                + Send
                + '_,
        >,
    > {
        let configuration = self
            .configuration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Box::pin(async move { Ok(configuration) })
    }

    fn apply(
        &self,
        update: SystemConfigurationUpdate,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SystemConfiguration, SystemConfigurationServiceError>>
                + Send
                + '_,
        >,
    > {
        let mut configuration = self
            .configuration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        configuration.smtp.enabled = update.smtp.enabled;
        configuration.smtp.host = update.smtp.host;
        configuration.smtp.port = update.smtp.port;
        configuration.smtp.username = update.smtp.username;
        configuration.smtp.from = update.smtp.from;
        configuration.smtp.to = update.smtp.to;
        configuration.smtp.timeout_seconds = update.smtp.timeout_seconds;
        configuration.tuning = update.tuning;
        let result = configuration.clone();
        Box::pin(async move { Ok(result) })
    }
}

async fn prepared_pool() -> PgPool {
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the local TimescaleDB test database");
    let pool = connect_api_database(&database_url).await.unwrap();
    migrate_api(&pool).await.unwrap();
    query(
        "TRUNCATE user_app_grants, users, api_access_tokens, device_tokens, device_claim_codes,
                  resource_shares, audit_events, devices, assets, device_profiles,
                  asset_profiles CASCADE",
    )
    .execute(&pool)
    .await
    .unwrap();

    query("INSERT INTO devices (device_id, display_name) VALUES ($1, $2)")
        .bind("esp-000123")
        .bind("Greenhouse sensor")
        .execute(&pool)
        .await
        .unwrap();
    bootstrap_users(&pool).await.unwrap();
    pool
}

fn lock_database_file() -> File {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(env::temp_dir().join("rush-iot-nano-timescaledb-tests.lock"))
        .unwrap();
    file.lock_exclusive().unwrap();
    file
}

fn with_admin_auth(builder: axum::http::request::Builder) -> axum::http::request::Builder {
    builder.header(header::AUTHORIZATION, format!("Session {ADMIN_SESSION}"))
}

fn with_viewer_auth(builder: axum::http::request::Builder) -> axum::http::request::Builder {
    builder.header(header::AUTHORIZATION, format!("Session {VIEWER_SESSION}"))
}

fn test_state(pool: PgPool) -> ApiState {
    ApiState::new(pool)
        .with_session(ADMIN_SESSION, "admin", Role::Admin)
        .with_session(VIEWER_SESSION, "viewer", Role::Viewer)
        .with_mqttd_device_transport_session_revoker(RecordingSessionRevoker::default())
}

fn with_mqttd_device_transport_auth(
    builder: axum::http::request::Builder,
) -> axum::http::request::Builder {
    builder.header("x-iot-nano-mqttd-api-secret", MQTTD_API_SECRET)
}

fn mqttd_device_transport_session_authorization_request(
    device_id: &str,
    token_id: &str,
) -> Request<Body> {
    with_mqttd_device_transport_auth(
        Request::builder()
            .method("POST")
            .uri("/internal/mqttd/session-authorization")
            .header(header::CONTENT_TYPE, "application/json"),
    )
    .body(Body::from(
        json!({
            "device_id": device_id,
            "token_id": token_id,
        })
        .to_string(),
    ))
    .unwrap()
}

#[test]
fn password_policy_requires_at_least_eight_ascii_mixed_characters() {
    assert!(validate_password(ADMIN_PASSWORD).is_ok());
    assert!(validate_password("Aa1!bcDe").is_ok());
    assert!(validate_password("Longer1!Password").is_ok());

    for invalid in [
        "short",
        "aa1!bcde",
        "AA1!BCDE",
        "Aa!!bcDe",
        "Aa11bcDe",
        "Aa1 bcDe",
        "Aa1!bcéD",
    ] {
        assert!(
            validate_password(invalid).is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[tokio::test]
async fn serves_openapi_3_1_document_and_swagger_ui() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let specification = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api-docs/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(specification.status(), StatusCode::OK);
    let body = to_bytes(specification.into_body(), usize::MAX)
        .await
        .unwrap();
    let document: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(document["openapi"], "3.1.0");
    assert!(document["paths"].get("/api/auth/login").is_some());
    assert!(
        document["paths"]
            .get("/api/apps/powermonitor/devices")
            .is_some()
    );
    assert!(document["paths"].get("/api/management/devices").is_some());
    assert!(document["paths"].get("/api/my/assets").is_some());
    assert!(document["paths"].get("/api/my/devices").is_some());
    assert!(document["paths"].get("/api/device-claims").is_some());
    assert!(
        document["paths"]
            .get("/api/devices/{device_id}/shares")
            .is_some()
    );
    assert!(
        document["paths"]
            .get("/api/resource-shares/{id}/accept")
            .is_some()
    );
    assert!(
        document["paths"]
            .get("/api/device-tokens/{id}/rotate")
            .is_some()
    );
    assert!(
        document["paths"]
            .get("/api/alert-incidents/{id}/acknowledge")
            .is_some()
    );
    assert!(document["components"]["securitySchemes"]["sessionAuth"].is_object());
    assert!(document["components"]["schemas"]["SystemConfiguration"].is_object());
    assert!(document["components"]["schemas"]["PowerDevice"].is_object());
    assert!(document["components"]["schemas"]["ManagementDevice"].is_object());

    let docs = app
        .oneshot(
            Request::builder()
                .uri("/docs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(docs.status(), StatusCode::OK);
}

#[tokio::test]
async fn hard_coded_users_bootstrap_an_empty_database_once() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query("TRUNCATE user_app_grants, users CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    bootstrap_users(&pool).await.unwrap();
    let initial_hashes = query(
        "SELECT password_hash
         FROM users
         ORDER BY username",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("password_hash"))
    .collect::<Vec<_>>();

    bootstrap_users(&pool).await.unwrap();
    let restarted_hashes = query(
        "SELECT password_hash
         FROM users
         ORDER BY username",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("password_hash"))
    .collect::<Vec<_>>();

    assert_eq!(initial_hashes, restarted_hashes);
    assert!(
        initial_hashes
            .iter()
            .all(|hash| hash.starts_with("$argon2"))
    );
    assert!(
        !initial_hashes
            .iter()
            .any(|hash| hash == ADMIN_PASSWORD || hash == VIEWER_PASSWORD)
    );
}

#[tokio::test]
async fn unauthenticated_api_requests_are_rejected_and_login_is_available() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let protected = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let login = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"WrongPass#2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(protected.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(login.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn username_password_login_issues_an_opaque_session_for_protected_requests() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let login_status = login.status();
    let login_body = to_bytes(login.into_body(), usize::MAX).await.unwrap();
    let login_json: serde_json::Value = serde_json::from_slice(&login_body).unwrap();
    let session_id = login_json["session_id"].as_str().unwrap();
    let protected = app
        .oneshot(
            Request::builder()
                .uri("/api/devices")
                .header(header::AUTHORIZATION, format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(login_status, StatusCode::OK);
    assert_eq!(login_json["role"], "admin");
    assert_eq!(login_json["username"], "admin");
    assert!(session_id.starts_with("session_"));
    assert_eq!(protected.status(), StatusCode::OK);
}

#[tokio::test]
async fn corrupted_password_storage_is_reported_as_a_server_error() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query("UPDATE users SET password_hash = 'not-a-password-hash'")
        .execute(&pool)
        .await
        .unwrap();
    let app = router(test_state(pool));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn admin_provisions_a_named_device_with_a_server_generated_uuid_v7() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let app = router(test_state(pool.clone()));

    let response = app
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/device-tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"display_name":"Python virtual sensor"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let device_id = Uuid::parse_str(created["device_id"].as_str().unwrap()).unwrap();
    let device = query("SELECT display_name FROM devices WHERE device_id = $1")
        .bind(device_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(device_id.get_version_num(), 7);
    assert!(created["token"].as_str().unwrap().starts_with("iotd_"));
    assert_eq!(
        device.get::<Option<String>, _>("display_name").as_deref(),
        Some("Python virtual sensor")
    );
}

#[tokio::test]
async fn token_mutations_revoke_only_the_prior_timescale_transport_session() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query("INSERT INTO devices (device_id, display_name) VALUES ($1, $2)")
        .bind("other-device")
        .bind("Other device")
        .execute(&pool)
        .await
        .unwrap();
    let revoker = RecordingSessionRevoker::default();
    let app = router(test_state(pool).with_mqttd_device_transport_session_revoker(revoker.clone()));

    let other = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/other-device/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let other: serde_json::Value =
        serde_json::from_slice(&to_bytes(other.into_body(), usize::MAX).await.unwrap()).unwrap();
    let other_token_id = Uuid::parse_str(other["id"].as_str().unwrap()).unwrap();

    let first = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let first: serde_json::Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let first_token_id = Uuid::parse_str(first["id"].as_str().unwrap()).unwrap();

    let second = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::CREATED);
    let second: serde_json::Value =
        serde_json::from_slice(&to_bytes(second.into_body(), usize::MAX).await.unwrap()).unwrap();
    let second_token_id = Uuid::parse_str(second["id"].as_str().unwrap()).unwrap();

    let third = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/device-tokens/{second_token_id}/rotate"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(third.status(), StatusCode::CREATED);
    let third: serde_json::Value =
        serde_json::from_slice(&to_bytes(third.into_body(), usize::MAX).await.unwrap()).unwrap();
    let third_token_id = Uuid::parse_str(third["id"].as_str().unwrap()).unwrap();

    let revoke = app
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/device-tokens/{third_token_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(revoke.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        revoker.calls(),
        vec![
            MqttdDeviceTransportSessionRevocation {
                device_id: "esp-000123".to_owned(),
                token_id: first_token_id,
            },
            MqttdDeviceTransportSessionRevocation {
                device_id: "esp-000123".to_owned(),
                token_id: second_token_id,
            },
            MqttdDeviceTransportSessionRevocation {
                device_id: "esp-000123".to_owned(),
                token_id: third_token_id,
            },
        ]
    );
    assert!(
        !revoker
            .calls()
            .iter()
            .any(|call| call.token_id == other_token_id)
    );
}

#[tokio::test]
async fn failed_timescale_session_revocation_returns_503_without_restoring_the_token() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let revoker = RecordingSessionRevoker::failing();
    let app = router(
        test_state(pool.clone()).with_mqttd_device_transport_session_revoker(revoker.clone()),
    );
    let first = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let first: serde_json::Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let first_token_id = Uuid::parse_str(first["id"].as_str().unwrap()).unwrap();

    let replacement = app
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let revoked_at: Option<chrono::DateTime<Utc>> =
        query("SELECT revoked_at FROM device_tokens WHERE id = $1")
            .bind(first_token_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get("revoked_at")
            .unwrap();

    assert_eq!(replacement.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(revoked_at.is_some());
    assert_eq!(
        revoker.calls(),
        vec![MqttdDeviceTransportSessionRevocation {
            device_id: "esp-000123".to_owned(),
            token_id: first_token_id,
        }]
    );
}

#[tokio::test]
async fn mqttd_device_transport_resolves_only_active_direct_device_tokens() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query(
        "INSERT INTO devices (device_id, display_name, is_gateway)
         VALUES ('gateway-001', 'Field gateway', TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let app = router(test_state(pool.clone()).with_mqttd_device_transport_secret(MQTTD_API_SECRET));

    let create = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(create.into_body(), usize::MAX).await.unwrap()).unwrap();
    let token = created["token"].as_str().unwrap().to_owned();
    let token_id = created["id"].as_str().unwrap().to_owned();
    let request_body = json!({
        "client_id": "transport-device-001",
        "username": token,
        "password": ""
    })
    .to_string();

    let missing_secret = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/session-resolution")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(request_body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    let bad_secret = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/session-resolution")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-iot-nano-mqttd-api-secret", "wrong-secret")
                .body(Body::from(request_body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    let password_auth = app
        .clone()
        .oneshot(
            with_mqttd_device_transport_auth(
                Request::builder()
                    .method("POST")
                    .uri("/internal/mqttd/session-resolution")
                    .header(header::CONTENT_TYPE, "application/json"),
            )
            .body(Body::from(
                json!({
                    "client_id": "transport-device-001",
                    "username": token,
                    "password": "not-empty"
                })
                .to_string(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let active = app
        .clone()
        .oneshot(
            with_mqttd_device_transport_auth(
                Request::builder()
                    .method("POST")
                    .uri("/internal/mqttd/session-resolution")
                    .header(header::CONTENT_TYPE, "application/json"),
            )
            .body(Body::from(request_body.clone()))
            .unwrap(),
        )
        .await
        .unwrap();
    let active_status = active.status();
    let active_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(active.into_body(), usize::MAX).await.unwrap()).unwrap();

    query("UPDATE devices SET gateway_device_id = $1 WHERE device_id = $2")
        .bind("gateway-001")
        .bind("esp-000123")
        .execute(&pool)
        .await
        .unwrap();
    let child_token = app
        .clone()
        .oneshot(
            with_mqttd_device_transport_auth(
                Request::builder()
                    .method("POST")
                    .uri("/internal/mqttd/session-resolution")
                    .header(header::CONTENT_TYPE, "application/json"),
            )
            .body(Body::from(request_body.clone()))
            .unwrap(),
        )
        .await
        .unwrap();
    query("UPDATE devices SET gateway_device_id = NULL WHERE device_id = $1")
        .bind("esp-000123")
        .execute(&pool)
        .await
        .unwrap();

    let revoke = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/device-tokens/{token_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let revoked = app
        .oneshot(
            with_mqttd_device_transport_auth(
                Request::builder()
                    .method("POST")
                    .uri("/internal/mqttd/session-resolution")
                    .header(header::CONTENT_TYPE, "application/json"),
            )
            .body(Body::from(request_body))
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(missing_secret.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(bad_secret.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(password_auth.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(active_status, StatusCode::OK);
    assert_eq!(
        active_body,
        json!({
            "device_id": "esp-000123",
            "token_id": token_id,
            "is_gateway": false
        })
    );
    assert_eq!(child_token.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(revoke.status(), StatusCode::NO_CONTENT);
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn mqttd_device_transport_authorizes_only_the_exact_active_direct_or_gateway_token() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query(
        "INSERT INTO devices (device_id, display_name, is_gateway)
         VALUES
            ('other-esp-000123', 'Other sensor', FALSE),
            ('gateway-authorization-001', 'Field gateway', TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let app = router(test_state(pool.clone()).with_mqttd_device_transport_secret(MQTTD_API_SECRET));

    let direct_create = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let direct_create_status = direct_create.status();
    let direct: serde_json::Value = serde_json::from_slice(
        &to_bytes(direct_create.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let direct_token_id = direct["id"].as_str().unwrap().to_owned();

    let other_create = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/other-esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let other: serde_json::Value = serde_json::from_slice(
        &to_bytes(other_create.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let other_token_id = other["id"].as_str().unwrap().to_owned();

    let gateway_create = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/gateway-authorization-001/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let gateway: serde_json::Value = serde_json::from_slice(
        &to_bytes(gateway_create.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let gateway_token_id = gateway["id"].as_str().unwrap().to_owned();

    let direct_authorized = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "esp-000123",
            &direct_token_id,
        ))
        .await
        .unwrap();
    let gateway_authorized = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "gateway-authorization-001",
            &gateway_token_id,
        ))
        .await
        .unwrap();
    let wrong_token = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "esp-000123",
            &other_token_id,
        ))
        .await
        .unwrap();

    query("UPDATE devices SET gateway_device_id = $1 WHERE device_id = $2")
        .bind("gateway-authorization-001")
        .bind("esp-000123")
        .execute(&pool)
        .await
        .unwrap();
    let child_device = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "esp-000123",
            &direct_token_id,
        ))
        .await
        .unwrap();
    query("UPDATE devices SET gateway_device_id = NULL WHERE device_id = $1")
        .bind("esp-000123")
        .execute(&pool)
        .await
        .unwrap();

    let rotate = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/device-tokens/{direct_token_id}/rotate"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let rotate_status = rotate.status();
    let rotated: serde_json::Value =
        serde_json::from_slice(&to_bytes(rotate.into_body(), usize::MAX).await.unwrap()).unwrap();
    let rotated_token_id = rotated["id"].as_str().unwrap().to_owned();
    let rotated_old_token = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "esp-000123",
            &direct_token_id,
        ))
        .await
        .unwrap();
    let rotated_token = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "esp-000123",
            &rotated_token_id,
        ))
        .await
        .unwrap();
    let revoke = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/device-tokens/{rotated_token_id}/revoke"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let revoked_token = app
        .oneshot(mqttd_device_transport_session_authorization_request(
            "esp-000123",
            &rotated_token_id,
        ))
        .await
        .unwrap();

    assert_eq!(direct_create_status, StatusCode::CREATED);
    assert_eq!(direct_authorized.status(), StatusCode::NO_CONTENT);
    assert_eq!(gateway_authorized.status(), StatusCode::NO_CONTENT);
    assert_eq!(wrong_token.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(child_device.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(rotate_status, StatusCode::CREATED);
    assert_eq!(rotated_old_token.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(rotated_token.status(), StatusCode::NO_CONTENT);
    assert_eq!(revoke.status(), StatusCode::NO_CONTENT);
    assert_eq!(revoked_token.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn powermonitor_reports_gateway_and_child_health_independently() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query(
        "INSERT INTO devices (
             device_id, is_gateway, last_seen_at, gateway_device_id,
             gateway_last_read_at, gateway_read_quality
         ) VALUES (
             'gateway-001', TRUE, now(), NULL, NULL, NULL
         ), (
             'child-fresh', FALSE, NULL, 'gateway-001', now(), 'good'
         ), (
             'child-stale', FALSE, NULL, 'gateway-001',
             now() - INTERVAL '10 minutes', 'good'
         ), (
             'child-unavailable', FALSE, NULL, 'gateway-001',
             now(), 'unavailable'
         )",
    )
    .execute(&pool)
    .await
    .unwrap();
    let app = router(test_state(pool));

    let response = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/apps/powermonitor/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let devices: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let status = |device_id: &str| {
        devices
            .iter()
            .find(|device| device["device_id"] == device_id)
            .unwrap()
    };

    assert_eq!(status("gateway-001")["gateway_status"], "online");
    assert_eq!(status("child-fresh")["child_status"], "fresh");
    assert_eq!(status("child-stale")["child_status"], "stale");
    assert_eq!(status("child-unavailable")["child_status"], "unavailable");
}

#[tokio::test]
async fn powermonitor_exposes_raw_measurement_records_for_an_unprofiled_device() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let response = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri(
                    "/api/apps/powermonitor/devices/esp-000123/telemetry/records?from=2026-09-04T10%3A00%3A00Z&to=2026-09-04T11%3A00%3A00Z",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let records: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(records[0]["measurements"]["temperature_c"], 26.4);
    assert_eq!(records[0]["measurements"]["power_w"], 529.9);
}

#[tokio::test]
async fn system_configuration_requires_system_account_and_redacts_smtp_password() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let service = TestSystemConfigurationService::new();
    let app = router(
        test_state(prepared_pool().await).with_system_configuration(Arc::new(service.clone())),
    );

    let system_login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"system","password":"NanoSystem@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let system_login_body = to_bytes(system_login.into_body(), usize::MAX)
        .await
        .unwrap();
    let system_session: serde_json::Value = serde_json::from_slice(&system_login_body).unwrap();
    let system_session = system_session["session_id"].as_str().unwrap().to_owned();

    let viewer = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/system-configuration")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let admin = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/system-configuration")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/system-configuration")
                .header(header::AUTHORIZATION, format!("Session {system_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let read_body = to_bytes(read.into_body(), usize::MAX).await.unwrap();
    let read_json: serde_json::Value = serde_json::from_slice(&read_body).unwrap();
    let update = SystemConfigurationUpdate {
        smtp: SmtpConfigurationUpdate {
            enabled: true,
            host: Some("smtp.changed.test".to_owned()),
            port: 587,
            username: Some("new-alerts@example.test".to_owned()),
            password: Some("new-secret".to_owned()),
            from: Some("new-alerts@example.test".to_owned()),
            to: Some("oncall@example.test".to_owned()),
            timeout_seconds: 30,
        },
        mqtt: None,
        tuning: IngestTuning {
            retention_seconds: 172_800,
            ..IngestTuning::default()
        },
    };
    let save = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/system-configuration")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Session {system_session}"))
                .body(Body::from(serde_json::to_string(&update).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let removed_restart = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system-configuration/restart")
                .header(header::AUTHORIZATION, format!("Session {system_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(viewer.status(), StatusCode::FORBIDDEN);
    assert_eq!(admin.status(), StatusCode::FORBIDDEN);
    assert_eq!(read_json["smtp"]["password_configured"], true);
    assert!(read_json["smtp"].get("password").is_none());
    assert_eq!(save.status(), StatusCode::OK);
    assert_eq!(removed_restart.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn timescaledb_user_share_limits_powermonitor_device_visibility() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let viewer_password_hash: String =
        query("SELECT password_hash FROM users WHERE username = 'viewer'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get("password_hash")
            .unwrap();
    let alice_id = Uuid::now_v7();
    let bob_id = Uuid::now_v7();
    for (id, username) in [(alice_id, "alice"), (bob_id, "bob")] {
        query(
            "INSERT INTO users (
                id, username, password_hash, role, account_class, default_app
             ) VALUES ($1, $2, $3, 'viewer', 'user', '/apps/powermonitor')",
        )
        .bind(id)
        .bind(username)
        .bind(&viewer_password_hash)
        .execute(&pool)
        .await
        .unwrap();
        query(
            "INSERT INTO user_app_grants (user_id, app_key)
             VALUES ($1, 'powermonitor')",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    }
    query("UPDATE devices SET owner_user_id = $1 WHERE device_id = 'esp-000123'")
        .bind(alice_id)
        .execute(&pool)
        .await
        .unwrap();
    query(
        "INSERT INTO devices (device_id, display_name, owner_user_id)
         VALUES ('unrelated-device', 'Unrelated device', $1)",
    )
    .bind(alice_id)
    .execute(&pool)
    .await
    .unwrap();

    let app = router(ApiState::new(pool));
    let mut sessions = std::collections::HashMap::new();
    for username in ["alice", "bob"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "username": username,
                            "password": VIEWER_PASSWORD,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        sessions.insert(username, payload["session_id"].as_str().unwrap().to_owned());
    }
    let alice_session = sessions["alice"].as_str();
    let bob_session = sessions["bob"].as_str();

    let share = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/esp-000123/shares")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Session {alice_session}"))
                .body(Body::from(
                    json!({
                        "username": "bob",
                        "permission": "viewer",
                        "inherit_children": false,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(share.status(), StatusCode::CREATED);
    let share_body = to_bytes(share.into_body(), usize::MAX).await.unwrap();
    let share: serde_json::Value = serde_json::from_slice(&share_body).unwrap();
    let share_id = share["id"].as_str().unwrap();

    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/resource-shares/{share_id}/accept"))
                .header(header::AUTHORIZATION, format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::NO_CONTENT);

    let visible = app
        .oneshot(
            Request::builder()
                .uri("/api/apps/powermonitor/devices")
                .header(header::AUTHORIZATION, format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(visible.status(), StatusCode::OK);
    let visible_body = to_bytes(visible.into_body(), usize::MAX).await.unwrap();
    let visible: serde_json::Value = serde_json::from_slice(&visible_body).unwrap();
    assert_eq!(visible.as_array().unwrap().len(), 1);
    assert_eq!(visible[0]["device_id"], "esp-000123");
}

#[tokio::test]
async fn timescaledb_viewer_claims_an_unowned_device_with_a_one_time_code() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let viewer_id: Uuid = query("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get("id")
        .unwrap();
    let app = router(ApiState::new(pool.clone()));
    let mut sessions = std::collections::HashMap::new();
    for (username, password) in [("admin", ADMIN_PASSWORD), ("viewer", VIEWER_PASSWORD)] {
        let login = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "username": username, "password": password }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(login.status(), StatusCode::OK);
        let body = to_bytes(login.into_body(), usize::MAX).await.unwrap();
        let login: serde_json::Value = serde_json::from_slice(&body).unwrap();
        sessions.insert(username, login["session_id"].as_str().unwrap().to_owned());
    }
    let admin_session = sessions["admin"].as_str();
    let viewer_session = sessions["viewer"].as_str();

    let device = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Session {admin_session}"))
                .body(Body::from(r#"{"display_name":"Claimable meter"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(device.status(), StatusCode::CREATED);
    let device_body = to_bytes(device.into_body(), usize::MAX).await.unwrap();
    let device: serde_json::Value = serde_json::from_slice(&device_body).unwrap();
    let device_id = device["device_id"].as_str().unwrap().to_owned();

    let claim_code = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/management/devices/{device_id}/claim-code"))
                .header(header::AUTHORIZATION, format!("Session {admin_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let claim_code_status = claim_code.status();
    let claim_code_body = to_bytes(claim_code.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        claim_code_status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&claim_code_body)
    );
    let claim_code: serde_json::Value = serde_json::from_slice(&claim_code_body).unwrap();
    let claim_code = claim_code["claim_code"].as_str().unwrap();

    let claimed = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/device-claims")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Session {viewer_session}"))
                .body(Body::from(
                    json!({
                        "device_id": device_id,
                        "claim_code": claim_code,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.status(), StatusCode::NO_CONTENT);
    let owner: Uuid = query("SELECT owner_user_id FROM devices WHERE device_id = $1")
        .bind(&device_id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get("owner_user_id")
        .unwrap();
    assert_eq!(owner, viewer_id);
}

#[tokio::test]
async fn login_enforces_roles_changes_password_and_rate_limits_failures() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let login_body = to_bytes(login.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&login_body).unwrap()["role"],
        "admin"
    );

    let viewer_read = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let viewer_write = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .method("POST")
                .uri("/api/alert-rules")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"Viewer rule","device_id":"esp-000123","metric_key":"temperature_c","rule_type":"event_threshold","comparison":"gt","threshold":40,"for_seconds":0}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let viewer_change_password = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .method("PUT")
                .uri("/api/auth/password")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"current_password":"NanoView@1234","new_password":"ViewerNext#2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let expired_viewer_session = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let old_password_login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"viewer","password":"NanoView@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let new_password_login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"viewer","password":"ViewerNext#2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(viewer_read.status(), StatusCode::OK);
    assert_eq!(viewer_write.status(), StatusCode::FORBIDDEN);
    assert_eq!(viewer_change_password.status(), StatusCode::NO_CONTENT);
    assert_eq!(expired_viewer_session.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(old_password_login.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(new_password_login.status(), StatusCode::OK);

    for _ in 0..5 {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"username":"admin","password":"WrongPass#2026"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let limited = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"WrongPass#2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn device_list_returns_last_seen_device_state() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let response = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let devices: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(devices[0]["device_id"], "esp-000123");
    assert_eq!(devices[0]["display_name"], "Greenhouse sensor");
}

#[tokio::test]
async fn raw_telemetry_query_returns_points_in_the_requested_range() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));
    let response = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri(
                    "/api/devices/esp-000123/telemetry?from=2026-09-04T10%3A00%3A00Z&to=2026-09-04T11%3A00%3A00Z&bucket=raw",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let points: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(points[0]["temperature_c"], 26.4);
    assert_eq!(points[0]["humidity_pct"], 71.2);
    assert_eq!(points[0]["event_count"], 1);
}

#[tokio::test]
async fn invalid_command_request_is_rejected_before_durable_enqueue() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let app = router(test_state(pool.clone()));
    let request = with_admin_auth(Request::builder())
        .method("POST")
        .uri("/api/devices/esp-000123/commands")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"method":"reboot now","params":{}}"#))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let queued: i64 = query("SELECT COUNT(*) FROM command_outbox")
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get(0)
        .unwrap();
    assert_eq!(queued, 0);
}

#[tokio::test]
async fn admin_enqueues_a_uuid_v7_command_and_reads_its_lifecycle() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let app = router(test_state(pool.clone()));
    let before_create = Utc::now();

    let create = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"method":"sample_now","params":{"source":"dashboard"},"mode":"two_way"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let create_status = create.status();
    let create_body = to_bytes(create.into_body(), usize::MAX).await.unwrap();
    let created: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    assert_eq!(create_status, StatusCode::ACCEPTED, "{created}");
    let after_create = Utc::now();
    let command_id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let expires_at = created["expires_at"]
        .as_str()
        .unwrap()
        .parse::<chrono::DateTime<Utc>>()
        .unwrap();

    let read = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .uri(format!("/api/device-commands/{command_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let read_status = read.status();
    let read_body = to_bytes(read.into_body(), usize::MAX).await.unwrap();
    let read: serde_json::Value = serde_json::from_slice(&read_body).unwrap();
    let viewer_create = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let row = query(
        "SELECT device_id, method, params::text, mode, state, expires_at
         FROM command_outbox
         WHERE id = $1",
    )
    .bind(command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    query(
        "INSERT INTO devices (device_id, display_name, is_gateway)
         VALUES ('command-gateway', 'Command gateway', TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    query("UPDATE devices SET gateway_device_id = 'command-gateway' WHERE device_id = $1")
        .bind("esp-000123")
        .execute(&pool)
        .await
        .unwrap();
    let child_command = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"method":"reboot","params":{"delay_seconds":5}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let child_command_status = child_command.status();
    assert_eq!(child_command_status, StatusCode::ACCEPTED);
    let child_command_body = to_bytes(child_command.into_body(), usize::MAX)
        .await
        .unwrap();
    let child_command: serde_json::Value = serde_json::from_slice(&child_command_body).unwrap();
    let child_command_id = Uuid::parse_str(child_command["id"].as_str().unwrap()).unwrap();
    let child_row = query(
        "SELECT device_id, method, params::text, state
         FROM command_outbox
         WHERE id = $1",
    )
    .bind(child_command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let gateway_command = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/command-gateway/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"method":"sample_now","params":{"scope":"gateway"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let gateway_command_status = gateway_command.status();
    let gateway_command_body = to_bytes(gateway_command.into_body(), usize::MAX)
        .await
        .unwrap();
    let gateway_command: serde_json::Value = serde_json::from_slice(&gateway_command_body).unwrap();
    let gateway_command_id = Uuid::parse_str(gateway_command["id"].as_str().unwrap()).unwrap();
    let gateway_row = query(
        "SELECT device_id, method, params::text
         FROM command_outbox
         WHERE id = $1",
    )
    .bind(gateway_command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    query("UPDATE devices SET is_gateway = FALSE WHERE device_id = 'command-gateway'")
        .execute(&pool)
        .await
        .unwrap();
    let non_gateway_child_command = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    query(
        "UPDATE devices
         SET is_gateway = TRUE, deleted_at = now()
         WHERE device_id = 'command-gateway'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let inactive_gateway_child_command = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(command_id.get_version_num(), 7);
    assert_eq!(created["state"], "queued");
    assert_eq!(created["mode"], "two_way");
    assert!(expires_at >= before_create + chrono::Duration::seconds(29));
    assert!(expires_at <= after_create + chrono::Duration::seconds(31));
    assert_eq!(read_status, StatusCode::OK);
    assert_eq!(read["id"], created["id"]);
    assert_eq!(read["state"], "queued");
    assert_eq!(read["mode"], "two_way");
    assert_eq!(read["expires_at"], created["expires_at"]);
    assert_eq!(viewer_create.status(), StatusCode::FORBIDDEN);
    assert_eq!(child_command["state"], "queued");
    assert_eq!(
        child_row.try_get::<String, _>("device_id").unwrap(),
        "command-gateway"
    );
    assert_eq!(
        child_row.try_get::<String, _>("method").unwrap(),
        "gateway_child_rpc"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &child_row.try_get::<String, _>("params").unwrap()
        )
        .unwrap(),
        json!({
            "child_device_id": "esp-000123",
            "method": "reboot",
            "params": {"delay_seconds": 5}
        })
    );
    assert_eq!(child_row.try_get::<String, _>("state").unwrap(), "queued");
    assert_eq!(
        gateway_command_status,
        StatusCode::ACCEPTED,
        "{gateway_command}"
    );
    assert_eq!(
        gateway_row.try_get::<String, _>("device_id").unwrap(),
        "command-gateway"
    );
    assert_eq!(
        gateway_row.try_get::<String, _>("method").unwrap(),
        "sample_now"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &gateway_row.try_get::<String, _>("params").unwrap()
        )
        .unwrap(),
        json!({"scope": "gateway"})
    );
    assert_eq!(non_gateway_child_command.status(), StatusCode::CONFLICT);
    assert_eq!(
        inactive_gateway_child_command.status(),
        StatusCode::CONFLICT
    );
    assert_eq!(row.try_get::<String, _>("device_id").unwrap(), "esp-000123");
    assert_eq!(row.try_get::<String, _>("method").unwrap(), "sample_now");
    assert_eq!(row.try_get::<String, _>("mode").unwrap(), "two_way");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row.try_get::<String, _>("params").unwrap())
            .unwrap(),
        json!({"source":"dashboard"})
    );
    assert_eq!(row.try_get::<String, _>("state").unwrap(), "queued");
    assert_eq!(
        row.try_get::<chrono::DateTime<Utc>, _>("expires_at")
            .unwrap(),
        expires_at
    );
}

#[tokio::test]
async fn domain_app_and_management_routes_enforce_the_platform_boundary() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let current_user = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let current_user_body = to_bytes(current_user.into_body(), usize::MAX)
        .await
        .unwrap();
    let current_user_json: serde_json::Value = serde_json::from_slice(&current_user_body).unwrap();
    let management_as_viewer = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/management/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let power_monitor_as_viewer = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/apps/powermonitor/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let power_monitor_status = power_monitor_as_viewer.status();
    let management_as_admin = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/management/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(current_user_json["default_app"], "/apps/powermonitor");
    assert_eq!(current_user_json["granted_apps"], json!(["powermonitor"]));
    assert_eq!(management_as_viewer.status(), StatusCode::FORBIDDEN);
    assert_eq!(power_monitor_status, StatusCode::NOT_FOUND);
    assert_eq!(management_as_admin.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_creates_profiles_assets_and_assigns_a_device() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));

    let asset_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/profiles/asset-profiles")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"Site"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_profile_body = to_bytes(asset_profile.into_body(), usize::MAX)
        .await
        .unwrap();
    let asset_profile_json: serde_json::Value =
        serde_json::from_slice(&asset_profile_body).unwrap();
    let asset_profile_id = asset_profile_json["id"].as_str().unwrap();

    let device_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/profiles/device-profiles")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"Three phase meter"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let device_profile_body = to_bytes(device_profile.into_body(), usize::MAX)
        .await
        .unwrap();
    let device_profile_json: serde_json::Value =
        serde_json::from_slice(&device_profile_body).unwrap();
    let device_profile_id = device_profile_json["id"].as_str().unwrap();

    let asset = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/assets")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "Main panel",
                        "asset_profile_id": asset_profile_id,
                        "metadata": {"site_code": "HCM-01"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_body = to_bytes(asset.into_body(), usize::MAX).await.unwrap();
    let asset_json: serde_json::Value = serde_json::from_slice(&asset_body).unwrap();
    let asset_id = asset_json["id"].as_str().unwrap();

    let update = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri("/api/management/devices/esp-000123")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "display_name": "Main meter",
                        "asset_id": asset_id,
                        "device_profile_id": device_profile_id
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let update_body = to_bytes(update.into_body(), usize::MAX).await.unwrap();
    let update_json: serde_json::Value = serde_json::from_slice(&update_body).unwrap();
    let power_assets = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/apps/powermonitor/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let power_assets_body = to_bytes(power_assets.into_body(), usize::MAX)
        .await
        .unwrap();
    let power_assets_json: serde_json::Value = serde_json::from_slice(&power_assets_body).unwrap();

    assert_eq!(asset_profile_json["name"], "Site");
    assert_eq!(device_profile_json["name"], "Three phase meter");
    assert_eq!(asset_json["name"], "Main panel");
    assert_eq!(update_json["display_name"], "Main meter");
    assert_eq!(update_json["asset_id"], asset_id);
    assert_eq!(update_json["device_profile_id"], device_profile_id);
    assert_eq!(power_assets_json[0]["name"], "Main panel");
    assert_eq!(power_assets_json[0]["device_count"], 1);
}

#[tokio::test]
async fn admin_soft_deletes_a_device_and_revokes_its_token() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));
    let token = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let token_body = to_bytes(token.into_body(), usize::MAX).await.unwrap();
    let _token = serde_json::from_slice::<serde_json::Value>(&token_body).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_owned();
    let deleted = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("DELETE")
                .uri("/api/management/devices/esp-000123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let management_devices = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/management/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let management_devices_body = to_bytes(management_devices.into_body(), usize::MAX)
        .await
        .unwrap();
    let management_devices_json: serde_json::Value =
        serde_json::from_slice(&management_devices_body).unwrap();
    let command = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/devices/esp-000123/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let power_summary = app
        .clone()
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/apps/powermonitor/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let power_summary_body = to_bytes(power_summary.into_body(), usize::MAX)
        .await
        .unwrap();
    let power_summary_json: serde_json::Value =
        serde_json::from_slice(&power_summary_body).unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert!(management_devices_json.as_array().unwrap().is_empty());
    assert_eq!(command.status(), StatusCode::NOT_FOUND);
    assert_eq!(power_summary_json["device_count"], 0);
}

#[tokio::test]
async fn admin_edits_and_deletes_assets_and_profiles() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));
    let asset_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/profiles/asset-profiles")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"Original asset profile","fields":{"site_code":"string"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_profile_body = to_bytes(asset_profile.into_body(), usize::MAX)
        .await
        .unwrap();
    let asset_profile_id = serde_json::from_slice::<serde_json::Value>(&asset_profile_body)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let device_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/profiles/device-profiles")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"Original device profile"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let device_profile_body = to_bytes(device_profile.into_body(), usize::MAX)
        .await
        .unwrap();
    let device_profile_id = serde_json::from_slice::<serde_json::Value>(&device_profile_body)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let asset = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/assets")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "Original asset",
                        "asset_profile_id": asset_profile_id,
                        "metadata": {"site_code": "HCM-01"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_body = to_bytes(asset.into_body(), usize::MAX).await.unwrap();
    let asset_id = serde_json::from_slice::<serde_json::Value>(&asset_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let edited_asset = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri(format!("/api/management/assets/{asset_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "Edited asset",
                        "asset_profile_id": asset_profile_id,
                        "parent_asset_id": null,
                        "metadata": {"site_code": "HCM-02"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let edited_asset_status = edited_asset.status();
    assert_eq!(edited_asset_status, StatusCode::OK);
    let edited_asset_body = to_bytes(edited_asset.into_body(), usize::MAX)
        .await
        .unwrap();
    let edited_asset_json: serde_json::Value = serde_json::from_slice(&edited_asset_body).unwrap();
    let edited_device_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri(format!(
                    "/api/management/profiles/device-profiles/{device_profile_id}"
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"Edited device profile","telemetry_schema":{"voltage_v":"number"},"metric_mapping":{"power":"power_w"},"reporting_settings":{"seconds":10}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let edited_asset_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri(format!(
                    "/api/management/profiles/asset-profiles/{asset_profile_id}"
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"Edited asset profile","fields":{"site_code":"string"},"dashboard_defaults":{"range":"24h"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let deleted_asset = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("DELETE")
                .uri(format!("/api/management/assets/{asset_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let deleted_device_profile = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("DELETE")
                .uri(format!(
                    "/api/management/profiles/device-profiles/{device_profile_id}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let deleted_asset_profile = app
        .oneshot(
            with_admin_auth(Request::builder())
                .method("DELETE")
                .uri(format!(
                    "/api/management/profiles/asset-profiles/{asset_profile_id}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(edited_asset_json["name"], "Edited asset");
    assert_eq!(edited_device_profile.status(), StatusCode::OK);
    assert_eq!(edited_asset_profile.status(), StatusCode::OK);
    assert_eq!(deleted_asset.status(), StatusCode::NO_CONTENT);
    assert_eq!(deleted_device_profile.status(), StatusCode::NO_CONTENT);
    assert_eq!(deleted_asset_profile.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn admin_manages_device_and_asset_key_value_attributes() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));
    let device = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri("/api/management/devices/esp-000123")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{
                        "display_name":"Main meter",
                        "asset_id":null,
                        "device_profile_id":null,
                        "attributes":{"serial_number":"PM-001","phase":"three"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let device_body = to_bytes(device.into_body(), usize::MAX).await.unwrap();
    let device_json: serde_json::Value = serde_json::from_slice(&device_body).unwrap();
    let asset = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri("/api/management/assets")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"Main panel","attributes":{"site":"HCM","floor":"2"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_body = to_bytes(asset.into_body(), usize::MAX).await.unwrap();
    let asset_json: serde_json::Value = serde_json::from_slice(&asset_body).unwrap();
    let asset_id = asset_json["id"].as_str().unwrap();
    let updated_asset = app
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri(format!("/api/management/assets/{asset_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{
                        "name":"Main panel",
                        "asset_profile_id":null,
                        "parent_asset_id":null,
                        "attributes":{"site":"HCM","floor":"3"}
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let updated_asset_body = to_bytes(updated_asset.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated_asset_json: serde_json::Value =
        serde_json::from_slice(&updated_asset_body).unwrap();

    assert_eq!(device_json["attributes"]["serial_number"], "PM-001");
    assert_eq!(device_json["attributes"]["phase"], "three");
    assert_eq!(asset_json["attributes"]["site"], "HCM");
    assert_eq!(updated_asset_json["attributes"]["floor"], "3");
}

#[tokio::test]
async fn viewer_without_a_powermonitor_grant_is_rejected() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query(
        "DELETE FROM user_app_grants
         WHERE user_id = (SELECT id FROM users WHERE username = 'viewer')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let app = router(ApiState::new(pool));

    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"viewer","password":"NanoView@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let login_body = to_bytes(login.into_body(), usize::MAX).await.unwrap();
    let session_id =
        serde_json::from_slice::<serde_json::Value>(&login_body).unwrap()["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/apps/powermonitor/summary")
                .header(header::AUTHORIZATION, format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn revoked_app_grants_remain_revoked_after_api_bootstrap() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    query(
        "DELETE FROM user_app_grants
         WHERE user_id = (SELECT id FROM users WHERE username = 'viewer')",
    )
    .execute(&pool)
    .await
    .unwrap();
    bootstrap_users(&pool).await.unwrap();
    let app = router(ApiState::new(pool));
    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"viewer","password":"NanoView@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let login_body = to_bytes(login.into_body(), usize::MAX).await.unwrap();
    let session_id =
        serde_json::from_slice::<serde_json::Value>(&login_body).unwrap()["session_id"]
            .as_str()
            .unwrap()
            .to_owned();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/apps/powermonitor/summary")
                .header(header::AUTHORIZATION, format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn changing_user_app_access_invalidates_existing_sessions() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let app = router(test_state(prepared_pool().await));
    let update = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri("/api/management/users/viewer")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"default_app":"/apps/other","granted_apps":["other"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let existing_session = app
        .oneshot(
            with_viewer_auth(Request::builder())
                .uri("/api/apps/powermonitor/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(update.status(), StatusCode::OK);
    assert_eq!(existing_session.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn additional_viewers_can_log_in_with_their_own_app_grants() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let password_hash = query("SELECT password_hash FROM users WHERE username = 'viewer'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<String, _>("password_hash");
    let user_id = Uuid::now_v7();
    query(
        "INSERT INTO users (id, username, password_hash, role, default_app)
         VALUES ($1, 'operator', $2, 'viewer', '/apps/powermonitor')",
    )
    .bind(user_id)
    .bind(password_hash)
    .execute(&pool)
    .await
    .unwrap();
    query("INSERT INTO user_app_grants (user_id, app_key) VALUES ($1, 'powermonitor')")
        .bind(user_id)
        .execute(&pool)
        .await
        .unwrap();

    let login = router(ApiState::new(pool))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"operator","password":"NanoView@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(login.status(), StatusCode::OK);
}

#[tokio::test]
async fn powermonitor_rolls_up_descendant_assets_and_honors_telemetry_buckets() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let site_id = Uuid::now_v7();
    let panel_id = Uuid::now_v7();
    query("INSERT INTO assets (id, name) VALUES ($1, 'Factory')")
        .bind(site_id)
        .execute(&pool)
        .await
        .unwrap();
    query(
        "INSERT INTO assets (id, name, parent_asset_id)
         VALUES ($1, 'Main panel', $2)",
    )
    .bind(panel_id)
    .bind(site_id)
    .execute(&pool)
    .await
    .unwrap();
    query("UPDATE devices SET asset_id = $1 WHERE device_id = 'esp-000123'")
        .bind(panel_id)
        .execute(&pool)
        .await
        .unwrap();
    query(
        "INSERT INTO devices (device_id, display_name, asset_id)
         VALUES ('esp-000456', 'Auxiliary meter', $1)",
    )
    .bind(panel_id)
    .execute(&pool)
    .await
    .unwrap();
    query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap())
    .bind(Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 1).unwrap())
    .bind("esp-000123")
    .bind(Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap())
    .bind(1843_i64)
    .bind(sqlx::types::Json(json!({
        "voltage_v": 231.0,
        "current_a": 2.4,
        "power_w": 554.4,
        "energy_kwh": 31.3
    })))
    .bind("iot/v1/devices/esp-000123/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 0).unwrap())
    .bind(Utc.with_ymd_and_hms(2026, 9, 4, 10, 13, 1).unwrap())
    .bind("esp-000456")
    .bind(Uuid::parse_str("e6a3a190-e12e-4df9-9d75-3d1b1cba7865").unwrap())
    .bind(1_i64)
    .bind(sqlx::types::Json(json!({
        "voltage_v": 229.8,
        "current_a": 0.5,
        "power_w": 100.0,
        "energy_kwh": 7.5
    })))
    .bind("iot/v1/devices/esp-000456/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    let app = router(test_state(pool));

    let assets = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/apps/powermonitor/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let assets_status = assets.status();
    let assets_body = to_bytes(assets.into_body(), usize::MAX).await.unwrap();
    let assets_json: serde_json::Value = serde_json::from_slice(&assets_body).unwrap();
    assert_eq!(assets_status, StatusCode::OK, "{assets_json}");
    let site = assets_json
        .as_array()
        .unwrap()
        .iter()
        .find(|asset| asset["id"] == site_id.to_string())
        .unwrap();
    let bucketed = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .uri(
                    "/api/apps/powermonitor/devices/esp-000123/telemetry?from=2026-09-04T10%3A00%3A00Z&to=2026-09-04T10%3A20%3A00Z&bucket=5m",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bucketed_body = to_bytes(bucketed.into_body(), usize::MAX).await.unwrap();
    let bucketed_json: serde_json::Value = serde_json::from_slice(&bucketed_body).unwrap();
    let asset_bucketed = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .uri(format!(
                    "/api/apps/powermonitor/assets/{site_id}/telemetry?from=2026-09-04T10%3A00%3A00Z&to=2026-09-04T10%3A20%3A00Z&bucket=5m"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_bucketed_status = asset_bucketed.status();
    let asset_bucketed_body = to_bytes(asset_bucketed.into_body(), usize::MAX)
        .await
        .unwrap();
    let asset_bucketed_json: serde_json::Value =
        serde_json::from_slice(&asset_bucketed_body).unwrap();
    let asset_raw = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri(format!(
                    "/api/apps/powermonitor/assets/{site_id}/telemetry?from=2026-09-04T10%3A00%3A00Z&to=2026-09-04T10%3A20%3A00Z&bucket=raw"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_raw_body = to_bytes(asset_raw.into_body(), usize::MAX).await.unwrap();
    let asset_raw_json: serde_json::Value = serde_json::from_slice(&asset_raw_body).unwrap();

    assert_eq!(site["device_count"], 2);
    assert_eq!(site["total_power_w"], 654.4);
    assert_eq!(site["total_energy_kwh"], 38.8);
    assert_eq!(bucketed_json.as_array().unwrap().len(), 1);
    assert_eq!(bucketed_json[0]["event_count"], 2);
    assert_eq!(
        asset_bucketed_status,
        StatusCode::OK,
        "{asset_bucketed_json}"
    );
    assert_eq!(asset_bucketed_json.as_array().unwrap().len(), 1);
    assert_eq!(asset_bucketed_json[0]["power_w"], 642.15);
    assert_eq!(asset_bucketed_json[0]["energy_kwh"], 38.8);
    assert_eq!(asset_bucketed_json[0]["event_count"], 3);
    assert_eq!(asset_raw_json.as_array().unwrap().len(), 2);
    assert_eq!(asset_raw_json[1]["power_w"], 654.4);
    assert_eq!(asset_raw_json[1]["energy_kwh"], 38.8);
}

#[tokio::test]
async fn creates_and_lists_a_window_alert_rule() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let request = with_admin_auth(Request::builder())
        .method("POST")
        .uri("/api/alert-rules")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "name": "High average",
                "metric_key": "temperature_c",
                "rule_type": "window_average",
                "comparison": "gt",
                "threshold": 40.0,
                "window_seconds": 300
            })
            .to_string(),
        ))
        .unwrap();

    let create_response = router(test_state(pool.clone()))
        .oneshot(request)
        .await
        .unwrap();
    let list_response = router(test_state(pool))
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/alert-rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let rules: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(create_response.status(), StatusCode::CREATED);
    assert_eq!(rules[0]["name"], "High average");
    assert_eq!(rules[0]["rule_type"], "window_average");
    assert_eq!(rules[0]["window_seconds"], 300);
}

#[tokio::test]
async fn toggles_rule_and_acknowledges_incident() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = Uuid::new_v4();
    let incident_id = Uuid::new_v4();
    query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds
         ) VALUES ($1, 'High temperature', 'temperature_c', 'event_threshold', 'gt', 40.0,
                   0, 300, 3600, 'warning', 86400)",
    )
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();
    query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, state_version
         ) VALUES ($1, $2, 'esp-000123', 'open', now(), now(), 1)",
    )
    .bind(incident_id)
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();

    let toggle = router(test_state(pool.clone()))
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/alert-rules/{rule_id}/toggle"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"enabled":false}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let acknowledge = router(test_state(pool))
        .oneshot(
            with_admin_auth(Request::builder())
                .method("POST")
                .uri(format!("/api/alert-incidents/{incident_id}/acknowledge"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let acknowledge_body = to_bytes(acknowledge.into_body(), usize::MAX).await.unwrap();
    let incident: serde_json::Value = serde_json::from_slice(&acknowledge_body).unwrap();

    assert_eq!(toggle.status(), StatusCode::OK);
    assert_eq!(incident["status"], "open");
    assert_eq!(incident["acknowledged_by"], "dashboard");
}

#[tokio::test]
async fn admin_updates_and_archives_rules_with_active_incidents() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = Uuid::new_v4();
    let incident_id = Uuid::new_v4();
    query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds
         ) VALUES ($1, 'High temperature', 'temperature_c', 'event_threshold', 'gt', 40.0,
                   0, 300, 3600, 'warning', 86400)",
    )
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();
    query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, last_value,
            state_version
         ) VALUES ($1, $2, 'esp-000123', 'open', now(), now(), 41.5, 1)",
    )
    .bind(incident_id)
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();
    let app = router(test_state(pool.clone()));

    let update = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("PUT")
                .uri(format!("/api/alert-rules/{rule_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "Updated temperature",
                        "metric_key": "temperature_c",
                        "rule_type": "event_threshold",
                        "comparison": "gt",
                        "threshold": 42.0
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);
    let update_body = to_bytes(update.into_body(), usize::MAX).await.unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&update_body).unwrap();

    let archive = app
        .clone()
        .oneshot(
            with_admin_auth(Request::builder())
                .method("DELETE")
                .uri(format!("/api/alert-rules/{rule_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list = app
        .oneshot(
            with_admin_auth(Request::builder())
                .uri("/api/alert-rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list_body = to_bytes(list.into_body(), usize::MAX).await.unwrap();
    let active_rules: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let incident = query(
        "SELECT status, resolved_at, state_version
         FROM alert_incidents
         WHERE id = $1",
    )
    .bind(incident_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let notification = query(
        "SELECT kind, dedupe_key, body
         FROM notification_outbox
         WHERE incident_id = $1",
    )
    .bind(incident_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(updated["threshold"], 42.0);
    assert_eq!(updated["enabled"], true);
    assert_eq!(archive.status(), StatusCode::NO_CONTENT);
    assert_eq!(active_rules, json!([]));
    assert_eq!(incident.get::<String, _>("status"), "resolved");
    assert!(
        incident
            .get::<Option<chrono::DateTime<Utc>>, _>("resolved_at")
            .is_some()
    );
    assert_eq!(incident.get::<i32, _>("state_version"), 2);
    assert_eq!(notification.get::<String, _>("kind"), "resolved");
    assert_eq!(
        notification.get::<String, _>("dedupe_key"),
        format!("incident:{incident_id}:resolved:2")
    );
    assert!(
        notification
            .get::<String, _>("body")
            .contains("Rule: Updated temperature")
    );
}
