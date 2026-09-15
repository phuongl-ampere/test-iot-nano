use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{Duration, TimeZone, Utc};
use iot_api::{
    MqttdDeviceTransportSessionRevocation, MqttdDeviceTransportSessionRevoker,
    MqttdDeviceTransportSessionRevokerError, Role, SqliteApiState,
    bootstrap_power_switcher_profile_sqlite, bootstrap_users_sqlite, sqlite_router,
};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use iot_storage::{NewCommandOutboxEntry, SqliteStore};
use serde_json::json;
use sqlx::Row;
use tower::ServiceExt;

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

fn mqttd_device_transport_session_authorization_request(
    device_id: &str,
    token_id: &str,
) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/internal/mqttd/session-authorization")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-iot-nano-mqttd-api-secret", MQTTD_API_SECRET)
        .body(Body::from(
            json!({
                "device_id": device_id,
                "token_id": token_id,
            })
            .to_string(),
        ))
        .unwrap()
}

fn gateway_authorization_request(
    gateway_device_id: &str,
    token_id: &str,
    child_device_id: Option<&str>,
    topic: &str,
    event_kind: &str,
) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/internal/mqttd/gateway-authorization")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-iot-nano-mqttd-api-secret", MQTTD_API_SECRET)
        .body(Body::from(
            json!({
                "gateway_device_id": gateway_device_id,
                "token_id": token_id,
                "child_device_id": child_device_id,
                "topic": topic,
                "event_kind": event_kind,
            })
            .to_string(),
        ))
        .unwrap()
}

#[tokio::test]
async fn sqlite_bootstrap_creates_system_admin_and_user_account_classes() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();

    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT username, role, account_class FROM users ORDER BY username")
            .fetch_all(store.pool())
            .await
            .unwrap();

    assert_eq!(
        rows,
        [
            (
                "admin".to_owned(),
                Role::Admin.as_str().to_owned(),
                "admin".to_owned()
            ),
            (
                "system".to_owned(),
                Role::Admin.as_str().to_owned(),
                "system".to_owned()
            ),
            (
                "viewer".to_owned(),
                Role::Viewer.as_str().to_owned(),
                "user".to_owned()
            )
        ]
    );
}

#[tokio::test]
async fn sqlite_bootstrap_adds_the_system_account_to_an_existing_user_database() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query("DELETE FROM users WHERE username = 'system'")
        .execute(store.pool())
        .await
        .unwrap();

    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let account_class: String =
        sqlx::query_scalar("SELECT account_class FROM users WHERE username = 'system'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(account_class, "system");
}

#[tokio::test]
async fn sqlite_bootstrap_installs_one_power_switcher_profile() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();

    bootstrap_power_switcher_profile_sqlite(store.pool())
        .await
        .unwrap();
    bootstrap_power_switcher_profile_sqlite(store.pool())
        .await
        .unwrap();

    let rows = sqlx::query(
        "SELECT name, telemetry_schema, metric_mapping, reporting_settings
         FROM device_profiles
         WHERE name = 'PowerSwitcher'",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &rows[0].try_get::<String, _>("telemetry_schema").unwrap()
        )
        .unwrap()["switch_state"]["type"],
        "boolean"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &rows[0].try_get::<String, _>("reporting_settings").unwrap()
        )
        .unwrap()["rpc"]["methods"],
        json!(["switch_on", "switch_off", "set_power"])
    );
}

#[tokio::test]
async fn sqlite_powermonitor_exposes_power_switcher_profile_and_latest_switch_state() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    bootstrap_power_switcher_profile_sqlite(store.pool())
        .await
        .unwrap();
    let profile_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM device_profiles WHERE name = 'PowerSwitcher'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id)
         VALUES ('switcher-1', 'Pump relay', ?)",
    )
    .bind(profile_id)
    .execute(store.pool())
    .await
    .unwrap();
    let at = Utc::now();
    store
        .write_telemetry(
            &TelemetryEvent {
                schema_version: 1,
                device_id: "switcher-1".to_owned(),
                boot_id: uuid::Uuid::new_v4(),
                sequence: 1,
                event_at: at,
                measurements: serde_json::Map::from_iter([
                    ("switch_state".to_owned(), json!(true)),
                    ("power_w".to_owned(), json!(552.0)),
                ]),
                gateway_device_id: None,
            },
            at,
            "v1/devices/me/telemetry",
        )
        .await
        .unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let response = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/devices")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1[0]["device_profile_name"], "PowerSwitcher");
    assert_eq!(response.1[0]["switch_state"], true);
}

#[tokio::test]
async fn sqlite_router_authenticates_the_bootstrapped_admin() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));

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
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let session: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(session["role"], "admin");
    assert!(session["session_id"].as_str().is_some());
}

#[tokio::test]
async fn sqlite_user_owned_resources_require_an_accepted_share_and_respect_permission() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let viewer_password_hash: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'viewer'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    let alice_id = uuid::Uuid::now_v7().to_string();
    let bob_id = uuid::Uuid::now_v7().to_string();
    for (id, username) in [(&alice_id, "alice"), (&bob_id, "bob")] {
        sqlx::query(
            "INSERT INTO users (
                id, username, password_hash, role, account_class, default_app
             ) VALUES (?, ?, ?, 'viewer', 'user', '/apps/powermonitor')",
        )
        .bind(id)
        .bind(username)
        .bind(&viewer_password_hash)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, app_key)
             VALUES (?, 'powermonitor')",
        )
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    }

    let root_asset_id = uuid::Uuid::now_v7().to_string();
    let child_asset_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO assets (id, name, owner_user_id)
         VALUES (?, 'Root asset', ?), (?, 'Child asset', ?)",
    )
    .bind(&root_asset_id)
    .bind(&alice_id)
    .bind(&child_asset_id)
    .bind(&alice_id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE assets SET parent_asset_id = ? WHERE id = ?")
        .bind(&root_asset_id)
        .bind(&child_asset_id)
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, owner_user_id, asset_id)
         VALUES ('alice-meter', 'Alice meter', ?, ?),
                ('unrelated-meter', 'Unrelated meter', ?, NULL)",
    )
    .bind(&alice_id)
    .bind(&child_asset_id)
    .bind(&alice_id)
    .execute(store.pool())
    .await
    .unwrap();

    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let alice_session = sqlite_session_id(&app, "alice", "NanoView@1234").await;
    let bob_session = sqlite_session_id(&app, "bob", "NanoView@1234").await;

    let direct_share = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/devices/alice-meter/shares")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {alice_session}"))
            .body(Body::from(
                json!({
                    "username": "bob",
                    "permission": "viewer",
                    "inherit_children": false
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(direct_share.0, StatusCode::CREATED);
    let direct_share_id = direct_share.1["id"].as_str().unwrap().to_owned();
    let audit_action: String =
        sqlx::query_scalar("SELECT action FROM audit_events ORDER BY created_at DESC LIMIT 1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(audit_action, "resource_share.created");

    let pending = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/me/resource-shares?state=pending")
            .header("authorization", format!("Session {bob_session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(pending.0, StatusCode::OK);
    assert_eq!(pending.1[0]["id"], direct_share_id);

    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/resource-shares/{direct_share_id}/accept"))
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::NO_CONTENT);
    let audit_action: String =
        sqlx::query_scalar("SELECT action FROM audit_events ORDER BY created_at DESC LIMIT 1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(audit_action, "resource_share.accepted");

    let visible_devices = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/devices")
            .header("authorization", format!("Session {bob_session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(visible_devices.0, StatusCode::OK);
    assert_eq!(visible_devices.1.as_array().unwrap().len(), 1);
    assert_eq!(visible_devices.1[0]["device_id"], "alice-meter");

    let generic_devices = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/devices")
            .header("authorization", format!("Session {bob_session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(generic_devices.0, StatusCode::OK);
    assert_eq!(generic_devices.1.as_array().unwrap().len(), 1);
    assert_eq!(generic_devices.1[0]["device_id"], "alice-meter");

    let visible_assets = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/assets")
            .header("authorization", format!("Session {bob_session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(visible_assets.0, StatusCode::OK, "{:?}", visible_assets.1);
    assert!(visible_assets.1.as_array().unwrap().is_empty());

    let summary = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/summary")
            .header("authorization", format!("Session {bob_session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(summary.0, StatusCode::OK);
    assert_eq!(summary.1["device_count"], 1);
    assert_eq!(summary.1["asset_count"], 0);

    let unrelated_telemetry = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(
                    "/api/apps/powermonitor/devices/unrelated-meter/telemetry?from=2026-09-01T00%3A00%3A00Z&to=2026-09-01T00%3A01%3A00Z&bucket=raw",
                )
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unrelated_telemetry.status(), StatusCode::FORBIDDEN);

    let viewer_command = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/alice-meter/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::from(
                    json!({
                        "method": "switch_on",
                        "params": {},
                        "mode": "two_way"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(viewer_command.status(), StatusCode::FORBIDDEN);

    let cancelled = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/resource-shares/{direct_share_id}"))
                .header("authorization", format!("Session {alice_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);

    let controller_share = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/devices/alice-meter/shares")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {alice_session}"))
            .body(Body::from(
                json!({
                    "username": "bob",
                    "permission": "controller",
                    "inherit_children": false
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(controller_share.0, StatusCode::CREATED);
    let controller_share_id = controller_share.1["id"].as_str().unwrap().to_owned();
    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/resource-shares/{controller_share_id}/accept"))
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::NO_CONTENT);

    let controller_command = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/alice-meter/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::from(
                    json!({
                        "method": "switch_on",
                        "params": {},
                        "mode": "two_way"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(controller_command.status(), StatusCode::ACCEPTED);

    let controller_tokens = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/devices/alice-meter/tokens")
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(controller_tokens.status(), StatusCode::FORBIDDEN);

    let cancelled = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/resource-shares/{controller_share_id}"))
                .header("authorization", format!("Session {alice_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
    let manager_share = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/devices/alice-meter/shares")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {alice_session}"))
            .body(Body::from(
                json!({
                    "username": "bob",
                    "permission": "manager",
                    "inherit_children": false
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(manager_share.0, StatusCode::CREATED);
    let manager_share_id = manager_share.1["id"].as_str().unwrap().to_owned();
    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/resource-shares/{manager_share_id}/accept"))
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::NO_CONTENT);
    let manager_tokens = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/devices/alice-meter/tokens")
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(manager_tokens.status(), StatusCode::OK);
    let manager_rule = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/alert-rules")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::from(
                    json!({
                        "name": "Alice meter high temperature",
                        "device_id": "alice-meter",
                        "metric_key": "temperature_c",
                        "rule_type": "event_threshold",
                        "comparison": "gt",
                        "threshold": 40.0,
                        "for_seconds": 0,
                        "severity": "warning"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(manager_rule.status(), StatusCode::CREATED);
    let manager_rule_body = to_bytes(manager_rule.into_body(), usize::MAX)
        .await
        .unwrap();
    let manager_rule: serde_json::Value = serde_json::from_slice(&manager_rule_body).unwrap();
    let manager_rule_id = manager_rule["id"].as_str().unwrap();
    let updated_manager_rule = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/alert-rules/{manager_rule_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::from(
                    json!({
                        "name": "Alice meter critical temperature",
                        "device_id": "alice-meter",
                        "metric_key": "temperature_c",
                        "rule_type": "event_threshold",
                        "comparison": "gt",
                        "threshold": 45.0,
                        "for_seconds": 0,
                        "severity": "critical"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(updated_manager_rule.status(), StatusCode::OK);

    let inherited_share = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/api/assets/{root_asset_id}/shares"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {alice_session}"))
            .body(Body::from(
                json!({
                    "username": "bob",
                    "permission": "viewer",
                    "inherit_children": true
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(inherited_share.0, StatusCode::CREATED);
    let inherited_share_id = inherited_share.1["id"].as_str().unwrap().to_owned();
    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/resource-shares/{inherited_share_id}/accept"))
                .header("authorization", format!("Session {bob_session}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::NO_CONTENT);

    let inherited_permission: String = sqlx::query_scalar(
        "WITH RECURSIVE ancestors(id) AS (
            SELECT asset_id FROM devices WHERE device_id = 'alice-meter'
            UNION ALL
            SELECT assets.parent_asset_id
            FROM assets
            JOIN ancestors ON assets.id = ancestors.id
            WHERE assets.parent_asset_id IS NOT NULL
        )
        SELECT permission
        FROM resource_shares
        WHERE resource_type = 'asset'
          AND resource_id IN (SELECT id FROM ancestors)
          AND target_user_id = ?
          AND state = 'active'
          AND inherit_children = 1",
    )
    .bind(&bob_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(inherited_permission, "viewer");
}

#[tokio::test]
async fn sqlite_user_can_provision_owned_assets_and_devices() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let viewer_password_hash: String =
        sqlx::query_scalar("SELECT password_hash FROM users WHERE username = 'viewer'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    let alice_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app
         ) VALUES (?, 'alice', ?, 'viewer', 'user', '/apps/powermonitor')",
    )
    .bind(&alice_id)
    .bind(viewer_password_hash)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO user_app_grants (user_id, app_key)
         VALUES (?, 'powermonitor')",
    )
    .bind(&alice_id)
    .execute(store.pool())
    .await
    .unwrap();

    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "alice", "NanoView@1234").await;
    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/my/assets")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"name":"Alice asset"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::CREATED);
    let asset_id = asset.1["id"].as_str().unwrap().to_owned();
    let second_asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/my/assets")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"name":"Alice second asset"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(second_asset.0, StatusCode::CREATED);
    let second_asset_id = second_asset.1["id"].as_str().unwrap().to_owned();

    let device = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/my/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "display_name": "Alice meter",
                    "asset_id": asset_id,
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(device.0, StatusCode::CREATED);
    assert!(device.1["token"].as_str().is_some());
    let device_id = device.1["device_id"].as_str().unwrap();
    let owner: String = sqlx::query_scalar("SELECT owner_user_id FROM devices WHERE device_id = ?")
        .bind(device_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let claimed_at: Option<String> =
        sqlx::query_scalar("SELECT claimed_at FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(owner, alice_id);
    assert!(claimed_at.is_some());

    let reassigned = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/my/devices/{device_id}/asset"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::from(
                    json!({ "asset_id": second_asset_id }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reassigned.status(), StatusCode::NO_CONTENT);
    let assigned_asset: Option<String> =
        sqlx::query_scalar("SELECT asset_id FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(assigned_asset.as_deref(), Some(second_asset_id.as_str()));
}

#[tokio::test]
async fn sqlite_command_lifecycle_enqueues_only_active_direct_devices() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, is_gateway, gateway_device_id)
         VALUES
            ('sqlite-command-device', 'Direct device', 0, NULL),
            ('sqlite-command-gateway', 'Gateway', 1, NULL),
            ('sqlite-command-child', 'Child device', 0, 'sqlite-command-gateway')",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let admin_session = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let viewer_session = sqlite_session_id(&app, "viewer", "NanoView@1234").await;
    let before_create = Utc::now();

    let created = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/devices/sqlite-command-device/commands")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {admin_session}"))
            .body(Body::from(
                r#"{"command":"sample_now","parameters":{"source":"dashboard"}}"#,
            ))
            .unwrap(),
    )
    .await;
    let after_create = Utc::now();
    let command_id = uuid::Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    let expires_at = created.1["expires_at"]
        .as_str()
        .unwrap()
        .parse::<chrono::DateTime<Utc>>()
        .unwrap();
    let lifecycle = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!("/api/device-commands/{command_id}"))
            .header("authorization", format!("Session {admin_session}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let viewer_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-command-device/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {viewer_session}"))
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let child = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-command-child/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {admin_session}"))
                .body(Body::from(
                    r#"{"method":"reboot","params":{"delay_seconds":5}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let child_status = child.status();
    assert_eq!(child_status, StatusCode::ACCEPTED);
    let child_body = to_bytes(child.into_body(), usize::MAX).await.unwrap();
    let child: serde_json::Value = serde_json::from_slice(&child_body).unwrap();
    let child_id = uuid::Uuid::parse_str(child["id"].as_str().unwrap()).unwrap();
    let child_row = sqlx::query(
        "SELECT device_id, method, params, state
         FROM command_outbox
         WHERE id = ?",
    )
    .bind(child_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let gateway = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-command-gateway/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {admin_session}"))
                .body(Body::from(
                    r#"{"method":"sample_now","params":{"scope":"gateway"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let gateway_status = gateway.status();
    let gateway_body = to_bytes(gateway.into_body(), usize::MAX).await.unwrap();
    let gateway: serde_json::Value = serde_json::from_slice(&gateway_body).unwrap();
    let gateway_id = uuid::Uuid::parse_str(gateway["id"].as_str().unwrap()).unwrap();
    let gateway_row = sqlx::query(
        "SELECT device_id, method, params
         FROM command_outbox
         WHERE id = ?",
    )
    .bind(gateway_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET is_gateway = 0 WHERE device_id = ?")
        .bind("sqlite-command-gateway")
        .execute(store.pool())
        .await
        .unwrap();
    let non_gateway_child = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-command-child/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {admin_session}"))
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE devices
         SET is_gateway = 1, deleted_at = ?
         WHERE device_id = ?",
    )
    .bind(Utc::now().to_rfc3339())
    .bind("sqlite-command-gateway")
    .execute(store.pool())
    .await
    .unwrap();
    let inactive_gateway_child = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-command-child/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {admin_session}"))
                .body(Body::from(r#"{"method":"sample_now","params":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let invalid_params = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-command-device/commands")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {admin_session}"))
                .body(Body::from(r#"{"method":"sample_now","params":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let row = sqlx::query(
        "SELECT device_id, method, params, state, expires_at
         FROM command_outbox
         WHERE id = ?",
    )
    .bind(command_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(created.0, StatusCode::ACCEPTED);
    assert_eq!(command_id.get_version_num(), 7);
    assert_eq!(created.1["state"], "queued");
    assert!(expires_at >= before_create + chrono::Duration::seconds(29));
    assert!(expires_at <= after_create + chrono::Duration::seconds(31));
    assert_eq!(lifecycle.0, StatusCode::OK);
    assert_eq!(lifecycle.1["id"], created.1["id"]);
    assert_eq!(lifecycle.1["state"], "queued");
    assert_eq!(lifecycle.1["expires_at"], created.1["expires_at"]);
    assert_eq!(viewer_create.status(), StatusCode::FORBIDDEN);
    assert_eq!(child["state"], "queued");
    assert_eq!(
        child_row.try_get::<String, _>("device_id").unwrap(),
        "sqlite-command-gateway"
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
            "child_device_id": "sqlite-command-child",
            "method": "reboot",
            "params": {"delay_seconds": 5}
        })
    );
    assert_eq!(child_row.try_get::<String, _>("state").unwrap(), "queued");
    assert_eq!(gateway_status, StatusCode::ACCEPTED, "{gateway}");
    assert_eq!(
        gateway_row.try_get::<String, _>("device_id").unwrap(),
        "sqlite-command-gateway"
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
    assert_eq!(non_gateway_child.status(), StatusCode::CONFLICT);
    assert_eq!(inactive_gateway_child.status(), StatusCode::CONFLICT);
    assert_eq!(invalid_params.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        row.try_get::<String, _>("device_id").unwrap(),
        "sqlite-command-device"
    );
    assert_eq!(row.try_get::<String, _>("method").unwrap(), "sample_now");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row.try_get::<String, _>("params").unwrap())
            .unwrap(),
        json!({"source":"dashboard"})
    );
    assert_eq!(row.try_get::<String, _>("state").unwrap(), "queued");
    assert_eq!(
        row.try_get::<String, _>("expires_at")
            .unwrap()
            .parse::<chrono::DateTime<Utc>>()
            .unwrap(),
        expires_at
    );
}

#[tokio::test]
async fn sqlite_transport_records_a_two_way_response_once_for_the_active_token() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('rpc-device')")
        .execute(store.pool())
        .await
        .unwrap();
    let token_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'rpc-device', 'iotd_rpc_response', 'unused-in-api-test')",
    )
    .bind(token_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    let command_id = uuid::Uuid::now_v7();
    let issued_at = Utc::now();
    store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            device_id: "rpc-device".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::TwoWay,
            expires_at: issued_at + Duration::seconds(30),
            next_attempt_at: issued_at,
        })
        .await
        .unwrap();
    store
        .claim_commands(issued_at, issued_at + Duration::seconds(30), 1)
        .await
        .unwrap();
    store
        .mark_command_published(&command_id.to_string(), issued_at + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    let app = sqlite_router(
        SqliteApiState::new(store.clone()).with_mqttd_device_transport_secret(MQTTD_API_SECRET),
    );
    let request = |response_token_id| {
        Request::builder()
            .method("POST")
            .uri("/internal/mqttd/rpc-response")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-iot-nano-mqttd-api-secret", MQTTD_API_SECRET)
            .body(Body::from(
                json!({
                    "command_id": command_id,
                    "device_id": "rpc-device",
                    "token_id": response_token_id,
                    "response": { "ok": true, "sampled_at": "2026-09-08T07:00:00Z" },
                })
                .to_string(),
            ))
            .unwrap()
    };

    let wrong_token = app
        .clone()
        .oneshot(request(uuid::Uuid::now_v7()))
        .await
        .unwrap();
    let first = app.clone().oneshot(request(token_id)).await.unwrap();
    let duplicate = app.oneshot(request(token_id)).await.unwrap();
    let row = sqlx::query(
        "SELECT state, response, responded_at
         FROM command_outbox
         WHERE id = ?",
    )
    .bind(command_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(wrong_token.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    assert_eq!(duplicate.status(), StatusCode::NO_CONTENT);
    assert_eq!(row.try_get::<String, _>("state").unwrap(), "responded");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&row.try_get::<String, _>("response").unwrap())
            .unwrap(),
        json!({ "ok": true, "sampled_at": "2026-09-08T07:00:00Z" })
    );
    assert!(
        row.try_get::<Option<String>, _>("responded_at")
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn sqlite_mqttd_device_transport_resolves_an_active_gateway_token() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, is_gateway)
         VALUES (?, ?, ?)",
    )
    .bind("gateway-001")
    .bind("Field gateway")
    .bind(1_i64)
    .execute(store.pool())
    .await
    .unwrap();
    let app = sqlite_router(
        SqliteApiState::new(store).with_mqttd_device_transport_secret(MQTTD_API_SECRET),
    );
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/gateway-001/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(create.into_body(), usize::MAX).await.unwrap()).unwrap();
    let token = created["token"].as_str().unwrap();
    let token_id = created["id"].as_str().unwrap();

    let resolution = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/session-resolution")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-iot-nano-mqttd-api-secret", MQTTD_API_SECRET)
                .body(Body::from(
                    json!({
                        "client_id": "transport-gateway-001",
                        "username": token,
                        "password": ""
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let resolution_status = resolution.status();
    let resolution_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resolution.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let missing_client_id = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/session-resolution")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-iot-nano-mqttd-api-secret", MQTTD_API_SECRET)
                .body(Body::from(
                    json!({
                        "client_id": "",
                        "username": token,
                        "password": ""
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resolution_status, StatusCode::OK);
    assert_eq!(
        resolution_body,
        json!({
            "device_id": "gateway-001",
            "token_id": token_id,
            "is_gateway": true
        })
    );
    assert_eq!(missing_client_id.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sqlite_mqttd_device_transport_authorizes_only_the_exact_active_direct_or_gateway_token() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, is_gateway)
         VALUES
            ('sqlite-authorization-device', 'Direct device', 0),
            ('sqlite-authorization-other', 'Other device', 0),
            ('sqlite-authorization-gateway', 'Field gateway', 1)",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let app = sqlite_router(
        SqliteApiState::new(store.clone()).with_mqttd_device_transport_secret(MQTTD_API_SECRET),
    );
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let direct_create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-authorization-device/tokens")
                .header("authorization", format!("Session {session_id}"))
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
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-authorization-other/tokens")
                .header("authorization", format!("Session {session_id}"))
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
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-authorization-gateway/tokens")
                .header("authorization", format!("Session {session_id}"))
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
            "sqlite-authorization-device",
            &direct_token_id,
        ))
        .await
        .unwrap();
    let gateway_authorized = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "sqlite-authorization-gateway",
            &gateway_token_id,
        ))
        .await
        .unwrap();
    let wrong_token = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "sqlite-authorization-device",
            &other_token_id,
        ))
        .await
        .unwrap();

    sqlx::query("UPDATE devices SET gateway_device_id = ? WHERE device_id = ?")
        .bind("sqlite-authorization-gateway")
        .bind("sqlite-authorization-device")
        .execute(store.pool())
        .await
        .unwrap();
    let child_device = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "sqlite-authorization-device",
            &direct_token_id,
        ))
        .await
        .unwrap();
    let gateway_heartbeat = app
        .clone()
        .oneshot(gateway_authorization_request(
            "sqlite-authorization-gateway",
            &gateway_token_id,
            None,
            "v1/gateways/me/telemetry",
            "heartbeat",
        ))
        .await
        .unwrap();
    let gateway_heartbeat_status = gateway_heartbeat.status();
    let gateway_heartbeat_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(gateway_heartbeat.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap_or_default();
    let gateway_child = app
        .clone()
        .oneshot(gateway_authorization_request(
            "sqlite-authorization-gateway",
            &gateway_token_id,
            Some("sqlite-authorization-device"),
            "v1/gateways/me/telemetry",
            "child_telemetry",
        ))
        .await
        .unwrap();
    let gateway_child_status = gateway_child.status();
    let gateway_child_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(gateway_child.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap_or_default();
    let foreign_child = app
        .clone()
        .oneshot(gateway_authorization_request(
            "sqlite-authorization-gateway",
            &gateway_token_id,
            Some("sqlite-authorization-other"),
            "v1/gateways/me/telemetry",
            "child_telemetry",
        ))
        .await
        .unwrap();
    let invalid_topic = app
        .clone()
        .oneshot(gateway_authorization_request(
            "sqlite-authorization-gateway",
            &gateway_token_id,
            None,
            "v1/devices/me/telemetry",
            "heartbeat",
        ))
        .await
        .unwrap();
    sqlx::query("UPDATE devices SET gateway_device_id = NULL WHERE device_id = ?")
        .bind("sqlite-authorization-device")
        .execute(store.pool())
        .await
        .unwrap();

    let rotate = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/device-tokens/{direct_token_id}/rotate"))
                .header("authorization", format!("Session {session_id}"))
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
            "sqlite-authorization-device",
            &direct_token_id,
        ))
        .await
        .unwrap();
    let rotated_token = app
        .clone()
        .oneshot(mqttd_device_transport_session_authorization_request(
            "sqlite-authorization-device",
            &rotated_token_id,
        ))
        .await
        .unwrap();
    let revoke = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/device-tokens/{rotated_token_id}/revoke"))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let revoked_token = app
        .oneshot(mqttd_device_transport_session_authorization_request(
            "sqlite-authorization-device",
            &rotated_token_id,
        ))
        .await
        .unwrap();

    assert_eq!(direct_create_status, StatusCode::CREATED);
    assert_eq!(direct_authorized.status(), StatusCode::NO_CONTENT);
    assert_eq!(gateway_authorized.status(), StatusCode::NO_CONTENT);
    assert_eq!(wrong_token.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(child_device.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(gateway_heartbeat_status, StatusCode::OK);
    assert_eq!(
        gateway_heartbeat_body["gateway_device_id"],
        "sqlite-authorization-gateway"
    );
    assert_eq!(gateway_heartbeat_body["event_kind"], "heartbeat");
    assert!(gateway_heartbeat_body["child_device_id"].is_null());
    assert_eq!(gateway_child_status, StatusCode::OK);
    assert_eq!(
        gateway_child_body["child_device_id"],
        "sqlite-authorization-device"
    );
    assert_eq!(gateway_child_body["event_kind"], "child_telemetry");
    assert_eq!(foreign_child.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(invalid_topic.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(rotate_status, StatusCode::CREATED);
    assert_eq!(rotated_old_token.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(rotated_token.status(), StatusCode::NO_CONTENT);
    assert_eq!(revoke.status(), StatusCode::NO_CONTENT);
    assert_eq!(revoked_token.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn sqlite_token_mutations_revoke_only_the_prior_transport_session() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name)
         VALUES ('sqlite-revocation-device', 'Device'), ('sqlite-revocation-other', 'Other')",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let revoker = RecordingSessionRevoker::default();
    let app = sqlite_router(
        SqliteApiState::new(store.clone())
            .with_mqttd_device_transport_session_revoker(revoker.clone()),
    );
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let other = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-revocation-other/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let other: serde_json::Value =
        serde_json::from_slice(&to_bytes(other.into_body(), usize::MAX).await.unwrap()).unwrap();
    let other_token_id = uuid::Uuid::parse_str(other["id"].as_str().unwrap()).unwrap();

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-revocation-device/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let first: serde_json::Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let first_token_id = uuid::Uuid::parse_str(first["id"].as_str().unwrap()).unwrap();

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-revocation-device/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::CREATED);
    let second: serde_json::Value =
        serde_json::from_slice(&to_bytes(second.into_body(), usize::MAX).await.unwrap()).unwrap();
    let second_token_id = uuid::Uuid::parse_str(second["id"].as_str().unwrap()).unwrap();

    let third = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/device-tokens/{second_token_id}/rotate"))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(third.status(), StatusCode::CREATED);
    let third: serde_json::Value =
        serde_json::from_slice(&to_bytes(third.into_body(), usize::MAX).await.unwrap()).unwrap();
    let third_token_id = uuid::Uuid::parse_str(third["id"].as_str().unwrap()).unwrap();

    let revoke = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/device-tokens/{third_token_id}/revoke"))
                .header("authorization", format!("Session {session_id}"))
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
                device_id: "sqlite-revocation-device".to_owned(),
                token_id: first_token_id,
            },
            MqttdDeviceTransportSessionRevocation {
                device_id: "sqlite-revocation-device".to_owned(),
                token_id: second_token_id,
            },
            MqttdDeviceTransportSessionRevocation {
                device_id: "sqlite-revocation-device".to_owned(),
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
async fn sqlite_device_token_history_never_discloses_raw_tokens() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id, display_name) VALUES ('token-history', 'Device')")
        .execute(store.pool())
        .await
        .unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let issued = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/token-history/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(issued.status(), StatusCode::CREATED);
    let issued: serde_json::Value =
        serde_json::from_slice(&to_bytes(issued.into_body(), usize::MAX).await.unwrap()).unwrap();
    let raw_token = issued["token"].as_str().unwrap().to_owned();
    let token_prefix = issued["token_prefix"].as_str().unwrap().to_owned();

    let history = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/devices/token-history/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(history.status(), StatusCode::OK);
    let history: serde_json::Value =
        serde_json::from_slice(&to_bytes(history.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(history.as_array().unwrap().len(), 1);
    assert_eq!(history[0]["token_prefix"], token_prefix);
    assert!(history[0].get("token").is_none() || history[0]["token"].is_null());
    assert_ne!(history[0]["token"], raw_token);
    assert!(history[0]["revoked_at"].is_null());
}

#[tokio::test]
async fn sqlite_failed_session_revocation_returns_503_without_restoring_the_token() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name)
         VALUES ('sqlite-revocation-failure', 'Device')",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let revoker = RecordingSessionRevoker::failing();
    let app = sqlite_router(
        SqliteApiState::new(store.clone())
            .with_mqttd_device_transport_session_revoker(revoker.clone()),
    );
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-revocation-failure/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let first: serde_json::Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let first_token_id = first["id"].as_str().unwrap().to_owned();

    let replacement = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/sqlite-revocation-failure/tokens")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let revoked_at: Option<String> =
        sqlx::query_scalar("SELECT revoked_at FROM device_tokens WHERE id = ?")
            .bind(&first_token_id)
            .fetch_one(store.pool())
            .await
            .unwrap();

    assert_eq!(replacement.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(revoked_at.is_some());
    assert_eq!(
        revoker.calls(),
        vec![MqttdDeviceTransportSessionRevocation {
            device_id: "sqlite-revocation-failure".to_owned(),
            token_id: uuid::Uuid::parse_str(&first_token_id).unwrap(),
        }]
    );
}

#[tokio::test]
async fn sqlite_router_lists_power_monitor_devices_from_sqlite_telemetry() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "sqlite-meter".to_owned(),
        boot_id: uuid::Uuid::new_v4(),
        sequence: 1,
        event_at: Utc.with_ymd_and_hms(2026, 9, 7, 1, 0, 0).unwrap(),
        measurements: serde_json::Map::from_iter([
            ("voltage_v".to_owned(), json!(230.4)),
            ("power_w".to_owned(), json!(529.9)),
        ]),
        gateway_device_id: None,
    };
    store
        .write_telemetry(&event, event.event_at, "topic")
        .await
        .unwrap();
    let app = sqlite_router(SqliteApiState::new(store));

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
    let session: serde_json::Value = serde_json::from_slice(&login_body).unwrap();
    let session_id = session["session_id"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/apps/powermonitor/devices")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let devices: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(devices[0]["device_id"], "sqlite-meter");
    assert_eq!(devices[0]["voltage_v"], 230.4);
    assert_eq!(devices[0]["power_w"], 529.9);
}

#[tokio::test]
async fn sqlite_dashboard_lists_devices_and_returns_raw_and_rollup_telemetry() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let first_at = Utc.with_ymd_and_hms(2026, 9, 7, 1, 1, 0).unwrap();
    let second_at = Utc.with_ymd_and_hms(2026, 9, 7, 1, 3, 0).unwrap();
    for (sequence, event_at, temperature_c, humidity_pct) in
        [(1, first_at, 40.0, 50.0), (2, second_at, 42.0, 52.0)]
    {
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: "sqlite-meter".to_owned(),
            boot_id: uuid::Uuid::new_v4(),
            sequence,
            event_at,
            measurements: serde_json::Map::from_iter([
                ("temperature_c".to_owned(), json!(temperature_c)),
                ("humidity_pct".to_owned(), json!(humidity_pct)),
            ]),
            gateway_device_id: None,
        };
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap();
    }
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let devices = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/devices")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(devices.0, StatusCode::OK);
    assert_eq!(devices.1[0]["device_id"], "sqlite-meter");
    assert_eq!(devices.1[0]["display_name"], serde_json::Value::Null);
    assert!(devices.1[0]["last_seen_at"].as_str().is_some());

    for (bucket, expected_count) in [("raw", 2), ("5m", 1), ("1h", 1)] {
        let telemetry = sqlite_json_response(
            &app,
            Request::builder()
                .uri(format!(
                    "/api/devices/sqlite-meter/telemetry?from=2026-09-07T00:59:00Z&to=2026-09-07T01:05:00Z&bucket={bucket}"
                ))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(telemetry.0, StatusCode::OK, "bucket={bucket}");
        assert_eq!(telemetry.1.as_array().unwrap().len(), expected_count);
    }

    let raw = sqlite_json_response(
        &app,
        Request::builder()
            .uri(
                "/api/devices/sqlite-meter/telemetry?from=2026-09-07T00:59:00Z&to=2026-09-07T01:05:00Z&bucket=raw",
            )
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(raw.1[0]["temperature_c"], 40.0);
    assert_eq!(raw.1[0]["humidity_pct"], 50.0);
    assert_eq!(raw.1[0]["event_count"], 1);

    for bucket in ["5m", "1h"] {
        let rollup = sqlite_json_response(
            &app,
            Request::builder()
                .uri(format!(
                    "/api/devices/sqlite-meter/telemetry?from=2026-09-07T00:59:00Z&to=2026-09-07T01:05:00Z&bucket={bucket}"
                ))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(rollup.1[0]["temperature_c"], 41.0, "bucket={bucket}");
        assert_eq!(rollup.1[0]["humidity_pct"], 51.0, "bucket={bucket}");
        assert_eq!(rollup.1[0]["event_count"], 2, "bucket={bucket}");
    }
}

#[tokio::test]
async fn sqlite_powermonitor_routes_return_summary_telemetry_and_records() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let site_id = uuid::Uuid::now_v7();
    let panel_id = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO assets (id, name) VALUES (?, ?)")
        .bind(site_id.to_string())
        .bind("Factory")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO assets (id, name, parent_asset_id) VALUES (?, ?, ?)")
        .bind(panel_id.to_string())
        .bind("Main panel")
        .bind(site_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let now = Utc::now();
    let bucket_start = Utc
        .timestamp_opt(now.timestamp().div_euclid(300) * 300, 0)
        .single()
        .unwrap();
    let first_at = bucket_start + chrono::Duration::seconds(30);
    let second_at = first_at + chrono::Duration::minutes(1);
    for (device_id, sequence, event_at, power_w, energy_kwh) in [
        ("sqlite-power-meter", 1, first_at, 100.0, 2.0),
        ("sqlite-power-meter", 2, second_at, 120.0, 2.5),
        ("sqlite-aux-meter", 1, first_at, 30.0, 1.0),
    ] {
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: device_id.to_owned(),
            boot_id: uuid::Uuid::new_v4(),
            sequence,
            event_at,
            measurements: serde_json::Map::from_iter([
                ("voltage_v".to_owned(), json!(230.0)),
                ("current_a".to_owned(), json!(2.0)),
                ("power_w".to_owned(), json!(power_w)),
                ("energy_kwh".to_owned(), json!(energy_kwh)),
                ("frequency_hz".to_owned(), json!(50.0)),
                ("power_factor".to_owned(), json!(0.98)),
            ]),
            gateway_device_id: None,
        };
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap();
        sqlx::query("UPDATE devices SET asset_id = ? WHERE device_id = ?")
            .bind(panel_id.to_string())
            .bind(device_id)
            .execute(store.pool())
            .await
            .unwrap();
    }

    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let from = (first_at - chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let to = (second_at + chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let summary = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/summary")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(summary.0, StatusCode::OK, "{:?}", summary.1);
    assert_eq!(summary.1["device_count"], 2);
    assert_eq!(summary.1["asset_count"], 2);
    assert_eq!(summary.1["total_power_w"], 150.0);
    assert_eq!(summary.1["total_energy_kwh"], 3.5);

    let device_raw = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(device_raw.0, StatusCode::OK, "{:?}", device_raw.1);
    assert_eq!(device_raw.1.as_array().unwrap().len(), 2);
    assert_eq!(device_raw.1[1]["power_w"], 120.0);
    assert_eq!(device_raw.1[1]["event_count"], 1);

    let device_rollup = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry?from={from}&to={to}&bucket=5m"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(device_rollup.0, StatusCode::OK, "{:?}", device_rollup.1);
    assert_eq!(device_rollup.1.as_array().unwrap().len(), 1);
    assert_eq!(device_rollup.1[0]["power_w"], 110.0);
    assert_eq!(device_rollup.1[0]["event_count"], 2);

    let records = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry/records?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(records.0, StatusCode::OK, "{:?}", records.1);
    assert_eq!(records.1.as_array().unwrap().len(), 2);
    assert_eq!(records.1[0]["measurements"]["power_w"], 120.0);

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/assets/{site_id}/telemetry?from={from}&to={to}&bucket=5m"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::OK, "{:?}", asset.1);
    assert_eq!(asset.1.as_array().unwrap().len(), 1);
    assert_eq!(asset.1[0]["power_w"], 140.0);
    assert_eq!(asset.1[0]["event_count"], 3);

    let invalid_device_id = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite.meter/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(invalid_device_id.0, StatusCode::BAD_REQUEST);

    let invalid_range = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry?from={to}&to={from}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(invalid_range.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sqlite_powermonitor_raw_routes_reject_ranges_over_24_hours() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let to = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let from = (Utc::now() - chrono::Duration::hours(24) - chrono::Duration::seconds(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let asset_id = uuid::Uuid::now_v7();

    for uri in [
        format!(
            "/api/apps/powermonitor/devices/sqlite-meter/telemetry?from={from}&to={to}&bucket=raw"
        ),
        format!(
            "/api/apps/powermonitor/devices/sqlite-meter/telemetry/records?from={from}&to={to}&bucket=raw"
        ),
        format!(
            "/api/apps/powermonitor/assets/{asset_id}/telemetry?from={from}&to={to}&bucket=raw"
        ),
    ] {
        let response = sqlite_json_response(
            &app,
            Request::builder()
                .uri(uri)
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{:?}", response.1);
    }

    let rollup = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-meter/telemetry?from={from}&to={to}&bucket=5m"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(rollup.0, StatusCode::OK, "{:?}", rollup.1);
}

#[tokio::test]
async fn sqlite_powermonitor_raw_responses_are_capped_at_10_000_rows() {
    const RAW_ROW_LIMIT: i64 = 10_000;
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let device_id = "sqlite-high-frequency-meter";
    let asset_id = uuid::Uuid::now_v7();
    let event_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    sqlx::query("INSERT INTO assets (id, name) VALUES (?, ?)")
        .bind(asset_id.to_string())
        .bind("High-frequency panel")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, asset_id) VALUES (?, ?)")
        .bind(device_id)
        .bind(asset_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let received_at = event_at.clone();
    let boot_id = uuid::Uuid::new_v4().to_string();
    let measurements = json!({ "power_w": 1.0 }).to_string();
    let mut transaction = store.pool().begin().await.unwrap();
    for start in (0..=RAW_ROW_LIMIT).step_by(1_000) {
        let end = (start + 1_000).min(RAW_ROW_LIMIT + 1);
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "INSERT INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic
             ) ",
        );
        query.push_values(start..end, |mut values, sequence| {
            values
                .push_bind(&event_at)
                .push_bind(&received_at)
                .push_bind(device_id)
                .push_bind(&boot_id)
                .push_bind(sequence)
                .push_bind(&measurements)
                .push_bind("test");
        });
        query.build().execute(&mut *transaction).await.unwrap();
    }
    transaction.commit().await.unwrap();

    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let from = (Utc::now() - chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let to = (Utc::now() + chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let telemetry = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/{device_id}/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(telemetry.0, StatusCode::OK, "{:?}", telemetry.1);
    assert_eq!(
        telemetry.1.as_array().unwrap().len(),
        RAW_ROW_LIMIT as usize
    );

    let records = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/{device_id}/telemetry/records?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(records.0, StatusCode::OK, "{:?}", records.1);
    assert_eq!(records.1.as_array().unwrap().len(), RAW_ROW_LIMIT as usize);

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/assets/{asset_id}/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::OK, "{:?}", asset.1);
    assert_eq!(asset.1.as_array().unwrap().len(), 1);
    assert_eq!(asset.1[0]["event_count"], RAW_ROW_LIMIT + 1);
}

#[tokio::test]
async fn sqlite_powermonitor_summary_uses_one_latest_row_for_tied_timestamps() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let event_at = Utc::now();
    for (sequence, power_w, energy_kwh) in [(1, 100.0, 2.0), (2, 200.0, 3.0)] {
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: "sqlite-tied-meter".to_owned(),
            boot_id: uuid::Uuid::new_v4(),
            sequence,
            event_at,
            measurements: serde_json::Map::from_iter([
                ("power_w".to_owned(), json!(power_w)),
                ("energy_kwh".to_owned(), json!(energy_kwh)),
            ]),
            gateway_device_id: None,
        };
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap();
    }

    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let summary = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/summary")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(summary.0, StatusCode::OK, "{:?}", summary.1);
    assert_eq!(summary.1["device_count"], 1);
    assert_eq!(summary.1["total_power_w"], 200.0);
    assert_eq!(summary.1["total_energy_kwh"], 3.0);
}

#[tokio::test]
async fn sqlite_dashboard_admin_manages_the_alert_rule_and_incident_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let rule = json!({
        "name": "High temperature",
        "device_id": "sqlite-meter",
        "metric_key": "temperature_c",
        "rule_type": "event_threshold",
        "comparison": "gt",
        "threshold": 40.0,
        "for_seconds": 0,
        "severity": "warning"
    });

    let created = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/alert-rules")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(rule.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED);
    let rule_id = created.1["id"].as_str().unwrap().to_owned();

    let updated_rule = json!({
        "name": "Critical temperature",
        "device_id": "sqlite-meter",
        "metric_key": "temperature_c",
        "rule_type": "event_threshold",
        "comparison": "gte",
        "threshold": 42.0,
        "for_seconds": 0,
        "severity": "critical"
    });
    let updated = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/alert-rules/{rule_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(updated_rule.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(updated.0, StatusCode::OK);
    assert_eq!(updated.1["name"], "Critical temperature");
    assert_eq!(updated.1["severity"], "critical");

    let toggled = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/api/alert-rules/{rule_id}/toggle"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"enabled":false}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(toggled.0, StatusCode::OK);
    assert_eq!(toggled.1["enabled"], false);

    let rules = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/alert-rules")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(rules.0, StatusCode::OK);
    assert_eq!(rules.1[0]["id"], rule_id);

    let incident_id = uuid::Uuid::new_v4().to_string();
    let occurred_at = "2026-09-07T01:00:00+00:00";
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at,
            last_value, state_version, created_at, updated_at
         ) VALUES (?, ?, 'sqlite-meter', 'open', ?, ?, 42.5, 1, ?, ?)",
    )
    .bind(&incident_id)
    .bind(&rule_id)
    .bind(occurred_at)
    .bind(occurred_at)
    .bind(occurred_at)
    .bind(occurred_at)
    .execute(store.pool())
    .await
    .unwrap();

    let incidents = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/alert-incidents")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(incidents.0, StatusCode::OK);
    assert_eq!(incidents.1[0]["id"], incident_id);
    assert_eq!(incidents.1[0]["rule_name"], "Critical temperature");

    let acknowledged = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/api/alert-incidents/{incident_id}/acknowledge"))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(acknowledged.0, StatusCode::OK);
    assert_eq!(acknowledged.1["acknowledged_by"], "dashboard");

    let archived = sqlite_json_response(
        &app,
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/alert-rules/{rule_id}"))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(archived.0, StatusCode::NO_CONTENT);

    let active_rules = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/alert-rules")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(active_rules.0, StatusCode::OK);
    assert!(active_rules.1.as_array().unwrap().is_empty());
    let incident_status: String =
        sqlx::query_scalar("SELECT status FROM alert_incidents WHERE id = ?")
            .bind(&incident_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(incident_status, "resolved");
}

#[tokio::test]
async fn sqlite_dashboard_alert_writes_require_an_admin_session() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "viewer", "NanoView@1234").await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/alert-rules")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::from(
                    json!({
                        "name": "High temperature",
                        "metric_key": "temperature_c",
                        "rule_type": "event_threshold",
                        "comparison": "gt",
                        "threshold": 40.0
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn sqlite_management_admin_crud_covers_profiles_assets_devices_and_users() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let device_profile = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/profiles/device-profiles")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Power meter",
                    "telemetry_schema": { "power_w": "number" },
                    "metric_mapping": { "power": "power_w" },
                    "reporting_settings": { "interval_seconds": 60 }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(device_profile.0, StatusCode::CREATED);
    let device_profile_id = device_profile.1["id"].as_str().unwrap().to_owned();

    let asset_profile = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/profiles/asset-profiles")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Electrical panel",
                    "fields": { "floor": "string" },
                    "dashboard_defaults": { "layout": "grid" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(asset_profile.0, StatusCode::CREATED);
    let asset_profile_id = asset_profile.1["id"].as_str().unwrap().to_owned();

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/assets")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Main panel",
                    "asset_profile_id": asset_profile_id,
                    "attributes": { "floor": "1" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::CREATED);
    let asset_id = asset.1["id"].as_str().unwrap().to_owned();

    let provisioned = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Meter 1"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(provisioned.0, StatusCode::CREATED);
    let device_id = provisioned.1["device_id"].as_str().unwrap().to_owned();
    assert!(uuid::Uuid::parse_str(&device_id).is_ok());

    let device = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{device_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "display_name": "Meter 1 updated",
                    "asset_id": asset_id,
                    "device_profile_id": device_profile_id,
                    "attributes": { "phase": "A" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(device.0, StatusCode::OK);
    assert_eq!(device.1["display_name"], "Meter 1 updated");
    assert_eq!(device.1["attributes"]["phase"], "A");

    let users = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/management/users")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(users.0, StatusCode::OK);
    assert_eq!(users.1.as_array().unwrap().len(), 3);

    let user = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri("/api/management/users/viewer")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                r#"{"default_app":"/apps/powermonitor","granted_apps":["powermonitor"],"role":"viewer"}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(user.0, StatusCode::OK);
    assert_eq!(user.1["role"], "viewer");
    assert_eq!(user.1["granted_apps"], json!(["powermonitor"]));

    let renamed_asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/assets/{asset_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Main panel updated",
                    "asset_profile_id": asset_profile_id,
                    "attributes": { "floor": "2" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(renamed_asset.0, StatusCode::OK);
    assert_eq!(renamed_asset.1["attributes"]["floor"], "2");

    for uri in [
        format!("/api/management/devices/{device_id}"),
        format!("/api/management/assets/{asset_id}"),
        format!("/api/management/profiles/device-profiles/{device_profile_id}"),
        format!("/api/management/profiles/asset-profiles/{asset_profile_id}"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(uri)
                    .header("authorization", format!("Session {session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let deleted_at: Option<String> =
        sqlx::query_scalar("SELECT deleted_at FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(deleted_at.is_some());
}

#[tokio::test]
async fn sqlite_management_asset_deletes_preserve_reference_invariants() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let profile = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/profiles/asset-profiles")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"name":"Referenced profile","fields":{}}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(profile.0, StatusCode::CREATED);
    let profile_id = profile.1["id"].as_str().unwrap().to_owned();

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/assets")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Referenced asset",
                    "asset_profile_id": profile_id,
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::CREATED);
    let asset_id = asset.1["id"].as_str().unwrap().to_owned();

    let device = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Referenced device"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(device.0, StatusCode::CREATED);
    let device_id = device.1["device_id"].as_str().unwrap().to_owned();

    let assigned = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{device_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "display_name": "Referenced device",
                    "asset_id": asset_id,
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(assigned.0, StatusCode::OK);

    let profile_in_use = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/api/management/profiles/asset-profiles/{profile_id}"
                ))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(profile_in_use.status(), StatusCode::CONFLICT);

    let deleted_asset = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/assets/{asset_id}"))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted_asset.status(), StatusCode::NO_CONTENT);

    let device_asset_id: Option<String> =
        sqlx::query_scalar("SELECT asset_id FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(device_asset_id, None);

    let deleted_profile = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/api/management/profiles/asset-profiles/{profile_id}"
                ))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted_profile.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn sqlite_management_writes_require_an_admin_session() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "viewer", "NanoView@1234").await;

    let read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/management/devices")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::FORBIDDEN);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/assets")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::from(r#"{"name":"Viewer cannot create this"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn sqlite_admin_can_create_a_normal_user_account() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let admin_session = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let created = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/users")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {admin_session}"))
            .body(Body::from(
                r#"{"username":"alice","password":"AliceUser@1234","default_app":"/apps/powermonitor","granted_apps":["powermonitor"]}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED);
    assert_eq!(created.1["account_class"], "user");

    let login = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"username":"alice","password":"AliceUser@1234"}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(login.0, StatusCode::OK);
    assert_eq!(login.1["account_class"], "user");
}

#[tokio::test]
async fn sqlite_user_claims_an_unowned_device_with_a_one_time_claim_code() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let admin_session = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let user = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/users")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {admin_session}"))
            .body(Body::from(
                r#"{"username":"alice","password":"AliceUser@1234","default_app":"/apps/powermonitor","granted_apps":["powermonitor"]}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(user.0, StatusCode::CREATED);
    let alice_id = user.1["id"].as_str().unwrap().to_owned();
    let alice_session = sqlite_session_id(&app, "alice", "AliceUser@1234").await;
    let device = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {admin_session}"))
            .body(Body::from(r#"{"display_name":"Claimable meter"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(device.0, StatusCode::CREATED);
    let device_id = device.1["device_id"].as_str().unwrap().to_owned();

    let claim_code = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/management/devices/{device_id}/claim-code"))
                .header("authorization", format!("Session {admin_session}"))
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
    let code = claim_code["claim_code"].as_str().unwrap();

    let claimed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/device-claims")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {alice_session}"))
                .body(Body::from(
                    json!({
                        "device_id": device_id,
                        "claim_code": code,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.status(), StatusCode::NO_CONTENT);
    let owner: String = sqlx::query_scalar("SELECT owner_user_id FROM devices WHERE device_id = ?")
        .bind(&device_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(owner, alice_id);

    let second_claim = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/device-claims")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {alice_session}"))
                .body(Body::from(
                    json!({
                        "device_id": device_id,
                        "claim_code": code,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_claim.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn sqlite_management_gateway_child_assignment_revokes_the_child_token() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let gateway = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Gateway"}"#))
            .unwrap(),
    )
    .await;
    let gateway_id = gateway.1["device_id"].as_str().unwrap().to_owned();
    let child = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Child meter"}"#))
            .unwrap(),
    )
    .await;
    let child_id = child.1["device_id"].as_str().unwrap().to_owned();

    let gateway_update = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{gateway_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                r#"{"display_name":"Gateway","topology":{"is_gateway":true}}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(gateway_update.0, StatusCode::OK);

    let child_update = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{child_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "display_name": "Child meter",
                    "topology": {
                        "is_gateway": false,
                        "gateway_device_id": gateway_id
                    }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(child_update.0, StatusCode::OK);
    assert_eq!(child_update.1["gateway_device_id"], gateway_id);

    let active_token_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM device_tokens WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(child_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(active_token_count, 0);
}

#[tokio::test]
async fn sqlite_management_delete_revokes_active_device_tokens() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let provisioned = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Delete me"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(provisioned.0, StatusCode::CREATED);
    let device_id = provisioned.1["device_id"].as_str().unwrap().to_owned();

    let deleted = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/devices/{device_id}"))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let active_token_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM device_tokens WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(device_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(active_token_count, 0);
}

async fn sqlite_session_id(app: &axum::Router, username: &str, password: &str) -> String {
    let response = app
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
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice::<serde_json::Value>(&body).unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn sqlite_json_response(
    app: &axum::Router,
    request: Request<Body>,
) -> (StatusCode, serde_json::Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, body)
}
