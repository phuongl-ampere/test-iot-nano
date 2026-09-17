use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{Duration, Utc};
use iot_api::{
    CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest, CoreFacade,
    CoreFacadeError, CoreTelemetryPoint, CoreTelemetryQuery, TokenVault, public_v1_router,
};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_storage::{ApplicationKind, ApplicationRepository, NewApplication, PlatformStore};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

const APP_ID: &str = "task-8-router-export";
const CLIENT_ID: &str = "task-8-router-export-client";
const ACCESS_TOKEN: &str = "task-8-router-export-token";
const APPLICATION_ONLY_ACCESS_TOKEN: &str = "task-8-router-export-application-token";
const SHARED_ACCESS_TOKEN: &str = "task-8-router-export-shared-token";
const DEVICE_ID: &str = "task-8-router-export-device";

fn tenant_id() -> Uuid {
    Uuid::from_u128(10_004)
}

fn user_id() -> Uuid {
    Uuid::from_u128(10_005)
}

fn access_token_hash(access_token: &str) -> String {
    format!("{:x}", Sha256::digest(access_token.as_bytes()))
}

#[derive(Clone, Default)]
struct RecordingCore {
    created: Arc<Mutex<Vec<CoreCommandCreateRequest>>>,
}

impl CoreFacade for RecordingCore {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let created = Arc::clone(&self.created);
        Box::pin(async move {
            let record = CoreCommandRecord {
                id: request.id,
                tenant_id: request.tenant_id,
                device_id: request.device_id.clone(),
                state: "queued".to_owned(),
                expires_at: request.expires_at,
                mode: request.mode,
                response: None,
                responded_at: None,
            };
            created.lock().unwrap().push(request);
            Ok(record)
        })
    }

    fn get_command(
        &self,
        _tenant_id: Uuid,
        _id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn record_command_response(
        &self,
        _request: CoreCommandResponseRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn telemetry(
        &self,
        _query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }
}

async fn exported_router() -> (tempfile::TempDir, Router, Arc<RecordingCore>) {
    let (directory, app, core, _store) = exported_router_with_store().await;
    (directory, app, core)
}

async fn exported_router_with_store() -> (
    tempfile::TempDir,
    Router,
    Arc<RecordingCore>,
    Arc<PlatformStore>,
) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-router-export.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'public-router', 'active')")
        .bind(tenant_id().to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-router-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id().to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id, owner_user_id) VALUES (?, ?, ?)")
        .bind(DEVICE_ID)
        .bind(tenant_id().to_string())
        .bind(user_id().to_string())
        .execute(pool)
        .await
        .unwrap();
    ApplicationRepository::upsert_application(
        &store,
        NewApplication {
            app_id: APP_ID.parse().unwrap(),
            tenant_id: tenant_id(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec!["https://client.example.test/callback".parse().unwrap()],
            allowed_scopes: vec![
                "assets:read".to_owned(),
                "assets:write".to_owned(),
                "alerts:read".to_owned(),
                "alerts:write".to_owned(),
                "commands:write".to_owned(),
                "devices:read".to_owned(),
                "devices:write".to_owned(),
                "authorization:read".to_owned(),
                "telemetry:read".to_owned(),
            ],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let issued_at = Utc::now();
    let expires_at = issued_at + Duration::hours(1);
    let user_scopes = json!([
        "assets:read",
        "assets:write",
        "alerts:read",
        "alerts:write",
        "commands:write",
        "devices:read",
        "devices:write",
        "authorization:read",
        "telemetry:read",
    ])
    .to_string();
    sqlx::query(
        "INSERT INTO oauth_access_tokens (
            token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(access_token_hash(ACCESS_TOKEN))
    .bind(APP_ID)
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .bind(&user_scopes)
    .bind(issued_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO oauth_access_tokens (
            token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
         ) VALUES (?, ?, ?, NULL, ?, ?, ?)",
    )
    .bind(access_token_hash(APPLICATION_ONLY_ACCESS_TOKEN))
    .bind(APP_ID)
    .bind(tenant_id().to_string())
    .bind(json!(["assets:read"]).to_string())
    .bind(issued_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let core = Arc::new(RecordingCore::default());
    let store = Arc::new(store);
    let app = public_v1_router::<()>(
        store.clone(),
        TokenVault::from_key_material("task-8-router-export-vault"),
        core.clone(),
    );
    (directory, app, core, store)
}

#[tokio::test]
async fn exported_public_router_mounts_resources_and_delegates_supplied_ports() {
    let (_directory, app, core) = exported_router().await;
    let authorization = format!("Bearer {ACCESS_TOKEN}");

    for uri in [
        "/api/v1/assets",
        "/api/v1/telemetry?from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z",
        "/api/v1/alerts",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, &authorization)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
    }

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/commands"))
                .header(header::AUTHORIZATION, authorization)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "router-export-command-1")
                .body(Body::from(
                    json!({"method":"setRelay","params":{"enabled":true}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["state"], "queued");
    let calls = core.created.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].device_id, DEVICE_ID);
    assert_eq!(calls[0].mode, RpcMode::OneWay);
}

#[tokio::test]
async fn exported_public_router_rejects_an_unavailable_device_profile() {
    let (_directory, app, _core) = exported_router().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "device_id": "task-8-missing-profile-device",
                        "metadata": {},
                        "device_profile_id": Uuid::now_v7(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn exported_public_router_rejects_application_only_tokens() {
    let (_directory, app, _core) = exported_router().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/assets")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {APPLICATION_ONLY_ACCESS_TOKEN}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {APPLICATION_ONLY_ACCESS_TOKEN}"),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "device_id": "application-only-device",
                        "metadata": {},
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
async fn exported_public_legacy_admin_lists_only_group_authorized_resources() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let member_id = Uuid::now_v7();
    let group_id = Uuid::now_v7();
    let root_asset_id = Uuid::now_v7();
    let child_asset_id = Uuid::now_v7();
    let unshared_asset_id = Uuid::now_v7();
    let device_id = format!("shared-list-device-{}", Uuid::now_v7());
    let issued_at = Utc::now();
    let expires_at = issued_at + Duration::hours(1);

    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-list-member', 'unused', 'admin', 'admin')",
    )
    .bind(member_id.to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'public-list-group')",
    )
    .bind(group_id.to_string())
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO user_group_members (tenant_id, group_id, user_id) VALUES (?, ?, ?)")
        .bind(tenant_id().to_string())
        .bind(group_id.to_string())
        .bind(member_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, parent_asset_id, owner_user_id)
         VALUES (?, ?, 'shared-root', NULL, ?),
                (?, ?, 'shared-child', ?, ?),
                (?, ?, 'unshared-admin-asset', NULL, ?)",
    )
    .bind(root_asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .bind(child_asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(root_asset_id.to_string())
    .bind(user_id().to_string())
    .bind(unshared_asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, asset_id, owner_user_id)
         VALUES (?, ?, ?, ?)",
    )
    .bind(&device_id)
    .bind(tenant_id().to_string())
    .bind(child_asset_id.to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_group_id, asset_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', 1, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(tenant_id().to_string())
    .bind(group_id.to_string())
    .bind(root_asset_id.to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO oauth_access_tokens (
            token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(access_token_hash(SHARED_ACCESS_TOKEN))
    .bind(APP_ID)
    .bind(tenant_id().to_string())
    .bind(member_id.to_string())
    .bind(json!(["assets:read", "devices:read"]).to_string())
    .bind(issued_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let authorization = format!("Bearer {SHARED_ACCESS_TOKEN}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/assets")
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let assets: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let assets = assets["items"].as_array().unwrap();
    assert!(
        !assets
            .iter()
            .any(|asset| asset["id"] == Value::String(unshared_asset_id.to_string()))
    );
    let root = assets
        .iter()
        .find(|asset| asset["id"] == Value::String(root_asset_id.to_string()))
        .unwrap();
    assert_eq!(root["effective_permission"], "viewer");
    assert_eq!(root["access_source"], "group");
    let child = assets
        .iter()
        .find(|asset| asset["id"] == Value::String(child_asset_id.to_string()))
        .unwrap();
    assert_eq!(child["effective_permission"], "viewer");
    assert_eq!(child["access_source"], "inherited_group");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let devices: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let device = devices["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|device| device["device_id"] == Value::String(device_id.clone()))
        .unwrap();
    assert_eq!(device["effective_permission"], "viewer");
    assert_eq!(device["access_source"], "inherited_group");
}

#[tokio::test]
async fn exported_public_router_rejects_an_unavailable_asset_profile() {
    let (_directory, app, _core) = exported_router().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/assets")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "task-8-missing-asset-profile",
                        "metadata": {},
                        "asset_profile_id": Uuid::now_v7(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn exported_public_router_scopes_alerts_to_the_token_tenant() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_b_id = Uuid::now_v7();
    let tenant_a_rule_id = Uuid::now_v7();
    let tenant_b_rule_id = Uuid::now_v7();
    let tenant_a_alert_id = Uuid::now_v7();
    let tenant_b_alert_id = Uuid::now_v7();
    let tenant_a_device_id = format!("task-8-router-alert-tenant-a-{}", Uuid::now_v7());
    let tenant_b_device_id = format!("task-8-router-alert-tenant-b-{}", Uuid::now_v7());
    let event_at = Utc::now();

    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'public-router-alert-b', 'active')",
    )
    .bind(tenant_b_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id)
         VALUES (?, ?, ?), (?, ?, NULL)",
    )
    .bind(&tenant_a_device_id)
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .bind(&tenant_b_device_id)
    .bind(tenant_b_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold
         ) VALUES
            (?, ?, 'Router tenant A rule', ?, 'temperature_c', 'event_threshold', 'gt', 25),
            (?, ?, 'Router tenant B rule', ?, 'temperature_c', 'event_threshold', 'gt', 25)",
    )
    .bind(tenant_a_rule_id.to_string())
    .bind(tenant_id().to_string())
    .bind(&tenant_a_device_id)
    .bind(tenant_b_rule_id.to_string())
    .bind(tenant_b_id.to_string())
    .bind(&tenant_b_device_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, updated_at
         ) VALUES
            (?, ?, ?, ?, 'open', ?, ?, ?),
            (?, ?, ?, ?, 'resolved', ?, ?, ?)",
    )
    .bind(tenant_a_alert_id.to_string())
    .bind(tenant_id().to_string())
    .bind(tenant_a_rule_id.to_string())
    .bind(&tenant_a_device_id)
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(tenant_b_alert_id.to_string())
    .bind(tenant_b_id.to_string())
    .bind(tenant_b_rule_id.to_string())
    .bind(&tenant_b_device_id)
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE alert_incidents SET rule_id = ?, device_id = ? WHERE id = ?")
        .bind(tenant_a_rule_id.to_string())
        .bind(&tenant_a_device_id)
        .bind(tenant_b_alert_id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    let authorization = format!("Bearer {ACCESS_TOKEN}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/alerts")
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let alerts = payload["items"].as_array().unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        alerts[0]["id"],
        Value::String(tenant_a_alert_id.to_string())
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/alerts/{tenant_b_alert_id}"))
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/alerts/{tenant_b_alert_id}/acknowledge"))
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let tenant_b_acknowledged_at: Option<String> =
        sqlx::query_scalar("SELECT acknowledged_at FROM alert_incidents WHERE id = ?")
            .bind(tenant_b_alert_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(tenant_b_acknowledged_at, None);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/alerts/{tenant_a_alert_id}/acknowledge"))
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let tenant_a_acknowledged_at: Option<String> =
        sqlx::query_scalar("SELECT acknowledged_at FROM alert_incidents WHERE id = ?")
            .bind(tenant_a_alert_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(tenant_a_acknowledged_at.is_some());
}
