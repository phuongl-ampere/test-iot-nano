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
    CoreAuthorizedCommandCreateRequest, CoreCommandCreateRequest, CoreCommandRecord,
    CoreCommandResponseRequest, CoreFacade, CoreFacadeError, CoreTelemetryPoint,
    CoreTelemetryQuery, TokenVault, create_platform_device_token, public_v1_router,
};
use iot_nano_foundation::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_storage::{
    ApplicationDomainProfileRepository, ApplicationDomainResourceKind, ApplicationKind,
    ApplicationRepository, CreateApplicationDomainProfile, DeviceClaimPolicy,
    DeviceClaimRepository, NewApplication, PlatformStore,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

const APP_ID: &str = "task-8-router-export";
const CLIENT_ID: &str = "task-8-router-export-client";
const ACCESS_TOKEN: &str = "task-8-router-export-token";
const APPLICATION_ONLY_ACCESS_TOKEN: &str = "task-8-router-export-application-token";
const SHARED_ACCESS_TOKEN: &str = "task-8-router-export-shared-token";
const COMMAND_RACE_TOKEN: &str = "task-8-router-export-command-race-token";
const DEVICE_ID: &str = "task-8-router-export-device";
const INVITED_ACCESS_TOKEN: &str = "task-8-router-export-invited-token";

fn tenant_id() -> Uuid {
    Uuid::from_u128(10_004)
}

fn user_id() -> Uuid {
    Uuid::from_u128(10_005)
}

fn invited_user_id() -> Uuid {
    Uuid::from_u128(10_006)
}

fn access_token_hash(access_token: &str) -> String {
    format!("{:x}", Sha256::digest(access_token.as_bytes()))
}

#[derive(Clone, Default)]
struct RecordingCore {
    created: Arc<Mutex<Vec<CoreCommandCreateRequest>>>,
}

#[derive(Clone)]
struct RevokingCore {
    store: Arc<PlatformStore>,
    permission_id: Uuid,
}

impl CoreFacade for RevokingCore {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        let permission_id = self.permission_id;
        Box::pin(async move {
            sqlx::query(
                "UPDATE resource_permissions SET revoked_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
            .bind(permission_id.to_string())
            .execute(store.sqlite_pool().expect("SQLite store"))
            .await
            .map_err(|_| CoreFacadeError::Unavailable)?;
            Ok(CoreCommandRecord {
                id: request.id,
                tenant_id: request.tenant_id,
                device_id: request.device_id,
                state: "queued".to_owned(),
                expires_at: request.expires_at,
                mode: request.mode,
                response: None,
                responded_at: None,
            })
        })
    }

    fn create_authorized_command(
        &self,
        request: CoreAuthorizedCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        let permission_id = self.permission_id;
        Box::pin(async move {
            sqlx::query(
                "UPDATE resource_permissions SET revoked_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
            .bind(permission_id.to_string())
            .execute(store.sqlite_pool().expect("SQLite store"))
            .await
            .map_err(|_| CoreFacadeError::Unavailable)?;
            let _ = request;
            Err(CoreFacadeError::NotFound)
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

    fn create_authorized_command(
        &self,
        request: CoreAuthorizedCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        self.create_command(request.command)
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
    for capability in [
        "create_assets",
        "create_devices",
        "edit_resources",
        "control_devices",
        "share_owned_resources",
        "assign_application_profiles",
        "manage_device_tokens",
    ] {
        sqlx::query(
            "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES (?, ?, ?)",
        )
        .bind(user_id().to_string())
        .bind(tenant_id().to_string())
        .bind(capability)
        .execute(pool)
        .await
        .unwrap();
    }
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
                "authorization:write".to_owned(),
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
        "authorization:write",
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
async fn exported_public_resource_invitation_requires_recipient_acceptance() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "DELETE FROM user_capabilities
         WHERE user_id = ? AND tenant_id = ? AND capability = 'share_owned_resources'",
    )
    .bind(user_id().to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let issued_at = Utc::now();
    let expires_at = issued_at + Duration::hours(1);
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-invited-user', 'unused', 'viewer', 'user')",
    )
    .bind(invited_user_id().to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO oauth_access_tokens (
            token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(access_token_hash(INVITED_ACCESS_TOKEN))
    .bind(APP_ID)
    .bind(tenant_id().to_string())
    .bind(invited_user_id().to_string())
    .bind(json!(["authorization:read", "authorization:write", "devices:read"]).to_string())
    .bind(issued_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let owner_authorization = format!("Bearer {ACCESS_TOKEN}");
    let invitation = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/resource-invitations"))
                .header(header::AUTHORIZATION, owner_authorization)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"username": "public-invited-user", "permission": "viewer"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invitation.status(), StatusCode::CREATED);
    let invitation: Value =
        serde_json::from_slice(&to_bytes(invitation.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let invitation_id = invitation["id"].as_str().unwrap();

    let invited_authorization = format!("Bearer {INVITED_ACCESS_TOKEN}");
    let devices_before_accept = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, &invited_authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(devices_before_accept.status(), StatusCode::OK);
    let devices_before_accept: Value = serde_json::from_slice(
        &to_bytes(devices_before_accept.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(devices_before_accept["items"], json!([]));

    let pending = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/resource-invitations")
                .header(header::AUTHORIZATION, &invited_authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pending.status(), StatusCode::OK);
    let pending: Value =
        serde_json::from_slice(&to_bytes(pending.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(pending["items"][0]["resource_kind"], "device");
    assert_eq!(pending["items"][0]["resource_id"], DEVICE_ID);

    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/resource-invitations/{invitation_id}/accept"
                ))
                .header(header::AUTHORIZATION, &invited_authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::NO_CONTENT);

    let devices_after_accept = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, invited_authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(devices_after_accept.status(), StatusCode::OK);
    let devices_after_accept: Value = serde_json::from_slice(
        &to_bytes(devices_after_accept.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(devices_after_accept["items"][0]["device_id"], DEVICE_ID);
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
async fn exported_public_command_allows_the_device_owner_without_global_control_capability() {
    let (_directory, app, core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("DELETE FROM user_capabilities WHERE user_id = ? AND tenant_id = ?")
        .bind(user_id().to_string())
        .bind(tenant_id().to_string())
        .execute(pool)
        .await
        .unwrap();

    let request = || {
        Request::builder()
            .method("POST")
            .uri(format!("/api/v1/devices/{DEVICE_ID}/commands"))
            .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", "capability-gated-command")
            .body(Body::from(
                json!({"method":"setRelay","params":{"enabled":true}}).to_string(),
            ))
            .unwrap()
    };

    let allowed = app.oneshot(request()).await.unwrap();
    assert_eq!(allowed.status(), StatusCode::ACCEPTED);
    assert_eq!(core.created.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn exported_public_claim_consumes_a_code_without_returning_it() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let device_id = format!("claim-via-public-api-{}", Uuid::now_v7());
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES (?, ?)")
        .bind(&device_id)
        .bind(tenant_id().to_string())
        .execute(pool)
        .await
        .unwrap();
    DeviceClaimRepository::update_device_claim_policy(
        store.as_ref(),
        tenant_id(),
        DeviceClaimPolicy {
            enabled: true,
            ..DeviceClaimPolicy::default()
        },
    )
    .await
    .unwrap();
    let issued =
        DeviceClaimRepository::issue_device_claim_code(store.as_ref(), tenant_id(), &device_id)
            .await
            .unwrap();

    let claim_request = || {
        Request::builder()
            .method("POST")
            .uri("/api/v1/devices/claim")
            .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"device_id": device_id, "code": issued.code}).to_string(),
            ))
            .unwrap()
    };

    let denied = app.clone().oneshot(claim_request()).await.unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    sqlx::query(
        "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES (?, ?, 'claim_devices')",
    )
    .bind(user_id().to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let claimed = app.clone().oneshot(claim_request()).await.unwrap();
    assert_eq!(claimed.status(), StatusCode::OK);
    let body = to_bytes(claimed.into_body(), usize::MAX).await.unwrap();
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["device_id"], device_id);
    assert!(response.get("code").is_none());
    assert!(!String::from_utf8_lossy(&body).contains(&issued.code));

    let consumed = app.oneshot(claim_request()).await.unwrap();
    assert_eq!(consumed.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn exported_public_user_capabilities_only_returns_the_callers_capabilities() {
    let (_directory, app, _core, _store) = exported_router_with_store().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/user-capabilities")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(
        response["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("create_assets"))
    );
    assert!(response.get("username").is_none());
}

#[tokio::test]
async fn exported_public_devices_include_current_runtime_health() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let seen_at = Utc::now();
    sqlx::query("UPDATE devices SET last_seen_at = ? WHERE device_id = ?")
        .bind(seen_at.to_rfc3339())
        .bind(DEVICE_ID)
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["items"][0]["online"], true);
    assert_eq!(
        payload["items"][0]["last_seen_at"],
        seen_at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
    );
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
async fn exported_public_device_live_view_ignores_a_legacy_global_profile_without_an_application_assignment()
 {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_profiles (
            id, tenant_id, name, telemetry_schema, metric_mapping, reporting_settings
         ) VALUES (?, ?, 'Power Meter v1', '{}', '{}', ?)",
    )
    .bind(profile_id.to_string())
    .bind(tenant_id().to_string())
    .bind(
        json!({
            "live_charts": [{
                "metric": "power_w",
                "label": "Active power",
                "unit": "W",
                "color": "#167b83",
                "aggregation": "last"
            }]
        })
        .to_string(),
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET device_profile_id = ? WHERE device_id = ?")
        .bind(profile_id.to_string())
        .bind(DEVICE_ID)
        .execute(pool)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices/{DEVICE_ID}/live-view"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["profile"], Value::Null);
    assert_eq!(payload["charts"], json!([]));
}

#[tokio::test]
async fn exported_public_application_domain_profile_routes_use_the_oauth_application() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let profile = ApplicationDomainProfileRepository::create_application_domain_profile(
        store.as_ref(),
        tenant_id(),
        CreateApplicationDomainProfile {
            app_id: APP_ID.parse().unwrap(),
            resource_kind: ApplicationDomainResourceKind::Device,
            name: "Power Meter".to_owned(),
            definition: json!({"telemetry_schema": {"power_w": {"type": "number"}}}),
            live_view: json!({
                "live_charts": [{
                    "metric": "power_w",
                    "label": "Active power",
                    "unit": "W",
                    "color": "#167b83",
                    "aggregation": "last"
                }]
            }),
        },
    )
    .await
    .unwrap();

    let catalog = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/application-domain/profiles?kind=device")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(catalog.status(), StatusCode::OK);
    let catalog: Value =
        serde_json::from_slice(&to_bytes(catalog.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        catalog,
        json!([{ "id": profile.id, "name": "Power Meter" }])
    );

    let assignment = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/application-profile"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"profile_id": profile.id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(assignment.status(), StatusCode::OK);

    let view = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices/{DEVICE_ID}/live-view"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(view.status(), StatusCode::OK);
    let view: Value =
        serde_json::from_slice(&to_bytes(view.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        view["profile"],
        json!({"id": profile.id, "name": "Power Meter"})
    );
    assert_eq!(view["charts"][0]["metric"], "power_w");
}

#[tokio::test]
async fn exported_public_profile_catalogs_are_scoped_to_the_token_tenant() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let device_profile_id = Uuid::now_v7();
    let asset_profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_profiles (
            id, tenant_id, name, telemetry_schema, metric_mapping, reporting_settings
         ) VALUES (?, ?, 'Power Meter v1', '{}', '{}', '{}')",
    )
    .bind(device_profile_id.to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name, fields, dashboard_defaults)
         VALUES (?, ?, 'Power Farm v1', '{}', '{}')",
    )
    .bind(asset_profile_id.to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let device_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/device-profiles")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let asset_response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/asset-profiles")
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(device_response.status(), StatusCode::OK);
    assert_eq!(asset_response.status(), StatusCode::OK);
    let devices: Value = serde_json::from_slice(
        &to_bytes(device_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let assets: Value = serde_json::from_slice(
        &to_bytes(asset_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        devices,
        json!([{ "id": device_profile_id, "name": "Power Meter v1" }])
    );
    assert_eq!(
        assets,
        json!([{ "id": asset_profile_id, "name": "Power Farm v1" }])
    );
}

#[tokio::test]
async fn exported_public_manager_can_assign_and_clear_a_device_profile() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_profiles (
            id, tenant_id, name, telemetry_schema, metric_mapping, reporting_settings
         ) VALUES (?, ?, 'Power Meter v1', '{}', '{}', '{}')",
    )
    .bind(profile_id.to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let assign_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/devices/{DEVICE_ID}"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "device_profile_id": profile_id }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(assign_response.status(), StatusCode::OK);

    let clear_response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/devices/{DEVICE_ID}"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "device_profile_id": null }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(clear_response.status(), StatusCode::OK);
    let cleared: Value = serde_json::from_slice(
        &to_bytes(clear_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(cleared["device_profile_id"], Value::Null);
}

#[tokio::test]
async fn exported_public_asset_live_view_ignores_a_legacy_global_profile_without_an_application_assignment()
 {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let profile_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name, fields, dashboard_defaults)
         VALUES (?, ?, 'Power Farm v1', '{}', ?)",
    )
    .bind(profile_id.to_string())
    .bind(tenant_id().to_string())
    .bind(
        json!({
            "live_charts": [{
                "metric": "power_w",
                "label": "Farm demand",
                "unit": "W",
                "color": "#d69731",
                "aggregation": "sum"
            }]
        })
        .to_string(),
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, asset_profile_id, owner_user_id)
         VALUES (?, ?, 'Demo farm', ?, ?)",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(profile_id.to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/assets/{asset_id}/live-view"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["profile"], Value::Null);
    assert_eq!(payload["charts"], json!([]));
}

#[tokio::test]
async fn exported_public_manager_can_assign_and_clear_an_asset_profile() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let profile_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name, fields, dashboard_defaults)
         VALUES (?, ?, 'Power Farm v1', '{}', '{}')",
    )
    .bind(profile_id.to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id)
         VALUES (?, ?, 'Demo farm', ?)",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let assign_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "asset_profile_id": profile_id }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(assign_response.status(), StatusCode::OK);

    let clear_response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "asset_profile_id": null }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(clear_response.status(), StatusCode::OK);
    let cleared: Value = serde_json::from_slice(
        &to_bytes(clear_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(cleared["asset_profile_id"], Value::Null);
}

#[tokio::test]
async fn exported_public_asset_telemetry_is_limited_to_the_selected_asset_tree() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let root_asset_id = Uuid::now_v7();
    let child_asset_id = Uuid::now_v7();
    let other_device_id = "task-8-router-other-asset-device";
    let event_at = Utc::now();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, parent_asset_id, owner_user_id)
         VALUES (?, ?, 'Telemetry farm', NULL, ?), (?, ?, 'Telemetry zone', ?, ?)",
    )
    .bind(root_asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .bind(child_asset_id.to_string())
    .bind(tenant_id().to_string())
    .bind(root_asset_id.to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET asset_id = ? WHERE device_id = ?")
        .bind(child_asset_id.to_string())
        .bind(DEVICE_ID)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id)
         VALUES (?, ?, ?)",
    )
    .bind(other_device_id)
    .bind(tenant_id().to_string())
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    for (device_id, sequence) in [(DEVICE_ID, 1), (other_device_id, 2)] {
        sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'public-live-view')",
        )
        .bind(event_at.to_rfc3339())
        .bind(event_at.to_rfc3339())
        .bind(tenant_id().to_string())
        .bind(device_id)
        .bind(Uuid::now_v7().to_string())
        .bind(sequence)
        .bind(json!({ "power_w": 42.0 }).to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/telemetry?asset_id={root_asset_id}&aggregate=asset&from={}&to={}",
                    (event_at - Duration::minutes(1))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    (event_at + Duration::minutes(1))
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                ))
                .header(header::AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);
    assert_eq!(payload["items"][0]["device_id"], DEVICE_ID);
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
async fn exported_public_command_does_not_enqueue_after_permission_revocation() {
    let (_directory, _app, _core, store) = exported_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let manager_id = Uuid::now_v7();
    let permission_id = Uuid::now_v7();
    let issued_at = Utc::now();
    let expires_at = issued_at + Duration::hours(1);
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-command-race-manager', 'unused', 'viewer', 'user')",
    )
    .bind(manager_id.to_string())
    .bind(tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'manager', 0, ?)",
    )
    .bind(permission_id.to_string())
    .bind(tenant_id().to_string())
    .bind(manager_id.to_string())
    .bind(DEVICE_ID)
    .bind(user_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO oauth_access_tokens (
            token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(access_token_hash(COMMAND_RACE_TOKEN))
    .bind(APP_ID)
    .bind(tenant_id().to_string())
    .bind(manager_id.to_string())
    .bind(json!(["commands:write"]).to_string())
    .bind(issued_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let app = public_v1_router::<()>(
        store.clone(),
        TokenVault::from_key_material("task-8-router-command-race-vault"),
        Arc::new(RevokingCore {
            store: store.clone(),
            permission_id,
        }),
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/commands"))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {COMMAND_RACE_TOKEN}"),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "command-revocation-race")
                .body(Body::from(
                    json!({"method":"setRelay","params":{"enabled":true}}).to_string(),
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

#[tokio::test]
async fn exported_public_owner_can_manage_a_device_token_and_alert_rules() {
    let (_directory, app, _core, store) = exported_router_with_store().await;
    let vault = TokenVault::from_key_material("task-8-router-export-vault");
    let issued = create_platform_device_token(store.as_ref(), &vault, tenant_id(), DEVICE_ID)
        .await
        .unwrap();
    let authorization = format!("Bearer {ACCESS_TOKEN}");

    let revealed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices/{DEVICE_ID}/token"))
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revealed.status(), StatusCode::OK);
    let revealed: Value =
        serde_json::from_slice(&to_bytes(revealed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(revealed["token"], json!(issued.token));

    let rotated = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/token"))
                .header(header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::OK);
    let rotated: Value =
        serde_json::from_slice(&to_bytes(rotated.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_ne!(rotated["token"], revealed["token"]);

    let created_rule = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/alert-rules"))
                .header(header::AUTHORIZATION, &authorization)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "name": "High active power",
                        "metric_key": "power_w",
                        "rule_type": "event_threshold",
                        "comparison": "gt",
                        "threshold": 500.0,
                        "severity": "warning"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created_rule.status(), StatusCode::CREATED);
    let created_rule: Value = serde_json::from_slice(
        &to_bytes(created_rule.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(created_rule["device_id"], json!(DEVICE_ID));

    let rules = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices/{DEVICE_ID}/alert-rules"))
                .header(header::AUTHORIZATION, authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rules.status(), StatusCode::OK);
    let rules: Value =
        serde_json::from_slice(&to_bytes(rules.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(rules["items"].as_array().unwrap().len(), 1);
    assert_eq!(rules["items"][0]["id"], created_rule["id"]);
}
