use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use chrono::Utc;
use iot_api::{
    CoreAuthorizedCommandCreateRequest, CoreCommandCreateRequest, CoreCommandRecord,
    CoreCommandResponseRequest, CoreFacade, CoreFacadeError, CoreTelemetryPoint,
    CoreTelemetryQuery, TokenVault, public_v1_router, validate_bearer_access_token,
};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AuditPrincipal, NewTenant, NewTenantAccount, NewTenantPersonalAccessToken, PlatformStore,
    TenantIdentityRepository, TenantPersonalAccessTokenRepository,
};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

const PAT_SECRET: &str = "iotpat_personal-access-token-secret";
const DISABLED_PAT_SECRET: &str = "iotpat_disabled-personal-access-token-secret";
const OTHER_TENANT_PAT_SECRET: &str = "iotpat_other-tenant-personal-access-token-secret";

struct NoopCoreFacade;

#[derive(Default)]
struct RecordingCoreFacade {
    authorized_commands: Mutex<Vec<CoreAuthorizedCommandCreateRequest>>,
}

impl CoreFacade for NoopCoreFacade {
    fn create_command(
        &self,
        _request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn create_authorized_command(
        &self,
        _request: CoreAuthorizedCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
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
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn telemetry(
        &self,
        _query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>
    {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }
}

impl CoreFacade for RecordingCoreFacade {
    fn create_command(
        &self,
        _request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn create_authorized_command(
        &self,
        request: CoreAuthorizedCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let record = CoreCommandRecord {
            id: request.command.id,
            tenant_id: request.command.tenant_id,
            device_id: request.command.device_id.clone(),
            state: "queued".to_owned(),
            expires_at: request.command.expires_at,
            mode: request.command.mode,
            response: None,
            responded_at: None,
        };
        self.authorized_commands.lock().unwrap().push(request);
        Box::pin(async move { Ok(record) })
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
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn telemetry(
        &self,
        _query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>
    {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }
}

async fn public_router() -> (
    tempfile::TempDir,
    Arc<PlatformStore>,
    Uuid,
    Uuid,
    axum::Router,
) {
    public_router_with_core(Arc::new(NoopCoreFacade)).await
}

async fn public_router_with_core(
    core_facade: Arc<dyn CoreFacade>,
) -> (
    tempfile::TempDir,
    Arc<PlatformStore>,
    Uuid,
    Uuid,
    axum::Router,
) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("personal-access-tokens.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    let (tenant, account) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "personal-access-token".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: "unused".to_owned(),
        },
    )
    .await
    .unwrap();
    let app = public_v1_router::<()>(
        Arc::clone(&store),
        TokenVault::from_key_material("personal-access-token-test-key-material"),
        core_facade,
    );
    (directory, store, tenant.id, account.id, app)
}

async fn seed_regular_user(store: &PlatformStore, tenant_id: Uuid, username: &str) -> Uuid {
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .bind(username)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    user_id
}

async fn issue_pat(store: &PlatformStore, tenant_id: Uuid, account_id: Uuid, secret: &str) {
    TenantPersonalAccessTokenRepository::rotate_tenant_personal_access_token(
        store,
        tenant_id,
        account_id,
        NewTenantPersonalAccessToken {
            id: Uuid::now_v7(),
            name: "CI".to_owned(),
            token_prefix: secret[..14].to_owned(),
            token_hash: format!("{:x}", Sha256::digest(secret.as_bytes())),
        },
        Utc::now(),
    )
    .await
    .unwrap();
}

fn bearer_request(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn tenant_account_pat_authenticates_the_public_devices_resource() {
    let (_directory, store, tenant_id, account_id, app) = public_router().await;
    issue_pat(store.as_ref(), tenant_id, account_id, PAT_SECRET).await;

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {PAT_SECRET}").parse().unwrap(),
    );
    let token = validate_bearer_access_token(store.as_ref(), &headers, Utc::now())
        .await
        .unwrap();
    assert_eq!(token.tenant_id, tenant_id);
    assert_eq!(token.user_id, None);
    assert_eq!(
        token.scopes,
        [
            "assets:read",
            "assets:write",
            "devices:read",
            "devices:write",
            "telemetry:read",
            "alerts:read",
            "alerts:write",
            "commands:read",
            "commands:write",
            "authorization:read",
            "authorization:write",
        ]
        .map(str::to_owned),
    );

    let response = app
        .oneshot(bearer_request("/api/v1/devices", PAT_SECRET))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn revoked_or_disabled_tenant_account_pat_is_denied() {
    let (_directory, store, tenant_id, account_id, app) = public_router().await;
    issue_pat(store.as_ref(), tenant_id, account_id, DISABLED_PAT_SECRET).await;

    TenantPersonalAccessTokenRepository::revoke_tenant_personal_access_token(
        store.as_ref(),
        tenant_id,
        account_id,
        Utc::now(),
    )
    .await
    .unwrap();
    let revoked = app
        .clone()
        .oneshot(bearer_request("/api/v1/devices", DISABLED_PAT_SECRET))
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);

    issue_pat(store.as_ref(), tenant_id, account_id, PAT_SECRET).await;
    TenantIdentityRepository::disable_tenant_account(store.as_ref(), "personal-access-token")
        .await
        .unwrap();
    let disabled = app
        .oneshot(bearer_request("/api/v1/devices", PAT_SECRET))
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn public_router_does_not_expose_management_routes_to_a_pat() {
    let (_directory, store, tenant_id, account_id, app) = public_router().await;
    issue_pat(store.as_ref(), tenant_id, account_id, PAT_SECRET).await;

    let response = app
        .oneshot(bearer_request(
            "/api/v1/management/personal-access-token",
            PAT_SECRET,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_account_pat_can_mutate_tenant_resources_without_user_capabilities() {
    let (_directory, store, tenant_id, account_id, app) = public_router().await;
    issue_pat(store.as_ref(), tenant_id, account_id, PAT_SECRET).await;

    let create_asset = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/assets")
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "PAT asset",
                        "asset_profile_id": null,
                        "parent_asset_id": null,
                        "metadata": {},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_asset.status(), StatusCode::CREATED);
    let asset: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_asset.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let asset_id = asset["id"].as_str().unwrap();

    let update_asset = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "name": "PAT asset updated" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update_asset.status(), StatusCode::OK);

    let create_device = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "device_id": "pat-device",
                        "display_name": null,
                        "metadata": {},
                        "asset_id": asset_id,
                        "device_profile_id": null,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_device.status(), StatusCode::CREATED);

    let update_device = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/devices/pat-device")
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "display_name": "PAT device updated" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update_device.status(), StatusCode::OK);

    let other_tenant_id = Uuid::now_v7();
    let other_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'pat-other-tenant', 'active');
         INSERT INTO assets (id, tenant_id, name, metadata)
         VALUES (?, ?, 'other tenant asset', '{}')",
    )
    .bind(other_tenant_id.to_string())
    .bind(other_asset_id.to_string())
    .bind(other_tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let cross_tenant_update = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/assets/{other_asset_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "name": "cross tenant update" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_tenant_update.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_account_pat_can_issue_commands_and_invitations_with_a_truthful_account_actor() {
    let core = Arc::new(RecordingCoreFacade::default());
    let (_directory, store, tenant_id, account_id, app) =
        public_router_with_core(core.clone()).await;
    issue_pat(store.as_ref(), tenant_id, account_id, PAT_SECRET).await;
    seed_regular_user(store.as_ref(), tenant_id, "pat-recipient").await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('pat-command-device', ?, 'PAT command device')",
    )
    .bind(tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let command = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices/pat-command-device/commands")
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", "tenant-account-pat-command")
                .body(Body::from(
                    serde_json::json!({
                        "method": "sample_now",
                        "params": { "source": "pat" },
                        "mode": "one_way",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(command.status(), StatusCode::ACCEPTED);
    let commands = core.authorized_commands.lock().unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].actor,
        AuditPrincipal::TenantAccount(account_id),
        "PAT command issuer must be the authenticated Tenant Account"
    );
    drop(commands);

    let invitation = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices/pat-command-device/resource-invitations")
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "username": "pat-recipient",
                        "permission": "manager",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invitation.status(), StatusCode::CREATED);
    let invitation_id: String = sqlx::query_scalar(
        "SELECT id FROM resource_invitations
         WHERE tenant_id = ? AND device_id = 'pat-command-device' AND state = 'pending'",
    )
    .bind(tenant_id.to_string())
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let sender: (String, String) = sqlx::query_as(
        "SELECT sender_principal_kind, sender_principal_id
         FROM resource_invitations WHERE id = ?",
    )
    .bind(&invitation_id)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(sender.0, "tenant_account");
    assert_eq!(sender.1, account_id.to_string());

    let cancelled = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/resource-invitations/{invitation_id}/cancel"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {PAT_SECRET}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);

    let (other_tenant, other_account) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "pat-command-other-tenant".to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: "unused".to_owned(),
        },
    )
    .await
    .unwrap();
    issue_pat(
        store.as_ref(),
        other_tenant.id,
        other_account.id,
        OTHER_TENANT_PAT_SECRET,
    )
    .await;
    let cross_tenant_command = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices/pat-command-device/commands")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {OTHER_TENANT_PAT_SECRET}"),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", "cross-tenant-pat-command")
                .body(Body::from(
                    serde_json::json!({ "method": "sample_now", "params": {} }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_tenant_command.status(), StatusCode::FORBIDDEN);
}
