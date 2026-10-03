use std::{future::Future, pin::Pin, sync::Arc};

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
    NewTenant, NewTenantAccount, NewTenantPersonalAccessToken, PlatformStore,
    TenantIdentityRepository, TenantPersonalAccessTokenRepository,
};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

const PAT_SECRET: &str = "iotpat_personal-access-token-secret";
const DISABLED_PAT_SECRET: &str = "iotpat_disabled-personal-access-token-secret";

struct NoopCoreFacade;

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

async fn public_router() -> (
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
        Arc::new(NoopCoreFacade),
    );
    (directory, store, tenant.id, account.id, app)
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
