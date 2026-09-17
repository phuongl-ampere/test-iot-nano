use std::{future::Future, pin::Pin, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use chrono::{Duration, Utc};
use iot_api::{
    CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest, CoreFacade,
    CoreFacadeError, CoreTelemetryPoint, CoreTelemetryQuery, TokenVault, public_v1_router,
};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret,
    OAuthClientCredentialsToken, OAuthRepository, PlatformStore,
};
use tower::ServiceExt;
use uuid::Uuid;

const APP_ID: &str = "sqlite-auth-app";
const CLIENT_ID: &str = "sqlite-auth-client";
const CLIENT_SECRET: &str = "sqlite-auth-client-secret";
const DEVICE_TOKEN: &str = "sqlite-auth-devices-token";
const ASSET_TOKEN: &str = "sqlite-auth-assets-token";

fn tenant_id() -> Uuid {
    Uuid::from_u128(10_006)
}

struct NoopCoreFacade;

impl CoreFacade for NoopCoreFacade {
    fn create_command(
        &self,
        _request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn get_command(
        &self,
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

async fn sqlite_public_router() -> (tempfile::TempDir, axum::Router) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("sqlite-auth.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'sqlite-auth', 'active')")
        .bind(tenant_id().to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    ApplicationRepository::upsert_application(
        store.as_ref(),
        NewApplication {
            app_id: APP_ID.parse().unwrap(),
            tenant_id: tenant_id(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec!["https://client.example.test/callback".parse().unwrap()],
            allowed_scopes: vec!["assets:read".to_owned(), "devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    OAuthRepository::register_client_secret(
        store.as_ref(),
        NewOAuthClientSecret {
            app_id: APP_ID.parse().unwrap(),
            tenant_id: tenant_id(),
            client_secret: CLIENT_SECRET.to_owned(),
        },
    )
    .await
    .unwrap();
    let now = Utc::now();
    for (access_token, scopes) in [
        (DEVICE_TOKEN, vec!["devices:read".to_owned()]),
        (ASSET_TOKEN, vec!["assets:read".to_owned()]),
    ] {
        OAuthRepository::issue_client_credentials_access_token(
            store.as_ref(),
            OAuthClientCredentialsToken {
                client_id: CLIENT_ID.parse().unwrap(),
                client_secret: CLIENT_SECRET.to_owned(),
                access_token: access_token.to_owned(),
                scopes,
                issued_at: now,
                expires_at: now + Duration::hours(1),
            },
        )
        .await
        .unwrap();
    }

    (
        directory,
        public_v1_router::<()>(
            store,
            TokenVault::from_key_material("sqlite-auth-router-vault-key-material"),
            Arc::new(NoopCoreFacade),
        ),
    )
}

#[tokio::test]
async fn sqlite_bearer_authentication_requires_an_exact_public_scope() {
    let (_directory, app) = sqlite_public_router().await;

    let anonymous = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    let insufficient_scope = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, format!("Bearer {ASSET_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(insufficient_scope.status(), StatusCode::FORBIDDEN);

    let permitted = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(header::AUTHORIZATION, format!("Bearer {DEVICE_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(permitted.status(), StatusCode::OK);
}
