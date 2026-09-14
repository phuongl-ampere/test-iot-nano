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
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret,
    OAuthClientCredentialsToken, OAuthRepository, PlatformStore,
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const APP_ID: &str = "task-8-router-export";
const CLIENT_ID: &str = "task-8-router-export-client";
const CLIENT_SECRET: &str = "task-8-router-export-secret";
const ACCESS_TOKEN: &str = "task-8-router-export-token";
const DEVICE_ID: &str = "task-8-router-export-device";

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
    sqlx::query("INSERT INTO devices (device_id) VALUES (?)")
        .bind(DEVICE_ID)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_grants
            (id, resource_type, resource_id, grantee_type, grantee_id, permission)
         VALUES (?, 'device', ?, 'application', ?, 'controller')",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(DEVICE_ID)
    .bind(APP_ID)
    .execute(pool)
    .await
    .unwrap();
    ApplicationRepository::upsert_application(
        &store,
        NewApplication {
            app_id: APP_ID.parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec!["https://client.example.test/callback".parse().unwrap()],
            allowed_scopes: vec![
                "assets:read".to_owned(),
                "alerts:read".to_owned(),
                "commands:write".to_owned(),
                "authorization:read".to_owned(),
                "telemetry:read".to_owned(),
            ],
            enabled: true,
        },
    )
    .await
    .unwrap();
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: APP_ID.parse().unwrap(),
            client_secret: CLIENT_SECRET.to_owned(),
        },
    )
    .await
    .unwrap();
    OAuthRepository::issue_client_credentials_access_token(
        &store,
        OAuthClientCredentialsToken {
            client_id: CLIENT_ID.parse().unwrap(),
            client_secret: CLIENT_SECRET.to_owned(),
            access_token: ACCESS_TOKEN.to_owned(),
            scopes: vec![
                "assets:read".to_owned(),
                "alerts:read".to_owned(),
                "commands:write".to_owned(),
                "authorization:read".to_owned(),
                "telemetry:read".to_owned(),
            ],
            issued_at: Utc::now(),
            expires_at: Utc::now() + Duration::hours(1),
        },
    )
    .await
    .unwrap();

    let core = Arc::new(RecordingCore::default());
    let app = public_v1_router::<()>(
        Arc::new(store),
        TokenVault::from_key_material("task-8-router-export-vault"),
        core.clone(),
    );
    (directory, app, core)
}

#[tokio::test]
async fn exported_public_router_mounts_resources_and_delegates_supplied_ports() {
    let (_directory, app, core) = exported_router().await;
    let authorization = format!("Bearer {ACCESS_TOKEN}");

    for uri in [
        "/api/v1/assets",
        "/api/v1/telemetry?from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z",
        "/api/v1/alerts",
        "/api/v1/resource-grants",
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
