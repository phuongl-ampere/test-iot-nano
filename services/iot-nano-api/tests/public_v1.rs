use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION},
    },
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use iot_api::{
    CoreClient, CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest,
    CoreFacade, CoreFacadeError, CoreTelemetryPoint, CoreTelemetryQuery,
};
use iot_api::{SqliteApiState, bootstrap_users_sqlite, sqlite_router};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_core::{CoreControlState, CoreSqliteStore, core_control_router};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, PlatformStore, SqliteStore,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tower::ServiceExt;
use uuid::Uuid;

const CLIENT_ID: &str = "public-v1-client";
const REDIRECT_URI: &str = "https://client.example.test/public-v1/callback";
const VERIFIER: &str = "public-v1-pkce-verifier-with-at-least-forty-three-characters";
const CORE_SECRET: &str = "core-control-secret-must-have-at-least-32";

async fn public_app() -> (tempfile::TempDir, SqliteStore, axum::Router) {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-v1.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let oauth_store = PlatformStore::open(&configuration).await.unwrap();
    let api_store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(api_store.pool()).await.unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: "public-v1-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec![
                "assets:read".to_owned(),
                "assets:write".to_owned(),
                "telemetry:read".to_owned(),
                "alerts:read".to_owned(),
                "alerts:write".to_owned(),
                "commands:read".to_owned(),
                "commands:write".to_owned(),
                "authorization:read".to_owned(),
                "authorization:write".to_owned(),
            ],
            enabled: true,
        },
    )
    .await
    .unwrap();

    let app = sqlite_router(SqliteApiState::new(api_store.clone()).with_oauth_store(oauth_store));
    (directory, api_store, app)
}

#[derive(Default)]
struct RecordingCore {
    commands: Mutex<HashMap<Uuid, (CoreCommandCreateRequest, CoreCommandRecord)>>,
}

impl CoreFacade for RecordingCore {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async move {
            let mut commands = self.commands.lock().unwrap();
            if let Some((existing, record)) = commands.get(&request.id) {
                if existing.device_id == request.device_id
                    && existing.method == request.method
                    && existing.params == request.params
                    && existing.mode == request.mode
                {
                    return Ok(record.clone());
                }
                return Err(CoreFacadeError::Rejected(409));
            }
            let record = CoreCommandRecord {
                id: request.id,
                device_id: request.device_id.clone(),
                state: "queued".to_owned(),
                expires_at: request.expires_at,
                mode: request.mode,
                response: None,
                responded_at: None,
            };
            commands.insert(request.id, (request, record.clone()));
            Ok(record)
        })
    }

    fn get_command(
        &self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async move {
            self.commands
                .lock()
                .unwrap()
                .get(&id)
                .map(|(_, record)| record.clone())
                .ok_or(CoreFacadeError::NotFound)
        })
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

async fn public_app_with_core() -> (tempfile::TempDir, SqliteStore, axum::Router) {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-v1-core.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let oauth_store = PlatformStore::open(&configuration).await.unwrap();
    let api_store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(api_store.pool()).await.unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: "public-v1-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["commands:write".to_owned(), "commands:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let state = SqliteApiState::new(api_store.clone())
        .with_oauth_store(oauth_store)
        .with_core_facade(Arc::new(RecordingCore::default()));
    (directory, api_store, sqlite_router(state))
}

async fn public_app_with_real_core() -> (
    tempfile::TempDir,
    SqliteStore,
    axum::Router,
    tokio::task::JoinHandle<()>,
) {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-v1-real-core.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let oauth_store = PlatformStore::open(&configuration).await.unwrap();
    let api_store = SqliteStore::open(&configuration).await.unwrap();
    let core_store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("core.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    bootstrap_users_sqlite(api_store.pool()).await.unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: "public-v1-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["commands:write".to_owned(), "commands:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let core_app = core_control_router(CoreControlState::sqlite(core_store, CORE_SECRET).unwrap());
    let core_server = tokio::spawn(async move {
        axum::serve(listener, core_app).await.unwrap();
    });
    let core_client = CoreClient::new(format!("http://{address}"), CORE_SECRET).unwrap();
    let state = SqliteApiState::new(api_store.clone())
        .with_oauth_store(oauth_store)
        .with_core_client(core_client);
    (directory, api_store, sqlite_router(state), core_server)
}

async fn oauth_bearer_token(app: &axum::Router, scope: &str) -> String {
    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"viewer","password":"NanoView@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(VERIFIER.as_bytes()));
    let scope = scope.replace(':', "%3A");
    let authorization = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fpublic-v1%2Fcallback&scope={scope}&state=public-v1&code_challenge={challenge}&code_challenge_method=S256"
                ))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorization.status(), StatusCode::FOUND);
    let code = authorization.headers()[LOCATION]
        .to_str()
        .unwrap()
        .split_once('?')
        .unwrap()
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix("code="))
        .unwrap();

    let token = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fpublic-v1%2Fcallback&client_id={CLIENT_ID}&code_verifier={VERIFIER}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(token.into_body(), usize::MAX).await.unwrap()).unwrap();
    payload["access_token"].as_str().unwrap().to_owned()
}

async fn assert_public_error(response: axum::response::Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["code"], code);
    assert!(
        payload["message"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        payload["request_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(payload["details"], Value::Null);
}

#[tokio::test]
async fn public_router_mounts_every_remaining_contract_resource() {
    let (_directory, _store, app) = public_app().await;
    let routes = [
        ("GET", "/api/v1/assets"),
        ("GET", "/api/v1/assets/00000000-0000-0000-0000-000000000001"),
        ("GET", "/api/v1/telemetry"),
        ("GET", "/api/v1/telemetry/device-001"),
        ("GET", "/api/v1/alerts"),
        ("GET", "/api/v1/alerts/00000000-0000-0000-0000-000000000001"),
        (
            "GET",
            "/api/v1/commands/00000000-0000-0000-0000-000000000001",
        ),
        ("GET", "/api/v1/resource-grants"),
        (
            "GET",
            "/api/v1/resource-grants/00000000-0000-0000-0000-000000000001",
        ),
        ("POST", "/api/v1/devices/device-001/commands"),
    ];

    for (method, uri) in routes {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
        assert_public_error(response, StatusCode::UNAUTHORIZED, "unauthorized").await;
    }
}

#[tokio::test]
async fn public_asset_write_checks_scope_before_resource_authorization() {
    let (_directory, _store, app) = public_app().await;
    let token = oauth_bearer_token(&app, "assets:read").await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/assets")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"new asset","metadata":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_public_error(response, StatusCode::FORBIDDEN, "forbidden").await;
}

#[tokio::test]
async fn public_asset_list_filters_to_authorized_assets_without_disclosure() {
    let (_directory, store, app) = public_app().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let admin_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'admin'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let owned = Uuid::now_v7();
    let shared = Uuid::now_v7();
    let hidden = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, name, owner_user_id)
         VALUES (?, 'owned', ?), (?, 'shared', ?), (?, 'hidden', ?)",
    )
    .bind(owned.to_string())
    .bind(&viewer_id)
    .bind(shared.to_string())
    .bind(&admin_id)
    .bind(hidden.to_string())
    .bind(&admin_id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares (
            id, resource_type, resource_id, target_user_id, permission,
            inherit_children, state, created_by_user_id
         ) VALUES (?, 'asset', ?, ?, 'viewer', 0, 'active', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(shared.to_string())
    .bind(&viewer_id)
    .bind(&admin_id)
    .execute(store.pool())
    .await
    .unwrap();

    let token = oauth_bearer_token(&app, "assets:read").await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/assets?limit=100")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let ids = payload["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, [owned.to_string(), shared.to_string()]);

    for id in [hidden, Uuid::now_v7()] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/assets/{id}"))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_public_error(response, StatusCode::FORBIDDEN, "forbidden").await;
    }
}

#[tokio::test]
async fn public_commands_replay_identical_idempotency_keys_and_conflict_on_payload_changes() {
    let (_directory, store, app) = public_app_with_core().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, owner_user_id) VALUES ('command-device', ?)")
        .bind(viewer_id)
        .execute(store.pool())
        .await
        .unwrap();
    let token = oauth_bearer_token(&app, "commands:write").await;

    let request = || {
        Request::builder()
            .method("POST")
            .uri("/api/v1/devices/command-device/commands")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", "command-retry-1")
            .body(Body::from(
                r#"{"method":"setRelay","params":{"enabled":true}}"#,
            ))
            .unwrap()
    };
    let first = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first: Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let second = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(second.status(), StatusCode::ACCEPTED);
    let second: Value =
        serde_json::from_slice(&to_bytes(second.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(first["id"], second["id"]);

    let conflict = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices/command-device/commands")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "command-retry-1")
                .body(Body::from(
                    r#"{"method":"setRelay","params":{"enabled":false}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(conflict, StatusCode::CONFLICT, "conflict").await;
}

#[tokio::test]
async fn public_commands_replay_through_the_real_core_control_path() {
    let (_directory, store, app, core_server) = public_app_with_real_core().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, owner_user_id) VALUES ('real-core-device', ?)")
        .bind(viewer_id)
        .execute(store.pool())
        .await
        .unwrap();
    let token = oauth_bearer_token(&app, "commands:write").await;
    let request = || {
        Request::builder()
            .method("POST")
            .uri("/api/v1/devices/real-core-device/commands")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", "real-core-replay-1")
            .body(Body::from(
                r#"{"method":"setRelay","params":{"enabled":true}}"#,
            ))
            .unwrap()
    };

    let first = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first: Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let replay = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    let replay: Value =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(replay, first);

    let conflict = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices/real-core-device/commands")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "real-core-replay-1")
                .body(Body::from(
                    r#"{"method":"setRelay","params":{"enabled":false}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(conflict, StatusCode::CONFLICT, "conflict").await;
    core_server.abort();
}

#[tokio::test]
async fn public_telemetry_reads_only_authorized_devices() {
    let (_directory, store, app) = public_app().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, owner_user_id)
         VALUES ('telemetry-visible', ?), ('telemetry-hidden', ?)",
    )
    .bind(&viewer_id)
    .bind(
        sqlx::query_scalar::<_, String>("SELECT id FROM users WHERE username = 'admin'")
            .fetch_one(store.pool())
            .await
            .unwrap(),
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES
            ('2026-01-01T00:00:01Z', '2026-01-01T00:00:02Z', 'telemetry-visible',
             'boot-visible', 1, '{"temperature_c":21.5}', 'devices/telemetry-visible/telemetry'),
            ('2026-01-01T00:00:01Z', '2026-01-01T00:00:02Z', 'telemetry-hidden',
             'boot-hidden', 1, '{"temperature_c":99}', 'devices/telemetry-hidden/telemetry')"#,
    )
    .execute(store.pool())
    .await
    .unwrap();
    let token = oauth_bearer_token(&app, "telemetry:read").await;

    let visible = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/telemetry/telemetry-visible?from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(visible.status(), StatusCode::OK);
    let visible: Value =
        serde_json::from_slice(&to_bytes(visible.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(visible["items"][0]["device_id"], "telemetry-visible");
    assert_eq!(visible["items"][0]["measurements"]["temperature_c"], 21.5);

    let hidden = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/telemetry/telemetry-hidden?from=2026-01-01T00:00:00Z&to=2026-01-01T00:01:00Z")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(hidden, StatusCode::FORBIDDEN, "forbidden").await;
}

#[tokio::test]
async fn public_resource_grants_support_crud_for_a_managed_asset() {
    let (_directory, store, app) = public_app().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let admin_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'admin'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let asset_id = Uuid::now_v7();
    sqlx::query("INSERT INTO assets (id, name, owner_user_id) VALUES (?, 'grantable', ?)")
        .bind(asset_id.to_string())
        .bind(&viewer_id)
        .execute(store.pool())
        .await
        .unwrap();
    let write_token = oauth_bearer_token(&app, "authorization:write").await;
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/resource-grants")
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"resource_type":"asset","resource_id":"{asset_id}","grantee_type":"user","grantee_id":"{admin_id}","permission":"viewer"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let grant: Value =
        serde_json::from_slice(&to_bytes(create.into_body(), usize::MAX).await.unwrap()).unwrap();
    let grant_id = grant["id"].as_str().unwrap().to_owned();

    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/resource-grants/{grant_id}"))
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"resource_type":"asset","resource_id":"{asset_id}","grantee_type":"user","grantee_id":"{admin_id}","permission":"manager"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);
    let updated: Value =
        serde_json::from_slice(&to_bytes(update.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(updated["permission"], "manager");

    let read_token = oauth_bearer_token(&app, "authorization:read").await;
    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/resource-grants")
                .header(AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let list: Value =
        serde_json::from_slice(&to_bytes(list.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(list["items"][0]["id"], grant_id);

    let delete = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/resource-grants/{grant_id}"))
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn public_alerts_list_detail_and_acknowledge_follow_device_authorization() {
    let (_directory, store, app) = public_app().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let device_id = "alert-device";
    sqlx::query("INSERT INTO devices (device_id, owner_user_id) VALUES (?, ?)")
        .bind(device_id)
        .bind(&viewer_id)
        .execute(store.pool())
        .await
        .unwrap();
    let rule_id = Uuid::now_v7();
    let alert_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
            severity
         ) VALUES (?, 'temperature alert', 1, ?, 'temperature_c', 'event_threshold',
                   'greater_than', 30, 'critical')",
    )
    .bind(rule_id.to_string())
    .bind(device_id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, last_value
         ) VALUES (?, ?, ?, 'open', '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z', 42)",
    )
    .bind(alert_id.to_string())
    .bind(rule_id.to_string())
    .bind(device_id)
    .execute(store.pool())
    .await
    .unwrap();

    let read_token = oauth_bearer_token(&app, "alerts:read").await;
    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/alerts")
                .header(AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let list: Value =
        serde_json::from_slice(&to_bytes(list.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(list["items"][0]["id"], alert_id.to_string());
    assert_eq!(list["items"][0]["severity"], "critical");

    let detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/alerts/{alert_id}"))
                .header(AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail.status(), StatusCode::OK);

    let write_token = oauth_bearer_token(&app, "alerts:write").await;
    let acknowledged = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/alerts/{alert_id}/acknowledge"))
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(acknowledged.status(), StatusCode::OK);
    let acknowledged: Value = serde_json::from_slice(
        &to_bytes(acknowledged.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(acknowledged["acknowledged_by"], viewer_id);
}
