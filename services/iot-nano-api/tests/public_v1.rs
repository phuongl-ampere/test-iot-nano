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
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret, OAuthRepository,
    PlatformStore, SqliteStore,
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
                "devices:read".to_owned(),
                "devices:write".to_owned(),
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
            allowed_scopes: vec![
                "assets:read".to_owned(),
                "assets:write".to_owned(),
                "devices:read".to_owned(),
                "devices:write".to_owned(),
                "commands:write".to_owned(),
                "commands:read".to_owned(),
                "authorization:read".to_owned(),
                "authorization:write".to_owned(),
            ],
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

async fn client_credentials_token(
    app: &axum::Router,
    client_id: &str,
    client_secret: &str,
    scope: &str,
) -> String {
    let token = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope={scope}"
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
        ("POST", "/api/v1/devices"),
        ("PATCH", "/api/v1/devices/device-001"),
        ("DELETE", "/api/v1/devices/device-001"),
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
async fn public_device_mutations_create_update_and_soft_delete_through_generic_repository() {
    let (_directory, store, app) = public_app().await;
    let write_token = oauth_bearer_token(&app, "devices:write").await;
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices")
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"device_id":"public-mutation-device","display_name":"Created","metadata":{"room":"lab"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let created: Value =
        serde_json::from_slice(&to_bytes(create.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(created["device_id"], "public-mutation-device");
    assert_eq!(created["display_name"], "Created");
    assert_eq!(created["metadata"]["room"], "lab");

    let read_token = oauth_bearer_token(&app, "devices:read").await;
    let listed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: Value =
        serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(listed["items"][0]["device_id"], "public-mutation-device");

    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/devices/public-mutation-device")
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"display_name":"Updated","metadata":{"room":"office"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);
    let updated: Value =
        serde_json::from_slice(&to_bytes(update.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(updated["display_name"], "Updated");
    assert_eq!(updated["metadata"]["room"], "office");

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/devices/public-mutation-device")
                .header(AUTHORIZATION, format!("Bearer {write_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM devices WHERE device_id = ?",
        )
        .bind("public-mutation-device")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .is_some()
    );

    let hidden = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices/public-mutation-device")
                .header(AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(hidden, StatusCode::FORBIDDEN, "forbidden").await;
}

#[tokio::test]
async fn public_device_mutations_deny_unknown_and_inaccessible_targets_without_disclosure() {
    let (_directory, store, app) = public_app().await;
    let admin_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'admin'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, owner_user_id)
         VALUES ('public-inaccessible-mutation-device', 'Hidden', ?)",
    )
    .bind(admin_id)
    .execute(store.pool())
    .await
    .unwrap();
    let token = oauth_bearer_token(&app, "devices:write").await;

    for device_id in [
        "public-inaccessible-mutation-device",
        "public-unknown-mutation-device",
    ] {
        let update = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/v1/devices/{device_id}"))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"display_name":"must not update"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_public_error(update, StatusCode::FORBIDDEN, "forbidden").await;

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/v1/devices/{device_id}"))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_public_error(delete, StatusCode::FORBIDDEN, "forbidden").await;
    }
}

#[tokio::test]
async fn application_clients_require_matching_resource_grants_for_devices_assets_and_commands() {
    const OWNER_APP_ID: &str = "public-v1-app";
    const OWNER_CLIENT_SECRET: &str = "public-v1-owner-client-secret";
    const GUEST_APP_ID: &str = "public-v1-guest-app";
    const GUEST_CLIENT_ID: &str = "public-v1-guest-client";
    const GUEST_CLIENT_SECRET: &str = "public-v1-guest-client-secret";
    const DEVICE_ID: &str = "application-owned-device";

    let (_directory, store, app) = public_app_with_core().await;
    let oauth_store = PlatformStore::Sqlite(store.clone());
    OAuthRepository::register_client_secret(
        &oauth_store,
        NewOAuthClientSecret {
            app_id: OWNER_APP_ID.parse().unwrap(),
            client_secret: OWNER_CLIENT_SECRET.to_owned(),
        },
    )
    .await
    .unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: GUEST_APP_ID.parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://guest.example.test".to_owned(),
            client_id: GUEST_CLIENT_ID.parse().unwrap(),
            redirect_uris: vec!["https://guest.example.test/callback".parse().unwrap()],
            allowed_scopes: vec![
                "assets:read".to_owned(),
                "assets:write".to_owned(),
                "devices:read".to_owned(),
                "devices:write".to_owned(),
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
    OAuthRepository::register_client_secret(
        &oauth_store,
        NewOAuthClientSecret {
            app_id: GUEST_APP_ID.parse().unwrap(),
            client_secret: GUEST_CLIENT_SECRET.to_owned(),
        },
    )
    .await
    .unwrap();

    let owner_write_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "devices%3Awrite").await;
    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/devices")
                .header(AUTHORIZATION, format!("Bearer {owner_write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"device_id":"{DEVICE_ID}","metadata":{{}}}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT permission FROM resource_grants
             WHERE resource_type = 'device' AND resource_id = ?
               AND grantee_type = 'application' AND grantee_id = ?",
        )
        .bind(DEVICE_ID)
        .bind(OWNER_APP_ID)
        .fetch_one(store.pool())
        .await
        .unwrap(),
        "manager"
    );

    let guest_read_token =
        client_credentials_token(&app, GUEST_CLIENT_ID, GUEST_CLIENT_SECRET, "devices%3Aread")
            .await;
    let read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices/{DEVICE_ID}"))
                .header(AUTHORIZATION, format!("Bearer {guest_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(read, StatusCode::FORBIDDEN, "forbidden").await;

    let guest_write_token = client_credentials_token(
        &app,
        GUEST_CLIENT_ID,
        GUEST_CLIENT_SECRET,
        "devices%3Awrite",
    )
    .await;
    let update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/devices/{DEVICE_ID}"))
                .header(AUTHORIZATION, format!("Bearer {guest_write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"display_name":"guest update"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(update, StatusCode::FORBIDDEN, "forbidden").await;
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/devices/{DEVICE_ID}"))
                .header(AUTHORIZATION, format!("Bearer {guest_write_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(delete, StatusCode::FORBIDDEN, "forbidden").await;

    let guest_authorization_token = client_credentials_token(
        &app,
        GUEST_CLIENT_ID,
        GUEST_CLIENT_SECRET,
        "authorization%3Awrite",
    )
    .await;
    let create_share = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/resource-grants")
                .header(AUTHORIZATION, format!("Bearer {guest_authorization_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"resource_type":"device","resource_id":"{DEVICE_ID}","grantee_type":"application","grantee_id":"{GUEST_APP_ID}","permission":"viewer"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(create_share, StatusCode::FORBIDDEN, "forbidden").await;

    let guest_command_token = client_credentials_token(
        &app,
        GUEST_CLIENT_ID,
        GUEST_CLIENT_SECRET,
        "commands%3Awrite",
    )
    .await;
    let command = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/commands"))
                .header(AUTHORIZATION, format!("Bearer {guest_command_token}"))
                .header(CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "guest-command")
                .body(Body::from(
                    r#"{"method":"setRelay","params":{"enabled":true}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(command, StatusCode::FORBIDDEN, "forbidden").await;

    let owner_read_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "devices%3Aread").await;
    let owner_read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices/{DEVICE_ID}"))
                .header(AUTHORIZATION, format!("Bearer {owner_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_read.status(), StatusCode::OK);
    let owner_command_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "commands%3Awrite").await;
    let owner_command = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/devices/{DEVICE_ID}/commands"))
                .header(AUTHORIZATION, format!("Bearer {owner_command_token}"))
                .header(CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "owner-command")
                .body(Body::from(
                    r#"{"method":"setRelay","params":{"enabled":true}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_command.status(), StatusCode::ACCEPTED);
    let owner_command: Value = serde_json::from_slice(
        &to_bytes(owner_command.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let command_id = owner_command["id"].as_str().unwrap().to_owned();

    let guest_command_read_token = client_credentials_token(
        &app,
        GUEST_CLIENT_ID,
        GUEST_CLIENT_SECRET,
        "commands%3Aread",
    )
    .await;
    let guest_command_read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/commands/{command_id}"))
                .header(AUTHORIZATION, format!("Bearer {guest_command_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(guest_command_read, StatusCode::FORBIDDEN, "forbidden").await;

    let owner_command_read_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "commands%3Aread").await;
    let owner_command_read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/commands/{command_id}"))
                .header(AUTHORIZATION, format!("Bearer {owner_command_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_command_read.status(), StatusCode::OK);
    let owner_command_read: Value = serde_json::from_slice(
        &to_bytes(owner_command_read.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(owner_command_read["id"], command_id);

    let owner_asset_write_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "assets%3Awrite").await;
    let create_asset = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/assets")
                .header(AUTHORIZATION, format!("Bearer {owner_asset_write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"application-owned-asset","metadata":{"zone":"lab"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_asset.status(), StatusCode::CREATED);
    let created_asset: Value = serde_json::from_slice(
        &to_bytes(create_asset.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let asset_id = created_asset["id"].as_str().unwrap().to_owned();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT permission FROM resource_grants
             WHERE resource_type = 'asset' AND resource_id = ?
               AND grantee_type = 'application' AND grantee_id = ?",
        )
        .bind(&asset_id)
        .bind(OWNER_APP_ID)
        .fetch_one(store.pool())
        .await
        .unwrap(),
        "manager"
    );

    let guest_asset_read_token =
        client_credentials_token(&app, GUEST_CLIENT_ID, GUEST_CLIENT_SECRET, "assets%3Aread").await;
    let guest_asset_read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(AUTHORIZATION, format!("Bearer {guest_asset_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(guest_asset_read, StatusCode::FORBIDDEN, "forbidden").await;
    let guest_asset_list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/assets")
                .header(AUTHORIZATION, format!("Bearer {guest_asset_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(guest_asset_list.status(), StatusCode::OK);
    let guest_asset_list: Value = serde_json::from_slice(
        &to_bytes(guest_asset_list.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        !guest_asset_list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == asset_id)
    );

    let guest_asset_write_token =
        client_credentials_token(&app, GUEST_CLIENT_ID, GUEST_CLIENT_SECRET, "assets%3Awrite")
            .await;
    let guest_asset_update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(AUTHORIZATION, format!("Bearer {guest_asset_write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"metadata":{"zone":"guest"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(guest_asset_update, StatusCode::FORBIDDEN, "forbidden").await;
    let guest_asset_grant = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/resource-grants")
                .header(AUTHORIZATION, format!("Bearer {guest_authorization_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"resource_type":"asset","resource_id":"{asset_id}","grantee_type":"application","grantee_id":"{GUEST_APP_ID}","permission":"viewer"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(guest_asset_grant, StatusCode::FORBIDDEN, "forbidden").await;
    let guest_asset_delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(AUTHORIZATION, format!("Bearer {guest_asset_write_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(guest_asset_delete, StatusCode::FORBIDDEN, "forbidden").await;

    let owner_asset_read_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "assets%3Aread").await;
    let owner_asset_read = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(AUTHORIZATION, format!("Bearer {owner_asset_read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_asset_read.status(), StatusCode::OK);
    let owner_asset_update = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(AUTHORIZATION, format!("Bearer {owner_asset_write_token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"metadata":{"zone":"office"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_asset_update.status(), StatusCode::OK);
    let owner_asset_update: Value = serde_json::from_slice(
        &to_bytes(owner_asset_update.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(owner_asset_update["metadata"]["zone"], "office");

    let owner_authorization_write_token = client_credentials_token(
        &app,
        CLIENT_ID,
        OWNER_CLIENT_SECRET,
        "authorization%3Awrite",
    )
    .await;
    let owner_asset_grant = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/resource-grants")
                .header(
                    AUTHORIZATION,
                    format!("Bearer {owner_authorization_write_token}"),
                )
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"resource_type":"asset","resource_id":"{asset_id}","grantee_type":"application","grantee_id":"public-v1-grant-recipient","permission":"viewer"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_asset_grant.status(), StatusCode::CREATED);
    let owner_asset_grant: Value = serde_json::from_slice(
        &to_bytes(owner_asset_grant.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let grant_id = owner_asset_grant["id"].as_str().unwrap().to_owned();

    let owner_authorization_read_token =
        client_credentials_token(&app, CLIENT_ID, OWNER_CLIENT_SECRET, "authorization%3Aread")
            .await;
    let owner_grant_list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/resource-grants")
                .header(
                    AUTHORIZATION,
                    format!("Bearer {owner_authorization_read_token}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_grant_list.status(), StatusCode::OK);
    let owner_grant_list: Value = serde_json::from_slice(
        &to_bytes(owner_grant_list.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        owner_grant_list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == grant_id)
    );
    let owner_grant_get = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/resource-grants/{grant_id}"))
                .header(
                    AUTHORIZATION,
                    format!("Bearer {owner_authorization_read_token}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_grant_get.status(), StatusCode::OK);

    let guest_authorization_read_token = client_credentials_token(
        &app,
        GUEST_CLIENT_ID,
        GUEST_CLIENT_SECRET,
        "authorization%3Aread",
    )
    .await;
    let guest_grant_list = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/resource-grants")
                .header(
                    AUTHORIZATION,
                    format!("Bearer {guest_authorization_read_token}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(guest_grant_list.status(), StatusCode::OK);
    let guest_grant_list: Value = serde_json::from_slice(
        &to_bytes(guest_grant_list.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        !guest_grant_list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == grant_id)
    );
    let guest_grant_get = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/resource-grants/{grant_id}"))
                .header(
                    AUTHORIZATION,
                    format!("Bearer {guest_authorization_read_token}"),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(guest_grant_get, StatusCode::FORBIDDEN, "forbidden").await;

    let owner_asset_delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/assets/{asset_id}"))
                .header(AUTHORIZATION, format!("Bearer {owner_asset_write_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(owner_asset_delete.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn public_device_mutation_scope_is_checked_before_target_or_body() {
    let (_directory, _store, app) = public_app().await;
    let token = oauth_bearer_token(&app, "devices:read").await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/devices/unknown-device")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("{not-json}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(response, StatusCode::FORBIDDEN, "forbidden").await;
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
async fn public_grant_list_and_get_apply_authorization_read_after_visibility_pagination() {
    let (_directory, store, app) = public_app().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let admin_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'admin'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let hidden_first = Uuid::from_u128(1);
    let hidden_second = Uuid::from_u128(2);
    let visible_first = Uuid::from_u128(3);
    let visible_second = Uuid::from_u128(4);
    for (id, created_by_user_id, grantee_id) in [
        (hidden_first, &admin_id, "public-grant-hidden-app-one"),
        (hidden_second, &admin_id, "public-grant-hidden-app-two"),
        (visible_first, &viewer_id, "public-grant-visible-app-one"),
        (visible_second, &viewer_id, "public-grant-visible-app-two"),
    ] {
        sqlx::query(
            "INSERT INTO resource_grants
                (id, resource_type, resource_id, grantee_type, grantee_id, permission,
                 created_by_user_id)
             VALUES (?, 'device', 'public-grant-page-device', 'application', ?, 'viewer', ?)",
        )
        .bind(id.to_string())
        .bind(grantee_id)
        .bind(created_by_user_id)
        .execute(store.pool())
        .await
        .unwrap();
    }
    let token = oauth_bearer_token(&app, "authorization:read").await;

    let hidden_get = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/resource-grants/{hidden_first}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(hidden_get, StatusCode::FORBIDDEN, "forbidden").await;

    let first_page = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/resource-grants?limit=1")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_page.status(), StatusCode::OK);
    let first_page: Value =
        serde_json::from_slice(&to_bytes(first_page.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(first_page["items"][0]["id"], visible_first.to_string());
    assert_eq!(first_page["has_more"], true);
    let cursor = first_page["next_cursor"].as_str().unwrap().to_owned();

    let second_page = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/resource-grants?limit=1&after={cursor}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.status(), StatusCode::OK);
    let second_page: Value =
        serde_json::from_slice(&to_bytes(second_page.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(second_page["items"][0]["id"], visible_second.to_string());
    assert_eq!(second_page["has_more"], false);
    assert!(second_page["next_cursor"].is_null());

    let visible_get = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/resource-grants/{visible_first}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(visible_get.status(), StatusCode::OK);
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
