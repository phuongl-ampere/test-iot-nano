use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION},
    },
    response::Response,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use iot_api::{ApiState, SqliteApiState, bootstrap_users_sqlite, routers, sqlite_router};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, PlatformStore, SqliteStore,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

const CLIENT_ID: &str = "public-devices-client";
const REDIRECT_URI: &str = "https://client.example.test/public-devices/callback";
const VERIFIER: &str = "public-device-list-pkce-verifier-with-at-least-forty-three-characters";

async fn public_device_app() -> (tempfile::TempDir, SqliteStore, axum::Router) {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-devices.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let oauth_store = PlatformStore::open(&configuration).await.unwrap();
    let api_store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(api_store.pool()).await.unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: "public-devices-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["assets:read".to_owned(), "devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();

    let app = sqlite_router(SqliteApiState::new(api_store.clone()).with_oauth_store(oauth_store));
    (directory, api_store, app)
}

async fn oauth_bearer_token(
    app: &axum::Router,
    username: &str,
    password: &str,
    scope: &str,
) -> String {
    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"username":"{username}","password":"{password}"}}"#
                )))
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
    let encoded_scope = scope.replace(':', "%3A");
    let authorization = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fpublic-devices%2Fcallback&scope={encoded_scope}&state=public-device-list&code_challenge={challenge}&code_challenge_method=S256"
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
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fpublic-devices%2Fcallback&client_id={CLIENT_ID}&code_verifier={VERIFIER}"
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

async fn assert_public_error(response: Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["code"], code);
    assert!(
        payload["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty())
    );
    assert!(
        payload["request_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(payload["details"], Value::Null);
}

async fn seed_visible_devices(store: &SqliteStore) {
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let admin_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'admin'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, owner_user_id)
         VALUES ('device-001-owned', 'Owned device', ?),
                ('device-002-shared', 'Shared device', ?),
                ('device-003-hidden', 'Hidden device', ?)",
    )
    .bind(&viewer_id)
    .bind(&admin_id)
    .bind(&admin_id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares (
            id, resource_type, resource_id, target_user_id, permission,
            inherit_children, state, created_by_user_id
         ) VALUES (?, 'device', 'device-002-shared', ?, 'viewer', 0, 'active', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(viewer_id)
    .bind(admin_id)
    .execute(store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn public_device_list_requires_an_oauth_bearer_token() {
    let (_directory, _store, app) = public_device_app().await;

    for authorization in [None, Some("Session legacy-session")] {
        let mut request = Request::builder().uri("/api/v1/devices");
        if let Some(authorization) = authorization {
            request = request.header(AUTHORIZATION, authorization);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_public_error(response, StatusCode::UNAUTHORIZED, "unauthorized").await;
    }
}

#[tokio::test]
async fn public_device_list_requires_the_exact_devices_read_scope() {
    let (_directory, _store, app) = public_device_app().await;
    let token = oauth_bearer_token(&app, "viewer", "NanoView@1234", "assets:read").await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_public_error(response, StatusCode::FORBIDDEN, "forbidden").await;
}

#[tokio::test]
async fn public_device_list_filters_to_the_token_subjects_owned_and_granted_devices() {
    let (_directory, store, app) = public_device_app().await;
    seed_visible_devices(&store).await;
    let token = oauth_bearer_token(&app, "viewer", "NanoView@1234", "devices:read").await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices?limit=100")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let ids: Vec<_> = payload["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["device_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["device-001-owned", "device-002-shared"]);
    assert_eq!(payload["next_cursor"], Value::Null);
    assert_eq!(payload["has_more"], false);
}

#[tokio::test]
async fn public_device_list_uses_opaque_cursor_pagination_and_bounded_limits() {
    let (_directory, store, app) = public_device_app().await;
    let viewer_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = 'viewer'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, owner_user_id)
         VALUES ('device-001', 'One', ?),
                ('device-002', 'Two', ?),
                ('device-003', 'Three', ?)",
    )
    .bind(&viewer_id)
    .bind(&viewer_id)
    .bind(&viewer_id)
    .execute(store.pool())
    .await
    .unwrap();
    let token = oauth_bearer_token(&app, "viewer", "NanoView@1234", "devices:read").await;

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices?limit=2")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first: Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        first["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["device_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["device-001", "device-002"]
    );
    assert_eq!(first["has_more"], true);
    let cursor = first["next_cursor"].as_str().unwrap();
    assert_ne!(cursor, "device-002");

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/devices?after={cursor}&limit=2"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second: Value =
        serde_json::from_slice(&to_bytes(second.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        second["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["device_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["device-003"]
    );
    assert_eq!(second["next_cursor"], Value::Null);
    assert_eq!(second["has_more"], false);

    let invalid_limit = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices?limit=101")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_public_error(invalid_limit, StatusCode::BAD_REQUEST, "invalid_request").await;
}

#[tokio::test]
async fn public_device_list_is_exposed_only_from_the_public_router() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://public-devices:public-devices@localhost/public-devices")
        .unwrap();
    let routers = routers(ApiState::new(pool));

    let public = routers
        .public
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(public.status(), StatusCode::UNAUTHORIZED);

    let management = routers
        .management
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(management.status(), StatusCode::NOT_FOUND);
}
