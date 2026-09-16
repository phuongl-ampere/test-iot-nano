use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION},
    },
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use iot_api::{
    ApiState, SqliteApiState, bootstrap_users_sqlite, router, routers, sqlite_router,
    sqlite_routers,
};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, PlatformStore, SqliteStore,
};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

const CLIENT_ID: &str = "router-split-oauth-client";
const REDIRECT_URI: &str = "https://client.example.test/callback";

fn test_state() -> ApiState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://router-split:router-split@localhost/router-split")
        .expect("the lazy test database URL is valid");
    ApiState::new(pool)
}

async fn sqlite_test_state() -> (tempfile::TempDir, SqliteApiState) {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("router-split.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let oauth_store = PlatformStore::open(&configuration).await.unwrap();
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: "router-split-oauth-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    (
        directory,
        SqliteApiState::new(store).with_oauth_store(oauth_store),
    )
}

fn s256_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

#[tokio::test]
async fn public_router_excludes_internal_session_management_and_docs_routes() {
    let app = routers(test_state()).public;

    for (method, uri) in [
        ("POST", "/internal/mqttd/session-resolution"),
        ("GET", "/api/auth/me"),
        ("GET", "/api/management/devices"),
        ("GET", "/docs/"),
        ("GET", "/api-docs/openapi.json"),
    ] {
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
            StatusCode::NOT_FOUND,
            "{method} {uri} must not be public"
        );
    }
}

#[tokio::test]
async fn management_router_keeps_session_routes_protected() {
    let response = routers(test_state())
        .management
        .oneshot(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn legacy_router_keeps_management_routes_without_retired_mqttd_transport_routes() {
    let app = router(test_state());

    let management = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(management.status(), StatusCode::UNAUTHORIZED);

    for uri in [
        "/internal/mqttd/session-resolution",
        "/internal/mqttd/session-authorization",
        "/internal/mqttd/gateway-authorization",
        "/internal/mqttd/rpc-response",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "{uri} must be retired from the legacy composite router"
        );
    }
}

#[tokio::test]
async fn sqlite_legacy_router_keeps_management_routes_without_retired_mqttd_transport_routes() {
    let (_directory, state) = sqlite_test_state().await;
    let app = sqlite_router(state);

    let management = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/auth/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(management.status(), StatusCode::UNAUTHORIZED);

    for uri in [
        "/internal/mqttd/session-resolution",
        "/internal/mqttd/session-authorization",
        "/internal/mqttd/gateway-authorization",
        "/internal/mqttd/rpc-response",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "{uri} must be retired from the SQLite composite router"
        );
    }
}

#[tokio::test]
async fn sqlite_public_router_mounts_oauth_and_public_device_routes() {
    let (_directory, state) = sqlite_test_state().await;
    let routers = sqlite_routers(state);
    let login = routers
        .management
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
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
    let verifier = "router-split-pkce-verifier-with-at-least-forty-three-characters";
    let challenge = s256_challenge(verifier);
    let authorization = routers
        .public
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=router-split&code_challenge={challenge}&code_challenge_method=S256"
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
    let token = routers
        .public
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&client_id={CLIENT_ID}&code_verifier={verifier}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token.status(), StatusCode::OK);
    let token: serde_json::Value =
        serde_json::from_slice(&to_bytes(token.into_body(), usize::MAX).await.unwrap()).unwrap();
    let access_token = token["access_token"].as_str().unwrap();
    let devices = routers
        .public
        .oneshot(
            Request::builder()
                .uri("/api/v1/devices")
                .header(AUTHORIZATION, format!("Bearer {access_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(devices.status(), StatusCode::OK);
}

#[tokio::test]
async fn sqlite_public_router_excludes_session_management_docs_and_internal_routes() {
    let (_directory, state) = sqlite_test_state().await;
    let app = sqlite_routers(state).public;

    for (method, uri) in [
        ("POST", "/api/auth/login"),
        ("GET", "/api/auth/me"),
        ("GET", "/api/management/devices"),
        ("GET", "/docs/"),
        ("GET", "/api-docs/openapi.json"),
        ("POST", "/internal/mqttd/session-resolution"),
        ("POST", "/internal/mqttd/session-authorization"),
        ("POST", "/internal/mqttd/gateway-authorization"),
        ("POST", "/internal/mqttd/rpc-response"),
    ] {
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
            StatusCode::NOT_FOUND,
            "{method} {uri} must not be public"
        );
    }
}

#[tokio::test]
async fn sqlite_management_router_mounts_session_management_and_docs_routes() {
    let (_directory, state) = sqlite_test_state().await;
    let routers = sqlite_routers(state);
    let login = routers
        .management
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let session_id = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(login.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let devices = routers
        .management
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/management/devices")
                .header(AUTHORIZATION, format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(devices.status(), StatusCode::OK);

    for uri in ["/docs/", "/api-docs/openapi.json"] {
        let response = routers
            .management
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri} must be mounted");
    }
}

#[tokio::test]
async fn sqlite_management_router_excludes_public_and_internal_routes() {
    let (_directory, state) = sqlite_test_state().await;
    let app = sqlite_routers(state).management;

    for (method, uri) in [
        ("GET", "/oauth/authorize"),
        ("POST", "/oauth/token"),
        ("GET", "/api/v1/devices"),
        ("POST", "/internal/mqttd/session-resolution"),
        ("POST", "/internal/mqttd/session-authorization"),
        ("POST", "/internal/mqttd/gateway-authorization"),
        ("POST", "/internal/mqttd/rpc-response"),
    ] {
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
            StatusCode::NOT_FOUND,
            "{method} {uri} must not be a management route"
        );
    }
}
