use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use iot_api::{ApiState, SqliteApiState, router, routers, sqlite_routers};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::SqliteStore;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

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
    let store = SqliteStore::open(&configuration).await.unwrap();
    (directory, SqliteApiState::new(store))
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
async fn legacy_router_keeps_management_and_internal_routes() {
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

    let internal = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/session-resolution")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"client_id":"router-split","username":"unused"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        internal.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "the legacy internal route must be matched before its unconfigured authentication fails"
    );
}

#[tokio::test]
async fn sqlite_public_router_mounts_oauth_and_public_device_routes() {
    let (_directory, state) = sqlite_test_state().await;
    let app = sqlite_routers(state).public;

    for (method, uri, body, expected_status) in [
        (
            "GET",
            "/oauth/authorize",
            Body::empty(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "POST",
            "/oauth/token",
            Body::from("grant_type=client_credentials"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            "GET",
            "/api/v1/devices",
            Body::empty(),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let mut request = Request::builder().method(method).uri(uri);
        if method == "POST" {
            request = request.header("content-type", "application/x-www-form-urlencoded");
        }
        let response = app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            expected_status,
            "{method} {uri} must be mounted on the SQLite public router"
        );
    }
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

    for (method, uri, body, expected_status) in [
        (
            "POST",
            "/api/auth/login",
            Body::from(r#"{"username":"missing","password":"missing"}"#),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/auth/me",
            Body::empty(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/management/devices",
            Body::empty(),
            StatusCode::UNAUTHORIZED,
        ),
        ("GET", "/docs/", Body::empty(), StatusCode::OK),
        (
            "GET",
            "/api-docs/openapi.json",
            Body::empty(),
            StatusCode::OK,
        ),
    ] {
        let mut request = Request::builder().method(method).uri(uri);
        if method == "POST" {
            request = request.header("content-type", "application/json");
        }
        let response = routers
            .management
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            expected_status,
            "{method} {uri} must be mounted on the SQLite management router"
        );
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
