use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use iot_api::{ApiState, router, routers};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn test_state() -> ApiState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://router-split:router-split@localhost/router-split")
        .expect("the lazy test database URL is valid");
    ApiState::new(pool)
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
