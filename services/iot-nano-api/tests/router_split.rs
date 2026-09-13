use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use iot_api::{ApiState, routers};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn test_state() -> ApiState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://router-split:router-split@localhost/router-split")
        .expect("the lazy test database URL is valid");
    ApiState::new(pool)
}

#[tokio::test]
async fn public_router_excludes_mqttd_internal_routes() {
    let response = routers(test_state())
        .public
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/session-resolution")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
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
