use std::sync::Arc;

use axum::{
    Extension,
    body::Body,
    extract::ConnectInfo,
    http::{
        HeaderMap, HeaderValue, Request, StatusCode,
        header::{CONTENT_TYPE, COOKIE, SET_COOKIE},
    },
};
use iot_api::{OAuthBrowserSessionVerifier, bootstrap_users_sqlite};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::ManagementSessionRouter;
use iot_storage::PlatformStore;
use std::net::SocketAddr;
use tower::ServiceExt;

async fn management_session_router() -> (tempfile::TempDir, ManagementSessionRouter) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("platform.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    bootstrap_users_sqlite(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let management = ManagementSessionRouter::new(store);
    let router = management
        .router
        .clone()
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    (
        directory,
        ManagementSessionRouter {
            router,
            session_verifier: management.session_verifier,
        },
    )
}

#[tokio::test]
async fn management_login_issues_a_cookie_used_by_the_oauth_session_verifier_and_logout_revokes_it()
{
    let (_directory, management) = management_session_router().await;
    let app = management.router.clone();
    let login = app
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let mut headers = HeaderMap::new();
    headers.insert(COOKIE, HeaderValue::from_str(&cookie).unwrap());
    assert!(
        management
            .session_verifier
            .authenticated_user_id(&headers)
            .is_some()
    );

    let logout = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/logout")
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert!(
        management
            .session_verifier
            .authenticated_user_id(&headers)
            .is_none()
    );
}

#[tokio::test]
async fn management_login_rejects_invalid_credentials_without_setting_a_session_cookie() {
    let (_directory, management) = management_session_router().await;
    let response = management
        .router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().get(SET_COOKIE).is_none());
}

#[tokio::test]
async fn management_login_rate_limits_repeated_invalid_credentials() {
    let (_directory, management) = management_session_router().await;
    let app = management.router;

    for _ in 0..5 {
        let response = app.clone().oneshot(invalid_login_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let response = app.oneshot(invalid_login_request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

fn invalid_login_request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
        .unwrap()
}
