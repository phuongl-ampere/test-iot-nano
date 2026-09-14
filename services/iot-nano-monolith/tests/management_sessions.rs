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
use iot_nano_monolith::{BootstrapAdminError, ManagementSessionRouter, bootstrap_admin};
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

#[tokio::test]
async fn bootstrap_admin_creates_the_only_initial_user_and_enables_management_login() {
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
    bootstrap_admin(&store, "initial-admin", "BootstrapAdmin@2026")
        .await
        .unwrap();
    assert!(matches!(
        bootstrap_admin(&store, "second-admin", "BootstrapAdmin@2026").await,
        Err(BootstrapAdminError::AlreadyInitialized)
    ));
    let grants: Vec<String> = sqlx::query_scalar(
        "SELECT app_key FROM user_app_grants WHERE user_id = ? ORDER BY app_key",
    )
    .bind(
        sqlx::query_scalar::<_, String>("SELECT id FROM users WHERE username = 'initial-admin'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
    )
    .fetch_all(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(grants, vec!["powermonitor"]);

    let management = ManagementSessionRouter::new(store);
    let router = management
        .router
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"initial-admin","password":"BootstrapAdmin@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn management_admin_can_register_an_oauth_application() {
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
    bootstrap_admin(&store, "initial-admin", "BootstrapAdmin@2026")
        .await
        .unwrap();
    let management = ManagementSessionRouter::new(Arc::clone(&store));
    let router = management
        .router
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"initial-admin","password":"BootstrapAdmin@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/applications")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(
                    r#"{"app_id":"alpha-client-app","kind":"full_stack","launch_url":"https://client.example.test","client_id":"alpha-client","redirect_uris":["https://client.example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true,"client_secret":"alpha-client-secret"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let applications: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM applications WHERE app_id = 'alpha-client-app'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(applications, 1);
}

fn invalid_login_request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
        .unwrap()
}
