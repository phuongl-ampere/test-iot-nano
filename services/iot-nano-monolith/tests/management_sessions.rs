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
use iot_api::{OAuthBrowserSessionVerifier, TokenVault, bootstrap_users_sqlite};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{BootstrapAdminError, ManagementSessionRouter, bootstrap_admin};
use iot_storage::PlatformStore;
use serde_json::json;
use std::net::SocketAddr;
use tower::ServiceExt;

async fn management_session_router() -> (tempfile::TempDir, ManagementSessionRouter) {
    let (directory, _store, management) = management_session_router_with_store().await;
    (directory, management)
}

async fn management_session_router_with_store() -> (
    tempfile::TempDir,
    Arc<PlatformStore>,
    ManagementSessionRouter,
) {
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
    let management = ManagementSessionRouter::new(Arc::clone(&store), test_token_vault());
    let router = management
        .router
        .clone()
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    (
        directory,
        store,
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

    let management = ManagementSessionRouter::new(store, test_token_vault());
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
    let management = ManagementSessionRouter::new(Arc::clone(&store), test_token_vault());
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

#[tokio::test]
async fn management_admin_can_provision_a_device_token() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let login = router
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let provisioned = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(r#"{"display_name":"Provisioned Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provisioned.status(), StatusCode::CREATED);
    let payload: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(provisioned.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        payload["token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );
}

#[tokio::test]
async fn management_admin_can_rotate_an_existing_device_token() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let login = router
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(r#"{"display_name":"Rotated Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provisioned.status(), StatusCode::CREATED);
    let provisioned: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(provisioned.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let device_id = provisioned["device_id"].as_str().unwrap();
    let first_token = provisioned["token"].as_str().unwrap();

    let rotated = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/management/devices/{device_id}/tokens"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::CREATED);
    let rotated: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(rotated.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(rotated["device_id"], device_id);
    assert!(
        rotated["token"]
            .as_str()
            .is_some_and(|token| token != first_token)
    );
}

#[tokio::test]
async fn management_admin_manages_devices_through_the_typed_storage_port() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let login = router
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let gateway = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"Gateway"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let gateway: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(gateway.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let gateway_id = gateway["device_id"].as_str().unwrap().to_owned();

    let child = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"Child"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let child: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(child.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let child_id = child["device_id"].as_str().unwrap().to_owned();

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/devices")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);

    let gateway_topology = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/devices/{gateway_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Gateway",
                        "topology": { "is_gateway": true },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gateway_topology.status(), StatusCode::OK);

    for request in [
        json!({
            "display_name": "Child",
            "asset_id": uuid::Uuid::now_v7(),
        }),
        json!({
            "display_name": "Child",
            "device_profile_id": uuid::Uuid::now_v7(),
        }),
        json!({
            "display_name": "Child",
            "topology": { "is_gateway": false, "gateway_device_id": "missing-gateway" },
        }),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/management/devices/{child_id}"))
                    .header(CONTENT_TYPE, "application/json")
                    .header(COOKIE, &cookie)
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    let assigned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/devices/{child_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Child",
                        "topology": { "is_gateway": false, "gateway_device_id": gateway_id.clone() },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(assigned.status(), StatusCode::OK);

    let gateway_delete = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/devices/{gateway_id}"))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gateway_delete.status(), StatusCode::CONFLICT);

    let child_delete = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/devices/{child_id}"))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(child_delete.status(), StatusCode::NO_CONTENT);

    let gateway_delete = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/devices/{gateway_id}"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gateway_delete.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn management_device_routes_require_an_admin_and_map_typed_errors() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;

    let anonymous = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    let viewer_login = router
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
    let viewer_cookie = viewer_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let viewer = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/devices")
                .header(COOKIE, viewer_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(viewer.status(), StatusCode::FORBIDDEN);

    let admin_login = router
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
    let admin_cookie = admin_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let invalid_id = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/management/devices/not.valid")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &admin_cookie)
                .body(Body::from(r#"{"display_name":"Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_id.status(), StatusCode::BAD_REQUEST);

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/management/devices/missing-device")
                .header(COOKIE, &admin_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let gateway = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &admin_cookie)
                .body(Body::from(r#"{"display_name":"Gateway"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let gateway: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(gateway.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let gateway_id = gateway["device_id"].as_str().unwrap();
    let invalid_topology = router
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/devices/{gateway_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, admin_cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Gateway",
                        "topology": { "is_gateway": true, "gateway_device_id": "other-gateway" },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_topology.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn management_mutations_require_admin_and_map_token_errors() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;

    for request in [
        Request::builder()
            .method("POST")
            .uri("/api/management/applications")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"app_id":"test-app","kind":"frontend","launch_url":"https://example.test","client_id":"test-client","redirect_uris":["https://example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
            ))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"display_name":"Anonymous"}"#))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/management/devices/missing-device/tokens")
            .body(Body::empty())
            .unwrap(),
    ] {
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let invalid_session = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, "iot_nano_session=invalid")
                .body(Body::from(r#"{"display_name":"Invalid session"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_session.status(), StatusCode::UNAUTHORIZED);

    let malformed_anonymous = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed_anonymous.status(), StatusCode::UNAUTHORIZED);

    let viewer_login = router
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
    let viewer_cookie = viewer_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let malformed_viewer = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, viewer_cookie)
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed_viewer.status(), StatusCode::FORBIDDEN);

    let login = router
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices/missing-device/tokens")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let gateway = management_provision_device(&router, &cookie, "Gateway").await;
    let gateway_id = gateway["device_id"].as_str().unwrap();
    let gateway_update = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/devices/{gateway_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Gateway",
                        "topology": { "is_gateway": true },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gateway_update.status(), StatusCode::OK);

    let child = management_provision_device(&router, &cookie, "Child").await;
    let child_id = child["device_id"].as_str().unwrap();
    let child_update = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/devices/{child_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Child",
                        "topology": { "is_gateway": false, "gateway_device_id": gateway_id },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(child_update.status(), StatusCode::OK);

    let child_token = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/management/devices/{child_id}/tokens"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(child_token.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn management_mutation_reports_unavailable_when_the_store_closes_after_login() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let login = router
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    store.sqlite_pool().unwrap().close().await;

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(r#"{"display_name":"Unavailable"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn management_mutation_preserves_json_rejection_semantics_after_authorization() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let login = router
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
    let cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let missing_content_type = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"No content type"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        missing_content_type.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );

    let oversized_body = json!({ "display_name": "x".repeat(2 * 1024 * 1024) }).to_string();
    let oversized = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(oversized_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn management_mutation_authorization_precedes_json_rejection_for_every_body_route() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let viewer_login = router
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
    let viewer_cookie = viewer_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    for (method, path) in [
        ("POST", "/api/management/applications"),
        ("POST", "/api/management/devices"),
        ("PUT", "/api/management/devices/not-valid"),
    ] {
        let anonymous = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED, "{path}");

        let viewer = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(CONTENT_TYPE, "application/json")
                    .header(COOKIE, &viewer_cookie)
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(viewer.status(), StatusCode::FORBIDDEN, "{path}");
    }
}

async fn management_provision_device(
    router: &axum::Router,
    cookie: &str,
    display_name: &str,
) -> serde_json::Value {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(
                    json!({ "display_name": display_name }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap()
}

fn invalid_login_request() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"username":"admin","password":"wrong"}"#))
        .unwrap()
}

fn test_token_vault() -> TokenVault {
    TokenVault::from_key_material("management-session-test-vault-key-material-0001")
}
