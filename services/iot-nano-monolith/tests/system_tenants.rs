use std::{net::SocketAddr, sync::Arc};

use axum::{
    Extension,
    body::Body,
    extract::ConnectInfo,
    http::{
        Request, StatusCode,
        header::{CONTENT_TYPE, COOKIE, SET_COOKIE},
    },
};
use iot_api::TokenVault;
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{ManagementSessionRouter, bootstrap_system};
use iot_storage::PlatformStore;
use tower::ServiceExt;

async fn system_router() -> (tempfile::TempDir, axum::Router) {
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
    bootstrap_system(&store, "system", "SystemAccount@2026")
        .await
        .unwrap();
    let management = ManagementSessionRouter::new(
        store,
        TokenVault::from_key_material("tenant-system-test-vault-key-material-0001"),
    );
    let router = management
        .router
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    (directory, router)
}

#[tokio::test]
async fn system_account_logs_in_and_creates_a_tenant_without_tenant_resource_access() {
    let (_directory, router) = system_router().await;
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"system","password":"SystemAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let system_cookie = login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let tenant = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &system_cookie)
                .body(Body::from(
                    r#"{"slug":"north","metadata":{"region":"north"},"tenant_account_password":"TenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tenant.status(), StatusCode::CREATED);

    let tenant_resource = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/devices")
                .header(COOKIE, system_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tenant_resource.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_account_logs_in_only_for_its_tenant_and_is_denied_system_routes() {
    let (_directory, router) = system_router().await;
    let system_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"system","password":"SystemAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let system_cookie = system_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let create_tenant = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &system_cookie)
                .body(Body::from(
                    r#"{"slug":"north","metadata":{},"tenant_account_password":"TenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_tenant.status(), StatusCode::CREATED);

    let tenant_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"north","password":"TenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tenant_login.status(), StatusCode::OK);
    let tenant_cookie = tenant_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let tenant_me = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/tenant/auth/me")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tenant_me.status(), StatusCode::OK);

    let system_route = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, tenant_cookie)
                .body(Body::from(
                    r#"{"slug":"south","metadata":{},"tenant_account_password":"TenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(system_route.status(), StatusCode::FORBIDDEN);
}
