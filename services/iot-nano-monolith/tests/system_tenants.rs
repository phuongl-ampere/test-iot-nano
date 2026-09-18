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

    let create_application = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/applications")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"app_id":"powermonitor","kind":"full_stack","launch_url":"https://powermonitor.example.test","client_id":"north-powermonitor-client","redirect_uris":["https://powermonitor.example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_application.status(), StatusCode::CREATED);

    let create_user = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/users")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"username":"north-user","password":"NorthUser@2026","default_app":"/apps/powermonitor","granted_apps":["powermonitor"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_user.status(), StatusCode::CREATED);

    let list_users = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/users")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_users.status(), StatusCode::OK);

    let system_users = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/users")
                .header(COOKIE, &system_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(system_users.status(), StatusCode::FORBIDDEN);

    let system_route = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"slug":"south","metadata":{},"tenant_account_password":"TenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(system_route.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn user_logs_in_with_tenant_slug_and_is_denied_tenant_management_routes() {
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
    let tenant_cookie = tenant_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let create_application = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/applications")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"app_id":"powermonitor","kind":"full_stack","launch_url":"https://powermonitor.example.test","client_id":"north-powermonitor-client","redirect_uris":["https://powermonitor.example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_application.status(), StatusCode::CREATED);
    let create_user = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/users")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"username":"north-user","password":"NorthUser@2026","default_app":"/apps/powermonitor","granted_apps":["powermonitor"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_user.status(), StatusCode::CREATED);

    let user_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/user/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"north","username":"north-user","password":"NorthUser@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(user_login.status(), StatusCode::OK);
    let user_cookie = user_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let user_me = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/user/auth/me")
                .header(COOKIE, &user_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(user_me.status(), StatusCode::OK);

    let tenant_users = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/users")
                .header(COOKIE, user_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tenant_users.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn system_account_controls_tenant_lifecycle_and_revokes_tenant_sessions() {
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
    let tenant_cookie = tenant_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let suspend = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants/north/suspend")
                .header(COOKIE, &system_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(suspend.status(), StatusCode::NO_CONTENT);
    let revoked_session = router
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
    assert_eq!(revoked_session.status(), StatusCode::UNAUTHORIZED);

    let reactivate = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants/north/reactivate")
                .header(COOKIE, &system_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reactivate.status(), StatusCode::NO_CONTENT);

    let reset = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants/north/tenant-account/reset")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &system_cookie)
                .body(Body::from(r#"{"password":"NewTenantAccount@2026"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reset.status(), StatusCode::NO_CONTENT);

    let old_password = router
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
    assert_eq!(old_password.status(), StatusCode::UNAUTHORIZED);

    let replacement_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"north","password":"NewTenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replacement_login.status(), StatusCode::OK);
    let replacement_cookie = replacement_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let disable = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants/north/tenant-account/disable")
                .header(COOKIE, &system_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disable.status(), StatusCode::NO_CONTENT);
    let disabled_session = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/tenant/auth/me")
                .header(COOKIE, replacement_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled_session.status(), StatusCode::UNAUTHORIZED);
    let disabled_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"north","password":"NewTenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled_login.status(), StatusCode::UNAUTHORIZED);

    let delete = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/tenants/north/delete")
                .header(COOKIE, system_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
}
