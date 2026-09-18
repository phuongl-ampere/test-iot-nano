use std::sync::Arc;

use axum::{
    Extension,
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{
        HeaderMap, HeaderValue, Request, StatusCode,
        header::{CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
    },
};
use iot_api::{OAuthBrowserSessionVerifier, TokenVault, hash_password};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{ManagementSessionRouter, bootstrap_system};
use iot_storage::{
    NewTenant, NewTenantAccount, PlatformStore, TenantIdentityRepository, TenantStatus,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
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
    seed_tenant_admin_users(&store).await;
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

async fn seed_tenant_admin_users(store: &PlatformStore) {
    bootstrap_system(store, "system", "SystemAccount@2026")
        .await
        .unwrap();
    let (tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store,
        NewTenant {
            slug: "test".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("TenantAccount@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO applications (
            app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
         ) VALUES ('powermonitor', ?, 'frontend', '/apps/powermonitor', ?, '[]', 1)",
    )
    .bind(tenant.id.to_string())
    .bind(format!("seed-{}-powermonitor", tenant.id))
    .execute(pool)
    .await
    .unwrap();
    for (username, password, role, account_class) in [
        ("admin", "NanoAdmin@1234", "admin", "admin"),
        ("viewer", "NanoView@1234", "viewer", "user"),
    ] {
        let user_id = uuid::Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO users (
                id, tenant_id, username, password_hash, role, account_class, default_app
             ) VALUES (?, ?, ?, ?, ?, ?, '/apps/powermonitor')",
        )
        .bind(&user_id)
        .bind(tenant.id.to_string())
        .bind(username)
        .bind(hash_password(password).unwrap())
        .bind(role)
        .bind(account_class)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, tenant_id, app_key)
             VALUES (?, ?, 'powermonitor')",
        )
        .bind(user_id)
        .bind(tenant.id.to_string())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn tenant_account_cookie(router: &axum::Router) -> String {
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"test","password":"TenantAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn management_user_cookie(router: &axum::Router, username: &str, password: &str) -> String {
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "username": username, "password": password }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn system_account_cookie(router: &axum::Router) -> String {
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
    login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn user_account_cookie(router: &axum::Router) -> String {
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/user/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"test","username":"viewer","password":"NanoView@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

fn platform_get(path: &str, cookie: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().uri(path);
    if let Some(cookie) = cookie {
        request = request.header(COOKIE, cookie);
    }
    request.body(Body::empty()).unwrap()
}

fn system_lifecycle_form(path: &str, cookie: Option<&str>, body: &str) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(cookie) = cookie {
        request = request.header(COOKIE, cookie);
    }
    request.body(Body::from(body.to_owned())).unwrap()
}

async fn tenant_account_login_status(
    router: &axum::Router,
    tenant_slug: &str,
    password: &str,
) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "tenant_slug": tenant_slug, "password": password }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn seed_user_workspace_devices(store: &PlatformStore) {
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let viewer_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let group_id = uuid::Uuid::now_v7().to_string();
    let inherited_asset_id = uuid::Uuid::now_v7().to_string();

    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Shared asset')")
        .bind(&inherited_asset_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .unwrap();

    for (device_id, display_name, owner_user_id, asset_id) in [
        (
            "owned-device",
            "Owned device",
            Some(viewer_id.as_str()),
            None,
        ),
        ("direct-device", "Direct device", None, None),
        ("group-device", "Group device", None, None),
        (
            "inherited-device",
            "Inherited device",
            None,
            Some(inherited_asset_id.as_str()),
        ),
        ("unshared-device", "Unshared device", None, None),
    ] {
        sqlx::query(
            "INSERT INTO devices (
                device_id, tenant_id, display_name, owner_user_id, asset_id, last_seen_at
             ) VALUES (?, ?, ?, ?, ?, '2026-09-18T10:20:30Z')",
        )
        .bind(device_id)
        .bind(&tenant_id)
        .bind(display_name)
        .bind(owner_user_id)
        .bind(asset_id)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'Workspace operators')",
    )
    .bind(&group_id)
    .bind(&tenant_id)
    .bind(&viewer_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO user_group_members (tenant_id, group_id, user_id)
         VALUES (?, ?, ?)",
    )
    .bind(&tenant_id)
    .bind(&group_id)
    .bind(&viewer_id)
    .execute(pool)
    .await
    .unwrap();

    for (subject_user_id, subject_group_id, asset_id, device_id, permission, inherit_children) in [
        (
            Some(viewer_id.as_str()),
            None,
            None,
            Some("direct-device"),
            "viewer",
            0_i64,
        ),
        (
            None,
            Some(group_id.as_str()),
            None,
            Some("group-device"),
            "manager",
            0_i64,
        ),
        (
            Some(viewer_id.as_str()),
            None,
            Some(inherited_asset_id.as_str()),
            None,
            "viewer",
            1_i64,
        ),
    ] {
        sqlx::query(
            "INSERT INTO resource_permissions (
                id, tenant_id, subject_user_id, subject_group_id, asset_id, device_id,
                permission, inherit_children, created_by_user_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(&tenant_id)
        .bind(subject_user_id)
        .bind(subject_group_id)
        .bind(asset_id)
        .bind(device_id)
        .bind(permission)
        .bind(inherit_children)
        .bind(&viewer_id)
        .execute(pool)
        .await
        .unwrap();
    }

    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store,
        NewTenant {
            slug: "other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, last_seen_at)
         VALUES ('other-tenant-device', ?, 'Cross tenant device', '2026-09-18T10:20:30Z')",
    )
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn platform_root_redirects_each_authenticated_session_kind_to_its_workspace() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (cookie, destination) in [
        (&system_cookie, "/system"),
        (&tenant_cookie, "/tenant"),
        (&user_cookie, "/app"),
    ] {
        let response = router
            .clone()
            .oneshot(platform_get("/", Some(cookie)))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers()[LOCATION], destination);
    }
}

#[tokio::test]
async fn platform_routes_deny_unauthenticated_and_cross_kind_sessions() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for path in ["/", "/system", "/tenant", "/app"] {
        let response = router
            .clone()
            .oneshot(platform_get(path, None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
    }

    for (cookie, path) in [
        (&system_cookie, "/tenant"),
        (&system_cookie, "/app"),
        (&tenant_cookie, "/system"),
        (&tenant_cookie, "/app"),
        (&user_cookie, "/system"),
        (&user_cookie, "/tenant"),
    ] {
        let response = router
            .clone()
            .oneshot(platform_get(path, Some(cookie)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }
}

#[tokio::test]
async fn platform_routes_render_the_matching_server_layout_and_local_css() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (cookie, path, heading) in [
        (&system_cookie, "/system", "System Console"),
        (&tenant_cookie, "/tenant", "Tenant Console"),
        (&user_cookie, "/app", "My Devices"),
    ] {
        let response = router
            .clone()
            .oneshot(platform_get(path, Some(cookie)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/html"),
            "{path}"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains(heading), "{path}");
        assert!(body.contains("/assets/platform-ui.css"), "{path}");
    }

    let stylesheet = router
        .oneshot(platform_get("/assets/platform-ui.css", None))
        .await
        .unwrap();
    assert_eq!(stylesheet.status(), StatusCode::OK);
    assert!(
        stylesheet.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/css")
    );
    let body = to_bytes(stylesheet.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains(".console-shell"));
}

#[tokio::test]
async fn system_tenants_navigation_resolves_to_the_system_overview_for_a_system_session() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;

    let response = router
        .oneshot(platform_get("/system", Some(&system_cookie)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("href=\"/system\">Tenants</a>"));
}

#[tokio::test]
async fn user_workspace_lists_only_authorized_devices_with_server_resolved_access() {
    let (_directory, store, management) = management_session_router_with_store().await;
    seed_user_workspace_devices(store.as_ref()).await;

    let router = management.router;
    let user_cookie = user_account_cookie(&router).await;
    let response = router
        .oneshot(platform_get("/app", Some(&user_cookie)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    for (device_name, permission, access_source) in [
        ("Owned device", "Owner", "Owner"),
        ("Direct device", "Viewer", "Direct user permission"),
        ("Group device", "Manager", "Group permission"),
        ("Inherited device", "Viewer", "Inherited user permission"),
    ] {
        assert!(body.contains(device_name), "missing {device_name}");
        assert!(body.contains(permission), "missing {permission}");
        assert!(body.contains(access_source), "missing {access_source}");
    }
    assert!(body.contains("2026-09-18T10:20:30Z"));
    assert!(!body.contains("Unshared device"));
    assert!(!body.contains("Cross tenant device"));
    assert!(!body.contains("href=\"/system"));
    assert!(!body.contains("href=\"/tenant"));
    assert!(!body.contains("/commands"));
    assert!(!body.contains("<form"));
}

#[tokio::test]
async fn user_workspace_device_detail_masks_unavailable_devices_and_denies_other_sessions() {
    let (_directory, store, management) = management_session_router_with_store().await;
    seed_user_workspace_devices(store.as_ref()).await;

    let router = management.router;
    let user_cookie = user_account_cookie(&router).await;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let system_cookie = system_account_cookie(&router).await;

    let authorized = router
        .clone()
        .oneshot(platform_get(
            "/app/devices/direct-device",
            Some(&user_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
    let authorized_body = to_bytes(authorized.into_body(), usize::MAX).await.unwrap();
    let authorized_body = String::from_utf8(authorized_body.to_vec()).unwrap();
    assert!(authorized_body.contains("Direct device"));
    assert!(authorized_body.contains("Viewer"));
    assert!(authorized_body.contains("Direct user permission"));
    assert!(!authorized_body.contains("<form"));
    assert!(!authorized_body.contains("/commands"));

    for device_id in ["other-tenant-device", "guessed-device"] {
        let response = router
            .clone()
            .oneshot(platform_get(
                &format!("/app/devices/{device_id}"),
                Some(&user_cookie),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{device_id}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Device unavailable"));
        assert!(!body.contains("Cross tenant device"));
    }

    for cookie in [&tenant_cookie, &system_cookie] {
        let response = router
            .clone()
            .oneshot(platform_get("/app/devices/direct-device", Some(cookie)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn system_page_lists_only_tenant_slug_and_status_with_neutral_runtime_fields() {
    let (_directory, store, management) = management_session_router_with_store().await;
    TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "suspended".to_owned(),
            metadata: json!({
                "private_token": "system-page-private-metadata",
            }),
        },
        NewTenantAccount {
            password_hash: hash_password("SeparateTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    TenantIdentityRepository::suspend_tenant(store.as_ref(), "suspended")
        .await
        .unwrap();

    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let response = router
        .oneshot(platform_get("/system", Some(&system_cookie)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("<td>test</td>"));
    assert!(body.contains("<td>active</td>"));
    assert!(body.contains("<td>suspended</td>"));
    assert_eq!(body.matches("Not reported").count(), 3);
    assert!(!body.contains("system-page-private-metadata"));
    assert!(!body.contains("SeparateTenant@2026"));
}

#[tokio::test]
async fn system_page_does_not_read_sensitive_invalid_tenant_metadata() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let metadata = "system-page-secret: do-not-read";
    sqlx::query("UPDATE tenants SET metadata = ? WHERE slug = ?")
        .bind(metadata)
        .bind("test")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let response = router
        .oneshot(platform_get("/system", Some(&system_cookie)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("<td>test</td>"));
    assert!(!body.contains(metadata));
}

#[tokio::test]
async fn system_html_forms_manage_tenant_lifecycle_with_non_secret_notices() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;

    let create = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants",
            Some(&system_cookie),
            "slug=ui-tenant&tenant_account_password=ChosenTenant%402026",
        ))
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::SEE_OTHER);
    assert_eq!(create.headers()[LOCATION], "/system?notice=tenant-created");
    assert!(
        TenantIdentityRepository::list_tenant_summaries(store.as_ref())
            .await
            .unwrap()
            .iter()
            .any(|tenant| tenant.slug == "ui-tenant" && tenant.status == TenantStatus::Active)
    );

    let suspend = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants/suspend",
            Some(&system_cookie),
            "slug=ui-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(suspend.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        suspend.headers()[LOCATION],
        "/system?notice=tenant-suspended"
    );
    assert!(
        TenantIdentityRepository::list_tenant_summaries(store.as_ref())
            .await
            .unwrap()
            .iter()
            .any(|tenant| tenant.slug == "ui-tenant" && tenant.status == TenantStatus::Suspended)
    );

    let reactivate = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants/reactivate",
            Some(&system_cookie),
            "slug=ui-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(reactivate.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        reactivate.headers()[LOCATION],
        "/system?notice=tenant-reactivated"
    );
    assert!(
        TenantIdentityRepository::list_tenant_summaries(store.as_ref())
            .await
            .unwrap()
            .iter()
            .any(|tenant| tenant.slug == "ui-tenant" && tenant.status == TenantStatus::Active)
    );

    let reset = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants/tenant-account/reset",
            Some(&system_cookie),
            "slug=ui-tenant&password=ReplacementTenant%402026",
        ))
        .await
        .unwrap();
    assert_eq!(reset.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        reset.headers()[LOCATION],
        "/system?notice=tenant-account-reset"
    );
    assert_eq!(
        tenant_account_login_status(&router, "ui-tenant", "ChosenTenant@2026").await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        tenant_account_login_status(&router, "ui-tenant", "ReplacementTenant@2026").await,
        StatusCode::OK
    );

    let delete = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants/delete",
            Some(&system_cookie),
            "slug=ui-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::SEE_OTHER);
    assert_eq!(delete.headers()[LOCATION], "/system?notice=tenant-deleted");
    assert!(
        TenantIdentityRepository::list_tenant_summaries(store.as_ref())
            .await
            .unwrap()
            .iter()
            .all(|tenant| tenant.slug != "ui-tenant")
    );

    let page = router
        .oneshot(platform_get(
            "/system?notice=tenant-deleted",
            Some(&system_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("Tenant deleted."));
    assert!(!page.contains("ChosenTenant@2026"));
    assert!(!page.contains("ReplacementTenant@2026"));
}

#[tokio::test]
async fn system_html_lifecycle_forms_deny_non_system_sessions_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (cookie, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(tenant_cookie.as_str()), StatusCode::FORBIDDEN),
        (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
    ] {
        for path in [
            "/system/tenants",
            "/system/tenants/suspend",
            "/system/tenants/reactivate",
            "/system/tenants/delete",
            "/system/tenants/tenant-account/reset",
        ] {
            let response = router
                .clone()
                .oneshot(system_lifecycle_form(path, cookie, "%"))
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{path}");
        }
    }
}

#[tokio::test]
async fn system_html_lifecycle_forms_redirect_invalid_input_without_reflecting_passwords() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;

    let response = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants",
            Some(&system_cookie),
            "slug=invalid-tenant&tenant_account_password=too-short",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        response.headers()[LOCATION],
        "/system?notice=invalid-request"
    );

    let page = router
        .oneshot(platform_get(
            "/system?notice=invalid-request",
            Some(&system_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("Request could not be processed."));
    assert!(!page.contains("invalid-tenant"));
    assert!(!page.contains("too-short"));
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
async fn management_openapi_has_only_the_operator_route_allowlist_without_sensitive_material() {
    let (_directory, management) = management_session_router().await;
    let response = management
        .router
        .oneshot(
            Request::builder()
                .uri("/api-docs/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let document: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(
        document["components"]["securitySchemes"]["managementSession"]["type"],
        "apiKey"
    );
    assert_eq!(
        document["components"]["securitySchemes"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["managementSession"])
    );

    let paths = document["paths"].as_object().unwrap();
    let actual_paths: BTreeSet<_> = paths.keys().map(String::as_str).collect();
    let expected_paths = BTreeSet::from([
        "/api/auth/login",
        "/api/auth/logout",
        "/api/auth/me",
        "/api/management/applications",
        "/api/management/assets",
        "/api/management/assets/{asset_id}",
        "/api/management/devices",
        "/api/management/devices/{device_id}",
        "/api/management/devices/{device_id}/tokens",
        "/api/management/profiles/asset-profiles",
        "/api/management/profiles/asset-profiles/{profile_id}",
        "/api/management/profiles/device-profiles",
        "/api/management/profiles/device-profiles/{profile_id}",
        "/api/management/users",
        "/api/management/users/{username}",
    ]);
    assert_eq!(actual_paths, expected_paths);

    let expected_methods = BTreeMap::from([
        ("/api/auth/login", BTreeSet::from(["post"])),
        ("/api/auth/logout", BTreeSet::from(["post"])),
        ("/api/auth/me", BTreeSet::from(["get"])),
        ("/api/management/applications", BTreeSet::from(["post"])),
        ("/api/management/assets", BTreeSet::from(["get", "post"])),
        (
            "/api/management/assets/{asset_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        ("/api/management/devices", BTreeSet::from(["get", "post"])),
        (
            "/api/management/devices/{device_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        (
            "/api/management/devices/{device_id}/tokens",
            BTreeSet::from(["post"]),
        ),
        (
            "/api/management/profiles/asset-profiles",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/management/profiles/asset-profiles/{profile_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        (
            "/api/management/profiles/device-profiles",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/management/profiles/device-profiles/{profile_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        ("/api/management/users", BTreeSet::from(["get", "post"])),
        ("/api/management/users/{username}", BTreeSet::from(["put"])),
    ]);
    for (path, expected_methods) in expected_methods {
        let actual_methods = paths[path]
            .as_object()
            .unwrap()
            .keys()
            .filter(|key| key.as_str() != "parameters")
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_methods, expected_methods, "methods for {path}");
    }

    for (path, method) in [
        ("/api/management/devices", "post"),
        ("/api/management/devices/{device_id}/tokens", "post"),
    ] {
        assert_eq!(
            paths[path][method]["responses"]["201"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/DeviceToken"
        );
    }

    let schemas = document["components"]["schemas"].as_object().unwrap();
    let actual_schemas: BTreeSet<_> = schemas.keys().map(String::as_str).collect();
    let expected_schemas = BTreeSet::from([
        "ApplicationRequest",
        "ApplicationResponse",
        "AssetProfile",
        "AssetProfileList",
        "AssetProfileRequest",
        "DeviceProfile",
        "DeviceProfileList",
        "DeviceProfileRequest",
        "DeviceProvisionRequest",
        "DeviceToken",
        "Error",
        "LoginRequest",
        "ManagementAsset",
        "ManagementAssetList",
        "ManagementAssetRequest",
        "ManagementDevice",
        "ManagementDeviceList",
        "ManagementDeviceUpdateRequest",
        "ManagementUser",
        "ManagementUserCreateRequest",
        "ManagementUserList",
        "ManagementUserUpdateRequest",
        "SessionResponse",
    ]);
    assert_eq!(actual_schemas, expected_schemas);

    let device_token = &schemas["DeviceToken"];
    assert_eq!(device_token["type"], "object");
    assert_eq!(device_token["additionalProperties"], false);
    assert_eq!(
        device_token["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "created_at",
            "device_id",
            "id",
            "last_used_at",
            "revoked_at",
            "token",
            "token_prefix",
        ])
    );
    assert_eq!(
        device_token["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "created_at",
            "device_id",
            "id",
            "last_used_at",
            "revoked_at",
            "token_prefix",
        ])
    );
    assert_eq!(device_token["properties"]["id"]["type"], "string");
    assert_eq!(device_token["properties"]["id"]["format"], "uuid");
    assert_eq!(
        device_token["properties"]["created_at"],
        json!({"type": "string", "format": "date-time"})
    );
    for field in ["last_used_at", "revoked_at"] {
        assert_eq!(
            device_token["properties"][field],
            json!({"type": ["string", "null"], "format": "date-time"})
        );
    }
    assert_eq!(device_token["properties"]["token"]["type"], "string");

    for (schema_name, field_name) in [
        ("SessionResponse", "user_id"),
        ("ManagementUser", "id"),
        ("DeviceProfile", "id"),
        ("AssetProfile", "id"),
        ("DeviceToken", "id"),
        ("ManagementAsset", "id"),
    ] {
        assert_eq!(
            schemas[schema_name]["properties"][field_name],
            json!({"type": "string", "format": "uuid"}),
            "{schema_name}.{field_name} must be non-null"
        );
    }
    for (schema_name, field_name) in [
        ("ManagementDeviceUpdateRequest", "asset_id"),
        ("ManagementDeviceUpdateRequest", "device_profile_id"),
        ("ManagementDevice", "asset_id"),
        ("ManagementDevice", "device_profile_id"),
        ("ManagementAssetRequest", "asset_profile_id"),
        ("ManagementAssetRequest", "parent_asset_id"),
        ("ManagementAsset", "asset_profile_id"),
        ("ManagementAsset", "parent_asset_id"),
    ] {
        assert_eq!(
            schemas[schema_name]["properties"][field_name],
            json!({"type": ["string", "null"], "format": "uuid"}),
            "{schema_name}.{field_name} must remain nullable"
        );
    }

    for forbidden_schema in [
        "AccessToken",
        "AlertPage",
        "AssetPage",
        "Command",
        "CommandRequest",
        "DevicePage",
        "GrantPage",
        "OAuthTokenRequest",
        "PublicAlert",
        "PublicAsset",
        "PublicAssetRequest",
        "PublicDevice",
        "PublicDeviceRequest",
        "ResourceGrant",
        "ResourceGrantRequest",
        "Telemetry",
        "TelemetryPage",
    ] {
        assert!(
            !schemas.contains_key(forbidden_schema),
            "management OpenAPI exposed public schema: {forbidden_schema}"
        );
    }

    let logout = &document["paths"]["/api/auth/logout"]["post"];
    assert!(logout.get("security").is_none());
    assert!(logout["responses"]["204"]["content"].is_null());

    let rendered = document.to_string();
    for forbidden in [
        "/api/v1/",
        "/oauth/",
        "/internal/",
        "bearerAuth",
        "oauth2",
        "client_secret",
        "password_hash",
        "token_hash",
        "database_url",
        "sqlite",
        "postgres",
        "/opt/",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "OpenAPI document exposed forbidden content: {forbidden}"
        );
    }
}

#[tokio::test]
async fn management_swagger_ui_serves_the_operator_documentation() {
    let (_directory, management) = management_session_router().await;
    let response = management
        .router
        .oneshot(
            Request::builder()
                .uri("/docs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("Swagger UI"));
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
async fn management_login_rate_limit_does_not_block_another_username_from_the_same_address() {
    let (_directory, management) = management_session_router().await;
    let app = management.router;

    for _ in 0..5 {
        let response = app.clone().oneshot(invalid_login_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let response = app
        .oneshot(invalid_login_request_for("other-user"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bootstrap_system_creates_the_only_initial_system_account_and_enables_system_login() {
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
    bootstrap_system(&store, "initial-system", "SystemAccount@2026")
        .await
        .unwrap();
    assert!(
        bootstrap_system(&store, "second-system", "SystemAccount@2026")
            .await
            .is_err()
    );

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
                .uri("/api/system/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"initial-system","password":"SystemAccount@2026"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn tenant_account_registers_tenant_bound_oauth_applications_and_denies_user_sessions() {
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
    seed_tenant_admin_users(&store).await;
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
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"test","password":"TenantAccount@2026"}"#,
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
        .clone()
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
    let application_tenant_id: String =
        sqlx::query_scalar("SELECT tenant_id FROM applications WHERE app_id = 'alpha-client-app'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let expected_tenant_id: String =
        sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(application_tenant_id, expected_tenant_id);

    let user_login = router
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
    let user_cookie = user_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let denied = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/applications")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, user_cookie)
                .body(Body::from(
                    r#"{"app_id":"user-client-app","kind":"full_stack","launch_url":"https://user.example.test","client_id":"user-client","redirect_uris":["https://user.example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn legacy_management_admin_cannot_provision_a_device_token() {
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
    let response = router
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
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_account_provisions_tenant_bound_devices_and_denies_user_sessions() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;

    let tenant_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"test","password":"TenantAccount@2026"}"#,
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

    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"display_name":"Tenant Provisioned Device"}"#,
                ))
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
    let stored_tenant_id: String =
        sqlx::query_scalar("SELECT tenant_id FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let expected_tenant_id: String =
        sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(stored_tenant_id, expected_tenant_id);

    for (username, password) in [("admin", "NanoAdmin@1234"), ("viewer", "NanoView@1234")] {
        let login = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/login")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "username": username, "password": password }).to_string(),
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
            .unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/management/devices")
                    .header(CONTENT_TYPE, "application/json")
                    .header(COOKIE, cookie)
                    .body(Body::from(r#"{"display_name":"Denied Device"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{username}");
    }
}

#[tokio::test]
async fn tenant_account_can_rotate_an_existing_device_token() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;
    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
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
async fn tenant_account_manages_devices_through_the_typed_storage_port() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;

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
async fn tenant_account_manages_assets_through_the_typed_storage_port() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"tenant_slug":"test","password":"TenantAccount@2026"}"#,
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

    let created = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/assets")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "name": "Operations Campus",
                        "asset_profile_id": null,
                        "parent_asset_id": null,
                        "metadata": { "region": "north" },
                        "attributes": { "region": "north" },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(created.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let asset_id = created["id"].as_str().unwrap();
    assert_eq!(created["name"], "Operations Campus");
    assert!(created["asset_profile_id"].is_null());
    assert!(created["parent_asset_id"].is_null());
    assert_eq!(created["metadata"], json!({ "region": "north" }));
    assert_eq!(created["attributes"], json!({ "region": "north" }));

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/assets")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(listed[0]["id"], asset_id);
    assert_eq!(listed[0]["name"], "Operations Campus");
    assert!(listed[0]["asset_profile_id"].is_null());
    assert!(listed[0]["parent_asset_id"].is_null());
    assert_eq!(listed[0]["metadata"], json!({ "region": "north" }));
    assert_eq!(listed[0]["attributes"], json!({ "region": "north" }));

    let updated = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/assets/{asset_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "name": "Operations Campus Updated",
                        "asset_profile_id": null,
                        "parent_asset_id": null,
                        "metadata": { "region": "south" },
                        "attributes": { "region": "south" },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(updated.status(), StatusCode::OK);
    let updated: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(updated.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(updated["id"], asset_id);
    assert_eq!(updated["name"], "Operations Campus Updated");
    assert!(updated["asset_profile_id"].is_null());
    assert!(updated["parent_asset_id"].is_null());
    assert_eq!(updated["metadata"], json!({ "region": "south" }));
    assert_eq!(updated["attributes"], json!({ "region": "south" }));

    let invalid_id = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/management/assets/not-a-uuid")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "name": "Ignored",
                        "asset_profile_id": null,
                        "parent_asset_id": null,
                        "metadata": {},
                        "attributes": {},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_id.status(), StatusCode::BAD_REQUEST);

    let invalid_delete_id = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/management/assets/not-a-uuid")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid_delete_id.status(), StatusCode::BAD_REQUEST);

    let duplicate = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/assets")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "name": "Operations Campus Updated",
                        "asset_profile_id": null,
                        "parent_asset_id": null,
                        "metadata": {},
                        "attributes": {},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/management/assets/00000000-0000-0000-0000-000000000000")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let deleted = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/assets/{asset_id}"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn management_device_routes_require_a_tenant_account_and_map_typed_errors() {
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

    let viewer_cookie = management_user_cookie(&router, "viewer", "NanoView@1234").await;
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

    let admin_cookie = management_user_cookie(&router, "admin", "NanoAdmin@1234").await;
    let legacy_admin = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/devices")
                .header(COOKIE, &admin_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(legacy_admin.status(), StatusCode::FORBIDDEN);

    let tenant_cookie = tenant_account_cookie(&router).await;

    let invalid_id = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/management/devices/not.valid")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
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
                .header(COOKIE, &tenant_cookie)
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
                .header(COOKIE, &tenant_cookie)
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
                .header(COOKIE, tenant_cookie)
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
async fn tenant_account_management_mutations_map_token_errors() {
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

    let viewer_cookie = management_user_cookie(&router, "viewer", "NanoView@1234").await;
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

    let admin_cookie = management_user_cookie(&router, "admin", "NanoAdmin@1234").await;
    let legacy_admin = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/devices/missing-device/tokens")
                .header(COOKIE, &admin_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(legacy_admin.status(), StatusCode::FORBIDDEN);

    let cookie = tenant_account_cookie(&router).await;

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
    let cookie = tenant_account_cookie(&router).await;
    store.sqlite_pool().unwrap().close().await;

    for request in [
        Request::builder()
            .method("POST")
            .uri("/api/management/applications")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(
                r#"{"app_id":"unavailable-app","kind":"frontend","launch_url":"https://example.test","client_id":"unavailable-client","redirect_uris":["https://example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
            ))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(r#"{"display_name":"Unavailable"}"#))
            .unwrap(),
        Request::builder()
            .method("PUT")
            .uri("/api/management/devices/unavailable-device")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(r#"{"display_name":"Unavailable"}"#))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/management/assets")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(
                r#"{"name":"Unavailable","asset_profile_id":null,"parent_asset_id":null,"metadata":{},"attributes":{}}"#,
            ))
            .unwrap(),
        Request::builder()
            .method("PUT")
            .uri("/api/management/assets/00000000-0000-0000-0000-000000000000")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(
                r#"{"name":"Unavailable","asset_profile_id":null,"parent_asset_id":null,"metadata":{},"attributes":{}}"#,
            ))
            .unwrap(),
    ] {
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[tokio::test]
async fn management_mutation_preserves_json_rejection_semantics_after_authorization() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;

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
    let viewer_cookie = management_user_cookie(&router, "viewer", "NanoView@1234").await;
    let admin_cookie = management_user_cookie(&router, "admin", "NanoAdmin@1234").await;

    for (method, path) in [
        ("POST", "/api/management/applications"),
        ("POST", "/api/management/devices"),
        ("PUT", "/api/management/devices/not-valid"),
        ("POST", "/api/management/assets"),
        ("PUT", "/api/management/assets/not-valid"),
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

        for cookie in [&viewer_cookie, &admin_cookie] {
            let user = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(CONTENT_TYPE, "application/json")
                        .header(COOKIE, cookie)
                        .body(Body::from("{"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(user.status(), StatusCode::FORBIDDEN, "{path}");
        }
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
    invalid_login_request_for("admin")
}

fn invalid_login_request_for(username: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "username": username, "password": "wrong" }).to_string(),
        ))
        .unwrap()
}

fn test_token_vault() -> TokenVault {
    TokenVault::from_key_material("management-session-test-vault-key-material-0001")
}
