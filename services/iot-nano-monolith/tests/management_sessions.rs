use std::sync::Arc;

use axum::{
    Extension,
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{
        HeaderMap, HeaderValue, Request, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
    },
};
use iot_api::{OAuthBrowserSessionVerifier, TokenVault, hash_password};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{ManagementSessionRouter, bootstrap_system};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret, NewTenant,
    NewTenantAccount, OAuthRepository, PlatformStore, TenantIdentityRepository, TenantStatus,
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

    seed_user_workspace_device_activity(pool, &tenant_id, &other_tenant.id.to_string()).await;
}

async fn seed_user_workspace_device_activity(
    pool: &sqlx::SqlitePool,
    tenant_id: &str,
    other_tenant_id: &str,
) {
    for (event_at, scoped_tenant_id, device_id, marker) in [
        (
            "2026-09-18T11:00:00Z",
            tenant_id,
            "direct-device",
            "direct-device-telemetry-<unsafe>",
        ),
        (
            "2026-09-18T11:01:00Z",
            tenant_id,
            "unshared-device",
            "unshared-device-telemetry",
        ),
        (
            "2026-09-18T11:02:00Z",
            other_tenant_id,
            "other-tenant-device",
            "cross-tenant-device-telemetry",
        ),
    ] {
        sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
             ) VALUES (?, ?, ?, ?, ?, 1, ?, 'workspace/test')",
        )
        .bind(event_at)
        .bind(event_at)
        .bind(scoped_tenant_id)
        .bind(device_id)
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(json!({ "marker": marker }).to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    for (scoped_tenant_id, device_id, rule_name) in [
        (tenant_id, "direct-device", "Direct alert <unsafe>"),
        (tenant_id, "unshared-device", "Unshared device alert"),
        (
            other_tenant_id,
            "other-tenant-device",
            "Cross tenant device alert",
        ),
    ] {
        let rule_id = uuid::Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO alert_rules (
                id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold
             ) VALUES (?, ?, ?, ?, 'temperature_c', 'event_threshold', 'gt', 80)",
        )
        .bind(&rule_id)
        .bind(scoped_tenant_id)
        .bind(rule_name)
        .bind(device_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at,
                last_value, state_version, updated_at
             ) VALUES (?, ?, ?, ?, 'open', '2026-09-18T11:00:00Z',
                '2026-09-18T11:00:00Z', 81.5, 1, '2026-09-18T11:03:00Z')",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(scoped_tenant_id)
        .bind(&rule_id)
        .bind(device_id)
        .execute(pool)
        .await
        .unwrap();
    }
}

struct UserWorkspaceAssets {
    direct_asset_id: uuid::Uuid,
    other_tenant_asset_id: uuid::Uuid,
}

async fn seed_user_workspace_assets(store: &PlatformStore) -> UserWorkspaceAssets {
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
    let owned_asset_id = uuid::Uuid::now_v7().to_string();
    let direct_asset_id = uuid::Uuid::now_v7();
    let direct_asset_id_string = direct_asset_id.to_string();
    let group_asset_id = uuid::Uuid::now_v7().to_string();
    let inherited_root_id = uuid::Uuid::now_v7().to_string();
    let inherited_child_id = uuid::Uuid::now_v7().to_string();
    let unshared_asset_id = uuid::Uuid::now_v7().to_string();

    for (asset_id, name, parent_asset_id, owner_user_id) in [
        (
            owned_asset_id.as_str(),
            "Owned asset",
            None,
            Some(viewer_id.as_str()),
        ),
        (direct_asset_id_string.as_str(), "Direct asset", None, None),
        (group_asset_id.as_str(), "Group asset", None, None),
        (inherited_root_id.as_str(), "Shared root", None, None),
        (
            inherited_child_id.as_str(),
            "Inherited asset",
            Some(inherited_root_id.as_str()),
            None,
        ),
        (unshared_asset_id.as_str(), "Unshared asset", None, None),
    ] {
        sqlx::query(
            "INSERT INTO assets (id, tenant_id, name, parent_asset_id, owner_user_id)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(asset_id)
        .bind(&tenant_id)
        .bind(name)
        .bind(parent_asset_id)
        .bind(owner_user_id)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'Asset operators')",
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

    for (subject_user_id, subject_group_id, asset_id, permission, inherit_children) in [
        (
            Some(viewer_id.as_str()),
            None,
            direct_asset_id_string,
            "viewer",
            0_i64,
        ),
        (
            None,
            Some(group_id.as_str()),
            group_asset_id,
            "manager",
            0_i64,
        ),
        (
            None,
            Some(group_id.as_str()),
            inherited_root_id,
            "viewer",
            1_i64,
        ),
    ] {
        sqlx::query(
            "INSERT INTO resource_permissions (
                id, tenant_id, subject_user_id, subject_group_id, asset_id, device_id,
                permission, inherit_children, created_by_user_id
             ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(&tenant_id)
        .bind(subject_user_id)
        .bind(subject_group_id)
        .bind(asset_id)
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
            slug: "asset-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("AssetOtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let other_tenant_asset_id = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Cross tenant asset')")
        .bind(other_tenant_asset_id.to_string())
        .bind(other_tenant.id.to_string())
        .execute(pool)
        .await
        .unwrap();

    UserWorkspaceAssets {
        direct_asset_id,
        other_tenant_asset_id,
    }
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

    let root = router
        .clone()
        .oneshot(platform_get("/", None))
        .await
        .unwrap();
    assert_eq!(root.status(), StatusCode::SEE_OTHER);
    assert_eq!(root.headers()[LOCATION], "/login");

    for path in ["/system", "/tenant", "/app"] {
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
async fn platform_login_renders_the_three_server_side_forms() {
    let (_directory, management) = management_session_router().await;
    let response = management
        .router
        .oneshot(platform_get("/login", None))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    for action in ["/login/system", "/login/tenant", "/login/user"] {
        assert!(body.contains(&format!("action=\"{action}\"")), "{action}");
    }
    for field in [
        "name=\"username\"",
        "name=\"tenant_slug\"",
        "name=\"password\"",
    ] {
        assert!(body.contains(field), "{field}");
    }
    assert!(!body.contains("<script"));
    assert!(!body.contains("<select"));
}

#[tokio::test]
async fn platform_login_forms_issue_sessions_redirect_and_preserve_cross_kind_guards() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;

    let mut cookies = Vec::new();
    for (path, body, destination) in [
        (
            "/login/system",
            "username=system&password=SystemAccount@2026",
            "/system",
        ),
        (
            "/login/tenant",
            "tenant_slug=test&password=TenantAccount@2026",
            "/tenant",
        ),
        (
            "/login/user",
            "tenant_slug=test&username=viewer&password=NanoView@1234",
            "/app",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(system_lifecycle_form(path, None, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER, "{path}");
        assert_eq!(response.headers()[LOCATION], destination, "{path}");
        let cookie = response.headers()[SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let destination_response = router
            .clone()
            .oneshot(platform_get(destination, Some(&cookie)))
            .await
            .unwrap();
        assert_eq!(destination_response.status(), StatusCode::OK, "{path}");
        cookies.push(cookie);
    }

    let invalid = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/login/user",
            None,
            "tenant_slug=test&username=viewer&password=not-the-password",
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::SEE_OTHER);
    assert_eq!(invalid.headers()[LOCATION], "/login?error=invalid");
    assert!(invalid.headers().get(SET_COOKIE).is_none());

    let invalid_page = router
        .clone()
        .oneshot(platform_get("/login?error=invalid", None))
        .await
        .unwrap();
    let invalid_body = String::from_utf8(
        to_bytes(invalid_page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(invalid_body.contains("Unable to sign in. Check your credentials and try again."));
    assert!(!invalid_body.contains("not-the-password"));

    let [system_cookie, tenant_cookie, user_cookie]: [String; 3] = cookies.try_into().unwrap();
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
    assert!(authorized_body.contains("Recent telemetry"));
    assert!(authorized_body.contains("direct-device-telemetry-&#60;unsafe&#62;"));
    assert!(authorized_body.contains("Recent alerts"));
    assert!(authorized_body.contains("Direct alert &#60;unsafe&#62;"));
    assert!(authorized_body.contains("Warning"));
    assert!(!authorized_body.contains("unshared-device-telemetry"));
    assert!(!authorized_body.contains("cross-tenant-device-telemetry"));
    assert!(!authorized_body.contains("Unshared device alert"));
    assert!(!authorized_body.contains("Cross tenant device alert"));
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
        assert!(!body.contains("direct-device-telemetry"));
        assert!(!body.contains("Direct alert"));
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
async fn user_workspace_asset_list_shows_only_authorized_assets_and_resolved_links() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let assets = seed_user_workspace_assets(store.as_ref()).await;

    let router = management.router;
    let user_cookie = user_account_cookie(&router).await;
    let response = router
        .clone()
        .oneshot(platform_get("/app/assets", Some(&user_cookie)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    for (asset_name, permission, access_source) in [
        ("Owned asset", "Owner", "Owner"),
        ("Direct asset", "Viewer", "Direct user permission"),
        ("Group asset", "Manager", "Group permission"),
        ("Inherited asset", "Viewer", "Inherited group permission"),
    ] {
        assert!(body.contains(asset_name), "missing {asset_name}");
        assert!(body.contains(permission), "missing {permission}");
        assert!(body.contains(access_source), "missing {access_source}");
    }
    assert!(!body.contains("Unshared asset"));
    assert!(!body.contains("Cross tenant asset"));
    assert!(!body.contains("href=\"/system"));
    assert!(!body.contains("href=\"/tenant"));
    assert!(!body.contains("<form"));

    let direct_asset_route = format!("/app/assets/{}", assets.direct_asset_id);
    assert!(body.contains(&format!("href=\"{direct_asset_route}\"")));
    let detail = router
        .oneshot(platform_get(&direct_asset_route, Some(&user_cookie)))
        .await
        .unwrap();
    assert_eq!(detail.status(), StatusCode::OK);
}

#[tokio::test]
async fn user_workspace_asset_detail_masks_unavailable_assets_and_requires_user_session() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let assets = seed_user_workspace_assets(store.as_ref()).await;

    let router = management.router;
    let user_cookie = user_account_cookie(&router).await;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let system_cookie = system_account_cookie(&router).await;
    let authorized_route = format!("/app/assets/{}", assets.direct_asset_id);
    let authorized = router
        .clone()
        .oneshot(platform_get(&authorized_route, Some(&user_cookie)))
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
    let authorized_body = to_bytes(authorized.into_body(), usize::MAX).await.unwrap();
    let authorized_body = String::from_utf8(authorized_body.to_vec()).unwrap();
    assert!(authorized_body.contains("Direct asset"));
    assert!(authorized_body.contains("Viewer"));
    assert!(authorized_body.contains("Direct user permission"));
    assert!(!authorized_body.contains("<form"));

    for asset_id in [
        assets.other_tenant_asset_id.to_string(),
        "00000000-0000-0000-0000-000000000000".to_owned(),
    ] {
        let response = router
            .clone()
            .oneshot(platform_get(
                &format!("/app/assets/{asset_id}"),
                Some(&user_cookie),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{asset_id}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Asset unavailable"));
        assert!(!body.contains("Cross tenant asset"));
    }

    for cookie in [
        None,
        Some(tenant_cookie.as_str()),
        Some(system_cookie.as_str()),
    ] {
        let response = router
            .clone()
            .oneshot(platform_get(&authorized_route, cookie))
            .await
            .unwrap();
        assert!(
            matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ),
            "unexpected status {}",
            response.status()
        );
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

    let disable = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants/tenant-account/disable",
            Some(&system_cookie),
            "slug=ui-tenant",
        ))
        .await
        .unwrap();
    assert_eq!(disable.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        disable.headers()[LOCATION],
        "/system?notice=tenant-account-disabled"
    );
    assert_eq!(
        tenant_account_login_status(&router, "ui-tenant", "ReplacementTenant@2026").await,
        StatusCode::UNAUTHORIZED
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
            "/system/tenants/tenant-account/disable",
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
        "/api/management/alerts",
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
        ("/api/management/alerts", BTreeSet::from(["get"])),
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
    assert_eq!(
        paths["/api/management/alerts"]["get"]["responses"]["403"]["description"],
        "Tenant Account required"
    );

    for (path, method, status, schema) in [
        (
            "/api/management/alerts",
            "get",
            "200",
            "#/components/schemas/ManagementAlertList",
        ),
        (
            "/api/management/devices",
            "post",
            "201",
            "#/components/schemas/DeviceToken",
        ),
        (
            "/api/management/devices/{device_id}/tokens",
            "post",
            "201",
            "#/components/schemas/DeviceToken",
        ),
    ] {
        assert_eq!(
            paths[path][method]["responses"][status]["content"]["application/json"]["schema"]["$ref"],
            schema
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
        "ManagementAlert",
        "ManagementAlertList",
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

    let management_alert = &schemas["ManagementAlert"];
    assert_eq!(management_alert["type"], "object");
    assert_eq!(management_alert["additionalProperties"], false);
    assert_eq!(
        management_alert["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "device_id",
            "id",
            "last_value",
            "rule_name",
            "severity",
            "status",
            "updated_at",
        ])
    );
    assert_eq!(
        management_alert["properties"]["id"],
        json!({"type": "string", "format": "uuid"})
    );
    assert_eq!(
        management_alert["properties"]["updated_at"],
        json!({"type": "string", "format": "date-time"})
    );
    assert!(management_alert["properties"].get("tenant_id").is_none());

    for (schema_name, field_name) in [
        ("SessionResponse", "user_id"),
        ("ManagementAlert", "id"),
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

#[tokio::test]
async fn tenant_asset_and_device_pages_only_render_the_authenticated_tenant_resources() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();

    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let root_asset_id = uuid::Uuid::now_v7().to_string();
    let child_asset_id = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Tenant site')")
        .bind(&root_asset_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, parent_asset_id)
         VALUES (?, ?, 'Tenant panel', ?)",
    )
    .bind(&child_asset_id)
    .bind(&tenant_id)
    .bind(&root_asset_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('tenant-device', ?, 'Tenant meter', ?)",
    )
    .bind(&tenant_id)
    .bind(&child_asset_id)
    .execute(pool)
    .await
    .unwrap();

    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "tenant-setup-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Other tenant asset')")
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(other_tenant.id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('other-tenant-device', ?, 'Other tenant device')",
    )
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let assets = router
        .clone()
        .oneshot(platform_get("/tenant/assets", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(assets.status(), StatusCode::OK);
    let assets = String::from_utf8(
        to_bytes(assets.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(assets.contains("Tenant site"));
    assert!(assets.contains("Tenant panel"));
    assert!(assets.contains(&root_asset_id));
    assert!(assets.contains(&format!("Tenant site ({root_asset_id})")));
    assert!(!assets.contains("Other tenant asset"));

    let devices = router
        .clone()
        .oneshot(platform_get("/tenant/devices", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(devices.status(), StatusCode::OK);
    let devices = String::from_utf8(
        to_bytes(devices.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(devices.contains("tenant-device"));
    assert!(devices.contains("Tenant meter"));
    assert!(devices.contains("Offline"));
    assert!(devices.contains(&format!("Tenant panel ({child_asset_id})")));
    assert!(!devices.contains("Other tenant device"));
}

#[tokio::test]
async fn tenant_asset_and_device_forms_create_current_tenant_resources_without_list_token_leaks() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();

    let create_asset = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/assets",
            Some(&tenant_cookie),
            "name=Created+asset",
        ))
        .await
        .unwrap();
    assert_eq!(create_asset.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        create_asset.headers()[LOCATION],
        "/tenant/assets?notice=asset-created"
    );
    let asset_tenant_id = sqlx::query_scalar::<_, String>(
        "SELECT tenant_id FROM assets WHERE name = 'Created asset'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let expected_tenant_id =
        sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(asset_tenant_id, expected_tenant_id);

    let provision_device = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/devices",
            Some(&tenant_cookie),
            "display_name=Provisioned+meter",
        ))
        .await
        .unwrap();
    assert_eq!(provision_device.status(), StatusCode::CREATED);
    assert_eq!(provision_device.headers()[CACHE_CONTROL], "no-store");
    let provisioned_body = String::from_utf8(
        to_bytes(provision_device.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let credential = provisioned_body
        .split("<code id=\"device-credential\" class=\"credential\">")
        .nth(1)
        .and_then(|value| value.split("</code>").next())
        .expect("provision result must contain the one-time credential")
        .to_owned();
    assert!(!credential.is_empty());
    assert!(provisioned_body.contains("Provisioned meter"));

    let device_tenant_id = sqlx::query_scalar::<_, String>(
        "SELECT tenant_id FROM devices WHERE display_name = 'Provisioned meter'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(device_tenant_id, expected_tenant_id);

    let device_list = router
        .clone()
        .oneshot(platform_get("/tenant/devices", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(device_list.status(), StatusCode::OK);
    let device_list = String::from_utf8(
        to_bytes(device_list.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(device_list.contains("Provisioned meter"));
    assert!(!device_list.contains(&credential));
}

#[tokio::test]
async fn tenant_profile_forms_and_device_token_pages_are_scoped_and_do_not_list_secrets() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();

    let created_device_profile = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/profiles/device",
            Some(&tenant_cookie),
            "name=Tenant+device+profile&telemetry_schema=%7B%7D&metric_mapping=%7B%7D&reporting_settings=%7B%7D",
        ))
        .await
        .unwrap();
    assert_eq!(created_device_profile.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        created_device_profile.headers()[LOCATION],
        "/tenant/profiles/device?notice=device-profile-created"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT tenant_id FROM device_profiles WHERE name = 'Tenant device profile'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        tenant_id
    );

    let created_asset_profile = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/profiles/asset",
            Some(&tenant_cookie),
            "name=Tenant+asset+profile&fields=%7B%7D&dashboard_defaults=%7B%7D",
        ))
        .await
        .unwrap();
    assert_eq!(created_asset_profile.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        created_asset_profile.headers()[LOCATION],
        "/tenant/profiles/asset?notice=asset-profile-created"
    );

    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "tenant-profile-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO device_profiles (id, tenant_id, name) VALUES (?, ?, 'Other device profile')",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name) VALUES (?, ?, 'Other asset profile')",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('other-token-device', ?, 'Other token device')",
    )
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    for (path, expected, excluded) in [
        (
            "/tenant/profiles/device",
            "Tenant device profile",
            "Other device profile",
        ),
        (
            "/tenant/profiles/asset",
            "Tenant asset profile",
            "Other asset profile",
        ),
    ] {
        let page = router
            .clone()
            .oneshot(platform_get(path, Some(&tenant_cookie)))
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK, "{path}");
        let body = String::from_utf8(
            to_bytes(page.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains(expected), "{path}");
        assert!(!body.contains(excluded), "{path}");
    }

    let provisioned = management_provision_device(&router, &tenant_cookie, "Token device").await;
    let device_id = provisioned["device_id"].as_str().unwrap();
    let issued = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/tenant/devices/{device_id}/tokens/issue"),
            Some(&tenant_cookie),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(issued.status(), StatusCode::CREATED);
    assert_eq!(issued.headers()[CACHE_CONTROL], "no-store");
    let issued_body = String::from_utf8(
        to_bytes(issued.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let credential = issued_body
        .split("<code id=\"device-token-credential\" class=\"credential\">")
        .nth(1)
        .and_then(|value| value.split("</code>").next())
        .expect("issue response must contain the one-time device token")
        .to_owned();
    assert!(!credential.is_empty());

    let token_page = router
        .clone()
        .oneshot(platform_get(
            &format!("/tenant/devices/{device_id}/tokens"),
            Some(&tenant_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(token_page.status(), StatusCode::OK);
    let token_page = String::from_utf8(
        to_bytes(token_page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(token_page.contains("Device tokens"));
    assert!(!token_page.contains(&credential));

    let active_token_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM device_tokens WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(device_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let revoked = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/tenant/devices/{device_id}/tokens/revoke"),
            Some(&tenant_cookie),
            &format!("token_id={active_token_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        revoked.headers()[LOCATION],
        format!("/tenant/devices/{device_id}/tokens?notice=token-revoked")
    );

    let other_tenant_page = router
        .oneshot(platform_get(
            "/tenant/devices/other-token-device/tokens",
            Some(&tenant_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(other_tenant_page.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_profile_and_token_routes_deny_non_tenant_sessions_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (path, form_path) in [
        ("/tenant/profiles/device", "/tenant/profiles/device"),
        ("/tenant/profiles/asset", "/tenant/profiles/asset"),
        (
            "/tenant/devices/not-a-tenant-device/tokens",
            "/tenant/devices/not-a-tenant-device/tokens/issue",
        ),
        (
            "/tenant/devices/not-a-tenant-device/tokens",
            "/tenant/devices/not-a-tenant-device/tokens/revoke",
        ),
    ] {
        for (cookie, expected_status) in [
            (None, StatusCode::UNAUTHORIZED),
            (Some(system_cookie.as_str()), StatusCode::FORBIDDEN),
            (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
        ] {
            let get = router
                .clone()
                .oneshot(platform_get(path, cookie))
                .await
                .unwrap();
            assert_eq!(get.status(), expected_status, "GET {path}");

            let post = router
                .clone()
                .oneshot(system_lifecycle_form(
                    form_path,
                    cookie,
                    "tenant_id=forbidden&not=a+valid+form",
                ))
                .await
                .unwrap();
            assert_eq!(post.status(), expected_status, "POST {form_path}");
        }
    }
}

#[tokio::test]
async fn tenant_asset_and_device_routes_require_a_tenant_session_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for path in ["/tenant/assets", "/tenant/devices"] {
        let unauthenticated = router
            .clone()
            .oneshot(platform_get(path, None))
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED, "{path}");

        for cookie in [&system_cookie, &user_cookie] {
            let forbidden = router
                .clone()
                .oneshot(platform_get(path, Some(cookie)))
                .await
                .unwrap();
            assert_eq!(forbidden.status(), StatusCode::FORBIDDEN, "{path}");
        }

        let unauthenticated_form = router
            .clone()
            .oneshot(system_lifecycle_form(path, None, "not=a+valid+form"))
            .await
            .unwrap();
        assert_eq!(
            unauthenticated_form.status(),
            StatusCode::UNAUTHORIZED,
            "{path} form"
        );

        for cookie in [&system_cookie, &user_cookie] {
            let forbidden_form = router
                .clone()
                .oneshot(system_lifecycle_form(
                    path,
                    Some(cookie),
                    "not=a+valid+form",
                ))
                .await
                .unwrap();
            assert_eq!(
                forbidden_form.status(),
                StatusCode::FORBIDDEN,
                "{path} form"
            );
        }
    }
}

#[tokio::test]
async fn tenant_group_and_permission_forms_are_scoped_to_the_authenticated_tenant_account() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();

    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let tenant_account_id =
        sqlx::query_scalar::<_, String>("SELECT id FROM tenant_accounts WHERE tenant_id = ?")
            .bind(&tenant_id)
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
    let asset_id = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Tenant asset')")
        .bind(&asset_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('tenant-device', ?, 'Tenant device')",
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .unwrap();

    let tenant_home = router
        .clone()
        .oneshot(platform_get("/tenant", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(tenant_home.status(), StatusCode::OK);
    let tenant_home_body = String::from_utf8(
        to_bytes(tenant_home.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    for path in [
        "/tenant/groups",
        "/tenant/permissions",
        "/tenant/assets",
        "/tenant/devices",
    ] {
        assert!(
            tenant_home_body.contains(&format!("href=\"{path}\"")),
            "{path}"
        );
        let response = router
            .clone()
            .oneshot(platform_get(path, Some(&tenant_cookie)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }

    let create_group = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/groups",
            Some(&tenant_cookie),
            &format!("name=operators&owner_user_id={viewer_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(create_group.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        create_group.headers()[LOCATION],
        "/tenant/groups?notice=group-created"
    );
    let group_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM user_groups WHERE tenant_id = ? AND name = 'operators'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();

    let add_member = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/groups/members",
            Some(&tenant_cookie),
            &format!("group_id={group_id}&user_id={viewer_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(add_member.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM user_group_members
             WHERE tenant_id = ? AND group_id = ? AND user_id = ?",
        )
        .bind(&tenant_id)
        .bind(&group_id)
        .bind(&viewer_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    let groups = router
        .clone()
        .oneshot(platform_get("/tenant/groups", Some(&tenant_cookie)))
        .await
        .unwrap();
    let groups_body = String::from_utf8(
        to_bytes(groups.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(groups_body.contains("operators"));
    assert!(groups_body.contains("viewer"));

    let remove_member = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/groups/members/remove",
            Some(&tenant_cookie),
            &format!("group_id={group_id}&user_id={viewer_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(remove_member.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM user_group_members
             WHERE tenant_id = ? AND group_id = ? AND user_id = ?",
        )
        .bind(&tenant_id)
        .bind(&group_id)
        .bind(&viewer_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );

    for body in [
        format!(
            "subject=user%3A{viewer_id}&scope=asset&resource_id={asset_id}&permission=manager&inherit_children=on"
        ),
        format!(
            "subject=group%3A{group_id}&scope=device&resource_id=tenant-device&permission=viewer"
        ),
    ] {
        let response = router
            .clone()
            .oneshot(system_lifecycle_form(
                "/tenant/permissions",
                Some(&tenant_cookie),
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers()[LOCATION],
            "/tenant/permissions?notice=permission-created"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions
             WHERE tenant_id = ? AND created_by_tenant_account_id = ?
               AND created_by_user_id IS NULL AND revoked_at IS NULL",
        )
        .bind(&tenant_id)
        .bind(&tenant_account_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        2
    );
    let permissions = router
        .clone()
        .oneshot(platform_get("/tenant/permissions", Some(&tenant_cookie)))
        .await
        .unwrap();
    let permissions_body = String::from_utf8(
        to_bytes(permissions.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(permissions_body.contains("Tenant asset"));
    assert!(permissions_body.contains("Tenant device"));
    assert!(permissions_body.contains("Manager"));
    assert!(permissions_body.contains("Viewer"));

    let permission_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM resource_permissions WHERE tenant_id = ? AND asset_id = ?",
    )
    .bind(&tenant_id)
    .bind(&asset_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let revoke = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/permissions/revoke",
            Some(&tenant_cookie),
            &format!("permission_id={permission_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE id = ? AND revoked_at IS NOT NULL",
        )
        .bind(&permission_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );

    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "other-tenant".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let other_user_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'other-user', 'unused', 'viewer', 'user')",
    )
    .bind(&other_user_id)
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let other_group_id = uuid::Uuid::now_v7().to_string();
    let other_asset_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'other-group')",
    )
    .bind(&other_group_id)
    .bind(other_tenant.id.to_string())
    .bind(&other_user_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Other asset')")
        .bind(&other_asset_id)
        .bind(other_tenant.id.to_string())
        .execute(pool)
        .await
        .unwrap();

    for request in [
        system_lifecycle_form(
            "/tenant/groups/members",
            Some(&tenant_cookie),
            &format!("group_id={other_group_id}&user_id={viewer_id}"),
        ),
        system_lifecycle_form(
            "/tenant/permissions",
            Some(&tenant_cookie),
            &format!(
                "subject=user%3A{viewer_id}&scope=asset&resource_id={other_asset_id}&permission=viewer"
            ),
        ),
    ] {
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user_group_members WHERE group_id = ?",)
            .bind(&other_group_id)
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE asset_id = ?",
        )
        .bind(&other_asset_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );

    for path in ["/tenant/groups", "/tenant/permissions"] {
        let unauthenticated = router
            .clone()
            .oneshot(platform_get(path, None))
            .await
            .unwrap();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        for cookie in [&system_cookie, &user_cookie] {
            let forbidden = router
                .clone()
                .oneshot(platform_get(path, Some(cookie)))
                .await
                .unwrap();
            assert_eq!(forbidden.status(), StatusCode::FORBIDDEN, "{path}");
        }
    }
    for cookie in [&system_cookie, &user_cookie] {
        let forbidden = router
            .clone()
            .oneshot(system_lifecycle_form(
                "/tenant/groups",
                Some(cookie),
                "not=a+valid+form",
            ))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn tenant_user_page_lists_and_creates_normal_users() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();

    let tenant_home = router
        .clone()
        .oneshot(platform_get("/tenant", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(tenant_home.status(), StatusCode::OK);
    let tenant_home_body = String::from_utf8(
        to_bytes(tenant_home.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(tenant_home_body.contains("href=\"/tenant/users\""));

    let initial_page = router
        .clone()
        .oneshot(platform_get("/tenant/users", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(initial_page.status(), StatusCode::OK);
    let initial_body = String::from_utf8(
        to_bytes(initial_page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(initial_body.contains("admin"));
    assert!(initial_body.contains("viewer"));
    assert!(initial_body.contains("Username"));
    assert!(initial_body.contains("Status"));
    assert!(initial_body.contains("Account class"));
    assert!(!initial_body.contains("Default app"));
    assert!(!initial_body.contains("Granted apps"));
    assert!(!initial_body.contains("Role"));

    let created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/users",
            Some(&tenant_cookie),
            "username=site-user&password=SiteUser%402026",
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        created.headers()[LOCATION],
        "/tenant/users?notice=user-created"
    );

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT tenant_id FROM users WHERE username = 'site-user'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        tenant_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT role FROM users WHERE username = 'site-user'")
            .fetch_one(pool)
            .await
            .unwrap(),
        "viewer"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT account_class FROM users WHERE username = 'site-user'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        "user"
    );

    let page = router
        .clone()
        .oneshot(platform_get(
            "/tenant/users?notice=user-created",
            Some(&tenant_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("User created."));
    assert!(body.contains("site-user"));
    assert!(body.contains("Active"));
    assert!(body.contains("User"));
    assert!(!body.contains("SiteUser@2026"));
    assert!(!body.contains("value=\"SiteUser@2026\""));
}

#[tokio::test]
async fn system_created_tenant_can_create_and_sign_in_a_user_from_the_platform_forms() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;

    let tenant_created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/system/tenants",
            Some(&system_cookie),
            "slug=fresh-tenant&tenant_account_password=FreshTenant%402026",
        ))
        .await
        .unwrap();
    assert_eq!(tenant_created.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        tenant_created.headers()[LOCATION],
        "/system?notice=tenant-created"
    );

    let tenant_login = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/login/tenant",
            None,
            "tenant_slug=fresh-tenant&password=FreshTenant%402026",
        ))
        .await
        .unwrap();
    assert_eq!(tenant_login.status(), StatusCode::SEE_OTHER);
    assert_eq!(tenant_login.headers()[LOCATION], "/tenant");
    let tenant_cookie = tenant_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let user_created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/users",
            Some(&tenant_cookie),
            "username=fresh-user&password=FreshUser%402026",
        ))
        .await
        .unwrap();
    assert_eq!(user_created.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        user_created.headers()[LOCATION],
        "/tenant/users?notice=user-created"
    );

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT default_app
             FROM users
             JOIN tenants ON tenants.id = users.tenant_id
             WHERE tenants.slug = 'fresh-tenant' AND users.username = 'fresh-user'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        "/app"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM user_app_grants
             JOIN users ON users.id = user_app_grants.user_id
             JOIN tenants ON tenants.id = users.tenant_id
             WHERE tenants.slug = 'fresh-tenant' AND users.username = 'fresh-user'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        0
    );

    let user_login = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/login/user",
            None,
            "tenant_slug=fresh-tenant&username=fresh-user&password=FreshUser%402026",
        ))
        .await
        .unwrap();
    assert_eq!(user_login.status(), StatusCode::SEE_OTHER);
    assert_eq!(user_login.headers()[LOCATION], "/app");
    let user_cookie = user_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let app = router
        .oneshot(platform_get("/app", Some(&user_cookie)))
        .await
        .unwrap();
    assert_eq!(app.status(), StatusCode::OK);
}

#[tokio::test]
async fn tenant_user_page_hides_other_tenant_users_and_rejects_tenant_form_values() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();
    let primary_tenant_id =
        sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
            .fetch_one(pool)
            .await
            .unwrap();
    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "tenant-user-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'other-tenant-user', 'unused', 'viewer', 'user')",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let page = router
        .clone()
        .oneshot(platform_get("/tenant/users", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!body.contains("other-tenant-user"));

    let created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/users",
            Some(&tenant_cookie),
            &format!(
                "username=tenant-scoped-user&password=TenantScoped%402026&tenant_id={}&tenant_slug=tenant-user-other",
                other_tenant.id
            ),
        ))
    .await
    .unwrap();
    assert_eq!(created.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        created.headers()[LOCATION],
        "/tenant/users?notice=invalid-request"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM users WHERE tenant_id = ? AND username = 'tenant-scoped-user'",
        )
        .bind(&primary_tenant_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM users WHERE tenant_id = ? AND username = 'tenant-scoped-user'",
        )
        .bind(other_tenant.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn tenant_user_routes_reject_non_tenant_sessions_before_form_parsing_or_mutation() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();

    for (cookie, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(system_cookie.as_str()), StatusCode::FORBIDDEN),
        (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
    ] {
        let get = router
            .clone()
            .oneshot(platform_get("/tenant/users", cookie))
            .await
            .unwrap();
        assert_eq!(get.status(), expected_status);

        let post = router
            .clone()
            .oneshot(system_lifecycle_form("/tenant/users", cookie, "%"))
            .await
            .unwrap();
        assert_eq!(post.status(), expected_status);
    }

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM users WHERE username = 'unauthorized-tenant-user'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn tenant_topology_forms_scope_gateway_children_and_reject_invalid_assignments() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();

    let gateway = management_provision_device(&router, &tenant_cookie, "Tenant gateway").await;
    let gateway_id = gateway["device_id"].as_str().unwrap().to_owned();
    let child = management_provision_device(&router, &tenant_cookie, "Tenant child").await;
    let child_id = child["device_id"].as_str().unwrap().to_owned();
    let direct = management_provision_device(&router, &tenant_cookie, "Direct device").await;
    let direct_id = direct["device_id"].as_str().unwrap().to_owned();

    let gateway_update = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/management/devices/{gateway_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Tenant gateway",
                        "topology": { "is_gateway": true },
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gateway_update.status(), StatusCode::OK);

    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "topology-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, is_gateway)
         VALUES ('other-tenant-gateway', ?, 'Other tenant gateway', 1)",
    )
    .bind(other_tenant.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let page = router
        .clone()
        .oneshot(platform_get("/tenant/topology", Some(&tenant_cookie)))
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
    assert!(page.contains("Gateway Topology"));
    assert!(page.contains(&gateway_id));
    assert!(page.contains(&child_id));
    assert!(!page.contains("Other tenant gateway"));
    assert!(!page.contains("gateway_child"));

    let assigned = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/topology/assign",
            Some(&tenant_cookie),
            &format!("child_device_id={child_id}&gateway_device_id={gateway_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(assigned.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        assigned.headers()[LOCATION],
        "/tenant/topology?notice=gateway-assigned"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT gateway_device_id FROM devices WHERE device_id = ?",
        )
        .bind(&child_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(gateway_id.clone())
    );

    let detached = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/topology/detach",
            Some(&tenant_cookie),
            &format!("child_device_id={child_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(detached.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        detached.headers()[LOCATION],
        "/tenant/topology?notice=gateway-detached"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT gateway_device_id FROM devices WHERE device_id = ?",
        )
        .bind(&child_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );

    for gateway_device_id in [&child_id, &direct_id, "other-tenant-gateway"] {
        let rejected = router
            .clone()
            .oneshot(system_lifecycle_form(
                "/tenant/topology/assign",
                Some(&tenant_cookie),
                &format!("child_device_id={child_id}&gateway_device_id={gateway_device_id}"),
            ))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            rejected.headers()[LOCATION],
            "/tenant/topology?notice=mutation-unavailable"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT gateway_device_id FROM devices WHERE device_id = ?",
        )
        .bind(&child_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
async fn tenant_topology_routes_require_a_tenant_session_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (cookie, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(system_cookie.as_str()), StatusCode::FORBIDDEN),
        (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
    ] {
        let page = router
            .clone()
            .oneshot(platform_get("/tenant/topology", cookie))
            .await
            .unwrap();
        assert_eq!(page.status(), expected_status);

        for path in ["/tenant/topology/assign", "/tenant/topology/detach"] {
            let mutation = router
                .clone()
                .oneshot(system_lifecycle_form(path, cookie, "%"))
                .await
                .unwrap();
            assert_eq!(mutation.status(), expected_status, "{path}");
        }
    }
}

#[tokio::test]
async fn tenant_relations_page_and_forms_use_the_authenticated_tenant() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();
    let first = management_provision_device(&router, &tenant_cookie, "Relation first").await;
    let first_id = first["device_id"].as_str().unwrap().to_owned();
    let second = management_provision_device(&router, &tenant_cookie, "Relation second").await;
    let second_id = second["device_id"].as_str().unwrap().to_owned();

    let overview = router
        .clone()
        .oneshot(platform_get("/tenant", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(overview.status(), StatusCode::OK);
    let overview = String::from_utf8(
        to_bytes(overview.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(overview.contains("href=\"/tenant/relations\""));

    let page = router
        .clone()
        .oneshot(platform_get("/tenant/relations", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);

    let injected_tenant = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/relations",
            Some(&tenant_cookie),
            &format!(
                "from_device_id={first_id}&to_device_id={second_id}&relation_type=located_near&tenant_id=injected"
            ),
        ))
        .await
        .unwrap();
    assert_eq!(injected_tenant.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        injected_tenant.headers()[LOCATION],
        "/tenant/relations?notice=invalid-request"
    );

    let created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/relations",
            Some(&tenant_cookie),
            &format!(
                "from_device_id={first_id}&to_device_id={second_id}&relation_type=located_near"
            ),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        created.headers()[LOCATION],
        "/tenant/relations?notice=relation-created"
    );
    let tenant_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let relation_id: String = sqlx::query_scalar(
        "SELECT id FROM device_relations
         WHERE tenant_id = ? AND from_device_id = ? AND to_device_id = ?",
    )
    .bind(&tenant_id)
    .bind(&first_id)
    .bind(&second_id)
    .fetch_one(pool)
    .await
    .unwrap();

    let deleted = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/relations/delete",
            Some(&tenant_cookie),
            &format!("relation_id={relation_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        deleted.headers()[LOCATION],
        "/tenant/relations?notice=relation-deleted"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM device_relations WHERE tenant_id = ?",)
            .bind(tenant_id)
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn tenant_relation_routes_require_a_tenant_session_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (cookie, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(system_cookie.as_str()), StatusCode::FORBIDDEN),
        (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
    ] {
        let page = router
            .clone()
            .oneshot(platform_get("/tenant/relations", cookie))
            .await
            .unwrap();
        assert_eq!(page.status(), expected_status);

        for path in ["/tenant/relations", "/tenant/relations/delete"] {
            let mutation = router
                .clone()
                .oneshot(system_lifecycle_form(path, cookie, "%"))
                .await
                .unwrap();
            assert_eq!(mutation.status(), expected_status, "{path}");
        }
    }
}

#[tokio::test]
async fn tenant_application_page_scopes_list_upserts_and_hides_client_secrets() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id: uuid::Uuid =
        sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
            .fetch_one(pool)
            .await
            .unwrap()
            .parse()
            .unwrap();
    let application = ApplicationRepository::upsert_application(
        store.as_ref(),
        NewApplication {
            app_id: "tenant-console".parse().unwrap(),
            tenant_id,
            kind: ApplicationKind::Frontend,
            launch_url: "https://tenant.example.test/console".to_owned(),
            client_id: "tenant-console-client".parse().unwrap(),
            redirect_uris: vec![
                "https://tenant.example.test/oauth/callback"
                    .parse()
                    .unwrap(),
            ],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    OAuthRepository::register_client_secret(
        store.as_ref(),
        NewOAuthClientSecret {
            app_id: application.app_id,
            tenant_id,
            client_secret: "tenant-console-secret".to_owned(),
        },
    )
    .await
    .unwrap();

    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "applications-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    ApplicationRepository::upsert_application(
        store.as_ref(),
        NewApplication {
            app_id: "other-console".parse().unwrap(),
            tenant_id: other_tenant.id,
            kind: ApplicationKind::Frontend,
            launch_url: "https://other.example.test/console".to_owned(),
            client_id: "other-console-client".parse().unwrap(),
            redirect_uris: vec!["https://other.example.test/oauth/callback".parse().unwrap()],
            allowed_scopes: vec!["assets:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();

    let overview = router
        .clone()
        .oneshot(platform_get("/tenant", Some(&tenant_cookie)))
        .await
        .unwrap();
    assert_eq!(overview.status(), StatusCode::OK);
    let overview = String::from_utf8(
        to_bytes(overview.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(overview.contains("href=\"/tenant/applications\""));

    let page = router
        .clone()
        .oneshot(platform_get("/tenant/applications", Some(&tenant_cookie)))
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
    assert!(page.contains("tenant-console"));
    assert!(page.contains("tenant-console-client"));
    assert!(!page.contains("other-console"));
    assert!(!page.contains("tenant-console-secret"));

    let saved = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/applications",
            Some(&tenant_cookie),
            "app_id=tenant-console&kind=full_stack&launch_url=https%3A%2F%2Ftenant.example.test%2Fupdated&client_id=tenant-console-client-v2&redirect_uris=https%3A%2F%2Ftenant.example.test%2Foauth%2Fcallback%0Ahttp%3A%2F%2Flocalhost%3A3000%2Foauth%2Fcallback&allowed_scopes=assets%3Aread%0Adevices%3Aread&enabled=on",
        ))
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        saved.headers()[LOCATION],
        "/tenant/applications?notice=application-saved"
    );

    let updated =
        ApplicationRepository::find_application_by_app_id(store.as_ref(), "tenant-console")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(updated.tenant_id, tenant_id);
    assert_eq!(updated.kind, ApplicationKind::FullStack);
    assert_eq!(updated.client_id.as_str(), "tenant-console-client-v2");
    assert_eq!(
        updated
            .redirect_uris
            .iter()
            .map(|uri| uri.as_str())
            .collect::<Vec<_>>(),
        [
            "http://localhost:3000/oauth/callback",
            "https://tenant.example.test/oauth/callback",
        ]
    );
    assert_eq!(updated.allowed_scopes, ["assets:read", "devices:read"]);
}

#[tokio::test]
async fn tenant_application_form_rejects_invalid_redirects_and_scopes() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;

    for (app_id, redirect_uris, allowed_scopes) in [
        (
            "invalid-redirect",
            "javascript%3Aalert%281%29",
            "devices%3Aread",
        ),
        (
            "invalid-scopes",
            "https%3A%2F%2Ftenant.example.test%2Fcallback",
            "",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(system_lifecycle_form(
                "/tenant/applications",
                Some(&tenant_cookie),
                &format!(
                    "app_id={app_id}&kind=frontend&launch_url=https%3A%2F%2Ftenant.example.test%2Fapp&client_id={app_id}-client&redirect_uris={redirect_uris}&allowed_scopes={allowed_scopes}&enabled=on"
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers()[LOCATION],
            "/tenant/applications?notice=invalid-request"
        );
        assert!(
            ApplicationRepository::find_application_by_app_id(store.as_ref(), app_id)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn tenant_application_form_uses_the_authenticated_tenant_and_rejects_injected_tenant() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let (other_tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "applications-injected".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();

    let response = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/applications",
            Some(&tenant_cookie),
            &format!(
                "app_id=injected-app&kind=frontend&launch_url=https%3A%2F%2Ftenant.example.test%2Fapp&client_id=injected-app-client&redirect_uris=https%3A%2F%2Ftenant.example.test%2Fcallback&allowed_scopes=devices%3Aread&enabled=on&tenant_id={}",
                other_tenant.id
            ),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        response.headers()[LOCATION],
        "/tenant/applications?notice=invalid-request"
    );
    assert!(
        ApplicationRepository::find_application_by_app_id(store.as_ref(), "injected-app")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn tenant_application_routes_require_a_tenant_session_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (cookie, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(system_cookie.as_str()), StatusCode::FORBIDDEN),
        (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
    ] {
        let page = router
            .clone()
            .oneshot(platform_get("/tenant/applications", cookie))
            .await
            .unwrap();
        assert_eq!(page.status(), expected_status);

        let mutation = router
            .clone()
            .oneshot(system_lifecycle_form("/tenant/applications", cookie, "%"))
            .await
            .unwrap();
        assert_eq!(mutation.status(), expected_status);
    }
}
