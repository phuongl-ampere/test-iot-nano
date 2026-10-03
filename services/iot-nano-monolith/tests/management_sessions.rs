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
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{ManagementSessionRouter, bootstrap_system};
use iot_storage::{
    ApplicationKind, ApplicationRepository, DeviceClaimError, DeviceClaimPolicy,
    DeviceClaimRepository, NewApplication, NewOAuthClientSecret, NewTenant, NewTenantAccount,
    OAuthRepository, PlatformStore, TenantIdentityRepository, TenantStatus,
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
    ApplicationRepository::upsert_application(
        store,
        NewApplication {
            app_id: "powermonitor".parse().unwrap(),
            tenant_id: tenant.id,
            kind: ApplicationKind::Frontend,
            launch_url: "/apps/powermonitor".to_owned(),
            client_id: format!("seed-{}-powermonitor", tenant.id).parse().unwrap(),
            redirect_uris: Vec::new(),
            allowed_scopes: Vec::new(),
            enabled: true,
        },
    )
    .await
    .unwrap();
    let pool = store.sqlite_pool().unwrap();
    for (username, password, role, account_class) in [
        ("admin", "NanoAdmin@1234", "admin", "admin"),
        ("viewer", "NanoView@1234", "viewer", "user"),
    ] {
        let user_id = uuid::Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO users (
                id, tenant_id, username, password_hash, role, account_class
             ) VALUES (?, ?, ?, ?, ?, ?)",
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
    }
}

async fn tenant_account_cookie(router: &axum::Router) -> String {
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tenant/auth/login")
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

#[tokio::test]
async fn tenant_account_replaces_a_users_elevated_capabilities() {
    let (_directory, _store, management) = management_session_router_with_store().await;
    let cookie = tenant_account_cookie(&management.router).await;

    let response = management
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/management/users/viewer/capabilities")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(
                    json!({
                        "capabilities": ["create_devices", "control_devices"]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        body["capabilities"],
        json!(["control_devices", "create_devices"])
    );
}

#[tokio::test]
async fn user_workspace_creation_requires_the_tenant_enabled_capability() {
    let (_directory, management) = management_session_router().await;
    let cookie = user_account_cookie(&management.router).await;

    let response = management
        .router
        .oneshot(system_lifecycle_form(
            "/app/assets",
            Some(&cookie),
            "name=Denied+asset&parent_asset_id=",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn user_claims_a_device_through_a_secret_post_form() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let viewer_id: String =
        sqlx::query_scalar("SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'")
            .bind(&tenant_id)
            .fetch_one(pool)
            .await
            .unwrap();
    grant_user_capabilities(pool, &tenant_id, &viewer_id, &["claim_devices"]).await;
    let device_id = "session-claim-device";
    let serial_number = "PM-SESSION-CLAIM-001";
    sqlx::query("INSERT INTO devices (device_id, tenant_id, serial_number) VALUES (?, ?, ?)")
        .bind(device_id)
        .bind(&tenant_id)
        .bind(serial_number)
        .execute(pool)
        .await
        .unwrap();
    let tenant_uuid = tenant_id.parse().unwrap();
    DeviceClaimRepository::update_device_claim_policy(
        store.as_ref(),
        tenant_uuid,
        DeviceClaimPolicy {
            enabled: true,
            ..DeviceClaimPolicy::default()
        },
    )
    .await
    .unwrap();
    let issued =
        DeviceClaimRepository::issue_device_claim_code(store.as_ref(), tenant_uuid, device_id)
            .await
            .unwrap();
    let cookie = user_account_cookie(&management.router).await;

    let claimed = management
        .router
        .clone()
        .oneshot(system_lifecycle_form(
            "/app/devices/claim",
            Some(&cookie),
            &format!("serial_number={serial_number}&code={}", issued.code),
        ))
        .await
        .unwrap();
    assert_eq!(claimed.status(), StatusCode::SEE_OTHER);
    assert_eq!(claimed.headers()[LOCATION], "/app?notice=device-claimed");
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner_user_id FROM devices WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(&tenant_id)
    .bind(device_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(owner.as_deref(), Some(viewer_id.as_str()));

    let workspace = management
        .router
        .oneshot(platform_get("/app?notice=device-claimed", Some(&cookie)))
        .await
        .unwrap();
    let body = String::from_utf8(
        to_bytes(workspace.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("Device added to your workspace."));
    assert!(body.contains(device_id));
    assert!(!body.contains(&issued.code));
}

#[tokio::test]
async fn tenant_updates_the_pairing_policy_without_a_tenant_side_code() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let cookie = tenant_account_cookie(&management.router).await;
    let page = management
        .router
        .clone()
        .oneshot(platform_get("/tenant/devices/claim-policy", Some(&cookie)))
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
    assert!(page.contains("Pairing policy"));
    assert!(page.contains("Pairing codes are always six digits."));
    assert!(!page.contains("data-device-claim-code"));

    let saved = management
        .router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/devices/claim-policy",
            Some(&cookie),
            "enabled=on&ttl_seconds=1200&code_length=6&max_failed_attempts=4&request_cooldown_seconds=45",
        ))
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        saved.headers()[LOCATION],
        "/tenant/devices/claim-policy?notice=claim-policy-saved"
    );
    let tenant_id: uuid::Uuid =
        sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap()
            .parse()
            .unwrap();
    assert_eq!(
        DeviceClaimRepository::get_device_claim_policy(store.as_ref(), tenant_id)
            .await
            .unwrap(),
        DeviceClaimPolicy {
            enabled: true,
            ttl_seconds: 1200,
            code_length: 6,
            max_failed_attempts: 4,
            request_cooldown_seconds: 45,
        }
    );
}

#[tokio::test]
async fn tenant_account_can_issue_and_immediately_replace_a_manual_device_claim_code() {
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
    DeviceClaimRepository::update_device_claim_policy(
        store.as_ref(),
        tenant_id,
        DeviceClaimPolicy {
            enabled: true,
            ..DeviceClaimPolicy::default()
        },
    )
    .await
    .unwrap();
    let device =
        management_provision_device(&router, &tenant_cookie, "Manual Pairing Device").await;
    let device_id = device["device_id"].as_str().unwrap();
    let serial_number: String =
        sqlx::query_scalar("SELECT serial_number FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(pool)
            .await
            .unwrap();

    let first = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/management/devices/{device_id}/claim-code"))
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);
    assert_eq!(first.headers()[CACHE_CONTROL], "no-store");
    let first: serde_json::Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();
    let first_code = first["code"].as_str().unwrap().to_owned();
    assert_eq!(first["device_id"], device_id);
    assert_eq!(first["serial_number"], serial_number);
    assert!(first["expires_at"].is_string());
    assert!(
        first["pairing_uri"]
            .as_str()
            .is_some_and(|uri| uri.contains(&format!("serial_number={serial_number}")))
    );
    assert!(
        first["qr_svg"]
            .as_str()
            .is_some_and(|svg| svg.starts_with("<svg"))
    );

    let replacement = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/management/devices/{device_id}/claim-code"))
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::CREATED);
    let replacement: serde_json::Value =
        serde_json::from_slice(&to_bytes(replacement.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let replacement_code = replacement["code"].as_str().unwrap();
    assert_ne!(replacement_code, first_code);

    let viewer_id: uuid::Uuid = sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'",
    )
    .bind(tenant_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
    .parse()
    .unwrap();
    grant_user_capabilities(
        pool,
        &tenant_id.to_string(),
        &viewer_id.to_string(),
        &["claim_devices"],
    )
    .await;
    assert!(matches!(
        DeviceClaimRepository::claim_device_with_code(
            store.as_ref(),
            tenant_id,
            viewer_id,
            device_id,
            &first_code,
        )
        .await,
        Err(DeviceClaimError::CodeUnavailable)
    ));
    assert_eq!(
        DeviceClaimRepository::claim_device_with_code(
            store.as_ref(),
            tenant_id,
            viewer_id,
            device_id,
            replacement_code,
        )
        .await
        .unwrap()
        .device_id,
        device_id
    );
}

async fn management_user_cookie(router: &axum::Router, username: &str, password: &str) -> String {
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
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
                .uri("/api/v1/system/auth/login")
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
    user_account_cookie_for(router, "viewer", "NanoView@1234").await
}

async fn user_account_cookie_for(router: &axum::Router, username: &str, password: &str) -> String {
    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/user/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "tenant_slug": "test", "username": username, "password": password })
                        .to_string(),
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

async fn grant_user_capabilities(
    pool: &sqlx::SqlitePool,
    tenant_id: &str,
    user_id: &str,
    capabilities: &[&str],
) {
    for capability in capabilities {
        sqlx::query(
            "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES (?, ?, ?)",
        )
        .bind(user_id)
        .bind(tenant_id)
        .bind(capability)
        .execute(pool)
        .await
        .unwrap();
    }
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
                .uri("/api/v1/tenant/auth/login")
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
async fn platform_login_renders_one_server_side_form() {
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
    assert!(body.contains("action=\"/login\""));
    for field in ["name=\"username\"", "name=\"password\""] {
        assert!(body.contains(field), "{field}");
    }
    assert!(!body.contains("name=\"tenant_slug\""));
    assert!(!body.contains("<script"));
    assert!(!body.contains("<select"));
}

#[tokio::test]
async fn platform_login_issues_the_matching_session_and_redirect() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;

    let mut cookies = Vec::new();
    for (path, body, destination) in [
        (
            "/login",
            "username=system&password=SystemAccount@2026",
            "/system",
        ),
        (
            "/login",
            "username=test&password=TenantAccount@2026",
            "/tenant",
        ),
        ("/login", "username=viewer&password=NanoView@1234", "/app"),
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
            "/login",
            None,
            "username=viewer&password=not-the-password",
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
async fn system_tenants_navigation_targets_the_tenant_lifecycle_section_for_a_system_session() {
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
    assert!(body.contains("href=\"/system#tenants\">Tenants</a>"));
}

fn assert_read_only_user_workspace_page_allows_only_logout_form(body: &str) {
    assert_eq!(body.matches("<form").count(), 1);
    assert!(body.contains("<form class=\"logout-form\" action=\"/logout\" method=\"post\">"));
    assert!(!body.contains("action=\"/system"));
    assert!(!body.contains("action=\"/tenant"));
    assert!(!body.contains("action=\"/commands"));
    assert!(!body.contains("href=\"/system"));
    assert!(!body.contains("href=\"/tenant"));
    assert!(!body.contains("href=\"/commands"));
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
        ("Direct device", "View", "Shared by owner"),
        ("Group device", "Control", "Group permission"),
        ("Inherited device", "View", "Inherited user permission"),
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
    assert!(!body.contains("Create device"));
    assert!(!body.contains("action=\"/app/devices\" method=\"post\""));
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
    assert!(authorized_body.contains("View"));
    assert!(authorized_body.contains("Shared by owner"));
    assert!(authorized_body.contains("Recent telemetry"));
    assert!(authorized_body.contains("direct-device-telemetry-&#60;unsafe&#62;"));
    assert!(authorized_body.contains("Recent alerts"));
    assert!(authorized_body.contains("Direct alert &#60;unsafe&#62;"));
    assert!(authorized_body.contains("Warning"));
    assert!(!authorized_body.contains("unshared-device-telemetry"));
    assert!(!authorized_body.contains("cross-tenant-device-telemetry"));
    assert!(!authorized_body.contains("Unshared device alert"));
    assert!(!authorized_body.contains("Cross tenant device alert"));
    assert!(!authorized_body.contains("/commands"));
    assert_read_only_user_workspace_page_allows_only_logout_form(&authorized_body);

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
        ("Direct asset", "View", "Shared by owner"),
        ("Group asset", "Control", "Group permission"),
        ("Inherited asset", "View", "Inherited group permission"),
    ] {
        assert!(body.contains(asset_name), "missing {asset_name}");
        assert!(body.contains(permission), "missing {permission}");
        assert!(body.contains(access_source), "missing {access_source}");
    }
    assert!(!body.contains("Unshared asset"));
    assert!(!body.contains("Cross tenant asset"));
    assert!(!body.contains("href=\"/system"));
    assert!(!body.contains("href=\"/tenant"));
    assert!(!body.contains("Create asset"));
    assert!(!body.contains("action=\"/app/assets\" method=\"post\""));

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
    assert!(authorized_body.contains("View"));
    assert!(authorized_body.contains("Shared by owner"));
    assert_read_only_user_workspace_page_allows_only_logout_form(&authorized_body);

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
async fn resource_owner_invitation_requires_recipient_acceptance_before_resource_access() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let owner_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let recipient_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'recipient', ?, 'viewer', 'user')",
    )
    .bind(&recipient_id)
    .bind(&tenant_id)
    .bind(hash_password("Recipient@1234").unwrap())
    .execute(pool)
    .await
    .unwrap();
    let observer_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'observer', ?, 'viewer', 'user')",
    )
    .bind(&observer_id)
    .bind(&tenant_id)
    .bind(hash_password("Observer@1234").unwrap())
    .execute(pool)
    .await
    .unwrap();
    let asset_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id)
         VALUES (?, ?, 'Owner farm', ?)",
    )
    .bind(asset_id.to_string())
    .bind(&tenant_id)
    .bind(&owner_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id, asset_id)
         VALUES ('owner-device', ?, 'Owner device', ?, ?)",
    )
    .bind(&tenant_id)
    .bind(&owner_id)
    .bind(asset_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, owner_user_id, asset_id)
         VALUES ('farm-child-device', ?, 'Farm child device', ?, ?)",
    )
    .bind(&tenant_id)
    .bind(&owner_id)
    .bind(asset_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let router = management.router;
    let owner_cookie = user_account_cookie(&router).await;
    let unknown_user = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/app/assets/{asset_id}/permissions"),
            Some(&owner_cookie),
            "username=unknown-user&permission=view",
        ))
        .await
        .unwrap();
    assert_eq!(unknown_user.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        unknown_user.headers()[LOCATION],
        format!("/app/assets/{asset_id}?notice=mutation-unavailable")
    );
    let asset_share = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/app/assets/{asset_id}/permissions"),
            Some(&owner_cookie),
            "username=recipient&permission=control",
        ))
        .await
        .unwrap();
    assert_eq!(asset_share.status(), StatusCode::SEE_OTHER);

    let device_share = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/app/devices/owner-device/permissions",
            Some(&owner_cookie),
            "username=recipient&permission=control",
        ))
        .await
        .unwrap();
    assert_eq!(device_share.status(), StatusCode::SEE_OTHER);

    let invitations = sqlx::query_as::<_, (String, Option<String>, Option<String>, String)>(
        "SELECT id, asset_id, device_id, state
         FROM resource_invitations
         WHERE tenant_id = ? AND recipient_user_id = ?
         ORDER BY device_id",
    )
    .bind(&tenant_id)
    .bind(&recipient_id)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        invitations,
        vec![
            (
                invitations[0].0.clone(),
                Some(asset_id.to_string()),
                None,
                "pending".to_owned(),
            ),
            (
                invitations[1].0.clone(),
                None,
                Some("owner-device".to_owned()),
                "pending".to_owned(),
            ),
        ]
    );
    let grants_before_accept = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM resource_permissions
         WHERE tenant_id = ? AND subject_user_id = ? AND revoked_at IS NULL",
    )
    .bind(&tenant_id)
    .bind(&recipient_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(grants_before_accept, 0);

    let asset_detail = router
        .clone()
        .oneshot(platform_get(
            &format!("/app/assets/{asset_id}"),
            Some(&owner_cookie),
        ))
        .await
        .unwrap();
    let asset_body = String::from_utf8(
        to_bytes(asset_detail.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(asset_body.contains("Invite user"));
    assert!(!asset_body.contains("inherit_children"));

    let recipient_cookie = user_account_cookie_for(&router, "recipient", "Recipient@1234").await;
    let recipient_workspace = router
        .clone()
        .oneshot(platform_get("/app", Some(&recipient_cookie)))
        .await
        .unwrap();
    let recipient_body = String::from_utf8(
        to_bytes(recipient_workspace.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        !recipient_body.contains("Farm child device"),
        "recipient workspace: {recipient_body}"
    );
    assert!(!recipient_body.contains("Owner device"));
    assert!(recipient_body.contains("Invitations (2)"));

    let invitation_page = router
        .clone()
        .oneshot(platform_get("/app/invitations", Some(&recipient_cookie)))
        .await
        .unwrap();
    assert_eq!(invitation_page.status(), StatusCode::OK);
    let invitation_body = String::from_utf8(
        to_bytes(invitation_page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(invitation_body.contains("Owner farm"));
    assert!(invitation_body.contains("Owner device"));

    let asset_accept = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/app/invitations/{}/accept", invitations[0].0),
            Some(&recipient_cookie),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(asset_accept.status(), StatusCode::SEE_OTHER);
    let device_accept = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/app/invitations/{}/accept", invitations[1].0),
            Some(&recipient_cookie),
            "",
        ))
        .await
        .unwrap();
    assert_eq!(device_accept.status(), StatusCode::SEE_OTHER);

    let grants = sqlx::query_as::<_, (Option<String>, Option<String>, String, i64)>(
        "SELECT asset_id, device_id, permission, inherit_children
         FROM resource_permissions
         WHERE tenant_id = ? AND subject_user_id = ? AND revoked_at IS NULL
         ORDER BY device_id",
    )
    .bind(&tenant_id)
    .bind(&recipient_id)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        grants,
        vec![
            (Some(asset_id.to_string()), None, "manager".to_owned(), 0),
            (
                None,
                Some("owner-device".to_owned()),
                "manager".to_owned(),
                0
            ),
        ]
    );
    let recipient_workspace = router
        .clone()
        .oneshot(platform_get("/app", Some(&recipient_cookie)))
        .await
        .unwrap();
    let recipient_body = String::from_utf8(
        to_bytes(recipient_workspace.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(recipient_body.contains("Owner device"));

    grant_user_capabilities(pool, &tenant_id, &recipient_id, &["share_owned_resources"]).await;
    let recipient_cannot_share = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/app/devices/owner-device/permissions",
            Some(&recipient_cookie),
            "username=observer&permission=view",
        ))
        .await
        .unwrap();
    assert_eq!(recipient_cannot_share.status(), StatusCode::FORBIDDEN);

    let asset_permission_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM resource_permissions
         WHERE asset_id = ? AND subject_user_id = ? AND revoked_at IS NULL",
    )
    .bind(asset_id.to_string())
    .bind(&recipient_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let revoke = router
        .clone()
        .oneshot(system_lifecycle_form(
            &format!("/app/assets/{asset_id}/permissions/revoke"),
            Some(&owner_cookie),
            &format!("permission_id={asset_permission_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::SEE_OTHER);
    let recipient_after_revoke = router
        .clone()
        .oneshot(platform_get("/app", Some(&recipient_cookie)))
        .await
        .unwrap();
    let recipient_after_revoke_body = String::from_utf8(
        to_bytes(recipient_after_revoke.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(!recipient_after_revoke_body.contains("Farm child device"));
    assert!(recipient_after_revoke_body.contains("Owner device"));

    let observer_cookie = user_account_cookie_for(&router, "observer", "Observer@1234").await;
    let rejected = router
        .oneshot(system_lifecycle_form(
            "/app/devices/owner-device/permissions",
            Some(&observer_cookie),
            "username=recipient&permission=view",
        ))
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn user_owner_can_create_and_update_an_owned_asset() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let owner_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    grant_user_capabilities(
        pool,
        &tenant_id,
        &owner_id,
        &["create_assets", "edit_resources"],
    )
    .await;
    let router = management.router;
    let owner_cookie = user_account_cookie(&router).await;

    let created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/app/assets",
            Some(&owner_cookie),
            "name=Owner+farm&parent_asset_id=",
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::SEE_OTHER);

    let asset_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM assets WHERE tenant_id = ? AND name = 'Owner farm'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let owner = sqlx::query_scalar::<_, String>("SELECT owner_user_id FROM assets WHERE id = ?")
        .bind(&asset_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(owner, owner_id);

    let updated = router
        .oneshot(system_lifecycle_form(
            &format!("/app/assets/{asset_id}"),
            Some(&owner_cookie),
            "name=Renamed+farm&parent_asset_id=",
        ))
        .await
        .unwrap();
    assert_eq!(updated.status(), StatusCode::SEE_OTHER);
    let name = sqlx::query_scalar::<_, String>("SELECT name FROM assets WHERE id = ?")
        .bind(&asset_id)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(name, "Renamed farm");
}

#[tokio::test]
async fn user_owner_can_create_and_update_an_owned_device() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let owner_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    grant_user_capabilities(
        pool,
        &tenant_id,
        &owner_id,
        &["create_devices", "edit_resources"],
    )
    .await;
    let asset_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id)
         VALUES (?, ?, 'Owner asset', ?)",
    )
    .bind(&asset_id)
    .bind(&tenant_id)
    .bind(&owner_id)
    .execute(pool)
    .await
    .unwrap();
    let router = management.router;
    let owner_cookie = user_account_cookie(&router).await;

    let created = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/app/devices",
            Some(&owner_cookie),
            &format!("display_name=Owner+device&asset_id={asset_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::SEE_OTHER);

    let device_id = sqlx::query_scalar::<_, String>(
        "SELECT device_id FROM devices WHERE tenant_id = ? AND display_name = 'Owner device'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let ownership = sqlx::query_as::<_, (String, String)>(
        "SELECT owner_user_id, asset_id FROM devices WHERE device_id = ?",
    )
    .bind(&device_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(ownership, (owner_id, asset_id));

    let updated = router
        .oneshot(system_lifecycle_form(
            &format!("/app/devices/{device_id}"),
            Some(&owner_cookie),
            "display_name=Renamed+device&asset_id=",
        ))
        .await
        .unwrap();
    assert_eq!(updated.status(), StatusCode::SEE_OTHER);
    let name =
        sqlx::query_scalar::<_, String>("SELECT display_name FROM devices WHERE device_id = ?")
            .bind(&device_id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(name, "Renamed device");
}

#[tokio::test]
async fn tenant_account_assigns_and_unassigns_device_and_asset_owners_revoking_existing_shares() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let owner_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM users WHERE tenant_id = ? AND username = 'viewer'",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let recipient_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'assignment-recipient', ?, 'viewer', 'user')",
    )
    .bind(&recipient_id)
    .bind(&tenant_id)
    .bind(hash_password("AssignmentRecipient@1234").unwrap())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('tenant-assignment-device', ?, 'Tenant assignment device')",
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, created_by_user_id
         ) VALUES (?, ?, ?, 'tenant-assignment-device', 'viewer', ?)",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(&tenant_id)
    .bind(&recipient_id)
    .bind(&owner_id)
    .execute(pool)
    .await
    .unwrap();

    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/management/devices/tenant-assignment-device/owner")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(json!({ "user_id": owner_id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_user_id FROM devices WHERE device_id = 'tenant-assignment-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(owner_id.clone())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions
             WHERE tenant_id = ? AND device_id = 'tenant-assignment-device' AND revoked_at IS NULL",
        )
        .bind(&tenant_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );

    let asset_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'Tenant assignment asset')",
    )
    .bind(asset_id.to_string())
    .bind(&tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, asset_id, permission, created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', ?)",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(&tenant_id)
    .bind(&recipient_id)
    .bind(asset_id.to_string())
    .bind(&owner_id)
    .execute(pool)
    .await
    .unwrap();

    let assign_asset = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/management/assets/{asset_id}/owner"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(json!({ "user_id": owner_id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(assign_asset.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_user_id FROM assets WHERE id = ?",)
            .bind(asset_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        Some(owner_id.clone())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions
             WHERE tenant_id = ? AND asset_id = ? AND revoked_at IS NULL",
        )
        .bind(&tenant_id)
        .bind(asset_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );

    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, asset_id, permission, created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', ?)",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(&tenant_id)
    .bind(&recipient_id)
    .bind(asset_id.to_string())
    .bind(&owner_id)
    .execute(pool)
    .await
    .unwrap();

    let unassign_asset = router
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/management/assets/{asset_id}/owner"))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(json!({ "user_id": null }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unassign_asset.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_user_id FROM assets WHERE id = ?")
            .bind(asset_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions
             WHERE tenant_id = ? AND asset_id = ? AND revoked_at IS NULL",
        )
        .bind(&tenant_id)
        .bind(asset_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn tenant_account_cannot_create_or_revoke_direct_resource_permissions() {
    let (_directory, store, management) = management_session_router_with_store().await;
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
    let asset_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'Tenant shared asset')",
    )
    .bind(asset_id.to_string())
    .bind(&tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('tenant-shared-device', ?, 'Tenant shared device')",
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .unwrap();

    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    for body in [
        format!(
            "subject=user:{viewer_id}&scope=device&resource_id=tenant-shared-device&permission=control"
        ),
        format!("subject=user:{viewer_id}&scope=asset&resource_id={asset_id}&permission=view"),
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
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    let permission_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO resource_permissions (
             id, tenant_id, subject_user_id, device_id, permission, created_by_user_id
         ) VALUES (?, ?, ?, 'tenant-shared-device', 'viewer', ?)",
    )
    .bind(&permission_id)
    .bind(&tenant_id)
    .bind(&viewer_id)
    .bind(&viewer_id)
    .execute(pool)
    .await
    .unwrap();
    let revoke = router
        .oneshot(system_lifecycle_form(
            "/tenant/permissions/revoke",
            Some(&tenant_cookie),
            &format!("permission_id={permission_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(&permission_id)
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn system_page_lists_only_tenant_slug_and_status_without_runtime_placeholders() {
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
    assert!(body.contains("status-chip--ready\">active</span>"));
    assert!(body.contains("status-chip--warning\">suspended</span>"));
    assert!(body.contains("Not ready"));
    assert!(!body.contains("Not reported"));
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
async fn system_infrastructure_page_and_fragment_require_a_system_account() {
    let (_directory, _store, management) = management_session_router_with_store().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;
    let admin_cookie = management_user_cookie(&router, "admin", "NanoAdmin@1234").await;

    for path in ["/system/infrastructure", "/system/infrastructure/status"] {
        let response = router
            .clone()
            .oneshot(platform_get(path, Some(&system_cookie)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");

        for cookie in [
            None,
            Some(tenant_cookie.as_str()),
            Some(user_cookie.as_str()),
            Some(admin_cookie.as_str()),
        ] {
            let response = router
                .clone()
                .oneshot(platform_get(path, cookie))
                .await
                .unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ),
                "{path}: unexpected status {}",
                response.status()
            );
        }
    }
}

#[tokio::test]
async fn system_infrastructure_page_renders_non_secret_runtime_status() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let tenant_metadata = "infrastructure-page-tenant-metadata";
    sqlx::query("UPDATE tenants SET metadata = ? WHERE slug = ?")
        .bind(tenant_metadata)
        .bind("test")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let response = router
        .clone()
        .oneshot(platform_get("/system/infrastructure", Some(&system_cookie)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("Infrastructure"));
    for status in [
        "Runtime health",
        "Not ready",
        "HTTP listener",
        "MQTT plaintext listener",
        "MQTT TLS listener",
        "Migrations",
        "Storage",
        "TLS",
    ] {
        assert!(body.contains(status), "missing {status}: {body}");
    }
    assert!(!body.contains("Not reported"));
    assert!(!body.contains(tenant_metadata));
    assert!(!body.contains("TenantAccount@2026"));
    assert!(!body.contains("SystemAccount@2026"));
    assert!(body.contains("src=\"/assets/htmx.min.js\""));
    assert!(body.contains("hx-get=\"/system/infrastructure/status\""));
    assert!(body.contains("hx-trigger=\"every 5s, visibilityrefresh\""));
    assert!(body.contains("data-pause-when-hidden"));

    let htmx = router
        .oneshot(platform_get("/assets/htmx.min.js", None))
        .await
        .unwrap();
    assert_eq!(htmx.status(), StatusCode::OK);
    assert!(
        htmx.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/javascript")
    );
    let htmx = to_bytes(htmx.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&htmx).contains("htmx"));
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
async fn system_html_lifecycle_forms_report_tenant_field_validation_without_reflecting_input() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;

    for (form, notice, message, private_value) in [
        (
            "slug=InvalidTenant&tenant_account_username=valid-admin&tenant_account_password=TenantAccount%402026",
            "invalid-tenant-slug",
            "Tenant slug must use lowercase letters, digits, or hyphens.",
            "InvalidTenant",
        ),
        (
            "slug=valid-tenant&tenant_account_username=invalid+username&tenant_account_password=TenantAccount%402026",
            "invalid-tenant-account-username",
            "Tenant Account username must be 3 to 64 characters using letters, digits, hyphens, or underscores.",
            "invalid username",
        ),
        (
            "slug=valid-tenant&tenant_account_username=valid-admin&tenant_account_password=too-short",
            "invalid-tenant-account-password",
            "Tenant Account password must be at least 8 ASCII characters and include uppercase, lowercase, a number, and a symbol.",
            "too-short",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(system_lifecycle_form(
                "/system/tenants",
                Some(&system_cookie),
                form,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers()[LOCATION],
            format!("/system?notice={notice}")
        );

        let page = router
            .clone()
            .oneshot(platform_get(
                &format!("/system?notice={notice}"),
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
        assert!(page.contains(message));
        assert!(!page.contains(private_value));
    }
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
                .uri("/api/v1/auth/login")
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
                .uri("/api/v1/auth/logout")
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
async fn platform_logout_revokes_session_clears_cookie_and_redirects_to_login() {
    let (_directory, _store, management) = management_session_router_with_store().await;
    let router = management.router;
    let cookie = system_account_cookie(&router).await;

    let logout = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/logout")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(logout.status(), StatusCode::SEE_OTHER);
    assert_eq!(logout.headers()[LOCATION], "/login");
    assert!(
        logout.headers()[SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );

    let revoked = router
        .oneshot(platform_get("/system", Some(&cookie)))
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn management_login_rejects_invalid_credentials_without_setting_a_session_cookie() {
    let (_directory, management) = management_session_router().await;
    let response = management
        .router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
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
        "/api/v1/auth/login",
        "/api/v1/auth/logout",
        "/api/v1/auth/me",
        "/api/v1/management/alerts",
        "/api/v1/management/alerts/summary",
        "/api/v1/management/alert-incidents",
        "/api/v1/management/alert-incidents/{incident_id}/acknowledge",
        "/api/v1/management/alert-rules",
        "/api/v1/management/alert-rules/{rule_id}",
        "/api/v1/management/alert-rules/{rule_id}/archive",
        "/api/v1/management/audit",
        "/api/v1/management/applications",
        "/api/v1/management/personal-access-token",
        "/api/v1/management/personal-access-token/revoke",
        "/api/v1/management/ota/artifacts",
        "/api/v1/management/ota/policy",
        "/api/v1/management/assets",
        "/api/v1/management/assets/{asset_id}",
        "/api/v1/management/devices",
        "/api/v1/management/devices/{device_id}",
        "/api/v1/management/devices/{device_id}/owner",
        "/api/v1/management/devices/{device_id}/claim-code",
        "/api/v1/management/devices/{device_id}/tokens",
        "/api/v1/management/devices/{device_id}/tokens/{token_id}/rotate",
        "/api/v1/management/devices/{device_id}/token",
        "/api/v1/management/devices/{device_id}/telemetry",
        "/api/v1/management/assets/{asset_id}/owner",
        "/api/v1/management/profiles/asset-profiles",
        "/api/v1/management/profiles/asset-profiles/{profile_id}",
        "/api/v1/management/profiles/device-profiles",
        "/api/v1/management/profiles/device-profiles/{profile_id}",
        "/api/v1/management/profile",
        "/api/v1/management/profile/export",
        "/api/v1/management/profile/import",
        "/api/v1/management/users",
        "/api/v1/management/users/{username}",
        "/api/v1/management/users/{username}/capabilities",
    ]);
    assert_eq!(actual_paths, expected_paths);

    let expected_methods = BTreeMap::from([
        ("/api/v1/auth/login", BTreeSet::from(["post"])),
        ("/api/v1/auth/logout", BTreeSet::from(["post"])),
        ("/api/v1/auth/me", BTreeSet::from(["get"])),
        ("/api/v1/management/alerts", BTreeSet::from(["get"])),
        ("/api/v1/management/alerts/summary", BTreeSet::from(["get"])),
        (
            "/api/v1/management/alert-incidents",
            BTreeSet::from(["get"]),
        ),
        (
            "/api/v1/management/alert-incidents/{incident_id}/acknowledge",
            BTreeSet::from(["post"]),
        ),
        (
            "/api/v1/management/alert-rules",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/v1/management/alert-rules/{rule_id}",
            BTreeSet::from(["put"]),
        ),
        (
            "/api/v1/management/alert-rules/{rule_id}/archive",
            BTreeSet::from(["post"]),
        ),
        ("/api/v1/management/audit", BTreeSet::from(["get"])),
        ("/api/v1/management/applications", BTreeSet::from(["post"])),
        (
            "/api/v1/management/personal-access-token",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/v1/management/personal-access-token/revoke",
            BTreeSet::from(["post"]),
        ),
        (
            "/api/v1/management/ota/artifacts",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/v1/management/ota/policy",
            BTreeSet::from(["get", "put"]),
        ),
        ("/api/v1/management/assets", BTreeSet::from(["get", "post"])),
        (
            "/api/v1/management/assets/{asset_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        (
            "/api/v1/management/devices",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/v1/management/devices/{device_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        (
            "/api/v1/management/devices/{device_id}/owner",
            BTreeSet::from(["put"]),
        ),
        (
            "/api/v1/management/devices/{device_id}/telemetry",
            BTreeSet::from(["get"]),
        ),
        (
            "/api/v1/management/devices/{device_id}/claim-code",
            BTreeSet::from(["post"]),
        ),
        (
            "/api/v1/management/devices/{device_id}/tokens",
            BTreeSet::from(["post"]),
        ),
        (
            "/api/v1/management/devices/{device_id}/tokens/{token_id}/rotate",
            BTreeSet::from(["post"]),
        ),
        (
            "/api/v1/management/devices/{device_id}/token",
            BTreeSet::from(["get"]),
        ),
        (
            "/api/v1/management/assets/{asset_id}/owner",
            BTreeSet::from(["put"]),
        ),
        (
            "/api/v1/management/profiles/asset-profiles",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/v1/management/profiles/asset-profiles/{profile_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        (
            "/api/v1/management/profiles/device-profiles",
            BTreeSet::from(["get", "post"]),
        ),
        (
            "/api/v1/management/profiles/device-profiles/{profile_id}",
            BTreeSet::from(["delete", "put"]),
        ),
        ("/api/v1/management/profile", BTreeSet::from(["get", "put"])),
        ("/api/v1/management/profile/export", BTreeSet::from(["get"])),
        ("/api/v1/management/profile/import", BTreeSet::from(["put"])),
        ("/api/v1/management/users", BTreeSet::from(["get", "post"])),
        (
            "/api/v1/management/users/{username}",
            BTreeSet::from(["put"]),
        ),
        (
            "/api/v1/management/users/{username}/capabilities",
            BTreeSet::from(["put"]),
        ),
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
        paths["/api/v1/management/alerts"]["get"]["responses"]["403"]["description"],
        "Tenant Account required"
    );
    assert_eq!(
        paths["/api/v1/management/audit"]["get"]["parameters"],
        json!([
            {
                "name": "after",
                "in": "query",
                "required": false,
                "description": "Opaque keyset cursor for older audit events.",
                "schema": {"type": "string"}
            },
            {
                "name": "limit",
                "in": "query",
                "required": false,
                "description": "Maximum number of audit events to return.",
                "schema": {"type": "integer", "minimum": 1, "maximum": 100}
            }
        ])
    );

    for (path, method, status, schema) in [
        (
            "/api/v1/management/alerts",
            "get",
            "200",
            "#/components/schemas/ManagementAlertList",
        ),
        (
            "/api/v1/management/alerts/summary",
            "get",
            "200",
            "#/components/schemas/ManagementAlertSummary",
        ),
        (
            "/api/v1/management/alert-incidents",
            "get",
            "200",
            "#/components/schemas/ManagementAlertIncidentList",
        ),
        (
            "/api/v1/management/alert-rules",
            "get",
            "200",
            "#/components/schemas/ManagementAlertRuleList",
        ),
        (
            "/api/v1/management/alert-rules",
            "post",
            "201",
            "#/components/schemas/ManagementAlertRule",
        ),
        (
            "/api/v1/management/audit",
            "get",
            "200",
            "#/components/schemas/ManagementAuditEventPage",
        ),
        (
            "/api/v1/management/personal-access-token",
            "get",
            "200",
            "#/components/schemas/PersonalAccessTokenGetResponse",
        ),
        (
            "/api/v1/management/personal-access-token",
            "post",
            "201",
            "#/components/schemas/PersonalAccessTokenCreateResponse",
        ),
        (
            "/api/v1/management/ota/artifacts",
            "get",
            "200",
            "#/components/schemas/OtaArtifactList",
        ),
        (
            "/api/v1/management/ota/artifacts",
            "post",
            "201",
            "#/components/schemas/OtaArtifact",
        ),
        (
            "/api/v1/management/ota/policy",
            "get",
            "200",
            "#/components/schemas/OtaPolicy",
        ),
        (
            "/api/v1/management/devices",
            "post",
            "201",
            "#/components/schemas/DeviceToken",
        ),
        (
            "/api/v1/management/devices/{device_id}/tokens",
            "post",
            "201",
            "#/components/schemas/DeviceToken",
        ),
        (
            "/api/v1/management/devices/{device_id}/claim-code",
            "post",
            "201",
            "#/components/schemas/ManagementDeviceClaimCode",
        ),
        (
            "/api/v1/management/devices/{device_id}/tokens/{token_id}/rotate",
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
        "ManagementAlertIncident",
        "ManagementAlertIncidentList",
        "ManagementAlertRule",
        "ManagementAlertRuleList",
        "ManagementAlertRuleRequest",
        "ManagementAlertList",
        "ManagementAlertSummary",
        "ManagementAuditEvent",
        "ManagementAuditEventPage",
        "ManagementAsset",
        "ManagementAssetList",
        "ManagementAssetRequest",
        "ManagementDevice",
        "ManagementDeviceClaimCode",
        "ManagementDeviceList",
        "ManagementDeviceTelemetry",
        "ManagementDeviceTelemetryPage",
        "ManagementDeviceUpdateRequest",
        "ManagementResourceOwnerRequest",
        "ManagementUser",
        "ManagementUserCapabilitiesRequest",
        "ManagementUserCreateRequest",
        "ManagementUserList",
        "ManagementUserUpdateRequest",
        "OtaArtifact",
        "OtaArtifactList",
        "OtaArtifactUpload",
        "OtaPolicy",
        "PersonalAccessTokenCreateResponse",
        "PersonalAccessTokenGetResponse",
        "PersonalAccessTokenMetadata",
        "PersonalAccessTokenRequest",
        "PlatformLoginResponse",
        "SessionResponse",
        "TenantProfileConfiguration",
        "TenantProfileContainmentRule",
        "TenantProfileDefinition",
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

    let personal_access_token_metadata = &schemas["PersonalAccessTokenMetadata"];
    assert!(
        personal_access_token_metadata["properties"]
            .get("secret")
            .is_none()
    );
    assert!(
        schemas["PersonalAccessTokenGetResponse"]["properties"]
            .get("secret")
            .is_none()
    );
    assert_eq!(
        schemas["PersonalAccessTokenCreateResponse"]["properties"]["secret"]["type"],
        "string"
    );

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

    let logout = &document["paths"]["/api/v1/auth/logout"]["post"];
    assert!(logout.get("security").is_none());
    assert!(logout["responses"]["204"]["content"].is_null());

    let rendered = document.to_string();
    for forbidden in [
        "\"/api/v1/devices",
        "\"/api/v1/assets",
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
                .uri("/api/v1/system/auth/login")
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
                .uri("/api/v1/tenant/auth/login")
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
                .uri("/api/v1/management/applications")
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
                .uri("/api/v1/auth/login")
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
                .uri("/api/v1/management/applications")
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
                .uri("/api/v1/auth/login")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/tenant/auth/login")
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
                .uri("/api/v1/management/devices")
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
                    .uri("/api/v1/auth/login")
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
                    .uri("/api/v1/management/devices")
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
async fn tenant_account_can_replace_a_device_token() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;
    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/devices")
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
                .uri(format!("/api/v1/management/devices/{device_id}/tokens"))
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
async fn tenant_account_manages_a_personal_access_token_without_list_secret_leaks() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;

    let page = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tenant/personal-access-tokens")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
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
    assert!(page.contains("Personal access tokens"));
    assert!(!page.contains("iotpat_"));

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/personal-access-token")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: serde_json::Value =
        serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(listed["token"].is_null());
    assert!(listed.get("secret").is_none());

    let invalid = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/personal-access-token")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(r#"{"name":"   "}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let created = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/personal-access-token")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(r#"{"name":"CI deployment"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(created.headers()[CACHE_CONTROL], "no-store");
    let created: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let first_secret = created["secret"].as_str().unwrap().to_owned();
    assert!(first_secret.starts_with("iotpat_"));
    assert_eq!(created["token"]["name"], "CI deployment");
    assert!(created["token"].get("token_hash").is_none());

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/personal-access-token")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: serde_json::Value =
        serde_json::from_slice(&to_bytes(listed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(listed["token"]["name"], "CI deployment");
    assert!(listed.get("secret").is_none());
    assert!(!listed.to_string().contains(&first_secret));

    let rotated = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/personal-access-token")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(r#"{"name":"CI deployment rotated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::CREATED);
    let rotated: serde_json::Value =
        serde_json::from_slice(&to_bytes(rotated.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_ne!(rotated["secret"], first_secret);

    let revoked = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/personal-access-token/revoke")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
    assert!(
        to_bytes(revoked.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let user_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
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
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/personal-access-token")
                .header(COOKIE, user_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let denied_page = router
        .oneshot(
            Request::builder()
                .uri("/tenant/personal-access-tokens")
                .header(COOKIE, user_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied_page.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_account_can_reveal_the_single_active_device_token() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;
    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"Copyable Token Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provisioned.status(), StatusCode::CREATED);
    let provisioned: serde_json::Value =
        serde_json::from_slice(&to_bytes(provisioned.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let device_id = provisioned["device_id"].as_str().unwrap();
    let first_token = provisioned["token"].as_str().unwrap();

    let revealed = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/management/devices/{device_id}/token"))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revealed.status(), StatusCode::OK);
    assert_eq!(revealed.headers()[CACHE_CONTROL], "no-store");
    let revealed: serde_json::Value =
        serde_json::from_slice(&to_bytes(revealed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(revealed["device_id"], device_id);
    assert_eq!(revealed["token"], first_token);

    let replaced = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/management/devices/{device_id}/tokens"))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replaced.status(), StatusCode::CREATED);
    let replaced: serde_json::Value =
        serde_json::from_slice(&to_bytes(replaced.into_body(), usize::MAX).await.unwrap()).unwrap();
    let replacement_token = replaced["token"].as_str().unwrap();
    assert_ne!(replacement_token, first_token);

    let revealed = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/management/devices/{device_id}/token"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revealed.status(), StatusCode::OK);
    let revealed: serde_json::Value =
        serde_json::from_slice(&to_bytes(revealed.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(revealed["token"], replacement_token);

    let active_tokens: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM device_tokens WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(device_id)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(active_tokens, 1);
}

#[tokio::test]
async fn tenant_account_can_list_raw_device_telemetry_for_a_selected_range() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;
    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"Telemetry Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provisioned.status(), StatusCode::CREATED);
    let provisioned: serde_json::Value =
        serde_json::from_slice(&to_bytes(provisioned.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let device_id = provisioned["device_id"].as_str().unwrap();
    let tenant_id: String = sqlx::query_scalar("SELECT tenant_id FROM devices WHERE device_id = ?")
        .bind(device_id)
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let observed_at = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(observed_at.to_rfc3339())
    .bind(observed_at.to_rfc3339())
    .bind(tenant_id)
    .bind(device_id)
    .bind("telemetry-boot-1")
    .bind(1_i64)
    .bind(r#"{"temperature_c":22.5,"switch":"on"}"#)
    .bind("v1/devices/me/telemetry")
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let telemetry = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/management/devices/{device_id}/telemetry?range=1h"
                ))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(telemetry.status(), StatusCode::OK);
    let telemetry: serde_json::Value =
        serde_json::from_slice(&to_bytes(telemetry.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(telemetry["items"][0]["device_id"], device_id);
    assert_eq!(telemetry["items"][0]["measurements"]["temperature_c"], 22.5);
    assert_eq!(telemetry["items"][0]["measurements"]["switch"], "on");
}

#[tokio::test]
async fn tenant_account_provisioning_accepts_assignment_and_attributes() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;

    let asset = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/assets")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"name":"Assigned Asset","metadata":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(asset.status(), StatusCode::CREATED);
    let asset: serde_json::Value =
        serde_json::from_slice(&to_bytes(asset.into_body(), usize::MAX).await.unwrap()).unwrap();

    let profile = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/profiles/device-profiles")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    r#"{"name":"Assigned Profile","telemetry_schema":{},"metric_mapping":{},"reporting_settings":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(profile.status(), StatusCode::CREATED);
    let profile: serde_json::Value =
        serde_json::from_slice(&to_bytes(profile.into_body(), usize::MAX).await.unwrap()).unwrap();

    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(
                    json!({
                        "display_name": "Assigned Device",
                        "asset_id": asset["id"],
                        "device_profile_id": profile["id"],
                        "attributes": {"site": "lab"},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provisioned.status(), StatusCode::CREATED);
    assert_eq!(provisioned.headers()[CACHE_CONTROL], "no-store");
    let provisioned: serde_json::Value =
        serde_json::from_slice(&to_bytes(provisioned.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let device_id = provisioned["device_id"].as_str().unwrap();

    let stored_asset: Option<String> =
        sqlx::query_scalar("SELECT asset_id FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let stored_profile: Option<String> =
        sqlx::query_scalar("SELECT device_profile_id FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let stored_serial_number: Option<String> =
        sqlx::query_scalar("SELECT serial_number FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let stored_attributes: String =
        sqlx::query_scalar("SELECT metadata FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(stored_asset.as_deref(), asset["id"].as_str());
    assert_eq!(stored_profile.as_deref(), profile["id"].as_str());
    assert_eq!(stored_serial_number, None);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored_attributes).unwrap(),
        json!({"site": "lab"})
    );
}

#[tokio::test]
async fn tenant_account_rotates_a_specific_active_device_token() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;
    let provisioned = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"Rotatable Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let provisioned: serde_json::Value =
        serde_json::from_slice(&to_bytes(provisioned.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let device_id = provisioned["device_id"].as_str().unwrap();
    let token_id = provisioned["id"].as_str().unwrap();
    let first_token = provisioned["token"].as_str().unwrap();

    let rotated = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/management/devices/{device_id}/tokens/{token_id}/rotate"
                ))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::CREATED);
    assert_eq!(rotated.headers()[CACHE_CONTROL], "no-store");
    let rotated: serde_json::Value =
        serde_json::from_slice(&to_bytes(rotated.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(rotated["device_id"], device_id);
    assert_ne!(rotated["id"], token_id);
    assert_ne!(rotated["token"].as_str().unwrap(), first_token);

    let old_token_revoked: Option<String> =
        sqlx::query_scalar("SELECT revoked_at FROM device_tokens WHERE id = ?")
            .bind(token_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(old_token_revoked.is_some());
    let active_tokens: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM device_tokens WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(device_id)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(active_tokens, 1);
}

#[tokio::test]
async fn tenant_account_manages_alert_rules_and_incidents() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let cookie = tenant_account_cookie(&router).await;
    let device = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(r#"{"display_name":"Alert Device"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let device: serde_json::Value =
        serde_json::from_slice(&to_bytes(device.into_body(), usize::MAX).await.unwrap()).unwrap();
    let device_id = device["device_id"].as_str().unwrap();

    let request = json!({
        "name": "High Power",
        "enabled": true,
        "device_id": device_id,
        "metric_key": "power_w",
        "rule_type": "event_threshold",
        "comparison": "gt",
        "threshold": 500.0,
        "severity": "warning",
    });
    let rule = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/management/alert-rules")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &cookie)
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rule.status(), StatusCode::CREATED);
    let rule: serde_json::Value =
        serde_json::from_slice(&to_bytes(rule.into_body(), usize::MAX).await.unwrap()).unwrap();
    let rule_id = rule["id"].as_str().unwrap();

    let tenant_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let incident_id = uuid::Uuid::now_v7().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO alert_incidents (
             id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, last_value
         ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?)",
    )
    .bind(&incident_id)
    .bind(&tenant_id)
    .bind(rule_id)
    .bind(device_id)
    .bind(&now)
    .bind(&now)
    .bind(750.0_f64)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let summary = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/alerts/summary")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(summary.status(), StatusCode::OK);
    let summary: serde_json::Value =
        serde_json::from_slice(&to_bytes(summary.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(summary["open_incident_count"], 1);

    let acknowledged = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/v1/management/alert-incidents/{incident_id}/acknowledge"
                ))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(acknowledged.status(), StatusCode::OK);
    let acknowledged: serde_json::Value = serde_json::from_slice(
        &to_bytes(acknowledged.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(acknowledged["id"], incident_id);
    assert!(acknowledged["acknowledged_at"].is_string());

    let archived = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/management/alert-rules/{rule_id}/archive"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(archived.status(), StatusCode::NO_CONTENT);
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
                .uri(format!("/api/v1/management/devices/{gateway_id}"))
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
                    .uri(format!("/api/v1/management/devices/{child_id}"))
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
                .uri(format!("/api/v1/management/devices/{child_id}"))
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
                .uri(format!("/api/v1/management/devices/{gateway_id}"))
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
                .uri(format!("/api/v1/management/devices/{child_id}"))
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
                .uri(format!("/api/v1/management/devices/{gateway_id}"))
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
                .uri("/api/v1/tenant/auth/login")
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
                .uri("/api/v1/management/assets")
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
                .uri("/api/v1/management/assets")
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
                .uri(format!("/api/v1/management/assets/{asset_id}"))
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
                .uri("/api/v1/management/assets/not-a-uuid")
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
                .uri("/api/v1/management/assets/not-a-uuid")
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
                .uri("/api/v1/management/assets")
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
                .uri("/api/v1/management/assets/00000000-0000-0000-0000-000000000000")
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
                .uri(format!("/api/v1/management/assets/{asset_id}"))
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices/not.valid")
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
                .uri("/api/v1/management/devices/missing-device")
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
                .uri("/api/v1/management/devices")
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
                .uri(format!("/api/v1/management/devices/{gateway_id}"))
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
            .uri("/api/v1/management/applications")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"app_id":"test-app","kind":"frontend","launch_url":"https://example.test","client_id":"test-client","redirect_uris":["https://example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
            ))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/management/devices")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"display_name":"Anonymous"}"#))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/management/devices/missing-device/tokens")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices/missing-device/tokens")
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
                .uri("/api/v1/management/devices/missing-device/tokens")
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
                .uri(format!("/api/v1/management/devices/{gateway_id}"))
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
                .uri(format!("/api/v1/management/devices/{child_id}"))
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
                .uri(format!("/api/v1/management/devices/{child_id}/tokens"))
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
            .uri("/api/v1/management/applications")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(
                r#"{"app_id":"unavailable-app","kind":"frontend","launch_url":"https://example.test","client_id":"unavailable-client","redirect_uris":["https://example.test/callback"],"allowed_scopes":["devices:read"],"enabled":true}"#,
            ))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/management/devices")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(r#"{"display_name":"Unavailable"}"#))
            .unwrap(),
        Request::builder()
            .method("PUT")
            .uri("/api/v1/management/devices/unavailable-device")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(r#"{"display_name":"Unavailable"}"#))
            .unwrap(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/management/assets")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, &cookie)
            .body(Body::from(
                r#"{"name":"Unavailable","asset_profile_id":null,"parent_asset_id":null,"metadata":{},"attributes":{}}"#,
            ))
            .unwrap(),
        Request::builder()
            .method("PUT")
            .uri("/api/v1/management/assets/00000000-0000-0000-0000-000000000000")
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
                .uri("/api/v1/management/devices")
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
                .uri("/api/v1/management/devices")
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
        ("POST", "/api/v1/management/applications"),
        ("POST", "/api/v1/management/devices"),
        ("PUT", "/api/v1/management/devices/not-valid"),
        ("POST", "/api/v1/management/assets"),
        ("PUT", "/api/v1/management/assets/not-valid"),
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
                .uri("/api/v1/management/devices")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(
                    json!({
                        "serial_number": format!("TEST-{}", uuid::Uuid::now_v7()),
                        "display_name": display_name,
                    })
                    .to_string(),
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
        .uri("/api/v1/auth/login")
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
async fn tenant_overview_displays_authenticated_tenant_resource_counts() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_id = sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'Overview asset')")
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(&tenant_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name)
         VALUES ('overview-device', ?, 'Overview device')",
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .unwrap();

    let response = router
        .oneshot(platform_get("/tenant", Some(&tenant_cookie)))
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

    assert!(
        body.contains(
            r#"data-testid="tenant-overview-users"><span>Users</span><strong>2</strong>"#
        )
    );
    assert!(
        body.contains(
            r#"data-testid="tenant-overview-assets"><span>Assets</span><strong>1</strong>"#
        )
    );
    assert!(body.contains(
        r#"data-testid="tenant-overview-devices"><span>Devices</span><strong>1</strong>"#
    ));
    assert!(body.contains(
        r#"data-testid="tenant-overview-open-alerts"><span>Open alerts</span><strong>0</strong>"#
    ));
    assert!(!body.contains("Data will appear after the route contract is connected."));
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
async fn tenant_profile_forms_are_scoped_to_the_current_tenant() {
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
}

#[tokio::test]
async fn legacy_device_token_html_routes_are_not_exposed() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;

    for (method, path) in [
        ("GET", "/tenant/devices/device-1/tokens"),
        ("POST", "/tenant/devices/device-1/tokens/issue"),
        ("POST", "/tenant/devices/device-1/tokens/revoke"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(COOKIE, &tenant_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
    }
}

#[tokio::test]
async fn tenant_profile_routes_deny_non_tenant_sessions_before_form_parsing() {
    let (_directory, management) = management_session_router().await;
    let router = management.router;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router).await;

    for (path, form_path) in [
        ("/tenant/profiles/device", "/tenant/profiles/device"),
        ("/tenant/profiles/asset", "/tenant/profiles/asset"),
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

    let permission_grant = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/permissions",
            Some(&tenant_cookie),
            &format!(
                "subject=user%3A{viewer_id}&scope=asset&resource_id={asset_id}&permission=control"
            ),
        ))
        .await
        .unwrap();
    assert_eq!(permission_grant.status(), StatusCode::FORBIDDEN);
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
        0
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
    assert!(permissions_body.contains("Assigned user"));
    assert!(permissions_body.contains("Only the assigned owner"));
    assert!(!permissions_body.contains("Grant asset permission"));
    assert!(!permissions_body.contains("Revoke permission"));

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

    let cross_tenant_group_member = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/groups/members",
            Some(&tenant_cookie),
            &format!("group_id={other_group_id}&user_id={viewer_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(cross_tenant_group_member.status(), StatusCode::SEE_OTHER);
    let cross_tenant_permission = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/permissions",
            Some(&tenant_cookie),
            &format!(
                "subject=user%3A{viewer_id}&scope=asset&resource_id={other_asset_id}&permission=view"
            ),
        ))
        .await
        .unwrap();
    assert_eq!(cross_tenant_permission.status(), StatusCode::FORBIDDEN);
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
            "slug=fresh-tenant&tenant_account_username=fresh-admin&tenant_account_password=FreshTenant%402026",
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
            "/login",
            None,
            "username=fresh-admin&password=FreshTenant%402026",
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
            "SELECT account_class
             FROM users
             JOIN tenants ON tenants.id = users.tenant_id
             WHERE tenants.slug = 'fresh-tenant' AND users.username = 'fresh-user'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        "user"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM user_capabilities
             JOIN users ON users.id = user_capabilities.user_id
             JOIN tenants ON tenants.id = users.tenant_id
             WHERE tenants.slug = 'fresh-tenant' AND users.username = 'fresh-user'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        5
    );

    let user_login = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/login",
            None,
            "username=fresh-user&password=FreshUser%402026",
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
                .uri(format!("/api/v1/management/devices/{gateway_id}"))
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
    let tenant_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(pool)
        .await
        .unwrap();
    let asset_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'Relation asset')",
    )
    .bind(asset_id.to_string())
    .bind(&tenant_id)
    .execute(pool)
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
                "from_device_id={first_id}&target_kind=device&to_device_id={second_id}&relation_type=located_near&tenant_id=injected"
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
                "from_device_id={first_id}&target_kind=device&to_device_id={second_id}&relation_type=located_near"
            ),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        created.headers()[LOCATION],
        "/tenant/relations?notice=relation-created"
    );
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

    let asset_relation = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/relations",
            Some(&tenant_cookie),
            &format!(
                "from_device_id={first_id}&target_kind=asset&to_asset_id={asset_id}&relation_type=measures"
            ),
        ))
        .await
        .unwrap();
    assert_eq!(asset_relation.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        asset_relation.headers()[LOCATION],
        "/tenant/relations?notice=relation-created"
    );
    let asset_relation_id: String = sqlx::query_scalar(
        "SELECT id FROM device_asset_relations
         WHERE tenant_id = ? AND from_device_id = ? AND to_asset_id = ?",
    )
    .bind(&tenant_id)
    .bind(&first_id)
    .bind(asset_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();

    let deleted = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/relations/delete",
            Some(&tenant_cookie),
            &format!("target_kind=device&relation_id={relation_id}"),
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
            .bind(&tenant_id)
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
    let asset_deleted = router
        .clone()
        .oneshot(system_lifecycle_form(
            "/tenant/relations/delete",
            Some(&tenant_cookie),
            &format!("target_kind=asset&relation_id={asset_relation_id}"),
        ))
        .await
        .unwrap();
    assert_eq!(asset_deleted.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        asset_deleted.headers()[LOCATION],
        "/tenant/relations?notice=relation-deleted"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM device_asset_relations WHERE tenant_id = ?",
        )
        .bind(&tenant_id)
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
async fn tenant_account_has_no_application_scoped_profile_page_or_api() {
    let (_directory, _store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;

    let page = router
        .clone()
        .oneshot(platform_get(
            "/tenant/applications/powermonitor",
            Some(&tenant_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::NOT_FOUND);

    let api = router
        .oneshot(platform_get(
            "/api/v1/management/applications/powermonitor/domain-profiles",
            Some(&tenant_cookie),
        ))
        .await
        .unwrap();
    assert_eq!(api.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_account_imports_and_exports_its_single_profile_configuration() {
    let (_directory, store, management) = management_session_router_with_store().await;
    let router = management.router;
    let tenant_cookie = tenant_account_cookie(&router).await;

    let page = router
        .clone()
        .oneshot(platform_get("/tenant/profile", Some(&tenant_cookie)))
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
    assert!(page.contains("data-tenant-profile-json"));

    let initial = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/profile/export")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(initial.status(), StatusCode::OK);
    let initial: serde_json::Value =
        serde_json::from_slice(&to_bytes(initial.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        initial,
        json!({
            "version": 1,
            "profiles": [],
            "containment_rules": [],
            "permission_definitions": {}
        })
    );

    let farm_id = uuid::Uuid::now_v7();
    let zone_id = uuid::Uuid::now_v7();
    let meter_id = uuid::Uuid::now_v7();
    let configuration = json!({
        "version": 1,
        "profiles": [
            {
                "id": farm_id,
                "resource_kind": "asset",
                "name": "Farm",
                "definition": {"hierarchy": {"level": "site"}},
                "live_view": {"widgets": []}
            },
            {
                "id": zone_id,
                "resource_kind": "asset",
                "name": "Zone",
                "definition": {"hierarchy": {"level": "zone", "inherits": "Farm"}},
                "live_view": {"widgets": []}
            },
            {
                "id": meter_id,
                "resource_kind": "device",
                "name": "Meter",
                "definition": {"telemetry_schema": {"power_w": {"type": "number"}}},
                "live_view": {"charts": []}
            }
        ],
        "containment_rules": [{
            "parent_profile_id": farm_id,
            "child_profile_id": zone_id
        }],
        "permission_definitions": {
            "roles": {"operator": {"permissions": ["assets:read"]}}
        }
    });
    let imported = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/management/profile/import")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(configuration.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(imported.status(), StatusCode::NO_CONTENT);

    let exported = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/profile")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exported.status(), StatusCode::OK);
    let exported: serde_json::Value =
        serde_json::from_slice(&to_bytes(exported.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(exported, configuration);

    let mut invalid = configuration.clone();
    invalid["permission_definitions"] = json!([]);
    let rejected = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/management/profile/import")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(invalid.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);

    let after_rejection = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/profile/export")
                .header(COOKIE, &tenant_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_rejection.status(), StatusCode::OK);
    let after_rejection: serde_json::Value = serde_json::from_slice(
        &to_bytes(after_rejection.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(after_rejection, configuration);

    TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
        NewTenant {
            slug: "profile-other".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    let other_login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tenant/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "tenant_slug": "profile-other",
                        "password": "OtherTenant@2026"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(other_login.status(), StatusCode::OK);
    let other_cookie = other_login.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let other_export = router
        .oneshot(
            Request::builder()
                .uri("/api/v1/management/profile/export")
                .header(COOKIE, other_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(other_export.status(), StatusCode::OK);
    let other_export: serde_json::Value = serde_json::from_slice(
        &to_bytes(other_export.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(other_export, initial);
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
