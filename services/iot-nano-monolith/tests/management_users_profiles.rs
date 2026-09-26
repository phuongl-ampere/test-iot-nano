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
use iot_api::{TokenVault, hash_password};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{ManagementSessionRouter, bootstrap_system};
use iot_storage::{NewTenant, NewTenantAccount, PlatformStore, TenantIdentityRepository};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

async fn management_router() -> (tempfile::TempDir, Arc<PlatformStore>, axum::Router) {
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
    let (tenant, _) = TenantIdentityRepository::create_tenant_with_account(
        &store,
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
    for (username, password, role, account_class) in [
        ("admin", "NanoAdmin@1234", "admin", "admin"),
        ("viewer", "NanoView@1234", "viewer", "user"),
    ] {
        sqlx::query(
            "INSERT INTO users (
                id, tenant_id, username, password_hash, role, account_class
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(tenant.id.to_string())
        .bind(username)
        .bind(hash_password(password).unwrap())
        .bind(role)
        .bind(account_class)
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    }
    let management = ManagementSessionRouter::new(Arc::clone(&store), test_token_vault());
    let router = management
        .router
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    (directory, store, router)
}

async fn seed_alert(store: &PlatformStore, tenant_id: Uuid, rule_name: &str) {
    let pool = store.sqlite_pool().unwrap();
    let device_id = format!("alert-device-{}", Uuid::now_v7());
    let rule_id = Uuid::now_v7();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES (?, ?)")
        .bind(&device_id)
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold, severity
         ) VALUES (?, ?, ?, ?, 'temperature_c', 'event_threshold', 'gt', 30, 'critical')",
    )
    .bind(rule_id.to_string())
    .bind(tenant_id.to_string())
    .bind(rule_name)
    .bind(&device_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at, last_value
         ) VALUES (?, ?, ?, ?, 'open', CURRENT_TIMESTAMP, 42.5)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(tenant_id.to_string())
    .bind(rule_id.to_string())
    .bind(&device_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_audit_event(
    store: &PlatformStore,
    tenant_id: Uuid,
    occurred_at: &str,
    action: &str,
    target_type: &str,
    target_id: &str,
    changes: Value,
) -> (Uuid, Uuid) {
    let pool = store.sqlite_pool().unwrap();
    let actor_id: String = sqlx::query_scalar("SELECT id FROM tenant_accounts WHERE tenant_id = ?")
        .bind(tenant_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
    let actor_id = Uuid::parse_str(&actor_id).unwrap();
    let event_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO audit_events (
            id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
            action, target_type, target_id, changes
         ) VALUES (?, ?, ?, 'tenant_account', ?, ?, ?, ?, ?)",
    )
    .bind(event_id.to_string())
    .bind(tenant_id.to_string())
    .bind(occurred_at)
    .bind(actor_id.to_string())
    .bind(action)
    .bind(target_type)
    .bind(target_id)
    .bind(changes.to_string())
    .execute(pool)
    .await
    .unwrap();
    (event_id, actor_id)
}

async fn user_account_cookie(
    router: &axum::Router,
    tenant_slug: &str,
    username: &str,
    password: &str,
) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/user/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "tenant_slug": tenant_slug,
                        "username": username,
                        "password": password,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn tenant_account_cookie(router: &axum::Router, tenant_slug: &str, password: &str) -> String {
    let response = router
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
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn system_account_cookie(router: &axum::Router) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/system/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "username": "system", "password": "SystemAccount@2026" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()[SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn json_request(
    router: &axum::Router,
    method: &str,
    path: &str,
    cookie: &str,
    body: Value,
    expected_status: StatusCode,
) -> Value {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, cookie)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), expected_status, "{method} {path}");
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn management_user_routes_authorize_before_json_and_support_crud() {
    let (_directory, _store, router) = management_router().await;
    let viewer_cookie = user_account_cookie(&router, "test", "viewer", "NanoView@1234").await;
    let admin_cookie = user_account_cookie(&router, "test", "admin", "NanoAdmin@1234").await;
    let tenant_cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;

    for path in [
        "/api/management/users",
        "/api/management/profiles/device-profiles",
        "/api/management/profiles/asset-profiles",
    ] {
        let anonymous = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED, "{path}");

        let viewer = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header(COOKIE, &viewer_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(viewer.status(), StatusCode::FORBIDDEN, "{path}");

        let user_admin = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header(COOKIE, &admin_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(user_admin.status(), StatusCode::FORBIDDEN, "{path}");
    }

    for (method, path) in [
        ("POST", "/api/management/users"),
        ("PUT", "/api/management/users/alice"),
        ("POST", "/api/management/profiles/device-profiles"),
        ("PUT", "/api/management/profiles/device-profiles/not-a-uuid"),
        ("POST", "/api/management/profiles/asset-profiles"),
        ("PUT", "/api/management/profiles/asset-profiles/not-a-uuid"),
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
        assert_eq!(
            anonymous.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );

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
        assert_eq!(viewer.status(), StatusCode::FORBIDDEN, "{method} {path}");

        let user_admin = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(CONTENT_TYPE, "application/json")
                    .header(COOKIE, &admin_cookie)
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            user_admin.status(),
            StatusCode::FORBIDDEN,
            "{method} {path}"
        );
    }

    let missing_content_type = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/users")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    r#"{"username":"content-type","password":"ContentType@123"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        missing_content_type.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let oversized = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/profiles/asset-profiles")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_cookie)
                .body(Body::from(
                    json!({
                        "name": "Oversized",
                        "fields": { "value": "x".repeat(2 * 1024 * 1024) },
                        "dashboard_defaults": {},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let listed = router
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
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: Value = serde_json::from_slice(
        &axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(listed[0]["username"], "admin");
    assert!(listed[0]["id"].is_string());
    assert_eq!(listed[0]["role"], "admin");
    assert_eq!(listed[0]["account_class"], "admin");
    assert!(listed[0].get("default_app").is_none());
    assert!(listed[0].get("granted_apps").is_none());

    let created = json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "alice",
            "password": "AlicePassword@123",
        }),
        StatusCode::CREATED,
    )
    .await;
    assert!(created["id"].is_string());
    assert_eq!(created["username"], "alice");
    assert_eq!(created["role"], "viewer");
    assert_eq!(created["account_class"], "user");
    assert!(created["capabilities"].is_array());

    json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "alice",
            "password": "AlicePassword@123",
        }),
        StatusCode::CONFLICT,
    )
    .await;
    json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "not a valid username",
            "password": "InvalidUsername@123",
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "PUT",
        "/api/management/users/missing",
        &tenant_cookie,
        json!({
            "role": "admin",
        }),
        StatusCode::NOT_FOUND,
    )
    .await;
    let updated = json_request(
        &router,
        "PUT",
        "/api/management/users/alice",
        &tenant_cookie,
        json!({
            "role": "admin",
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated["role"], "admin");
    assert_eq!(updated["account_class"], "admin");

    json_request(
        &router,
        "PUT",
        "/api/management/users/alice",
        &tenant_cookie,
        json!({
            "role": "operator",
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;

    let alice_first_session =
        user_account_cookie(&router, "test", "alice", "AlicePassword@123").await;
    let alice_second_session =
        user_account_cookie(&router, "test", "alice", "AlicePassword@123").await;
    for cookie in [&alice_first_session, &alice_second_session] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/management/users")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    json_request(
        &router,
        "PUT",
        "/api/management/users/alice",
        &tenant_cookie,
        json!({
            "role": "viewer",
        }),
        StatusCode::OK,
    )
    .await;
    for cookie in [&alice_first_session, &alice_second_session] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/management/users")
                    .header(COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn management_user_routes_reject_removed_application_fields() {
    let (_directory, _store, router) = management_router().await;
    let tenant_cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;

    json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "legacy-contract-user",
            "password": "LegacyContract@123",
            "default_app": "/apps/powermonitor",
            "granted_apps": ["powermonitor"],
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;

    json_request(
        &router,
        "PUT",
        "/api/management/users/admin",
        &tenant_cookie,
        json!({
            "role": "admin",
            "default_app": "/apps/powermonitor",
            "granted_apps": ["powermonitor"],
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
}

#[tokio::test]
async fn management_device_profile_routes_support_crud_and_typed_errors() {
    let (_directory, _store, router) = management_router().await;
    let cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    let request = json!({
        "name": "Environmental Sensor",
        "telemetry_schema": {"temperature_c": {"type": "number"}},
        "metric_mapping": {"temperature_c": "temperature"},
        "reporting_settings": {"interval_seconds": 60},
    });
    let created = json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &cookie,
        request,
        StatusCode::CREATED,
    )
    .await;
    assert!(created["id"].is_string());
    assert_eq!(created["name"], "Environmental Sensor");
    assert_eq!(
        created["telemetry_schema"],
        json!({"temperature_c": {"type": "number"}})
    );
    assert_eq!(
        created["metric_mapping"],
        json!({"temperature_c": "temperature"})
    );
    assert_eq!(
        created["reporting_settings"],
        json!({"interval_seconds": 60})
    );
    let id = created["id"].as_str().unwrap().to_owned();

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/profiles/device-profiles")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);

    let updated = json_request(
        &router,
        "PUT",
        &format!("/api/management/profiles/device-profiles/{id}"),
        &cookie,
        json!({
            "name": "Environmental Sensor v2",
            "telemetry_schema": {"humidity_pct": {"type": "number"}},
            "metric_mapping": {"humidity_pct": "humidity"},
            "reporting_settings": {"interval_seconds": 300},
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated["name"], "Environmental Sensor v2");

    json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &cookie,
        json!({
            "name": "Environmental Sensor v2",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::CONFLICT,
    )
    .await;
    json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &cookie,
        json!({
            "name": "Invalid",
            "telemetry_schema": [],
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "PUT",
        "/api/management/profiles/device-profiles/not-a-uuid",
        &cookie,
        json!({
            "name": "Ignored",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "PUT",
        "/api/management/profiles/device-profiles/00000000-0000-0000-0000-000000000000",
        &cookie,
        json!({
            "name": "Missing",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::NOT_FOUND,
    )
    .await;

    for (profile_id, expected_status) in [
        ("not-a-uuid", StatusCode::BAD_REQUEST),
        (
            "00000000-0000-0000-0000-000000000000",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!(
                        "/api/management/profiles/device-profiles/{profile_id}"
                    ))
                    .header(COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
    }

    let device = json_request(
        &router,
        "POST",
        "/api/management/devices",
        &cookie,
        json!({"display_name": "Profile Reference Device"}),
        StatusCode::CREATED,
    )
    .await;
    let device_id = device["device_id"].as_str().unwrap().to_owned();
    json_request(
        &router,
        "PUT",
        &format!("/api/management/devices/{device_id}"),
        &cookie,
        json!({
            "display_name": "Profile Reference Device",
            "device_profile_id": id,
        }),
        StatusCode::OK,
    )
    .await;

    let referenced = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/profiles/device-profiles/{id}"))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(referenced.status(), StatusCode::CONFLICT);
    let disposable = json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &cookie,
        json!({
            "name": "Disposable",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::CREATED,
    )
    .await;
    let disposable_id = disposable["id"].as_str().unwrap();
    let deleted = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/api/management/profiles/device-profiles/{disposable_id}"
                ))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn management_asset_profile_routes_support_crud_and_typed_errors() {
    let (_directory, _store, router) = management_router().await;
    let cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    let created = json_request(
        &router,
        "POST",
        "/api/management/profiles/asset-profiles",
        &cookie,
        json!({
            "name": "Campus",
            "fields": {"location": {"type": "string"}},
            "dashboard_defaults": {"layout": "summary"},
        }),
        StatusCode::CREATED,
    )
    .await;
    assert!(created["id"].is_string());
    assert_eq!(created["name"], "Campus");
    assert_eq!(created["fields"], json!({"location": {"type": "string"}}));
    assert_eq!(created["dashboard_defaults"], json!({"layout": "summary"}));
    let id = created["id"].as_str().unwrap().to_owned();

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/profiles/asset-profiles")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);

    let updated = json_request(
        &router,
        "PUT",
        &format!("/api/management/profiles/asset-profiles/{id}"),
        &cookie,
        json!({
            "name": "Campus v2",
            "fields": {"floor": {"type": "integer"}},
            "dashboard_defaults": {"layout": "detail"},
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated["name"], "Campus v2");
    assert_eq!(updated["fields"], json!({"floor": {"type": "integer"}}));

    json_request(
        &router,
        "POST",
        "/api/management/profiles/asset-profiles",
        &cookie,
        json!({
            "name": "Campus v2",
            "fields": {},
            "dashboard_defaults": {},
        }),
        StatusCode::CONFLICT,
    )
    .await;
    json_request(
        &router,
        "POST",
        "/api/management/profiles/asset-profiles",
        &cookie,
        json!({
            "name": "Invalid",
            "fields": [],
            "dashboard_defaults": {},
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "PUT",
        "/api/management/profiles/asset-profiles/not-a-uuid",
        &cookie,
        json!({
            "name": "Ignored",
            "fields": {},
            "dashboard_defaults": {},
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "PUT",
        "/api/management/profiles/asset-profiles/00000000-0000-0000-0000-000000000000",
        &cookie,
        json!({
            "name": "Missing",
            "fields": {},
            "dashboard_defaults": {},
        }),
        StatusCode::NOT_FOUND,
    )
    .await;

    for (profile_id, expected_status) in [
        ("not-a-uuid", StatusCode::BAD_REQUEST),
        (
            "00000000-0000-0000-0000-000000000000",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!(
                        "/api/management/profiles/asset-profiles/{profile_id}"
                    ))
                    .header(COOKIE, &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
    }

    json_request(
        &router,
        "POST",
        "/api/management/assets",
        &cookie,
        json!({
            "name": "Referenced profile asset",
            "asset_profile_id": id,
            "parent_asset_id": null,
            "metadata": {},
            "attributes": {},
        }),
        StatusCode::CREATED,
    )
    .await;
    let referenced = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/profiles/asset-profiles/{id}"))
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(referenced.status(), StatusCode::CONFLICT);
    let disposable = json_request(
        &router,
        "POST",
        "/api/management/profiles/asset-profiles",
        &cookie,
        json!({
            "name": "Disposable asset profile",
            "fields": {},
            "dashboard_defaults": {},
        }),
        StatusCode::CREATED,
    )
    .await;
    let disposable_id = disposable["id"].as_str().unwrap();

    let deleted = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/api/management/profiles/asset-profiles/{disposable_id}"
                ))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn management_profile_routes_are_scoped_to_the_tenant_account() {
    let (_directory, store, router) = management_router().await;
    let tenant_a_cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    let (_tenant_b, _) = TenantIdentityRepository::create_tenant_with_account(
        store.as_ref(),
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
    let tenant_b_cookie = tenant_account_cookie(&router, "other", "OtherTenant@2026").await;

    let tenant_a_device_profile = json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &tenant_a_cookie,
        json!({
            "name": "Shared Device Profile",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::CREATED,
    )
    .await;
    let tenant_b_device_profile = json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &tenant_b_cookie,
        json!({
            "name": "Shared Device Profile",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_ne!(tenant_a_device_profile["id"], tenant_b_device_profile["id"]);

    for cookie in [&tenant_a_cookie, &tenant_b_cookie] {
        let profiles = json_request(
            &router,
            "GET",
            "/api/management/profiles/device-profiles",
            cookie,
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(profiles.as_array().unwrap().len(), 1);
        assert_eq!(profiles[0]["name"], "Shared Device Profile");
    }

    let cross_tenant_update = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/api/management/profiles/device-profiles/{}",
                    tenant_a_device_profile["id"].as_str().unwrap()
                ))
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, &tenant_b_cookie)
                .body(Body::from(
                    json!({
                        "name": "Other Tenant Cannot Update This",
                        "telemetry_schema": {},
                        "metric_mapping": {},
                        "reporting_settings": {},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_tenant_update.status(), StatusCode::NOT_FOUND);

    for cookie in [&tenant_a_cookie, &tenant_b_cookie] {
        let profile = json_request(
            &router,
            "POST",
            "/api/management/profiles/asset-profiles",
            cookie,
            json!({
                "name": "Shared Asset Profile",
                "fields": {},
                "dashboard_defaults": {},
            }),
            StatusCode::CREATED,
        )
        .await;
        assert_eq!(profile["name"], "Shared Asset Profile");
    }
}

#[tokio::test]
async fn management_users_and_profiles_report_unavailable_storage() {
    let (_directory, store, router) = management_router().await;
    let cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    store.sqlite_pool().unwrap().close().await;

    let list_users = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/users")
                .header(COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_users.status(), StatusCode::SERVICE_UNAVAILABLE);
    json_request(
        &router,
        "POST",
        "/api/management/profiles/device-profiles",
        &cookie,
        json!({
            "name": "Unavailable",
            "telemetry_schema": {},
            "metric_mapping": {},
            "reporting_settings": {},
        }),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    json_request(
        &router,
        "POST",
        "/api/management/profiles/asset-profiles",
        &cookie,
        json!({
            "name": "Unavailable",
            "fields": {},
            "dashboard_defaults": {},
        }),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
}

#[tokio::test]
async fn management_alerts_are_guarded_and_scoped_to_the_authenticated_tenant() {
    let (_directory, store, router) = management_router().await;
    let tenant_a_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let tenant_a_id = Uuid::parse_str(&tenant_a_id).unwrap();
    seed_alert(&store, tenant_a_id, "Tenant A alert").await;

    let (tenant_b, _) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "other-alerts".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    seed_alert(&store, tenant_b.id, "Tenant B alert").await;

    let tenant_a_cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    let tenant_b_cookie = tenant_account_cookie(&router, "other-alerts", "OtherTenant@2026").await;
    let viewer_cookie = user_account_cookie(&router, "test", "viewer", "NanoView@1234").await;

    let anonymous = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    let viewer = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/alerts")
                .header(COOKIE, &viewer_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(viewer.status(), StatusCode::FORBIDDEN);

    let tenant_a_alerts = json_request(
        &router,
        "GET",
        &format!("/api/management/alerts?tenant_id={}", tenant_b.id),
        &tenant_a_cookie,
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(tenant_a_alerts.as_array().unwrap().len(), 1);
    assert_eq!(tenant_a_alerts[0]["rule_name"], "Tenant A alert");
    assert!(tenant_a_alerts[0].get("tenant_id").is_none());

    let tenant_b_alerts = json_request(
        &router,
        "GET",
        "/api/management/alerts",
        &tenant_b_cookie,
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(tenant_b_alerts.as_array().unwrap().len(), 1);
    assert_eq!(tenant_b_alerts[0]["rule_name"], "Tenant B alert");
}

fn assert_read_only_tenant_alert_page_allows_only_logout_form(page: &str) {
    assert_eq!(page.matches("<form").count(), 1);
    assert!(page.contains("<form class=\"logout-form\" action=\"/logout\" method=\"post\">"));
    assert!(!page.contains("action=\"/system"));
    assert!(!page.contains("action=\"/tenant"));
    assert!(!page.contains("action=\"/commands"));
    assert!(!page.contains("href=\"/system"));
    assert!(!page.contains("href=\"/commands"));
}

#[tokio::test]
async fn tenant_alert_page_is_guarded_and_renders_only_the_session_tenant() {
    let (_directory, store, router) = management_router().await;
    let tenant_a_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let tenant_a_id = Uuid::parse_str(&tenant_a_id).unwrap();
    seed_alert(&store, tenant_a_id, "Tenant A page alert").await;

    let (tenant_b, _) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "other-alert-page".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    seed_alert(&store, tenant_b.id, "Tenant B page alert").await;

    let tenant_a_cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    let viewer_cookie = user_account_cookie(&router, "test", "viewer", "NanoView@1234").await;

    let anonymous = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tenant/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    let viewer = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tenant/alerts")
                .header(COOKIE, &viewer_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(viewer.status(), StatusCode::FORBIDDEN);

    let page = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/tenant/alerts?tenant_id={}", tenant_b.id))
                .header(COOKIE, &tenant_a_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        axum::body::to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("data-alert-rules-panel"));
    assert!(page.contains("data-alert-incidents-panel"));
    assert!(page.contains("data-alert-rule-form"));
    assert!(!page.contains("Tenant A page alert"));
    assert!(!page.contains("Tenant B page alert"));
    assert!(page.contains("href=\"/tenant/alerts\" aria-current=\"page\""));
    assert!(!page.contains("action=\"/system"));
    assert!(!page.contains("href=\"/system"));
}

#[tokio::test]
async fn tenant_audit_routes_require_a_tenant_account_scope_events_and_follow_keyset_cursors() {
    let (_directory, store, router) = management_router().await;
    bootstrap_system(&store, "system", "SystemAccount@2026")
        .await
        .unwrap();
    let tenant_a_id: String = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = 'test'")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let tenant_a_id = Uuid::parse_str(&tenant_a_id).unwrap();

    seed_audit_event(
        &store,
        tenant_a_id,
        "2026-09-18T10:00:00Z",
        "permission.granted",
        "resource_permission",
        "permission-a",
        json!({ "permission": "viewer" }),
    )
    .await;
    let (_, tenant_a_actor_id) = seed_audit_event(
        &store,
        tenant_a_id,
        "2026-09-18T11:00:00Z",
        "gateway.assigned",
        "device",
        "tenant-a-device<unsafe>",
        json!({ "change": "<unsafe>" }),
    )
    .await;

    let (tenant_b, _) = TenantIdentityRepository::create_tenant_with_account(
        &store,
        NewTenant {
            slug: "other-audit".to_owned(),
            metadata: json!({}),
        },
        NewTenantAccount {
            password_hash: hash_password("OtherTenant@2026").unwrap(),
        },
    )
    .await
    .unwrap();
    seed_audit_event(
        &store,
        tenant_b.id,
        "2026-09-18T12:00:00Z",
        "device_relation.created",
        "device_relation",
        "Tenant B audit",
        json!({ "relation_type": "feeds" }),
    )
    .await;

    let tenant_a_cookie = tenant_account_cookie(&router, "test", "TenantAccount@2026").await;
    let tenant_b_cookie = tenant_account_cookie(&router, "other-audit", "OtherTenant@2026").await;
    let system_cookie = system_account_cookie(&router).await;
    let user_cookie = user_account_cookie(&router, "test", "viewer", "NanoView@1234").await;

    for (cookie, expected_status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(system_cookie.as_str()), StatusCode::FORBIDDEN),
        (Some(user_cookie.as_str()), StatusCode::FORBIDDEN),
    ] {
        for path in ["/api/management/audit", "/tenant/audit"] {
            let mut request = Request::builder().method("GET").uri(path);
            if let Some(cookie) = cookie {
                request = request.header(COOKIE, cookie);
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status, "{path}");
        }
    }

    let over_limit = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/management/audit?limit=101")
                .header(COOKIE, &tenant_a_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(over_limit.status(), StatusCode::BAD_REQUEST);

    for path in [
        format!("/api/management/audit?tenant_id={}", tenant_b.id),
        format!("/tenant/audit?tenant_id={}", tenant_b.id),
    ] {
        let arbitrary_tenant = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header(COOKIE, &tenant_a_cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(arbitrary_tenant.status(), StatusCode::BAD_REQUEST);
    }

    let first_page = json_request(
        &router,
        "GET",
        "/api/management/audit?limit=1",
        &tenant_a_cookie,
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(first_page["items"][0]["action"], "gateway.assigned");
    assert_eq!(first_page["items"][0]["actor_kind"], "tenant_account");
    assert_eq!(
        first_page["items"][0]["actor_id"],
        tenant_a_actor_id.to_string()
    );
    assert_eq!(first_page["items"][0]["target_type"], "device");
    assert_eq!(
        first_page["items"][0]["target_id"],
        "tenant-a-device<unsafe>"
    );
    assert_eq!(first_page["items"][0]["changes"]["change"], "<unsafe>");
    assert!(first_page["items"][0].get("tenant_id").is_none());
    assert_eq!(first_page["has_more"], true);
    let next_cursor = first_page["next_cursor"].as_str().unwrap();

    let cross_tenant_cursor = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/management/audit?limit=1&after={next_cursor}"))
                .header(COOKIE, &tenant_b_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_tenant_cursor.status(), StatusCode::BAD_REQUEST);

    let second_page = json_request(
        &router,
        "GET",
        &format!("/api/management/audit?limit=1&after={next_cursor}"),
        &tenant_a_cookie,
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(second_page["items"][0]["action"], "permission.granted");
    assert_eq!(second_page["has_more"], false);
    assert!(second_page["next_cursor"].is_null());

    let tenant_b_page = json_request(
        &router,
        "GET",
        "/api/management/audit",
        &tenant_b_cookie,
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(tenant_b_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(tenant_b_page["items"][0]["target_id"], "Tenant B audit");

    let page = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tenant/audit")
                .header(COOKIE, &tenant_a_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        axum::body::to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("2026-09-18T11:00:00+00:00"));
    assert!(page.contains("Tenant account"));
    assert!(page.contains(&tenant_a_actor_id.to_string()));
    assert!(page.contains("gateway.assigned"));
    assert!(page.contains("tenant-a-device&#60;unsafe&#62;"));
    assert!(page.contains("&#60;unsafe&#62;"));
    assert!(!page.contains("Tenant B audit"));
    assert!(page.contains("href=\"/tenant/audit\" aria-current=\"page\""));
    assert_eq!(page.matches("<form").count(), 1);
    assert!(page.contains("<form class=\"logout-form\" action=\"/logout\" method=\"post\">"));
}

fn test_token_vault() -> TokenVault {
    TokenVault::from_key_material("management-users-profiles-test-vault-key-material")
}
