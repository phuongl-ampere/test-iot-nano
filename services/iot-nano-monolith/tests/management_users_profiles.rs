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
use iot_api::{TokenVault, hash_password, seed_tenant_test_users_sqlite};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::ManagementSessionRouter;
use iot_storage::{NewTenant, NewTenantAccount, PlatformStore, TenantIdentityRepository};
use serde_json::{Value, json};
use tower::ServiceExt;

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
    for app_id in ["fleet", "powermonitor"] {
        sqlx::query(
            "INSERT INTO applications (
                app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
             ) VALUES (?, ?, 'frontend', ?, ?, '[]', 1)",
        )
        .bind(app_id)
        .bind(tenant.id.to_string())
        .bind(format!("https://example.test/{app_id}"))
        .bind(format!("management-{app_id}-client"))
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    }
    seed_tenant_test_users_sqlite(store.sqlite_pool().unwrap(), tenant.id)
        .await
        .unwrap();
    let management = ManagementSessionRouter::new(Arc::clone(&store), test_token_vault());
    let router = management
        .router
        .layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            0,
        )))));
    (directory, store, router)
}

async fn login_cookie(router: &axum::Router, username: &str, password: &str) -> String {
    let response = router
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
    let viewer_cookie = login_cookie(&router, "viewer", "NanoView@1234").await;
    let admin_cookie = login_cookie(&router, "admin", "NanoAdmin@1234").await;
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
                    r#"{"username":"content-type","password":"ContentType@123","default_app":"/apps/fleet","granted_apps":["fleet"]}"#,
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
    assert_eq!(listed[0]["default_app"], "/apps/powermonitor");
    assert!(listed[0]["granted_apps"].is_array());

    let created = json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "alice",
            "password": "AlicePassword@123",
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet", "powermonitor"],
        }),
        StatusCode::CREATED,
    )
    .await;
    assert!(created["id"].is_string());
    assert_eq!(created["username"], "alice");
    assert_eq!(created["role"], "viewer");
    assert_eq!(created["account_class"], "user");
    assert_eq!(created["default_app"], "/apps/fleet");
    assert_eq!(created["granted_apps"], json!(["fleet", "powermonitor"]));

    json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "alice",
            "password": "AlicePassword@123",
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet"],
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
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet"],
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "invalid-default-app",
            "password": "InvalidDefaultApp@123",
            "default_app": "/apps/not valid",
            "granted_apps": ["fleet"],
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    json_request(
        &router,
        "POST",
        "/api/management/users",
        &tenant_cookie,
        json!({
            "username": "invalid-grants",
            "password": "InvalidGrants@123",
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet", "fleet"],
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
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet"],
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
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet"],
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
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet"],
            "role": "operator",
        }),
        StatusCode::BAD_REQUEST,
    )
    .await;

    let alice_first_session = login_cookie(&router, "alice", "AlicePassword@123").await;
    let alice_second_session = login_cookie(&router, "alice", "AlicePassword@123").await;
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
            "default_app": "/apps/fleet",
            "granted_apps": ["fleet"],
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

fn test_token_vault() -> TokenVault {
    TokenVault::from_key_material("management-users-profiles-test-vault-key-material")
}
