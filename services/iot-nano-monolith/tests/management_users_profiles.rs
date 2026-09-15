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
use iot_api::{TokenVault, bootstrap_users_sqlite};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::ManagementSessionRouter;
use iot_storage::PlatformStore;
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
    bootstrap_users_sqlite(store.sqlite_pool().unwrap())
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
    }

    let admin_cookie = login_cookie(&router, "admin", "NanoAdmin@1234").await;
    let missing_content_type = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/users")
                .header(COOKIE, &admin_cookie)
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
                .header(COOKIE, &admin_cookie)
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
                .header(COOKIE, &admin_cookie)
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
        &admin_cookie,
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
        &admin_cookie,
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
        "PUT",
        "/api/management/users/missing",
        &admin_cookie,
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
        &admin_cookie,
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
}

#[tokio::test]
async fn management_device_profile_routes_support_crud_and_typed_errors() {
    let (_directory, _store, router) = management_router().await;
    let cookie = login_cookie(&router, "admin", "NanoAdmin@1234").await;
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
    let cookie = login_cookie(&router, "admin", "NanoAdmin@1234").await;
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

    let deleted = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/management/profiles/asset-profiles/{id}"))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn management_users_and_profiles_report_unavailable_storage() {
    let (_directory, store, router) = management_router().await;
    let cookie = login_cookie(&router, "admin", "NanoAdmin@1234").await;
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
