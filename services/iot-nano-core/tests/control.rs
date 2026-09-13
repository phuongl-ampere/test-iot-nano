use std::env;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{
    CoreControlState, CoreSqliteStore, connect_core_database, core_control_router, migrate,
};
use serde_json::json;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

const CORE_SECRET: &str = "core-control-secret-must-have-at-least-32";

async fn store() -> (tempfile::TempDir, CoreSqliteStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("core.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn timescale_pool() -> PgPool {
    let database_url = env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must point to the local TimescaleDB test database");
    let pool = connect_core_database(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano_core CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn authenticated_command_control_creates_and_reads_a_sqlite_command() {
    let (_directory, store) = store().await;
    let app = core_control_router(CoreControlState::sqlite(store.clone(), CORE_SECRET).unwrap());
    let id = Uuid::now_v7();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/commands")
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "id": id,
                "device_id": "core-control-device",
                "method": "setRelay",
                "params": {"enabled": true},
                "mode": "two_way",
                "issued_at": Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap(),
                "expires_at": Utc.with_ymd_and_hms(2026, 9, 10, 8, 5, 0).unwrap()
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let request = Request::builder()
        .method("GET")
        .uri(format!("/internal/commands/{id}"))
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["id"], id.to_string());
    assert_eq!(body["device_id"], "core-control-device");
    assert_eq!(body["state"], "queued");
    assert_eq!(body["mode"], "two_way");
}

#[tokio::test]
async fn authenticated_command_control_records_a_two_way_response_after_publish() {
    let (_directory, store) = store().await;
    let app = core_control_router(CoreControlState::sqlite(store.clone(), CORE_SECRET).unwrap());
    let id = Uuid::now_v7();
    let now = Utc::now();
    let expires_at = now + chrono::Duration::minutes(5);
    let create = Request::builder()
        .method("POST")
        .uri("/internal/commands")
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "id": id,
                "device_id": "response-device",
                "method": "setRelay",
                "params": {"enabled": true},
                "mode": "two_way",
                "issued_at": now,
                "expires_at": expires_at
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(create).await.unwrap().status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        store
            .claim_commands(now, now + chrono::Duration::seconds(30), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .mark_command_published(&id.to_string(), now)
            .await
            .unwrap()
            .is_some()
    );

    let response = Request::builder()
        .method("POST")
        .uri("/internal/commands/response")
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "command_id": id,
                "device_id": "response-device",
                "response": {"ok": true},
                "responded_at": now + chrono::Duration::seconds(30)
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(response).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    let request = Request::builder()
        .method("GET")
        .uri(format!("/internal/commands/{id}"))
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["state"], "responded");
    assert_eq!(body["response"], json!({"ok": true}));
}

#[tokio::test]
async fn authenticated_telemetry_control_reads_raw_sqlite_telemetry() {
    let (_directory, store) = store().await;
    let at = Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap();
    store
        .write_telemetry(
            &TelemetryEvent {
                schema_version: 1,
                device_id: "telemetry-device".to_owned(),
                boot_id: Uuid::now_v7(),
                sequence: 1,
                event_at: at,
                measurements: serde_json::Map::from_iter([
                    ("temperature_c".to_owned(), json!(26.4)),
                    ("humidity_pct".to_owned(), json!(51.0)),
                ]),
                gateway_device_id: None,
            },
            at,
            "iot/v1/devices/telemetry-device/telemetry",
        )
        .await
        .unwrap();
    let app = core_control_router(CoreControlState::sqlite(store, CORE_SECRET).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri("/internal/telemetry/devices/telemetry-device?from=2026-09-10T07%3A00%3A00Z&to=2026-09-10T09%3A00%3A00Z&bucket=raw")
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.as_array().unwrap().len(), 1);
    assert_eq!(body[0]["temperature_c"], 26.4);
    assert_eq!(body[0]["humidity_pct"], 51.0);
    assert_eq!(body[0]["event_count"], 1);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL pointing to a disposable TimescaleDB database"]
async fn timescale_telemetry_control_normalizes_malformed_raw_metrics_to_none() {
    let pool = timescale_pool().await;
    let at = Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5, $6)",
    )
    .bind(at)
    .bind("telemetry-device")
    .bind(Uuid::now_v7())
    .bind(1_i64)
    .bind(sqlx::types::Json(json!({
        "temperature_c": "not-a-number",
        "humidity_pct": "not-a-number"
    })))
    .bind("iot/v1/devices/telemetry-device/telemetry")
    .execute(&pool)
    .await
    .unwrap();

    let app = core_control_router(CoreControlState::timescale(pool, CORE_SECRET).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri("/internal/telemetry/devices/telemetry-device?from=2026-09-10T07%3A00%3A00Z&to=2026-09-10T09%3A00%3A00Z&bucket=raw")
        .header("x-iot-nano-api-core-secret", CORE_SECRET)
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.as_array().unwrap().len(), 1);
    assert_eq!(body[0]["temperature_c"], serde_json::Value::Null);
    assert_eq!(body[0]["humidity_pct"], serde_json::Value::Null);
    assert_eq!(body[0]["event_count"], 1);
}

#[tokio::test]
async fn legacy_core_header_is_rejected() {
    let (_directory, store) = store().await;
    let app = core_control_router(CoreControlState::sqlite(store, CORE_SECRET).unwrap());
    let request = Request::builder()
        .method("GET")
        .uri(format!("/internal/commands/{}", Uuid::now_v7()))
        .header("x-iot-nano-core-secret", CORE_SECRET)
        .body(Body::empty())
        .unwrap();

    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}
