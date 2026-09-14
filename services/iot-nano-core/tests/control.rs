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
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use tower::ServiceExt;
use uuid::Uuid;

const CORE_SECRET: &str = "core-control-secret-must-have-at-least-32";
const TIMESCALE_TEST_URL: &str = "postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform";

struct TimescaleTestLock {
    _connection: PoolConnection<Postgres>,
}

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

async fn timescale_pool() -> (TimescaleTestLock, PgPool) {
    let database_url = env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must point to the local TimescaleDB test database");
    assert_eq!(
        database_url, TIMESCALE_TEST_URL,
        "refusing to reset a TimescaleDB URL other than the disposable test database"
    );
    let pool = connect_core_database(&database_url).await.unwrap();
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:core-control-test-setup'))")
        .execute(&mut *connection)
        .await
        .unwrap();

    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:core-control-test'))")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano_core CASCADE")
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    migrate(&pool).await.unwrap();
    (
        TimescaleTestLock {
            _connection: connection,
        },
        pool,
    )
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
async fn authenticated_command_control_replays_matching_sqlite_commands_and_conflicts_on_changes() {
    let (_directory, store) = store().await;
    let app = core_control_router(CoreControlState::sqlite(store, CORE_SECRET).unwrap());
    let id = Uuid::now_v7();
    let first_issued_at = Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap();
    let first_expires_at = Utc.with_ymd_and_hms(2026, 9, 10, 8, 5, 0).unwrap();
    let create = |issued_at, expires_at, params| {
        Request::builder()
            .method("POST")
            .uri("/internal/commands")
            .header("x-iot-nano-api-core-secret", CORE_SECRET)
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "id": id,
                    "device_id": "replay-device",
                    "method": "setRelay",
                    "params": params,
                    "mode": "two_way",
                    "issued_at": issued_at,
                    "expires_at": expires_at
                })
                .to_string(),
            ))
            .unwrap()
    };

    let first = app
        .clone()
        .oneshot(create(
            first_issued_at,
            first_expires_at,
            json!({"enabled": true, "level": 1}),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);

    let replay = app
        .clone()
        .oneshot(create(
            first_issued_at + chrono::Duration::seconds(1),
            first_expires_at + chrono::Duration::seconds(1),
            json!({"level": 1, "enabled": true}),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    let replay_body = to_bytes(replay.into_body(), 16 * 1024).await.unwrap();
    let replay_body: serde_json::Value = serde_json::from_slice(&replay_body).unwrap();
    assert_eq!(replay_body["id"], id.to_string());
    assert_eq!(replay_body["expires_at"], "2026-09-10T08:05:00Z");

    let conflict = app
        .oneshot(create(
            first_issued_at + chrono::Duration::seconds(2),
            first_expires_at + chrono::Duration::seconds(2),
            json!({"enabled": false, "level": 1}),
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
}

#[tokio::test]
#[ignore = "requires the exact disposable IOT_NANO_TIMESCALE_TEST_URL"]
async fn authenticated_command_control_replays_matching_timescale_commands_and_conflicts_on_changes()
 {
    let (_lock, pool) = timescale_pool().await;
    let app = core_control_router(CoreControlState::timescale(pool, CORE_SECRET).unwrap());
    let id = Uuid::now_v7();
    let first_issued_at = Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap();
    let first_expires_at = Utc.with_ymd_and_hms(2026, 9, 10, 8, 5, 0).unwrap();
    let create = |issued_at, expires_at, params| {
        Request::builder()
            .method("POST")
            .uri("/internal/commands")
            .header("x-iot-nano-api-core-secret", CORE_SECRET)
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "id": id,
                    "device_id": "replay-timescale-device",
                    "method": "setRelay",
                    "params": params,
                    "mode": "two_way",
                    "issued_at": issued_at,
                    "expires_at": expires_at
                })
                .to_string(),
            ))
            .unwrap()
    };

    let first = app
        .clone()
        .oneshot(create(
            first_issued_at,
            first_expires_at,
            json!({"enabled": true, "level": 1}),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);

    let replay = app
        .clone()
        .oneshot(create(
            first_issued_at + chrono::Duration::seconds(1),
            first_expires_at + chrono::Duration::seconds(1),
            json!({"level": 1, "enabled": true}),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    let replay_body = to_bytes(replay.into_body(), 16 * 1024).await.unwrap();
    let replay_body: serde_json::Value = serde_json::from_slice(&replay_body).unwrap();
    assert_eq!(replay_body["id"], id.to_string());
    assert_eq!(replay_body["expires_at"], "2026-09-10T08:05:00Z");

    let conflict = app
        .oneshot(create(
            first_issued_at + chrono::Duration::seconds(2),
            first_expires_at + chrono::Duration::seconds(2),
            json!({"enabled": false, "level": 1}),
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
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
// docker compose -f infra/compose.yaml up -d timescaledb
// docker compose -f infra/compose.yaml exec -T timescaledb createdb -U iot iot_nano_test_platform # first run only
// IOT_NANO_TIMESCALE_TEST_URL=postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform cargo test -p iot-nano-core --test control timescale_telemetry_control_normalizes_malformed_raw_metrics_to_none -- --ignored --exact
// docker compose -f infra/compose.yaml stop timescaledb
#[ignore = "requires the exact disposable IOT_NANO_TIMESCALE_TEST_URL"]
async fn timescale_telemetry_control_normalizes_malformed_raw_metrics_to_none() {
    let (_lock, pool) = timescale_pool().await;
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
        "humidity_pct": 51.0
    })))
    .bind("iot/v1/devices/telemetry-device/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5, $6)",
    )
    .bind(at + chrono::Duration::seconds(3))
    .bind("telemetry-device")
    .bind(Uuid::now_v7())
    .bind(4_i64)
    .bind(sqlx::types::Json(json!({
        "temperature_c": null,
        "humidity_pct": true
    })))
    .bind("iot/v1/devices/telemetry-device/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5, $6)",
    )
    .bind(at + chrono::Duration::seconds(4))
    .bind("telemetry-device")
    .bind(Uuid::now_v7())
    .bind(5_i64)
    .bind(sqlx::types::Json(json!({
        "temperature_c": {},
        "humidity_pct": []
    })))
    .bind("iot/v1/devices/telemetry-device/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5::jsonb, $6)",
    )
    .bind(at + chrono::Duration::seconds(5))
    .bind("telemetry-device")
    .bind(Uuid::now_v7())
    .bind(6_i64)
    .bind(r#"{"temperature_c": 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368, "humidity_pct": -179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368}"#)
    .bind("iot/v1/devices/telemetry-device/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5::jsonb, $6)",
    )
    .bind(at + chrono::Duration::seconds(2))
    .bind("telemetry-device")
    .bind(Uuid::now_v7())
    .bind(3_i64)
    .bind(r#"{"temperature_c": 9e999, "humidity_pct": -9e999}"#)
    .bind("iot/v1/devices/telemetry-device/telemetry")
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5, $6)",
    )
    .bind(at + chrono::Duration::seconds(1))
    .bind("telemetry-device")
    .bind(Uuid::now_v7())
    .bind(2_i64)
    .bind(sqlx::types::Json(json!({
        "temperature_c": 26.4
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
    assert_eq!(body.as_array().unwrap().len(), 6);
    assert_eq!(body[0]["temperature_c"], serde_json::Value::Null);
    assert_eq!(body[0]["humidity_pct"], 51.0);
    assert_eq!(body[0]["event_count"], 1);
    assert_eq!(body[1]["temperature_c"], 26.4);
    assert_eq!(body[1]["humidity_pct"], serde_json::Value::Null);
    assert_eq!(body[1]["event_count"], 1);
    assert_eq!(body[2]["temperature_c"], serde_json::Value::Null);
    assert_eq!(body[2]["humidity_pct"], serde_json::Value::Null);
    assert_eq!(body[2]["event_count"], 1);
    assert_eq!(body[3]["temperature_c"], serde_json::Value::Null);
    assert_eq!(body[3]["humidity_pct"], serde_json::Value::Null);
    assert_eq!(body[3]["event_count"], 1);
    assert_eq!(body[4]["temperature_c"], serde_json::Value::Null);
    assert_eq!(body[4]["humidity_pct"], serde_json::Value::Null);
    assert_eq!(body[4]["event_count"], 1);
    assert_eq!(body[5]["temperature_c"].as_f64(), Some(f64::MAX));
    assert_eq!(body[5]["humidity_pct"].as_f64(), Some(-f64::MAX));
    assert_eq!(body[5]["event_count"], 1);
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
