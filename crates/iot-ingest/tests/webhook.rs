use std::{
    env,
    fs::{File, OpenOptions},
    sync::{Arc, LazyLock, Mutex},
};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use fs2::FileExt;
use iot_core::{
    DEVICE_TELEMETRY_TOPIC, DatabaseStorage, GATEWAY_DISCONNECT_TOPIC, GATEWAY_TELEMETRY_TOPIC,
    StorageConfiguration, generate_device_token, hash_device_token,
};
use iot_ingest::{
    IngestMetrics, SqliteTokenWebhookIngress, TelemetryWriter, TokenWebhookIngress, migrate,
    sqlite_webhook_router, sqlite_webhook_router_with_transport_secret, webhook_router,
};
use iot_storage::SqliteStore;
use iot_stream::{GroupStart, LocalStream, StreamConfig};
use serde_json::json;
use sqlx::{PgPool, Row, SqlitePool, query, query_scalar};
use tower::ServiceExt;
use uuid::Uuid;

const WEBHOOK_SECRET: &str = "test-nanomq-webhook-secret-must-have-32-bytes";
const TRANSPORT_WEBHOOK_SECRET: &str = "test-transport-webhook-secret-must-have-32-bytes";
static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn prepared_pool() -> PgPool {
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the local TimescaleDB test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    query("TRUNCATE command_outbox, device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices")
        .execute(&pool)
        .await
        .unwrap();
    pool
}

fn lock_database_file() -> File {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(env::temp_dir().join("rush-iot-nano-timescaledb-tests.lock"))
        .unwrap();
    file.lock_exclusive().unwrap();
    file
}

async fn active_device_token(pool: &PgPool) -> String {
    let token = generate_device_token();
    query("INSERT INTO devices (device_id) VALUES ('esp-000123')")
        .execute(pool)
        .await
        .unwrap();
    query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES ($1, 'esp-000123', $2, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(&token[..16])
    .bind(hash_device_token(&token).unwrap())
    .execute(pool)
    .await
    .unwrap();
    token
}

async fn active_gateway_token(pool: &PgPool) -> String {
    let token = generate_device_token();
    query(
        "INSERT INTO devices (device_id, is_gateway)
         VALUES ('gateway-001', TRUE), ('child-001', FALSE)",
    )
    .execute(pool)
    .await
    .unwrap();
    query(
        "UPDATE devices
         SET gateway_device_id = 'gateway-001'
         WHERE device_id = 'child-001'",
    )
    .execute(pool)
    .await
    .unwrap();
    query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES ($1, 'gateway-001', $2, $3)",
    )
    .bind(Uuid::new_v4())
    .bind(&token[..16])
    .bind(hash_device_token(&token).unwrap())
    .execute(pool)
    .await
    .unwrap();
    token
}

async fn sqlite_store(directory: &tempfile::TempDir) -> SqliteStore {
    SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap()
}

async fn active_sqlite_device_token(pool: &SqlitePool) -> String {
    let token = generate_device_token();
    query("INSERT INTO devices (device_id) VALUES ('esp-000123')")
        .execute(pool)
        .await
        .unwrap();
    query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'esp-000123', ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(&token[..16])
    .bind(hash_device_token(&token).unwrap())
    .execute(pool)
    .await
    .unwrap();
    token
}

async fn active_sqlite_gateway_token(pool: &SqlitePool) -> String {
    let token = generate_device_token();
    query(
        "INSERT INTO devices (device_id, is_gateway)
         VALUES ('gateway-001', 1), ('child-001', 0), ('child-foreign', 0)",
    )
    .execute(pool)
    .await
    .unwrap();
    query(
        "UPDATE devices
         SET gateway_device_id = 'gateway-001'
         WHERE device_id = 'child-001'",
    )
    .execute(pool)
    .await
    .unwrap();
    query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'gateway-001', ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(&token[..16])
    .bind(hash_device_token(&token).unwrap())
    .execute(pool)
    .await
    .unwrap();
    token
}

fn webhook_body(token: &str, qos: u8) -> String {
    let payload = json!({
        "schema_version": 1,
        "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
        "sequence": 1842,
        "event_at": "2026-09-04T10:12:00Z",
        "measurements": {"temperature_c": 26.4}
    });
    json!({
        "action": "message_publish",
        "from_username": token,
        "topic": DEVICE_TELEMETRY_TOPIC,
        "qos": qos,
        "ts": 1788516721000_i64,
        "payload": payload.to_string()
    })
    .to_string()
}

fn webhook_request(token: &str, secret: &str) -> Request<Body> {
    webhook_request_with_qos(token, secret, 1)
}

fn webhook_request_with_qos(token: &str, secret: &str, qos: u8) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/internal/nanomq/telemetry")
        .header("content-type", "application/json")
        .header("x-iot-nanomq-webhook", secret)
        .body(Body::from(webhook_body(token, qos)))
        .unwrap()
}

fn transport_webhook_request(token: &str, secret: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/internal/mqtt-transport/telemetry")
        .header("content-type", "application/json")
        .header("x-iot-mqtt-transport-webhook", secret)
        .body(Body::from(webhook_body(token, 1)))
        .unwrap()
}

fn gateway_webhook_request(token: &str, topic: &str, payload: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/internal/nanomq/telemetry")
        .header("content-type", "application/json")
        .header("x-iot-nanomq-webhook", WEBHOOK_SECRET)
        .body(Body::from(
            json!({
                "action": "message_publish",
                "from_username": token,
                "topic": topic,
                "qos": 1,
                "ts": 1788516721000_i64,
                "payload": payload.to_string()
            })
            .to_string(),
        ))
        .unwrap()
}

fn gateway_stream(tempdir: &tempfile::TempDir) -> LocalStream {
    let mut config = StreamConfig::for_test(8);
    config.max_record_bytes = 2 * 1024;
    LocalStream::open(tempdir.path().join("stream"), config).unwrap()
}

async fn wait_for_stream_record(stream: &LocalStream) {
    for _ in 0..50 {
        if stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .any(|partition| partition.next_offset > 0)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("webhook worker did not append a stream record");
}

#[tokio::test]
async fn active_token_webhook_appends_then_writes_timescaledb() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let token = active_device_token(&pool).await;
    let tempdir = tempfile::tempdir().unwrap();
    let stream =
        LocalStream::open(tempdir.path().join("stream"), StreamConfig::for_test(8)).unwrap();
    let app = webhook_router(
        TokenWebhookIngress::new(
            pool.clone(),
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .oneshot(webhook_request(&token, WEBHOOK_SECRET))
        .await
        .unwrap();
    wait_for_stream_record(&stream).await;
    let mut consumer = stream
        .join_group(
            "timescaledb-writer",
            "webhook-writer",
            GroupStart::Earliest,
            Utc::now(),
        )
        .unwrap();
    let result = TelemetryWriter::new(pool.clone(), 10)
        .flush_once(&mut consumer, Utc::now())
        .await
        .unwrap();
    let row = query("SELECT device_id, topic FROM telemetry")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(result.inserted, 1);
    assert_eq!(row.get::<String, _>("device_id"), "esp-000123");
    assert_eq!(row.get::<String, _>("topic"), DEVICE_TELEMETRY_TOPIC);
}

#[tokio::test]
async fn sqlite_transport_webhook_uses_its_dedicated_secret() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let token = active_sqlite_device_token(store.pool()).await;
    let stream =
        LocalStream::open(tempdir.path().join("stream"), StreamConfig::for_test(8)).unwrap();
    let app = sqlite_webhook_router_with_transport_secret(
        SqliteTokenWebhookIngress::new(
            store,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
        TRANSPORT_WEBHOOK_SECRET,
    );

    let response = app
        .oneshot(transport_webhook_request(&token, TRANSPORT_WEBHOOK_SECRET))
        .await
        .unwrap();
    wait_for_stream_record(&stream).await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn sqlite_token_webhook_appends_to_stream_and_marks_token_used() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let pool = store.pool().clone();
    let token = active_sqlite_device_token(&pool).await;
    let stream =
        LocalStream::open(tempdir.path().join("stream"), StreamConfig::for_test(8)).unwrap();
    let app = sqlite_webhook_router(
        SqliteTokenWebhookIngress::new(
            store,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .oneshot(webhook_request(&token, WEBHOOK_SECRET))
        .await
        .unwrap();
    wait_for_stream_record(&stream).await;
    let mut last_used_at: Option<String> = None;
    for _ in 0..50 {
        last_used_at =
            query_scalar("SELECT last_used_at FROM device_tokens WHERE device_id = 'esp-000123'")
                .fetch_one(&pool)
                .await
                .unwrap();
        if last_used_at.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_eq!(response.status(), StatusCode::OK);
    assert!(last_used_at.is_some());
}

#[tokio::test]
async fn sqlite_gateway_webhook_enforces_ownership_and_updates_health() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let pool = store.pool().clone();
    let token = active_sqlite_gateway_token(&pool).await;
    let stream = gateway_stream(&tempdir);
    let app = sqlite_webhook_router(
        SqliteTokenWebhookIngress::new(
            store,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .clone()
        .oneshot(gateway_webhook_request(
            &token,
            GATEWAY_TELEMETRY_TOPIC,
            json!({
                "schema_version": 1,
                "kind": "child_telemetry",
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1842,
                "event_at": "2026-09-04T10:12:00Z",
                "child_device_id": "child-001",
                "measurements": {"temperature_c": 26.4}
            }),
        ))
        .await
        .unwrap();
    wait_for_stream_record(&stream).await;

    for _ in 0..50 {
        let health = query(
            "SELECT
                (SELECT last_seen_at FROM devices WHERE device_id = 'gateway-001')
                    AS gateway_last_seen_at,
                gateway_last_read_at,
                gateway_read_quality
             FROM devices
             WHERE device_id = 'child-001'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if health
            .try_get::<Option<String>, _>("gateway_last_seen_at")
            .unwrap()
            .is_some()
            && health
                .try_get::<Option<String>, _>("gateway_last_read_at")
                .unwrap()
                .as_deref()
                == Some("2026-09-04T10:12:00+00:00")
            && health
                .try_get::<Option<String>, _>("gateway_read_quality")
                .unwrap()
                .as_deref()
                == Some("good")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let health = query(
        "SELECT
            (SELECT last_seen_at FROM devices WHERE device_id = 'gateway-001')
                AS gateway_last_seen_at,
            gateway_last_read_at,
            gateway_read_quality
         FROM devices
         WHERE device_id = 'child-001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let unowned = app
        .oneshot(gateway_webhook_request(
            &token,
            GATEWAY_TELEMETRY_TOPIC,
            json!({
                "schema_version": 1,
                "kind": "child_telemetry",
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1843,
                "event_at": "2026-09-04T10:12:00Z",
                "child_device_id": "child-foreign",
                "measurements": {"temperature_c": 26.4}
            }),
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(unowned.status(), StatusCode::OK);
    assert!(
        health
            .try_get::<Option<String>, _>("gateway_last_seen_at")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        health
            .try_get::<Option<String>, _>("gateway_last_read_at")
            .unwrap()
            .as_deref(),
        Some("2026-09-04T10:12:00+00:00")
    );
    assert_eq!(
        health
            .try_get::<Option<String>, _>("gateway_read_quality")
            .unwrap()
            .as_deref(),
        Some("good")
    );
    assert_eq!(
        stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .map(|partition| partition.next_offset)
            .sum::<u64>(),
        1
    );
}

#[tokio::test]
async fn sqlite_gateway_disconnect_marks_child_unavailable() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let pool = store.pool().clone();
    let token = active_sqlite_gateway_token(&pool).await;
    let stream = gateway_stream(&tempdir);
    let app = sqlite_webhook_router(
        SqliteTokenWebhookIngress::new(
            store,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .oneshot(gateway_webhook_request(
            &token,
            GATEWAY_DISCONNECT_TOPIC,
            json!({
                "schema_version": 1,
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1842,
                "event_at": "2026-09-04T10:12:00Z",
                "child_device_id": "child-001"
            }),
        ))
        .await
        .unwrap();

    for _ in 0..50 {
        let row = query(
            "SELECT
                (SELECT last_seen_at FROM devices WHERE device_id = 'gateway-001')
                    AS gateway_last_seen_at,
                gateway_read_quality,
                (SELECT last_used_at FROM device_tokens WHERE device_id = 'gateway-001')
                    AS token_last_used_at
             FROM devices
             WHERE device_id = 'child-001'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if row
            .try_get::<Option<String>, _>("gateway_last_seen_at")
            .unwrap()
            .is_some()
            && row
                .try_get::<Option<String>, _>("gateway_read_quality")
                .unwrap()
                .as_deref()
                == Some("unavailable")
            && row
                .try_get::<Option<String>, _>("token_last_used_at")
                .unwrap()
                .is_some()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let row = query(
        "SELECT
            (SELECT last_seen_at FROM devices WHERE device_id = 'gateway-001')
                AS gateway_last_seen_at,
            gateway_read_quality,
            (SELECT last_used_at FROM device_tokens WHERE device_id = 'gateway-001')
                AS token_last_used_at
         FROM devices
         WHERE device_id = 'child-001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        row.try_get::<Option<String>, _>("gateway_last_seen_at")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        row.try_get::<Option<String>, _>("gateway_read_quality")
            .unwrap()
            .as_deref(),
        Some("unavailable")
    );
    assert!(
        row.try_get::<Option<String>, _>("token_last_used_at")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .map(|partition| partition.next_offset)
            .sum::<u64>(),
        0
    );
}

#[tokio::test]
async fn sqlite_webhook_rejects_invalid_qos_tokens_and_topic_roles() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let pool = store.pool().clone();
    let device_token = active_sqlite_device_token(&pool).await;
    let gateway_token = active_sqlite_gateway_token(&pool).await;
    let stream = gateway_stream(&tempdir);
    let metrics = Arc::new(IngestMetrics::default());
    let app = sqlite_webhook_router(
        SqliteTokenWebhookIngress::new(
            store,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            metrics.clone(),
        )
        .unwrap(),
    );
    let mut invalid_token = device_token.clone();
    let replacement = if invalid_token.ends_with('a') {
        'b'
    } else {
        'a'
    };
    invalid_token.pop();
    invalid_token.push(replacement);

    let invalid_qos = app
        .clone()
        .oneshot(webhook_request_with_qos(&device_token, WEBHOOK_SECRET, 0))
        .await
        .unwrap();
    let invalid_token_response = app
        .clone()
        .oneshot(webhook_request(&invalid_token, WEBHOOK_SECRET))
        .await
        .unwrap();
    let device_on_gateway_topic = app
        .clone()
        .oneshot(gateway_webhook_request(
            &device_token,
            GATEWAY_TELEMETRY_TOPIC,
            json!({
                "schema_version": 1,
                "kind": "heartbeat",
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1842,
                "event_at": "2026-09-04T10:12:00Z"
            }),
        ))
        .await
        .unwrap();
    let gateway_on_device_topic = app
        .oneshot(webhook_request(&gateway_token, WEBHOOK_SECRET))
        .await
        .unwrap();

    for _ in 0..100 {
        if metrics
            .render_prometheus()
            .contains("iot_ingest_rejected_total 4\n")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let token_uses: i64 =
        query_scalar("SELECT COUNT(*) FROM device_tokens WHERE last_used_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(invalid_qos.status(), StatusCode::OK);
    assert_eq!(invalid_token_response.status(), StatusCode::OK);
    assert_eq!(device_on_gateway_topic.status(), StatusCode::OK);
    assert_eq!(gateway_on_device_topic.status(), StatusCode::OK);
    assert!(
        metrics
            .render_prometheus()
            .contains("iot_ingest_rejected_total 4\n")
    );
    assert_eq!(token_uses, 0);
    assert_eq!(
        stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .map(|partition| partition.next_offset)
            .sum::<u64>(),
        0
    );
}

#[tokio::test]
async fn gateway_child_telemetry_is_written_for_an_assigned_child() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let token = active_gateway_token(&pool).await;
    let tempdir = tempfile::tempdir().unwrap();
    let stream = gateway_stream(&tempdir);
    let app = webhook_router(
        TokenWebhookIngress::new(
            pool.clone(),
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .oneshot(gateway_webhook_request(
            &token,
            GATEWAY_TELEMETRY_TOPIC,
            json!({
                "schema_version": 1,
                "kind": "child_telemetry",
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1842,
                "event_at": "2026-09-04T10:12:00Z",
                "child_device_id": "child-001",
                "measurements": {"temperature_c": 26.4}
            }),
        ))
        .await
        .unwrap();
    wait_for_stream_record(&stream).await;
    let mut consumer = stream
        .join_group(
            "timescaledb-writer",
            "gateway-webhook-writer",
            GroupStart::Earliest,
            Utc::now(),
        )
        .unwrap();
    let result = TelemetryWriter::new(pool.clone(), 10)
        .flush_once(&mut consumer, Utc::now())
        .await
        .unwrap();
    let row = query(
        "SELECT device_id, gateway_device_id, topic
         FROM telemetry
         WHERE device_id = 'child-001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(result.inserted, 1);
    assert_eq!(row.get::<String, _>("device_id"), "child-001");
    assert_eq!(
        row.get::<Option<String>, _>("gateway_device_id").as_deref(),
        Some("gateway-001")
    );
    assert_eq!(row.get::<String, _>("topic"), GATEWAY_TELEMETRY_TOPIC);
}

#[tokio::test]
async fn gateway_cannot_write_an_unassigned_child() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let token = active_gateway_token(&pool).await;
    query(
        "INSERT INTO devices (device_id, is_gateway)
         VALUES ('gateway-002', TRUE), ('child-foreign', FALSE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    query(
        "UPDATE devices
         SET gateway_device_id = 'gateway-002'
         WHERE device_id = 'child-foreign'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let stream = gateway_stream(&tempdir);
    let app = webhook_router(
        TokenWebhookIngress::new(
            pool,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .oneshot(gateway_webhook_request(
            &token,
            GATEWAY_TELEMETRY_TOPIC,
            json!({
                "schema_version": 1,
                "kind": "child_telemetry",
                "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                "sequence": 1842,
                "event_at": "2026-09-04T10:12:00Z",
                "child_device_id": "child-foreign",
                "measurements": {"temperature_c": 26.4}
            }),
        ))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .all(|partition| partition.next_offset == 0)
    );
}

#[tokio::test]
async fn invalid_webhook_secret_or_revoked_token_never_appends_a_record() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let token = active_device_token(&pool).await;
    let tempdir = tempfile::tempdir().unwrap();
    let stream =
        LocalStream::open(tempdir.path().join("stream"), StreamConfig::for_test(8)).unwrap();
    let app = webhook_router(
        TokenWebhookIngress::new(
            pool.clone(),
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let bad_secret = app
        .clone()
        .oneshot(webhook_request(&token, "wrong-secret"))
        .await
        .unwrap();
    query("UPDATE device_tokens SET revoked_at = now()")
        .execute(&pool)
        .await
        .unwrap();
    let revoked = app
        .oneshot(webhook_request(&token, WEBHOOK_SECRET))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(bad_secret.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(revoked.status(), StatusCode::OK);
    assert!(
        stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .all(|partition| partition.next_offset == 0)
    );
}

#[tokio::test]
async fn qos_zero_webhook_is_rejected_before_stream_append() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let token = active_device_token(&pool).await;
    let tempdir = tempfile::tempdir().unwrap();
    let stream =
        LocalStream::open(tempdir.path().join("stream"), StreamConfig::for_test(8)).unwrap();
    let app = webhook_router(
        TokenWebhookIngress::new(
            pool,
            stream.clone(),
            WEBHOOK_SECRET,
            tempdir.path().join("inbox"),
            Arc::new(IngestMetrics::default()),
        )
        .unwrap(),
    );

    let response = app
        .oneshot(webhook_request_with_qos(&token, WEBHOOK_SECRET, 0))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        stream
            .stats()
            .unwrap()
            .partitions
            .iter()
            .all(|partition| partition.next_offset == 0)
    );
}
