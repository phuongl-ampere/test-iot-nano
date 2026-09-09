use std::{
    env,
    fs::{File, OpenOptions},
    sync::{LazyLock, Mutex},
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use fs2::FileExt;
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_ingest::{AlertEvaluator, SqliteAlertEvaluator, migrate};
use iot_storage::SqliteStore;
use iot_stream::{GroupStart, LocalStream, StreamConfig, StreamConsumer, TelemetryMessage};
use serde_json::json;
use sqlx::{PgPool, Row};
use tempfile::TempDir;
use uuid::Uuid;

const DEVICE_ID: &str = "esp-000123";
const TOPIC: &str = "iot/v1/devices/esp-000123/telemetry";

static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the local TimescaleDB test database")
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

async fn prepared_pool() -> PgPool {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "TRUNCATE command_outbox, device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices",
    )
        .execute(&pool)
        .await
        .unwrap();
    pool
}

fn alert_consumer(dir: &TempDir, now: DateTime<Utc>) -> (LocalStream, StreamConsumer) {
    let stream = LocalStream::open(dir.path().join("stream"), StreamConfig::for_test(1)).unwrap();
    let consumer = stream
        .join_group("alert-evaluator", "alert-test", GroupStart::Earliest, now)
        .unwrap();
    (stream, consumer)
}

fn at(second: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 5, 10, 0, 0).unwrap() + Duration::seconds(second)
}

async fn insert_event_rule(pool: &PgPool, for_seconds: i32, resolve_after_seconds: i32) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds
         ) VALUES ($1, 'High temperature', 'temperature_c', 'event_threshold', 'gt', 40.0,
                   $2, $3, 3600, 'warning', 86400)",
    )
    .bind(id)
    .bind(for_seconds)
    .bind(resolve_after_seconds)
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn insert_window_rule(pool: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, window_seconds,
            for_seconds, resolve_after_seconds, reopen_grace_seconds, severity,
            reminder_interval_seconds
         ) VALUES ($1, 'High average', 'temperature_c', 'window_average', 'gt', 40.0, 300,
                   0, 0, 3600, 'warning', 86400)",
    )
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn insert_telemetry(pool: &PgPool, value: f64, event_at: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO devices (device_id, last_seen_at)
         VALUES ($1, $2)
         ON CONFLICT (device_id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at",
    )
    .bind(DEVICE_ID)
    .bind(event_at)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES ($1, $1, $2, $3, $4, $5, $6)",
    )
    .bind(event_at)
    .bind(DEVICE_ID)
    .bind(Uuid::new_v4())
    .bind(event_at.timestamp())
    .bind(sqlx::types::Json(json!({ "temperature_c": value })))
    .bind(TOPIC)
    .execute(pool)
    .await
    .unwrap();
}

async fn sqlite_store(directory: &TempDir) -> SqliteStore {
    SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap()
}

async fn insert_sqlite_event_rule(store: &SqliteStore) -> Uuid {
    insert_sqlite_event_rule_with_timing(store, 0, 0).await
}

async fn insert_sqlite_event_rule_with_timing(
    store: &SqliteStore,
    for_seconds: i32,
    resolve_after_seconds: i32,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds
         ) VALUES (?, 'High temperature', 'temperature_c', 'event_threshold', 'gt', 40.0,
                   ?, ?, 3600, 'warning', 86400)",
    )
    .bind(id.to_string())
    .bind(for_seconds)
    .bind(resolve_after_seconds)
    .execute(store.pool())
    .await
    .unwrap();
    id
}

async fn insert_sqlite_window_rule(
    store: &SqliteStore,
    for_seconds: i32,
    resolve_after_seconds: i32,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, window_seconds,
            for_seconds, resolve_after_seconds, reopen_grace_seconds, severity,
            reminder_interval_seconds
         ) VALUES (?, 'High average', 'temperature_c', 'window_average', 'gt', 40.0, 300,
                   ?, ?, 3600, 'warning', 86400)",
    )
    .bind(id.to_string())
    .bind(for_seconds)
    .bind(resolve_after_seconds)
    .execute(store.pool())
    .await
    .unwrap();
    id
}

fn temperature_message(value: f64, received_at: DateTime<Utc>) -> TelemetryMessage {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(value));
    TelemetryMessage {
        topic: TOPIC.to_owned(),
        payload: format!(r#"{{"temperature_c":{value}}}"#).into_bytes(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: DEVICE_ID.to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence: u64::try_from(received_at.timestamp()).unwrap(),
            event_at: received_at,
            measurements,
            gateway_device_id: None,
        },
        received_at,
    }
}

#[tokio::test]
async fn sqlite_event_threshold_opens_incident_and_enqueues_notification() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    insert_sqlite_event_rule(&store).await;
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    stream.append(temperature_message(41.0, at(0))).unwrap();

    let result = SqliteAlertEvaluator::new(store.clone(), 100)
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();

    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE device_id = ?")
            .bind(DEVICE_ID)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let opened = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'opened'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.opened, 1);
    assert_eq!(status, "open");
    assert_eq!(opened, 1);
    assert!(consumer.poll(1, at(0)).unwrap().records.is_empty());
}

#[tokio::test]
async fn sqlite_event_threshold_deduplicates_replayed_telemetry_per_rule() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let rule_id = insert_sqlite_event_rule(&store).await;
    sqlx::query(
        "UPDATE alert_rules
         SET reminder_interval_seconds = 1
         WHERE id = ?",
    )
    .bind(rule_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let original = temperature_message(41.0, at(0));
    let mut replay = original.clone();
    replay.received_at = at(2);
    stream.append(original).unwrap();
    stream.append(replay).unwrap();

    let result = SqliteAlertEvaluator::new(store.clone(), 100)
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();
    let outbox_rows = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
        .fetch_one(store.pool())
        .await
        .unwrap();

    assert_eq!(result.read, 2);
    assert_eq!(result.evaluated, 1);
    assert_eq!(result.opened, 1);
    assert_eq!(result.reminders, 0);
    assert_eq!(outbox_rows, 1);
}

#[tokio::test]
async fn sqlite_event_threshold_sends_reminder_for_unacknowledged_open_incident() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let rule_id = insert_sqlite_event_rule(&store).await;
    sqlx::query(
        "UPDATE alert_rules
         SET reminder_interval_seconds = 1
         WHERE id = ?",
    )
    .bind(rule_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = SqliteAlertEvaluator::new(store.clone(), 100);

    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(41.0, at(2))).unwrap();

    let result = evaluator
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();
    let reminders = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'reminder'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.reminders, 1);
    assert_eq!(reminders, 1);
}

#[tokio::test]
async fn sqlite_event_threshold_resolves_incident_and_enqueues_notification() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    insert_sqlite_event_rule(&store).await;
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = SqliteAlertEvaluator::new(store.clone(), 100);

    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(39.0, at(1))).unwrap();

    let result = evaluator
        .flush_event_rules(&mut consumer, at(1))
        .await
        .unwrap();
    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE device_id = ?")
            .bind(DEVICE_ID)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let resolved = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'resolved'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.resolved, 1);
    assert_eq!(status, "resolved");
    assert_eq!(resolved, 1);
}

#[tokio::test]
async fn sqlite_event_threshold_honors_breach_and_recovery_durations() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    insert_sqlite_event_rule_with_timing(&store, 60, 60).await;
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = SqliteAlertEvaluator::new(store.clone(), 100);

    stream.append(temperature_message(41.0, at(0))).unwrap();
    let first_breach = evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    consumer.heartbeat(at(29)).unwrap();
    consumer.heartbeat(at(58)).unwrap();
    stream.append(temperature_message(41.0, at(61))).unwrap();
    let sustained_breach = evaluator
        .flush_event_rules(&mut consumer, at(61))
        .await
        .unwrap();
    stream.append(temperature_message(39.0, at(62))).unwrap();
    let first_normal = evaluator
        .flush_event_rules(&mut consumer, at(62))
        .await
        .unwrap();
    consumer.heartbeat(at(87)).unwrap();
    consumer.heartbeat(at(116)).unwrap();
    stream.append(temperature_message(39.0, at(123))).unwrap();
    let sustained_normal = evaluator
        .flush_event_rules(&mut consumer, at(123))
        .await
        .unwrap();
    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE device_id = ?")
            .bind(DEVICE_ID)
            .fetch_one(store.pool())
            .await
            .unwrap();

    assert_eq!(first_breach.opened, 0);
    assert_eq!(sustained_breach.opened, 1);
    assert_eq!(first_normal.resolved, 0);
    assert_eq!(sustained_normal.resolved, 1);
    assert_eq!(status, "resolved");
}

#[tokio::test]
async fn sqlite_event_threshold_applies_hysteresis_and_reopens_within_grace() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    let rule_id = insert_sqlite_event_rule(&store).await;
    sqlx::query("UPDATE alert_rules SET hysteresis = 2.0 WHERE id = ?")
        .bind(rule_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = SqliteAlertEvaluator::new(store.clone(), 100);

    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(39.0, at(1))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(1))
        .await
        .unwrap();
    let status_after_indeterminate =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE rule_id = ?")
            .bind(rule_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    stream.append(temperature_message(38.0, at(2))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();
    stream.append(temperature_message(41.0, at(3))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(3))
        .await
        .unwrap();
    let incidents =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents WHERE rule_id = ?")
            .bind(rule_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE rule_id = ?")
            .bind(rule_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();

    assert_eq!(status_after_indeterminate, "open");
    assert_eq!(incidents, 1);
    assert_eq!(status, "open");
}

#[tokio::test]
async fn sqlite_window_average_honors_incident_state_transitions() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = sqlite_store(&tempdir).await;
    insert_sqlite_window_rule(&store, 60, 0).await;
    let high = temperature_message(41.0, at(240)).event;
    store.write_telemetry(&high, at(240), TOPIC).await.unwrap();
    let evaluator = SqliteAlertEvaluator::new(store.clone(), 100);

    let first_window = evaluator.flush_window_rules(at(300)).await.unwrap();
    let sustained_window = evaluator.flush_window_rules(at(361)).await.unwrap();
    let low = temperature_message(39.0, at(540)).event;
    store.write_telemetry(&low, at(540), TOPIC).await.unwrap();
    let normal_window = evaluator.flush_window_rules(at(541)).await.unwrap();
    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE device_id = ?")
            .bind(DEVICE_ID)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let opened = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'opened'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let resolved = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'resolved'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(first_window.opened, 0);
    assert_eq!(sustained_window.opened, 1);
    assert_eq!(normal_window.resolved, 1);
    assert_eq!(status, "resolved");
    assert_eq!(opened, 1);
    assert_eq!(resolved, 1);
}

#[tokio::test]
async fn breach_opens_after_for_duration_and_enqueues_one_email() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    insert_event_rule(&pool, 60, 300).await;
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    stream.append(temperature_message(41.0, at(0))).unwrap();
    stream.append(temperature_message(41.0, at(61))).unwrap();
    consumer.heartbeat(at(29)).unwrap();
    consumer.heartbeat(at(58)).unwrap();

    AlertEvaluator::new(pool.clone(), 100)
        .flush_event_rules(&mut consumer, at(61))
        .await
        .unwrap();

    let incident = sqlx::query(
        "SELECT status, opened_at
         FROM alert_incidents
         WHERE device_id = $1",
    )
    .bind(DEVICE_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let opened = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'opened'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(incident.get::<String, _>("status"), "open");
    assert!(
        incident
            .get::<Option<DateTime<Utc>>, _>("opened_at")
            .is_some()
    );
    assert_eq!(opened, 1);
    assert!(consumer.poll(1, at(61)).unwrap().records.is_empty());
}

#[tokio::test]
async fn normal_value_resolves_open_incident_and_enqueues_one_email() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    insert_event_rule(&pool, 0, 0).await;
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    stream.append(temperature_message(41.0, at(0))).unwrap();

    let evaluator = AlertEvaluator::new(pool.clone(), 100);
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(39.0, at(1))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(1))
        .await
        .unwrap();

    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE device_id = $1")
            .bind(DEVICE_ID)
            .fetch_one(&pool)
            .await
            .unwrap();
    let resolved = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'resolved'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(status, "resolved");
    assert_eq!(resolved, 1);
}

#[tokio::test]
async fn five_minute_average_opens_window_rule() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    insert_window_rule(&pool).await;
    insert_telemetry(&pool, 41.0, at(240)).await;

    AlertEvaluator::new(pool.clone(), 100)
        .flush_window_rules(at(300))
        .await
        .unwrap();

    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE device_id = $1")
            .bind(DEVICE_ID)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(status, "open");
}

#[tokio::test]
async fn acknowledged_open_incident_does_not_enqueue_reminder() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = insert_event_rule(&pool, 0, 300).await;
    sqlx::query(
        "UPDATE alert_rules
         SET reminder_interval_seconds = 1
         WHERE id = $1",
    )
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = AlertEvaluator::new(pool.clone(), 100);
    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE alert_incidents
         SET acknowledged_at = $1, acknowledged_by = 'dashboard'
         WHERE rule_id = $2",
    )
    .bind(at(1))
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();
    stream.append(temperature_message(41.0, at(2))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();

    let reminders = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'reminder'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(reminders, 0);
}

#[tokio::test]
async fn open_unacknowledged_incident_enqueues_periodic_reminder() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = insert_event_rule(&pool, 0, 300).await;
    sqlx::query(
        "UPDATE alert_rules
         SET reminder_interval_seconds = 1
         WHERE id = $1",
    )
    .bind(rule_id)
    .execute(&pool)
    .await
    .unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = AlertEvaluator::new(pool.clone(), 100);
    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(41.0, at(2))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();

    let reminders = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'reminder'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(reminders, 1);
}

#[tokio::test]
async fn breach_within_reopen_grace_reuses_resolved_incident() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    insert_event_rule(&pool, 0, 0).await;
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = AlertEvaluator::new(pool.clone(), 100);

    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    let incident_id =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM alert_incidents WHERE device_id = $1")
            .bind(DEVICE_ID)
            .fetch_one(&pool)
            .await
            .unwrap();

    stream.append(temperature_message(39.0, at(1))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(1))
        .await
        .unwrap();
    stream.append(temperature_message(41.0, at(2))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();

    let reopened_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM alert_incidents WHERE device_id = $1 AND status = 'open'",
    )
    .bind(DEVICE_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let incident_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents WHERE device_id = $1")
            .bind(DEVICE_ID)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(reopened_id, incident_id);
    assert_eq!(incident_count, 1);
}

#[tokio::test]
async fn hysteresis_requires_recovery_boundary_before_resolving() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = insert_event_rule(&pool, 0, 0).await;
    sqlx::query("UPDATE alert_rules SET hysteresis = 2.0 WHERE id = $1")
        .bind(rule_id)
        .execute(&pool)
        .await
        .unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    let evaluator = AlertEvaluator::new(pool.clone(), 100);

    stream.append(temperature_message(41.0, at(0))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(39.0, at(1))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(1))
        .await
        .unwrap();
    let status_after_indeterminate =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE rule_id = $1")
            .bind(rule_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    stream.append(temperature_message(38.0, at(2))).unwrap();
    evaluator
        .flush_event_rules(&mut consumer, at(2))
        .await
        .unwrap();
    let status_after_recovery =
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE rule_id = $1")
            .bind(rule_id)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(status_after_indeterminate, "open");
    assert_eq!(status_after_recovery, "resolved");
}

#[tokio::test]
async fn disabled_rule_does_not_create_incident() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = insert_event_rule(&pool, 0, 300).await;
    sqlx::query("UPDATE alert_rules SET enabled = FALSE WHERE id = $1")
        .bind(rule_id)
        .execute(&pool)
        .await
        .unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    stream.append(temperature_message(41.0, at(0))).unwrap();

    let result = AlertEvaluator::new(pool.clone(), 100)
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    let incidents = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(result.evaluated, 0);
    assert_eq!(incidents, 0);
}

#[tokio::test]
async fn archived_rule_does_not_create_incident() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = insert_event_rule(&pool, 0, 300).await;
    sqlx::query("UPDATE alert_rules SET archived_at = now() WHERE id = $1")
        .bind(rule_id)
        .execute(&pool)
        .await
        .unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    stream.append(temperature_message(41.0, at(0))).unwrap();

    let result = AlertEvaluator::new(pool.clone(), 100)
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    let incidents = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(result.evaluated, 0);
    assert_eq!(incidents, 0);
}

#[tokio::test]
async fn evaluator_waits_for_an_archive_rule_lock_before_transitioning_incidents() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let rule_id = insert_event_rule(&pool, 0, 300).await;
    let tempdir = tempfile::tempdir().unwrap();
    let (stream, mut consumer) = alert_consumer(&tempdir, at(0));
    stream.append(temperature_message(41.0, at(0))).unwrap();
    let evaluator = AlertEvaluator::new(pool.clone(), 100);
    evaluator
        .flush_event_rules(&mut consumer, at(0))
        .await
        .unwrap();
    stream.append(temperature_message(41.0, at(1))).unwrap();

    let mut archive_transaction = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM alert_rules WHERE id = $1 FOR UPDATE")
        .bind(rule_id)
        .fetch_one(&mut *archive_transaction)
        .await
        .unwrap();

    let blocked = tokio::time::timeout(
        StdDuration::from_millis(100),
        evaluator.flush_event_rules(&mut consumer, at(1)),
    )
    .await;

    assert!(blocked.is_err(), "evaluator must wait for archive lock");

    archive_transaction.rollback().await.unwrap();
    let result = evaluator
        .flush_event_rules(&mut consumer, at(1))
        .await
        .unwrap();
    assert_eq!(result.evaluated, 1);
}
