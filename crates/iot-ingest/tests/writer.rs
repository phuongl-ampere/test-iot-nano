use std::{
    env,
    fs::{File, OpenOptions},
    sync::{LazyLock, Mutex},
};

use chrono::{TimeZone, Utc};
use fs2::FileExt;
use iot_core::TelemetryEvent;
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_ingest::{TelemetryWriter, migrate};
use iot_storage::SqliteStore;
use iot_stream::{GroupStart, LocalStream, StreamConfig, StreamConsumer, TelemetryMessage};
use serde_json::json;
use sqlx::{PgPool, Row};
use tempfile::TempDir;
use uuid::Uuid;

const TOPIC: &str = "iot/v1/devices/esp-000123/telemetry";
const GATEWAY_TOPIC: &str = "v1/gateways/me/telemetry";

// Every test targets the same TimescaleDB database and resets shared tables.
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
        "TRUNCATE device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices",
    )
        .execute(&pool)
        .await
        .unwrap();
    pool
}

fn consumer(dir: &TempDir, now: chrono::DateTime<Utc>) -> (LocalStream, StreamConsumer) {
    let stream = LocalStream::open(dir.path().join("stream"), StreamConfig::for_test(1)).unwrap();
    let consumer = stream
        .join_group(
            "timescaledb-writer",
            "writer-test",
            GroupStart::Earliest,
            now,
        )
        .unwrap();
    (stream, consumer)
}

fn event_with_sequence(sequence: u64) -> TelemetryEvent {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));

    TelemetryEvent {
        schema_version: 1,
        device_id: "esp-000123".to_owned(),
        boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
        sequence,
        event_at: Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap(),
        measurements,
        gateway_device_id: None,
    }
}

fn event() -> TelemetryEvent {
    event_with_sequence(1842)
}

#[tokio::test]
async fn flush_once_commits_unique_events_and_advances_group_offsets() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let tempdir = tempfile::tempdir().unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, mut consumer) = consumer(&tempdir, now);
    let event = event();
    for _ in 0..2 {
        stream
            .append(TelemetryMessage {
                topic: TOPIC.to_owned(),
                payload: br#"{"sequence":1842}"#.to_vec(),
                event: event.clone(),
                received_at: now,
            })
            .unwrap();
    }

    let writer = TelemetryWriter::new(pool.clone(), 1_000);
    let result = writer.flush_once(&mut consumer, now).await.unwrap();

    let telemetry_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(result.read, 2);
    assert_eq!(result.inserted, 1);
    assert_eq!(result.duplicates, 1);
    assert_eq!(telemetry_rows, 1);
    assert_eq!(consumer.poll(1, now).unwrap().records.len(), 0);
}

#[tokio::test]
async fn writer_persists_gateway_provenance_for_child_telemetry() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    sqlx::query("INSERT INTO devices (device_id, is_gateway) VALUES ('gateway-001', TRUE)")
        .execute(&pool)
        .await
        .unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, mut consumer) = consumer(&tempdir, now);
    let mut event = event();
    event.device_id = "child-001".to_owned();
    event.gateway_device_id = Some("gateway-001".to_owned());
    stream
        .append(TelemetryMessage {
            topic: GATEWAY_TOPIC.to_owned(),
            payload: br#"{"kind":"child_telemetry"}"#.to_vec(),
            event,
            received_at: now,
        })
        .unwrap();

    let writer = TelemetryWriter::new(pool.clone(), 1_000);
    writer.flush_once(&mut consumer, now).await.unwrap();

    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id FROM telemetry WHERE device_id = 'child-001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(gateway_device_id.as_deref(), Some("gateway-001"));
}

#[tokio::test]
async fn sqlite_writer_commits_stream_records_and_rollups() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(tempdir.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, mut consumer) = consumer(&tempdir, now);
    stream
        .append(TelemetryMessage {
            topic: TOPIC.to_owned(),
            payload: br#"{"sequence":1842}"#.to_vec(),
            event: event(),
            received_at: now,
        })
        .unwrap();

    let writer = iot_ingest::SqliteTelemetryWriter::new(store.clone(), 1_000);
    let result = writer.flush_once(&mut consumer, now).await.unwrap();
    let telemetry_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(result.read, 1);
    assert_eq!(result.inserted, 1);
    assert_eq!(telemetry_rows, 1);
    assert_eq!(consumer.poll(1, now).unwrap().records.len(), 0);
}

#[tokio::test]
async fn writer_does_not_advance_group_offset_when_database_conversion_fails() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let tempdir = tempfile::tempdir().unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, mut consumer) = consumer(&tempdir, now);
    let event = event_with_sequence(u64::MAX);
    stream
        .append(TelemetryMessage {
            topic: TOPIC.to_owned(),
            payload: br#"{"sequence":1842}"#.to_vec(),
            event,
            received_at: now,
        })
        .unwrap();

    let writer = TelemetryWriter::new(pool, 1_000);
    assert!(writer.flush_once(&mut consumer, now).await.is_err());

    let replay = consumer.poll(1, now).unwrap();
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].offset, 0);
}

#[tokio::test]
async fn migration_installs_retention_and_compression_policies() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;

    let policy_names = sqlx::query(
        "SELECT proc_name
         FROM timescaledb_information.jobs
         WHERE hypertable_name = 'telemetry'",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("proc_name"))
    .collect::<Vec<_>>();

    assert!(policy_names.iter().any(|name| name == "policy_compression"));
    assert!(policy_names.iter().any(|name| name == "policy_retention"));
}

#[tokio::test]
async fn migration_installs_refresh_jobs_for_both_dashboard_aggregates() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;

    let refresh_jobs = sqlx::query(
        "SELECT COUNT(*) AS count
         FROM timescaledb_information.jobs
         WHERE proc_name = 'policy_refresh_continuous_aggregate'",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get::<i64, _>("count");

    assert_eq!(refresh_jobs, 2);
}

#[tokio::test]
async fn migration_installs_alert_rules_incidents_and_outbox() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;

    let tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name IN ('alert_rules', 'alert_incidents', 'notification_outbox')
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        tables,
        ["alert_incidents", "alert_rules", "notification_outbox"]
    );
}

#[tokio::test]
async fn migration_installs_auth_and_rule_archive_schema() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;

    let has_access_tokens = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'api_access_tokens'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let has_archived_at = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = 'alert_rules'
               AND column_name = 'archived_at'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(has_access_tokens);
    assert!(has_archived_at);

    let domain_tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name IN (
               'users',
               'user_app_grants',
               'assets',
               'asset_profiles',
               'device_profiles'
           )
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        domain_tables,
        [
            "asset_profiles",
            "assets",
            "device_profiles",
            "user_app_grants",
            "users"
        ]
    );
}
