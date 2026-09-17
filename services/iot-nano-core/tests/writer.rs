use std::{
    env,
    fs::{File, OpenOptions},
    sync::Arc,
    sync::{LazyLock, Mutex},
};

use chrono::{TimeZone, Utc};
use fs2::FileExt;
use iot_core::TelemetryEvent;
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_core::{
    CoreSqliteStore, CoreStreamConsumer, TelemetryWriter, connect_core_database, migrate,
};
use iot_stream::{
    GatewayEvent, GatewayEventKind, GatewayMessage, LocalStream, StreamConfig, TelemetryMessage,
};
use serde_json::json;
use sqlx::{PgPool, Row};
use tempfile::TempDir;
use uuid::Uuid;

const TOPIC: &str = "iot/v1/devices/esp-000123/telemetry";
const GATEWAY_TOPIC: &str = "v1/gateways/me/telemetry";
const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

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
    let pool = connect_core_database(&database_url()).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "TRUNCATE gateway_event_receipts, command_outbox, notification_outbox, alert_rule_event_evaluations, alert_incidents, alert_rules, telemetry, device_runtime_state CASCADE",
    )
        .execute(&pool)
        .await
        .unwrap();
    pool
}

async fn consumer(dir: &TempDir, _now: chrono::DateTime<Utc>) -> (LocalStream, CoreStreamConsumer) {
    let stream = LocalStream::open(
        StreamConfig::sqlite(dir.path().join("stream.sqlite")).with_partitions(1),
    )
    .await
    .unwrap();
    let consumer = CoreStreamConsumer::new(
        Arc::new(stream.clone()),
        "timescaledb-writer",
        "writer-test",
    );
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
async fn flush_once_commits_stream_deduplicated_events_and_advances_group_offsets() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let tempdir = tempfile::tempdir().unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, consumer) = consumer(&tempdir, now).await;
    let event = event();
    for _ in 0..2 {
        stream
            .append(TelemetryMessage {
                tenant_id: TEST_TENANT_ID,
                topic: TOPIC.to_owned(),
                payload: br#"{"sequence":1842}"#.to_vec(),
                event: event.clone(),
                received_at: now,
            })
            .await
            .unwrap();
    }

    let writer = TelemetryWriter::new(pool.clone(), 1_000);
    let result = writer.flush_once(&consumer, now).await.unwrap();

    let telemetry_rows =
        sqlx::query("SELECT COUNT(*) AS count FROM telemetry WHERE tenant_id = $1")
            .bind(TEST_TENANT_ID)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<i64, _>("count");

    assert_eq!(result.read, 1);
    assert_eq!(result.inserted, 1);
    assert_eq!(result.duplicates, 0);
    assert_eq!(telemetry_rows, 1);
    assert!(consumer.claim(1).await.unwrap().is_empty());
}

#[tokio::test]
async fn writer_persists_gateway_child_telemetry_and_one_receipt() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;

    let tempdir = tempfile::tempdir().unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, consumer) = consumer(&tempdir, now).await;
    let mut event = event();
    event.device_id = "child-001".to_owned();
    event.gateway_device_id = Some("gateway-001".to_owned());
    let gateway_message = GatewayMessage {
        tenant_id: TEST_TENANT_ID,
        topic: GATEWAY_TOPIC.to_owned(),
        payload: br#"{"kind":"child_telemetry"}"#.to_vec(),
        gateway_event: GatewayEvent {
            schema_version: 1,
            gateway_device_id: "gateway-001".to_owned(),
            child_device_id: Some("child-001".to_owned()),
            token_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            session_id: Some("gateway-session".to_owned()),
            event_kind: GatewayEventKind::ChildTelemetry,
            event_at: now,
            payload: json!({"kind": "child_telemetry"}),
            idempotency_key: "gateway-001:boot-1:1".to_owned(),
        },
        telemetry_event: Some(event),
        received_at: now,
    };
    stream.append(gateway_message.clone()).await.unwrap();
    stream.append(gateway_message).await.unwrap();

    let writer = TelemetryWriter::new(pool.clone(), 1_000);
    let result = writer.flush_once(&consumer, now).await.unwrap();

    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id FROM telemetry
         WHERE tenant_id = $1 AND device_id = 'child-001'",
    )
    .bind(TEST_TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(gateway_device_id.as_deref(), Some("gateway-001"));
    assert_eq!(result.read, 1);
    assert_eq!(result.inserted, 1);
    let receipt_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM gateway_event_receipts
         WHERE tenant_id = $1 AND gateway_device_id = 'gateway-001'
           AND idempotency_key = 'gateway-001:boot-1:1'",
    )
    .bind(TEST_TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
}

#[tokio::test]
async fn sqlite_writer_commits_stream_records_and_rollups() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(tempdir.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, consumer) = consumer(&tempdir, now).await;
    stream
        .append(TelemetryMessage {
            tenant_id: TEST_TENANT_ID,
            topic: TOPIC.to_owned(),
            payload: br#"{"sequence":1842}"#.to_vec(),
            event: event(),
            received_at: now,
        })
        .await
        .unwrap();

    let writer = iot_nano_core::SqliteTelemetryWriter::new(store.clone(), 1_000);
    let result = writer.flush_once(&consumer, now).await.unwrap();
    let telemetry_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry WHERE tenant_id = ?")
        .bind(TEST_TENANT_ID.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(result.read, 1);
    assert_eq!(result.inserted, 1);
    assert_eq!(telemetry_rows, 1);
    assert!(consumer.claim(1).await.unwrap().is_empty());
}

#[tokio::test]
async fn sqlite_writer_acknowledges_gateway_records_without_writing_telemetry() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(tempdir.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, consumer) = consumer(&tempdir, now).await;
    stream
        .append(GatewayMessage {
            tenant_id: TEST_TENANT_ID,
            topic: "iot/v1/gateways/gateway-001/events".to_owned(),
            payload: br#"{"kind":"heartbeat"}"#.to_vec(),
            gateway_event: GatewayEvent {
                schema_version: 1,
                gateway_device_id: "gateway-001".to_owned(),
                child_device_id: None,
                token_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
                session_id: Some("gateway-session".to_owned()),
                event_kind: GatewayEventKind::Heartbeat,
                event_at: now,
                payload: json!({"kind": "heartbeat"}),
                idempotency_key: "gateway-001:boot-1:1".to_owned(),
            },
            telemetry_event: None,
            received_at: now,
        })
        .await
        .unwrap();

    let writer = iot_nano_core::SqliteTelemetryWriter::new(store.clone(), 1_000);
    let result = writer.flush_once(&consumer, now).await.unwrap();
    let telemetry_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry WHERE tenant_id = ?")
        .bind(TEST_TENANT_ID.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(result.read, 1);
    assert_eq!(result.inserted, 0);
    assert_eq!(result.duplicates, 0);
    assert_eq!(telemetry_rows, 0);
    let gateway_last_seen = sqlx::query_scalar::<_, Option<String>>(
        "SELECT last_seen_at
         FROM device_runtime_state
         WHERE tenant_id = ? AND device_id = 'gateway-001'",
    )
    .bind(TEST_TENANT_ID.to_string())
    .fetch_optional(store.pool())
    .await
    .unwrap()
    .flatten();
    assert!(gateway_last_seen.is_some());
    assert!(consumer.claim(1).await.unwrap().is_empty());
}

#[tokio::test]
async fn sqlite_writer_persists_canonical_child_telemetry_and_one_gateway_receipt() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(tempdir.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, consumer) = consumer(&tempdir, now).await;
    let mut child_event = event();
    child_event.device_id = "child-001".to_owned();
    child_event.gateway_device_id = Some("gateway-001".to_owned());
    let gateway_message = GatewayMessage {
        tenant_id: TEST_TENANT_ID,
        topic: "iot/v1/gateways/gateway-001/events".to_owned(),
        payload: br#"{"kind":"child_telemetry"}"#.to_vec(),
        gateway_event: GatewayEvent {
            schema_version: 1,
            gateway_device_id: "gateway-001".to_owned(),
            child_device_id: Some("child-001".to_owned()),
            token_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            session_id: Some("gateway-session".to_owned()),
            event_kind: GatewayEventKind::ChildTelemetry,
            event_at: now,
            payload: json!({"kind": "child_telemetry"}),
            idempotency_key: "gateway-001:boot-1:2".to_owned(),
        },
        telemetry_event: Some(child_event),
        received_at: now,
    };
    stream.append(gateway_message.clone()).await.unwrap();
    stream.append(gateway_message).await.unwrap();

    let writer = iot_nano_core::SqliteTelemetryWriter::new(store.clone(), 1_000);
    let result = writer.flush_once(&consumer, now).await.unwrap();
    let row = sqlx::query(
        "SELECT device_id, gateway_device_id FROM telemetry
         WHERE tenant_id = ? AND device_id = 'child-001'",
    )
    .bind(TEST_TENANT_ID.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.inserted, 1);
    assert_eq!(result.read, 1);
    assert_eq!(row.get::<String, _>("device_id"), "child-001");
    assert_eq!(
        row.get::<Option<String>, _>("gateway_device_id").as_deref(),
        Some("gateway-001")
    );
    let receipt_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM gateway_event_receipts
         WHERE tenant_id = ? AND gateway_device_id = 'gateway-001'
           AND idempotency_key = 'gateway-001:boot-1:2'",
    )
    .bind(TEST_TENANT_ID.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
}

#[tokio::test]
async fn sqlite_writer_marks_disconnected_gateway_child_unavailable() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(tempdir.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();
    let (stream, consumer) = consumer(&tempdir, now).await;
    stream
        .append(GatewayMessage {
            tenant_id: TEST_TENANT_ID,
            topic: "iot/v1/gateways/gateway-001/events".to_owned(),
            payload: br#"{"kind":"disconnect"}"#.to_vec(),
            gateway_event: GatewayEvent {
                schema_version: 1,
                gateway_device_id: "gateway-001".to_owned(),
                child_device_id: Some("child-001".to_owned()),
                token_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
                session_id: Some("gateway-session".to_owned()),
                event_kind: GatewayEventKind::Disconnect,
                event_at: now,
                payload: json!({"kind": "disconnect"}),
                idempotency_key: "gateway-001:boot-1:3".to_owned(),
            },
            telemetry_event: None,
            received_at: now,
        })
        .await
        .unwrap();

    iot_nano_core::SqliteTelemetryWriter::new(store.clone(), 1_000)
        .flush_once(&consumer, now)
        .await
        .unwrap();
    let quality = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_read_quality
         FROM device_runtime_state
         WHERE tenant_id = ? AND device_id = 'child-001'",
    )
    .bind(TEST_TENANT_ID.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(quality.as_deref(), Some("unavailable"));
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
    let (stream, consumer) = consumer(&tempdir, now).await;
    let event = event_with_sequence(u64::MAX);
    stream
        .append(TelemetryMessage {
            tenant_id: TEST_TENANT_ID,
            topic: TOPIC.to_owned(),
            payload: br#"{"sequence":1842}"#.to_vec(),
            event,
            received_at: now,
        })
        .await
        .unwrap();

    let writer = TelemetryWriter::new(pool, 1_000);
    assert!(writer.flush_once(&consumer, now).await.is_err());

    let replay = consumer.claim(1).await.unwrap();
    assert_eq!(replay.records().len(), 1);
    assert_eq!(replay.records()[0].offset, 0);
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
         WHERE proc_name = 'policy_refresh_continuous_aggregate'
           AND hypertable_schema = 'iot_nano_core'",
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
         WHERE table_schema = 'iot_nano_core'
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
async fn migration_installs_rule_archive_without_api_schema() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;

    let has_access_tokens = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.tables
             WHERE table_schema = 'iot_nano_core' AND table_name = 'api_access_tokens'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let has_archived_at = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'iot_nano_core'
               AND table_name = 'alert_rules'
               AND column_name = 'archived_at'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(!has_access_tokens);
    assert!(has_archived_at);

    let domain_tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano_core'
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
    assert!(domain_tables.is_empty());
}

#[tokio::test]
async fn migration_owns_the_core_schema_without_api_tables() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = connect_core_database(&database_url()).await.unwrap();
    migrate(&pool).await.unwrap();

    let core_tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano_core'
           AND table_name IN ('telemetry', 'device_runtime_state', 'command_outbox')
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let api_tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano_core'
           AND table_name = 'api_access_tokens'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        core_tables,
        ["command_outbox", "device_runtime_state", "telemetry"]
    );
    assert!(api_tables.is_empty());
}

#[tokio::test]
#[ignore = "requires DATABASE_URL for an isolated TimescaleDB test database"]
async fn timescale_migration_rejects_pre_tenant_core_schema_without_partial_replay() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = connect_core_database(&database_url()).await.unwrap();
    sqlx::query(
        "DROP SCHEMA IF EXISTS iot_nano_core CASCADE;
         CREATE SCHEMA iot_nano_core;
         CREATE TABLE iot_nano_core.telemetry (
             event_at TIMESTAMPTZ NOT NULL,
             device_id TEXT NOT NULL,
             boot_id UUID NOT NULL,
             sequence BIGINT NOT NULL
         );",
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = migrate(&pool).await.unwrap_err();
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("telemetry"),
        "unexpected migration error: {error}"
    );

    let runtime_state_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('iot_nano_core.device_runtime_state') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    let tenant_id_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'iot_nano_core'
               AND table_name = 'telemetry'
               AND column_name = 'tenant_id'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(!runtime_state_table_exists);
    assert!(!tenant_id_column_exists);
    sqlx::query("DROP SCHEMA iot_nano_core CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
}

#[tokio::test]
async fn core_pool_does_not_resolve_public_metadata_tables() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = connect_core_database(&database_url()).await.unwrap();
    migrate(&pool).await.unwrap();

    assert!(
        sqlx::query("SELECT device_id FROM devices")
            .fetch_optional(&pool)
            .await
            .is_err()
    );
}
