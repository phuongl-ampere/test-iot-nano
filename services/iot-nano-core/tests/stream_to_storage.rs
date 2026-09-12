use std::sync::Arc;

use chrono::{TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{
    CoreSqliteStore, CoreStreamConsumer, SqliteAlertEvaluator, SqliteTelemetryWriter,
};
use iot_stream::{LocalStream, StreamConfig, TelemetryMessage};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

fn message() -> TelemetryMessage {
    TelemetryMessage {
        topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
        payload: br#"{"temperature_c":26.4}"#.to_vec(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: "esp-000123".to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence: 1,
            event_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap(),
            measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
            gateway_device_id: None,
        },
        received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
    }
}

async fn stream(directory: &tempfile::TempDir) -> LocalStream {
    LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
        .await
        .unwrap()
}

async fn store(directory: &tempfile::TempDir) -> CoreSqliteStore {
    CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("core.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn local_stream_record_is_written_before_its_group_offset_is_acknowledged() {
    let directory = tempfile::tempdir().unwrap();
    let stream = stream(&directory).await;
    stream.append(message()).await.unwrap();
    let store = store(&directory).await;
    let consumer = CoreStreamConsumer::new(
        Arc::new(stream.clone()),
        "timescaledb-writer",
        "core-writer-test",
    );

    let result = SqliteTelemetryWriter::new(store.clone(), 100)
        .flush_once(&consumer, Utc::now())
        .await
        .unwrap();

    let rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
    assert_eq!(result.read, 1);
    assert_eq!(result.inserted, 1);
    assert_eq!(rows, 1);
    assert!(consumer.claim(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn local_stream_alert_group_acknowledges_only_after_evaluation() {
    let directory = tempfile::tempdir().unwrap();
    let stream = stream(&directory).await;
    stream.append(message()).await.unwrap();
    let consumer = CoreStreamConsumer::new(Arc::new(stream), "alert-evaluator", "core-alert-test");

    let result = SqliteAlertEvaluator::new(store(&directory).await, 100)
        .flush_event_rules(&consumer, Utc::now())
        .await
        .unwrap();

    assert_eq!(result.read, 1);
    assert_eq!(result.evaluated, 0);
    assert!(consumer.claim(10).await.unwrap().is_empty());
}
