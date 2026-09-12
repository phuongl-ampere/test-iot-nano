use std::time::Duration;

use chrono::{TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{HttpStreamConsumer, SqliteAlertEvaluator, SqliteTelemetryWriter};
use iot_stream::{LocalStream, StreamConfig, TelemetryMessage, http::StreamHttpState};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

const STREAM_SECRET: &str = "stream-secret-must-have-at-least-32-ascii";
const CORE_STREAM_SECRET: &str = "core-stream-secret-must-have-at-least-32-xx";

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

async fn start_stream(stream: LocalStream) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = iot_stream::http::router(StreamHttpState::new(
        stream,
        STREAM_SECRET,
        CORE_STREAM_SECRET,
    ));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), task)
}

#[tokio::test]
async fn http_stream_record_is_written_to_sqlite_before_its_group_offset_is_acknowledged() {
    let directory = tempfile::tempdir().unwrap();
    let stream =
        LocalStream::open(directory.path().join("stream"), StreamConfig::for_test(1)).unwrap();
    stream.append(message()).unwrap();
    let (stream_url, server) = start_stream(stream).await;

    let store = iot_nano_core::CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("core.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let mut consumer = HttpStreamConsumer::new(
        &stream_url,
        CORE_STREAM_SECRET,
        "timescaledb-writer",
        "core-writer-test",
    )
    .unwrap();
    let writer = SqliteTelemetryWriter::new(store.clone(), 100);

    let result = writer
        .flush_http_once(&mut consumer, Utc::now())
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
    assert!(consumer.claim(10).await.unwrap().records.is_empty());

    server.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), server).await;
}

#[tokio::test]
async fn http_stream_alert_group_acknowledges_only_after_evaluation() {
    let directory = tempfile::tempdir().unwrap();
    let stream =
        LocalStream::open(directory.path().join("stream"), StreamConfig::for_test(1)).unwrap();
    stream.append(message()).unwrap();
    let (stream_url, server) = start_stream(stream).await;

    let store = iot_nano_core::CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("core.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let consumer = HttpStreamConsumer::new(
        &stream_url,
        CORE_STREAM_SECRET,
        "alert-evaluator",
        "core-alert-test",
    )
    .unwrap();
    let evaluator = SqliteAlertEvaluator::new(store, 100);

    let result = evaluator
        .flush_http_event_rules(&consumer, Utc::now())
        .await
        .unwrap();

    assert_eq!(result.read, 1);
    assert_eq!(result.evaluated, 0);
    assert!(consumer.claim(10).await.unwrap().records.is_empty());

    server.abort();
    let _ = tokio::time::timeout(Duration::from_secs(1), server).await;
}
