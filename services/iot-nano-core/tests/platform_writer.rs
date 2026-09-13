use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{CoreStreamConsumer, PlatformTelemetryWriter, WriterError};
use iot_storage::{
    GatewayIngestEventKind, GatewayIngestRepository, GatewayIngestRequest, GatewayIngestResult,
    PlatformStore, PlatformStoreError, TelemetryRepository,
};
use iot_stream::{
    GatewayEvent, GatewayEventKind, GatewayMessage, LocalStream, StreamConfig, TelemetryMessage,
};
use serde_json::json;
use sqlx::Row;
use tempfile::TempDir;
use uuid::Uuid;

const TOPIC: &str = "iot/v1/devices/direct-1/telemetry";

#[derive(Clone)]
struct FakeRepository {
    telemetry_calls: Arc<Mutex<Vec<(TelemetryEvent, DateTime<Utc>, String)>>>,
    gateway_calls: Arc<Mutex<Vec<GatewayIngestRequest>>>,
    telemetry_result: Arc<Mutex<Result<bool, ()>>>,
    telemetry_failure_call: Arc<Mutex<Option<usize>>>,
    gateway_result: Arc<Mutex<Result<GatewayIngestResult, ()>>>,
}

impl FakeRepository {
    fn failing_direct() -> Self {
        Self {
            telemetry_result: Arc::new(Mutex::new(Err(()))),
            ..Self::default()
        }
    }

    fn failing_direct_on_call(call: usize) -> Self {
        Self {
            telemetry_failure_call: Arc::new(Mutex::new(Some(call))),
            ..Self::default()
        }
    }

    fn failing_gateway() -> Self {
        Self {
            gateway_result: Arc::new(Mutex::new(Err(()))),
            ..Self::default()
        }
    }
}

impl Default for FakeRepository {
    fn default() -> Self {
        Self {
            telemetry_calls: Arc::default(),
            gateway_calls: Arc::default(),
            telemetry_result: Arc::new(Mutex::new(Ok(true))),
            telemetry_failure_call: Arc::default(),
            gateway_result: Arc::new(Mutex::new(Ok(GatewayIngestResult {
                receipt_inserted: true,
                telemetry_inserted: false,
            }))),
        }
    }
}

impl TelemetryRepository for FakeRepository {
    fn write_telemetry<'a>(
        &'a self,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        let calls = Arc::clone(&self.telemetry_calls);
        let result = *self.telemetry_result.lock().unwrap();
        let failure_call = *self.telemetry_failure_call.lock().unwrap();
        Box::pin(async move {
            let mut calls = calls.lock().unwrap();
            calls.push((event.clone(), received_at, topic.to_owned()));
            if failure_call == Some(calls.len()) {
                Err(PlatformStoreError::InvalidConfiguration)
            } else {
                result.map_err(|()| PlatformStoreError::InvalidConfiguration)
            }
        })
    }
}

impl GatewayIngestRepository for FakeRepository {
    fn ingest_gateway<'a>(
        &'a self,
        request: GatewayIngestRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayIngestResult, PlatformStoreError>> + Send + 'a>>
    {
        let calls = Arc::clone(&self.gateway_calls);
        let result = self.gateway_result.lock().unwrap().clone();
        Box::pin(async move {
            calls.lock().unwrap().push(request);
            result.map_err(|()| PlatformStoreError::InvalidConfiguration)
        })
    }
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 13, 10, 0, 0).unwrap() + Duration::seconds(seconds)
}

fn event(device_id: &str, gateway_device_id: Option<&str>, sequence: u64) -> TelemetryEvent {
    TelemetryEvent {
        schema_version: 1,
        device_id: device_id.to_owned(),
        boot_id: Uuid::from_u128(1),
        sequence,
        event_at: at(sequence as i64),
        measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(20.0))]),
        gateway_device_id: gateway_device_id.map(str::to_owned),
    }
}

fn telemetry_message(sequence: u64, received_at: DateTime<Utc>) -> TelemetryMessage {
    TelemetryMessage {
        topic: TOPIC.to_owned(),
        payload: Vec::new(),
        event: event("direct-1", None, sequence),
        received_at,
    }
}

fn gateway_message(
    kind: GatewayEventKind,
    child_device_id: Option<&str>,
    telemetry_event: Option<TelemetryEvent>,
    received_at: DateTime<Utc>,
    idempotency_key: &str,
) -> GatewayMessage {
    GatewayMessage {
        topic: "iot/v1/gateways/gateway-1/events".to_owned(),
        payload: Vec::new(),
        gateway_event: GatewayEvent {
            schema_version: 1,
            gateway_device_id: "gateway-1".to_owned(),
            child_device_id: child_device_id.map(str::to_owned),
            token_id: Uuid::from_u128(2),
            session_id: Some("session-1".to_owned()),
            event_kind: kind,
            event_at: received_at - Duration::seconds(1),
            payload: json!({"kind": "event"}),
            idempotency_key: idempotency_key.to_owned(),
        },
        telemetry_event,
        received_at,
    }
}

async fn stream_and_consumer(
    directory: &TempDir,
    lease_duration: StdDuration,
) -> (LocalStream, CoreStreamConsumer) {
    let stream = LocalStream::open(
        StreamConfig::sqlite(directory.path().join("stream.sqlite"))
            .with_partitions(1)
            .with_lease_duration(lease_duration),
    )
    .await
    .unwrap();
    let consumer = CoreStreamConsumer::new(Arc::new(stream.clone()), "platform-writer", "test");
    consumer.heartbeat().await.unwrap();
    (stream, consumer)
}

#[tokio::test]
async fn repository_failure_leaves_direct_batch_reclaimable() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_millis(25)).await;
    stream.append(telemetry_message(1, at(1))).await.unwrap();

    let writer = PlatformTelemetryWriter::new(FakeRepository::failing_direct(), 1);
    assert!(matches!(
        writer.flush_once(&consumer, at(2)).await,
        Err(WriterError::Platform(
            PlatformStoreError::InvalidConfiguration
        ))
    ));
    tokio::time::sleep(StdDuration::from_millis(40)).await;
    assert_eq!(consumer.claim(1).await.unwrap().records().len(), 1);
}

#[tokio::test]
async fn repository_failure_leaves_gateway_batch_reclaimable() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_millis(25)).await;
    stream
        .append(gateway_message(
            GatewayEventKind::Heartbeat,
            None,
            None,
            at(1),
            "heartbeat-1",
        ))
        .await
        .unwrap();

    let writer = PlatformTelemetryWriter::new(FakeRepository::failing_gateway(), 1);
    assert!(writer.flush_once(&consumer, at(2)).await.is_err());
    tokio::time::sleep(StdDuration::from_millis(40)).await;
    assert_eq!(consumer.claim(1).await.unwrap().records().len(), 1);
}

#[tokio::test]
async fn later_repository_failure_keeps_the_entire_batch_reclaimable() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_millis(25)).await;
    stream.append(telemetry_message(1, at(1))).await.unwrap();
    stream
        .append(gateway_message(
            GatewayEventKind::Heartbeat,
            None,
            None,
            at(2),
            "heartbeat-1",
        ))
        .await
        .unwrap();

    let writer = PlatformTelemetryWriter::new(FakeRepository::failing_gateway(), 10);
    assert!(writer.flush_once(&consumer, at(3)).await.is_err());
    tokio::time::sleep(StdDuration::from_millis(40)).await;
    assert_eq!(consumer.claim(10).await.unwrap().records().len(), 2);
}

#[tokio::test]
async fn later_direct_failure_keeps_the_entire_batch_reclaimable() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_millis(25)).await;
    stream.append(telemetry_message(1, at(1))).await.unwrap();
    stream.append(telemetry_message(2, at(2))).await.unwrap();

    let writer = PlatformTelemetryWriter::new(FakeRepository::failing_direct_on_call(2), 10);
    assert!(matches!(
        writer.flush_once(&consumer, at(3)).await,
        Err(WriterError::Platform(
            PlatformStoreError::InvalidConfiguration
        ))
    ));
    tokio::time::sleep(StdDuration::from_millis(40)).await;
    assert_eq!(consumer.claim(10).await.unwrap().records().len(), 2);
}

#[tokio::test]
async fn every_gateway_kind_maps_to_the_platform_request() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    let received_at = at(10);
    let child_event = event("child-1", Some("gateway-1"), 7);
    stream
        .append(gateway_message(
            GatewayEventKind::Connect,
            Some("child-1"),
            None,
            received_at,
            "connect-1",
        ))
        .await
        .unwrap();
    stream
        .append(gateway_message(
            GatewayEventKind::Disconnect,
            Some("child-1"),
            None,
            received_at,
            "disconnect-1",
        ))
        .await
        .unwrap();
    stream
        .append(gateway_message(
            GatewayEventKind::ChildTelemetry,
            Some("child-1"),
            Some(child_event.clone()),
            received_at,
            "child-1:7",
        ))
        .await
        .unwrap();
    stream
        .append(gateway_message(
            GatewayEventKind::Heartbeat,
            None,
            None,
            received_at,
            "heartbeat-1",
        ))
        .await
        .unwrap();

    let repository = FakeRepository {
        gateway_result: Arc::new(Mutex::new(Ok(GatewayIngestResult {
            receipt_inserted: true,
            telemetry_inserted: false,
        }))),
        ..FakeRepository::default()
    };
    let writer = PlatformTelemetryWriter::new(repository.clone(), 10);
    let result = writer.flush_once(&consumer, received_at).await.unwrap();

    assert_eq!(result.read, 4);
    assert_eq!(result.inserted, 0);
    assert_eq!(result.duplicates, 1);
    assert!(repository.telemetry_calls.lock().unwrap().is_empty());
    assert_eq!(
        repository.gateway_calls.lock().unwrap().as_slice(),
        &[
            GatewayIngestRequest {
                gateway_device_id: "gateway-1".to_owned(),
                child_device_id: Some("child-1".to_owned()),
                event_kind: GatewayIngestEventKind::Connect,
                event_at: received_at - Duration::seconds(1),
                idempotency_key: "connect-1".to_owned(),
                telemetry_event: None,
                topic: "iot/v1/gateways/gateway-1/events".to_owned(),
                received_at,
            },
            GatewayIngestRequest {
                gateway_device_id: "gateway-1".to_owned(),
                child_device_id: Some("child-1".to_owned()),
                event_kind: GatewayIngestEventKind::Disconnect,
                event_at: received_at - Duration::seconds(1),
                idempotency_key: "disconnect-1".to_owned(),
                telemetry_event: None,
                topic: "iot/v1/gateways/gateway-1/events".to_owned(),
                received_at,
            },
            GatewayIngestRequest {
                gateway_device_id: "gateway-1".to_owned(),
                child_device_id: Some("child-1".to_owned()),
                event_kind: GatewayIngestEventKind::ChildTelemetry,
                event_at: received_at - Duration::seconds(1),
                idempotency_key: "child-1:7".to_owned(),
                telemetry_event: Some(child_event),
                topic: "iot/v1/gateways/gateway-1/events".to_owned(),
                received_at,
            },
            GatewayIngestRequest {
                gateway_device_id: "gateway-1".to_owned(),
                child_device_id: None,
                event_kind: GatewayIngestEventKind::Heartbeat,
                event_at: received_at - Duration::seconds(1),
                idempotency_key: "heartbeat-1".to_owned(),
                telemetry_event: None,
                topic: "iot/v1/gateways/gateway-1/events".to_owned(),
                received_at,
            }
        ]
    );
}

#[tokio::test]
async fn gateway_telemetry_insert_is_independent_of_a_direct_duplicate() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    let received_at = at(10);
    stream
        .append(telemetry_message(1, received_at))
        .await
        .unwrap();
    stream
        .append(gateway_message(
            GatewayEventKind::ChildTelemetry,
            Some("child-1"),
            Some(event("child-1", Some("gateway-1"), 2)),
            received_at,
            "child-1:2",
        ))
        .await
        .unwrap();

    let repository = FakeRepository {
        telemetry_result: Arc::new(Mutex::new(Ok(false))),
        gateway_result: Arc::new(Mutex::new(Ok(GatewayIngestResult {
            receipt_inserted: true,
            telemetry_inserted: true,
        }))),
        ..FakeRepository::default()
    };
    let writer = PlatformTelemetryWriter::new(repository, 10);
    let result = writer.flush_once(&consumer, received_at).await.unwrap();

    assert_eq!((result.read, result.inserted, result.duplicates), (2, 1, 1));
}

async fn sqlite_store() -> (TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

#[tokio::test]
async fn platform_store_writer_persists_topology_runtime_receipt_and_rollup() {
    let (directory, store) = sqlite_store().await;
    store.register_device("direct-1").await.unwrap();
    sqlx::query("INSERT INTO devices (device_id, is_gateway) VALUES ('gateway-1', 1)")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, gateway_device_id)
         VALUES ('child-1', 'gateway-1')",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    let received_at = at(10);
    stream
        .append(telemetry_message(1, received_at))
        .await
        .unwrap();
    stream
        .append(gateway_message(
            GatewayEventKind::ChildTelemetry,
            Some("child-1"),
            Some(event("child-1", Some("gateway-1"), 2)),
            received_at,
            "child-1:2",
        ))
        .await
        .unwrap();

    let writer = PlatformTelemetryWriter::new(store.clone(), 10);
    let result = writer.flush_once(&consumer, received_at).await.unwrap();

    assert_eq!((result.read, result.inserted, result.duplicates), (2, 2, 0));
    let child = sqlx::query(
        "SELECT gateway_last_read_at, gateway_read_quality
         FROM devices WHERE device_id = 'child-1'",
    )
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        child.try_get::<String, _>("gateway_read_quality").unwrap(),
        "good"
    );
    assert_eq!(
        child.try_get::<String, _>("gateway_last_read_at").unwrap(),
        (received_at - Duration::seconds(1)).to_rfc3339()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM gateway_event_receipts
             WHERE gateway_device_id = 'gateway-1' AND idempotency_key = 'child-1:2'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        1
    );
    let aggregate = store
        .average_metric("child-1", "temperature_c", at(0), at(20))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aggregate.sample_count, 1);
    assert_eq!(aggregate.average, 20.0);
}
