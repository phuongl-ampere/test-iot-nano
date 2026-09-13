use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{AlertError, CoreStreamConsumer, PlatformAlertEvaluator};
use iot_storage::{
    AlertEvaluationEvent, AlertEvaluationRepository, AlertEvaluationResult, PlatformStore,
    PlatformStoreError,
};
use iot_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GatewayEvent, GatewayEventKind,
    GatewayMessage, GroupAssignment, HeartbeatRequest, LocalStream, StreamConfig, StreamError,
    StreamMessage, StreamPort, TelemetryMessage,
};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

const DEVICE_ID: &str = "esp-000123";
const TOPIC: &str = "iot/v1/devices/esp-000123/telemetry";

#[derive(Clone)]
struct FakeRepository {
    events: Arc<Mutex<Vec<AlertEvaluationEvent>>>,
    event_result: Arc<Mutex<Result<AlertEvaluationResult, String>>>,
    event_calls: Arc<Mutex<usize>>,
    event_acknowledgement_counts: Arc<Mutex<Vec<usize>>>,
    acknowledgements: Arc<Mutex<usize>>,
    window_result: AlertEvaluationResult,
    windows_called: Arc<Mutex<usize>>,
}

impl FakeRepository {
    fn successful(result: AlertEvaluationResult) -> Self {
        Self {
            events: Arc::default(),
            event_result: Arc::new(Mutex::new(Ok(result))),
            event_calls: Arc::default(),
            event_acknowledgement_counts: Arc::default(),
            acknowledgements: Arc::default(),
            window_result: AlertEvaluationResult::default(),
            windows_called: Arc::default(),
        }
    }

    fn failing() -> Self {
        Self {
            events: Arc::default(),
            event_result: Arc::new(Mutex::new(Err("repository failed".to_owned()))),
            event_calls: Arc::default(),
            event_acknowledgement_counts: Arc::default(),
            acknowledgements: Arc::default(),
            window_result: AlertEvaluationResult::default(),
            windows_called: Arc::default(),
        }
    }

    fn observing_acknowledgements(mut self, acknowledgements: Arc<Mutex<usize>>) -> Self {
        self.acknowledgements = acknowledgements;
        self
    }
}

impl AlertEvaluationRepository for FakeRepository {
    fn evaluate_alert_events<'a>(
        &'a self,
        events: &'a [AlertEvaluationEvent],
        _evaluated_at: DateTime<Utc>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<AlertEvaluationResult, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        let captured = Arc::clone(&self.events);
        let event_calls = Arc::clone(&self.event_calls);
        let event_acknowledgement_counts = Arc::clone(&self.event_acknowledgement_counts);
        let acknowledgements = Arc::clone(&self.acknowledgements);
        let result = self.event_result.lock().unwrap().clone();
        Box::pin(async move {
            *event_calls.lock().unwrap() += 1;
            event_acknowledgement_counts
                .lock()
                .unwrap()
                .push(*acknowledgements.lock().unwrap());
            captured.lock().unwrap().extend_from_slice(events);
            result.map_err(|_| PlatformStoreError::InvalidConfiguration)
        })
    }

    fn evaluate_alert_windows<'a>(
        &'a self,
        _evaluated_at: DateTime<Utc>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<AlertEvaluationResult, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        let called = Arc::clone(&self.windows_called);
        let result = self.window_result;
        Box::pin(async move {
            *called.lock().unwrap() += 1;
            Ok(result)
        })
    }
}

#[derive(Clone)]
struct AcknowledgementCountingStream {
    stream: LocalStream,
    acknowledgements: Arc<Mutex<usize>>,
}

impl AcknowledgementCountingStream {
    fn new(stream: LocalStream) -> Self {
        Self {
            stream,
            acknowledgements: Arc::default(),
        }
    }

    fn acknowledgements(&self) -> Arc<Mutex<usize>> {
        Arc::clone(&self.acknowledgements)
    }
}

impl StreamPort for AcknowledgementCountingStream {
    fn append(
        &self,
        message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        let stream = self.stream.clone();
        Box::pin(async move { StreamPort::append(&stream, message).await })
    }

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        let stream = self.stream.clone();
        Box::pin(async move { StreamPort::claim(&stream, request).await })
    }

    fn acknowledge(
        &self,
        request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        let stream = self.stream.clone();
        let acknowledgements = Arc::clone(&self.acknowledgements);
        Box::pin(async move {
            StreamPort::acknowledge(&stream, request).await?;
            *acknowledgements.lock().unwrap() += 1;
            Ok(())
        })
    }

    fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        let stream = self.stream.clone();
        Box::pin(async move { StreamPort::heartbeat(&stream, request).await })
    }

    fn drain(
        &self,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        let stream = self.stream.clone();
        Box::pin(async move { StreamPort::drain(&stream, deadline).await })
    }
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 13, 10, 0, 0).unwrap() + Duration::seconds(seconds)
}

fn telemetry(value: f64, received_at: DateTime<Utc>) -> TelemetryMessage {
    TelemetryMessage {
        topic: TOPIC.to_owned(),
        payload: Vec::new(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: DEVICE_ID.to_owned(),
            boot_id: Uuid::from_u128(1),
            sequence: 7,
            event_at: received_at - Duration::seconds(1),
            measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(value))]),
            gateway_device_id: None,
        },
        received_at,
    }
}

fn gateway_heartbeat(received_at: DateTime<Utc>) -> GatewayMessage {
    GatewayMessage {
        topic: "iot/v1/gateways/gateway-1/events".to_owned(),
        payload: Vec::new(),
        gateway_event: GatewayEvent {
            schema_version: 1,
            gateway_device_id: "gateway-1".to_owned(),
            child_device_id: None,
            token_id: Uuid::from_u128(2),
            session_id: None,
            event_kind: GatewayEventKind::Heartbeat,
            event_at: received_at,
            payload: json!({}),
            idempotency_key: "heartbeat-1".to_owned(),
        },
        telemetry_event: None,
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
    let consumer = CoreStreamConsumer::new(Arc::new(stream.clone()), "platform-alert", "test");
    consumer.heartbeat().await.unwrap();
    (stream, consumer)
}

async fn counted_stream_and_consumer(
    directory: &TempDir,
    lease_duration: StdDuration,
) -> (
    LocalStream,
    AcknowledgementCountingStream,
    CoreStreamConsumer,
) {
    let stream = LocalStream::open(
        StreamConfig::sqlite(directory.path().join("stream.sqlite"))
            .with_partitions(1)
            .with_lease_duration(lease_duration),
    )
    .await
    .unwrap();
    let counted_stream = AcknowledgementCountingStream::new(stream.clone());
    let consumer = CoreStreamConsumer::new(
        Arc::new(counted_stream.clone()),
        "platform-alert",
        "counting-test",
    );
    consumer.heartbeat().await.unwrap();
    (stream, counted_stream, consumer)
}

#[tokio::test]
async fn event_flush_maps_events_and_acknowledges_after_evaluation() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, counted_stream, consumer) =
        counted_stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    stream.append(telemetry(41.0, at(1))).await.unwrap();
    stream.append(gateway_heartbeat(at(2))).await.unwrap();
    let repository = FakeRepository::successful(AlertEvaluationResult {
        evaluated: 1,
        opened: 1,
        resolved: 2,
        reminders: 3,
    })
    .observing_acknowledgements(counted_stream.acknowledgements());
    let events = Arc::clone(&repository.events);
    let event_calls = Arc::clone(&repository.event_calls);
    let event_acknowledgement_counts = Arc::clone(&repository.event_acknowledgement_counts);

    let result = PlatformAlertEvaluator::new(repository, 10)
        .flush_event_rules(&consumer, at(3))
        .await
        .unwrap();

    assert_eq!(result.read, 2);
    assert_eq!(result.evaluated, 1);
    assert_eq!(result.opened, 1);
    assert_eq!(result.resolved, 2);
    assert_eq!(result.reminders, 3);
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_at, at(0));
    assert_eq!(events[0].received_at, at(1));
    assert_eq!(events[0].device_id, DEVICE_ID);
    assert_eq!(events[0].sequence, 7);
    assert_eq!(events[0].measurements["temperature_c"], json!(41.0));
    assert_eq!(*event_calls.lock().unwrap(), 1);
    assert_eq!(*event_acknowledgement_counts.lock().unwrap(), vec![0]);
    assert_eq!(*counted_stream.acknowledgements().lock().unwrap(), 1);
    assert!(consumer.claim(10).await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_event_evaluation_leaves_claim_reclaimable() {
    let directory = tempfile::tempdir().unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_millis(50)).await;
    stream.append(telemetry(41.0, at(1))).await.unwrap();

    let error = PlatformAlertEvaluator::new(FakeRepository::failing(), 1)
        .flush_event_rules(&consumer, at(2))
        .await
        .unwrap_err();
    assert!(matches!(error, AlertError::Store(_)));

    tokio::time::sleep(StdDuration::from_millis(75)).await;
    assert_eq!(consumer.claim(1).await.unwrap().records().len(), 1);
}

#[tokio::test]
async fn empty_event_batch_does_not_call_repository_or_acknowledge() {
    let directory = tempfile::tempdir().unwrap();
    let (_stream, consumer) = stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    let repository = FakeRepository::successful(AlertEvaluationResult {
        evaluated: 9,
        ..AlertEvaluationResult::default()
    });

    let result = PlatformAlertEvaluator::new(repository, 0)
        .flush_event_rules(&consumer, at(0))
        .await
        .unwrap();

    assert_eq!(result.read, 0);
    assert_eq!(result.evaluated, 0);
}

#[tokio::test]
async fn window_flush_delegates_once_without_stream_access() {
    let directory = tempfile::tempdir().unwrap();
    let (_stream, consumer) = stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    let repository = FakeRepository {
        window_result: AlertEvaluationResult {
            evaluated: 4,
            opened: 2,
            resolved: 1,
            reminders: 3,
        },
        ..FakeRepository::successful(AlertEvaluationResult::default())
    };
    let windows_called = Arc::clone(&repository.windows_called);
    let event_calls = Arc::clone(&repository.event_calls);

    let result = PlatformAlertEvaluator::new(repository, 1)
        .flush_window_rules(at(4))
        .await
        .unwrap();

    assert_eq!(
        result,
        iot_nano_core::AlertFlushResult {
            read: 0,
            evaluated: 4,
            opened: 2,
            resolved: 1,
            reminders: 3,
        }
    );
    assert_eq!(*windows_called.lock().unwrap(), 1);
    assert_eq!(*event_calls.lock().unwrap(), 0);
    assert!(consumer.claim(1).await.unwrap().is_empty());
}

#[tokio::test]
async fn platform_store_event_rule_opens_incident() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES (?)")
        .bind(DEVICE_ID)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO alert_rules
         (id, name, metric_key, rule_type, comparison, threshold, for_seconds,
          resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds)
         VALUES (?, 'High temperature', 'temperature_c', 'event_threshold', 'gt', 40, 0, 0, 3600,
                 'warning', 86400)",
    )
    .bind(Uuid::from_u128(3).to_string())
    .execute(pool)
    .await
    .unwrap();
    let (stream, consumer) = stream_and_consumer(&directory, StdDuration::from_secs(1)).await;
    stream.append(telemetry(41.0, at(1))).await.unwrap();

    let result = PlatformAlertEvaluator::new(store.clone(), 10)
        .flush_event_rules(&consumer, at(2))
        .await
        .unwrap();

    assert_eq!(result.opened, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents WHERE status = 'open'")
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
}
