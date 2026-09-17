use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{CoreSqliteStore, CoreStreamConsumer, SqliteTelemetryWriter};
use iot_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment,
    HeartbeatRequest, LocalStream, PartitionId, StreamConfig, StreamError, StreamMessage,
    StreamPort, TelemetryMessage,
};
use serde_json::json;

const TEST_TENANT_ID: uuid::Uuid = uuid::Uuid::from_u128(1);

#[derive(Clone)]
struct CommitCheckingStream {
    records: Arc<Vec<ClaimedRecord>>,
    telemetry_pool: sqlx::SqlitePool,
    acknowledged: Arc<tokio::sync::Mutex<bool>>,
}

impl CommitCheckingStream {
    fn new(record: ClaimedRecord, telemetry_pool: sqlx::SqlitePool) -> Self {
        Self {
            records: Arc::new(vec![record]),
            telemetry_pool,
            acknowledged: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }

    async fn was_acknowledged(&self) -> bool {
        *self.acknowledged.lock().await
    }
}

impl StreamPort for CommitCheckingStream {
    fn append(
        &self,
        _message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        Box::pin(async { Err(StreamError::InvalidConfig("append is not used".to_owned())) })
    }

    fn claim(
        &self,
        _request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        let records = Arc::clone(&self.records);
        Box::pin(async move { Ok(records.as_ref().clone()) })
    }

    fn acknowledge(
        &self,
        _request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        let telemetry_pool = self.telemetry_pool.clone();
        let acknowledged = Arc::clone(&self.acknowledged);
        Box::pin(async move {
            let rows = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM telemetry")
                .fetch_one(&telemetry_pool)
                .await
                .map_err(|error| StreamError::CorruptStore(error.to_string()))?;
            if rows != 1 {
                return Err(StreamError::CorruptStore(
                    "telemetry must commit before stream acknowledgement".to_owned(),
                ));
            }
            *acknowledged.lock().await = true;
            Ok(())
        })
    }

    fn heartbeat(
        &self,
        _request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        Box::pin(async {
            Ok(GroupAssignment {
                generation: 1,
                partitions: vec![PartitionId::new(0)],
            })
        })
    }

    fn drain(
        &self,
        _deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn sqlite_writer_commits_before_acknowledging_the_claim() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "esp-000123".to_owned(),
        boot_id: uuid::Uuid::new_v4(),
        sequence: 1,
        event_at: now,
        measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
        gateway_device_id: None,
    };
    let stream = CommitCheckingStream::new(
        ClaimedRecord {
            partition: PartitionId::new(0),
            offset: 0,
            message: TelemetryMessage {
                tenant_id: TEST_TENANT_ID,
                topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
                payload: br#"{"sequence":1}"#.to_vec(),
                event,
                received_at: now,
            }
            .into(),
            generation: 1,
        },
        store.pool().clone(),
    );
    let consumer = CoreStreamConsumer::new(Arc::new(stream.clone()), "writer", "writer-test");

    let result = SqliteTelemetryWriter::new(store, 10)
        .flush_once(&consumer, now)
        .await
        .unwrap();

    assert_eq!(result.inserted, 1);
    assert_eq!(result.committed_partitions, 1);
    assert!(stream.was_acknowledged().await);
}

#[tokio::test]
async fn heartbeat_renews_an_active_inflight_claim_before_acknowledgement() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(
        StreamConfig::sqlite(directory.path().join("stream.sqlite"))
            .with_partitions(1)
            .with_lease_duration(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "esp-000123".to_owned(),
        boot_id: uuid::Uuid::new_v4(),
        sequence: 1,
        event_at: now,
        measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
        gateway_device_id: None,
    };
    stream
        .append(TelemetryMessage {
            tenant_id: TEST_TENANT_ID,
            topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
            payload: serde_json::to_vec(&event).unwrap(),
            event,
            received_at: now,
        })
        .await
        .unwrap();

    let consumer = CoreStreamConsumer::new(Arc::new(stream), "writer", "writer-test");
    let batch = consumer.claim(1).await.unwrap();
    assert_eq!(batch.records().len(), 1);

    tokio::time::sleep(Duration::from_millis(150)).await;
    consumer.heartbeat().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    consumer.acknowledge(&batch).await.unwrap();
}
