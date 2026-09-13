use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_core::{CoreRuntime, CoreRuntimeConfig, CoreRuntimeError, IngestMetrics};
use iot_storage::PlatformStore;
use iot_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment,
    HeartbeatRequest, StreamError, StreamMessage, StreamPort,
};
use tempfile::TempDir;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct RecordingStream {
    claims: Arc<AtomicUsize>,
    heartbeats: Arc<AtomicUsize>,
    drains: Arc<AtomicUsize>,
    fail_claim: Arc<AtomicBool>,
    slow_drain: Arc<AtomicBool>,
}

impl RecordingStream {
    fn claims(&self) -> usize {
        self.claims.load(Ordering::SeqCst)
    }

    fn drains(&self) -> usize {
        self.drains.load(Ordering::SeqCst)
    }

    fn slow_drain(&self) {
        self.slow_drain.store(true, Ordering::SeqCst);
    }
}

impl StreamPort for RecordingStream {
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
        self.claims.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail_claim.load(Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                Err(StreamError::Draining)
            } else {
                Ok(Vec::new())
            }
        })
    }

    fn acknowledge(
        &self,
        _request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn heartbeat(
        &self,
        _request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        self.heartbeats.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(GroupAssignment {
                generation: 1,
                partitions: Vec::new(),
            })
        })
    }

    fn drain(
        &self,
        _deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        self.drains.fetch_add(1, Ordering::SeqCst);
        let slow = self.slow_drain.load(Ordering::SeqCst);
        Box::pin(async move {
            if slow {
                sleep(Duration::from_secs(1)).await;
            }
            Ok(())
        })
    }
}

async fn runtime_config(stream: Arc<dyn StreamPort>) -> (TempDir, CoreRuntimeConfig) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    (
        directory,
        CoreRuntimeConfig {
            store: Arc::new(store),
            stream,
            writer_batch_size: 10,
            alert_batch_size: 10,
            writer_group: "writer".to_owned(),
            alert_group: "alerts".to_owned(),
            writer_member_id: "runtime-test".to_owned(),
            alert_member_id: "runtime-alert".to_owned(),
            writer_interval: Duration::from_millis(10),
            event_alert_interval: Duration::from_millis(10),
            window_alert_interval: Duration::from_millis(10),
            writer_heartbeat_interval: Duration::from_millis(10),
            alert_heartbeat_interval: Duration::from_millis(10),
            cancellation: CancellationToken::new(),
            metrics: Arc::new(IngestMetrics::default()),
        },
    )
}

#[tokio::test]
async fn invalid_config_rejects_before_any_worker_claims_or_starts() {
    let stream = RecordingStream::default();
    let (_directory, mut config) = runtime_config(Arc::new(stream.clone())).await;
    config.writer_batch_size = 0;

    let result = CoreRuntime::start(config).await;

    assert!(matches!(result, Err(CoreRuntimeError::Configuration(_))));
    assert_eq!(stream.claims(), 0);
    assert_eq!(stream.drains(), 0);
}

#[tokio::test]
async fn all_startup_barriers_are_required_before_ready() {
    let stream = RecordingStream::default();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;

    let runtime = CoreRuntime::start(config).await.unwrap();

    assert!(runtime.ready());
    assert!(stream.heartbeats.load(Ordering::SeqCst) >= 2);
    runtime.stop_claiming();
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn stop_prevents_later_claims_and_drain_is_called_once() {
    let stream = RecordingStream::default();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let runtime = CoreRuntime::start(config).await.unwrap();
    sleep(Duration::from_millis(25)).await;
    runtime.stop_claiming();
    let claims = stream.claims();

    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(stream.claims(), claims);
    assert_eq!(stream.drains(), 1);
}

#[tokio::test]
async fn fatal_worker_failure_is_returned_by_join_and_cancels_siblings() {
    let stream = RecordingStream::default();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let runtime = CoreRuntime::start(config).await.unwrap();
    stream.fail_claim.store(true, Ordering::SeqCst);

    let result = runtime.join().await;

    assert!(matches!(result, Err(CoreRuntimeError::Worker(_))));
}

#[tokio::test]
async fn drain_respects_deadline_when_stream_drain_does_not_return() {
    let stream = RecordingStream::default();
    stream.slow_drain();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let runtime = CoreRuntime::start(config).await.unwrap();

    let result = runtime
        .drain(Instant::now() + Duration::from_millis(20))
        .await;

    assert!(matches!(result, Err(CoreRuntimeError::Deadline)));
    assert_eq!(stream.drains(), 1);
}

#[test]
fn runtime_source_has_no_internal_transport_configuration() {
    let source = include_str!("../src/runtime.rs");
    for forbidden in ["http://", "https://", "secret", "reqwest", "hyper"] {
        assert!(!source.contains(forbidden), "runtime contains {forbidden}");
    }
}
