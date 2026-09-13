use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use iot_core::RpcMode;
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::{
    CommandTransport, CommandTransportError, CoreRuntime, CoreRuntimeConfig, CoreRuntimeError,
    CoreRuntimeWorkerError, CoreStreamConsumer, EmailSender, IngestMetrics, NotificationError,
    PlatformTelemetryWriter, TransportRpcPublishRequest, WriterError,
};
use iot_storage::{NewCommandOutboxEntry as PlatformCommand, PlatformStore};
use iot_stream::{
    AcknowledgeRequest, AppendReceipt, ClaimRequest, ClaimedRecord, GroupAssignment,
    HeartbeatRequest, LocalStream, PartitionId, StreamConfig, StreamError, StreamMessage,
    StreamPort, TelemetryMessage,
};
use serde_json::{Map, json};
use sqlx::Row;
use tempfile::TempDir;
use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
struct UnavailableTransport;

impl CommandTransport for UnavailableTransport {
    fn publish(
        &self,
        _request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(async {
            Err(CommandTransportError::Unavailable(
                "broker unavailable".to_owned(),
            ))
        })
    }
}

#[derive(Clone)]
struct FailingEmailSender;

impl EmailSender for FailingEmailSender {
    fn send(
        &self,
        _subject: String,
        _body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        Box::pin(async { Err(NotificationError::Send("email delivery failed".to_owned())) })
    }
}

struct ActiveClaim {
    active_claims: Arc<AtomicUsize>,
}

impl ActiveClaim {
    fn new(active_claims: Arc<AtomicUsize>) -> Self {
        active_claims.fetch_add(1, Ordering::SeqCst);
        Self { active_claims }
    }
}

impl Drop for ActiveClaim {
    fn drop(&mut self) {
        self.active_claims.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct AckCountingStream {
    inner: LocalStream,
    acknowledgements: Arc<AtomicUsize>,
}

impl AckCountingStream {
    fn new(inner: LocalStream) -> Self {
        Self {
            inner,
            acknowledgements: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn acknowledgements(&self) -> usize {
        self.acknowledgements.load(Ordering::SeqCst)
    }
}

impl StreamPort for AckCountingStream {
    fn append(
        &self,
        message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        Box::pin(self.inner.append(message))
    }

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        Box::pin(self.inner.claim(request))
    }

    fn acknowledge(
        &self,
        request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        let acknowledgements = Arc::clone(&self.acknowledgements);
        Box::pin(async move {
            let result = self.inner.acknowledge(request).await;
            if result.is_ok() {
                acknowledgements.fetch_add(1, Ordering::SeqCst);
            }
            result
        })
    }

    fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        Box::pin(self.inner.heartbeat(request))
    }

    fn drain(
        &self,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(self.inner.drain_until(deadline))
    }
}

#[derive(Clone, Default)]
struct RecordingStream {
    claims: Arc<AtomicUsize>,
    heartbeats: Arc<AtomicUsize>,
    drains: Arc<AtomicUsize>,
    stop_claiming_calls: Arc<AtomicUsize>,
    fail_claim: Arc<AtomicBool>,
    fail_writer_claim: Arc<AtomicBool>,
    panic_writer_claim: Arc<AtomicBool>,
    fail_heartbeat: Arc<AtomicBool>,
    block_claims: Arc<AtomicBool>,
    claim_started: Arc<Notify>,
    active_claims: Arc<AtomicUsize>,
    writer_records: Arc<Mutex<Vec<ClaimedRecord>>>,
    acknowledgements: Arc<AtomicUsize>,
    claim_members: Arc<Mutex<Vec<String>>>,
    heartbeat_members: Arc<Mutex<Vec<String>>>,
    heartbeat_requires_claim: Arc<AtomicBool>,
    slow_drain: Arc<AtomicBool>,
    drain_timeout: Arc<AtomicBool>,
    block_drain: Arc<AtomicBool>,
    drain_started: Arc<Notify>,
    drain_release: Arc<Notify>,
}

impl RecordingStream {
    fn claims(&self) -> usize {
        self.claims.load(Ordering::SeqCst)
    }

    fn drains(&self) -> usize {
        self.drains.load(Ordering::SeqCst)
    }

    fn stop_claiming_calls(&self) -> usize {
        self.stop_claiming_calls.load(Ordering::SeqCst)
    }

    fn slow_drain(&self) {
        self.slow_drain.store(true, Ordering::SeqCst);
    }

    fn drain_times_out(&self) {
        self.drain_timeout.store(true, Ordering::SeqCst);
    }

    fn block_claims(&self) {
        self.block_claims.store(true, Ordering::SeqCst);
    }

    async fn wait_for_claim_start(&self) {
        while self.active_claims() == 0 {
            self.claim_started.notified().await;
        }
    }

    fn active_claims(&self) -> usize {
        self.active_claims.load(Ordering::SeqCst)
    }

    fn fail_writer_claims(&self) {
        self.fail_writer_claim.store(true, Ordering::SeqCst);
    }

    fn panic_writer_claims(&self) {
        self.panic_writer_claim.store(true, Ordering::SeqCst);
    }

    fn fail_heartbeats(&self) {
        self.fail_heartbeat.store(true, Ordering::SeqCst);
    }

    fn set_writer_records(&self, records: Vec<ClaimedRecord>) {
        *self
            .writer_records
            .lock()
            .expect("writer records mutex poisoned") = records;
    }

    fn require_claim_on_heartbeat(&self) {
        self.heartbeat_requires_claim.store(true, Ordering::SeqCst);
    }

    fn claims_for(&self, member_id: &str) -> usize {
        self.claim_members
            .lock()
            .expect("claim members mutex poisoned")
            .iter()
            .filter(|member| member.as_str() == member_id)
            .count()
    }

    fn heartbeats_for(&self, member_id: &str) -> usize {
        self.heartbeat_members
            .lock()
            .expect("heartbeat members mutex poisoned")
            .iter()
            .filter(|member| member.as_str() == member_id)
            .count()
    }

    fn block_drain(&self) {
        self.block_drain.store(true, Ordering::SeqCst);
    }

    async fn wait_for_drain_start(&self) {
        while self.drains() == 0 {
            self.drain_started.notified().await;
        }
    }

    fn release_drain(&self) {
        self.drain_release.notify_one();
    }
}

impl StreamPort for RecordingStream {
    fn stop_claiming(&self) {
        self.stop_claiming_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn append(
        &self,
        _message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        Box::pin(async { Err(StreamError::InvalidConfig("append is not used".to_owned())) })
    }

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        self.claims.fetch_add(1, Ordering::SeqCst);
        self.claim_members
            .lock()
            .expect("claim members mutex poisoned")
            .push(request.member_id.clone());
        if self.stop_claiming_calls.load(Ordering::SeqCst) > 0 {
            return Box::pin(async { Err(StreamError::Draining) });
        }
        if request.group == "writer" && self.panic_writer_claim.load(Ordering::SeqCst) {
            return Box::pin(async move { panic!("writer claim panic") });
        }
        if self.block_claims.load(Ordering::SeqCst) {
            let started = Arc::clone(&self.claim_started);
            let active_claims = Arc::clone(&self.active_claims);
            return Box::pin(async move {
                let _active_claim = ActiveClaim::new(active_claims);
                started.notify_one();
                std::future::pending::<()>().await;
                unreachable!()
            });
        }
        let fail = self.fail_claim.load(Ordering::SeqCst)
            || (request.group == "writer" && self.fail_writer_claim.load(Ordering::SeqCst));
        let records = if request.group == "writer" {
            self.writer_records
                .lock()
                .expect("writer records mutex poisoned")
                .clone()
        } else {
            Vec::new()
        };
        Box::pin(async move {
            if fail {
                Err(StreamError::Draining)
            } else {
                Ok(records)
            }
        })
    }

    fn acknowledge(
        &self,
        _request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        self.acknowledgements.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }

    fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        self.heartbeats.fetch_add(1, Ordering::SeqCst);
        self.heartbeat_members
            .lock()
            .expect("heartbeat members mutex poisoned")
            .push(request.member_id.clone());
        let require_claim = self.heartbeat_requires_claim.load(Ordering::SeqCst);
        let fail = self.fail_heartbeat.load(Ordering::SeqCst);
        Box::pin(async move {
            if require_claim {
                Err(StreamError::GroupMemberNotFound {
                    group: request.group,
                    member_id: request.member_id,
                })
            } else if fail {
                Err(StreamError::Draining)
            } else {
                Ok(GroupAssignment {
                    generation: 1,
                    partitions: Vec::new(),
                })
            }
        })
    }

    fn drain(
        &self,
        _deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        self.drains.fetch_add(1, Ordering::SeqCst);
        let slow = self.slow_drain.load(Ordering::SeqCst);
        let drain_timeout = self.drain_timeout.load(Ordering::SeqCst);
        let block = self.block_drain.load(Ordering::SeqCst);
        let started = Arc::clone(&self.drain_started);
        let release = Arc::clone(&self.drain_release);
        Box::pin(async move {
            if drain_timeout {
                return Err(StreamError::DrainTimeout { remaining: 0 });
            }
            if block {
                started.notify_one();
                release.notified().await;
            }
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
            command_transport: Arc::new(UnavailableTransport),
            email_sender: Arc::new(FailingEmailSender),
            writer_batch_size: 10,
            alert_batch_size: 10,
            command_batch_size: 10,
            notification_batch_size: 10,
            writer_group: "writer".to_owned(),
            alert_group: "alerts".to_owned(),
            writer_member_id: "runtime-test".to_owned(),
            alert_member_id: "runtime-alert".to_owned(),
            writer_interval: Duration::from_millis(10),
            event_alert_interval: Duration::from_millis(10),
            window_alert_interval: Duration::from_millis(10),
            command_interval: Duration::from_millis(10),
            notification_interval: Duration::from_millis(10),
            writer_heartbeat_interval: Duration::from_millis(10),
            alert_heartbeat_interval: Duration::from_millis(10),
            notification_send_timeout: Duration::from_millis(100),
            notification_lease_duration: chrono::Duration::seconds(30),
            notification_retry_base: chrono::Duration::seconds(1),
            notification_retry_max: chrono::Duration::seconds(60),
            cancellation: CancellationToken::new(),
            metrics: Arc::new(IngestMetrics::default()),
        },
    )
}

async fn seed_platform_notification(store: &PlatformStore, now: chrono::DateTime<Utc>) -> Uuid {
    let rule_id = Uuid::new_v4();
    let incident_id = Uuid::new_v4();
    let notification_id = Uuid::new_v4();
    let pool = store.sqlite_pool().expect("runtime test uses SQLite");
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, window_seconds,
            for_seconds, resolve_after_seconds, reopen_grace_seconds, severity,
            reminder_interval_seconds
         ) VALUES (?, 'Runtime rule', 'temperature_c', 'event_threshold', 'gt', 40, 0,
                   0, 0, 3600, 'warning', 86400)",
    )
    .bind(rule_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, state_version
         ) VALUES (?, ?, 'runtime-device', 'open', ?, ?, 1)",
    )
    .bind(incident_id.to_string())
    .bind(rule_id.to_string())
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES (?, ?, 'opened', ?, 'subject', 'body', ?)",
    )
    .bind(notification_id.to_string())
    .bind(incident_id.to_string())
    .bind(format!("runtime-{notification_id}"))
    .bind(now.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();
    notification_id
}

#[tokio::test]
async fn command_worker_keeps_runtime_live_and_releases_unavailable_command() {
    let stream = RecordingStream::default();
    let (_directory, config) = runtime_config(Arc::new(stream)).await;
    config
        .store
        .register_device("runtime-device")
        .await
        .unwrap();
    let command_id = Uuid::now_v7();
    let now = Utc::now();
    let original_next_attempt_at = now;
    config
        .store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            device_id: "runtime-device".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + chrono::Duration::minutes(5),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let pool = config.store.sqlite_pool().unwrap().clone();

    let runtime = CoreRuntime::start(config).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    assert!(runtime.ready());
    let row =
        sqlx::query("SELECT state, last_error, next_attempt_at FROM command_outbox WHERE id = ?")
            .bind(command_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        row.get::<String, _>("state"),
        "queued",
        "{}",
        row.get::<Option<String>, _>("last_error")
            .unwrap_or_default()
    );
    let last_error = row.get::<Option<String>, _>("last_error");
    assert!(!last_error.as_deref().unwrap_or_default().is_empty());
    let next_attempt_at =
        chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("next_attempt_at"))
            .unwrap()
            .with_timezone(&Utc);
    assert!(next_attempt_at > original_next_attempt_at);
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn notification_worker_keeps_runtime_live_and_releases_send_failure() {
    let stream = RecordingStream::default();
    let (_directory, config) = runtime_config(Arc::new(stream)).await;
    config
        .store
        .register_device("runtime-device")
        .await
        .unwrap();
    let original_next_attempt_at = Utc::now();
    let notification_id = seed_platform_notification(&config.store, original_next_attempt_at).await;
    let metrics = Arc::clone(&config.metrics);
    let pool = config.store.sqlite_pool().unwrap().clone();

    let runtime = CoreRuntime::start(config).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    assert!(runtime.ready());
    let row = sqlx::query(
        "SELECT state, last_error, next_attempt_at FROM notification_outbox WHERE id = ?",
    )
    .bind(notification_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "pending");
    let last_error = row.get::<Option<String>, _>("last_error");
    assert!(!last_error.as_deref().unwrap_or_default().is_empty());
    let next_attempt_at =
        chrono::DateTime::parse_from_rfc3339(&row.get::<String, _>("next_attempt_at"))
            .unwrap()
            .with_timezone(&Utc);
    assert!(next_attempt_at > original_next_attempt_at);
    assert!(
        metrics
            .render_prometheus()
            .contains("iot_ingest_notification_failures_total 1\n")
    );
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn command_repository_failure_is_returned_by_join_and_records_database_failure() {
    let stream = RecordingStream::default();
    let (_directory, mut config) = runtime_config(Arc::new(stream)).await;
    config.writer_interval = Duration::from_secs(60);
    config.event_alert_interval = Duration::from_secs(60);
    config.window_alert_interval = Duration::from_secs(60);
    config.command_interval = Duration::from_millis(10);
    config.notification_interval = Duration::from_secs(60);
    let metrics = Arc::clone(&config.metrics);
    let pool = config.store.sqlite_pool().unwrap().clone();

    let runtime = CoreRuntime::start(config).await.unwrap();
    assert_eq!(runtime.startup_barrier_count(), 5);
    sqlx::query("DROP TABLE command_outbox")
        .execute(&pool)
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), runtime.join()).await;

    assert!(matches!(
        result,
        Ok(Err(CoreRuntimeError::Worker(
            CoreRuntimeWorkerError::Failed {
                worker: "commands",
                ..
            }
        )))
    ));
    assert!(
        metrics
            .render_prometheus()
            .contains("iot_ingest_database_failures_total 1\n")
    );
}

#[tokio::test]
async fn notification_repository_failure_is_returned_by_join_and_records_database_failure() {
    let stream = RecordingStream::default();
    let (_directory, mut config) = runtime_config(Arc::new(stream)).await;
    config.writer_interval = Duration::from_secs(60);
    config.event_alert_interval = Duration::from_secs(60);
    config.window_alert_interval = Duration::from_secs(60);
    config.command_interval = Duration::from_secs(60);
    config.notification_interval = Duration::from_millis(10);
    let metrics = Arc::clone(&config.metrics);
    let pool = config.store.sqlite_pool().unwrap().clone();

    let runtime = CoreRuntime::start(config).await.unwrap();
    assert_eq!(runtime.startup_barrier_count(), 5);
    sqlx::query("DROP TABLE notification_outbox")
        .execute(&pool)
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), runtime.join()).await;

    assert!(matches!(
        result,
        Ok(Err(CoreRuntimeError::Worker(
            CoreRuntimeWorkerError::Failed {
                worker: "notifications",
                ..
            }
        )))
    ));
    assert!(
        metrics
            .render_prometheus()
            .contains("iot_ingest_database_failures_total 1\n")
    );
}

fn telemetry_record(device_id: &str) -> ClaimedRecord {
    let now = Utc::now();
    ClaimedRecord {
        partition: PartitionId::new(0),
        offset: 0,
        message: StreamMessage::Telemetry(TelemetryMessage {
            topic: format!("iot/v1/devices/{device_id}/telemetry"),
            payload: b"{}".to_vec(),
            event: TelemetryEvent {
                schema_version: 1,
                device_id: device_id.to_owned(),
                boot_id: Uuid::nil(),
                sequence: 1,
                event_at: now,
                measurements: Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
                gateway_device_id: None,
            },
            received_at: now,
        }),
        generation: 1,
    }
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
    assert_eq!(runtime.startup_barrier_count(), 5);
    assert!(stream.heartbeats.load(Ordering::SeqCst) >= 2);
    runtime.stop_claiming();
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn startup_barrier_failure_returns_a_worker_error_without_hanging() {
    let stream = RecordingStream::default();
    stream.fail_heartbeats();
    let (_directory, config) = runtime_config(Arc::new(stream)).await;

    let result = timeout(Duration::from_secs(1), CoreRuntime::start(config)).await;

    assert!(matches!(result, Ok(Err(CoreRuntimeError::Worker(_)))));
}

#[tokio::test]
async fn parent_cancellation_stops_workers_and_allows_normal_join() {
    let stream = RecordingStream::default();
    let (_directory, mut config) = runtime_config(Arc::new(stream)).await;
    let parent_cancellation = CancellationToken::new();
    config.cancellation = parent_cancellation.clone();
    let runtime = CoreRuntime::start(config).await.unwrap();

    parent_cancellation.cancel();

    assert!(matches!(
        timeout(Duration::from_secs(1), runtime.join()).await,
        Ok(Ok(()))
    ));
}

#[tokio::test]
async fn runtime_stop_claiming_closes_the_shared_stream_boundary() {
    let stream = RecordingStream::default();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let runtime = CoreRuntime::start(config).await.unwrap();

    runtime.stop_claiming();

    assert_eq!(stream.stop_claiming_calls(), 1);
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
    let claim_after_stop = stream
        .claim(ClaimRequest::new("writer", "runtime-test"))
        .await;
    let claims_after_stop = stream.claims();

    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(claims_after_stop, claims + 1);
    assert_eq!(stream.claims(), claims_after_stop);
    assert!(matches!(claim_after_stop, Err(StreamError::Draining)));
    assert_eq!(stream.drains(), 1);
}

#[tokio::test]
async fn concurrent_drains_wait_for_one_stream_drain_before_cancellation_and_join() {
    let stream = RecordingStream::default();
    stream.block_drain();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let runtime = Arc::new(CoreRuntime::start(config).await.unwrap());

    let first_runtime = Arc::clone(&runtime);
    let first = tokio::spawn(async move {
        first_runtime
            .drain(Instant::now() + Duration::from_secs(1))
            .await
    });
    stream.wait_for_drain_start().await;

    let second_runtime = Arc::clone(&runtime);
    let mut second = tokio::spawn(async move {
        second_runtime
            .drain(Instant::now() + Duration::from_secs(1))
            .await
    });

    assert!(
        timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "the second drain must wait for the first stream drain"
    );
    assert!(
        runtime.ready(),
        "workers must not be cancelled before drain completes"
    );

    stream.release_drain();

    assert!(matches!(first.await, Ok(Ok(()))));
    assert!(matches!(second.await, Ok(Ok(()))));
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
async fn join_reports_a_panicked_worker_and_cancels_its_siblings() {
    let stream = RecordingStream::default();
    stream.panic_writer_claims();
    let (_directory, config) = runtime_config(Arc::new(stream)).await;
    let runtime = CoreRuntime::start(config).await.unwrap();

    let result = timeout(Duration::from_millis(100), runtime.join()).await;

    assert!(matches!(
        result,
        Ok(Err(CoreRuntimeError::Worker(
            CoreRuntimeWorkerError::Join {
                worker: "writer",
                ..
            }
        )))
    ));
}

#[tokio::test]
async fn writer_error_metrics_preserve_stream_and_platform_taxonomy() {
    let stream = RecordingStream::default();
    stream.fail_writer_claims();
    let (_directory, config) = runtime_config(Arc::new(stream)).await;
    let stream_metrics = Arc::clone(&config.metrics);
    let stream_runtime = CoreRuntime::start(config).await.unwrap();

    assert!(matches!(
        stream_runtime.join().await,
        Err(CoreRuntimeError::Worker(_))
    ));
    let rendered = stream_metrics.render_prometheus();
    assert!(rendered.contains("iot_ingest_stream_failures_total 1\n"));
    assert!(rendered.contains("iot_ingest_database_failures_total 0\n"));

    let stream = RecordingStream::default();
    stream.set_writer_records(vec![telemetry_record("unregistered-device")]);
    let (_directory, config) = runtime_config(Arc::new(stream)).await;
    let platform_metrics = Arc::clone(&config.metrics);
    let platform_runtime = CoreRuntime::start(config).await.unwrap();

    assert!(matches!(
        platform_runtime.join().await,
        Err(CoreRuntimeError::Worker(_))
    ));
    let rendered = platform_metrics.render_prometheus();
    assert!(rendered.contains("iot_ingest_stream_failures_total 0\n"));
    assert!(rendered.contains("iot_ingest_database_failures_total 1\n"));
}

#[tokio::test]
async fn failed_persistence_is_reclaimed_after_lease_expiry_and_acknowledged_once() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(
        StreamConfig::sqlite(directory.path().join("stream.sqlite"))
            .with_partitions(1)
            .with_lease_duration(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    stream
        .append(telemetry_record("retry-device").message)
        .await
        .unwrap();

    let (_platform_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let first_consumer =
        CoreStreamConsumer::new(Arc::new(stream.clone()), "writer", "first-member");
    let writer = PlatformTelemetryWriter::new((*config.store).clone(), 1);

    let first_attempt = writer.flush_once(&first_consumer, Utc::now()).await;

    assert!(matches!(first_attempt, Err(WriterError::Platform(_))));
    let counted_stream = AckCountingStream::new(stream.clone());
    assert_eq!(counted_stream.acknowledgements(), 0);

    sleep(Duration::from_millis(60)).await;
    config.store.register_device("retry-device").await.unwrap();

    let second_consumer =
        CoreStreamConsumer::new(Arc::new(counted_stream.clone()), "writer", "second-member");
    let retry = writer
        .flush_once(&second_consumer, Utc::now())
        .await
        .unwrap();
    let after_ack = counted_stream
        .claim(ClaimRequest::new("writer", "second-member"))
        .await
        .unwrap();

    assert_eq!(retry.read, 1);
    assert_eq!(after_ack.len(), 0);
    assert_eq!(counted_stream.acknowledgements(), 1);
}

#[tokio::test]
async fn window_loop_does_not_heartbeat_or_claim_the_alert_stream_consumer() {
    let stream = RecordingStream::default();
    stream.require_claim_on_heartbeat();
    let (_directory, mut config) = runtime_config(Arc::new(stream.clone())).await;
    config.writer_interval = Duration::from_secs(1);
    config.event_alert_interval = Duration::from_secs(1);
    config.window_alert_interval = Duration::from_millis(5);
    config.writer_heartbeat_interval = Duration::from_secs(1);
    config.alert_heartbeat_interval = Duration::from_secs(1);

    let runtime = CoreRuntime::start(config).await.unwrap();
    sleep(Duration::from_millis(25)).await;

    assert_eq!(stream.heartbeats_for("runtime-alert"), 1);
    assert_eq!(stream.claims_for("runtime-alert"), 1);
    runtime
        .drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn drain_deadline_aborts_and_awaits_blocked_workers_before_returning() {
    let stream = RecordingStream::default();
    stream.block_claims();
    let (_directory, config) = runtime_config(Arc::new(stream.clone())).await;
    let runtime = CoreRuntime::start(config).await.unwrap();
    stream.wait_for_claim_start().await;

    let result = runtime
        .drain(Instant::now() + Duration::from_millis(20))
        .await;

    assert!(matches!(result, Err(CoreRuntimeError::Deadline)));
    assert_eq!(stream.active_claims(), 0, "blocked workers must be awaited");
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

#[tokio::test]
async fn drain_maps_stream_timeout_to_runtime_deadline() {
    let stream = RecordingStream::default();
    stream.drain_times_out();
    let (_directory, config) = runtime_config(Arc::new(stream)).await;
    let runtime = CoreRuntime::start(config).await.unwrap();

    let result = runtime.drain(Instant::now() + Duration::from_secs(1)).await;

    assert!(matches!(result, Err(CoreRuntimeError::Deadline)));
}

#[test]
fn runtime_source_has_no_internal_transport_configuration() {
    let source = include_str!("../src/runtime.rs");
    for forbidden in ["http://", "https://", "secret", "reqwest", "hyper"] {
        assert!(!source.contains(forbidden), "runtime contains {forbidden}");
    }
}
