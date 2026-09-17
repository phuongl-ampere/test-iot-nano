use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{Duration as ChronoDuration, Utc};
use iot_storage::PlatformStore;
use iot_stream::{StreamError, StreamPort};
use thiserror::Error;
use tokio::{
    sync::Mutex as AsyncMutex,
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    CommandError, CommandTransport, CoreStreamConsumer, EmailSender, IngestMetrics,
    PlatformAlertEvaluator, PlatformCommandDispatcher, PlatformNotificationDispatcher,
    PlatformTelemetryWriter, WriterError,
};

#[derive(Clone)]
pub struct CoreRuntimeConfig {
    pub store: Arc<PlatformStore>,
    pub stream: Arc<dyn StreamPort>,
    pub command_transport: Arc<dyn CommandTransport>,
    pub email_sender: Arc<dyn EmailSender>,
    pub writer_batch_size: usize,
    pub alert_batch_size: usize,
    pub command_batch_size: u32,
    pub notification_batch_size: usize,
    pub writer_group: String,
    pub alert_group: String,
    pub writer_member_id: String,
    pub alert_member_id: String,
    pub writer_interval: Duration,
    pub event_alert_interval: Duration,
    pub window_alert_interval: Duration,
    pub command_interval: Duration,
    pub notification_interval: Duration,
    pub writer_heartbeat_interval: Duration,
    pub alert_heartbeat_interval: Duration,
    pub notification_send_timeout: Duration,
    pub notification_lease_duration: ChronoDuration,
    pub notification_retry_base: ChronoDuration,
    pub notification_retry_max: ChronoDuration,
    pub cancellation: CancellationToken,
    pub metrics: Arc<IngestMetrics>,
}

#[derive(Debug, Error)]
pub enum CoreRuntimeError {
    #[error("invalid runtime configuration: {0}")]
    Configuration(String),
    #[error("runtime startup failed: {0}")]
    Startup(String),
    #[error(transparent)]
    Worker(#[from] CoreRuntimeWorkerError),
    #[error("runtime drain failed: {0}")]
    Drain(#[source] StreamError),
    #[error("runtime command drain failed: {0}")]
    CommandDrain(#[source] CommandError),
    #[error("runtime drain deadline elapsed")]
    Deadline,
}

#[derive(Debug, Error, Clone)]
pub enum CoreRuntimeWorkerError {
    #[error("{worker} worker failed: {error}")]
    Failed { worker: &'static str, error: String },
    #[error("{worker} worker join failed: {error}")]
    Join { worker: &'static str, error: String },
}

type WorkerHandle = JoinHandle<Result<(), CoreRuntimeWorkerError>>;
const WORKER_COUNT: usize = 5;

#[derive(Debug, Error)]
enum WorkError {
    #[error("stream: {0}")]
    Stream(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("alert: {0}")]
    Alert(String),
}

type Work = Box<dyn FnMut() -> Pin<Box<dyn Future<Output = Result<(), WorkError>> + Send>> + Send>;

#[derive(Default)]
struct CommandWorkerHandoff {
    quiescing: AtomicBool,
    dispatch_lock: AsyncMutex<()>,
}

pub struct CoreRuntime {
    stop_claiming: Arc<AtomicBool>,
    cancellation: CancellationToken,
    stream: Arc<dyn StreamPort>,
    command_dispatcher: Arc<PlatformCommandDispatcher<Arc<dyn CommandTransport>>>,
    command_handoff: Arc<CommandWorkerHandoff>,
    workers: Mutex<Option<Vec<WorkerHandle>>>,
    startup_barriers: AtomicUsize,
    drain_lock: AsyncMutex<()>,
    drain_started: AtomicBool,
}

impl CoreRuntime {
    pub async fn start(config: CoreRuntimeConfig) -> Result<Self, CoreRuntimeError> {
        validate(&config)?;
        let stop_claiming = Arc::new(AtomicBool::new(false));
        let cancellation = config.cancellation.child_token();
        let writer_consumer = CoreStreamConsumer::new(
            Arc::clone(&config.stream),
            config.writer_group,
            config.writer_member_id,
        );
        let event_consumer = CoreStreamConsumer::new(
            Arc::clone(&config.stream),
            config.alert_group,
            config.alert_member_id,
        );
        let writer = Arc::new(PlatformTelemetryWriter::new(
            (*config.store).clone(),
            config.writer_batch_size,
        ));
        let evaluator = Arc::new(PlatformAlertEvaluator::new(
            (*config.store).clone(),
            config.alert_batch_size,
        ));
        let window_evaluator = Arc::new(PlatformAlertEvaluator::new(
            (*config.store).clone(),
            config.alert_batch_size,
        ));
        let command_dispatcher = Arc::new(PlatformCommandDispatcher::new(
            Arc::clone(&config.store),
            Arc::clone(&config.command_transport),
            config.command_batch_size,
        ));
        let command_handoff = Arc::new(CommandWorkerHandoff::default());
        let notification_dispatcher = Arc::new(
            PlatformNotificationDispatcher::new(
                Arc::clone(&config.store),
                Arc::clone(&config.email_sender),
                config.notification_batch_size,
            )
            .with_timeout(config.notification_send_timeout)
            .with_delivery_policy(
                config.notification_lease_duration,
                config.notification_retry_base,
                config.notification_retry_max,
            ),
        );
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(WORKER_COUNT);
        let writer_stop = Arc::clone(&stop_claiming);
        let writer_handle = tokio::spawn(run_worker(
            "writer",
            writer_consumer.clone(),
            config.writer_interval,
            config.writer_heartbeat_interval,
            cancellation.clone(),
            Arc::clone(&config.metrics),
            ready_tx.clone(),
            Box::new(move || {
                let writer = Arc::clone(&writer);
                let consumer = writer_consumer.clone();
                let stop = Arc::clone(&writer_stop);
                Box::pin(async move {
                    if stop.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    writer
                        .flush_once(&consumer, Utc::now())
                        .await
                        .map(|_| ())
                        .map_err(writer_work_error)
                })
            }),
        ));
        let alert_stop = Arc::clone(&stop_claiming);
        let event_handle = tokio::spawn(run_worker(
            "event alerts",
            event_consumer.clone(),
            config.event_alert_interval,
            config.alert_heartbeat_interval,
            cancellation.clone(),
            Arc::clone(&config.metrics),
            ready_tx.clone(),
            Box::new(move || {
                let evaluator = Arc::clone(&evaluator);
                let consumer = event_consumer.clone();
                let stop = Arc::clone(&alert_stop);
                Box::pin(async move {
                    if stop.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    evaluator
                        .flush_event_rules(&consumer, Utc::now())
                        .await
                        .map(|_| ())
                        .map_err(|error| match error {
                            crate::AlertError::Stream(error) => {
                                WorkError::Stream(error.to_string())
                            }
                            error => WorkError::Alert(error.to_string()),
                        })
                })
            }),
        ));
        let window_handle = tokio::spawn(run_window_worker(
            "window alerts",
            config.window_alert_interval,
            cancellation.clone(),
            Arc::clone(&config.metrics),
            ready_tx.clone(),
            Box::new(move || {
                let evaluator = Arc::clone(&window_evaluator);
                Box::pin(async move {
                    evaluator
                        .flush_window_rules(Utc::now())
                        .await
                        .map(|_| ())
                        .map_err(|error| WorkError::Alert(error.to_string()))
                })
            }),
        ));
        let command_dispatcher_for_worker = Arc::clone(&command_dispatcher);
        let command_handoff_for_worker = Arc::clone(&command_handoff);
        let command_handle = tokio::spawn(run_outbox_worker(
            "commands",
            config.command_interval,
            cancellation.clone(),
            Arc::clone(&config.metrics),
            ready_tx.clone(),
            Box::new(move || {
                let dispatcher = Arc::clone(&command_dispatcher_for_worker);
                let handoff = Arc::clone(&command_handoff_for_worker);
                Box::pin(async move {
                    if handoff.quiescing.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    let _dispatch_lock = handoff.dispatch_lock.lock().await;
                    if handoff.quiescing.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    dispatcher
                        .dispatch_once(Utc::now())
                        .await
                        .map(|_| ())
                        .map_err(|error| WorkError::Storage(error.to_string()))
                })
            }),
        ));
        let notification_metrics = Arc::clone(&config.metrics);
        let notification_handle = tokio::spawn(run_outbox_worker(
            "notifications",
            config.notification_interval,
            cancellation.clone(),
            Arc::clone(&config.metrics),
            ready_tx,
            Box::new(move || {
                let dispatcher = Arc::clone(&notification_dispatcher);
                let metrics = Arc::clone(&notification_metrics);
                Box::pin(async move {
                    let result = dispatcher
                        .dispatch_once(Utc::now())
                        .await
                        .map_err(|error| WorkError::Storage(error.to_string()))?;
                    metrics.record_notification_failures(result.retried);
                    Ok(())
                })
            }),
        ));
        let runtime = Self {
            stop_claiming,
            cancellation,
            stream: config.stream,
            command_dispatcher,
            command_handoff,
            workers: Mutex::new(Some(vec![
                writer_handle,
                event_handle,
                window_handle,
                command_handle,
                notification_handle,
            ])),
            startup_barriers: AtomicUsize::new(0),
            drain_lock: AsyncMutex::new(()),
            drain_started: AtomicBool::new(false),
        };
        for _ in 0..WORKER_COUNT {
            match ready_rx.recv().await {
                Some(Ok(())) => {
                    runtime.startup_barriers.fetch_add(1, Ordering::Release);
                }
                Some(Err(error)) => {
                    runtime.cancellation.cancel();
                    let _ = runtime
                        .join_all(Instant::now() + Duration::from_secs(1))
                        .await;
                    return Err(error);
                }
                None => {
                    runtime.cancellation.cancel();
                    let _ = runtime
                        .join_all(Instant::now() + Duration::from_secs(1))
                        .await;
                    return Err(CoreRuntimeError::Startup(
                        "worker startup channel closed".to_owned(),
                    ));
                }
            }
        }
        Ok(runtime)
    }

    pub fn ready(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self.startup_barrier_count() == WORKER_COUNT
            && self
                .workers
                .lock()
                .expect("runtime worker mutex poisoned")
                .as_ref()
                .is_some_and(|workers| workers.iter().all(|worker| !worker.is_finished()))
    }

    pub fn startup_barrier_count(&self) -> usize {
        self.startup_barriers.load(Ordering::Acquire)
    }

    pub fn stop_claiming(&self) {
        self.stop_claiming.store(true, Ordering::Release);
        self.stream.stop_claiming();
    }

    pub async fn quiesce_and_drain_commands(
        &self,
        deadline: Instant,
    ) -> Result<(), CoreRuntimeError> {
        self.command_handoff
            .quiescing
            .store(true, Ordering::Release);
        let remaining = remaining_until(deadline)?;
        let _dispatch_lock = time::timeout(remaining, self.command_handoff.dispatch_lock.lock())
            .await
            .map_err(|_| CoreRuntimeError::Deadline)?;
        loop {
            let remaining = remaining_until(deadline)?;
            let result =
                time::timeout(remaining, self.command_dispatcher.dispatch_once(Utc::now()))
                    .await
                    .map_err(|_| CoreRuntimeError::Deadline)?
                    .map_err(CoreRuntimeError::CommandDrain)?;
            if result.claimed == 0 {
                return Ok(());
            }
        }
    }

    pub async fn drain(&self, deadline: Instant) -> Result<(), CoreRuntimeError> {
        self.stop_claiming();
        let _drain_lock = self.drain_lock.lock().await;
        if !self.drain_started.swap(true, Ordering::AcqRel) {
            if let Err(error) = self.quiesce_and_drain_commands(deadline).await {
                self.cancellation.cancel();
                let _ = self.join_all(deadline).await;
                return Err(error);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let drain_result = time::timeout(remaining, self.stream.drain(deadline)).await;
            match drain_result {
                Err(_) | Ok(Err(StreamError::DrainTimeout { .. })) => {
                    self.cancellation.cancel();
                    let _ = self.join_all(deadline).await;
                    return Err(CoreRuntimeError::Deadline);
                }
                Ok(Err(error)) => {
                    self.cancellation.cancel();
                    let _ = self.join_all(deadline).await;
                    return Err(CoreRuntimeError::Drain(error));
                }
                Ok(Ok(())) => {}
            }
        }
        self.cancellation.cancel();
        self.join_all(deadline).await
    }

    pub async fn join(self) -> Result<(), CoreRuntimeError> {
        self.join_all(Instant::now() + Duration::from_secs(365 * 24 * 60 * 60))
            .await
    }

    async fn join_all(&self, deadline: Instant) -> Result<(), CoreRuntimeError> {
        let worker_handles = self
            .workers
            .lock()
            .expect("runtime worker mutex poisoned")
            .take()
            .unwrap_or_default();
        let mut first_error = None;
        let mut handles = [
            "writer",
            "event alerts",
            "window alerts",
            "commands",
            "notifications",
        ]
        .into_iter()
        .zip(worker_handles)
        .collect::<Vec<_>>();
        for index in 0..handles.len() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let worker = handles[index].0;
            let result = {
                let (_, handle) = &mut handles[index];
                time::timeout(remaining, handle).await
            };
            match result {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    if first_error.is_none() {
                        first_error = Some(CoreRuntimeError::Worker(error));
                    }
                    self.cancellation.cancel();
                }
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error =
                            Some(CoreRuntimeError::Worker(CoreRuntimeWorkerError::Join {
                                worker,
                                error: error.to_string(),
                            }));
                    }
                    self.cancellation.cancel();
                }
                Err(_error) => {
                    for (_, handle) in handles.iter_mut().skip(index) {
                        handle.abort();
                    }
                    for (_, handle) in handles.iter_mut().skip(index) {
                        let _ = handle.await;
                    }
                    return Err(CoreRuntimeError::Deadline);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn remaining_until(deadline: Instant) -> Result<Duration, CoreRuntimeError> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or(CoreRuntimeError::Deadline)
}

fn validate(config: &CoreRuntimeConfig) -> Result<(), CoreRuntimeError> {
    if config.writer_batch_size == 0
        || config.alert_batch_size == 0
        || config.command_batch_size == 0
        || config.notification_batch_size == 0
    {
        return Err(CoreRuntimeError::Configuration(
            "batch sizes must be greater than zero".to_owned(),
        ));
    }
    for (name, duration) in [
        ("writer interval", config.writer_interval),
        ("event alert interval", config.event_alert_interval),
        ("window alert interval", config.window_alert_interval),
        ("command interval", config.command_interval),
        ("notification interval", config.notification_interval),
        (
            "writer heartbeat interval",
            config.writer_heartbeat_interval,
        ),
        ("alert heartbeat interval", config.alert_heartbeat_interval),
    ] {
        if duration.is_zero() {
            return Err(CoreRuntimeError::Configuration(format!(
                "{name} must be greater than zero"
            )));
        }
    }
    if config.notification_send_timeout.is_zero() {
        return Err(CoreRuntimeError::Configuration(
            "notification send timeout must be greater than zero".to_owned(),
        ));
    }
    for (name, duration) in [
        (
            "notification lease duration",
            config.notification_lease_duration,
        ),
        ("notification retry base", config.notification_retry_base),
        ("notification retry max", config.notification_retry_max),
    ] {
        if duration <= ChronoDuration::zero() {
            return Err(CoreRuntimeError::Configuration(format!(
                "{name} must be greater than zero"
            )));
        }
    }
    for (name, value) in [
        ("writer group", config.writer_group.as_str()),
        ("alert group", config.alert_group.as_str()),
        ("writer member ID", config.writer_member_id.as_str()),
        ("alert member ID", config.alert_member_id.as_str()),
    ] {
        if value.is_empty() {
            return Err(CoreRuntimeError::Configuration(format!(
                "{name} must not be empty"
            )));
        }
    }
    Ok(())
}

async fn run_worker(
    name: &'static str,
    consumer: CoreStreamConsumer,
    work_interval: Duration,
    heartbeat_interval: Duration,
    cancellation: CancellationToken,
    metrics: Arc<IngestMetrics>,
    ready: tokio::sync::mpsc::Sender<Result<(), CoreRuntimeError>>,
    mut work: Work,
) -> Result<(), CoreRuntimeWorkerError> {
    if let Err(error) = consumer.heartbeat().await {
        metrics.record_stream_failure();
        cancellation.cancel();
        let worker_error = worker_error(name, error);
        let _ = ready
            .send(Err(CoreRuntimeError::Worker(worker_error.clone())))
            .await;
        return Err(worker_error);
    }
    let _ = ready.send(Ok(())).await;
    let mut work_tick = time::interval(work_interval);
    let mut heartbeat_tick = time::interval(heartbeat_interval);
    work_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    work_tick.tick().await;
    heartbeat_tick.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = heartbeat_tick.tick() => {
                if let Err(error) = consumer.heartbeat().await {
                    metrics.record_stream_failure();
                    cancellation.cancel();
                    return Err(worker_error(name, error));
                }
            }
            _ = work_tick.tick() => {
                if let Err(error) = work().await {
                    match &error {
                        WorkError::Stream(_) => metrics.record_stream_failure(),
                        WorkError::Storage(_) => metrics.record_database_failure(),
                        WorkError::Alert(_) => metrics.record_alert_failure(),
                    }
                    cancellation.cancel();
                    return Err(worker_error(name, error));
                }
            }
        }
    }
}

async fn run_window_worker(
    name: &'static str,
    work_interval: Duration,
    cancellation: CancellationToken,
    metrics: Arc<IngestMetrics>,
    ready: tokio::sync::mpsc::Sender<Result<(), CoreRuntimeError>>,
    mut work: Work,
) -> Result<(), CoreRuntimeWorkerError> {
    let _ = ready.send(Ok(())).await;
    let mut work_tick = time::interval(work_interval);
    work_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    work_tick.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = work_tick.tick() => {
                if let Err(error) = work().await {
                    match &error {
                        WorkError::Stream(_) => metrics.record_stream_failure(),
                        WorkError::Storage(_) => metrics.record_database_failure(),
                        WorkError::Alert(_) => metrics.record_alert_failure(),
                    }
                    cancellation.cancel();
                    return Err(worker_error(name, error));
                }
            }
        }
    }
}

async fn run_outbox_worker(
    name: &'static str,
    work_interval: Duration,
    cancellation: CancellationToken,
    metrics: Arc<IngestMetrics>,
    ready: tokio::sync::mpsc::Sender<Result<(), CoreRuntimeError>>,
    mut work: Work,
) -> Result<(), CoreRuntimeWorkerError> {
    let _ = ready.send(Ok(())).await;
    let mut work_tick = time::interval(work_interval);
    work_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    work_tick.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            _ = work_tick.tick() => {
                if let Err(error) = work().await {
                    match &error {
                        WorkError::Stream(_) => metrics.record_stream_failure(),
                        WorkError::Storage(_) => metrics.record_database_failure(),
                        WorkError::Alert(_) => metrics.record_alert_failure(),
                    }
                    cancellation.cancel();
                    return Err(worker_error(name, error));
                }
            }
        }
    }
}

fn worker_error(name: &'static str, error: impl std::fmt::Display) -> CoreRuntimeWorkerError {
    CoreRuntimeWorkerError::Failed {
        worker: name,
        error: error.to_string(),
    }
}

fn writer_work_error(error: WriterError) -> WorkError {
    match error {
        WriterError::Stream(error) => WorkError::Stream(error.to_string()),
        WriterError::Platform(error) => WorkError::Storage(error.to_string()),
    }
}
