use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use iot_storage::PlatformStore;
use iot_stream::{StreamError, StreamPort};
use thiserror::Error;
use tokio::{
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{CoreStreamConsumer, IngestMetrics, PlatformAlertEvaluator, PlatformTelemetryWriter};

#[derive(Clone)]
pub struct CoreRuntimeConfig {
    pub store: Arc<PlatformStore>,
    pub stream: Arc<dyn StreamPort>,
    pub writer_batch_size: usize,
    pub alert_batch_size: usize,
    pub writer_group: String,
    pub alert_group: String,
    pub writer_member_id: String,
    pub alert_member_id: String,
    pub writer_interval: Duration,
    pub event_alert_interval: Duration,
    pub window_alert_interval: Duration,
    pub writer_heartbeat_interval: Duration,
    pub alert_heartbeat_interval: Duration,
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

pub struct CoreRuntime {
    stop_claiming: Arc<AtomicBool>,
    cancellation: CancellationToken,
    stream: Arc<dyn StreamPort>,
    workers: Mutex<Option<Vec<WorkerHandle>>>,
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
        let alert_consumer = CoreStreamConsumer::new(
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
        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(3);
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
                        .map_err(|error| WorkError::Storage(error.to_string()))
                })
            }),
        ));
        let alert_stop = Arc::clone(&stop_claiming);
        let event_consumer = alert_consumer.clone();
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
        let window_handle = tokio::spawn(run_worker(
            "window alerts",
            alert_consumer,
            config.window_alert_interval,
            config.alert_heartbeat_interval,
            cancellation.clone(),
            Arc::clone(&config.metrics),
            ready_tx,
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
        let runtime = Self {
            stop_claiming,
            cancellation,
            stream: config.stream,
            workers: Mutex::new(Some(vec![writer_handle, event_handle, window_handle])),
            drain_started: AtomicBool::new(false),
        };
        for _ in 0..3 {
            match ready_rx.recv().await {
                Some(Ok(())) => {}
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
            && self
                .workers
                .lock()
                .expect("runtime worker mutex poisoned")
                .is_some()
    }

    pub fn stop_claiming(&self) {
        self.stop_claiming.store(true, Ordering::Release);
    }

    pub async fn drain(&self, deadline: Instant) -> Result<(), CoreRuntimeError> {
        self.stop_claiming();
        if !self.drain_started.swap(true, Ordering::AcqRel) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let drain_result = time::timeout(remaining, self.stream.drain(deadline)).await;
            if drain_result.is_err() {
                self.cancellation.cancel();
                let _ = self.join_all(deadline).await;
                return Err(CoreRuntimeError::Deadline);
            }
            if let Err(error) = drain_result.expect("drain result was checked above") {
                self.cancellation.cancel();
                let _ = self.join_all(deadline).await;
                return Err(CoreRuntimeError::Drain(error));
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
        let handles = self
            .workers
            .lock()
            .expect("runtime worker mutex poisoned")
            .take()
            .unwrap_or_default();
        let mut first_error = None;
        let mut handles = ["writer", "event alerts", "window alerts"]
            .into_iter()
            .zip(handles)
            .peekable();
        while let Some((worker, handle)) = handles.next() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let result = time::timeout(remaining, handle).await;
            match result {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    if first_error.is_none() {
                        first_error = Some(CoreRuntimeError::Worker(error));
                    }
                }
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error =
                            Some(CoreRuntimeError::Worker(CoreRuntimeWorkerError::Join {
                                worker,
                                error: error.to_string(),
                            }));
                    }
                }
                Err(_error) => {
                    for (_, handle) in handles {
                        handle.abort();
                    }
                    return Err(first_error.unwrap_or(CoreRuntimeError::Deadline));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn validate(config: &CoreRuntimeConfig) -> Result<(), CoreRuntimeError> {
    if config.writer_batch_size == 0 || config.alert_batch_size == 0 {
        return Err(CoreRuntimeError::Configuration(
            "batch sizes must be greater than zero".to_owned(),
        ));
    }
    for (name, duration) in [
        ("writer interval", config.writer_interval),
        ("event alert interval", config.event_alert_interval),
        ("window alert interval", config.window_alert_interval),
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

fn worker_error(name: &'static str, error: impl std::fmt::Display) -> CoreRuntimeWorkerError {
    CoreRuntimeWorkerError::Failed {
        worker: name,
        error: error.to_string(),
    }
}
