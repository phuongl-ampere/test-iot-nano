use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use iot_nano_stream::StreamPort;
use rumqttd::InProcessBrokerControl;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, watch},
    task::{Id as TaskId, JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    AuthenticatedDevice, AuthorizationError, BrokerStorage, CacheEntry, CachePort,
    CommandResponsePort, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, ListenerConfiguration, MqttdDeviceTransport, MqttdError,
    MuxSettings, PreboundBackendListeners, ProtocolBackends, PublicMuxAcceptanceGate,
    RpcSessionRouter, TransportAuthRequest, create_in_process_broker_with_prebound_listeners,
    load_tls_acceptor, serve_public_plaintext_device_only_mux, serve_public_tls_device_only_mux,
    wait_for_backends,
};

const AUTHORIZATION_CACHE_TTL: Duration = Duration::from_secs(30);
const AUTHORIZATION_CACHE_PREFIX: &str = "mqttd:device-auth:";

#[derive(Debug, Clone)]
pub struct MqttListenerConfig {
    pub plaintext_address: SocketAddr,
    pub tls_address: SocketAddr,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub max_connections: usize,
    pub max_payload_size: usize,
    pub max_inflight_count: usize,
}

pub struct MqttRuntimeConfig {
    pub listeners: MqttListenerConfig,
    pub storage: Arc<dyn BrokerStorage>,
    pub authorization: Arc<dyn DeviceAuthorizationPort>,
    pub stream: Arc<dyn StreamPort>,
    pub command_responses: Arc<dyn CommandResponsePort>,
    pub cache: Arc<dyn CachePort>,
    pub session_router: RpcSessionRouter,
    pub cancellation: CancellationToken,
}

pub struct MqttRuntime {
    supervisor: Arc<AsyncMutex<RuntimeTaskSupervisor>>,
    supervisor_monitor: Option<JoinHandle<Result<(), MqttRuntimeError>>>,
    session_router: RpcSessionRouter,
    shutdown: RuntimeShutdownControl,
    parent_cancellation_watcher: Option<JoinHandle<()>>,
    drain_started: watch::Sender<bool>,
    public_connections: Arc<AtomicUsize>,
    _cache: Arc<dyn CachePort>,
}

#[derive(Clone)]
struct RuntimeShutdownControl {
    broker: InProcessBrokerControl,
    device_admission: DeviceAdmissionControl,
    force_cancellation: CancellationToken,
    public_accept_gate: PublicMuxAcceptanceGate,
    public_accept_shutdown: watch::Sender<bool>,
    public_force_shutdown: watch::Sender<bool>,
    accepting: Arc<AtomicBool>,
}

impl RuntimeShutdownControl {
    fn stop_accepting(&self) {
        if self.accepting.swap(false, Ordering::AcqRel) {
            self.public_accept_gate.close();
            self.public_accept_shutdown.send_replace(true);
            self.broker.stop_accepting();
            self.device_admission.stop();
        }
    }

    fn force_stop(&self) {
        self.stop_accepting();
        self.public_force_shutdown.send_replace(true);
        self.force_cancellation.cancel();
        self.broker.force_stop();
    }

    fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug)]
enum RuntimeTaskName {
    Broker,
    DeviceV311,
    DeviceV5,
    PublicPlaintext,
    PublicTls,
}

impl std::fmt::Display for RuntimeTaskName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Broker => "broker",
            Self::DeviceV311 => "device MQTT 3.1.1 listener",
            Self::DeviceV5 => "device MQTT 5 listener",
            Self::PublicPlaintext => "public plaintext listener",
            Self::PublicTls => "public TLS listener",
        })
    }
}

struct RuntimeTaskSupervisor {
    tasks: JoinSet<(RuntimeTaskName, Result<(), MqttRuntimeError>)>,
    task_names: HashMap<TaskId, RuntimeTaskName>,
}

impl RuntimeTaskSupervisor {
    fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            task_names: HashMap::new(),
        }
    }

    fn spawn<F>(&mut self, name: RuntimeTaskName, task: F)
    where
        F: Future<Output = Result<(), MqttRuntimeError>> + Send + 'static,
    {
        let handle = self.tasks.spawn(async move { (name, task.await) });
        self.task_names.insert(handle.id(), name);
    }

    #[cfg(test)]
    async fn join_until(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        while !self.tasks.is_empty() {
            let joined = timeout_until(deadline, self.tasks.join_next_with_id())
                .await
                .map_err(|_| MqttRuntimeError::DeadlineElapsed)?;
            match joined {
                Some(Ok((id, (_name, Ok(()))))) => {
                    self.task_names.remove(&id);
                }
                Some(Ok((id, (name, Err(error))))) => {
                    let name = self.task_names.remove(&id).unwrap_or(name);
                    return Err(match error {
                        MqttRuntimeError::Worker(message) => {
                            MqttRuntimeError::Worker(format!("{name} task failed: {message}"))
                        }
                        error => error,
                    });
                }
                Some(Err(error)) => {
                    let name = self
                        .task_names
                        .remove(&error.id())
                        .map_or_else(|| "managed".to_owned(), |name| name.to_string());
                    return Err(MqttRuntimeError::Worker(format!(
                        "{name} task join failure: {error}"
                    )));
                }
                None => break,
            }
        }
        Ok(())
    }

    async fn run_until_stopped<F>(
        &mut self,
        force_cancellation: CancellationToken,
        accepting: Arc<AtomicBool>,
        mut on_failure: F,
    ) -> Result<(), MqttRuntimeError>
    where
        F: FnMut(),
    {
        let mut failures = Vec::new();
        let mut force_requested = force_cancellation.is_cancelled();
        if force_requested {
            self.abort_all();
        }

        while !self.tasks.is_empty() {
            tokio::select! {
                _ = force_cancellation.cancelled(), if !force_requested => {
                    force_requested = true;
                    self.abort_all();
                }
                joined = self.tasks.join_next_with_id() => match joined {
                    Some(Ok((id, (name, Ok(()))))) => {
                        let name = self.task_names.remove(&id).unwrap_or(name);
                        if accepting.load(Ordering::Acquire) && !force_requested {
                            failures.push(format!("{name} task exited before shutdown"));
                            force_requested = true;
                            on_failure();
                            force_cancellation.cancel();
                            self.abort_all();
                        }
                    }
                    Some(Ok((id, (name, Err(error))))) => {
                        let name = self.task_names.remove(&id).unwrap_or(name);
                        failures.push(format!(
                            "{name} task failed: {}",
                            task_error_message(error),
                        ));
                        if !force_requested {
                            force_requested = true;
                            on_failure();
                            force_cancellation.cancel();
                            self.abort_all();
                        }
                    }
                    Some(Err(error)) if !error.is_cancelled() => {
                        let name = self
                            .task_names
                            .remove(&error.id())
                            .map_or_else(|| "managed".to_owned(), |name| name.to_string());
                        failures.push(format!("{name} task join failure: {error}"));
                        if !force_requested {
                            force_requested = true;
                            on_failure();
                            force_cancellation.cancel();
                            self.abort_all();
                        }
                    }
                    Some(Err(_)) | None => {}
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(MqttRuntimeError::Worker(failures.join("; ")))
        }
    }

    fn abort_all(&mut self) {
        self.tasks.abort_all();
    }

    async fn abort_and_join(&mut self) {
        self.abort_all();
        while let Some(joined) = self.tasks.join_next_with_id().await {
            match joined {
                Ok((id, _)) => {
                    self.task_names.remove(&id);
                }
                Err(error) => {
                    self.task_names.remove(&error.id());
                }
            }
        }
    }
}

fn task_error_message(error: MqttRuntimeError) -> String {
    match error {
        MqttRuntimeError::Worker(message) => message,
        error => error.to_string(),
    }
}

#[derive(Clone)]
struct DeviceAdmissionControl {
    accepting: Arc<Mutex<bool>>,
    accept_cancellation: CancellationToken,
    #[cfg(test)]
    admission_barrier: Arc<Mutex<Option<DeviceAdmissionBarrier>>>,
}

impl DeviceAdmissionControl {
    fn new() -> Self {
        Self {
            accepting: Arc::new(Mutex::new(true)),
            accept_cancellation: CancellationToken::new(),
            #[cfg(test)]
            admission_barrier: Arc::new(Mutex::new(None)),
        }
    }

    /// Closes private admission before waking listener workers so the mutex is
    /// the linearization point for sockets accepted during shutdown.
    fn stop(&self) {
        *self
            .accepting
            .lock()
            .expect("device admission gate is not poisoned") = false;
        self.accept_cancellation.cancel();
    }

    fn try_admit(&self) -> bool {
        *self
            .accepting
            .lock()
            .expect("device admission gate is not poisoned")
    }

    fn accept_cancellation(&self) -> CancellationToken {
        self.accept_cancellation.clone()
    }

    #[cfg(test)]
    fn test_admission_barrier(&self) -> DeviceAdmissionBarrier {
        let barrier = DeviceAdmissionBarrier::default();
        *self
            .admission_barrier
            .lock()
            .expect("device admission test barrier is not poisoned") = Some(barrier.clone());
        barrier
    }

    #[cfg(test)]
    async fn wait_before_admission(&self) {
        let barrier = self
            .admission_barrier
            .lock()
            .expect("device admission test barrier is not poisoned")
            .clone();
        if let Some(barrier) = barrier {
            barrier.reached.notify_one();
            tokio::time::timeout(Duration::from_secs(1), barrier.proceed.notified())
                .await
                .expect("device admission test barrier was not released within one second");
        }
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
struct DeviceAdmissionBarrier {
    reached: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl DeviceAdmissionBarrier {
    async fn wait_until_reached(&self, description: &str) {
        tokio::time::timeout(Duration::from_secs(1), self.reached.notified())
            .await
            .unwrap_or_else(|_| panic!("{description} was not reached within one second"));
    }

    fn release(&self) {
        self.proceed.notify_one();
    }
}

impl MqttRuntime {
    pub async fn start(config: MqttRuntimeConfig) -> Result<Self, MqttRuntimeStartError> {
        validate_listeners(&config.listeners)?;
        // The standalone binary installs this itself. The runtime needs the same setup when
        // embedded directly by the monolith; an already-installed provider is compatible.
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let tls_acceptor = load_tls_acceptor(
            &config.listeners.tls_cert_path,
            &config.listeners.tls_key_path,
        )
        .map_err(MqttRuntimeStartError::Tls)?;

        let v311_backend_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .map_err(MqttRuntimeStartError::PrivateBackendBind)?;
        let v311_backend_address = v311_backend_listener
            .local_addr()
            .map_err(MqttRuntimeStartError::PrivateBackendBind)?;
        let v5_backend_listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .map_err(MqttRuntimeStartError::PrivateBackendBind)?;
        let v5_backend_address = v5_backend_listener
            .local_addr()
            .map_err(MqttRuntimeStartError::PrivateBackendBind)?;
        let device_v311_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(MqttRuntimeStartError::DeviceBackendBind)?;
        let device_v311_address = device_v311_listener
            .local_addr()
            .map_err(MqttRuntimeStartError::DeviceBackendBind)?;
        let device_v5_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(MqttRuntimeStartError::DeviceBackendBind)?;
        let device_v5_address = device_v5_listener
            .local_addr()
            .map_err(MqttRuntimeStartError::DeviceBackendBind)?;

        let (broker, broker_control) = create_in_process_broker_with_prebound_listeners(
            ListenerConfiguration {
                plaintext_address: config.listeners.plaintext_address,
                tls_address: config.listeners.tls_address,
                v311_backend_address,
                v5_backend_address,
                tls_cert_path: config.listeners.tls_cert_path.clone(),
                tls_key_path: config.listeners.tls_key_path.clone(),
                websocket_address: None,
                websocket_tls: false,
                bridge: None,
                max_connections: config.listeners.max_connections,
                max_payload_size: config.listeners.max_payload_size,
                max_inflight_count: config.listeners.max_inflight_count,
                token_authenticator: None,
                auth_handler: None,
                authorization_handler: None,
            },
            PreboundBackendListeners {
                v311: v311_backend_listener,
                v5: v5_backend_listener,
            },
            Arc::clone(&config.storage),
        )
        .map_err(MqttRuntimeStartError::Broker)?;

        let force_cancellation = CancellationToken::new();
        let device_admission = DeviceAdmissionControl::new();
        let public_accept_gate = PublicMuxAcceptanceGate::default();
        let (public_accept_shutdown, _) = watch::channel(false);
        let (public_force_shutdown, _) = watch::channel(false);
        let accepting = Arc::new(AtomicBool::new(true));
        let shutdown = RuntimeShutdownControl {
            broker: broker_control,
            device_admission: device_admission.clone(),
            force_cancellation: force_cancellation.clone(),
            public_accept_gate: public_accept_gate.clone(),
            public_accept_shutdown,
            public_force_shutdown,
            accepting,
        };
        let mut supervisor = RuntimeTaskSupervisor::new();
        supervisor.spawn(RuntimeTaskName::Broker, async move {
            broker
                .run()
                .await
                .map_err(|error| MqttRuntimeError::Broker(MqttdError::Broker(Box::new(error))))
        });
        if let Err(error) = wait_for_backends(
            [v311_backend_address, v5_backend_address],
            Duration::from_secs(5),
        )
        .await
        {
            cancel_startup(&shutdown, &mut supervisor).await;
            return Err(MqttRuntimeStartError::Broker(error));
        }

        let public_connections = Arc::new(AtomicUsize::new(0));
        let authorization: Arc<dyn DeviceAuthorizationPort> = Arc::new(
            CachedDeviceAuthorization::new(config.authorization, Arc::clone(&config.cache)),
        );
        let transport = MqttdDeviceTransport::with_local_ports_and_router(
            config.session_router.clone(),
            authorization,
            config.stream,
            config.command_responses,
        );
        let v311_transport = transport.clone();
        let v311_admission = device_admission.clone();
        let v311_force_cancellation = force_cancellation.clone();
        supervisor.spawn(RuntimeTaskName::DeviceV311, async move {
            serve_device_backend(
                device_v311_listener,
                v311_transport,
                false,
                v311_admission,
                v311_force_cancellation,
            )
            .await
        });
        let v5_admission = shutdown.device_admission.clone();
        let v5_force_cancellation = shutdown.force_cancellation.clone();
        supervisor.spawn(RuntimeTaskName::DeviceV5, async move {
            serve_device_backend(
                device_v5_listener,
                transport,
                true,
                v5_admission,
                v5_force_cancellation,
            )
            .await
        });

        let plaintext_listener = match TcpListener::bind(config.listeners.plaintext_address).await {
            Ok(listener) => listener,
            Err(error) => {
                cancel_startup(&shutdown, &mut supervisor).await;
                return Err(MqttRuntimeStartError::PlaintextListenerBind(error));
            }
        };
        let tls_listener = match TcpListener::bind(config.listeners.tls_address).await {
            Ok(listener) => listener,
            Err(error) => {
                cancel_startup(&shutdown, &mut supervisor).await;
                return Err(MqttRuntimeStartError::TlsListenerBind(error));
            }
        };
        let backends = ProtocolBackends {
            v311: v311_backend_address,
            v5: v5_backend_address,
            device_v311: Some(device_v311_address),
            device_v5: Some(device_v5_address),
        };
        supervisor.spawn(RuntimeTaskName::PublicPlaintext, {
            let accept_shutdown = shutdown.public_accept_shutdown.subscribe();
            let force_shutdown = shutdown.public_force_shutdown.subscribe();
            let accept_gate = shutdown.public_accept_gate.clone();
            let connection_counter = Arc::clone(&public_connections);
            async move {
                serve_public_plaintext_device_only_mux(
                    plaintext_listener,
                    backends,
                    MuxSettings::default(),
                    accept_shutdown,
                    force_shutdown,
                    accept_gate,
                    connection_counter,
                )
                .await
                .map_err(|source| {
                    MqttRuntimeError::PublicWorker(MqttdError::PublicWorkerIo {
                        worker: "public plaintext mux".to_owned(),
                        source,
                    })
                })
            }
        });
        supervisor.spawn(RuntimeTaskName::PublicTls, {
            let accept_shutdown = shutdown.public_accept_shutdown.subscribe();
            let force_shutdown = shutdown.public_force_shutdown.subscribe();
            let accept_gate = shutdown.public_accept_gate.clone();
            let connection_counter = Arc::clone(&public_connections);
            async move {
                serve_public_tls_device_only_mux(
                    tls_listener,
                    tls_acceptor,
                    backends,
                    MuxSettings::default(),
                    accept_shutdown,
                    force_shutdown,
                    accept_gate,
                    connection_counter,
                )
                .await
                .map_err(|source| {
                    MqttRuntimeError::PublicWorker(MqttdError::PublicWorkerIo {
                        worker: "public TLS mux".to_owned(),
                        source,
                    })
                })
            }
        });

        let supervisor = Arc::new(AsyncMutex::new(supervisor));
        let (supervisor_completed, _) = watch::channel(false);
        let supervisor_monitor = spawn_task_supervisor(
            Arc::clone(&supervisor),
            shutdown.clone(),
            supervisor_completed.clone(),
        );
        let (drain_started, _) = watch::channel(false);
        let parent_cancellation_watcher = spawn_parent_cancellation_watcher(
            config.cancellation,
            shutdown.clone(),
            supervisor_completed.subscribe(),
        );

        Ok(Self {
            supervisor,
            supervisor_monitor: Some(supervisor_monitor),
            session_router: config.session_router,
            shutdown,
            parent_cancellation_watcher: Some(parent_cancellation_watcher),
            drain_started,
            public_connections,
            _cache: config.cache,
        })
    }

    pub fn session_router(&self) -> RpcSessionRouter {
        self.session_router.clone()
    }

    pub fn is_accepting(&self) -> bool {
        self.shutdown.is_accepting()
    }

    /// Returns the number of public mux connections accepted and still in flight.
    pub fn public_connection_count(&self) -> usize {
        self.public_connections.load(Ordering::Relaxed)
    }

    /// Subscribes to the transition after public acceptance has stopped for a drain.
    pub fn drain_started_receiver(&self) -> watch::Receiver<bool> {
        self.drain_started.subscribe()
    }

    pub async fn stop_accepting(&mut self) -> Result<(), MqttRuntimeError> {
        self.shutdown.stop_accepting();
        Ok(())
    }

    pub async fn drain(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        self.stop_accepting().await?;
        self.drain_started.send_replace(true);
        self.join(deadline).await
    }

    pub async fn join(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        let result = self.join_supervisor_until(deadline).await;
        if result.is_err() {
            self.shutdown.force_stop();
        };
        self.stop_parent_cancellation_watcher().await;
        result
    }

    async fn join_supervisor_until(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        let Some(mut monitor) = self.supervisor_monitor.take() else {
            return Ok(());
        };
        match timeout_until(deadline, &mut monitor).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(MqttRuntimeError::Worker(format!(
                "runtime supervisor join failure: {error}"
            ))),
            Err(()) => {
                self.shutdown.force_stop();
                if let Ok(mut supervisor) = self.supervisor.try_lock() {
                    supervisor.abort_all();
                }
                let _ = monitor.await;
                Err(MqttRuntimeError::DeadlineElapsed)
            }
        }
    }

    async fn stop_parent_cancellation_watcher(&mut self) {
        if let Some(watcher) = self.parent_cancellation_watcher.take() {
            watcher.abort();
            let _ = watcher.await;
        }
    }
}

fn spawn_parent_cancellation_watcher(
    parent_cancellation: CancellationToken,
    shutdown: RuntimeShutdownControl,
    mut supervisor_completed: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        parent_cancellation.cancelled().await;
        shutdown.force_stop();
        while !*supervisor_completed.borrow() {
            if supervisor_completed.changed().await.is_err() {
                break;
            }
        }
    })
}

fn spawn_task_supervisor(
    supervisor: Arc<AsyncMutex<RuntimeTaskSupervisor>>,
    shutdown: RuntimeShutdownControl,
    supervisor_completed: watch::Sender<bool>,
) -> JoinHandle<Result<(), MqttRuntimeError>> {
    tokio::spawn(async move {
        let failure_shutdown = shutdown.clone();
        let result = supervisor
            .lock()
            .await
            .run_until_stopped(
                shutdown.force_cancellation.clone(),
                Arc::clone(&shutdown.accepting),
                move || failure_shutdown.force_stop(),
            )
            .await;
        if result.is_err() {
            shutdown.force_stop();
        }
        supervisor_completed.send_replace(true);
        result
    })
}

impl Drop for MqttRuntime {
    fn drop(&mut self) {
        if let Some(watcher) = self.parent_cancellation_watcher.take() {
            watcher.abort();
        }
        self.shutdown.force_stop();
        if let Ok(mut supervisor) = self.supervisor.try_lock() {
            supervisor.abort_all();
        }
    }
}

#[derive(Debug, Error)]
pub enum MqttRuntimeStartError {
    #[error("MQTT public plaintext and TLS listener addresses must differ")]
    DuplicatePublicListenerAddress,
    #[error("MQTT listener limit {0} must be greater than zero")]
    InvalidLimit(&'static str),
    #[error("MQTT TLS setup failed")]
    Tls(#[source] MqttdError),
    #[error("MQTT private backend bind failed")]
    PrivateBackendBind(#[source] io::Error),
    #[error("MQTT private device backend bind failed")]
    DeviceBackendBind(#[source] io::Error),
    #[error("MQTT broker startup failed")]
    Broker(#[source] MqttdError),
    #[error("MQTT public plaintext listener bind failed")]
    PlaintextListenerBind(#[source] io::Error),
    #[error("MQTT public TLS listener bind failed")]
    TlsListenerBind(#[source] io::Error),
    #[error("MQTT public listener worker failed to start")]
    PublicWorker(#[source] MqttdError),
}

#[derive(Debug, Error)]
pub enum MqttRuntimeError {
    #[error("MQTT runtime drain deadline elapsed")]
    DeadlineElapsed,
    #[error("MQTT runtime worker failed: {0}")]
    Worker(String),
    #[error("MQTT public listener worker failed")]
    PublicWorker(#[source] MqttdError),
    #[error("MQTT broker shutdown failed")]
    Broker(#[source] MqttdError),
}

fn validate_listeners(listeners: &MqttListenerConfig) -> Result<(), MqttRuntimeStartError> {
    if listeners.plaintext_address == listeners.tls_address {
        return Err(MqttRuntimeStartError::DuplicatePublicListenerAddress);
    }
    for (name, value) in [
        ("max_connections", listeners.max_connections),
        ("max_payload_size", listeners.max_payload_size),
        ("max_inflight_count", listeners.max_inflight_count),
    ] {
        if value == 0 {
            return Err(MqttRuntimeStartError::InvalidLimit(name));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct CachedDeviceAuthorization {
    authorization: Arc<dyn DeviceAuthorizationPort>,
    cache: Arc<dyn CachePort>,
}

impl CachedDeviceAuthorization {
    fn new(authorization: Arc<dyn DeviceAuthorizationPort>, cache: Arc<dyn CachePort>) -> Self {
        Self {
            authorization,
            cache,
        }
    }
}

impl DeviceAuthorizationPort for CachedDeviceAuthorization {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        let authorization = Arc::clone(&self.authorization);
        let cache = Arc::clone(&self.cache);
        Box::pin(async move {
            let cache_key = authentication_cache_key(&request);
            if let Ok(Some(value)) = cache.get(&cache_key).await
                && let Ok(device) = serde_json::from_slice::<CachedAuthenticatedDevice>(&value)
                && device.is_valid()
            {
                return Ok(device.into());
            }

            let device = authorization.authenticate(request).await?;
            let value = serde_json::to_vec(&CachedAuthenticatedDevice::from(&device))
                .expect("cached authenticated device is serializable");
            let _ = cache
                .put(CacheEntry {
                    key: cache_key,
                    value,
                    expires_at_ms: authorization_cache_expiration_ms(),
                })
                .await;
            Ok(device)
        })
    }

    fn authorize_session(
        &self,
        device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        self.authorization.authorize_session(device)
    }

    fn authorize_gateway_uplink(
        &self,
        request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        self.authorization.authorize_gateway_uplink(request)
    }
}

#[derive(Deserialize, Serialize)]
struct CachedAuthenticatedDevice {
    token_id: uuid::Uuid,
    device_id: String,
    is_gateway: bool,
}

impl CachedAuthenticatedDevice {
    fn is_valid(&self) -> bool {
        !self.token_id.is_nil() && !self.device_id.trim().is_empty()
    }
}

impl From<&AuthenticatedDevice> for CachedAuthenticatedDevice {
    fn from(device: &AuthenticatedDevice) -> Self {
        Self {
            token_id: device.token_id,
            device_id: device.device_id.clone(),
            is_gateway: device.is_gateway,
        }
    }
}

impl From<CachedAuthenticatedDevice> for AuthenticatedDevice {
    fn from(device: CachedAuthenticatedDevice) -> Self {
        Self {
            token_id: device.token_id,
            device_id: device.device_id,
            is_gateway: device.is_gateway,
        }
    }
}

fn authentication_cache_key(request: &TransportAuthRequest) -> String {
    let mut digest = Sha256::new();
    digest.update(request.username.as_bytes());
    digest.update([0]);
    digest.update(request.password.as_bytes());
    let digest = digest.finalize();
    format!("{AUTHORIZATION_CACHE_PREFIX}{digest:x}")
}

fn authorization_cache_expiration_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
        .saturating_add(AUTHORIZATION_CACHE_TTL.as_millis() as u64)
}

async fn cancel_startup(shutdown: &RuntimeShutdownControl, supervisor: &mut RuntimeTaskSupervisor) {
    shutdown.force_stop();
    supervisor.abort_and_join().await;
}

async fn serve_device_backend(
    listener: TcpListener,
    transport: MqttdDeviceTransport,
    mqtt5: bool,
    device_admission: DeviceAdmissionControl,
    force_cancellation: CancellationToken,
) -> Result<(), MqttRuntimeError> {
    let protocol = if mqtt5 { "MQTT 5" } else { "MQTT 3.1.1" };
    let accept_cancellation = device_admission.accept_cancellation();
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = accept_cancellation.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    #[cfg(test)]
                    device_admission.wait_before_admission().await;
                    if !device_admission.try_admit() {
                        continue;
                    }
                    let transport = transport.clone();
                    let force_cancellation = force_cancellation.clone();
                    connections.spawn(async move {
                        tokio::select! {
                            result = async {
                                if mqtt5 {
                                    transport.serve_v5_connection(stream).await
                                } else {
                                    transport.serve_connection(stream).await
                                }
                            } => {
                                if let Err(error) = result {
                                    eprintln!("iot-mqttd device transport error: {error}");
                                }
                            }
                            _ = force_cancellation.cancelled() => {}
                        }
                    });
                }
                Err(error) => {
                    if !accept_cancellation.is_cancelled() {
                        return Err(MqttRuntimeError::Worker(format!(
                            "device {protocol} backend accept failed: {error}"
                        )));
                    }
                    break;
                }
            },
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    return Err(MqttRuntimeError::Worker(format!(
                        "device {protocol} connection task failed: {error}"
                    )));
                }
            }
        }
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            return Err(MqttRuntimeError::Worker(format!(
                "device {protocol} connection task failed: {error}"
            )));
        }
    }
    Ok(())
}

async fn timeout_until<T>(deadline: Instant, future: T) -> Result<T::Output, ()>
where
    T: Future,
{
    let remaining = deadline.saturating_duration_since(Instant::now());
    timeout(remaining, future).await.map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    use super::{
        DeviceAdmissionControl, MqttRuntimeError, MqttdDeviceTransport, RuntimeTaskName,
        RuntimeTaskSupervisor, TransportAuthRequest, serve_device_backend,
    };
    use crate::{
        AuthenticatedDevice, DeviceAuthenticator, TransportError, TransportUplink, UplinkForwarder,
    };
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    #[derive(Clone)]
    struct CountingAuthenticator {
        calls: Arc<AtomicUsize>,
    }

    impl DeviceAuthenticator for CountingAuthenticator {
        fn authenticate(
            &self,
            _request: TransportAuthRequest,
        ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
        {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(AuthenticatedDevice {
                    token_id: Uuid::nil(),
                    device_id: "meter-a".to_owned(),
                    is_gateway: false,
                })
            })
        }
    }

    struct PanicAuthenticator;

    impl DeviceAuthenticator for PanicAuthenticator {
        fn authenticate(
            &self,
            _request: TransportAuthRequest,
        ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, TransportError>> + Send + '_>>
        {
            Box::pin(async { panic!("injected device authenticator panic") })
        }
    }

    struct UnusedUplink;

    impl UplinkForwarder for UnusedUplink {
        fn forward(
            &self,
            _token: &str,
            _message: TransportUplink,
        ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn transport(calls: Arc<AtomicUsize>) -> MqttdDeviceTransport {
        MqttdDeviceTransport::new(CountingAuthenticator { calls }, UnusedUplink)
    }

    #[tokio::test]
    async fn managed_supervisor_surfaces_the_first_failure_before_forcing_remaining_tasks() {
        let mut supervisor = RuntimeTaskSupervisor::new();
        let (held_started_tx, held_started_rx) = oneshot::channel();
        let (_held_release_tx, held_release_rx) = oneshot::channel::<()>();

        supervisor.spawn(RuntimeTaskName::PublicPlaintext, async {
            Err(MqttRuntimeError::Worker(
                "injected public plaintext failure".to_owned(),
            ))
        });
        supervisor.spawn(RuntimeTaskName::DeviceV311, async move {
            let _ = held_started_tx.send(());
            let _ = held_release_rx.await;
            Ok(())
        });

        held_started_rx
            .await
            .expect("held managed task did not start");
        let error = timeout(
            Duration::from_secs(1),
            supervisor.join_until(Instant::now() + Duration::from_secs(2)),
        )
        .await
        .expect("supervisor did not surface the failed task before the held task drained")
        .expect_err("first managed task failure must reach the supervisor");
        assert!(
            matches!(
                error,
                MqttRuntimeError::Worker(ref message)
                    if message == "public plaintext listener task failed: injected public plaintext failure"
            ),
            "unexpected supervisor error: {error:?}"
        );
        supervisor.abort_and_join().await;
    }

    #[tokio::test]
    async fn managed_supervisor_forces_and_joins_siblings_after_a_task_failure() {
        let mut supervisor = RuntimeTaskSupervisor::new();
        let force_cancellation = CancellationToken::new();
        let accepting = Arc::new(AtomicBool::new(true));
        let shutdown_started = Arc::new(AtomicBool::new(false));
        let (held_started_tx, held_started_rx) = oneshot::channel();
        let (held_finished_tx, held_finished_rx) = oneshot::channel();

        supervisor.spawn(RuntimeTaskName::PublicPlaintext, async {
            Err(MqttRuntimeError::Worker(
                "injected public plaintext failure".to_owned(),
            ))
        });
        let held_force_cancellation = force_cancellation.clone();
        supervisor.spawn(RuntimeTaskName::DeviceV311, async move {
            let _ = held_started_tx.send(());
            held_force_cancellation.cancelled().await;
            let _ = held_finished_tx.send(());
            Ok(())
        });

        held_started_rx
            .await
            .expect("held managed task did not start");
        let failure_shutdown_started = Arc::clone(&shutdown_started);
        let error = supervisor
            .run_until_stopped(
                force_cancellation.clone(),
                Arc::clone(&accepting),
                move || failure_shutdown_started.store(true, Ordering::Release),
            )
            .await
            .expect_err("task failure must reach the supervisor");
        assert!(force_cancellation.is_cancelled());
        assert!(shutdown_started.load(Ordering::Acquire));
        let _ = timeout(Duration::from_secs(1), held_finished_rx)
            .await
            .expect("supervisor did not join the force-cancelled sibling");
        assert!(matches!(
            error,
            MqttRuntimeError::Worker(ref message)
                if message == "public plaintext listener task failed: injected public plaintext failure"
        ));
    }

    #[tokio::test]
    async fn managed_supervisor_names_a_panicking_task() {
        let mut supervisor = RuntimeTaskSupervisor::new();
        let force_cancellation = CancellationToken::new();
        let accepting = Arc::new(AtomicBool::new(true));
        supervisor.spawn(RuntimeTaskName::PublicTls, async {
            panic!("injected public TLS task panic");
            #[allow(unreachable_code)]
            Ok(())
        });

        let error = supervisor
            .run_until_stopped(force_cancellation, accepting, || {})
            .await
            .expect_err("task panic must reach the supervisor");
        assert!(matches!(
            error,
            MqttRuntimeError::Worker(ref message)
                if message.contains("public TLS listener task join failure")
        ));
    }

    fn v311_connect() -> Vec<u8> {
        let client_id = "meter-a";
        let username = "iotd_device_token";
        let password = "valid-token";
        let remaining = 10 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
        let mut packet = vec![
            0x10,
            remaining as u8,
            0x00,
            0x04,
            b'M',
            b'Q',
            b'T',
            b'T',
            4,
            0xc2,
            0x00,
            0x3c,
            0x00,
            client_id.len() as u8,
        ];
        packet.extend_from_slice(client_id.as_bytes());
        packet.extend_from_slice(&(username.len() as u16).to_be_bytes());
        packet.extend_from_slice(username.as_bytes());
        packet.extend_from_slice(&(password.len() as u16).to_be_bytes());
        packet.extend_from_slice(password.as_bytes());
        packet
    }

    async fn wait_for_authentication(calls: &AtomicUsize) {
        timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("device transport did not authenticate the admitted connection");
    }

    #[tokio::test]
    async fn normal_stop_rejects_private_connection_after_accept_before_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let admission = DeviceAdmissionControl::new();
        let barrier = admission.test_admission_barrier();
        let calls = Arc::new(AtomicUsize::new(0));
        let force_cancellation = CancellationToken::new();
        let worker = tokio::spawn(serve_device_backend(
            listener,
            transport(Arc::clone(&calls)),
            false,
            admission.clone(),
            force_cancellation,
        ));

        let mut client = TcpStream::connect(address).await.unwrap();
        barrier
            .wait_until_reached("private device admission barrier")
            .await;
        admission.stop();
        barrier.release();
        client.write_all(&v311_connect()).await.unwrap();

        timeout(Duration::from_secs(1), worker)
            .await
            .expect("private device worker did not stop after normal admission closure")
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn device_backend_surfaces_a_connection_task_panic() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let worker = tokio::spawn(serve_device_backend(
            listener,
            MqttdDeviceTransport::new(PanicAuthenticator, UnusedUplink),
            false,
            DeviceAdmissionControl::new(),
            CancellationToken::new(),
        ));

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&v311_connect()).await.unwrap();
        let result = timeout(Duration::from_secs(1), worker)
            .await
            .expect("device backend did not surface the child task panic")
            .expect("device backend task panicked");
        assert!(matches!(
            result,
            Err(MqttRuntimeError::Worker(message))
                if message.contains("device MQTT 3.1.1 connection task failed")
        ));
    }

    #[tokio::test]
    async fn normal_stop_preserves_admitted_private_transport_until_force() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let admission = DeviceAdmissionControl::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let force_cancellation = CancellationToken::new();
        let worker = tokio::spawn(serve_device_backend(
            listener,
            transport(Arc::clone(&calls)),
            false,
            admission.clone(),
            force_cancellation.clone(),
        ));

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(&v311_connect()).await.unwrap();
        let mut connack = [0_u8; 4];
        client.read_exact(&mut connack).await.unwrap();
        assert_eq!(connack, [0x20, 0x02, 0x00, 0x00]);
        wait_for_authentication(&calls).await;

        admission.stop();
        client.write_all(&[0xc0, 0x00]).await.unwrap();
        let mut ping_response = [0_u8; 2];
        timeout(
            Duration::from_secs(1),
            client.read_exact(&mut ping_response),
        )
        .await
        .expect("admitted private transport did not process ping after normal admission closure")
        .unwrap();
        assert_eq!(ping_response, [0xd0, 0x00]);

        force_cancellation.cancel();
        let mut response = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("force cancellation did not close the admitted private transport")
            .unwrap();
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("private device worker did not join after force cancellation")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn normal_stop_rejects_v5_private_connection_after_accept_before_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let admission = DeviceAdmissionControl::new();
        let barrier = admission.test_admission_barrier();
        let calls = Arc::new(AtomicUsize::new(0));
        let force_cancellation = CancellationToken::new();
        let worker = tokio::spawn(serve_device_backend(
            listener,
            transport(Arc::clone(&calls)),
            true,
            admission.clone(),
            force_cancellation,
        ));

        let client = TcpStream::connect(address).await.unwrap();
        barrier
            .wait_until_reached("V5 private device admission barrier")
            .await;
        admission.stop();
        barrier.release();
        drop(client);

        timeout(Duration::from_secs(1), worker)
            .await
            .expect("V5 private device worker did not stop after normal admission closure")
            .unwrap()
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
