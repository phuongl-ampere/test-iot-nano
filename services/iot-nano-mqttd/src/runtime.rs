use std::{
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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    AuthenticatedDevice, AuthorizationError, BrokerLifecycleHandle, BrokerStorage, CacheEntry,
    CachePort, CommandResponsePort, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, ListenerConfiguration, MqttdDeviceTransport, MqttdError,
    MuxSettings, PreboundBackendListeners, ProtocolBackends, RpcSessionRouter,
    TransportAuthRequest, load_tls_acceptor, start_broker_with_prebound_listeners,
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
    broker: Arc<Mutex<Option<BrokerLifecycleHandle>>>,
    device_workers: Vec<JoinHandle<()>>,
    session_router: RpcSessionRouter,
    accept_cancellation: CancellationToken,
    force_cancellation: CancellationToken,
    parent_cancellation_watcher: Option<JoinHandle<()>>,
    accepting: Arc<AtomicBool>,
    drain_started: watch::Sender<bool>,
    public_connections: Arc<AtomicUsize>,
    _cache: Arc<dyn CachePort>,
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

        let broker = start_broker_with_prebound_listeners(
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
        .await
        .map_err(MqttRuntimeStartError::Broker)?;

        let force_cancellation = config.cancellation.child_token();
        let accept_cancellation = CancellationToken::new();
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
        let device_workers = vec![
            tokio::spawn(serve_device_backend(
                device_v311_listener,
                transport.clone(),
                false,
                accept_cancellation.clone(),
                force_cancellation.clone(),
            )),
            tokio::spawn(serve_device_backend(
                device_v5_listener,
                transport,
                true,
                accept_cancellation.clone(),
                force_cancellation.clone(),
            )),
        ];

        let plaintext_listener = match StdTcpListener::bind(config.listeners.plaintext_address) {
            Ok(listener) => listener,
            Err(error) => {
                cancel_startup(&broker, &force_cancellation, device_workers).await;
                return Err(MqttRuntimeStartError::PlaintextListenerBind(error));
            }
        };
        let tls_listener = match StdTcpListener::bind(config.listeners.tls_address) {
            Ok(listener) => listener,
            Err(error) => {
                cancel_startup(&broker, &force_cancellation, device_workers).await;
                return Err(MqttRuntimeStartError::TlsListenerBind(error));
            }
        };
        let backends = ProtocolBackends {
            v311: v311_backend_address,
            v5: v5_backend_address,
            device_v311: Some(device_v311_address),
            device_v5: Some(device_v5_address),
        };
        if let Err(error) = broker.spawn_public_plaintext_device_only_mux_with_connection_counter(
            plaintext_listener,
            backends,
            MuxSettings::default(),
            Arc::clone(&public_connections),
        ) {
            cancel_startup(&broker, &force_cancellation, device_workers).await;
            return Err(MqttRuntimeStartError::PublicWorker(error));
        }
        if let Err(error) = broker.spawn_public_tls_device_only_mux_with_connection_counter(
            tls_listener,
            tls_acceptor,
            backends,
            MuxSettings::default(),
            Arc::clone(&public_connections),
        ) {
            cancel_startup(&broker, &force_cancellation, device_workers).await;
            return Err(MqttRuntimeStartError::PublicWorker(error));
        }

        let broker = Arc::new(Mutex::new(Some(broker)));
        let accepting = Arc::new(AtomicBool::new(true));
        let (drain_started, _) = watch::channel(false);
        let parent_cancellation_watcher = spawn_parent_cancellation_watcher(
            config.cancellation,
            accept_cancellation.clone(),
            force_cancellation.clone(),
            Arc::clone(&broker),
            Arc::clone(&accepting),
        );

        Ok(Self {
            broker,
            device_workers,
            session_router: config.session_router,
            accept_cancellation,
            force_cancellation,
            parent_cancellation_watcher: Some(parent_cancellation_watcher),
            accepting,
            drain_started,
            public_connections,
            _cache: config.cache,
        })
    }

    pub fn session_router(&self) -> RpcSessionRouter {
        self.session_router.clone()
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
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
        if self.accepting.swap(false, Ordering::AcqRel) {
            self.accept_cancellation.cancel();
            let broker = self
                .broker
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(broker) = broker.as_ref() {
                broker.stop_public_accepting();
            }
        }
        Ok(())
    }

    pub async fn drain(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        self.stop_accepting().await?;
        self.drain_started.send_replace(true);
        let public_workers = self
            .broker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(BrokerLifecycleHandle::take_public_workers);
        let mut public_join = public_workers
            .map(|workers| tokio::task::spawn_blocking(move || join_public_workers(workers)));
        let mut drain_result = self.drain_device_workers(deadline).await;
        if drain_result.is_ok() {
            if let Some(join) = public_join.as_mut() {
                drain_result = timeout_until(deadline, join)
                    .await
                    .map_err(|_| MqttRuntimeError::DeadlineElapsed)
                    .and_then(|result| {
                        result.map_err(|error| MqttRuntimeError::Worker(error.to_string()))
                    });
            }
        }

        let result = if let Err(error) = drain_result {
            self.force_cancellation.cancel();
            self.shutdown_broker();
            self.abort_and_join_device_workers().await;
            if let Some(join) = public_join.take() {
                let _ = join.await;
            }
            self.join_broker_after_shutdown().await;
            Err(error)
        } else {
            drop(public_join);
            self.force_cancellation.cancel();
            self.join_broker_until(deadline).await
        };
        self.stop_parent_cancellation_watcher().await;
        result
    }

    async fn drain_device_workers(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        while let Some(mut worker) = self.device_workers.pop() {
            match timeout_until(deadline, &mut worker).await {
                Ok(result) => {
                    result.map_err(|error| MqttRuntimeError::Worker(error.to_string()))?
                }
                Err(()) => {
                    self.device_workers.push(worker);
                    return Err(MqttRuntimeError::DeadlineElapsed);
                }
            }
        }
        Ok(())
    }

    async fn abort_and_join_device_workers(&mut self) {
        for worker in &self.device_workers {
            worker.abort();
        }
        while let Some(worker) = self.device_workers.pop() {
            let _ = worker.await;
        }
    }

    async fn join_broker_until(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        let Some(broker) = self.take_broker() else {
            return Ok(());
        };
        let mut join = tokio::task::spawn_blocking(move || broker.join());
        match timeout_until(deadline, &mut join).await {
            Ok(result) => result
                .map_err(|error| MqttRuntimeError::Worker(error.to_string()))?
                .map_err(MqttRuntimeError::Broker),
            Err(()) => {
                let _ = join.await;
                Err(MqttRuntimeError::DeadlineElapsed)
            }
        }
    }

    async fn join_broker_after_shutdown(&mut self) {
        let Some(broker) = self.take_broker() else {
            return;
        };
        let _ = tokio::task::spawn_blocking(move || broker.join()).await;
    }

    async fn stop_parent_cancellation_watcher(&mut self) {
        if let Some(watcher) = self.parent_cancellation_watcher.take() {
            watcher.abort();
            let _ = watcher.await;
        }
    }

    fn shutdown_broker(&self) {
        let broker = self
            .broker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(broker) = broker.as_ref() {
            broker.shutdown();
        }
    }

    fn take_broker(&self) -> Option<BrokerLifecycleHandle> {
        self.broker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

fn spawn_parent_cancellation_watcher(
    parent_cancellation: CancellationToken,
    accept_cancellation: CancellationToken,
    force_cancellation: CancellationToken,
    broker: Arc<Mutex<Option<BrokerLifecycleHandle>>>,
    accepting: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        parent_cancellation.cancelled().await;
        accepting.store(false, Ordering::Release);
        accept_cancellation.cancel();
        force_cancellation.cancel();
        let broker = broker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(broker) = broker.as_ref() {
            broker.shutdown();
        }
    })
}

impl Drop for MqttRuntime {
    fn drop(&mut self) {
        if let Some(watcher) = self.parent_cancellation_watcher.take() {
            watcher.abort();
        }
        self.accept_cancellation.cancel();
        self.force_cancellation.cancel();
        for worker in &self.device_workers {
            worker.abort();
        }
        self.shutdown_broker();
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

async fn cancel_startup(
    broker: &BrokerLifecycleHandle,
    cancellation: &CancellationToken,
    workers: Vec<JoinHandle<()>>,
) {
    cancellation.cancel();
    for worker in workers {
        worker.abort();
        let _ = worker.await;
    }
    broker.shutdown();
}

async fn serve_device_backend(
    listener: TcpListener,
    transport: MqttdDeviceTransport,
    mqtt5: bool,
    accept_cancellation: CancellationToken,
    force_cancellation: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = accept_cancellation.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
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
                        eprintln!("iot-mqttd device backend accept error: {error}");
                    }
                    break;
                }
            },
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    eprintln!("iot-mqttd device worker failed: {error}");
                }
            }
        }
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            eprintln!("iot-mqttd device worker failed: {error}");
        }
    }
}

async fn timeout_until<T>(deadline: Instant, future: T) -> Result<T::Output, ()>
where
    T: Future,
{
    let remaining = deadline.saturating_duration_since(Instant::now());
    timeout(remaining, future).await.map_err(|_| ())
}

fn join_public_workers(workers: Vec<std::thread::JoinHandle<()>>) {
    for worker in workers {
        let _ = worker.join();
    }
}
