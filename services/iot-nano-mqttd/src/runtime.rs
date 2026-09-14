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
    device_admission: DeviceAdmissionControl,
    force_cancellation: CancellationToken,
    parent_cancellation_watcher: Option<JoinHandle<()>>,
    accepting: Arc<AtomicBool>,
    drain_started: watch::Sender<bool>,
    public_connections: Arc<AtomicUsize>,
    _cache: Arc<dyn CachePort>,
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
    fn is_accepting(&self) -> bool {
        *self
            .accepting
            .lock()
            .expect("device admission gate is not poisoned")
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

        let force_cancellation = CancellationToken::new();
        let device_admission = DeviceAdmissionControl::new();
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
                device_admission.clone(),
                force_cancellation.clone(),
            )),
            tokio::spawn(serve_device_backend(
                device_v5_listener,
                transport,
                true,
                device_admission.clone(),
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
            device_admission.clone(),
            force_cancellation.clone(),
            Arc::clone(&broker),
            Arc::clone(&accepting),
        );

        Ok(Self {
            broker,
            device_workers,
            session_router: config.session_router,
            device_admission,
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
            let broker = self
                .broker
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(broker) = broker.as_ref() {
                broker.stop_public_accepting();
            }
            self.device_admission.stop();
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
    device_admission: DeviceAdmissionControl,
    force_cancellation: CancellationToken,
    broker: Arc<Mutex<Option<BrokerLifecycleHandle>>>,
    accepting: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        parent_cancellation.cancelled().await;
        accepting.store(false, Ordering::Release);
        let broker_handle = broker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(broker) = broker_handle.as_ref() {
            broker.stop_public_accepting();
        }
        drop(broker_handle);
        device_admission.stop();
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
        self.device_admission.stop();
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
    device_admission: DeviceAdmissionControl,
    force_cancellation: CancellationToken,
) {
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

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    use super::{
        DeviceAdmissionControl, MqttdDeviceTransport, TransportAuthRequest, serve_device_backend,
        spawn_parent_cancellation_watcher,
    };
    use crate::{
        AuthenticatedDevice, DeviceAuthenticator, TransportError, TransportUplink, UplinkForwarder,
    };
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
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
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
        let mut byte = [0_u8; 1];
        assert!(
            timeout(Duration::from_millis(50), client.read(&mut byte))
                .await
                .is_err()
        );

        force_cancellation.cancel();
        let mut response = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("force cancellation did not close the admitted private transport")
            .unwrap();
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("private device worker did not join after force cancellation")
            .unwrap();
    }

    #[tokio::test]
    async fn parent_cancellation_closes_private_admission_before_forcing_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let parent_cancellation = CancellationToken::new();
        let admission = DeviceAdmissionControl::new();
        let force_cancellation = CancellationToken::new();
        let accepting = Arc::new(AtomicBool::new(true));
        let watcher = spawn_parent_cancellation_watcher(
            parent_cancellation.clone(),
            admission.clone(),
            force_cancellation.clone(),
            Arc::new(Mutex::new(None)),
            Arc::clone(&accepting),
        );
        let calls = Arc::new(AtomicUsize::new(0));
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
        wait_for_authentication(&calls).await;

        parent_cancellation.cancel();
        let mut response = Vec::new();
        timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("parent cancellation did not force the admitted private transport")
            .unwrap();
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("private device worker did not join after parent cancellation")
            .unwrap();
        watcher.await.unwrap();

        assert!(!accepting.load(Ordering::Acquire));
        assert!(!admission.is_accepting());
        assert!(admission.accept_cancellation().is_cancelled());
        assert!(force_cancellation.is_cancelled());
    }
}
