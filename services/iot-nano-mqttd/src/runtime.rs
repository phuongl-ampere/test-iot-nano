use std::{
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use iot_nano_stream::StreamPort;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::{
    AuthenticatedDevice, AuthorizationError, BrokerLifecycleHandle, BrokerStorage, CacheEntry,
    CachePort, CommandResponsePort, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, ListenerConfiguration, MqttdDeviceTransport, MqttdError,
    MuxSettings, ProtocolBackends, RpcSessionRouter, TransportAuthRequest, load_tls_acceptor,
    start_broker_with_storage,
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
    broker: Option<BrokerLifecycleHandle>,
    device_workers: Vec<JoinHandle<()>>,
    session_router: RpcSessionRouter,
    cancellation: CancellationToken,
    accepting: AtomicBool,
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

        let v311_backend_address =
            reserve_private_address().map_err(MqttRuntimeStartError::PrivateBackendReservation)?;
        let v5_backend_address =
            reserve_private_address().map_err(MqttRuntimeStartError::PrivateBackendReservation)?;
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

        let broker = start_broker_with_storage(
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
            Arc::clone(&config.storage),
        )
        .await
        .map_err(MqttRuntimeStartError::Broker)?;

        let cancellation = config.cancellation.child_token();
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
                cancellation.clone(),
            )),
            tokio::spawn(serve_device_backend(
                device_v5_listener,
                transport,
                true,
                cancellation.clone(),
            )),
        ];

        let plaintext_listener = match StdTcpListener::bind(config.listeners.plaintext_address) {
            Ok(listener) => listener,
            Err(error) => {
                cancel_startup(&broker, &cancellation, device_workers).await;
                return Err(MqttRuntimeStartError::PlaintextListenerBind(error));
            }
        };
        let tls_listener = match StdTcpListener::bind(config.listeners.tls_address) {
            Ok(listener) => listener,
            Err(error) => {
                cancel_startup(&broker, &cancellation, device_workers).await;
                return Err(MqttRuntimeStartError::TlsListenerBind(error));
            }
        };
        let backends = ProtocolBackends {
            v311: v311_backend_address,
            v5: v5_backend_address,
            device_v311: Some(device_v311_address),
            device_v5: Some(device_v5_address),
        };
        if let Err(error) = broker.spawn_public_plaintext_device_only_mux(
            plaintext_listener,
            backends,
            MuxSettings::default(),
        ) {
            cancel_startup(&broker, &cancellation, device_workers).await;
            return Err(MqttRuntimeStartError::PublicWorker(error));
        }
        if let Err(error) = broker.spawn_public_tls_device_only_mux(
            tls_listener,
            tls_acceptor,
            backends,
            MuxSettings::default(),
        ) {
            cancel_startup(&broker, &cancellation, device_workers).await;
            return Err(MqttRuntimeStartError::PublicWorker(error));
        }

        Ok(Self {
            broker: Some(broker),
            device_workers,
            session_router: config.session_router,
            cancellation,
            accepting: AtomicBool::new(true),
            _cache: config.cache,
        })
    }

    pub fn session_router(&self) -> RpcSessionRouter {
        self.session_router.clone()
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    pub async fn stop_accepting(&mut self) -> Result<(), MqttRuntimeError> {
        if self.accepting.swap(false, Ordering::AcqRel) {
            self.cancellation.cancel();
            if let Some(broker) = &self.broker {
                broker.shutdown();
            }
        }
        Ok(())
    }

    pub async fn drain(&mut self, deadline: Instant) -> Result<(), MqttRuntimeError> {
        self.stop_accepting().await?;

        while let Some(mut worker) = self.device_workers.pop() {
            match timeout_until(deadline, &mut worker).await {
                Ok(result) => {
                    result.map_err(|error| MqttRuntimeError::Worker(error.to_string()))?
                }
                Err(()) => {
                    worker.abort();
                    return Err(MqttRuntimeError::DeadlineElapsed);
                }
            }
        }

        let Some(broker) = self.broker.take() else {
            return Ok(());
        };
        let mut join = tokio::task::spawn_blocking(move || broker.join());
        match timeout_until(deadline, &mut join).await {
            Ok(result) => result
                .map_err(|error| MqttRuntimeError::Worker(error.to_string()))?
                .map_err(MqttRuntimeError::Broker),
            Err(()) => {
                join.abort();
                Err(MqttRuntimeError::DeadlineElapsed)
            }
        }
    }
}

impl Drop for MqttRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for worker in &self.device_workers {
            worker.abort();
        }
        if let Some(broker) = &self.broker {
            broker.shutdown();
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
    #[error("MQTT private backend reservation failed")]
    PrivateBackendReservation(#[source] io::Error),
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

fn reserve_private_address() -> io::Result<SocketAddr> {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
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
    cancellation: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let transport = transport.clone();
                    let cancellation = cancellation.clone();
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
                            _ = cancellation.cancelled() => {}
                        }
                    });
                }
                Err(error) => {
                    if !cancellation.is_cancelled() {
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
