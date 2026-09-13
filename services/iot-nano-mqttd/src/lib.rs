#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    io::BufReader,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use reqwest::StatusCode;
use rumqttd::{
    AuthHandler, AuthorizationHandler, BridgeConfig as CoreBridgeConfig, Broker,
    BrokerHandle as CoreBrokerHandle, Config, ConnectionSettings, RouterConfig, ServerSettings,
    TlsConfig,
};
use serde::Serialize;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

mod config;
mod management;
mod policy;
mod ports;
mod runtime;
mod storage;
mod transport;

pub use config::{
    AclRule, BridgeConfig, BrokerFileConfig, BrokerLimits, Capability, ConfigError,
    DeviceTransportConfig, HttpAuthorizationConfig, ListenersConfig, ManagementConfig,
    NativeDeviceProtocol, QuicListenerConfig, RuleConfig, StaticAclConfig, StaticUser,
    StorageConfig, TcpListenerConfig, TlsListenerConfig, WebSocketListenerConfig,
};
pub use management::{
    management_router, management_router_with_config, management_router_with_config_and_runtime,
    management_router_with_config_and_runtime_and_policy,
};
pub use policy::{PolicyAdapters, PolicyError, build_policy};
pub use ports::{
    AuthorizationError, CacheEntry, CacheError, CachePort, CommandResponseError,
    CommandResponsePort, DeviceAuthorizationPort, GatewayAuthorization,
    GatewayAuthorizationRequest, LocalDeviceAuthenticator, LocalRpcResponseForwarder,
    LocalStreamUplinkForwarder, LocalUplinkForwarder,
};
pub use rumqttd::{
    BrokerStorage, BrokerStorageState, InboundQos2CommitResult, InboundQos2CompletionResult,
    InboundQos2JournalEntry, InboundQos2JournalState, InboundQos2PrepareResult, MemoryStorage,
    RetentionPolicy, StorageError, StoredInflight, StoredPublish, StoredSession,
};
pub use runtime::{
    MqttListenerConfig, MqttRuntime, MqttRuntimeConfig, MqttRuntimeError, MqttRuntimeStartError,
};
pub use storage::SqliteStorage;
pub use transport::{
    AuthenticatedDevice, DeviceAuthenticator, HttpDeviceAuthenticator, HttpRpcResponseForwarder,
    HttpStreamUplinkForwarder, MqttdDeviceTransport, RpcResponseForwarder, RpcSessionRouter,
    SessionError, SessionRegistration, TransportAuthRequest, TransportError, TransportRpcResponse,
    TransportUplink, UplinkForwarder,
};

pub struct BrokerLifecycleHandle {
    inner: Option<CoreBrokerHandle>,
    public_accept_stop: watch::Sender<bool>,
    public_workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

pub struct PreboundBackendListeners {
    pub v311: StdTcpListener,
    pub v5: StdTcpListener,
}

impl BrokerLifecycleHandle {
    pub fn shutdown(&self) {
        self.stop_public_accepting();
        if let Some(inner) = &self.inner {
            inner.shutdown();
        }
    }

    pub fn stop_public_accepting(&self) {
        self.public_accept_stop.send_replace(true);
    }

    pub fn take_public_workers(&self) -> Vec<thread::JoinHandle<()>> {
        std::mem::take(
            &mut *self
                .public_workers
                .lock()
                .expect("public worker mutex is not poisoned"),
        )
    }

    pub fn join(mut self) -> Result<(), MqttdError> {
        self.shutdown();
        for worker in self
            .public_workers
            .get_mut()
            .expect("public worker mutex is not poisoned")
            .drain(..)
        {
            let _ = worker.join();
        }
        self.inner
            .take()
            .expect("broker lifecycle handle is available")
            .join()
            .map_err(|error| MqttdError::Broker(Box::new(error)))
    }

    pub fn shutdown_receiver(&self) -> tokio::sync::watch::Receiver<bool> {
        self.inner
            .as_ref()
            .expect("broker lifecycle handle is available")
            .shutdown_receiver()
    }

    pub fn link(
        &self,
        client_id: &str,
    ) -> Result<(rumqttd::local::LinkTx, rumqttd::local::LinkRx), MqttdError> {
        self.inner
            .as_ref()
            .expect("broker lifecycle handle is available")
            .link(client_id)
            .map_err(MqttdError::LocalLink)
    }

    pub fn spawn_republish_rule_worker(&self, rule: RuleConfig) -> Result<(), MqttdError> {
        let (mut link_tx, mut link_rx) = self.link(&format!("iot-mqttd-rule-{}", rule.name))?;
        link_tx
            .subscribe(rule.source_topic.clone())
            .map_err(MqttdError::LocalLink)?;
        let mut shutdown = self.shutdown_receiver();
        let worker = thread::Builder::new()
            .name(format!("iot-mqttd-rule-{}", rule.name))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(async move {
                        loop {
                            tokio::select! {
                                _ = shutdown.changed() => break,
                                notification = link_rx.next() => {
                                    let Ok(Some(rumqttd::Notification::Forward(forward))) = notification else {
                                        continue;
                                    };
                                    if let Err(error) = link_tx.publish(
                                        rule.target_topic.clone(),
                                        forward.publish.payload,
                                    ) {
                                        eprintln!("iot-mqttd republish rule failed: {error}");
                                    }
                                }
                            }
                        }
                    }),
                    Err(error) => eprintln!("iot-mqttd rule runtime failed: {error}"),
                }
            })
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        self.public_workers
            .lock()
            .expect("public worker mutex is not poisoned")
            .push(worker);
        Ok(())
    }

    pub fn spawn_public_plaintext_mux(
        &self,
        listener: std::net::TcpListener,
        backends: ProtocolBackends,
        settings: MuxSettings,
    ) -> Result<(), MqttdError> {
        self.spawn_public_worker(
            "iot-mqttd-plaintext-mux",
            listener,
            move |listener, accept_stop, force_stop| async move {
                serve_plaintext_mux_with_shutdowns(
                    listener,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                )
                .await
            },
        )
    }

    pub fn spawn_public_plaintext_device_only_mux(
        &self,
        listener: std::net::TcpListener,
        backends: ProtocolBackends,
        settings: MuxSettings,
    ) -> Result<(), MqttdError> {
        self.spawn_public_plaintext_device_only_mux_with_counter(listener, backends, settings, None)
    }

    pub(crate) fn spawn_public_plaintext_device_only_mux_with_connection_counter(
        &self,
        listener: std::net::TcpListener,
        backends: ProtocolBackends,
        settings: MuxSettings,
        connection_counter: Arc<AtomicUsize>,
    ) -> Result<(), MqttdError> {
        self.spawn_public_plaintext_device_only_mux_with_counter(
            listener,
            backends,
            settings,
            Some(connection_counter),
        )
    }

    fn spawn_public_plaintext_device_only_mux_with_counter(
        &self,
        listener: std::net::TcpListener,
        backends: ProtocolBackends,
        settings: MuxSettings,
        connection_counter: Option<Arc<AtomicUsize>>,
    ) -> Result<(), MqttdError> {
        self.spawn_public_worker(
            "iot-mqttd-plaintext-mux",
            listener,
            move |listener, accept_stop, force_stop| async move {
                serve_plaintext_mux_with_shutdowns_and_mode(
                    listener,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                    MuxRouteMode::DeviceOnly,
                    connection_counter,
                )
                .await
            },
        )
    }

    pub fn spawn_public_tls_mux(
        &self,
        listener: std::net::TcpListener,
        acceptor: TlsAcceptor,
        backends: ProtocolBackends,
        settings: MuxSettings,
    ) -> Result<(), MqttdError> {
        self.spawn_public_worker(
            "iot-mqttd-tls-mux",
            listener,
            move |listener, accept_stop, force_stop| async move {
                serve_tls_mux_with_shutdowns(
                    listener,
                    acceptor,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                )
                .await
            },
        )
    }

    pub fn spawn_public_tls_device_only_mux(
        &self,
        listener: std::net::TcpListener,
        acceptor: TlsAcceptor,
        backends: ProtocolBackends,
        settings: MuxSettings,
    ) -> Result<(), MqttdError> {
        self.spawn_public_tls_device_only_mux_with_counter(
            listener, acceptor, backends, settings, None,
        )
    }

    pub(crate) fn spawn_public_tls_device_only_mux_with_connection_counter(
        &self,
        listener: std::net::TcpListener,
        acceptor: TlsAcceptor,
        backends: ProtocolBackends,
        settings: MuxSettings,
        connection_counter: Arc<AtomicUsize>,
    ) -> Result<(), MqttdError> {
        self.spawn_public_tls_device_only_mux_with_counter(
            listener,
            acceptor,
            backends,
            settings,
            Some(connection_counter),
        )
    }

    fn spawn_public_tls_device_only_mux_with_counter(
        &self,
        listener: std::net::TcpListener,
        acceptor: TlsAcceptor,
        backends: ProtocolBackends,
        settings: MuxSettings,
        connection_counter: Option<Arc<AtomicUsize>>,
    ) -> Result<(), MqttdError> {
        self.spawn_public_worker(
            "iot-mqttd-tls-mux",
            listener,
            move |listener, accept_stop, force_stop| async move {
                serve_tls_mux_with_shutdowns_and_mode(
                    listener,
                    acceptor,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                    MuxRouteMode::DeviceOnly,
                    connection_counter,
                )
                .await
            },
        )
    }

    fn spawn_public_worker<F, Fut>(
        &self,
        name: &str,
        listener: std::net::TcpListener,
        serve: F,
    ) -> Result<(), MqttdError>
    where
        F: FnOnce(TcpListener, watch::Receiver<bool>, watch::Receiver<bool>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = std::io::Result<()>> + Send + 'static,
    {
        listener
            .set_nonblocking(true)
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        let accept_stop = self.public_accept_stop.subscribe();
        let force_stop = self.shutdown_receiver();
        let worker = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => {
                        let result = runtime.block_on(async {
                            let listener = TcpListener::from_std(listener)?;
                            serve(listener, accept_stop, force_stop).await
                        });
                        if let Err(error) = result {
                            eprintln!("iot-mqttd public listener stopped: {error}");
                        }
                    }
                    Err(error) => eprintln!("iot-mqttd public runtime failed: {error}"),
                }
            })
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        self.public_workers
            .lock()
            .expect("public worker mutex is not poisoned")
            .push(worker);
        Ok(())
    }
}

impl Drop for BrokerLifecycleHandle {
    fn drop(&mut self) {
        self.shutdown();
        for worker in self
            .public_workers
            .get_mut()
            .expect("public worker mutex is not poisoned")
            .drain(..)
        {
            let _ = worker.join();
        }
        if let Some(inner) = self.inner.take() {
            let _ = inner.join();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttProtocol {
    V311,
    V5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectRoute {
    Broker(MqttProtocol),
    DeviceV311,
    DeviceV5,
}

#[derive(Debug, Error)]
pub enum ProtocolDetectionError {
    #[error("first MQTT packet must be CONNECT")]
    NotConnect,
    #[error("truncated MQTT CONNECT packet")]
    Truncated,
    #[error("invalid MQTT remaining length")]
    InvalidRemainingLength,
    #[error("unsupported MQTT protocol name or level")]
    Unsupported,
    #[error("MQTT {0:?} device-token transport is unsupported")]
    DeviceTokenUnsupported(MqttProtocol),
}

#[derive(Debug, Clone, Copy)]
pub struct ProtocolBackends {
    pub v311: SocketAddr,
    pub v5: SocketAddr,
    pub device_v311: Option<SocketAddr>,
    pub device_v5: Option<SocketAddr>,
}

#[derive(Debug, Clone)]
pub struct RuntimeAddressConfiguration {
    pub plaintext_address: SocketAddr,
    pub tls_address: SocketAddr,
    pub v311_backend_address: SocketAddr,
    pub v5_backend_address: SocketAddr,
    pub device_backend_address: SocketAddr,
    pub device_v5_backend_address: SocketAddr,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub websocket_address: Option<SocketAddr>,
    pub websocket_tls: bool,
    pub bridge: Option<CoreBridgeConfig>,
    pub max_connections: usize,
    pub max_payload_size: usize,
    pub max_inflight_count: usize,
}

impl Default for RuntimeAddressConfiguration {
    fn default() -> Self {
        Self {
            plaintext_address: "0.0.0.0:1883".parse().expect("valid default"),
            tls_address: "0.0.0.0:8883".parse().expect("valid default"),
            v311_backend_address: "127.0.0.1:18831".parse().expect("valid default"),
            v5_backend_address: "127.0.0.1:18832".parse().expect("valid default"),
            device_backend_address: "127.0.0.1:18833".parse().expect("valid default"),
            device_v5_backend_address: "127.0.0.1:18834".parse().expect("valid default"),
            tls_cert_path: "server.crt".into(),
            tls_key_path: "server.key".into(),
            websocket_address: None,
            websocket_tls: false,
            bridge: None,
            max_connections: 10_000,
            max_payload_size: 2 * 1024 * 1024,
            max_inflight_count: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeviceTransportEndpoints {
    pub api_base_url: Option<String>,
    pub transport_secret: Option<String>,
}

#[derive(Debug, Error)]
pub enum RuntimeConfigurationError {
    #[error(
        "native device transport is enabled but API, transport secret, ingest webhook URL, and ingest webhook secret are all required"
    )]
    IncompleteDeviceTransportEndpoints,
}

#[derive(Debug, Clone)]
pub struct ResolvedRuntimeConfiguration {
    pub listener: ListenerConfiguration,
    pub backends: ProtocolBackends,
    pub device_backend_address: SocketAddr,
    pub device_v5_backend_address: SocketAddr,
    pub device_transport_enabled: bool,
    device_transport_protocol: NativeDeviceProtocol,
    config_mode: bool,
}

impl ResolvedRuntimeConfiguration {
    pub fn set_device_transport_backends(&mut self, enabled: bool) {
        self.backends.device_v311 = (enabled
            && matches!(
                self.device_transport_protocol,
                NativeDeviceProtocol::Mqtt311 | NativeDeviceProtocol::Both
            ))
        .then_some(self.device_backend_address);
        self.backends.device_v5 = (enabled
            && matches!(
                self.device_transport_protocol,
                NativeDeviceProtocol::Mqtt5 | NativeDeviceProtocol::Both
            ))
        .then_some(self.device_v5_backend_address);
    }

    pub fn device_transport_activation(
        &self,
        endpoints: &DeviceTransportEndpoints,
    ) -> Result<bool, RuntimeConfigurationError> {
        let complete = endpoints.api_base_url.is_some() && endpoints.transport_secret.is_some();
        if self.config_mode {
            if self.device_transport_enabled && !complete {
                return Err(RuntimeConfigurationError::IncompleteDeviceTransportEndpoints);
            }
            return Ok(self.device_transport_enabled);
        }
        if !complete && (endpoints.api_base_url.is_some() || endpoints.transport_secret.is_some()) {
            return Err(RuntimeConfigurationError::IncompleteDeviceTransportEndpoints);
        }
        Ok(complete)
    }

    pub fn validate_device_transport_endpoints(
        &self,
        endpoints: &DeviceTransportEndpoints,
    ) -> Result<(), RuntimeConfigurationError> {
        self.device_transport_activation(endpoints).map(|_| ())
    }
}

pub fn resolve_runtime_configuration(
    file_config: Option<&BrokerFileConfig>,
    fallback: RuntimeAddressConfiguration,
) -> Result<ResolvedRuntimeConfiguration, ConfigError> {
    let (
        listener,
        device_backend_address,
        device_v5_backend_address,
        device_transport_enabled,
        device_transport_protocol,
        config_mode,
    ) = match file_config {
        Some(config) => {
            let listener = config.to_listener_configuration()?;
            (
                listener.clone(),
                config.listeners.device_backend_address,
                config.listeners.device_v5_backend_address,
                config.device_transport.enabled,
                config.device_transport.protocol,
                true,
            )
        }
        None => {
            let listener = ListenerConfiguration {
                plaintext_address: fallback.plaintext_address,
                tls_address: fallback.tls_address,
                v311_backend_address: fallback.v311_backend_address,
                v5_backend_address: fallback.v5_backend_address,
                tls_cert_path: fallback.tls_cert_path,
                tls_key_path: fallback.tls_key_path,
                websocket_address: fallback.websocket_address,
                websocket_tls: fallback.websocket_tls,
                bridge: None,
                max_connections: fallback.max_connections,
                max_payload_size: fallback.max_payload_size,
                max_inflight_count: fallback.max_inflight_count,
                token_authenticator: None,
                auth_handler: None,
                authorization_handler: None,
            };
            (
                listener,
                fallback.device_backend_address,
                fallback.device_v5_backend_address,
                false,
                NativeDeviceProtocol::Both,
                false,
            )
        }
    };
    Ok(ResolvedRuntimeConfiguration {
        backends: ProtocolBackends {
            v311: listener.v311_backend_address,
            v5: listener.v5_backend_address,
            device_v311: None,
            device_v5: None,
        },
        listener,
        device_backend_address,
        device_v5_backend_address,
        device_transport_enabled,
        device_transport_protocol,
        config_mode,
    })
    .map(|mut resolved| {
        resolved.set_device_transport_backends(resolved.device_transport_enabled);
        resolved
    })
}

pub const DEFAULT_MUX_MAX_PREAMBLE_SIZE: usize = 128 * 1024;
pub const DEFAULT_MUX_PREAMBLE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_BROKER_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub struct MuxSettings {
    pub max_preamble_size: usize,
    pub preamble_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MuxRouteMode {
    GenericBroker,
    DeviceOnly,
}

struct ActiveMuxConnection(Option<Arc<AtomicUsize>>);

impl ActiveMuxConnection {
    fn new(counter: Option<Arc<AtomicUsize>>) -> Self {
        if let Some(counter) = &counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        Self(counter)
    }
}

impl Drop for ActiveMuxConnection {
    fn drop(&mut self) {
        if let Some(counter) = &self.0 {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Default for MuxSettings {
    fn default() -> Self {
        Self {
            max_preamble_size: DEFAULT_MUX_MAX_PREAMBLE_SIZE,
            preamble_timeout: DEFAULT_MUX_PREAMBLE_TIMEOUT,
        }
    }
}

pub async fn serve_plaintext_mux(
    listener: TcpListener,
    backends: ProtocolBackends,
) -> std::io::Result<()> {
    serve_plaintext_mux_with_settings(listener, backends, MuxSettings::default()).await
}

pub async fn serve_plaintext_mux_with_settings(
    listener: TcpListener,
    backends: ProtocolBackends,
    settings: MuxSettings,
) -> std::io::Result<()> {
    let (_shutdown, receiver) = watch::channel(false);
    serve_plaintext_mux_with_shutdown(listener, backends, settings, receiver).await
}

pub async fn serve_plaintext_mux_with_shutdown(
    listener: TcpListener,
    backends: ProtocolBackends,
    settings: MuxSettings,
    shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve_plaintext_mux_with_shutdowns(listener, backends, settings, shutdown.clone(), shutdown)
        .await
}

pub async fn serve_plaintext_mux_with_shutdowns(
    listener: TcpListener,
    backends: ProtocolBackends,
    settings: MuxSettings,
    accept_shutdown: watch::Receiver<bool>,
    force_shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve_plaintext_mux_with_shutdowns_and_mode(
        listener,
        backends,
        settings,
        accept_shutdown,
        force_shutdown,
        MuxRouteMode::GenericBroker,
        None,
    )
    .await
}

async fn serve_plaintext_mux_with_shutdowns_and_mode(
    listener: TcpListener,
    backends: ProtocolBackends,
    settings: MuxSettings,
    mut accept_shutdown: watch::Receiver<bool>,
    mut force_shutdown: watch::Receiver<bool>,
    route_mode: MuxRouteMode,
    connection_counter: Option<Arc<AtomicUsize>>,
) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let mut connection_shutdown = force_shutdown.clone();
                let connection_counter = connection_counter.clone();
                connections.spawn(async move {
                    let _connection = ActiveMuxConnection::new(connection_counter);
                    tokio::select! {
                        result = proxy_plaintext_connection(stream, backends, settings, route_mode) => {
                            if let Err(error) = result {
                                eprintln!("iot-mqttd protocol mux connection error: {error}");
                            }
                        }
                        _ = connection_shutdown.changed() => {}
                    }
                });
            }
            _ = accept_shutdown.changed() => break,
            _ = force_shutdown.changed() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn proxy_plaintext_connection(
    inbound: TcpStream,
    backends: ProtocolBackends,
    settings: MuxSettings,
    route_mode: MuxRouteMode,
) -> std::io::Result<()> {
    proxy_stream(inbound, backends, settings, route_mode).await
}

pub async fn serve_tls_mux(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    backends: ProtocolBackends,
) -> std::io::Result<()> {
    serve_tls_mux_with_settings(listener, acceptor, backends, MuxSettings::default()).await
}

pub async fn serve_tls_mux_with_settings(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    backends: ProtocolBackends,
    settings: MuxSettings,
) -> std::io::Result<()> {
    let (_shutdown, receiver) = watch::channel(false);
    serve_tls_mux_with_shutdown(listener, acceptor, backends, settings, receiver).await
}

pub async fn serve_tls_mux_with_shutdown(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    backends: ProtocolBackends,
    settings: MuxSettings,
    shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve_tls_mux_with_shutdowns(
        listener,
        acceptor,
        backends,
        settings,
        shutdown.clone(),
        shutdown,
    )
    .await
}

pub async fn serve_tls_mux_with_shutdowns(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    backends: ProtocolBackends,
    settings: MuxSettings,
    accept_shutdown: watch::Receiver<bool>,
    force_shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve_tls_mux_with_shutdowns_and_mode(
        listener,
        acceptor,
        backends,
        settings,
        accept_shutdown,
        force_shutdown,
        MuxRouteMode::GenericBroker,
        None,
    )
    .await
}

async fn serve_tls_mux_with_shutdowns_and_mode(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    backends: ProtocolBackends,
    settings: MuxSettings,
    mut accept_shutdown: watch::Receiver<bool>,
    mut force_shutdown: watch::Receiver<bool>,
    route_mode: MuxRouteMode,
    connection_counter: Option<Arc<AtomicUsize>>,
) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let acceptor = acceptor.clone();
                let mut connection_shutdown = force_shutdown.clone();
                let connection_counter = connection_counter.clone();
                connections.spawn(async move {
                    let _connection = ActiveMuxConnection::new(connection_counter);
                    tokio::select! {
                        _ = connection_shutdown.changed() => {}
                        _ = async {
                            match tokio::time::timeout(settings.preamble_timeout, acceptor.accept(stream)).await {
                                Ok(Ok(stream)) => {
                                    if let Err(error) = proxy_stream(stream, backends, settings, route_mode).await {
                                        eprintln!("iot-mqttd TLS protocol mux connection error: {error}");
                                    }
                                }
                                Ok(Err(error)) => eprintln!("iot-mqttd TLS handshake error: {error}"),
                                Err(_) => eprintln!("iot-mqttd TLS handshake timed out"),
                            }
                        } => {}
                    }
                });
            }
            _ = accept_shutdown.changed() => break,
            _ = force_shutdown.changed() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

pub fn load_tls_acceptor(
    certificate_path: &PathBuf,
    key_path: &PathBuf,
) -> Result<TlsAcceptor, MqttdError> {
    let mut certificate = BufReader::new(
        std::fs::File::open(certificate_path)
            .map_err(|_| MqttdError::MissingCertificate(certificate_path.clone()))?,
    );
    let certificates = rustls_pemfile::certs(&mut certificate)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| MqttdError::Tls(Box::new(error)))?;
    let mut key = BufReader::new(
        std::fs::File::open(key_path).map_err(|_| MqttdError::MissingKey(key_path.clone()))?,
    );
    let key = rustls_pemfile::private_key(&mut key)
        .map_err(|error| MqttdError::Tls(Box::new(error)))?
        .ok_or_else(|| MqttdError::MissingKey(key_path.clone()))?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|error| MqttdError::Tls(Box::new(error)))?;
    Ok(TlsAcceptor::from(std::sync::Arc::new(config)))
}

async fn proxy_stream<S>(
    mut inbound: S,
    backends: ProtocolBackends,
    settings: MuxSettings,
    route_mode: MuxRouteMode,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (prefix, route) = tokio::time::timeout(settings.preamble_timeout, async {
        let mut prefix = Vec::with_capacity(settings.max_preamble_size.min(256));
        let route = loop {
            let mut chunk = [0_u8; 256];
            let read = inbound.read(&mut chunk).await?;
            if read == 0 {
                return Ok::<_, std::io::Error>((prefix, None));
            }
            if prefix.len().saturating_add(read) > settings.max_preamble_size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "MQTT CONNECT preamble exceeds configured limit",
                ));
            }
            prefix.extend_from_slice(&chunk[..read]);
            match detect_connect_route(&prefix) {
                Ok(route) => break Some(route),
                Err(ProtocolDetectionError::Truncated) => continue,
                Err(error) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
                }
            }
        };
        Ok((prefix, route))
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "MQTT CONNECT timed out"))??;
    let Some(route) = route else {
        return Ok(());
    };
    if route_mode == MuxRouteMode::DeviceOnly && matches!(route, ConnectRoute::Broker(_)) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "MQTT device credentials are required",
        ));
    }
    let backend_address = match route {
        ConnectRoute::Broker(MqttProtocol::V311) => backends.v311,
        ConnectRoute::Broker(MqttProtocol::V5) => backends.v5,
        ConnectRoute::DeviceV311 => backends.device_v311.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "native device transport backend is not configured",
            )
        })?,
        ConnectRoute::DeviceV5 => backends.device_v5.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "native MQTT5 device transport backend is not configured",
            )
        })?,
    };
    let mut outbound = TcpStream::connect(backend_address).await?;
    outbound.write_all(&prefix).await?;
    tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await?;
    Ok(())
}

pub fn detect_connect_protocol(bytes: &[u8]) -> Result<MqttProtocol, ProtocolDetectionError> {
    let body = connect_body(bytes)?;
    if body.len() < 7 || body[..6] != [0, 4, b'M', b'Q', b'T', b'T'] {
        return Err(ProtocolDetectionError::Unsupported);
    }
    match body[6] {
        4 => Ok(MqttProtocol::V311),
        5 => Ok(MqttProtocol::V5),
        _ => Err(ProtocolDetectionError::Unsupported),
    }
}

pub fn detect_connect_route(bytes: &[u8]) -> Result<ConnectRoute, ProtocolDetectionError> {
    let protocol = detect_connect_protocol(bytes)?;
    let body = connect_body(bytes)?;
    let username = connect_username(body, protocol)?;
    if protocol == MqttProtocol::V5 {
        if username.is_some_and(|username| username.starts_with("iotd_")) {
            return Ok(ConnectRoute::DeviceV5);
        }
        return Ok(ConnectRoute::Broker(protocol));
    }
    let Some(username) = username else {
        return Ok(ConnectRoute::Broker(protocol));
    };
    if username.starts_with("iotd_") {
        Ok(ConnectRoute::DeviceV311)
    } else {
        Ok(ConnectRoute::Broker(protocol))
    }
}

fn connect_body(bytes: &[u8]) -> Result<&[u8], ProtocolDetectionError> {
    if bytes
        .first()
        .copied()
        .ok_or(ProtocolDetectionError::Truncated)?
        != 0x10
    {
        return Err(ProtocolDetectionError::NotConnect);
    }
    let (remaining_length, remaining_length_bytes) = decode_remaining_length(&bytes[1..])?;
    let body_start = 1 + remaining_length_bytes;
    let body_end = body_start
        .checked_add(remaining_length)
        .ok_or(ProtocolDetectionError::InvalidRemainingLength)?;
    bytes
        .get(body_start..body_end)
        .ok_or(ProtocolDetectionError::Truncated)
}

fn connect_username(
    body: &[u8],
    protocol: MqttProtocol,
) -> Result<Option<&str>, ProtocolDetectionError> {
    let connect_flags = *body.get(7).ok_or(ProtocolDetectionError::Truncated)?;
    let mut index = 10;
    if protocol == MqttProtocol::V5 {
        let (properties_length, properties_bytes) = decode_remaining_length(&body[index..])?;
        index = index
            .checked_add(properties_bytes)
            .and_then(|index| index.checked_add(properties_length))
            .ok_or(ProtocolDetectionError::Truncated)?;
    }
    let (_, next_index) = read_mqtt_string(body, index)?;
    index = next_index;
    if connect_flags & 0x04 != 0 {
        if protocol == MqttProtocol::V5 {
            let (properties_length, properties_bytes) = decode_remaining_length(&body[index..])?;
            index = index
                .checked_add(properties_bytes)
                .and_then(|index| index.checked_add(properties_length))
                .ok_or(ProtocolDetectionError::Truncated)?;
        }
        let (_, next_index) = read_mqtt_string(body, index)?;
        index = next_index;
        let (_, next_index) = read_mqtt_binary(body, index)?;
        index = next_index;
    }
    if connect_flags & 0x80 == 0 {
        return Ok(None);
    }
    let (username, _) = read_mqtt_string(body, index)?;
    Ok(Some(username))
}

fn read_mqtt_string(bytes: &[u8], index: usize) -> Result<(&str, usize), ProtocolDetectionError> {
    let length_bytes = bytes
        .get(index..index + 2)
        .ok_or(ProtocolDetectionError::Truncated)?;
    let length = usize::from(u16::from_be_bytes([length_bytes[0], length_bytes[1]]));
    let start = index + 2;
    let end = start
        .checked_add(length)
        .ok_or(ProtocolDetectionError::Truncated)?;
    let value = bytes
        .get(start..end)
        .ok_or(ProtocolDetectionError::Truncated)?;
    let value = std::str::from_utf8(value).map_err(|_| ProtocolDetectionError::Unsupported)?;
    Ok((value, end))
}

fn read_mqtt_binary(bytes: &[u8], index: usize) -> Result<(&[u8], usize), ProtocolDetectionError> {
    let length_bytes = bytes
        .get(index..index + 2)
        .ok_or(ProtocolDetectionError::Truncated)?;
    let length = usize::from(u16::from_be_bytes([length_bytes[0], length_bytes[1]]));
    let start = index + 2;
    let end = start
        .checked_add(length)
        .ok_or(ProtocolDetectionError::Truncated)?;
    let value = bytes
        .get(start..end)
        .ok_or(ProtocolDetectionError::Truncated)?;
    Ok((value, end))
}

fn decode_remaining_length(bytes: &[u8]) -> Result<(usize, usize), ProtocolDetectionError> {
    let mut value = 0_usize;
    let mut multiplier = 1_usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        value = value
            .checked_add(usize::from(byte & 0x7f) * multiplier)
            .ok_or(ProtocolDetectionError::InvalidRemainingLength)?;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        multiplier = multiplier
            .checked_mul(128)
            .ok_or(ProtocolDetectionError::InvalidRemainingLength)?;
        if index == 3 {
            return Err(ProtocolDetectionError::InvalidRemainingLength);
        }
    }
    Err(ProtocolDetectionError::Truncated)
}

#[derive(Clone)]
pub struct ListenerConfiguration {
    pub plaintext_address: SocketAddr,
    pub tls_address: SocketAddr,
    pub v311_backend_address: SocketAddr,
    pub v5_backend_address: SocketAddr,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub websocket_address: Option<SocketAddr>,
    pub websocket_tls: bool,
    pub bridge: Option<CoreBridgeConfig>,
    pub max_connections: usize,
    pub max_payload_size: usize,
    pub max_inflight_count: usize,
    pub token_authenticator: Option<HttpTokenAuthenticator>,
    pub auth_handler: Option<AuthHandler>,
    pub authorization_handler: Option<AuthorizationHandler>,
}

impl std::fmt::Debug for ListenerConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListenerConfiguration")
            .field("plaintext_address", &self.plaintext_address)
            .field("tls_address", &self.tls_address)
            .field("v311_backend_address", &self.v311_backend_address)
            .field("v5_backend_address", &self.v5_backend_address)
            .field("tls_cert_path", &self.tls_cert_path)
            .field("tls_key_path", &self.tls_key_path)
            .field("websocket_address", &self.websocket_address)
            .field("websocket_tls", &self.websocket_tls)
            .field("bridge", &self.bridge.as_ref().map(|bridge| &bridge.name))
            .field("max_connections", &self.max_connections)
            .field("max_payload_size", &self.max_payload_size)
            .field("max_inflight_count", &self.max_inflight_count)
            .field("token_authenticator", &self.token_authenticator.is_some())
            .field("auth_handler", &self.auth_handler.is_some())
            .field(
                "authorization_handler",
                &self.authorization_handler.is_some(),
            )
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct HttpTokenAuthenticator {
    client: reqwest::Client,
    session_resolution_url: String,
    secret: String,
}

#[derive(Serialize)]
struct SessionResolutionRequest<'a> {
    client_id: &'a str,
    username: &'a str,
    password: &'a str,
}

impl HttpTokenAuthenticator {
    pub fn new(api_base_url: &str, secret: &str) -> Result<Self, MqttdError> {
        if secret.len() < 32
            || !secret.is_ascii()
            || secret.bytes().any(|value| value.is_ascii_whitespace())
        {
            return Err(MqttdError::InvalidTransportSecret);
        }
        Ok(Self {
            client: reqwest::Client::new(),
            session_resolution_url: format!(
                "{}/internal/mqttd/session-resolution",
                api_base_url.trim_end_matches('/')
            ),
            secret: secret.to_owned(),
        })
    }

    pub async fn authenticate(
        &self,
        client_id: String,
        username: String,
        password: String,
    ) -> bool {
        if username.is_empty() || !password.is_empty() {
            return false;
        }
        self.client
            .post(&self.session_resolution_url)
            .header("x-iot-nano-mqttd-api-secret", &self.secret)
            .json(&SessionResolutionRequest {
                client_id: &client_id,
                username: &username,
                password: &password,
            })
            .send()
            .await
            .is_ok_and(|response| response.status() == StatusCode::OK)
    }
}

#[derive(Debug, Error)]
pub enum MqttdError {
    #[error("TLS certificate path does not exist: {0}")]
    MissingCertificate(PathBuf),
    #[error("TLS key path does not exist: {0}")]
    MissingKey(PathBuf),
    #[error("TLS configuration failed")]
    Tls(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("IOT_NANO_MQTTD_API_SECRET must use at least 32 ASCII non-whitespace characters")]
    InvalidTransportSecret,
    #[error("MQTT broker failed to start")]
    Broker(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("broker local link failed")]
    LocalLink(#[source] rumqttd::local::LinkError),
}

pub fn broker_config(configuration: &ListenerConfiguration) -> Result<Config, MqttdError> {
    if !configuration.tls_cert_path.is_file() {
        return Err(MqttdError::MissingCertificate(
            configuration.tls_cert_path.clone(),
        ));
    }
    if !configuration.tls_key_path.is_file() {
        return Err(MqttdError::MissingKey(configuration.tls_key_path.clone()));
    }

    let connections = ConnectionSettings {
        connection_timeout_ms: 60_000,
        max_payload_size: configuration.max_payload_size,
        max_inflight_count: configuration.max_inflight_count,
        auth: None,
        external_auth: configuration.auth_handler.clone(),
        authorization_handler: configuration.authorization_handler.clone(),
        dynamic_filters: true,
    };
    let mut v311 = ServerSettings {
        name: "iot-mqttd-v311".to_owned(),
        listen: configuration.v311_backend_address,
        tls: None,
        next_connection_delay_ms: 1,
        connections: connections.clone(),
    };
    let mut v5 = ServerSettings {
        name: "iot-mqttd-v5".to_owned(),
        listen: configuration.v5_backend_address,
        tls: None,
        next_connection_delay_ms: 1,
        connections: connections.clone(),
    };
    if configuration.auth_handler.is_none() {
        if let Some(authenticator) = &configuration.token_authenticator {
            let v311_authenticator = authenticator.clone();
            v311.connections
                .set_auth_handler(move |client_id, username, password| {
                    let authenticator = v311_authenticator.clone();
                    async move {
                        authenticator
                            .authenticate(client_id, username, password)
                            .await
                    }
                });
            let v5_authenticator = authenticator.clone();
            v5.connections
                .set_auth_handler(move |client_id, username, password| {
                    let authenticator = v5_authenticator.clone();
                    async move {
                        authenticator
                            .authenticate(client_id, username, password)
                            .await
                    }
                });
        }
    }

    let ws = configuration.websocket_address.map(|address| {
        let mut ws = ServerSettings {
            name: "iot-mqttd-ws-v311".to_owned(),
            listen: address,
            tls: configuration.websocket_tls.then(|| TlsConfig::Rustls {
                capath: None,
                certpath: configuration.tls_cert_path.to_string_lossy().into_owned(),
                keypath: configuration.tls_key_path.to_string_lossy().into_owned(),
            }),
            next_connection_delay_ms: 1,
            connections,
        };
        if configuration.auth_handler.is_none() {
            if let Some(authenticator) = &configuration.token_authenticator {
                let authenticator = authenticator.clone();
                ws.connections
                    .set_auth_handler(move |client_id, username, password| {
                        let authenticator = authenticator.clone();
                        async move {
                            authenticator
                                .authenticate(client_id, username, password)
                                .await
                        }
                    });
            }
        }
        let mut listeners = HashMap::new();
        listeners.insert("ws-v311".to_owned(), ws);
        listeners
    });

    let mut v311_listeners = HashMap::new();
    v311_listeners.insert("v311".to_owned(), v311);
    let mut v5_listeners = HashMap::new();
    v5_listeners.insert("v5".to_owned(), v5);

    Ok(Config {
        id: 0,
        router: RouterConfig {
            max_connections: configuration.max_connections,
            max_outgoing_packet_count: 200,
            max_segment_size: 64 * 1024 * 1024,
            max_segment_count: 10,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Default::default(),
        },
        v4: Some(v311_listeners),
        v5: Some(v5_listeners),
        ws,
        cluster: None,
        console: None,
        bridge: configuration.bridge.clone(),
        prometheus: None,
        metrics: None,
        storage: None,
        storage_policy: RetentionPolicy::default(),
    })
}

pub async fn start_broker(
    configuration: ListenerConfiguration,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    start_broker_with_timeout_and_storage(
        configuration,
        DEFAULT_BROKER_STARTUP_TIMEOUT,
        None,
        RetentionPolicy::default(),
    )
    .await
}

pub async fn start_broker_with_timeout(
    configuration: ListenerConfiguration,
    startup_timeout: Duration,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    start_broker_with_timeout_and_storage(
        configuration,
        startup_timeout,
        None,
        RetentionPolicy::default(),
    )
    .await
}

pub async fn start_broker_with_storage(
    configuration: ListenerConfiguration,
    storage: std::sync::Arc<dyn BrokerStorage>,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    start_broker_with_storage_and_policy(configuration, storage, RetentionPolicy::default()).await
}

pub async fn start_broker_with_prebound_listeners(
    configuration: ListenerConfiguration,
    listeners: PreboundBackendListeners,
    storage: std::sync::Arc<dyn BrokerStorage>,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    start_broker_with_timeout_storage_and_prebound_listeners(
        configuration,
        DEFAULT_BROKER_STARTUP_TIMEOUT,
        Some(storage),
        RetentionPolicy::default(),
        Some(listeners),
    )
    .await
}

pub async fn start_broker_with_storage_and_policy(
    configuration: ListenerConfiguration,
    storage: std::sync::Arc<dyn BrokerStorage>,
    policy: RetentionPolicy,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    start_broker_with_timeout_and_storage(
        configuration,
        DEFAULT_BROKER_STARTUP_TIMEOUT,
        Some(storage),
        policy,
    )
    .await
}

async fn start_broker_with_timeout_and_storage(
    configuration: ListenerConfiguration,
    startup_timeout: Duration,
    storage: Option<std::sync::Arc<dyn BrokerStorage>>,
    policy: RetentionPolicy,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    start_broker_with_timeout_storage_and_prebound_listeners(
        configuration,
        startup_timeout,
        storage,
        policy,
        None,
    )
    .await
}

async fn start_broker_with_timeout_storage_and_prebound_listeners(
    configuration: ListenerConfiguration,
    startup_timeout: Duration,
    storage: Option<std::sync::Arc<dyn BrokerStorage>>,
    policy: RetentionPolicy,
    listeners: Option<PreboundBackendListeners>,
) -> Result<BrokerLifecycleHandle, MqttdError> {
    let mut config = broker_config(&configuration)?;
    config.storage_policy = policy;
    if let Some(storage) = &storage {
        let now = now_ms();
        storage
            .load(now)
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        storage
            .prune(now, config.storage_policy)
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        config.storage = Some(storage.clone());
    }
    if listeners.is_none() {
        for address in [
            configuration.v311_backend_address,
            configuration.v5_backend_address,
        ] {
            std::net::TcpListener::bind(address).map_err(|error| {
                MqttdError::Broker(Box::new(std::io::Error::new(
                    error.kind(),
                    format!("MQTT backend {address} is unavailable: {error}"),
                )))
            })?;
        }
    }
    let (public_accept_stop, _public_accept_shutdown) = watch::channel(false);
    let broker = Broker::new(config).map_err(|error| MqttdError::Broker(Box::new(error)))?;
    let inner = match listeners {
        Some(listeners) => {
            let mut v4_listeners = HashMap::new();
            v4_listeners.insert("v311".to_owned(), listeners.v311);
            let mut v5_listeners = HashMap::new();
            v5_listeners.insert("v5".to_owned(), listeners.v5);
            broker.spawn_with_prebound_listeners(v4_listeners, v5_listeners)
        }
        None => broker.spawn(),
    };
    let handle = BrokerLifecycleHandle {
        inner: Some(inner),
        public_accept_stop,
        public_workers: Mutex::new(Vec::new()),
    };
    if let Err(error) = wait_for_backends(
        [
            configuration.v311_backend_address,
            configuration.v5_backend_address,
        ],
        startup_timeout,
    )
    .await
    {
        handle.shutdown();
        let _ = handle.join();
        return Err(error);
    }
    Ok(handle)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

async fn wait_for_backends(
    addresses: [SocketAddr; 2],
    timeout: Duration,
) -> Result<(), MqttdError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut ready = true;
        for address in addresses {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let reachable = if remaining.is_zero() {
                false
            } else {
                match tokio::time::timeout(remaining, TcpStream::connect(address)).await {
                    Ok(result) => result.is_ok(),
                    Err(_) => false,
                }
            };
            if !reachable {
                ready = false;
                break;
            }
        }
        if ready {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(MqttdError::Broker(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "MQTT broker backends did not become ready",
            ))));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
