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
    public_accept_gate: PublicMuxAcceptanceGate,
    public_accept_stop: watch::Sender<bool>,
    public_workers: Mutex<Vec<PublicWorker>>,
}

struct PublicWorker {
    name: String,
    handle: thread::JoinHandle<Result<(), MqttdError>>,
}

pub(crate) fn join_public_workers(workers: Vec<PublicWorker>) -> Result<(), MqttdError> {
    for PublicWorker { name, handle } in workers {
        match handle.join() {
            Ok(result) => result?,
            Err(_) => return Err(MqttdError::PublicWorkerPanic { worker: name }),
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct PublicMuxAcceptanceGate {
    accepting: Arc<Mutex<bool>>,
    #[cfg(test)]
    admission_barrier: Arc<Mutex<Option<MuxAdmissionBarrier>>>,
    #[cfg(test)]
    post_admission_barrier: Arc<Mutex<Option<MuxAdmissionBarrier>>>,
}

impl Default for PublicMuxAcceptanceGate {
    fn default() -> Self {
        Self {
            accepting: Arc::new(Mutex::new(true)),
            #[cfg(test)]
            admission_barrier: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            post_admission_barrier: Arc::new(Mutex::new(None)),
        }
    }
}

impl PublicMuxAcceptanceGate {
    /// Closes admission for new public mux proxies.
    ///
    /// The mutex-protected state transition is the linearization point shared
    /// with `try_admit_with_shutdowns`: a proxy that observes `true` won
    /// before this close.
    pub fn close(&self) {
        *self
            .accepting
            .lock()
            .expect("public accept gate is not poisoned") = false;
    }

    fn try_admit_with_shutdowns(
        &self,
        accept_shutdown: &mut watch::Receiver<bool>,
        force_shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        // This acquisition order defines free-API admission: gate, force
        // watch, then accept watch. A completed send before either borrow is
        // observed as stopped; a send blocked by these borrows follows this
        // admission, even if the proxy task has not started yet.
        let accepting = self
            .accepting
            .lock()
            .expect("public accept gate is not poisoned");
        if !*accepting {
            return false;
        }
        let force_stopped = force_shutdown.borrow_and_update();
        let accept_stopped = accept_shutdown.borrow_and_update();
        !*force_stopped && !*accept_stopped
    }

    #[cfg(test)]
    fn test_admission_barrier(&self) -> MuxAdmissionBarrier {
        let barrier = MuxAdmissionBarrier::default();
        *self
            .admission_barrier
            .lock()
            .expect("public mux admission test barrier is not poisoned") = Some(barrier.clone());
        barrier
    }

    #[cfg(test)]
    async fn wait_before_admission(&self) {
        let barrier = self
            .admission_barrier
            .lock()
            .expect("public mux admission test barrier is not poisoned")
            .clone();
        if let Some(barrier) = barrier {
            barrier.reached.notify_one();
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                barrier.proceed.notified(),
            )
            .await
            .expect("public mux admission test barrier was not released within one second");
        }
    }

    #[cfg(test)]
    fn test_post_admission_barrier(&self) -> MuxAdmissionBarrier {
        let barrier = MuxAdmissionBarrier::default();
        *self
            .post_admission_barrier
            .lock()
            .expect("public mux post-admission test barrier is not poisoned") =
            Some(barrier.clone());
        barrier
    }

    #[cfg(test)]
    async fn wait_after_admission(&self) {
        let barrier = self
            .post_admission_barrier
            .lock()
            .expect("public mux post-admission test barrier is not poisoned")
            .clone();
        if let Some(barrier) = barrier {
            barrier.reached.notify_one();
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                barrier.proceed.notified(),
            )
            .await
            .expect("public mux post-admission test barrier was not released within one second");
        }
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
struct MuxAdmissionBarrier {
    reached: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl MuxAdmissionBarrier {
    async fn wait_until_reached(&self, description: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(1), self.reached.notified())
            .await
            .unwrap_or_else(|_| panic!("{description} was not reached within one second"));
    }

    fn release(&self) {
        self.proceed.notify_one();
    }
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
        self.public_accept_gate.close();
        self.public_accept_stop.send_replace(true);
    }

    #[cfg(test)]
    pub(crate) fn test_handle() -> Self {
        let (public_accept_stop, _) = watch::channel(false);
        Self {
            inner: None,
            public_accept_gate: PublicMuxAcceptanceGate::default(),
            public_accept_stop,
            public_workers: Mutex::new(Vec::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_public_accepting(&self) -> bool {
        *self
            .public_accept_gate
            .accepting
            .lock()
            .expect("public accept gate is not poisoned")
    }

    pub(crate) fn take_public_workers(&self) -> Vec<PublicWorker> {
        std::mem::take(
            &mut *self
                .public_workers
                .lock()
                .expect("public worker mutex is not poisoned"),
        )
    }

    pub fn join(mut self) -> Result<(), MqttdError> {
        self.shutdown();
        let public_result = join_public_workers(
            self.public_workers
                .get_mut()
                .expect("public worker mutex is not poisoned")
                .drain(..)
                .collect(),
        );
        let broker_result = self
            .inner
            .take()
            .expect("broker lifecycle handle is available")
            .join()
            .map_err(|error| MqttdError::Broker(Box::new(error)));
        public_result.and(broker_result)
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
        let name = format!("iot-mqttd-rule-{}", rule.name);
        let worker_name = name.clone();
        let worker = thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|source| MqttdError::PublicWorkerRuntime {
                        worker: worker_name.clone(),
                        source,
                    })?;
                runtime.block_on(async move {
                    loop {
                        tokio::select! {
                            _ = shutdown.changed() => break,
                            notification = link_rx.next() => {
                                let Ok(Some(rumqttd::Notification::Forward(forward))) = notification else {
                                    continue;
                                };
                                link_tx
                                    .publish(rule.target_topic.clone(), forward.publish.payload)
                                    .map_err(MqttdError::LocalLink)?;
                            }
                        }
                    }
                    Ok(())
                })
            })
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        self.public_workers
            .lock()
            .expect("public worker mutex is not poisoned")
            .push(PublicWorker {
                name,
                handle: worker,
            });
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
            move |listener, accept_stop, force_stop, accept_gate| async move {
                serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
                    listener,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                    accept_gate,
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
            move |listener, accept_stop, force_stop, accept_gate| async move {
                serve_plaintext_mux_with_shutdowns_and_mode(
                    listener,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                    accept_gate,
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
            move |listener, accept_stop, force_stop, accept_gate| async move {
                serve_tls_mux_with_shutdowns_and_acceptance_gate(
                    listener,
                    acceptor,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                    accept_gate,
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
            move |listener, accept_stop, force_stop, accept_gate| async move {
                serve_tls_mux_with_shutdowns_and_mode(
                    listener,
                    acceptor,
                    backends,
                    settings,
                    accept_stop,
                    force_stop,
                    accept_gate,
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
        F: FnOnce(
                TcpListener,
                watch::Receiver<bool>,
                watch::Receiver<bool>,
                PublicMuxAcceptanceGate,
            ) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = std::io::Result<()>> + Send + 'static,
    {
        listener
            .set_nonblocking(true)
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        let accept_stop = self.public_accept_stop.subscribe();
        let force_stop = self.shutdown_receiver();
        let accept_gate = self.public_accept_gate.clone();
        let worker_name = name.to_owned();
        let thread_name = worker_name.clone();
        let worker = thread::Builder::new()
            .name(worker_name.clone())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|source| MqttdError::PublicWorkerRuntime {
                        worker: thread_name.clone(),
                        source,
                    })?;
                runtime
                    .block_on(async {
                        let listener = TcpListener::from_std(listener)?;
                        serve(listener, accept_stop, force_stop, accept_gate).await
                    })
                    .map_err(|source| MqttdError::PublicWorkerIo {
                        worker: thread_name,
                        source,
                    })
            })
            .map_err(|error| MqttdError::Broker(Box::new(error)))?;
        self.public_workers
            .lock()
            .expect("public worker mutex is not poisoned")
            .push(PublicWorker {
                name: worker_name,
                handle: worker,
            });
        Ok(())
    }
}

impl Drop for BrokerLifecycleHandle {
    fn drop(&mut self) {
        self.shutdown();
        let _ = join_public_workers(
            self.public_workers
                .get_mut()
                .expect("public worker mutex is not poisoned")
                .drain(..)
                .collect(),
        );
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
    serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
        listener,
        backends,
        settings,
        accept_shutdown,
        force_shutdown,
        PublicMuxAcceptanceGate::default(),
    )
    .await
}

pub async fn serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
    listener: TcpListener,
    backends: ProtocolBackends,
    settings: MuxSettings,
    accept_shutdown: watch::Receiver<bool>,
    force_shutdown: watch::Receiver<bool>,
    accept_gate: PublicMuxAcceptanceGate,
) -> std::io::Result<()> {
    serve_plaintext_mux_with_shutdowns_and_mode(
        listener,
        backends,
        settings,
        accept_shutdown,
        force_shutdown,
        accept_gate,
        MuxRouteMode::GenericBroker,
        None,
    )
    .await
}

#[cfg(test)]
async fn start_broker_for_public_worker_test() -> BrokerLifecycleHandle {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let plaintext_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let tls_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let v311_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let v5_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let plaintext_address = plaintext_listener.local_addr().unwrap();
    let tls_address = tls_listener.local_addr().unwrap();
    let v311_backend_address = v311_listener.local_addr().unwrap();
    let v5_backend_address = v5_listener.local_addr().unwrap();
    drop((plaintext_listener, tls_listener, v311_listener, v5_listener));
    start_broker(ListenerConfiguration {
        plaintext_address,
        tls_address,
        v311_backend_address,
        v5_backend_address,
        tls_cert_path: fixtures.join("server.crt"),
        tls_key_path: fixtures.join("server.key"),
        websocket_address: None,
        websocket_tls: false,
        bridge: None,
        max_connections: 32,
        max_payload_size: 1024 * 1024,
        max_inflight_count: 16,
        token_authenticator: None,
        auth_handler: None,
        authorization_handler: None,
    })
    .await
    .unwrap()
}

#[cfg(test)]
mod mux_shutdown_tests {
    use super::*;
    use std::{fs::File, io::BufReader, path::PathBuf};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tokio_rustls::{
        TlsConnector,
        rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
    };

    fn connect_packet() -> Vec<u8> {
        vec![
            0x10, 0x0e, 0x00, 0x04, b'M', b'Q', b'T', b'T', 4, 0x02, 0x00, 0x3c, 0x00, 0x02, b'i',
            b'd',
        ]
    }

    fn spawn_backend_notifier(
        listener: TcpListener,
    ) -> (mpsc::UnboundedReceiver<()>, tokio::task::JoinHandle<()>) {
        let (accepted, connections) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Ok(Ok((_stream, _))) =
                timeout(Duration::from_secs(5), listener.accept()).await
            {
                if accepted.send(()).is_err() {
                    break;
                }
            }
        });
        (connections, task)
    }

    fn spawn_backend_that_holds_connection(
        listener: TcpListener,
    ) -> (
        mpsc::UnboundedReceiver<()>,
        Arc<tokio::sync::Notify>,
        tokio::task::JoinHandle<()>,
    ) {
        let (accepted, connections) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let task_release = release.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("held test backend did not accept a connection within five seconds")
                .expect("held test backend listener failed");
            accepted
                .send(())
                .expect("held test backend acceptance receiver was dropped");
            timeout(Duration::from_secs(5), task_release.notified())
                .await
                .expect("held test backend was not released within five seconds");
            drop(stream);
        });
        (connections, release, task)
    }

    #[derive(Clone, Copy)]
    enum ShutdownSignal {
        Accept,
        Force,
    }

    fn signal_shutdown(
        signal: ShutdownSignal,
        accept_stop: &watch::Sender<bool>,
        force_stop: &watch::Sender<bool>,
    ) {
        match signal {
            ShutdownSignal::Accept => accept_stop.send_replace(true),
            ShutdownSignal::Force => force_stop.send_replace(true),
        };
    }

    fn tls_connector() -> TlsConnector {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let mut certificate = BufReader::new(File::open(fixtures.join("server.crt")).unwrap());
        let mut roots = RootCertStore::empty();
        roots
            .add(
                rustls_pemfile::certs(&mut certificate)
                    .next()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        TlsConnector::from(Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ))
    }

    async fn assert_plaintext_unowned_gate_rejects_shutdown_before_admission(
        signal: ShutdownSignal,
    ) {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let (mut backend_connections, backend_task) = spawn_backend_notifier(backend_listener);
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let (accept_stop, accept_shutdown) = watch::channel(false);
        let (force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let barrier = gate.test_admission_barrier();
        let mux_task = tokio::spawn(serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate,
        ));

        let mut client = TcpStream::connect(public_address).await.unwrap();
        client.write_all(&connect_packet()).await.unwrap();
        barrier
            .wait_until_reached("plaintext mux admission barrier")
            .await;
        signal_shutdown(signal, &accept_stop, &force_stop);
        barrier.release();

        drop(client);
        assert!(
            timeout(Duration::from_secs(1), mux_task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert!(matches!(
            backend_connections.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        backend_task.abort();
    }

    async fn assert_tls_unowned_gate_rejects_shutdown_before_admission(signal: ShutdownSignal) {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let (mut backend_connections, backend_task) = spawn_backend_notifier(backend_listener);
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let acceptor =
            load_tls_acceptor(&fixtures.join("server.crt"), &fixtures.join("server.key")).unwrap();
        let (accept_stop, accept_shutdown) = watch::channel(false);
        let (force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let barrier = gate.test_admission_barrier();
        let mux_task = tokio::spawn(serve_tls_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            acceptor,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate,
        ));

        let client = TcpStream::connect(public_address).await.unwrap();
        barrier
            .wait_until_reached("TLS mux admission barrier")
            .await;
        signal_shutdown(signal, &accept_stop, &force_stop);
        barrier.release();
        if let Ok(Ok(mut client)) = timeout(
            Duration::from_secs(1),
            tls_connector().connect(ServerName::try_from("localhost").unwrap(), client),
        )
        .await
        {
            client.write_all(&connect_packet()).await.unwrap();
        }

        assert!(
            timeout(Duration::from_secs(1), mux_task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert!(matches!(
            backend_connections.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        backend_task.abort();
    }

    async fn assert_plaintext_admitted_before_accept_stop_still_proxies() {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let (mut backend_connections, backend_task) = spawn_backend_notifier(backend_listener);
        let (accept_stop, accept_shutdown) = watch::channel(false);
        let (_force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let lifecycle = BrokerLifecycleHandle {
            inner: None,
            public_accept_gate: gate.clone(),
            public_accept_stop: accept_stop,
            public_workers: Mutex::new(Vec::new()),
        };
        let barrier = gate.test_post_admission_barrier();
        let mux_task = tokio::spawn(serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate,
        ));

        let mut client = TcpStream::connect(public_address).await.unwrap();
        client.write_all(&connect_packet()).await.unwrap();
        barrier
            .wait_until_reached("plaintext mux post-admission barrier")
            .await;
        lifecycle.stop_public_accepting();
        barrier.release();
        timeout(Duration::from_secs(1), backend_connections.recv())
            .await
            .expect("plaintext mux did not proxy the admitted connection")
            .expect("plaintext backend notifier closed unexpectedly");
        drop(client);

        timeout(Duration::from_secs(1), mux_task)
            .await
            .expect("plaintext mux did not stop after graceful accept-stop")
            .unwrap()
            .unwrap();
        assert!(matches!(
            backend_connections.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        backend_task.abort();
    }

    async fn assert_tls_admitted_before_accept_stop_still_proxies() {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let (mut backend_connections, backend_task) = spawn_backend_notifier(backend_listener);
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let acceptor =
            load_tls_acceptor(&fixtures.join("server.crt"), &fixtures.join("server.key")).unwrap();
        let (accept_stop, accept_shutdown) = watch::channel(false);
        let (_force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let lifecycle = BrokerLifecycleHandle {
            inner: None,
            public_accept_gate: gate.clone(),
            public_accept_stop: accept_stop,
            public_workers: Mutex::new(Vec::new()),
        };
        let barrier = gate.test_post_admission_barrier();
        let mux_task = tokio::spawn(serve_tls_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            acceptor,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate,
        ));

        let client = TcpStream::connect(public_address).await.unwrap();
        barrier
            .wait_until_reached("TLS mux post-admission barrier")
            .await;
        lifecycle.stop_public_accepting();
        barrier.release();
        let mut client = timeout(
            Duration::from_secs(1),
            tls_connector().connect(ServerName::try_from("localhost").unwrap(), client),
        )
        .await
        .expect("TLS client did not complete the admitted handshake")
        .unwrap();
        client.write_all(&connect_packet()).await.unwrap();
        timeout(Duration::from_secs(1), backend_connections.recv())
            .await
            .expect("TLS mux did not proxy the admitted connection")
            .expect("TLS backend notifier closed unexpectedly");
        drop(client);

        timeout(Duration::from_secs(1), mux_task)
            .await
            .expect("TLS mux did not stop after graceful accept-stop")
            .unwrap()
            .unwrap();
        assert!(matches!(
            backend_connections.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        backend_task.abort();
    }

    #[tokio::test]
    async fn lifecycle_accept_stop_after_plaintext_admission_still_proxies() {
        assert_plaintext_admitted_before_accept_stop_still_proxies().await;
    }

    #[tokio::test]
    async fn lifecycle_accept_stop_after_tls_admission_still_proxies() {
        assert_tls_admitted_before_accept_stop_still_proxies().await;
    }

    #[tokio::test]
    async fn force_stop_after_plaintext_proxy_establishes_closes_the_client() {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let (mut backend_connections, backend_release, backend_task) =
            spawn_backend_that_holds_connection(backend_listener);
        let (_accept_stop, accept_shutdown) = watch::channel(false);
        let (force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let barrier = gate.test_post_admission_barrier();
        let mux_task = tokio::spawn(serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate,
        ));

        let mut client = TcpStream::connect(public_address).await.unwrap();
        client.write_all(&connect_packet()).await.unwrap();
        barrier
            .wait_until_reached("plaintext mux post-admission barrier")
            .await;
        barrier.release();
        timeout(Duration::from_secs(1), backend_connections.recv())
            .await
            .expect("plaintext mux did not establish a backend proxy before force-stop")
            .expect("plaintext backend notifier closed unexpectedly");
        force_stop.send_replace(true);

        let mut response = Vec::new();
        let _ = timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("force-stop did not close the established plaintext proxy");
        timeout(Duration::from_secs(1), mux_task)
            .await
            .expect("plaintext mux did not stop after force-stop")
            .unwrap()
            .unwrap();
        assert!(
            !backend_task.is_finished(),
            "backend completed while its stream was held, so it could have closed the proxy independently"
        );
        backend_release.notify_one();
        timeout(Duration::from_secs(1), backend_task)
            .await
            .expect("plaintext held backend did not clean up after release")
            .unwrap();
    }

    #[tokio::test]
    async fn force_stop_after_tls_proxy_establishes_closes_the_client() {
        tokio_rustls::rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let (mut backend_connections, backend_release, backend_task) =
            spawn_backend_that_holds_connection(backend_listener);
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let acceptor =
            load_tls_acceptor(&fixtures.join("server.crt"), &fixtures.join("server.key")).unwrap();
        let (_accept_stop, accept_shutdown) = watch::channel(false);
        let (force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let barrier = gate.test_post_admission_barrier();
        let mux_task = tokio::spawn(serve_tls_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            acceptor,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate,
        ));

        let client = TcpStream::connect(public_address).await.unwrap();
        barrier
            .wait_until_reached("TLS mux post-admission barrier")
            .await;
        barrier.release();
        let mut client = timeout(
            Duration::from_secs(1),
            tls_connector().connect(ServerName::try_from("localhost").unwrap(), client),
        )
        .await
        .expect("TLS client did not complete the admitted handshake")
        .unwrap();
        client.write_all(&connect_packet()).await.unwrap();
        timeout(Duration::from_secs(1), backend_connections.recv())
            .await
            .expect("TLS mux did not establish a backend proxy before force-stop")
            .expect("TLS backend notifier closed unexpectedly");
        force_stop.send_replace(true);

        let mut response = Vec::new();
        let _ = timeout(Duration::from_secs(1), client.read_to_end(&mut response))
            .await
            .expect("force-stop did not close the established TLS proxy");
        timeout(Duration::from_secs(1), mux_task)
            .await
            .expect("TLS mux did not stop after force-stop")
            .unwrap()
            .unwrap();
        assert!(
            !backend_task.is_finished(),
            "backend completed while its stream was held, so it could have closed the TLS proxy independently"
        );
        backend_release.notify_one();
        timeout(Duration::from_secs(1), backend_task)
            .await
            .expect("TLS held backend did not clean up after release")
            .unwrap();
    }

    #[tokio::test]
    async fn unowned_gate_rejects_plaintext_proxy_when_accept_shutdown_precedes_admission() {
        assert_plaintext_unowned_gate_rejects_shutdown_before_admission(ShutdownSignal::Accept)
            .await;
    }

    #[tokio::test]
    async fn unowned_gate_rejects_plaintext_proxy_when_force_shutdown_precedes_admission() {
        assert_plaintext_unowned_gate_rejects_shutdown_before_admission(ShutdownSignal::Force)
            .await;
    }

    #[tokio::test]
    async fn unowned_gate_rejects_tls_proxy_when_accept_shutdown_precedes_admission() {
        assert_tls_unowned_gate_rejects_shutdown_before_admission(ShutdownSignal::Accept).await;
    }

    #[tokio::test]
    async fn unowned_gate_rejects_tls_proxy_when_force_shutdown_precedes_admission() {
        assert_tls_unowned_gate_rejects_shutdown_before_admission(ShutdownSignal::Force).await;
    }

    #[tokio::test]
    async fn lifecycle_accept_gate_rejects_a_connection_stopped_after_accept_before_spawn() {
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend_listener.local_addr().unwrap();
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let (mut backend_connections, backend_task) = spawn_backend_notifier(backend_listener);

        let (accept_stop, accept_shutdown) = watch::channel(false);
        let (_force_stop, force_shutdown) = watch::channel(false);
        let gate = PublicMuxAcceptanceGate::default();
        let lifecycle = BrokerLifecycleHandle {
            inner: None,
            public_accept_gate: gate.clone(),
            public_accept_stop: accept_stop,
            public_workers: Mutex::new(Vec::new()),
        };
        let barrier = gate.test_admission_barrier();
        let mux_task = tokio::spawn(serve_plaintext_mux_with_shutdowns_and_acceptance_gate(
            public_listener,
            ProtocolBackends {
                v311: backend_address,
                v5: backend_address,
                device_v311: None,
                device_v5: None,
            },
            MuxSettings::default(),
            accept_shutdown,
            force_shutdown,
            gate.clone(),
        ));

        let mut client = TcpStream::connect(public_address).await.unwrap();
        client.write_all(&connect_packet()).await.unwrap();
        barrier
            .wait_until_reached("plaintext mux admission barrier")
            .await;
        lifecycle.stop_public_accepting();
        barrier.release();

        assert!(
            timeout(Duration::from_secs(1), mux_task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert!(matches!(
            backend_connections.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        backend_task.abort();
    }

    #[tokio::test]
    async fn lifecycle_join_returns_a_fatal_public_listener_io_error() {
        let broker = start_broker_for_public_worker_test().await;
        broker
            .spawn_public_worker(
                "injected-listener-error",
                StdTcpListener::bind("127.0.0.1:0").unwrap(),
                |_listener, _accept_stop, _force_stop, _accept_gate| async {
                    Err(std::io::Error::other("injected listener failure"))
                },
            )
            .unwrap();

        let error = broker
            .join()
            .expect_err("fatal public listener I/O error must reach lifecycle join");

        match error {
            MqttdError::PublicWorkerIo { worker, source } => {
                assert_eq!(worker, "injected-listener-error");
                assert_eq!(source.kind(), std::io::ErrorKind::Other);
                assert_eq!(source.to_string(), "injected listener failure");
            }
            error => panic!("expected public listener I/O error, got {error:?}"),
        }
    }

    #[tokio::test]
    async fn lifecycle_join_returns_a_fatal_public_listener_panic() {
        let broker = start_broker_for_public_worker_test().await;
        broker
            .spawn_public_worker(
                "injected-listener-panic",
                StdTcpListener::bind("127.0.0.1:0").unwrap(),
                |_listener, _accept_stop, _force_stop, _accept_gate| async {
                    panic!("injected listener panic");
                    #[allow(unreachable_code)]
                    Ok(())
                },
            )
            .unwrap();

        let error = broker
            .join()
            .expect_err("fatal public listener panic must reach lifecycle join");

        assert!(matches!(
            error,
            MqttdError::PublicWorkerPanic { worker } if worker == "injected-listener-panic"
        ));
    }
}

fn public_mux_shutdown_requested(
    accept_shutdown: &watch::Receiver<bool>,
    force_shutdown: &watch::Receiver<bool>,
) -> bool {
    *force_shutdown.borrow() || *accept_shutdown.borrow()
}

async fn serve_plaintext_mux_with_shutdowns_and_mode(
    listener: TcpListener,
    backends: ProtocolBackends,
    settings: MuxSettings,
    mut accept_shutdown: watch::Receiver<bool>,
    mut force_shutdown: watch::Receiver<bool>,
    accept_gate: PublicMuxAcceptanceGate,
    route_mode: MuxRouteMode,
    connection_counter: Option<Arc<AtomicUsize>>,
) -> std::io::Result<()> {
    if public_mux_shutdown_requested(&accept_shutdown, &force_shutdown) {
        accept_gate.close();
        return Ok(());
    }

    let mut connections = JoinSet::new();
    loop {
        if public_mux_shutdown_requested(&accept_shutdown, &force_shutdown) {
            accept_gate.close();
            break;
        }

        tokio::select! {
            biased;
            _ = force_shutdown.changed() => {
                accept_gate.close();
                break;
            }
            _ = accept_shutdown.changed() => {
                accept_gate.close();
                break;
            }
            result = listener.accept() => {
                let (stream, _) = result?;
                #[cfg(test)]
                accept_gate.wait_before_admission().await;
                if !accept_gate.try_admit_with_shutdowns(&mut accept_shutdown, &mut force_shutdown) {
                    continue;
                }
                let mut connection_shutdown = force_shutdown.clone();
                #[cfg(test)]
                accept_gate.wait_after_admission().await;
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
    serve_tls_mux_with_shutdowns_and_acceptance_gate(
        listener,
        acceptor,
        backends,
        settings,
        accept_shutdown,
        force_shutdown,
        PublicMuxAcceptanceGate::default(),
    )
    .await
}

pub async fn serve_tls_mux_with_shutdowns_and_acceptance_gate(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    backends: ProtocolBackends,
    settings: MuxSettings,
    accept_shutdown: watch::Receiver<bool>,
    force_shutdown: watch::Receiver<bool>,
    accept_gate: PublicMuxAcceptanceGate,
) -> std::io::Result<()> {
    serve_tls_mux_with_shutdowns_and_mode(
        listener,
        acceptor,
        backends,
        settings,
        accept_shutdown,
        force_shutdown,
        accept_gate,
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
    accept_gate: PublicMuxAcceptanceGate,
    route_mode: MuxRouteMode,
    connection_counter: Option<Arc<AtomicUsize>>,
) -> std::io::Result<()> {
    if public_mux_shutdown_requested(&accept_shutdown, &force_shutdown) {
        accept_gate.close();
        return Ok(());
    }

    let mut connections = JoinSet::new();
    loop {
        if public_mux_shutdown_requested(&accept_shutdown, &force_shutdown) {
            accept_gate.close();
            break;
        }

        tokio::select! {
            biased;
            _ = force_shutdown.changed() => {
                accept_gate.close();
                break;
            }
            _ = accept_shutdown.changed() => {
                accept_gate.close();
                break;
            }
            result = listener.accept() => {
                let (stream, _) = result?;
                #[cfg(test)]
                accept_gate.wait_before_admission().await;
                if !accept_gate.try_admit_with_shutdowns(&mut accept_shutdown, &mut force_shutdown) {
                    continue;
                }
                let acceptor = acceptor.clone();
                let mut connection_shutdown = force_shutdown.clone();
                #[cfg(test)]
                accept_gate.wait_after_admission().await;
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
    #[error("MQTT public listener worker {worker} failed: {source}")]
    PublicWorkerIo {
        worker: String,
        #[source]
        source: std::io::Error,
    },
    #[error("MQTT public listener worker {worker} runtime failed: {source}")]
    PublicWorkerRuntime {
        worker: String,
        #[source]
        source: std::io::Error,
    },
    #[error("MQTT public listener worker {worker} panicked")]
    PublicWorkerPanic { worker: String },
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
    let (public_accept_stop, _public_accept_shutdown) = watch::channel(false);
    let inner = match listeners {
        Some(listeners) => Broker::new_with_prebound_listeners(
            config,
            vec![("v311".to_owned(), listeners.v311)],
            vec![("v5".to_owned(), listeners.v5)],
            startup_timeout,
        )
        .map_err(|error| MqttdError::Broker(Box::new(error)))?,
        None => Broker::new(config)
            .map_err(|error| MqttdError::Broker(Box::new(error)))?
            .spawn(),
    };
    let handle = BrokerLifecycleHandle {
        inner: Some(inner),
        public_accept_gate: PublicMuxAcceptanceGate::default(),
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
