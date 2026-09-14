use crate::link::alerts::{self};
use crate::link::console::ConsoleLink;
use crate::link::network::{self, Network, N};
use crate::link::remote::{
    self, authenticated_principal as configured_principal, authorize_connect,
    mqtt_connect_with_identity, RemoteLink,
};
use crate::link::{bridge, timer};
use crate::local::LinkBuilder;
use crate::protocol::v4::V4;
use crate::protocol::v5::V5;
use crate::protocol::{Packet, Protocol};
#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use crate::server::tls::{self, TLSAcceptor};
use crate::{meters, ConnectionSettings, Meter};
use flume::{RecvError, SendError, Sender};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use tracing::{error, field, info, warn, Instrument};

#[cfg(feature = "websocket")]
use async_tungstenite::tokio::accept_hdr_async;
#[cfg(feature = "websocket")]
use async_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
#[cfg(feature = "websocket")]
use async_tungstenite::tungstenite::http::HeaderValue;
#[cfg(feature = "websocket")]
use ws_stream_tungstenite::WsStream;

use metrics::gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::time::{Duration, Instant};
use std::{io, thread};

use crate::link::console;
use crate::link::local::{self, LinkRx, LinkTx};
use crate::router::{Event, Router};
use crate::{Config, ConnectionId, ServerSettings};

use tokio::net::{TcpListener, TcpStream};
use tokio::time::error::Elapsed;
use tokio::{
    sync::watch,
    task::{self, JoinError, JoinSet},
    time,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
    #[error("Certs error = {0}")]
    Certs(#[from] tls::Error),
    #[error("Accept error = {0}")]
    Accept(String),
    #[error("Remote error = {0}")]
    Remote(#[from] remote::Error),
    #[error("Invalid configuration")]
    Config(String),
    #[error("Storage {0}")]
    Storage(#[from] crate::StorageError),
    #[error(
        "prebound {protocol} listener names are invalid: missing {missing:?}, extra {extra:?}, duplicates {duplicates:?}"
    )]
    PreboundListenerNames {
        protocol: &'static str,
        missing: Vec<String>,
        extra: Vec<String>,
        duplicates: Vec<String>,
    },
    #[error(
        "prebound listener {name:?} was supplied for {provided} but is configured for {expected}"
    )]
    PreboundListenerProtocol {
        name: String,
        expected: &'static str,
        provided: &'static str,
    },
    #[error(
        "prebound {protocol} listener source {actual:?} does not match configured server {expected:?}"
    )]
    PreboundListenerName {
        protocol: &'static str,
        expected: String,
        actual: String,
    },
    #[error(
        "prebound listener {listener:?} has address {actual}, but configuration requires {expected}"
    )]
    PreboundListenerAddress {
        listener: PreboundListenerSource,
        expected: SocketAddr,
        actual: SocketAddr,
    },
    #[error("prebound listener {listener:?} {operation} failed: {source}")]
    PreboundListenerIo {
        listener: PreboundListenerSource,
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("prebound listener {listener:?} startup failed: {error}")]
    PreboundListenerStartup {
        listener: PreboundListenerSource,
        error: String,
    },
    #[error("prebound listeners did not become ready within {timeout:?}: pending {pending:?}")]
    PreboundListenerStartupTimeout {
        timeout: Duration,
        pending: Vec<PreboundListenerSource>,
    },
    #[error("managed server child task failed: {0}")]
    ManagedTask(String),
}

pub type NamedListener = (String, StdTcpListener);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreboundListenerSource {
    pub protocol: &'static str,
    pub name: String,
}

struct StartupStatus {
    listener: PreboundListenerSource,
    result: Result<(), String>,
}

type StartupSender = mpsc::Sender<StartupStatus>;

#[cfg(test)]
type PreboundServerSpawnHook = Arc<dyn Fn(&str) -> io::Result<()> + Send + Sync>;

#[cfg(test)]
type PreboundStartupStatusHook = Arc<dyn Fn(&PreboundListenerSource) -> bool + Send + Sync>;

#[cfg(test)]
type PostAcceptAdmissionHook = Arc<dyn Fn(&str) + Send + Sync>;

#[cfg(test)]
type ManagedRemoteSpawnHook = Arc<dyn Fn(&str) + Send + Sync>;

#[cfg(test)]
#[derive(Default)]
struct PreboundTestHooks {
    server_spawn: Option<PreboundServerSpawnHook>,
    suppress_startup_status: Option<PreboundStartupStatusHook>,
    post_accept_admission: Option<PostAcceptAdmissionHook>,
    managed_remote_spawn: Option<ManagedRemoteSpawnHook>,
    withheld_startup_senders: Vec<StartupSender>,
}

#[cfg(test)]
static PREBOUND_TEST_HOOKS: OnceLock<Mutex<PreboundTestHooks>> = OnceLock::new();

#[cfg(test)]
static PREBOUND_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct PreboundTestHooksGuard;

#[cfg(test)]
impl Drop for PreboundTestHooksGuard {
    fn drop(&mut self) {
        *PREBOUND_TEST_HOOKS
            .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
            .lock()
            .expect("prebound test hook mutex is not poisoned") = PreboundTestHooks::default();
    }
}

#[cfg(test)]
fn install_prebound_test_hooks(
    server_spawn: Option<PreboundServerSpawnHook>,
    suppress_startup_status: Option<PreboundStartupStatusHook>,
) -> PreboundTestHooksGuard {
    *PREBOUND_TEST_HOOKS
        .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
        .lock()
        .expect("prebound test hook mutex is not poisoned") = PreboundTestHooks {
        server_spawn,
        suppress_startup_status,
        post_accept_admission: None,
        managed_remote_spawn: None,
        withheld_startup_senders: Vec::new(),
    };
    PreboundTestHooksGuard
}

#[cfg(test)]
fn install_post_accept_admission_hook(hook: PostAcceptAdmissionHook) -> PreboundTestHooksGuard {
    PREBOUND_TEST_HOOKS
        .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
        .lock()
        .expect("prebound test hook mutex is not poisoned")
        .post_accept_admission = Some(hook);
    PreboundTestHooksGuard
}

#[cfg(test)]
fn install_managed_remote_spawn_hook(hook: ManagedRemoteSpawnHook) -> PreboundTestHooksGuard {
    PREBOUND_TEST_HOOKS
        .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
        .lock()
        .expect("prebound test hook mutex is not poisoned")
        .managed_remote_spawn = Some(hook);
    PreboundTestHooksGuard
}

#[cfg(test)]
fn before_accept_admission(server_name: &str) {
    let hook = PREBOUND_TEST_HOOKS
        .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
        .lock()
        .expect("prebound test hook mutex is not poisoned")
        .post_accept_admission
        .clone();
    if let Some(hook) = hook {
        hook(server_name);
    }
}

#[cfg(test)]
fn after_managed_remote_spawn(server_name: &str) {
    let hook = PREBOUND_TEST_HOOKS
        .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
        .lock()
        .expect("prebound test hook mutex is not poisoned")
        .managed_remote_spawn
        .clone();
    if let Some(hook) = hook {
        hook(server_name);
    }
}

#[cfg(test)]
fn prebound_test_serial() -> &'static Mutex<()> {
    PREBOUND_TEST_SERIAL.get_or_init(|| Mutex::new(()))
}

fn before_prebound_server_spawn(name: &str) -> io::Result<()> {
    #[cfg(test)]
    let hook = {
        PREBOUND_TEST_HOOKS
            .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
            .lock()
            .expect("prebound test hook mutex is not poisoned")
            .server_spawn
            .clone()
    };
    #[cfg(test)]
    if let Some(hook) = hook {
        return hook(name);
    }
    let _ = name;
    Ok(())
}

fn suppress_prebound_startup_status(source: &PreboundListenerSource) -> bool {
    #[cfg(test)]
    let hook = {
        PREBOUND_TEST_HOOKS
            .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
            .lock()
            .expect("prebound test hook mutex is not poisoned")
            .suppress_startup_status
            .clone()
    };
    #[cfg(test)]
    if let Some(hook) = hook {
        return hook(source);
    }
    let _ = source;
    false
}

fn hold_prebound_startup_sender(sender: StartupSender) {
    #[cfg(test)]
    {
        PREBOUND_TEST_HOOKS
            .get_or_init(|| Mutex::new(PreboundTestHooks::default()))
            .lock()
            .expect("prebound test hook mutex is not poisoned")
            .withheld_startup_senders
            .push(sender);
    }
    #[cfg(not(test))]
    drop(sender);
}

struct PreboundListeners {
    v4: HashMap<String, StdTcpListener>,
    v5: HashMap<String, StdTcpListener>,
}

impl PreboundListeners {
    fn prepare(
        config: &Config,
        v4_listeners: Vec<NamedListener>,
        v5_listeners: Vec<NamedListener>,
    ) -> Result<Self, Error> {
        let v4_names = listener_names(&v4_listeners);
        let v5_names = listener_names(&v5_listeners);
        let configured_v4 = configured_listener_names(config.v4.as_ref());
        let configured_v5 = configured_listener_names(config.v5.as_ref());

        validate_listener_protocols(&v4_names, &configured_v4, &configured_v5, "v4", "v5")?;
        validate_listener_protocols(&v5_names, &configured_v5, &configured_v4, "v5", "v4")?;
        validate_listener_names("v4", &v4_names, &configured_v4)?;
        validate_listener_names("v5", &v5_names, &configured_v5)?;
        validate_listener_addresses("v4", &v4_listeners, config.v4.as_ref())?;
        validate_listener_addresses("v5", &v5_listeners, config.v5.as_ref())?;

        Ok(Self {
            v4: prepare_listener_map("v4", v4_listeners)?,
            v5: prepare_listener_map("v5", v5_listeners)?,
        })
    }

    fn len(&self) -> usize {
        self.v4.len() + self.v5.len()
    }
}

fn listener_source(protocol: &'static str, name: &str) -> PreboundListenerSource {
    PreboundListenerSource {
        protocol,
        name: name.to_owned(),
    }
}

fn listener_names(listeners: &[NamedListener]) -> Vec<String> {
    listeners.iter().map(|(name, _)| name.clone()).collect()
}

fn configured_listener_names(
    listeners: Option<&HashMap<String, ServerSettings>>,
) -> HashSet<String> {
    listeners
        .into_iter()
        .flat_map(|listeners| listeners.keys().cloned())
        .collect()
}

fn validate_listener_protocols(
    names: &[String],
    configured: &HashSet<String>,
    other_protocol: &HashSet<String>,
    protocol: &'static str,
    other_protocol_name: &'static str,
) -> Result<(), Error> {
    for name in names {
        if !configured.contains(name) && other_protocol.contains(name) {
            return Err(Error::PreboundListenerProtocol {
                name: name.clone(),
                expected: other_protocol_name,
                provided: protocol,
            });
        }
    }
    Ok(())
}

fn validate_listener_names(
    protocol: &'static str,
    names: &[String],
    configured: &HashSet<String>,
) -> Result<(), Error> {
    let mut seen = HashSet::new();
    let mut duplicates = Vec::new();
    for name in names {
        if !seen.insert(name.clone()) && !duplicates.contains(name) {
            duplicates.push(name.clone());
        }
    }

    let supplied: HashSet<_> = names.iter().cloned().collect();
    let mut missing: Vec<_> = configured.difference(&supplied).cloned().collect();
    let mut extra: Vec<_> = supplied.difference(configured).cloned().collect();
    missing.sort_unstable();
    extra.sort_unstable();
    duplicates.sort_unstable();

    if missing.is_empty() && extra.is_empty() && duplicates.is_empty() {
        Ok(())
    } else {
        Err(Error::PreboundListenerNames {
            protocol,
            missing,
            extra,
            duplicates,
        })
    }
}

fn validate_listener_addresses(
    protocol: &'static str,
    listeners: &[NamedListener],
    configured: Option<&HashMap<String, ServerSettings>>,
) -> Result<(), Error> {
    if listeners.is_empty() {
        return Ok(());
    }
    let configured = configured.expect("prebound listener names were validated");
    for (name, listener) in listeners {
        let listener_source = listener_source(protocol, name);
        let actual = listener
            .local_addr()
            .map_err(|source| Error::PreboundListenerIo {
                listener: listener_source.clone(),
                operation: "local_addr",
                source,
            })?;
        let expected = configured
            .get(name)
            .expect("prebound listener names were validated")
            .listen;
        if actual != expected {
            return Err(Error::PreboundListenerAddress {
                listener: listener_source,
                expected,
                actual,
            });
        }
    }
    Ok(())
}

fn prepare_listener_map(
    protocol: &'static str,
    listeners: Vec<NamedListener>,
) -> Result<HashMap<String, StdTcpListener>, Error> {
    let mut prepared = HashMap::with_capacity(listeners.len());
    for (name, listener) in listeners {
        listener
            .set_nonblocking(true)
            .map_err(|source| Error::PreboundListenerIo {
                listener: listener_source(protocol, &name),
                operation: "set_nonblocking",
                source,
            })?;
        prepared.insert(name, listener);
    }
    Ok(prepared)
}

pub struct Broker {
    config: Arc<Config>,
    router_tx: Sender<(ConnectionId, Event)>,
    router_join: Option<thread::JoinHandle<Result<(), crate::router::RouterError>>>,
}

struct ServerAcceptanceGate {
    closed: RwLock<bool>,
}

const REMOTE_TASKS_RUNNING: u8 = 0;
const REMOTE_TASKS_DRAIN: u8 = 1;
const REMOTE_TASKS_ABORT: u8 = 2;

#[derive(Clone)]
struct RemoteTaskShutdown {
    mode: Arc<AtomicU8>,
}

impl RemoteTaskShutdown {
    fn new() -> Self {
        Self {
            mode: Arc::new(AtomicU8::new(REMOTE_TASKS_RUNNING)),
        }
    }

    fn request_drain(&self) {
        let _ = self.mode.compare_exchange(
            REMOTE_TASKS_RUNNING,
            REMOTE_TASKS_DRAIN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn abort(&self) {
        self.mode.store(REMOTE_TASKS_ABORT, Ordering::Release);
    }

    fn should_drain(&self) -> bool {
        self.mode.load(Ordering::Acquire) == REMOTE_TASKS_DRAIN
    }
}

impl ServerAcceptanceGate {
    fn new() -> Self {
        Self {
            closed: RwLock::new(false),
        }
    }

    fn close(&self) {
        *self
            .closed
            .write()
            .expect("server acceptance gate lock is not poisoned") = true;
    }

    fn admit(&self, shutdown: &watch::Receiver<bool>) -> bool {
        let closed = self
            .closed
            .read()
            .expect("server acceptance gate lock is not poisoned");
        let shutting_down = shutdown.borrow();
        !*closed && !*shutting_down
    }
}

#[derive(Clone)]
pub struct InProcessBrokerControl {
    acceptance_gate: Arc<ServerAcceptanceGate>,
    graceful_shutdown: watch::Sender<bool>,
    force_cancellation: CancellationToken,
}

impl InProcessBrokerControl {
    pub fn stop_accepting(&self) {
        self.acceptance_gate.close();
        self.graceful_shutdown.send_replace(true);
    }

    pub fn force_stop(&self) {
        self.stop_accepting();
        self.force_cancellation.cancel();
    }
}

#[derive(Clone, Debug)]
enum InProcessTaskId {
    Router,
    Server(PreboundListenerSource),
}

impl std::fmt::Display for InProcessTaskId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Router => formatter.write_str("router"),
            Self::Server(source) => {
                write!(formatter, "{} listener {:?}", source.protocol, source.name)
            }
        }
    }
}

type InProcessTaskFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

struct PreparedInProcessTask {
    id: InProcessTaskId,
    future: InProcessTaskFuture,
}

/// A production-oriented broker composition that never creates a native
/// broker thread or a child Tokio runtime.
pub struct InProcessBroker {
    control: InProcessBrokerControl,
    graceful_state: watch::Receiver<bool>,
    router_cancellation: CancellationToken,
    tasks: Vec<PreparedInProcessTask>,
    server_count: usize,
}

impl Broker {
    pub fn new(config: Config) -> Result<Broker, Error> {
        let config = Arc::new(config);
        let router_config = config.router.clone();
        let router = Router::new_with_storage(
            config.id,
            router_config,
            config.storage.clone(),
            config.storage_policy,
        )?;

        // Setup cluster if cluster settings are configured.
        let (router_tx, router_join) = match config.cluster.clone() {
            Some(_cluster_config) => {
                // let node_id = cluster_config.node_id;
                // let listen = cluster_config.listen;
                // let seniors = cluster_config.seniors;
                // let mut cluster = RemoteCluster::new(node_id, &listen, seniors);
                // Broker::setup_remote_cluster(&mut router, node_id, &mut cluster);

                // Start router first and then cluster in the background
                let (router_tx, router_join) = router.spawn_with_join();
                // cluster.spawn();
                (router_tx, router_join)
            }
            None => router.spawn_with_join(),
        };
        Ok(Broker {
            config,
            router_tx,
            router_join: Some(router_join),
        })
    }

    pub fn spawn(self) -> BrokerHandle {
        self.spawn_with_listeners(None, None)
            .expect("broker thread must start")
    }

    pub fn new_with_prebound_listeners(
        config: Config,
        v4_listeners: Vec<NamedListener>,
        v5_listeners: Vec<NamedListener>,
        startup_timeout: Duration,
    ) -> Result<BrokerHandle, Error> {
        let listeners = PreboundListeners::prepare(&config, v4_listeners, v5_listeners)?;
        Self::new(config)?.spawn_prepared_prebound_listeners(listeners, startup_timeout)
    }

    pub fn spawn_with_prebound_listeners(
        mut self,
        v4_listeners: Vec<NamedListener>,
        v5_listeners: Vec<NamedListener>,
        startup_timeout: Duration,
    ) -> Result<BrokerHandle, Error> {
        let listeners = match PreboundListeners::prepare(&self.config, v4_listeners, v5_listeners) {
            Ok(listeners) => listeners,
            Err(error) => {
                self.shutdown_router()?;
                return Err(error);
            }
        };
        self.spawn_prepared_prebound_listeners(listeners, startup_timeout)
    }

    fn spawn_prepared_prebound_listeners(
        self,
        listeners: PreboundListeners,
        startup_timeout: Duration,
    ) -> Result<BrokerHandle, Error> {
        let listener_count = listeners.len();
        let mut pending = prebound_listener_sources(&listeners);
        let (startup_sender, startup_receiver) = mpsc::channel();
        let handle = self.spawn_with_listeners(Some(listeners), Some(startup_sender))?;
        let deadline = Instant::now().checked_add(startup_timeout);

        for _ in 0..listener_count {
            let remaining = deadline
                .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                .unwrap_or_default();
            if remaining.is_zero() {
                return shutdown_prebound_startup(
                    handle,
                    Error::PreboundListenerStartupTimeout {
                        timeout: startup_timeout,
                        pending,
                    },
                );
            }
            match startup_receiver.recv_timeout(remaining) {
                Ok(StartupStatus {
                    listener,
                    result: Ok(()),
                }) => pending.retain(|pending_listener| pending_listener != &listener),
                Ok(StartupStatus {
                    listener,
                    result: Err(error),
                }) => {
                    return shutdown_prebound_startup(
                        handle,
                        Error::PreboundListenerStartup { listener, error },
                    );
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return shutdown_prebound_startup(
                        handle,
                        Error::PreboundListenerStartupTimeout {
                            timeout: startup_timeout,
                            pending,
                        },
                    );
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return shutdown_prebound_startup(
                        handle,
                        Error::PreboundListenerStartup {
                            listener: pending
                                .first()
                                .cloned()
                                .expect("prebound listeners were configured"),
                            error: "broker exited before the listener became ready".to_owned(),
                        },
                    );
                }
            }
        }

        Ok(handle)
    }

    fn spawn_with_listeners(
        mut self,
        listeners: Option<PreboundListeners>,
        startup: Option<StartupSender>,
    ) -> Result<BrokerHandle, Error> {
        let (shutdown, receiver) = watch::channel(false);
        let acceptance_gate = Arc::new(ServerAcceptanceGate::new());
        let remote_task_shutdown = RemoteTaskShutdown::new();
        let router_tx = self.router_tx.clone();
        let shutdown_for_thread = shutdown.clone();
        let acceptance_gate_for_thread = Arc::clone(&acceptance_gate);
        let remote_task_shutdown_for_thread = remote_task_shutdown.clone();
        let join = thread::Builder::new()
            .name("iot-mqtt-core-broker".to_owned())
            .spawn(move || {
                self.start_with_shutdown(
                    receiver,
                    shutdown_for_thread,
                    acceptance_gate_for_thread,
                    remote_task_shutdown_for_thread,
                    listeners,
                    startup,
                )
            })?;
        Ok(BrokerHandle {
            shutdown,
            acceptance_gate,
            remote_task_shutdown,
            join: Some(join),
            router_tx,
        })
    }

    // pub fn new_local_cluster(
    //     config: Config,
    //     node_id: NodeId,
    //     seniors: Vec<(NodeId, Sender<ReplicationData>)>,
    // ) -> (Broker, Sender<ReplicationData>) {
    //     let config = Arc::new(config);
    //     let router_config = config.router.clone();

    //     let mut router = Router::new(config.id, router_config);
    //     let (mut cluster, tx) = LocalCluster::new(node_id, seniors);
    //     Broker::setup_local_cluster(&mut router, node_id, &mut cluster);

    //     // Start router first and then cluster in the background
    //     let router_tx = router.spawn();
    //     cluster.spawn();

    //     (Broker { config, router_tx }, tx)
    // }

    // fn setup_remote_cluster(router: &mut Router, node_id: NodeId, cluster: &mut RemoteCluster) {
    //     // Retrieve pre-configured list of router <-> replica link from router
    //     // and add the link to cluster
    //     let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
    //     for node_id in nodes {
    //         let link = router.get_replica_handle(node_id);
    //         cluster.add_replica_router_handle(node_id, link);
    //     }
    // }

    // fn setup_local_cluster(router: &mut Router, node_id: NodeId, cluster: &mut LocalCluster) {
    //     // Retrieve pre-configured list of router <-> replica link from router
    //     // and add the link to cluster
    //     let nodes: Vec<NodeId> = (0..MAX_NODES).filter(|v| *v != node_id).collect();
    //     for node_id in nodes {
    //         let link = router.get_replica_handle(node_id);
    //         cluster.add_replica_router_handle(node_id, link);
    //     }
    // }

    // Link to get meters
    pub fn meters(&self) -> Result<meters::MetersLink, meters::LinkError> {
        let link = meters::MetersLink::new(self.router_tx.clone())?;
        Ok(link)
    }

    // Link to get alerts
    pub fn alerts(&self) -> Result<alerts::AlertsLink, alerts::LinkError> {
        let link = alerts::AlertsLink::new(self.router_tx.clone())?;
        Ok(link)
    }

    pub fn link(&self, client_id: &str) -> Result<(LinkTx, LinkRx), local::LinkError> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex. max connection limit).
        let (link_tx, link_rx, _ack) =
            LinkBuilder::new(client_id, self.router_tx.clone()).build()?;
        Ok((link_tx, link_rx))
    }

    #[tracing::instrument(skip(self))]
    pub fn start(&mut self) -> Result<(), Error> {
        let (shutdown, receiver) = watch::channel(false);
        self.start_with_shutdown(
            receiver,
            shutdown,
            Arc::new(ServerAcceptanceGate::new()),
            RemoteTaskShutdown::new(),
            None,
            None,
        )
    }

    #[tracing::instrument(skip(self, acceptance_gate, remote_task_shutdown, listeners, startup))]
    fn start_with_shutdown(
        &mut self,
        shutdown: watch::Receiver<bool>,
        shutdown_sender: watch::Sender<bool>,
        acceptance_gate: Arc<ServerAcceptanceGate>,
        remote_task_shutdown: RemoteTaskShutdown,
        mut listeners: Option<PreboundListeners>,
        startup: Option<StartupSender>,
    ) -> Result<(), Error> {
        if self.config.v4.is_none()
            && self.config.v5.is_none()
            && (cfg!(not(feature = "websocket")) || self.config.ws.is_none())
        {
            let _ = self.shutdown_router();
            return Err(Error::Config(
                "Atleast one server config must be specified, \
                consider adding either of [v4.x]/[v5.x] or [ws.x] (if enabled) in config file."
                    .to_string(),
            ));
        }
        if self.config.metrics.is_some()
            || self.config.prometheus.is_some()
            || self.config.console.is_some()
        {
            let _ = self.shutdown_router();
            return Err(Error::Config(
                "metrics, prometheus, and console workers are unsupported by the controlled broker lifecycle"
                    .to_owned(),
            ));
        }

        // we don't know which servers (v4/v5/ws) user will spawn
        // so we collect handles for all of the spawned servers
        let mut server_thread_handles = Vec::new();

        if let Some(metrics_config) = self.config.metrics.clone() {
            let timer_thread = thread::Builder::new().name("timer".to_owned());
            let router_tx = self.router_tx.clone();
            timer_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async move {
                    timer::start(metrics_config, router_tx).await;
                });
            })?;
        }

        // Spawn bridge in a separate thread.
        if let Some(bridge_config) = self.config.bridge.clone() {
            let bridge_thread = thread::Builder::new().name(bridge_config.name.clone());
            let router_tx = self.router_tx.clone();
            let shutdown = shutdown.clone();
            let handle = match bridge_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();

                runtime.block_on(async move {
                    if let Err(e) =
                        bridge::start_with_shutdown(bridge_config, router_tx, V4, shutdown).await
                    {
                        error!(error=?e, "Bridge Link error");
                    };
                });
            }) {
                Ok(handle) => handle,
                Err(error) => {
                    self.shutdown_started_threads(
                        &shutdown_sender,
                        &acceptance_gate,
                        &remote_task_shutdown,
                        server_thread_handles,
                    );
                    return Err(error.into());
                }
            };
            server_thread_handles.push(handle);
        }

        // Spawn servers in a separate thread.
        if let Some(v4_config) = &self.config.v4 {
            for (name, config) in v4_config.clone() {
                let server_name = config.name.clone();
                let server_thread = thread::Builder::new().name(server_name.clone());
                let mut server = Server::new(config, self.router_tx.clone(), V4);
                server.set_acceptance_gate(Arc::clone(&acceptance_gate));
                server.set_remote_task_shutdown(remote_task_shutdown.clone());
                let shutdown = shutdown.clone();
                let listener = listeners.as_mut().map(|listeners| {
                    listeners
                        .v4
                        .remove(&name)
                        .expect("prebound v4 listener names were validated")
                });
                let startup = startup.clone();
                let source = listener_source("v4", &name);
                if let Err(error) = before_prebound_server_spawn(&server_name) {
                    self.shutdown_started_threads(
                        &shutdown_sender,
                        &acceptance_gate,
                        &remote_task_shutdown,
                        server_thread_handles,
                    );
                    return Err(error.into());
                }
                let handle = match server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        let result = match listener {
                            Some(listener) => {
                                server
                                    .start_with_prebound_listener(
                                        listener,
                                        LinkType::Remote,
                                        shutdown,
                                        startup.expect("prebound listener startup channel exists"),
                                        source,
                                    )
                                    .await
                            }
                            None => server.start(LinkType::Remote, shutdown).await,
                        };
                        if let Err(e) = result {
                            error!(error=?e, "Server error - V4");
                        }
                    });
                }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.shutdown_started_threads(
                            &shutdown_sender,
                            &acceptance_gate,
                            &remote_task_shutdown,
                            server_thread_handles,
                        );
                        return Err(error.into());
                    }
                };
                server_thread_handles.push(handle)
            }
        }

        if let Some(v5_config) = &self.config.v5 {
            for (name, config) in v5_config.clone() {
                let server_name = config.name.clone();
                let server_thread = thread::Builder::new().name(server_name.clone());
                let mut server = Server::new(config, self.router_tx.clone(), V5);
                server.set_acceptance_gate(Arc::clone(&acceptance_gate));
                server.set_remote_task_shutdown(remote_task_shutdown.clone());
                let shutdown = shutdown.clone();
                let listener = listeners.as_mut().map(|listeners| {
                    listeners
                        .v5
                        .remove(&name)
                        .expect("prebound v5 listener names were validated")
                });
                let startup = startup.clone();
                let source = listener_source("v5", &name);
                if let Err(error) = before_prebound_server_spawn(&server_name) {
                    self.shutdown_started_threads(
                        &shutdown_sender,
                        &acceptance_gate,
                        &remote_task_shutdown,
                        server_thread_handles,
                    );
                    return Err(error.into());
                }
                let handle = match server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        let result = match listener {
                            Some(listener) => {
                                server
                                    .start_with_prebound_listener(
                                        listener,
                                        LinkType::Remote,
                                        shutdown,
                                        startup.expect("prebound listener startup channel exists"),
                                        source,
                                    )
                                    .await
                            }
                            None => server.start(LinkType::Remote, shutdown).await,
                        };
                        if let Err(e) = result {
                            error!(error=?e, "Server error - V5");
                        }
                    });
                }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.shutdown_started_threads(
                            &shutdown_sender,
                            &acceptance_gate,
                            &remote_task_shutdown,
                            server_thread_handles,
                        );
                        return Err(error.into());
                    }
                };
                server_thread_handles.push(handle)
            }
        }

        drop(startup);

        #[cfg(not(feature = "websocket"))]
        if self.config.ws.is_some() {
            warn!("websocket feature is disabled, [ws] config will be ignored.");
        }

        #[cfg(feature = "websocket")]
        if let Some(ws_config) = &self.config.ws {
            for (_, config) in ws_config.clone() {
                let server_thread = thread::Builder::new().name(config.name.clone());
                //TODO: Add support for V5 procotol with websockets. Registered in config or on ServerSettings
                let mut server = Server::new(config, self.router_tx.clone(), V4);
                server.set_acceptance_gate(Arc::clone(&acceptance_gate));
                server.set_remote_task_shutdown(remote_task_shutdown.clone());
                let shutdown = shutdown.clone();
                let handle = match server_thread.spawn(move || {
                    let mut runtime = tokio::runtime::Builder::new_current_thread();
                    let runtime = runtime.enable_all().build().unwrap();

                    runtime.block_on(async {
                        if let Err(e) = server.start(LinkType::Websocket, shutdown).await {
                            error!(error=?e, "Server error - WS");
                        }
                    });
                }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.shutdown_started_threads(
                            &shutdown_sender,
                            &acceptance_gate,
                            &remote_task_shutdown,
                            server_thread_handles,
                        );
                        return Err(error.into());
                    }
                };
                server_thread_handles.push(handle)
            }
        }

        if let Some(prometheus_setting) = &self.config.prometheus {
            let timeout = prometheus_setting.interval;
            // If port is specified use it instead of listen.
            // NOTE: This means listen is ignored when `port` is specified.
            // `port` will be removed in future release in favour of `listen`
            let addr = {
                #[allow(deprecated)]
                match prometheus_setting.port {
                    Some(port) => SocketAddr::new("127.0.0.1".parse().unwrap(), port),
                    None => prometheus_setting.listen.unwrap_or(SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        9042,
                    )),
                }
            };
            let metrics_thread = thread::Builder::new().name("Metrics".to_owned());
            let meter_link = self.meters().unwrap();
            metrics_thread.spawn(move || {
                let builder = PrometheusBuilder::new().with_http_listener(addr);
                builder.install().unwrap();

                let total_publishes = gauge!("metrics.router.total_publishes");
                let total_connections = gauge!("metrics.router.total_connections");
                let failed_publishes = gauge!("metrics.router.failed_publishes");
                loop {
                    if let Ok(metrics) = meter_link.recv() {
                        for m in metrics {
                            match m {
                                Meter::Router(_, ref r) => {
                                    total_connections.set(r.total_connections as f64);
                                    total_publishes.set(r.total_publishes as f64);
                                    failed_publishes.set(r.failed_publishes as f64);
                                }
                                _ => continue,
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_secs(timeout));
                }
            })?;
        }

        if let Some(console) = self.config.console.clone() {
            let console_link = ConsoleLink::new(console, self.router_tx.clone());

            let console_link = Arc::new(console_link);
            let console_thread = thread::Builder::new().name("Console".to_string());
            console_thread.spawn(move || {
                let mut runtime = tokio::runtime::Builder::new_current_thread();
                let runtime = runtime.enable_all().build().unwrap();
                runtime.block_on(console::start(console_link));
            })?;
        }

        // in ideal case, where server doesn't crash, join() will never resolve
        // we still try to join threads so that we don't return from function
        // unless everything crashes.
        server_thread_handles.into_iter().for_each(|handle| {
            // join() might panic in case the thread panics
            // we just ignore it
            let _ = handle.join();
        });

        self.shutdown_router()?;
        Ok(())
    }

    fn shutdown_started_threads(
        &mut self,
        shutdown: &watch::Sender<bool>,
        acceptance_gate: &ServerAcceptanceGate,
        remote_task_shutdown: &RemoteTaskShutdown,
        server_thread_handles: Vec<thread::JoinHandle<()>>,
    ) {
        acceptance_gate.close();
        remote_task_shutdown.abort();
        shutdown.send_replace(true);
        for handle in server_thread_handles {
            let _ = handle.join();
        }
        let _ = self.shutdown_router();
    }

    fn shutdown_router(&mut self) -> Result<(), Error> {
        self.router_tx
            .send((0, Event::Shutdown))
            .map_err(Error::Send)?;
        if let Some(router_join) = self.router_join.take() {
            match router_join.join() {
                Err(_) => return Err(Error::Config("router thread panicked".to_owned())),
                Ok(Ok(())) | Ok(Err(crate::router::RouterError::Shutdown)) => {}
                Ok(Err(error)) => return Err(Error::Config(error.to_string())),
            }
        }
        Ok(())
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.shutdown_router();
    }
}

impl InProcessBroker {
    pub fn new_with_prebound_listeners(
        config: Config,
        v4_listeners: Vec<NamedListener>,
        v5_listeners: Vec<NamedListener>,
    ) -> Result<(Self, InProcessBrokerControl), Error> {
        if config.v4.is_none() && config.v5.is_none() {
            return Err(Error::Config(
                "at least one v4 or v5 listener must be configured for the in-process broker"
                    .to_owned(),
            ));
        }
        if config.bridge.is_some() {
            return Err(Error::Config(
                "bridge configuration is not yet supported by the in-process broker".to_owned(),
            ));
        }
        if config.ws.is_some() {
            return Err(Error::Config(
                "websocket configuration is not yet supported by the in-process broker".to_owned(),
            ));
        }
        if config.metrics.is_some() || config.prometheus.is_some() || config.console.is_some() {
            return Err(Error::Config(
                "metrics, prometheus, and console workers are not yet supported by the in-process broker"
                    .to_owned(),
            ));
        }
        if config.cluster.is_some() {
            return Err(Error::Config(
                "cluster configuration is not yet supported by the in-process broker".to_owned(),
            ));
        }

        let listeners = PreboundListeners::prepare(&config, v4_listeners, v5_listeners)?;
        let config = Arc::new(config);
        let router = Router::new_with_storage(
            config.id,
            config.router.clone(),
            config.storage.clone(),
            config.storage_policy,
        )?;
        let router_cancellation = CancellationToken::new();
        let managed_router = router.into_managed(router_cancellation.clone());
        let router_tx = managed_router.link();
        let acceptance_gate = Arc::new(ServerAcceptanceGate::new());
        let (graceful_shutdown, graceful_state) = watch::channel(false);
        let force_cancellation = CancellationToken::new();
        let control = InProcessBrokerControl {
            acceptance_gate: Arc::clone(&acceptance_gate),
            graceful_shutdown,
            force_cancellation: force_cancellation.clone(),
        };
        let PreboundListeners { mut v4, mut v5 } = listeners;
        let mut tasks = Vec::with_capacity(v4.len() + v5.len() + 1);

        if let Some(configured) = &config.v4 {
            let mut names = v4.keys().cloned().collect::<Vec<_>>();
            names.sort_unstable();
            for name in names {
                let listener = v4
                    .remove(&name)
                    .expect("prebound v4 listener names were validated");
                let settings = configured
                    .get(&name)
                    .expect("prebound v4 server names were validated")
                    .clone();
                let source = listener_source("v4", &settings.name);
                let mut server = Server::new(settings, router_tx.clone(), V4);
                server.set_acceptance_gate(Arc::clone(&acceptance_gate));
                let managed = server.into_managed_prebound(
                    listener,
                    source.clone(),
                    "v4",
                    LinkType::Remote,
                    control.graceful_shutdown.subscribe(),
                    force_cancellation.clone(),
                )?;
                tasks.push(PreparedInProcessTask {
                    id: InProcessTaskId::Server(source),
                    future: Box::pin(managed.run()),
                });
            }
        }

        if let Some(configured) = &config.v5 {
            let mut names = v5.keys().cloned().collect::<Vec<_>>();
            names.sort_unstable();
            for name in names {
                let listener = v5
                    .remove(&name)
                    .expect("prebound v5 listener names were validated");
                let settings = configured
                    .get(&name)
                    .expect("prebound v5 server names were validated")
                    .clone();
                let source = listener_source("v5", &settings.name);
                let mut server = Server::new(settings, router_tx.clone(), V5);
                server.set_acceptance_gate(Arc::clone(&acceptance_gate));
                let managed = server.into_managed_prebound(
                    listener,
                    source.clone(),
                    "v5",
                    LinkType::Remote,
                    control.graceful_shutdown.subscribe(),
                    force_cancellation.clone(),
                )?;
                tasks.push(PreparedInProcessTask {
                    id: InProcessTaskId::Server(source),
                    future: Box::pin(managed.run()),
                });
            }
        }

        tasks.push(PreparedInProcessTask {
            id: InProcessTaskId::Router,
            future: Box::pin(async move {
                managed_router
                    .run()
                    .await
                    .map_err(|error| Error::ManagedTask(format!("router: {error}")))
            }),
        });
        let server_count = tasks
            .iter()
            .filter(|task| matches!(task.id, InProcessTaskId::Server(_)))
            .count();

        Ok((
            Self {
                control: control.clone(),
                graceful_state,
                router_cancellation,
                tasks,
                server_count,
            },
            control,
        ))
    }

    pub async fn run(mut self) -> Result<(), Error> {
        let mut tasks = JoinSet::new();
        for PreparedInProcessTask { id, future } in self.tasks.drain(..) {
            tasks.spawn(async move { (id, future.await) });
        }

        let mut servers_remaining = self.server_count;
        let mut router_cancelled = false;
        let mut force_requested = false;
        let mut primary_error = None;
        if servers_remaining == 0 {
            self.router_cancellation.cancel();
            router_cancelled = true;
        }

        while !tasks.is_empty() {
            tokio::select! {
                _ = self.control.force_cancellation.cancelled(), if !force_requested => {
                    force_requested = true;
                }
                joined = tasks.join_next() => {
                    let (id, result) = match joined {
                        Some(Ok(outcome)) => outcome,
                        Some(Err(error)) => {
                            if primary_error.is_none() {
                                primary_error = Some(Error::ManagedTask(
                                    format!("in-process broker task join failure: {error}"),
                                ));
                            }
                            force_requested = true;
                            self.control.force_cancellation.cancel();
                            self.router_cancellation.cancel();
                            continue;
                        }
                        None => break,
                    };

                    let graceful_requested = *self.graceful_state.borrow();
                    let task_error = match result {
                        Ok(()) => match &id {
                            InProcessTaskId::Server(_) if !graceful_requested && !force_requested => {
                                Some(Error::ManagedTask(format!("{id} exited before shutdown")))
                            }
                            InProcessTaskId::Router if servers_remaining > 0 && !force_requested => {
                                Some(Error::ManagedTask("router exited before server tasks".to_owned()))
                            }
                            _ => None,
                        },
                        Err(error) => Some(Error::ManagedTask(format!("{id}: {error}"))),
                    };
                    if let Some(error) = task_error {
                        if primary_error.is_none() {
                            primary_error = Some(error);
                        }
                        force_requested = true;
                        self.control.force_cancellation.cancel();
                    }

                    if matches!(id, InProcessTaskId::Server(_)) {
                        servers_remaining = servers_remaining.saturating_sub(1);
                    }
                    if servers_remaining == 0 && !router_cancelled {
                        self.router_cancellation.cancel();
                        router_cancelled = true;
                    }
                }
            }
        }

        primary_error.map_or(Ok(()), Err)
    }
}

fn prebound_listener_sources(listeners: &PreboundListeners) -> Vec<PreboundListenerSource> {
    let mut sources = listeners
        .v4
        .keys()
        .map(|name| listener_source("v4", name))
        .chain(listeners.v5.keys().map(|name| listener_source("v5", name)))
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| (left.protocol, &left.name).cmp(&(right.protocol, &right.name)));
    sources
}

fn shutdown_prebound_startup(handle: BrokerHandle, error: Error) -> Result<BrokerHandle, Error> {
    handle.shutdown_immediately();
    handle.join()?;
    Err(error)
}

pub struct BrokerHandle {
    shutdown: watch::Sender<bool>,
    acceptance_gate: Arc<ServerAcceptanceGate>,
    remote_task_shutdown: RemoteTaskShutdown,
    join: Option<thread::JoinHandle<Result<(), Error>>>,
    router_tx: Sender<(ConnectionId, Event)>,
}

impl BrokerHandle {
    pub fn shutdown(&self) {
        self.acceptance_gate.close();
        self.remote_task_shutdown.request_drain();
        let _ = self.shutdown.send(true);
    }

    fn shutdown_immediately(&self) {
        self.acceptance_gate.close();
        self.remote_task_shutdown.abort();
        let _ = self.shutdown.send(true);
    }

    pub fn join(mut self) -> Result<(), Error> {
        self.join
            .take()
            .expect("broker join handle available")
            .join()
            .map_err(|_| Error::Config("broker thread panicked".to_owned()))?
    }

    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn link(&self, client_id: &str) -> Result<(LinkTx, LinkRx), local::LinkError> {
        let (link_tx, link_rx, _ack) =
            LinkBuilder::new(client_id, self.router_tx.clone()).build()?;
        Ok((link_tx, link_rx))
    }
}

#[derive(Copy, Clone)]
pub enum LinkType {
    #[cfg(feature = "websocket")]
    Websocket,
    Remote,
}

#[derive(PartialEq)]
enum AwaitingWill {
    Cancel,
    Fire,
}

pub struct Server<P> {
    config: ServerSettings,
    router_tx: Sender<(ConnectionId, Event)>,
    protocol: P,
    acceptance_gate: Arc<ServerAcceptanceGate>,
    remote_task_shutdown: RemoteTaskShutdown,
    awaiting_will_handler: Arc<Mutex<HashMap<String, Sender<AwaitingWill>>>>,
}

/// A prebound server future whose parent owns both listener and remote-link
/// task lifetimes.
pub struct ManagedServer<P> {
    server: Server<P>,
    listener: Option<TcpListener>,
    link_type: LinkType,
    graceful_shutdown: watch::Receiver<bool>,
    force_cancellation: CancellationToken,
}

impl<P: Protocol + Clone + Send + 'static> Server<P> {
    pub fn new(
        config: ServerSettings,
        router_tx: Sender<(ConnectionId, Event)>,
        protocol: P,
    ) -> Server<P> {
        Server {
            config,
            router_tx,
            protocol,
            acceptance_gate: Arc::new(ServerAcceptanceGate::new()),
            remote_task_shutdown: RemoteTaskShutdown::new(),
            awaiting_will_handler: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    fn set_acceptance_gate(&mut self, acceptance_gate: Arc<ServerAcceptanceGate>) {
        self.acceptance_gate = acceptance_gate;
    }

    fn set_remote_task_shutdown(&mut self, remote_task_shutdown: RemoteTaskShutdown) {
        self.remote_task_shutdown = remote_task_shutdown;
    }

    /// Converts a validated prebound listener before the parent registers the
    /// managed server task as ready.
    pub fn into_managed_prebound(
        self,
        listener: StdTcpListener,
        source: PreboundListenerSource,
        expected_protocol: &'static str,
        link_type: LinkType,
        graceful_shutdown: watch::Receiver<bool>,
        force_cancellation: CancellationToken,
    ) -> Result<ManagedServer<P>, Error> {
        if source.protocol != expected_protocol {
            return Err(Error::PreboundListenerProtocol {
                name: source.name,
                expected: expected_protocol,
                provided: source.protocol,
            });
        }
        if source.name != self.config.name {
            return Err(Error::PreboundListenerName {
                protocol: expected_protocol,
                expected: self.config.name.clone(),
                actual: source.name,
            });
        }
        let actual = listener
            .local_addr()
            .map_err(|io_error| Error::PreboundListenerIo {
                listener: source.clone(),
                operation: "local_addr",
                source: io_error,
            })?;
        if actual != self.config.listen {
            return Err(Error::PreboundListenerAddress {
                listener: source,
                expected: self.config.listen,
                actual,
            });
        }
        listener
            .set_nonblocking(true)
            .map_err(|io_error| Error::PreboundListenerIo {
                listener: source.clone(),
                operation: "set_nonblocking",
                source: io_error,
            })?;
        Ok(ManagedServer {
            server: self,
            listener: Some(TcpListener::from_std(listener).map_err(|io_error| {
                Error::PreboundListenerIo {
                    listener: source,
                    operation: "from_std",
                    source: io_error,
                }
            })?),
            link_type,
            graceful_shutdown,
            force_cancellation,
        })
    }

    // Depending on TLS or not create a new Network
    async fn tls_accept(&self, stream: TcpStream) -> Result<(Box<dyn N>, Option<String>), Error> {
        #[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
        match &self.config.tls {
            Some(c) => {
                let (tenant_id, network) = TLSAcceptor::new(c)?.accept(stream).await?;
                Ok((network, tenant_id))
            }
            None => Ok((Box::new(stream), None)),
        }
        #[cfg(not(any(feature = "use-rustls", feature = "use-native-tls")))]
        Ok((Box::new(stream), None))
    }

    pub async fn start(
        &mut self,
        link_type: LinkType,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), Error> {
        let listener = TcpListener::bind(&self.config.listen).await?;
        self.accept(listener, link_type, shutdown).await
    }

    async fn start_with_prebound_listener(
        &mut self,
        listener: StdTcpListener,
        link_type: LinkType,
        shutdown: watch::Receiver<bool>,
        startup: StartupSender,
        source: PreboundListenerSource,
    ) -> Result<(), Error> {
        let listener = match TcpListener::from_std(listener) {
            Ok(listener) => {
                if suppress_prebound_startup_status(&source) {
                    hold_prebound_startup_sender(startup);
                } else {
                    let _ = startup.send(StartupStatus {
                        listener: source,
                        result: Ok(()),
                    });
                }
                listener
            }
            Err(error) => {
                let _ = startup.send(StartupStatus {
                    listener: source,
                    result: Err(error.to_string()),
                });
                return Err(error.into());
            }
        };
        self.accept(listener, link_type, shutdown).await
    }

    async fn accept(
        &self,
        listener: TcpListener,
        link_type: LinkType,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), Error> {
        let delay = Duration::from_millis(self.config.next_connection_delay_ms);
        let mut count: usize = 0;
        let mut remote_tasks = Vec::new();

        let config = Arc::new(self.config.connections.clone());
        info!(
            config = self.config.name,
            listen_addr = self.config.listen.to_string(),
            "Listening for remote connections",
        );
        loop {
            // Await new network connection.
            let (stream, addr) = tokio::select! {
                result = listener.accept() => match result {
                    Ok((s, r)) => (s, r),
                    Err(e) => {
                        error!(error=?e, "Unable to accept socket.");
                        continue;
                    }
                },
                _ = shutdown.changed() => break,
            };

            #[cfg(test)]
            before_accept_admission(&self.config.name);

            if !self.acceptance_gate.admit(&shutdown) {
                continue;
            }

            let (network, tenant_id) = match tokio::select! {
                result = self.tls_accept(stream) => result,
                _ = shutdown.changed() => break,
            } {
                Ok(o) => o,
                Err(e) => {
                    error!(error=?e, "Tls accept error");
                    continue;
                }
            };

            info!(
                name=?self.config.name, ?addr, count, tenant=?tenant_id, "accept"
            );

            let config = config.clone();
            let router_tx = self.router_tx.clone();
            count += 1;

            let protocol = self.protocol.clone();
            match link_type {
                #[cfg(feature = "websocket")]
                LinkType::Websocket => {
                    let stream = tokio::select! {
                        result = accept_hdr_async(network, WSCallback) => match result {
                            Ok(stream) => Box::new(WsStream::new(stream)),
                            Err(error) => {
                                error!(error=?error, "Websocket failed handshake");
                                continue;
                            }
                        },
                        _ = shutdown.changed() => break,
                    };
                    remote_tasks.push(task::spawn(
                        remote(
                            config,
                            tenant_id.clone(),
                            router_tx,
                            stream,
                            protocol,
                            self.awaiting_will_handler.clone(),
                            shutdown.clone(),
                        )
                        .instrument(tracing::info_span!(
                            "websocket_link",
                            client_id = field::Empty,
                            connection_id = field::Empty
                        )),
                    ));
                }
                LinkType::Remote => remote_tasks.push(task::spawn(
                    remote(
                        config,
                        tenant_id.clone(),
                        router_tx,
                        network,
                        protocol,
                        self.awaiting_will_handler.clone(),
                        shutdown.clone(),
                    )
                    .instrument(tracing::error_span!(
                        "remote_link",
                        ?tenant_id,
                        client_id = field::Empty,
                        connection_id = field::Empty,
                    )),
                )),
            };

            tokio::select! {
                _ = time::sleep(delay) => {}
                _ = shutdown.changed() => break,
            }
        }
        if !self.remote_task_shutdown.should_drain() {
            for task in &remote_tasks {
                task.abort();
            }
        }
        for task in remote_tasks {
            let _ = task.await;
        }
        Ok(())
    }
}

impl<P: Protocol + Clone + Send + 'static> ManagedServer<P> {
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        let listener = self
            .listener
            .as_ref()
            .ok_or_else(|| Error::Config("managed listener is closed".to_owned()))?;
        Ok(listener.local_addr()?)
    }

    /// Runs the server in its parent Tokio runtime.
    ///
    /// Graceful shutdown closes admission but leaves established remote links
    /// running. Force cancellation signals those links, aborts any remaining
    /// work, and joins every child before returning.
    pub async fn run(mut self) -> Result<(), Error> {
        let delay = Duration::from_millis(self.server.config.next_connection_delay_ms);
        let config = Arc::new(self.server.config.connections.clone());
        let (force_shutdown, _) = watch::channel(false);
        let mut remote_tasks = JoinSet::new();
        let mut graceful_closed = *self.graceful_shutdown.borrow();
        if graceful_closed {
            self.server.acceptance_gate.close();
            self.listener.take();
        }

        info!(
            config = self.server.config.name,
            listen_addr = self.server.config.listen.to_string(),
            "Listening for managed remote connections",
        );

        'server: loop {
            if let Err(error) = Self::reap_finished_remote_tasks(&mut remote_tasks) {
                return Self::stop_after_remote_failure(
                    &mut self.listener,
                    &self.server,
                    &force_shutdown,
                    &mut remote_tasks,
                    error,
                )
                .await;
            }
            if self.force_cancellation.is_cancelled() {
                return Self::force_stop(
                    &mut self.listener,
                    &self.server,
                    &force_shutdown,
                    &mut remote_tasks,
                )
                .await;
            }
            if graceful_closed {
                if remote_tasks.is_empty() {
                    return Ok(());
                }
                tokio::select! {
                    _ = self.force_cancellation.cancelled() => {
                        return Self::force_stop(
                            &mut self.listener,
                            &self.server,
                            &force_shutdown,
                            &mut remote_tasks,
                        ).await;
                    }
                    result = remote_tasks.join_next() => {
                        if let Err(error) = Self::handle_remote_result(result) {
                            return Self::stop_after_remote_failure(
                                &mut self.listener,
                                &self.server,
                                &force_shutdown,
                                &mut remote_tasks,
                                error,
                            ).await;
                        }
                    }
                }
                continue;
            }

            tokio::select! {
                _ = self.force_cancellation.cancelled() => {
                    return Self::force_stop(
                        &mut self.listener,
                        &self.server,
                        &force_shutdown,
                        &mut remote_tasks,
                    ).await;
                }
                changed = self.graceful_shutdown.changed() => {
                    let _ = changed;
                    self.server.acceptance_gate.close();
                    self.listener.take();
                    graceful_closed = true;
                }
                result = remote_tasks.join_next(), if !remote_tasks.is_empty() => {
                    if let Err(error) = Self::handle_remote_result(result) {
                        return Self::stop_after_remote_failure(
                            &mut self.listener,
                            &self.server,
                            &force_shutdown,
                            &mut remote_tasks,
                            error,
                        ).await;
                    }
                }
                accepted = self.listener.as_ref().expect("managed listener is open while accepting").accept() => {
                    let (stream, addr) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            error!(?error, "Unable to accept managed socket.");
                            continue;
                        }
                    };

                    #[cfg(test)]
                    before_accept_admission(&self.server.config.name);

                    if !self.server.acceptance_gate.admit(&self.graceful_shutdown) {
                        continue;
                    }

                    let tls_accept = self.server.tls_accept(stream);
                    tokio::pin!(tls_accept);
                    let (network, tenant_id) = loop {
                        tokio::select! {
                            result = &mut tls_accept => match result {
                                Ok(network) => break network,
                                Err(error) => {
                                    error!(?error, "Managed TLS accept error");
                                    continue 'server;
                                }
                            },
                            result = remote_tasks.join_next(), if !remote_tasks.is_empty() => {
                                if let Err(error) = Self::handle_remote_result(result) {
                                    return Self::stop_after_remote_failure(
                                        &mut self.listener,
                                        &self.server,
                                        &force_shutdown,
                                        &mut remote_tasks,
                                        error,
                                    ).await;
                                }
                            }
                            changed = self.graceful_shutdown.changed(), if !graceful_closed => {
                                let _ = changed;
                                self.server.acceptance_gate.close();
                                self.listener.take();
                                graceful_closed = true;
                            }
                            _ = self.force_cancellation.cancelled() => {
                                return Self::force_stop(
                                    &mut self.listener,
                                    &self.server,
                                    &force_shutdown,
                                    &mut remote_tasks,
                                ).await;
                            }
                        }
                    };

                    info!(
                        name = ?self.server.config.name,
                        ?addr,
                        tenant = ?tenant_id,
                        "managed accept"
                    );

                    let remote_config = Arc::clone(&config);
                    let router_tx = self.server.router_tx.clone();
                    let protocol = self.server.protocol.clone();
                    let will_handlers = Arc::clone(&self.server.awaiting_will_handler);
                    let remote_shutdown = force_shutdown.subscribe();
                    match self.link_type {
                        #[cfg(feature = "websocket")]
                        LinkType::Websocket => {
                            let websocket_accept = accept_hdr_async(network, WSCallback);
                            tokio::pin!(websocket_accept);
                            let stream = loop {
                                tokio::select! {
                                    result = &mut websocket_accept => match result {
                                        Ok(stream) => break Box::new(WsStream::new(stream)),
                                        Err(error) => {
                                            error!(?error, "Managed websocket handshake failed");
                                            continue 'server;
                                        }
                                    },
                                    result = remote_tasks.join_next(), if !remote_tasks.is_empty() => {
                                        if let Err(error) = Self::handle_remote_result(result) {
                                            return Self::stop_after_remote_failure(
                                                &mut self.listener,
                                                &self.server,
                                                &force_shutdown,
                                                &mut remote_tasks,
                                                error,
                                            ).await;
                                        }
                                    }
                                    changed = self.graceful_shutdown.changed(), if !graceful_closed => {
                                        let _ = changed;
                                        self.server.acceptance_gate.close();
                                        self.listener.take();
                                        graceful_closed = true;
                                    }
                                    _ = self.force_cancellation.cancelled() => {
                                        return Self::force_stop(
                                            &mut self.listener,
                                            &self.server,
                                            &force_shutdown,
                                            &mut remote_tasks,
                                        ).await;
                                    }
                                }
                            };
                            remote_tasks.spawn(
                                remote(
                                    remote_config,
                                    tenant_id.clone(),
                                    router_tx,
                                    stream,
                                    protocol,
                                    will_handlers,
                                    remote_shutdown,
                                )
                                .instrument(tracing::info_span!(
                                    "managed_websocket_link",
                                    client_id = field::Empty,
                                    connection_id = field::Empty
                                )),
                            );
                        }
                        LinkType::Remote => {
                            remote_tasks.spawn(
                                remote(
                                    remote_config,
                                    tenant_id.clone(),
                                    router_tx,
                                    network,
                                    protocol,
                                    will_handlers,
                                    remote_shutdown,
                                )
                                .instrument(tracing::error_span!(
                                    "managed_remote_link",
                                    ?tenant_id,
                                    client_id = field::Empty,
                                    connection_id = field::Empty,
                                )),
                            );
                        }
                    }

                    #[cfg(test)]
                    after_managed_remote_spawn(&self.server.config.name);

                    let next_connection_delay = time::sleep(delay);
                    tokio::pin!(next_connection_delay);
                    loop {
                        tokio::select! {
                            _ = &mut next_connection_delay => break,
                            result = remote_tasks.join_next(), if !remote_tasks.is_empty() => {
                                if let Err(error) = Self::handle_remote_result(result) {
                                    return Self::stop_after_remote_failure(
                                        &mut self.listener,
                                        &self.server,
                                        &force_shutdown,
                                        &mut remote_tasks,
                                        error,
                                    ).await;
                                }
                            }
                            _ = self.force_cancellation.cancelled() => {
                                return Self::force_stop(
                                    &mut self.listener,
                                    &self.server,
                                    &force_shutdown,
                                    &mut remote_tasks,
                                ).await;
                            }
                            changed = self.graceful_shutdown.changed() => {
                                let _ = changed;
                                self.server.acceptance_gate.close();
                                self.listener.take();
                                graceful_closed = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    fn reap_finished_remote_tasks(remote_tasks: &mut JoinSet<()>) -> Result<(), Error> {
        while let Some(result) = remote_tasks.try_join_next() {
            Self::handle_remote_result(Some(result))?;
        }
        Ok(())
    }

    fn handle_remote_result(result: Option<Result<(), JoinError>>) -> Result<(), Error> {
        match result {
            Some(Ok(())) | None => Ok(()),
            Some(Err(error)) => Err(Error::ManagedTask(error.to_string())),
        }
    }

    async fn force_stop(
        listener: &mut Option<TcpListener>,
        server: &Server<P>,
        force_shutdown: &watch::Sender<bool>,
        remote_tasks: &mut JoinSet<()>,
    ) -> Result<(), Error> {
        listener.take();
        server.acceptance_gate.close();
        force_shutdown.send_replace(true);
        remote_tasks.abort_all();

        let mut first_error = None;
        while let Some(result) = remote_tasks.join_next().await {
            if let Err(error) = result {
                if !error.is_cancelled() && first_error.is_none() {
                    first_error = Some(Error::ManagedTask(error.to_string()));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn stop_after_remote_failure(
        listener: &mut Option<TcpListener>,
        server: &Server<P>,
        force_shutdown: &watch::Sender<bool>,
        remote_tasks: &mut JoinSet<()>,
        error: Error,
    ) -> Result<(), Error> {
        let _ = Self::force_stop(listener, server, force_shutdown, remote_tasks).await;
        Err(error)
    }
}

/// Configures the Websocket connection to indicate the correct protocol
/// by adding the "sec-websocket-protocol" with value of "mqtt" to the response header
#[cfg(feature = "websocket")]
struct WSCallback;
#[cfg(feature = "websocket")]
impl Callback for WSCallback {
    fn on_request(
        self,
        _request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        response
            .headers_mut()
            .insert("sec-websocket-protocol", HeaderValue::from_static("mqtt"));
        Ok(response)
    }
}

/// A new network connection should wait for a mqtt connect packet. This should be handled
/// asynchronously to avoid blocking other new connections while this connection is
/// waiting for mqtt connect packet. Also this honours connection wait time as per config to prevent
/// denial of service attacks (rogue clients which only establish network connections without
/// sending a mqtt connection packet to make the server reach its concurrent connection limit).
async fn remote<P: Protocol>(
    config: Arc<ConnectionSettings>,
    tenant_id: Option<String>,
    router_tx: Sender<(ConnectionId, Event)>,
    stream: Box<dyn N>,
    protocol: P,
    will_handlers: Arc<Mutex<HashMap<String, Sender<AwaitingWill>>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut network = Network::new(
        stream,
        config.max_payload_size,
        config.max_inflight_count,
        protocol,
    );

    let dynamic_filters = config.dynamic_filters;

    let connect_result = match mqtt_connect_with_identity(
        config.clone(),
        &mut network,
        tenant_id.as_deref(),
        &mut shutdown,
    )
    .await
    {
        Ok(result) => result,
        Err(remote::Error::Shutdown) => return,
        Err(e) => {
            error!(error=?e, "Error while handling MQTT connect packet");
            return;
        }
    };
    let remote::MqttConnectResult {
        packet: connect_packet,
        assigned_client_id,
        effective_client_id,
    } = connect_result;

    let clean_session = match &connect_packet {
        Packet::Connect(ref connect, _, _, _, _) => connect.clean_session,
        _ => unreachable!(),
    };

    let login = match &connect_packet {
        Packet::Connect(_, _, _, _, login) => login.as_ref(),
        _ => unreachable!(),
    };
    let authenticated_principal = configured_principal(&config, login);
    if let Err(error) = authorize_connect(
        config.authorization_handler.as_ref(),
        &effective_client_id,
        login,
        authenticated_principal.clone(),
    )
    .await
    {
        error!(error=?error, "Error while authorizing MQTT CONNECT");
        return;
    }

    if let Some(sender) = will_handlers.lock().unwrap().remove(&effective_client_id) {
        let awaiting_will = if clean_session {
            AwaitingWill::Fire
        } else {
            AwaitingWill::Cancel
        };
        if sender.try_send(awaiting_will).is_err() {
            warn!(client_id = %effective_client_id, "stale MQTT will cancellation receiver");
        }
    }

    // Start the link
    let mut link = match RemoteLink::new(
        router_tx.clone(),
        tenant_id.clone(),
        network,
        connect_packet,
        dynamic_filters,
        assigned_client_id,
        effective_client_id.clone(),
        authenticated_principal,
        config.authorization_handler.clone(),
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            error!(error=?e, "Remote link error");
            return;
        }
    };
    let (will_tx, will_rx) = flume::bounded::<AwaitingWill>(1);
    will_handlers
        .lock()
        .unwrap()
        .insert(effective_client_id.clone(), will_tx);

    let connection_id = link.connection_id;
    let will_delay_interval = link.will_delay_interval;
    let mut send_disconnect = true;

    match link.start(shutdown.clone()).await {
        // Connection got closed. This shouldn't usually happen.
        Ok(_) => error!("connection-stop"),
        // No need to send a disconnect message when disconnection
        // originated internally in the router.
        Err(remote::Error::Link(e)) => {
            error!(error=?e, "router-drop");
            send_disconnect = false;
        }
        // Connection was closed by peer
        Err(remote::Error::Network(network::Error::Io(err)) | remote::Error::Io(err))
            if err.kind() == io::ErrorKind::ConnectionAborted =>
        {
            info!(error=?err, "disconnected");
        }
        // Any other error
        Err(e) => {
            error!(error=?e, "disconnected");
        }
    };

    let shutting_down = *shutdown.borrow();
    if send_disconnect {
        let disconnect = Event::Disconnect;
        let message = (connection_id, disconnect);
        router_tx.send(message).ok();
    }

    // this is important to stop the connection
    drop(link);
    if shutting_down {
        return;
    }

    let publish_will = match tokio::time::timeout(
        Duration::from_secs(will_delay_interval as u64),
        will_rx.recv_async(),
    )
    .await
    {
        Ok(w) => w.is_ok_and(|k| k == AwaitingWill::Fire),
        Err(_) => {
            // no need to keep the sender after timeout
            will_handlers.lock().unwrap().remove(&effective_client_id);
            // as will delay interval has passed, publish the will message
            true
        }
    };

    if publish_will {
        let message = Event::PublishWill((effective_client_id, tenant_id));
        // is this connection_id really correct at this point?
        // as we have disconnected already, some other connection
        // might be using this connection ID!
        // It won't matter in this case as we don't use it
        // but might affect logs?
        router_tx.send((connection_id, message)).ok();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        sync::{mpsc, Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    use crate::router::Event;
    use crate::{
        BridgeConfig, Config, ConnectionSettings, PrometheusSetting, ServerSettings, Transport,
    };
    use tokio::{
        io::AsyncReadExt, net::TcpStream as TokioTcpStream, sync::watch, task::JoinSet,
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;

    use super::{
        install_managed_remote_spawn_hook, install_post_accept_admission_hook,
        install_prebound_test_hooks, prebound_test_serial, Broker, BrokerHandle, Error,
        InProcessBroker, LinkType, PreboundListenerSource, Server, V4,
    };

    #[test]
    fn prebound_v5_spawn_failure_joins_the_started_v4_server() {
        let _serial = prebound_test_serial()
            .lock()
            .expect("prebound test serial mutex is not poisoned");
        let v4_name = "v311-spawn-failure";
        let v5_name = "v5-spawn-failure";
        let (mut config, v4_listener, v5_listener) = prebound_test_config_named(v4_name, v5_name);
        let v4_address = v4_listener.local_addr().unwrap();
        let v5_address = v5_listener.local_addr().unwrap();
        let (authorization_started_tx, authorization_started_rx) = mpsc::channel();
        let (authorization_release_tx, authorization_release_rx) = tokio::sync::oneshot::channel();
        let authorization_release = Arc::new(Mutex::new(Some(authorization_release_rx)));
        install_blocking_v4_auth(
            &mut config,
            v4_name,
            authorization_started_tx,
            Arc::clone(&authorization_release),
        );

        let (v5_spawned_tx, v5_spawned_rx) = mpsc::sync_channel(1);
        let (allow_v5_failure_tx, allow_v5_failure_rx) = mpsc::sync_channel(1);
        let allow_v5_failure_rx = Arc::new(Mutex::new(allow_v5_failure_rx));
        let (v4_ready_tx, v4_ready_rx) = mpsc::sync_channel(1);
        let _hooks = install_prebound_test_hooks(
            Some(Arc::new(move |name| {
                if name == v5_name {
                    v5_spawned_tx.send(()).unwrap();
                    allow_v5_failure_rx.lock().unwrap().recv().unwrap();
                    return Err(std::io::Error::other("injected v5 spawn failure"));
                }
                Ok(())
            })),
            Some(Arc::new(move |source: &PreboundListenerSource| {
                if source.protocol == "v4" && source.name == v4_name {
                    let _ = v4_ready_tx.send(());
                }
                false
            })),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let starter = thread::spawn(move || {
            result_tx
                .send(Broker::new_with_prebound_listeners(
                    config,
                    vec![(v4_name.to_owned(), v4_listener)],
                    vec![(v5_name.to_owned(), v5_listener)],
                    Duration::from_secs(2),
                ))
                .unwrap();
        });

        v5_spawned_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("v5 startup hook was not reached");
        v4_ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("v4 listener did not report ready");
        let stream = connect_until(v4_address);
        if authorization_started_rx
            .recv_timeout(Duration::from_secs(1))
            .is_err()
        {
            allow_v5_failure_tx.send(()).unwrap();
            let _ = result_rx.recv_timeout(Duration::from_secs(1));
            starter.join().unwrap();
            panic!("v4 authorization did not start");
        }
        allow_v5_failure_tx.send(()).unwrap();
        let result = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("spawn failure did not return");
        assert!(result.is_err());

        let v4_rebindable = TcpListener::bind(v4_address).is_ok();
        let v5_rebindable = TcpListener::bind(v5_address).is_ok();

        drop(stream);
        let _ = authorization_release_tx.send(());
        starter.join().unwrap();
        wait_until_bindable(v4_address);
        wait_until_bindable(v5_address);

        assert!(v4_rebindable, "started v4 listener remained detached");
        assert!(v5_rebindable, "unspawned v5 listener remained owned");
    }

    #[test]
    fn prebound_startup_timeout_aborts_stalled_ready_remote_work() {
        let _serial = prebound_test_serial()
            .lock()
            .expect("prebound test serial mutex is not poisoned");
        let v4_name = "v311-startup-timeout";
        let v5_name = "v5-startup-timeout";
        let (mut config, v4_listener, v5_listener) = prebound_test_config_named(v4_name, v5_name);
        let v4_address = v4_listener.local_addr().unwrap();
        let v5_address = v5_listener.local_addr().unwrap();
        let (authorization_started_tx, authorization_started_rx) = mpsc::channel();
        let (authorization_release_tx, authorization_release_rx) = tokio::sync::oneshot::channel();
        let authorization_release = Arc::new(Mutex::new(Some(authorization_release_rx)));
        install_blocking_v4_auth(
            &mut config,
            v4_name,
            authorization_started_tx,
            Arc::clone(&authorization_release),
        );
        let (v4_ready_tx, v4_ready_rx) = mpsc::sync_channel(1);
        let (v5_status_tx, v5_status_rx) = mpsc::sync_channel(1);
        let (allow_v5_status_tx, allow_v5_status_rx) = mpsc::sync_channel(1);
        let allow_v5_status_rx = Arc::new(Mutex::new(allow_v5_status_rx));
        let _hooks = install_prebound_test_hooks(
            None,
            Some(Arc::new(move |source: &PreboundListenerSource| {
                if source.protocol == "v4" && source.name == v4_name {
                    let _ = v4_ready_tx.send(());
                }
                if source.protocol == "v5" && source.name == v5_name {
                    let _ = v5_status_tx.send(());
                    allow_v5_status_rx.lock().unwrap().recv().unwrap();
                    return true;
                }
                false
            })),
        );
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let starter = thread::spawn(move || {
            result_tx
                .send(Broker::new_with_prebound_listeners(
                    config,
                    vec![(v4_name.to_owned(), v4_listener)],
                    vec![(v5_name.to_owned(), v5_listener)],
                    Duration::from_secs(1),
                ))
                .unwrap();
        });

        v4_ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("v4 listener did not report ready");
        v5_status_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("v5 listener did not reach the readiness gate");
        let stream = connect_until(v4_address);
        let authorization_started = authorization_started_rx
            .recv_timeout(Duration::from_secs(1))
            .is_ok();
        allow_v5_status_tx.send(()).unwrap();
        if !authorization_started {
            drop(stream);
            let _ = authorization_release_tx.send(());
            let _ = result_rx.recv_timeout(Duration::from_secs(2));
            starter.join().unwrap();
            panic!("v4 authorization did not start");
        }
        let result = result_rx.recv_timeout(Duration::from_millis(1500));
        let returned_before_bound = result.is_ok();

        drop(stream);
        let _ = authorization_release_tx.send(());
        if returned_before_bound {
            starter.join().unwrap();
            let error = match result.unwrap() {
                Ok(handle) => {
                    handle.shutdown();
                    let _ = handle.join();
                    panic!("startup timeout unexpectedly returned a broker handle");
                }
                Err(error) => error,
            };
            assert!(matches!(
                error,
                Error::PreboundListenerStartupTimeout { .. }
            ));
        } else {
            starter.join().unwrap();
        }
        wait_until_bindable(v4_address);
        wait_until_bindable(v5_address);

        assert!(
            returned_before_bound,
            "startup timeout must abort stalled remote work before returning"
        );
    }

    #[test]
    fn prebound_listeners_reject_missing_names_without_rebinding_configured_addresses() {
        let (config, v4_guard, v5_guard) = prebound_test_config();

        let error = prebound_error(Broker::new_with_prebound_listeners(
            config,
            vec![],
            vec![("v5".to_owned(), v5_guard.try_clone().unwrap())],
            Duration::from_millis(250),
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerNames {
                protocol: "v4",
                missing,
                extra,
                duplicates,
            } if missing == vec!["v311"] && extra.is_empty() && duplicates.is_empty()
        ));
        assert_configured_addresses_remained_guarded(&v4_guard, &v5_guard);
    }

    #[test]
    fn prebound_listeners_reject_extra_names_without_rebinding_configured_addresses() {
        let (config, v4_guard, v5_guard) = prebound_test_config();
        let v311_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let extra_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let error = prebound_error(Broker::new_with_prebound_listeners(
            config,
            vec![
                ("v311".to_owned(), v311_listener),
                ("extra".to_owned(), extra_listener),
            ],
            vec![("v5".to_owned(), v5_guard.try_clone().unwrap())],
            Duration::from_millis(250),
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerNames {
                protocol: "v4",
                missing,
                extra,
                duplicates,
            } if missing.is_empty() && extra == vec!["extra"] && duplicates.is_empty()
        ));
        assert_configured_addresses_remained_guarded(&v4_guard, &v5_guard);
    }

    #[test]
    fn prebound_listeners_reject_wrong_protocol_names_without_rebinding_configured_addresses() {
        let (config, v4_guard, v5_guard) = prebound_test_config();
        let v4_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let v5_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let error = prebound_error(Broker::new_with_prebound_listeners(
            config,
            vec![("v5".to_owned(), v4_listener)],
            vec![("v311".to_owned(), v5_listener)],
            Duration::from_millis(250),
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerProtocol {
                name,
                expected: "v5",
                provided: "v4",
            } if name == "v5"
        ));
        assert_configured_addresses_remained_guarded(&v4_guard, &v5_guard);
    }

    #[test]
    fn prebound_listeners_reject_duplicate_names_without_rebinding_configured_addresses() {
        let (config, v4_guard, v5_guard) = prebound_test_config();
        let first_v311_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let second_v311_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let error = prebound_error(Broker::new_with_prebound_listeners(
            config,
            vec![
                ("v311".to_owned(), first_v311_listener),
                ("v311".to_owned(), second_v311_listener),
            ],
            vec![("v5".to_owned(), v5_guard.try_clone().unwrap())],
            Duration::from_millis(250),
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerNames {
                protocol: "v4",
                missing,
                extra,
                duplicates,
            } if missing.is_empty() && extra.is_empty() && duplicates == vec!["v311"]
        ));
        assert_configured_addresses_remained_guarded(&v4_guard, &v5_guard);
    }

    #[test]
    fn prebound_listeners_reject_listener_address_mismatch_before_router_startup() {
        let (config, v4_guard, v5_guard) = prebound_test_config();
        let v311_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let actual = v311_listener.local_addr().unwrap();
        let expected = v4_guard.local_addr().unwrap();

        let error = prebound_error(Broker::new_with_prebound_listeners(
            config,
            vec![("v311".to_owned(), v311_listener)],
            vec![("v5".to_owned(), v5_guard.try_clone().unwrap())],
            Duration::from_millis(250),
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerAddress {
                listener,
                expected: error_expected,
                actual: error_actual,
            } if listener.protocol == "v4"
                && listener.name == "v311"
                && error_expected == expected
                && error_actual == actual
        ));
        assert_configured_addresses_remained_guarded(&v4_guard, &v5_guard);
    }

    #[test]
    fn prebound_listener_readiness_deadline_shuts_down_all_threads() {
        let (config, v4_guard, v5_guard) = prebound_test_config();

        let error = prebound_error(Broker::new(config).unwrap().spawn_with_prebound_listeners(
            vec![("v311".to_owned(), v4_guard.try_clone().unwrap())],
            vec![("v5".to_owned(), v5_guard.try_clone().unwrap())],
            Duration::ZERO,
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerStartupTimeout { pending, .. }
                if pending.len() == 2
        ));
        assert_configured_addresses_remained_guarded(&v4_guard, &v5_guard);
    }

    #[test]
    fn prebound_listener_validation_allows_an_absent_protocol_with_no_listeners() {
        let (mut config, v4_guard, _v5_guard) = prebound_test_config();
        config.v5 = None;

        let error = prebound_error(Broker::new_with_prebound_listeners(
            config,
            vec![("v311".to_owned(), v4_guard.try_clone().unwrap())],
            vec![],
            Duration::ZERO,
        ));

        assert!(matches!(
            error,
            Error::PreboundListenerStartupTimeout { pending, .. }
                if pending
                    == vec![super::PreboundListenerSource {
                        protocol: "v4",
                        name: "v311".to_owned(),
                    }]
        ));
    }

    #[test]
    fn shutdown_rejects_v4_prebound_connection_accepted_before_admission() {
        shutdown_rejects_prebound_connection_accepted_before_admission("v4");
    }

    #[test]
    fn shutdown_rejects_v5_prebound_connection_accepted_before_admission() {
        shutdown_rejects_prebound_connection_accepted_before_admission("v5");
    }

    #[test]
    fn acceptance_gate_rejects_admission_when_shutdown_closes_first() {
        let gate = super::ServerAcceptanceGate::new();
        let (shutdown, receiver) = watch::channel(false);

        gate.close();

        assert!(!gate.admit(&receiver));
        shutdown.send(true).unwrap();
        assert!(!gate.admit(&receiver));
    }

    #[tokio::test]
    async fn managed_prebound_server_converts_listener_before_parent_run() {
        let (config, v4_listener, _v5_listener) = prebound_test_config();
        let address = v4_listener.local_addr().unwrap();
        let settings = config.v4.as_ref().unwrap()["v311"].clone();
        let (router_tx, _router_rx) = flume::bounded(1);
        let server = Server::new(settings, router_tx, V4);
        let (graceful_shutdown, graceful_receiver) = watch::channel(false);
        let force = CancellationToken::new();

        let managed = server
            .into_managed_prebound(
                v4_listener,
                v4_prebound_source(),
                "v4",
                LinkType::Remote,
                graceful_receiver,
                force,
            )
            .expect("prebound listener must convert before the parent spawns the task");
        assert_eq!(managed.local_addr().unwrap(), address);

        graceful_shutdown.send(true).unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(managed.run());
        assert!(matches!(
            timeout(Duration::from_secs(1), tasks.join_next())
                .await
                .expect("managed server must stop after graceful admission closure"),
            Some(Ok(Ok(())))
        ));
    }

    #[tokio::test]
    async fn in_process_broker_owns_prebound_listeners_and_parent_tasks() {
        let (config, v4_listener, v5_listener) = prebound_test_config();
        let v4_address = v4_listener.local_addr().unwrap();
        let v5_address = v5_listener.local_addr().unwrap();
        let (broker, control) = InProcessBroker::new_with_prebound_listeners(
            config,
            vec![("v311".to_owned(), v4_listener)],
            vec![("v5".to_owned(), v5_listener)],
        )
        .expect("in-process broker must validate and retain prebound listeners");

        assert!(TcpListener::bind(v4_address).is_err());
        assert!(TcpListener::bind(v5_address).is_err());

        let mut tasks = JoinSet::new();
        tasks.spawn(broker.run());
        control.stop_accepting();

        assert!(matches!(
            timeout(Duration::from_secs(1), tasks.join_next())
                .await
                .expect("parent must join the in-process broker task"),
            Some(Ok(Ok(())))
        ));
        assert!(TcpListener::bind(v4_address).is_ok());
        assert!(TcpListener::bind(v5_address).is_ok());
    }

    #[tokio::test]
    async fn in_process_broker_accepts_prebound_keys_that_differ_from_server_names() {
        let (mut config, v4_listener, v5_listener) =
            prebound_test_config_named("v4-map-key", "v5-map-key");
        config
            .v4
            .as_mut()
            .unwrap()
            .get_mut("v4-map-key")
            .unwrap()
            .name = "runtime-v4".to_owned();
        config
            .v5
            .as_mut()
            .unwrap()
            .get_mut("v5-map-key")
            .unwrap()
            .name = "runtime-v5".to_owned();

        let (broker, control) = InProcessBroker::new_with_prebound_listeners(
            config,
            vec![("v4-map-key".to_owned(), v4_listener)],
            vec![("v5-map-key".to_owned(), v5_listener)],
        )
        .expect("prebound listener map keys must remain independent from server names");

        let mut tasks = JoinSet::new();
        tasks.spawn(broker.run());
        control.stop_accepting();
        assert!(matches!(
            timeout(Duration::from_secs(1), tasks.join_next())
                .await
                .expect("in-process broker must stop after graceful admission closure"),
            Some(Ok(Ok(())))
        ));
    }

    #[test]
    fn managed_prebound_server_rejects_an_address_mismatch_before_parent_run() {
        let (config, _v4_listener, _v5_listener) = prebound_test_config();
        let settings = config.v4.as_ref().unwrap()["v311"].clone();
        let mismatched = TcpListener::bind("127.0.0.1:0").unwrap();
        let actual = mismatched.local_addr().unwrap();
        let (router_tx, _router_rx) = flume::bounded(1);
        let server = Server::new(settings.clone(), router_tx, V4);
        let (_graceful_shutdown, graceful_receiver) = watch::channel(false);

        assert!(matches!(
            server.into_managed_prebound(
                mismatched,
                v4_prebound_source(),
                "v4",
                LinkType::Remote,
                graceful_receiver,
                CancellationToken::new(),
            ),
            Err(Error::PreboundListenerAddress {
                listener,
                expected,
                actual: error_actual,
            }) if listener == v4_prebound_source()
                && expected == settings.listen
                && error_actual == actual
        ));
    }

    #[test]
    fn managed_prebound_server_rejects_mismatched_source_metadata_before_parent_run() {
        let (config, v4_listener, _v5_listener) = prebound_test_config();
        let settings = config.v4.as_ref().unwrap()["v311"].clone();
        let (_graceful_shutdown, graceful_receiver) = watch::channel(false);
        let (router_tx, _router_rx) = flume::bounded(1);
        let server = Server::new(settings.clone(), router_tx, V4);

        assert!(matches!(
            server.into_managed_prebound(
                v4_listener.try_clone().unwrap(),
                PreboundListenerSource {
                    protocol: "v5",
                    name: "v311".to_owned(),
                },
                "v4",
                LinkType::Remote,
                graceful_receiver,
                CancellationToken::new(),
            ),
            Err(Error::PreboundListenerProtocol {
                name,
                expected: "v4",
                provided: "v5",
            }) if name == "v311"
        ));

        let (_graceful_shutdown, graceful_receiver) = watch::channel(false);
        let (router_tx, _router_rx) = flume::bounded(1);
        let server = Server::new(settings, router_tx, V4);
        assert!(matches!(
            server.into_managed_prebound(
                v4_listener,
                PreboundListenerSource {
                    protocol: "v4",
                    name: "wrong-name".to_owned(),
                },
                "v4",
                LinkType::Remote,
                graceful_receiver,
                CancellationToken::new(),
            ),
            Err(Error::PreboundListenerName {
                protocol: "v4",
                expected,
                actual,
            }) if expected == "v311" && actual == "wrong-name"
        ));
    }

    #[tokio::test]
    async fn managed_prebound_server_force_cancels_and_joins_a_remote_task() {
        let _serial = prebound_test_serial()
            .lock()
            .expect("prebound test serial mutex is not poisoned");
        let (config, v4_listener, _v5_listener) = prebound_test_config();
        let settings = config.v4.as_ref().unwrap()["v311"].clone();
        let (router_tx, _router_rx) = flume::bounded(1);
        let server = Server::new(settings, router_tx, V4);
        let (graceful_shutdown, graceful_receiver) = watch::channel(false);
        let force = CancellationToken::new();
        let force_signal = force.clone();
        let (spawned_tx, spawned_rx) = mpsc::sync_channel(1);
        let _hooks = install_managed_remote_spawn_hook(Arc::new(move |_| {
            spawned_tx.send(()).unwrap();
        }));
        let managed = server
            .into_managed_prebound(
                v4_listener,
                v4_prebound_source(),
                "v4",
                LinkType::Remote,
                graceful_receiver,
                force,
            )
            .unwrap();
        let address = managed.local_addr().unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(managed.run());

        let mut client = TokioTcpStream::connect(address).await.unwrap();
        tokio::task::spawn_blocking(move || spawned_rx.recv_timeout(Duration::from_secs(1)))
            .await
            .expect("spawned-task waiter must not panic")
            .expect("managed server must spawn the remote task");

        force_signal.cancel();
        assert!(matches!(
            timeout(Duration::from_secs(1), tasks.join_next())
                .await
                .expect("force cancellation must join the managed server"),
            Some(Ok(Ok(())))
        ));

        let mut byte = [0_u8; 1];
        assert!(matches!(
            timeout(Duration::from_secs(1), client.read(&mut byte)).await,
            Ok(Ok(0)) | Ok(Err(_))
        ));
        drop(graceful_shutdown);
    }

    #[tokio::test]
    async fn managed_prebound_server_graceful_shutdown_keeps_an_admitted_remote_task_alive() {
        let _serial = prebound_test_serial()
            .lock()
            .expect("prebound test serial mutex is not poisoned");
        let (config, v4_listener, _v5_listener) = prebound_test_config();
        let settings = config.v4.as_ref().unwrap()["v311"].clone();
        let (router_tx, _router_rx) = flume::bounded(1);
        let server = Server::new(settings, router_tx, V4);
        let (graceful_shutdown, graceful_receiver) = watch::channel(false);
        let force = CancellationToken::new();
        let force_signal = force.clone();
        let (spawned_tx, spawned_rx) = mpsc::sync_channel(1);
        let _hooks = install_managed_remote_spawn_hook(Arc::new(move |_| {
            spawned_tx.send(()).unwrap();
        }));
        let managed = server
            .into_managed_prebound(
                v4_listener,
                v4_prebound_source(),
                "v4",
                LinkType::Remote,
                graceful_receiver,
                force,
            )
            .unwrap();
        let address = managed.local_addr().unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(managed.run());

        let mut client = TokioTcpStream::connect(address).await.unwrap();
        tokio::task::spawn_blocking(move || spawned_rx.recv_timeout(Duration::from_secs(1)))
            .await
            .expect("spawned-task waiter must not panic")
            .expect("managed server must spawn the remote task");

        graceful_shutdown.send(true).unwrap();
        let rebound = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                match TcpListener::bind(address) {
                    Ok(listener) => return listener,
                    Err(error) if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(5));
                        let _ = error;
                    }
                    Err(error) => panic!("graceful shutdown did not release {address}: {error}"),
                }
            }
        })
        .await
        .expect("listener rebind waiter must not panic");
        drop(rebound);
        assert!(timeout(Duration::from_millis(100), tasks.join_next())
            .await
            .is_err());
        let mut byte = [0_u8; 1];
        assert!(timeout(Duration::from_millis(100), client.read(&mut byte))
            .await
            .is_err());

        force_signal.cancel();
        assert!(matches!(
            timeout(Duration::from_secs(1), tasks.join_next())
                .await
                .expect("force cancellation must join the managed server"),
            Some(Ok(Ok(())))
        ));
    }

    #[test]
    fn dropping_an_unspawned_broker_stops_the_router() {
        let broker = Broker::new(Config::default()).unwrap();
        let router_tx = broker.router_tx.clone();

        drop(broker);

        assert!(router_tx.send((0, Event::Shutdown)).is_err());
    }

    fn prebound_test_config() -> (Config, TcpListener, TcpListener) {
        prebound_test_config_named("v311", "v5")
    }

    fn v4_prebound_source() -> PreboundListenerSource {
        PreboundListenerSource {
            protocol: "v4",
            name: "v311".to_owned(),
        }
    }

    #[test]
    fn managed_server_api_reexports_prebound_listener_source() {
        let source: crate::PreboundListenerSource = v4_prebound_source();
        assert_eq!(source.protocol, "v4");
        assert_eq!(source.name, "v311");
    }

    fn shutdown_rejects_prebound_connection_accepted_before_admission(protocol: &str) {
        let _serial = prebound_test_serial()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (config, v4_listener, v5_listener) = prebound_test_config();
        let v4_address = v4_listener.local_addr().unwrap();
        let v5_address = v5_listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let expected_server = match protocol {
            "v4" => "v311",
            "v5" => "v5",
            _ => panic!("unsupported protocol {protocol}"),
        }
        .to_owned();
        let hook_server = expected_server.clone();
        let _hooks = install_post_accept_admission_hook(Arc::new(move |server_name| {
            if server_name == hook_server {
                accepted_tx.send(server_name.to_owned()).unwrap();
                release_rx
                    .lock()
                    .expect("admission release mutex is not poisoned")
                    .recv()
                    .expect("admission test must release the server");
            }
        }));
        let handle = Broker::new_with_prebound_listeners(
            config,
            vec![("v311".to_owned(), v4_listener)],
            vec![("v5".to_owned(), v5_listener)],
            Duration::from_secs(1),
        )
        .unwrap();
        let address = if protocol == "v4" {
            v4_address
        } else {
            v5_address
        };
        let mut stream = TcpStream::connect(address).unwrap();
        let connect = if protocol == "v4" {
            mqtt_v4_connect_with_login()
        } else {
            mqtt_v5_connect_with_login()
        };
        stream.write_all(&connect).unwrap();
        assert_eq!(
            accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected_server
        );

        handle.shutdown();
        release_tx.send(()).unwrap();
        handle.join().unwrap();

        assert_socket_closed_without_response(&mut stream);
        assert!(TcpListener::bind(v4_address).is_ok());
        assert!(TcpListener::bind(v5_address).is_ok());
    }

    fn prebound_test_config_named(
        v4_name: &str,
        v5_name: &str,
    ) -> (Config, TcpListener, TcpListener) {
        let v4_guard = TcpListener::bind("127.0.0.1:0").unwrap();
        let v5_guard = TcpListener::bind("127.0.0.1:0").unwrap();
        let v4_address = v4_guard.local_addr().unwrap();
        let v5_address = v5_guard.local_addr().unwrap();
        let connection_settings = ConnectionSettings {
            connection_timeout_ms: 60_000,
            max_payload_size: 1024,
            max_inflight_count: 10,
            auth: None,
            external_auth: None,
            authorization_handler: None,
            dynamic_filters: true,
        };
        let v4 = HashMap::from([(
            v4_name.to_owned(),
            ServerSettings {
                name: v4_name.to_owned(),
                listen: v4_address,
                tls: None,
                next_connection_delay_ms: 0,
                connections: connection_settings.clone(),
            },
        )]);
        let v5 = HashMap::from([(
            v5_name.to_owned(),
            ServerSettings {
                name: v5_name.to_owned(),
                listen: v5_address,
                tls: None,
                next_connection_delay_ms: 0,
                connections: connection_settings,
            },
        )]);
        (
            Config {
                v4: Some(v4),
                v5: Some(v5),
                ..Config::default()
            },
            v4_guard,
            v5_guard,
        )
    }

    fn install_blocking_v4_auth(
        config: &mut Config,
        v4_name: &str,
        started: mpsc::Sender<()>,
        release: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    ) {
        config
            .v4
            .as_mut()
            .unwrap()
            .get_mut(v4_name)
            .unwrap()
            .connections
            .external_auth = Some(Arc::new(move |_, _, _| {
            let started = started.clone();
            let release = release
                .lock()
                .expect("authorization release mutex is not poisoned")
                .take()
                .expect("authorization is invoked once");
            Box::pin(async move {
                started.send(()).unwrap();
                release.await.is_ok()
            })
        }));
    }

    fn connect_until(address: SocketAddr) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match TcpStream::connect(address) {
                Ok(mut stream) => {
                    stream.write_all(&mqtt_v4_connect_with_login()).unwrap();
                    return stream;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("listener did not start: {error}"),
            }
        }
    }

    fn mqtt_v4_connect_with_login() -> Vec<u8> {
        let client_id = b"test";
        let username = b"user";
        let password = b"pass";
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
            0x04,
            0xc2,
            0x00,
            0x3c,
            0x00,
            client_id.len() as u8,
        ];
        packet.extend_from_slice(client_id);
        packet.extend_from_slice(&[0x00, username.len() as u8]);
        packet.extend_from_slice(username);
        packet.extend_from_slice(&[0x00, password.len() as u8]);
        packet.extend_from_slice(password);
        packet
    }

    fn mqtt_v5_connect_with_login() -> Vec<u8> {
        let client_id = b"test";
        let username = b"user";
        let password = b"pass";
        let remaining = 11 + 2 + client_id.len() + 2 + username.len() + 2 + password.len();
        let mut packet = vec![
            0x10,
            remaining as u8,
            0x00,
            0x04,
            b'M',
            b'Q',
            b'T',
            b'T',
            0x05,
            0xc2,
            0x00,
            0x3c,
            0x00,
            0x00,
            client_id.len() as u8,
        ];
        packet.extend_from_slice(client_id);
        packet.extend_from_slice(&[0x00, username.len() as u8]);
        packet.extend_from_slice(username);
        packet.extend_from_slice(&[0x00, password.len() as u8]);
        packet.extend_from_slice(password);
        packet
    }

    fn assert_socket_closed_without_response(stream: &mut TcpStream) {
        let mut response = [0; 16];
        match stream.read(&mut response) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::NotConnected
                ) => {}
            Ok(read) => panic!("shutdown sent {read} response bytes after admission closed"),
            Err(error) => panic!("shutdown did not close accepted socket: {error}"),
        }
    }

    fn connect_before_deadline(address: SocketAddr) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match TcpStream::connect(address) {
                Ok(stream) => return stream,
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::yield_now();
                }
                Err(error) => panic!("listener did not start: {error}"),
            }
        }
    }

    fn wait_until_bindable(address: SocketAddr) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Ok(listener) = TcpListener::bind(address) {
                drop(listener);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "listener {address} was not released"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn assert_configured_addresses_remained_guarded(
        v4_guard: &TcpListener,
        v5_guard: &TcpListener,
    ) {
        for listener in [v4_guard, v5_guard] {
            let error = TcpListener::bind(listener.local_addr().unwrap()).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        }
    }

    fn prebound_error(result: Result<BrokerHandle, Error>) -> Error {
        match result {
            Ok(handle) => {
                handle.shutdown();
                let _ = handle.join();
                panic!("invalid prebound listener set unexpectedly started the broker");
            }
            Err(error) => error,
        }
    }

    #[test]
    fn controlled_lifecycle_rejects_detached_worker_configuration() {
        #[allow(deprecated)]
        let config = Config {
            v4: Some(HashMap::new()),
            prometheus: Some(PrometheusSetting {
                port: None,
                listen: None,
                interval: 1,
            }),
            ..Config::default()
        };

        let Error::Config(message) = Broker::new(config).unwrap().spawn().join().unwrap_err()
        else {
            panic!("detached worker configuration must return a configuration error");
        };
        assert!(
            message.contains("unsupported"),
            "unexpected lifecycle error: {message}"
        );
        assert!(
            message.contains("controlled broker lifecycle"),
            "unexpected lifecycle error: {message}"
        );
    }

    #[test]
    fn no_listener_configuration_stops_router_before_returning_error() {
        let mut broker = Broker::new(Config::default()).unwrap();
        assert!(matches!(broker.start(), Err(Error::Config(_))));
        assert!(broker.router_join.is_none());
    }

    #[test]
    fn shutdown_cancels_bridge_reconnect_delay() {
        let bridge = BridgeConfig {
            name: "test-bridge".to_owned(),
            addr: "127.0.0.1:1".to_owned(),
            qos: 1,
            sub_path: "#".to_owned(),
            reconnection_delay: 60,
            ping_delay: 60,
            connections: ConnectionSettings {
                connection_timeout_ms: 60_000,
                max_payload_size: 1024,
                max_inflight_count: 10,
                auth: None,
                external_auth: None,
                authorization_handler: None,
                dynamic_filters: true,
            },
            transport: Transport::Tcp,
        };
        let handle = Broker::new(Config {
            v4: Some(HashMap::new()),
            bridge: Some(bridge),
            ..Config::default()
        })
        .unwrap()
        .spawn();
        thread::sleep(Duration::from_millis(100));
        handle.shutdown();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let _ = sender.send(handle.join());
        });
        assert!(
            receiver.recv_timeout(Duration::from_secs(1)).is_ok(),
            "shutdown must cancel bridge reconnect delay"
        );
    }

    #[cfg(feature = "websocket")]
    #[test]
    fn shutdown_rejects_websocket_upgrade_accepted_before_admission() {
        let _serial = prebound_test_serial()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let _hooks = install_post_accept_admission_hook(Arc::new(move |server_name| {
            if server_name == "ws-admission" {
                accepted_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .expect("admission release mutex is not poisoned")
                    .recv()
                    .expect("admission test must release the server");
            }
        }));
        let handle = Broker::new(Config {
            ws: Some(HashMap::from([(
                "ws-admission".to_owned(),
                ServerSettings {
                    name: "ws-admission".to_owned(),
                    listen: address,
                    tls: None,
                    next_connection_delay_ms: 0,
                    connections: ConnectionSettings {
                        connection_timeout_ms: 60_000,
                        max_payload_size: 1024,
                        max_inflight_count: 10,
                        auth: None,
                        external_auth: None,
                        authorization_handler: None,
                        dynamic_filters: true,
                    },
                },
            )])),
            ..Config::default()
        })
        .unwrap()
        .spawn();
        let mut stream = connect_before_deadline(address);
        stream
            .write_all(
                b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n",
            )
            .unwrap();
        accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        handle.shutdown();
        release_tx.send(()).unwrap();
        handle.join().unwrap();

        assert_socket_closed_without_response(&mut stream);
        assert!(TcpListener::bind(address).is_ok());
    }

    #[cfg(feature = "websocket")]
    #[test]
    fn shutdown_cancels_stalled_websocket_upgrade() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address: SocketAddr = listener.local_addr().unwrap();
        drop(listener);
        let mut listeners = HashMap::new();
        listeners.insert(
            "ws".to_owned(),
            ServerSettings {
                name: "ws".to_owned(),
                listen: address,
                tls: None,
                next_connection_delay_ms: 0,
                connections: ConnectionSettings {
                    connection_timeout_ms: 60_000,
                    max_payload_size: 1024,
                    max_inflight_count: 10,
                    auth: None,
                    external_auth: None,
                    authorization_handler: None,
                    dynamic_filters: true,
                },
            },
        );
        let handle = Broker::new(Config {
            ws: Some(listeners),
            ..Config::default()
        })
        .unwrap()
        .spawn();
        let deadline = Instant::now() + Duration::from_secs(1);
        let _stalled = loop {
            match TcpStream::connect(address) {
                Ok(stream) => break stream,
                Err(error) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                    let _ = error;
                }
                Err(error) => panic!("websocket listener did not start: {error}"),
            }
        };
        thread::sleep(Duration::from_millis(100));
        handle.shutdown();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let _ = sender.send(handle.join());
        });
        assert!(
            receiver.recv_timeout(Duration::from_secs(1)).is_ok(),
            "shutdown must not wait for a websocket upgrade"
        );
    }
}
