use std::{
    fs::File,
    future::Future,
    io::{self, Read},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use fs2::FileExt;
use iot_api::TokenVault;
use iot_nano_core::{
    CommandTransport, CoreRuntime, CoreRuntimeConfig, EmailSender, IngestMetrics, NotificationError,
};
use iot_nano_foundation::StorageConfiguration;
use iot_nano_mqttd::{
    BrokerStorage, CachePort, CommandResponsePort, DeviceAuthorizationPort, DeviceClaimCodePort,
    MqttListenerConfig, MqttRuntime, MqttRuntimeConfig, RetentionPolicy, RpcSessionRouter,
    SqliteStorage,
};
use iot_nano_stream::{LocalStream, StreamConfig, StreamPort};
use iot_storage::{PlatformStore, PlatformStoreError};
use thiserror::Error;
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::management::SystemInfrastructureStatus;
use crate::{
    BootstrapSystemError, CacheError, ManagementSessionRouter, MonolithConfig, PersistentCache,
    PlatformCommandResponse, PlatformCommandTransport, PlatformCoreFacade,
    PlatformDeviceAuthorization, PlatformDeviceClaimCode, Readiness,
};

const INTERNAL_DIRECTORY_MARKER: &str = ".iot-nano-monolith-state";
const INTERNAL_DIRECTORY_MARKER_CONTENT: &[u8] = b"iot-nano-monolith-state-v1\n";
const INTERNAL_STATE_FILES: [&str; 4] = [
    "stream.sqlite",
    "mqttd.sqlite",
    "cache.sqlite",
    "instance.lock",
];
const INTERNAL_SQLITE_STATE_FILES: [&str; 3] = ["stream.sqlite", "mqttd.sqlite", "cache.sqlite"];

pub struct MonolithRuntime {
    internal_directory: Option<InternalDirectory>,
    instance_lock: Option<InstanceLock>,
    platform: Option<Arc<PlatformStore>>,
    stream: Option<Arc<LocalStream>>,
    mqtt_storage: Option<Arc<SqliteStorage>>,
    cache: Option<Arc<PersistentCache>>,
    core: Option<Arc<CoreRuntime>>,
    mqtt: Option<MqttRuntime>,
    http_cancellation: CancellationToken,
    http_tasks: Vec<JoinHandle<io::Result<()>>>,
    readiness_monitor: Option<JoinHandle<()>>,
    readiness: Readiness,
    cancellation: CancellationToken,
    failure_cancellation: CancellationToken,
}

impl MonolithRuntime {
    pub async fn bootstrap_system(
        storage: &StorageConfiguration,
        username: &str,
        password: &str,
    ) -> Result<(), BootstrapSystemError> {
        let platform = PlatformStore::open(storage)
            .await
            .map_err(BootstrapSystemError::PlatformMigration)?;
        crate::management::bootstrap_system(&platform, username, password)
            .await
            .map(|_| ())
    }

    pub async fn migrate(config: &MonolithConfig) -> Result<(), StartupError> {
        let internal_directory = prepare_internal_directory(&config.internal_dir)?;
        let instance_lock = InstanceLock::acquire_blocking(&internal_directory)?;
        let platform = PlatformStore::open(&config.storage)
            .await
            .map_err(StartupError::PlatformMigration)?;
        drop(platform);
        drop(instance_lock);
        drop(internal_directory);
        Ok(())
    }

    pub async fn start(config: MonolithConfig) -> Result<Self, StartupError> {
        let internal_directory = prepare_internal_directory(&config.internal_dir)?;
        let instance_lock = InstanceLock::acquire(&internal_directory)?;
        let cancellation = CancellationToken::new();
        let failure_cancellation = CancellationToken::new();
        let http_cancellation = CancellationToken::new();
        let platform = Arc::new(
            PlatformStore::open(&config.storage)
                .await
                .map_err(StartupError::PlatformMigration)?,
        );
        let stream_path = internal_directory
            .prepare_state_file("stream.sqlite")
            .map_err(StartupError::InternalDirectory)?;
        let stream = Arc::new(
            LocalStream::open(StreamConfig::sqlite(stream_path))
                .await
                .map_err(StartupError::StreamRecovery)?,
        );
        let mqtt_path = internal_directory
            .prepare_state_file("mqttd.sqlite")
            .map_err(StartupError::InternalDirectory)?;
        let mqtt_storage = Arc::new(
            tokio::task::spawn_blocking(move || recover_mqtt_storage(mqtt_path))
                .await
                .map_err(|error| StartupError::MqttStorageTask(error.to_string()))?
                .map_err(StartupError::MqttStorage)?,
        );
        let cache_file = internal_directory
            .open_state_file("cache.sqlite")
            .map_err(StartupError::InternalDirectory)?;
        let cache_path = internal_directory
            .state_path("cache.sqlite")
            .map_err(StartupError::InternalDirectory)?;
        let cache = Arc::new(
            PersistentCache::open_file(cache_file, cache_path)
                .await
                .map_err(StartupError::CacheRecovery)?,
        );

        let device_authorization: Arc<dyn DeviceAuthorizationPort> =
            Arc::new(PlatformDeviceAuthorization::new(Arc::clone(&platform)));
        let command_responses: Arc<dyn CommandResponsePort> =
            Arc::new(PlatformCommandResponse::new(Arc::clone(&platform)));
        let device_claim_codes: Arc<dyn DeviceClaimCodePort> =
            Arc::new(PlatformDeviceClaimCode::new(Arc::clone(&platform)));
        let mqtt_storage_port: Arc<dyn BrokerStorage> = mqtt_storage.clone();
        let stream_port: Arc<dyn StreamPort> = stream.clone();
        let cache_port: Arc<dyn CachePort> = cache.clone();
        let session_router = RpcSessionRouter::default();
        let command_transport: Arc<dyn CommandTransport> = Arc::new(PlatformCommandTransport::new(
            session_router.clone(),
            Arc::clone(&device_authorization),
        ));
        let core = Arc::new(
            CoreRuntime::start(default_core_runtime_config(
                Arc::clone(&platform),
                Arc::clone(&stream_port),
                command_transport,
                cancellation.clone(),
            ))
            .await
            .map_err(StartupError::CoreRuntime)?,
        );
        let readiness = Readiness::default();
        let token_vault = TokenVault::from_key_material(&config.device_token_vault_key);
        let infrastructure_status = SystemInfrastructureStatus::starting(readiness.clone());
        let management_sessions = ManagementSessionRouter::new_with_infrastructure_status(
            Arc::clone(&platform),
            token_vault.clone(),
            infrastructure_status.clone(),
        );
        let browser_session_verifier: Arc<dyn iot_api::OAuthBrowserSessionVerifier> =
            management_sessions.session_verifier.clone();
        let public_router = health_router(readiness.clone())
            .merge(crate::ota::router(Arc::clone(&platform)))
            .merge(iot_api::public_v1_router(
                Arc::clone(&platform),
                token_vault,
                Arc::new(PlatformCoreFacade::new(Arc::clone(&platform))),
            ))
            .merge(iot_api::public_oauth_router_with_browser_session_verifier(
                Arc::clone(&platform),
                browser_session_verifier,
            ));
        let mqtt_cancellation = cancellation.child_token();
        let mut mqtt = match MqttRuntime::start(MqttRuntimeConfig {
            listeners: MqttListenerConfig {
                plaintext_address: config.mqtt_tcp,
                tls_address: config.mqtt_tls,
                tls_cert_path: config.tls_cert_path.clone(),
                tls_key_path: config.tls_key_path.clone(),
                max_connections: 1_024,
                max_payload_size: 1_048_576,
                max_inflight_count: 64,
            },
            storage: mqtt_storage_port,
            authorization: device_authorization,
            stream: stream_port,
            command_responses,
            device_claim_codes: Some(device_claim_codes),
            cache: cache_port,
            session_router,
            cancellation: mqtt_cancellation.clone(),
        })
        .await
        {
            Ok(mqtt) => mqtt,
            Err(error) => {
                cancellation.cancel();
                let _ = core.drain(Instant::now() + config.shutdown_deadline).await;
                return Err(StartupError::MqttRuntime(error));
            }
        };

        let public_listener = match TcpListener::bind(config.public_http).await {
            Ok(listener) => listener,
            Err(error) => {
                cleanup_started_components(&core, &mut mqtt, config.shutdown_deadline).await;
                return Err(StartupError::PublicHttpBind(error));
            }
        };
        let management_listener = match TcpListener::bind(config.management_http).await {
            Ok(listener) => listener,
            Err(error) => {
                cleanup_started_components(&core, &mut mqtt, config.shutdown_deadline).await;
                return Err(StartupError::ManagementHttpBind(error));
            }
        };
        infrastructure_status.mark_started(
            &config.storage,
            public_listener.local_addr().unwrap_or(config.public_http),
            management_listener
                .local_addr()
                .unwrap_or(config.management_http),
            mqtt.plaintext_address(),
        );
        let http_tasks = vec![
            spawn_http_server(public_listener, public_router, http_cancellation.clone()),
            spawn_http_server(
                management_listener,
                health_router(readiness.clone()).merge(management_sessions.router),
                http_cancellation.clone(),
            ),
        ];
        let readiness_monitor = spawn_readiness_monitor(
            readiness.clone(),
            cancellation.clone(),
            failure_cancellation.clone(),
            Arc::clone(&core),
            mqtt_cancellation,
        );
        readiness.mark_ready();

        Ok(Self {
            internal_directory: Some(internal_directory),
            instance_lock: Some(instance_lock),
            platform: Some(platform),
            stream: Some(stream),
            mqtt_storage: Some(mqtt_storage),
            cache: Some(cache),
            core: Some(core),
            mqtt: Some(mqtt),
            http_cancellation,
            http_tasks,
            readiness_monitor: Some(readiness_monitor),
            readiness,
            cancellation,
            failure_cancellation,
        })
    }

    pub fn readiness(&self) -> Readiness {
        self.readiness.clone()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn failure_token(&self) -> CancellationToken {
        self.failure_cancellation.clone()
    }

    pub fn platform(&self) -> Option<&PlatformStore> {
        self.platform.as_deref()
    }

    pub fn stream(&self) -> Option<&Arc<LocalStream>> {
        self.stream.as_ref()
    }

    pub fn mqtt_storage(&self) -> Option<&Arc<SqliteStorage>> {
        self.mqtt_storage.as_ref()
    }

    pub fn mqtt_tcp_address(&self) -> Option<SocketAddr> {
        self.mqtt.as_ref().map(MqttRuntime::plaintext_address)
    }

    pub fn cache(&self) -> Option<&Arc<PersistentCache>> {
        self.cache.as_ref()
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), ShutdownError> {
        let deadline_was_elapsed = Instant::now() > deadline;
        self.readiness.mark_not_ready();
        self.stop_readiness_monitor().await;
        self.http_cancellation.cancel();
        let mut first_error = join_http_tasks(&mut self.http_tasks, deadline).await.err();
        if let Some(mqtt) = self.mqtt.as_mut()
            && let Err(error) = mqtt.stop_accepting().await
        {
            first_error.get_or_insert(ShutdownError::MqttRuntime(error));
        }
        if let Some(core) = self.core.take()
            && let Err(error) = core.drain(deadline).await
        {
            first_error.get_or_insert(ShutdownError::CoreRuntime(error));
        }
        if let Some(mut mqtt) = self.mqtt.take() {
            if let Err(error) = mqtt.drain(deadline).await {
                first_error.get_or_insert(ShutdownError::MqttRuntime(error));
            }
        }
        self.cancellation.cancel();
        self.cache.take();
        self.mqtt_storage.take();
        self.stream.take();
        self.platform.take();
        self.instance_lock.take();
        self.internal_directory.take();
        if deadline_was_elapsed || Instant::now() > deadline {
            return Err(ShutdownError::DeadlineElapsed);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    async fn stop_readiness_monitor(&mut self) {
        if let Some(monitor) = self.readiness_monitor.take() {
            monitor.abort();
            let _ = monitor.await;
        }
    }
}

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("failed to prepare internal state directory")]
    InternalDirectory(#[source] io::Error),
    #[error("internal state directory instance lock is already held: {path}")]
    InstanceLocked {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to acquire internal state directory instance lock: {path}")]
    InstanceLock {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("platform storage migration failed: {0}")]
    PlatformMigration(#[source] PlatformStoreError),
    #[error("stream recovery failed")]
    StreamRecovery(#[source] iot_nano_stream::StreamError),
    #[error("MQTTD state recovery failed")]
    MqttStorage(#[source] iot_nano_mqttd::StorageError),
    #[error("MQTTD state recovery task failed: {0}")]
    MqttStorageTask(String),
    #[error("cache state recovery failed")]
    CacheRecovery(#[source] CacheError),
    #[error("MQTT runtime startup failed")]
    MqttRuntime(#[source] iot_nano_mqttd::MqttRuntimeStartError),
    #[error("Core runtime startup failed")]
    CoreRuntime(#[source] iot_nano_core::CoreRuntimeError),
    #[error("public HTTP listener bind failed")]
    PublicHttpBind(#[source] io::Error),
    #[error("management HTTP listener bind failed")]
    ManagementHttpBind(#[source] io::Error),
}

#[derive(Debug, Error)]
pub enum ShutdownError {
    #[error("runtime shutdown deadline elapsed")]
    DeadlineElapsed,
    #[error("HTTP server task failed: {0}")]
    HttpServer(String),
    #[error("MQTT runtime shutdown failed")]
    MqttRuntime(#[source] iot_nano_mqttd::MqttRuntimeError),
    #[error("Core runtime shutdown failed")]
    CoreRuntime(#[source] iot_nano_core::CoreRuntimeError),
}

fn health_router(readiness: Readiness) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(healthz))
        .with_state(readiness)
}

async fn healthz(State(readiness): State<Readiness>) -> StatusCode {
    if readiness.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn spawn_http_server(
    listener: TcpListener,
    router: Router,
    cancellation: CancellationToken,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            cancellation.cancelled().await;
        })
        .await
    })
}

async fn cleanup_started_components(
    core: &CoreRuntime,
    mqtt: &mut MqttRuntime,
    deadline: Duration,
) {
    let deadline = Instant::now() + deadline;
    let _ = mqtt.stop_accepting().await;
    let _ = core.drain(deadline).await;
    let _ = mqtt.drain(deadline).await;
}

fn spawn_readiness_monitor(
    readiness: Readiness,
    cancellation: CancellationToken,
    failure_cancellation: CancellationToken,
    core: Arc<CoreRuntime>,
    mqtt_cancellation: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    readiness.mark_not_ready();
                    return;
                }
                _ = mqtt_cancellation.cancelled() => {
                    if !cancellation.is_cancelled() {
                        request_orderly_shutdown(&readiness, &failure_cancellation);
                    } else {
                        readiness.mark_not_ready();
                    }
                    return;
                }
                _ = interval.tick() => {
                    if !core.ready() {
                        request_orderly_shutdown(&readiness, &failure_cancellation);
                        return;
                    }
                }
            }
        }
    })
}

fn request_orderly_shutdown(readiness: &Readiness, failure_cancellation: &CancellationToken) {
    readiness.mark_not_ready();
    failure_cancellation.cancel();
}

async fn join_http_tasks(
    tasks: &mut Vec<JoinHandle<io::Result<()>>>,
    deadline: Instant,
) -> Result<(), ShutdownError> {
    let mut first_error = None;
    while let Some(mut task) = tasks.pop() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                first_error.get_or_insert(ShutdownError::HttpServer(error.to_string()));
            }
            Ok(Err(error)) => {
                first_error.get_or_insert(ShutdownError::HttpServer(error.to_string()));
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                first_error.get_or_insert(ShutdownError::DeadlineElapsed);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod unit_tests {
    use std::{future::pending, io, time::Instant};

    use tokio::{
        sync::oneshot,
        task::JoinHandle,
        time::{Duration, timeout},
    };

    use crate::Readiness;
    use tokio_util::sync::CancellationToken;

    use super::{ShutdownError, join_http_tasks, request_orderly_shutdown};

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn expired_http_shutdown_aborts_and_joins_every_server_task() {
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let task: JoinHandle<io::Result<()>> = tokio::spawn(async move {
            let _signal = DropSignal(Some(dropped_tx));
            pending::<io::Result<()>>().await
        });
        let mut tasks = vec![task];

        let error = join_http_tasks(&mut tasks, Instant::now())
            .await
            .expect_err("an expired deadline must be reported");

        assert!(matches!(error, ShutdownError::DeadlineElapsed));
        timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("HTTP task was detached instead of joined")
            .expect("HTTP task drop signal was lost");
        assert!(tasks.is_empty());
    }

    #[test]
    fn failure_request_marks_not_ready_without_cancelling_component_parent() {
        let readiness = Readiness::default();
        readiness.mark_ready();
        let component_parent = CancellationToken::new();
        let failure = CancellationToken::new();

        request_orderly_shutdown(&readiness, &failure);

        assert!(!readiness.is_ready());
        assert!(failure.is_cancelled());
        assert!(!component_parent.is_cancelled());
    }
}

#[derive(Clone)]
struct UnconfiguredEmailSender;

impl EmailSender for UnconfiguredEmailSender {
    fn send(
        &self,
        _subject: String,
        _body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        Box::pin(async {
            Err(NotificationError::Configuration(
                "SMTP is not configured for iot-nano-monolith".to_owned(),
            ))
        })
    }
}

fn default_core_runtime_config(
    store: Arc<PlatformStore>,
    stream: Arc<dyn StreamPort>,
    command_transport: Arc<dyn CommandTransport>,
    cancellation: CancellationToken,
) -> CoreRuntimeConfig {
    CoreRuntimeConfig {
        store,
        stream,
        command_transport,
        email_sender: Arc::new(UnconfiguredEmailSender),
        writer_batch_size: 100,
        alert_batch_size: 100,
        command_batch_size: 100,
        notification_batch_size: 100,
        writer_group: "platform-writer".to_owned(),
        alert_group: "platform-alerts".to_owned(),
        writer_member_id: "monolith-writer".to_owned(),
        alert_member_id: "monolith-alerts".to_owned(),
        writer_interval: Duration::from_secs(1),
        event_alert_interval: Duration::from_secs(1),
        window_alert_interval: Duration::from_secs(1),
        command_interval: Duration::from_secs(1),
        notification_interval: Duration::from_secs(1),
        writer_heartbeat_interval: Duration::from_secs(1),
        alert_heartbeat_interval: Duration::from_secs(1),
        notification_send_timeout: Duration::from_secs(15),
        notification_lease_duration: chrono::Duration::seconds(30),
        notification_retry_base: chrono::Duration::seconds(1),
        notification_retry_max: chrono::Duration::seconds(60),
        cancellation,
        metrics: Arc::new(IngestMetrics::default()),
    }
}

struct InstanceLock {
    file: File,
}

impl InstanceLock {
    fn acquire(directory: &InternalDirectory) -> Result<Self, StartupError> {
        let (path, file) = Self::open(directory)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file }),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                Err(StartupError::InstanceLocked { path, source })
            }
            Err(source) => Err(StartupError::InstanceLock { path, source }),
        }
    }

    fn acquire_blocking(directory: &InternalDirectory) -> Result<Self, StartupError> {
        let (path, file) = Self::open(directory)?;
        file.lock_exclusive()
            .map_err(|source| StartupError::InstanceLock { path, source })?;
        Ok(Self { file })
    }

    fn open(directory: &InternalDirectory) -> Result<(PathBuf, File), StartupError> {
        let path = directory.path.join("instance.lock");
        let file = directory
            .open_state_file("instance.lock")
            .map_err(|source| StartupError::InstanceLock {
                path: path.clone(),
                source,
            })?;
        Ok((path, file))
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

struct InternalDirectory {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
}

impl InternalDirectory {
    fn prepare_state_file(&self, name: &str) -> io::Result<PathBuf> {
        self.open_state_file(name)?;
        self.state_path(name)
    }

    #[cfg(unix)]
    fn open_state_file(&self, name: &str) -> io::Result<File> {
        use rustix::{
            fs::{Mode, OFlags, fchmod, fstat, openat},
            io::Errno,
            process::geteuid,
        };

        let (file, created) = loop {
            match openat(
                &self.directory,
                name,
                OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(file) => break (file, false),
                Err(Errno::NOENT) => match openat(
                    &self.directory,
                    name,
                    OFlags::CREATE
                        | OFlags::EXCL
                        | OFlags::RDWR
                        | OFlags::CLOEXEC
                        | OFlags::NOFOLLOW,
                    Mode::from(0o600),
                ) {
                    Ok(file) => break (file, true),
                    Err(Errno::EXIST) => continue,
                    Err(error) => return Err(error.into()),
                },
                Err(error) => return Err(error.into()),
            }
        };
        let path = self.path.join(name);
        ensure_owned_regular_file(&file, &path, geteuid().as_raw())?;
        if created {
            fchmod(&file, Mode::from(0o600)).map_err(io::Error::from)?;
        } else if fstat(&file).map_err(io::Error::from)?.st_mode as u32 & 0o777 != 0o600 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "internal state file must retain owner-only permissions: {}",
                    path.display()
                ),
            ));
        }
        Ok(File::from(file))
    }

    #[cfg(not(unix))]
    fn open_state_file(&self, name: &str) -> io::Result<File> {
        let path = self.path.join(name);
        let metadata = std::fs::symlink_metadata(&path).ok();
        if metadata.is_some_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "symbolic links are not allowed for internal state: {}",
                    path.display()
                ),
            ));
        }
        std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
    }

    #[cfg(unix)]
    fn state_path(&self, name: &str) -> io::Result<PathBuf> {
        anchored_state_path(&self.directory, name)
    }

    #[cfg(not(unix))]
    fn state_path(&self, name: &str) -> io::Result<PathBuf> {
        Ok(self.path.join(name))
    }
}

fn prepare_internal_directory(path: &Path) -> Result<InternalDirectory, StartupError> {
    #[cfg(unix)]
    {
        return prepare_internal_directory_unix(path).map_err(StartupError::InternalDirectory);
    }

    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(path).map_err(StartupError::InternalDirectory)?;
        let metadata = std::fs::symlink_metadata(path).map_err(StartupError::InternalDirectory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StartupError::InternalDirectory(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("internal state path is not a directory: {}", path.display()),
            )));
        }
        retire_legacy_marker_and_validate_internal_directory(path)
            .map_err(StartupError::InternalDirectory)?;
        return Ok(InternalDirectory {
            path: path.to_path_buf(),
        });
    }
}

#[cfg(unix)]
fn prepare_internal_directory_unix(path: &Path) -> io::Result<InternalDirectory> {
    use rustix::{
        fs::{Mode, OFlags, fchmod, mkdirat, openat},
        io::Errno,
        process::geteuid,
    };

    if !path.is_absolute() {
        return Err(invalid_internal_state_path(path, "path must be absolute"));
    }

    let components = path
        .components()
        .filter_map(|component| match component {
            Component::RootDir => None,
            Component::Normal(name) => Some(Ok(name)),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => Some(Err(())),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|()| {
            invalid_internal_state_path(path, "path components must not contain `.` or `..`")
        })?;
    if components.is_empty() {
        return Err(invalid_internal_state_path(
            path,
            "filesystem root cannot be the internal state directory",
        ));
    }
    let component_count = components.len();

    let current_uid = geteuid().as_raw();
    let mut directory = File::from(
        openat(
            rustix::fs::CWD,
            Path::new("/"),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let mut current_path = PathBuf::from("/");
    let mut created_internal_directory = false;
    for (index, name) in components.into_iter().enumerate() {
        ensure_parent_directory_is_not_replaceable(&directory, &current_path, current_uid)?;
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
        let child = match openat(&directory, name, flags, Mode::empty()) {
            Ok(child) => child,
            Err(Errno::NOENT) => {
                if index + 1 != component_count {
                    return Err(invalid_internal_state_path(
                        path,
                        "internal state parent directory does not exist",
                    ));
                }
                match mkdirat(&directory, name, Mode::from(0o700)) {
                    Ok(()) => created_internal_directory = true,
                    Err(Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                openat(&directory, name, flags, Mode::empty()).map_err(io::Error::from)?
            }
            Err(error) => return Err(error.into()),
        };
        directory = File::from(child);
        current_path.push(name);
    }

    if created_internal_directory {
        ensure_new_internal_directory(&directory, path, current_uid)?;
        fchmod(&directory, Mode::from(0o700)).map_err(io::Error::from)?;
    } else {
        ensure_reusable_internal_directory(&directory, path, current_uid)?;
    }
    retire_legacy_marker_and_validate_internal_directory(&directory, path, current_uid)?;

    Ok(InternalDirectory {
        path: path.to_path_buf(),
        directory,
    })
}

#[cfg(unix)]
fn ensure_parent_directory_is_not_replaceable(
    directory: &File,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    use rustix::fs::fstat;

    let metadata = fstat(directory).map_err(io::Error::from)?;
    let mode = metadata.st_mode as u32;
    let writable_by_group_or_other = mode & 0o022 != 0;
    let sticky = mode & 0o1000 != 0;
    let trusted_owner = metadata.st_uid == current_uid || metadata.st_uid == 0;
    if !trusted_owner {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state parent is not owned by the current user or root: {}",
                path.display()
            ),
        ));
    }
    if writable_by_group_or_other && !sticky {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state parent can be replaced by another user: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_new_internal_directory(
    directory: &File,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    let metadata = rustix::fs::fstat(directory).map_err(io::Error::from)?;
    if metadata.st_uid != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "new internal state directory is not owned by the current user: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_reusable_internal_directory(
    directory: &File,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    use rustix::fs::fstat;

    let metadata = fstat(directory).map_err(io::Error::from)?;
    if metadata.st_uid != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state directory is not owned by the current user: {}",
                path.display()
            ),
        ));
    }
    if metadata.st_mode as u32 & 0o777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state directory must retain owner-only permissions: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn retire_legacy_marker_and_validate_internal_directory(
    directory: &File,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    let (has_legacy_marker, entries) = declared_internal_state_entries(path)?;
    for name in entries {
        validate_existing_internal_state_entry(directory, path, &name, current_uid)?;
    }

    if has_legacy_marker {
        verify_internal_directory_marker(directory, path, current_uid)?;
        rustix::fs::unlinkat(
            directory,
            INTERNAL_DIRECTORY_MARKER,
            rustix::fs::AtFlags::empty(),
        )
        .map_err(io::Error::from)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn retire_legacy_marker_and_validate_internal_directory(path: &Path) -> io::Result<()> {
    let (has_legacy_marker, _) = declared_internal_state_entries(path)?;
    if has_legacy_marker {
        let marker_path = path.join(INTERNAL_DIRECTORY_MARKER);
        let contents = std::fs::read(&marker_path)?;
        if contents != INTERNAL_DIRECTORY_MARKER_CONTENT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "internal state marker is invalid: {}",
                    marker_path.display()
                ),
            ));
        }
        std::fs::remove_file(marker_path)?;
    }
    Ok(())
}

fn declared_internal_state_entries(path: &Path) -> io::Result<(bool, Vec<String>)> {
    let mut has_legacy_marker = false;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "internal state entry is not valid UTF-8: {}",
                    entry.path().display()
                ),
            )
        })?;
        if !file_type.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "internal state entry is not a regular file: {}",
                    entry.path().display()
                ),
            ));
        }
        if name == INTERNAL_DIRECTORY_MARKER {
            has_legacy_marker = true;
        } else if is_declared_internal_state_entry(name) {
            entries.push(name.to_owned());
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "internal state directory contains an undeclared entry: {}",
                    entry.path().display()
                ),
            ));
        }
    }
    Ok((has_legacy_marker, entries))
}

fn is_declared_internal_state_entry(name: &str) -> bool {
    INTERNAL_STATE_FILES.contains(&name)
        || INTERNAL_SQLITE_STATE_FILES
            .iter()
            .any(|file| name == format!("{file}-wal") || name == format!("{file}-shm"))
}

#[cfg(unix)]
fn validate_existing_internal_state_entry(
    directory: &File,
    path: &Path,
    name: &str,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags, fstat, openat};

    let entry_path = path.join(name);
    let entry = openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    ensure_owned_regular_file(&entry, &entry_path, current_uid)?;
    if fstat(&entry).map_err(io::Error::from)?.st_mode as u32 & 0o777 != 0o600 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state file must retain owner-only permissions: {}",
                entry_path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_internal_directory_marker(
    directory: &File,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags, fstat, openat};

    let marker_path = path.join(INTERNAL_DIRECTORY_MARKER);
    let marker = openat(
        directory,
        INTERNAL_DIRECTORY_MARKER,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    ensure_owned_regular_file(&marker, &marker_path, current_uid)?;
    if fstat(&marker).map_err(io::Error::from)?.st_mode as u32 & 0o777 != 0o600 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state marker must retain owner-only permissions: {}",
                marker_path.display()
            ),
        ));
    }
    let mut marker = File::from(marker);
    let mut contents = Vec::new();
    marker.read_to_end(&mut contents)?;
    if contents != INTERNAL_DIRECTORY_MARKER_CONTENT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "internal state marker is invalid: {}",
                marker_path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_owned_regular_file(
    file: &impl std::os::fd::AsFd,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    use rustix::fs::{FileType, fstat};

    let metadata = fstat(file).map_err(io::Error::from)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "internal state path is not a regular file: {}",
                path.display()
            ),
        ));
    }
    if metadata.st_uid != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state file is not owned by the current user: {}",
                path.display()
            ),
        ));
    }
    if metadata.st_nlink != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "internal state file must not have additional hard links: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(all(unix, any(target_os = "linux", target_os = "android")))]
fn anchored_state_path(directory: &File, name: &str) -> io::Result<PathBuf> {
    use std::os::fd::AsRawFd;

    Ok(PathBuf::from(format!(
        "/proc/self/fd/{}/{name}",
        directory.as_raw_fd()
    )))
}

#[cfg(all(unix, any(target_os = "macos", target_os = "ios")))]
fn anchored_state_path(directory: &File, name: &str) -> io::Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    let path = rustix::fs::getpath(directory)
        .map_err(io::Error::from)?
        .into_bytes();
    Ok(PathBuf::from(std::ffi::OsString::from_vec(path)).join(name))
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn anchored_state_path(_directory: &File, _name: &str) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-anchored internal state paths are unsupported on this platform",
    ))
}

#[cfg(unix)]
fn invalid_internal_state_path(path: &Path, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsafe internal state path {}: {reason}", path.display()),
    )
}

fn recover_mqtt_storage(path: PathBuf) -> Result<SqliteStorage, iot_nano_mqttd::StorageError> {
    let storage = SqliteStorage::open(path)?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64);
    storage.load(now_ms)?;
    storage.prune(now_ms, RetentionPolicy::default())?;
    Ok(storage)
}

#[cfg(all(test, unix))]
mod tests {
    use crate::{CacheError, PersistentCache};

    use super::prepare_internal_directory_unix;

    #[test]
    fn state_paths_stay_anchored_to_the_open_directory_after_parent_replacement() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let parent = root.join("parent");
        let internal = parent.join("internal");
        std::fs::create_dir(&parent).unwrap();
        let directory = prepare_internal_directory_unix(&internal).unwrap();

        let relocated_parent = root.join("relocated-parent");
        std::fs::rename(&parent, &relocated_parent).unwrap();
        std::fs::create_dir(&parent).unwrap();
        std::fs::create_dir(parent.join("internal")).unwrap();

        let state_path = directory.prepare_state_file("stream.sqlite").unwrap();
        std::fs::write(&state_path, b"anchored").unwrap();

        assert_eq!(
            std::fs::read(relocated_parent.join("internal/stream.sqlite")).unwrap(),
            b"anchored"
        );
        assert!(!parent.join("internal/stream.sqlite").exists());
    }

    #[tokio::test]
    async fn cache_open_rejects_a_symlink_replacing_the_validated_state_file() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let parent = root.join("parent");
        let internal = parent.join("internal");
        std::fs::create_dir(&parent).unwrap();
        let directory = prepare_internal_directory_unix(&internal).unwrap();
        let cache_file = directory.open_state_file("cache.sqlite").unwrap();
        let cache_path = directory.state_path("cache.sqlite").unwrap();

        let original = internal.join("original-cache.sqlite");
        std::fs::rename(internal.join("cache.sqlite"), &original).unwrap();
        std::os::unix::fs::symlink(&original, internal.join("cache.sqlite")).unwrap();

        assert!(matches!(
            PersistentCache::open_file(cache_file, cache_path).await,
            Err(CacheError::Sqlite(_))
        ));
    }
}
