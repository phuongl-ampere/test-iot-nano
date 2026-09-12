use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use fs2::FileExt;
use iot_nano_mqttd::SqliteStorage;
use iot_nano_stream::{LocalStream, StreamConfig};
use iot_storage::{PlatformStore, PlatformStoreError};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{MonolithConfig, Readiness};

pub struct MonolithRuntime {
    instance_lock: Option<InstanceLock>,
    platform: Option<PlatformStore>,
    stream: Option<Arc<LocalStream>>,
    mqtt_storage: Option<Arc<SqliteStorage>>,
    readiness: Readiness,
    cancellation: CancellationToken,
}

impl MonolithRuntime {
    pub async fn start(config: MonolithConfig) -> Result<Self, StartupError> {
        let internal_dir = prepare_internal_directory(&config.internal_dir)?;
        let instance_lock = InstanceLock::acquire(internal_dir.join("instance.lock"))?;
        let platform = PlatformStore::open(&config.storage)
            .await
            .map_err(StartupError::PlatformMigration)?;
        let stream = Arc::new(
            LocalStream::open(StreamConfig::sqlite(internal_dir.join("stream.sqlite")))
                .await
                .map_err(StartupError::StreamRecovery)?,
        );
        let mqtt_path = internal_dir.join("mqttd.sqlite");
        let mqtt_storage = Arc::new(
            tokio::task::spawn_blocking(move || SqliteStorage::open(mqtt_path))
                .await
                .map_err(|error| StartupError::MqttStorageTask(error.to_string()))?
                .map_err(StartupError::MqttStorage)?,
        );

        let readiness = Readiness::default();
        readiness.mark_ready();

        Ok(Self {
            instance_lock: Some(instance_lock),
            platform: Some(platform),
            stream: Some(stream),
            mqtt_storage: Some(mqtt_storage),
            readiness,
            cancellation: CancellationToken::new(),
        })
    }

    pub fn readiness(&self) -> Readiness {
        self.readiness.clone()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn platform(&self) -> Option<&PlatformStore> {
        self.platform.as_ref()
    }

    pub fn stream(&self) -> Option<&Arc<LocalStream>> {
        self.stream.as_ref()
    }

    pub fn mqtt_storage(&self) -> Option<&Arc<SqliteStorage>> {
        self.mqtt_storage.as_ref()
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), ShutdownError> {
        self.readiness.mark_not_ready();
        self.cancellation.cancel();
        self.mqtt_storage.take();
        self.stream.take();
        self.platform.take();
        self.instance_lock.take();
        if Instant::now() > deadline {
            return Err(ShutdownError::DeadlineElapsed);
        }
        Ok(())
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
    #[error("platform storage migration failed")]
    PlatformMigration(#[source] PlatformStoreError),
    #[error("stream recovery failed")]
    StreamRecovery(#[source] iot_nano_stream::StreamError),
    #[error("MQTTD state recovery failed")]
    MqttStorage(#[source] iot_nano_mqttd::StorageError),
    #[error("MQTTD state recovery task failed: {0}")]
    MqttStorageTask(String),
}

#[derive(Debug, Error)]
pub enum ShutdownError {
    #[error("runtime shutdown deadline elapsed")]
    DeadlineElapsed,
}

struct InstanceLock {
    file: File,
}

impl InstanceLock {
    fn acquire(path: PathBuf) -> Result<Self, StartupError> {
        reject_symlink(&path).map_err(StartupError::InternalDirectory)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| StartupError::InstanceLock {
                path: path.clone(),
                source,
            })?;

        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            StartupError::InstanceLock {
                path: path.clone(),
                source,
            }
        })?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file }),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                Err(StartupError::InstanceLocked { path, source })
            }
            Err(source) => Err(StartupError::InstanceLock { path, source }),
        }
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn prepare_internal_directory(path: &Path) -> Result<PathBuf, StartupError> {
    fs::create_dir_all(path).map_err(StartupError::InternalDirectory)?;
    reject_symlink(path).map_err(StartupError::InternalDirectory)?;
    let metadata = fs::metadata(path).map_err(StartupError::InternalDirectory)?;
    if !metadata.is_dir() {
        return Err(StartupError::InternalDirectory(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("internal state path is not a directory: {}", path.display()),
        )));
    }

    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(StartupError::InternalDirectory)?;

    Ok(path.to_path_buf())
}

fn reject_symlink(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "symbolic links are not allowed for internal state: {}",
                        path.display()
                    ),
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
