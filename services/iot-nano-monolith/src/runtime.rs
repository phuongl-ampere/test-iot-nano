use std::{
    fs::File,
    io,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use fs2::FileExt;
use iot_nano_mqttd::{BrokerStorage, RetentionPolicy, SqliteStorage};
use iot_nano_stream::{LocalStream, StreamConfig};
use iot_storage::{PlatformStore, PlatformStoreError};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{MonolithConfig, Readiness};

pub struct MonolithRuntime {
    internal_directory: Option<InternalDirectory>,
    instance_lock: Option<InstanceLock>,
    platform: Option<PlatformStore>,
    stream: Option<Arc<LocalStream>>,
    mqtt_storage: Option<Arc<SqliteStorage>>,
    readiness: Readiness,
    cancellation: CancellationToken,
}

impl MonolithRuntime {
    pub async fn start(config: MonolithConfig) -> Result<Self, StartupError> {
        let internal_directory = prepare_internal_directory(&config.internal_dir)?;
        let instance_lock = InstanceLock::acquire(&internal_directory)?;
        let platform = PlatformStore::open(&config.storage)
            .await
            .map_err(StartupError::PlatformMigration)?;
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

        let readiness = Readiness::default();
        readiness.mark_ready();

        Ok(Self {
            internal_directory: Some(internal_directory),
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
        self.internal_directory.take();
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
    fn acquire(directory: &InternalDirectory) -> Result<Self, StartupError> {
        let path = directory.path.join("instance.lock");
        let file = directory
            .open_state_file("instance.lock")
            .map_err(|source| StartupError::InstanceLock {
                path: path.clone(),
                source,
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

struct InternalDirectory {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
}

impl InternalDirectory {
    fn prepare_state_file(&self, name: &str) -> io::Result<PathBuf> {
        self.open_state_file(name)?;
        Ok(self.state_path(name))
    }

    #[cfg(unix)]
    fn open_state_file(&self, name: &str) -> io::Result<File> {
        use rustix::{
            fs::{Mode, OFlags, fchmod, openat},
            process::geteuid,
        };

        let file = openat(
            &self.directory,
            name,
            OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::from(0o600),
        )
        .map_err(io::Error::from)?;
        ensure_owned_regular_file(&file, &self.path.join(name), geteuid().as_raw())?;
        fchmod(&file, Mode::from(0o600)).map_err(io::Error::from)?;
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
    fn state_path(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    #[cfg(not(unix))]
    fn state_path(&self, name: &str) -> PathBuf {
        self.path.join(name)
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
        return Ok(InternalDirectory {
            path: path.to_path_buf(),
        });
    }
}

#[cfg(unix)]
fn prepare_internal_directory_unix(path: &Path) -> io::Result<InternalDirectory> {
    use rustix::{
        fs::{Mode, OFlags, fchmod, fstat, mkdirat, openat},
        io::Errno,
        process::geteuid,
    };

    if !path.is_absolute() {
        return Err(invalid_internal_state_path(path, "path must be absolute"));
    }

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
    for component in path.components() {
        let Component::Normal(name) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(invalid_internal_state_path(
                path,
                "path components must not contain `.` or `..`",
            ));
        };

        ensure_parent_directory_is_not_replaceable(&directory, &current_path, current_uid)?;
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
        let child = match openat(&directory, name, flags, Mode::empty()) {
            Ok(child) => child,
            Err(Errno::NOENT) => {
                match mkdirat(&directory, name, Mode::from(0o700)) {
                    Ok(()) | Err(Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                openat(&directory, name, flags, Mode::empty()).map_err(io::Error::from)?
            }
            Err(error) => return Err(error.into()),
        };
        directory = File::from(child);
        current_path.push(name);
    }

    let metadata = fstat(&directory).map_err(io::Error::from)?;
    if metadata.st_uid != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "internal state directory is not owned by the current user: {}",
                path.display()
            ),
        ));
    }
    fchmod(&directory, Mode::from(0o700)).map_err(io::Error::from)?;

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
    if writable_by_group_or_other && (!sticky || !trusted_owner) {
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
