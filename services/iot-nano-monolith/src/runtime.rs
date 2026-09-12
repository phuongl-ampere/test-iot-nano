use std::{
    fs::File,
    io::{self, Read, Write},
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

const INTERNAL_DIRECTORY_MARKER: &str = ".iot-nano-monolith-state";
const INTERNAL_DIRECTORY_MARKER_CONTENT: &[u8] = b"iot-nano-monolith-state-v1\n";

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
        self.state_path(name)
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
        create_internal_directory_marker(&directory, path, current_uid)?;
    } else {
        ensure_reusable_internal_directory(&directory, path, current_uid)?;
        verify_internal_directory_marker(&directory, path, current_uid)?;
    }

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
fn create_internal_directory_marker(
    directory: &File,
    path: &Path,
    current_uid: rustix::process::RawUid,
) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags, fchmod, openat};

    let marker_path = path.join(INTERNAL_DIRECTORY_MARKER);
    let marker = openat(
        directory,
        INTERNAL_DIRECTORY_MARKER,
        OFlags::CREATE | OFlags::EXCL | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from(0o600),
    )
    .map_err(io::Error::from)?;
    ensure_owned_regular_file(&marker, &marker_path, current_uid)?;
    fchmod(&marker, Mode::from(0o600)).map_err(io::Error::from)?;
    let mut marker = File::from(marker);
    marker.write_all(INTERNAL_DIRECTORY_MARKER_CONTENT)?;
    marker.sync_all()
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
}
