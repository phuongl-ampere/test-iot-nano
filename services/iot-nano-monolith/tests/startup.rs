use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{
    MonolithConfig, MonolithRuntime, PersistentCache, ShutdownError, StartupError,
};
use iot_nano_mqttd::SqliteStorage;
use iot_nano_stream::{LocalStream, StreamConfig};
use tempfile::TempDir;
use tokio::net::TcpListener;

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
}

impl Fixture {
    async fn sqlite() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mqtt_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");
        let tls_cert_path = mqtt_fixtures.join("server.crt");
        let tls_key_path = mqtt_fixtures.join("server.key");

        Self {
            config: MonolithConfig {
                storage: StorageConfiguration {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(root.join("platform.sqlite")),
                    sqlite_busy_timeout_ms: 5_000,
                },
                internal_dir: root.join("internal"),
                public_http: reserve_address().await,
                management_http: reserve_address().await,
                mqtt_tcp: reserve_address().await,
                mqtt_tls: reserve_address().await,
                tls_cert_path,
                tls_key_path,
                shutdown_deadline: Duration::from_secs(1),
            },
            _directory: directory,
        }
    }

    fn config(&self) -> MonolithConfig {
        self.config.clone()
    }

    fn internal_path(&self, name: &str) -> PathBuf {
        self.config.internal_dir.join(name)
    }

    async fn assert_configured_addresses_are_unbound(&self) {
        for address in [
            self.config.public_http,
            self.config.management_http,
            self.config.mqtt_tcp,
            self.config.mqtt_tls,
        ] {
            let listener = TcpListener::bind(address).await.unwrap_or_else(|error| {
                panic!("{address} was bound before startup completed: {error}")
            });
            drop(listener);
        }
    }
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn write_corrupt_sqlite(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, "not a SQLite database").unwrap();
}

fn write_semantically_corrupt_mqttd_sqlite(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(
            "
            CREATE TABLE retained (
                topic TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                stored_at_ms INTEGER NOT NULL
            );
            ",
        )
        .unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    connection
        .execute(
            "INSERT INTO retained(topic, value, stored_at_ms) VALUES (?1, ?2, ?3)",
            rusqlite::params!["devices/meter-a/state", b"not JSON".as_slice(), now_ms],
        )
        .unwrap();
    set_owner_only_mode(path);
}

fn write_semantically_corrupt_cache_sqlite(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(
            "
            CREATE TABLE cache_entries (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL
            );
            ",
        )
        .unwrap();
    set_owner_only_mode(path);
}

#[cfg(unix)]
fn set_owner_only_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(not(unix))]
fn set_owner_only_mode(_path: &Path) {}

async fn prepare_internal_state(fixture: &Fixture) {
    let mut runtime = MonolithRuntime::start(fixture.config()).await.unwrap();
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn second_runtime_cannot_acquire_the_same_internal_instance_lock() {
    let fixture = Fixture::sqlite().await;
    let mut first = MonolithRuntime::start(fixture.config()).await.unwrap();

    assert!(first.readiness().is_ready());
    assert!(fixture.internal_path("instance.lock").is_file());
    assert!(fixture.internal_path("stream.sqlite").is_file());
    assert!(fixture.internal_path("mqttd.sqlite").is_file());
    assert!(fixture.internal_path("cache.sqlite").is_file());
    assert!(first.cache().is_some());

    let mut locked_config = fixture.config();
    locked_config.storage.sqlite_path = None;
    let error = match MonolithRuntime::start(locked_config).await {
        Ok(_) => panic!("the second runtime unexpectedly acquired the instance lock"),
        Err(error) => error,
    };
    assert!(matches!(error, StartupError::InstanceLocked { .. }));

    first
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(!first.readiness().is_ready());
    assert!(first.cancellation_token().is_cancelled());

    let mut second = MonolithRuntime::start(fixture.config()).await.unwrap();
    second
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_platform_configuration_binds_no_configured_listener() {
    let fixture = Fixture::sqlite().await;
    let mut config = fixture.config();
    config.storage.sqlite_path = None;

    let error = match MonolithRuntime::start(config).await {
        Ok(_) => panic!("invalid platform configuration unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::PlatformMigration(_)));
    assert!(!fixture.internal_path("stream.sqlite").exists());
    assert!(!fixture.internal_path("mqttd.sqlite").exists());
    fixture.assert_configured_addresses_are_unbound().await;
}

#[tokio::test]
async fn corrupt_stream_state_binds_no_configured_listener() {
    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    let mqtt_path = fixture.internal_path("mqttd.sqlite");
    let mqtt_before = std::fs::read(&mqtt_path).unwrap();
    write_corrupt_sqlite(&fixture.internal_path("stream.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("corrupt stream state unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::StreamRecovery(_)));
    assert_eq!(std::fs::read(mqtt_path).unwrap(), mqtt_before);
    fixture.assert_configured_addresses_are_unbound().await;

    std::fs::remove_file(fixture.internal_path("stream.sqlite")).unwrap();
    let mut restarted = MonolithRuntime::start(fixture.config()).await.unwrap();
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn corrupt_mqttd_state_binds_no_configured_listener() {
    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    write_corrupt_sqlite(&fixture.internal_path("mqttd.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("corrupt MQTTD state unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::MqttStorage(_)));
    fixture.assert_configured_addresses_are_unbound().await;

    std::fs::remove_file(fixture.internal_path("mqttd.sqlite")).unwrap();
    let mut restarted = MonolithRuntime::start(fixture.config()).await.unwrap();
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn semantic_mqttd_recovery_failure_prevents_readiness() {
    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    std::fs::remove_file(fixture.internal_path("mqttd.sqlite")).unwrap();
    write_semantically_corrupt_mqttd_sqlite(&fixture.internal_path("mqttd.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("semantically corrupt MQTTD state unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::MqttStorage(_)));
    fixture.assert_configured_addresses_are_unbound().await;

    std::fs::remove_file(fixture.internal_path("mqttd.sqlite")).unwrap();
    let mut restarted = MonolithRuntime::start(fixture.config()).await.unwrap();
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn expired_shutdown_releases_local_state_before_reporting_deadline() {
    let fixture = Fixture::sqlite().await;
    let mut runtime = MonolithRuntime::start(fixture.config()).await.unwrap();
    let cancellation = runtime.cancellation_token();

    let error = runtime
        .shutdown(Instant::now() - Duration::from_millis(1))
        .await
        .unwrap_err();

    assert!(
        matches!(error, ShutdownError::DeadlineElapsed),
        "unexpected expired shutdown error: {error:?}"
    );
    assert!(!runtime.readiness().is_ready());
    assert!(cancellation.is_cancelled());

    let stream = LocalStream::open(StreamConfig::sqlite(fixture.internal_path("stream.sqlite")))
        .await
        .unwrap();
    drop(stream);
    let mqtt_storage = SqliteStorage::open(fixture.internal_path("mqttd.sqlite")).unwrap();
    drop(mqtt_storage);
    let cache = PersistentCache::open(fixture.internal_path("cache.sqlite"))
        .await
        .unwrap();
    drop(cache);

    let mut restarted = MonolithRuntime::start(fixture.config()).await.unwrap();
    assert!(restarted.cache().is_some());
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn corrupt_cache_state_binds_no_configured_listener() {
    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    write_corrupt_sqlite(&fixture.internal_path("cache.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("corrupt cache state unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::CacheRecovery(_)));
    fixture.assert_configured_addresses_are_unbound().await;

    std::fs::remove_file(fixture.internal_path("cache.sqlite")).unwrap();
    let mut restarted = MonolithRuntime::start(fixture.config()).await.unwrap();
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn semantic_cache_recovery_failure_prevents_readiness() {
    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    std::fs::remove_file(fixture.internal_path("cache.sqlite")).unwrap();
    write_semantically_corrupt_cache_sqlite(&fixture.internal_path("cache.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("semantically corrupt cache unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::CacheRecovery(_)));
    fixture.assert_configured_addresses_are_unbound().await;

    std::fs::remove_file(fixture.internal_path("cache.sqlite")).unwrap();
    let mut restarted = MonolithRuntime::start(fixture.config()).await.unwrap();
    restarted
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_existing_cache_mode_prevents_runtime_startup() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    let cache_path = fixture.internal_path("cache.sqlite");
    std::fs::set_permissions(&cache_path, std::fs::Permissions::from_mode(0o644)).unwrap();

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("unsafe cache mode unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InternalDirectory(_)));
    assert_eq!(
        cache_path.metadata().unwrap().permissions().mode() & 0o777,
        0o644
    );
    fixture.assert_configured_addresses_are_unbound().await;
}

#[cfg(unix)]
#[tokio::test]
async fn parent_symlink_for_internal_state_is_rejected_before_it_is_followed() {
    let fixture = Fixture::sqlite().await;
    let root = fixture.config.internal_dir.parent().unwrap();
    let target_parent = root.join("real-parent");
    let symlink_parent = root.join("symlink-parent");
    std::fs::create_dir(&target_parent).unwrap();
    std::os::unix::fs::symlink(&target_parent, &symlink_parent).unwrap();

    let mut config = fixture.config();
    config.internal_dir = symlink_parent.join("internal");
    let error = match MonolithRuntime::start(config).await {
        Ok(_) => panic!("parent symlink unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InternalDirectory(_)));
    assert!(!target_parent.join("internal").exists());
    fixture.assert_configured_addresses_are_unbound().await;
}

#[cfg(unix)]
#[tokio::test]
async fn final_internal_state_symlink_is_rejected_without_changing_its_target() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::sqlite().await;
    let target = fixture._directory.path().join("real-internal");
    std::fs::create_dir(&target).unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink(&target, &fixture.config.internal_dir).unwrap();

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("final internal directory symlink unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InternalDirectory(_)));
    assert_eq!(
        target.metadata().unwrap().permissions().mode() & 0o777,
        0o755
    );
    fixture.assert_configured_addresses_are_unbound().await;
}

#[cfg(unix)]
#[tokio::test]
async fn writable_nonsticky_parent_for_internal_state_is_rejected() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::sqlite().await;
    let unsafe_parent = fixture
        .config
        .internal_dir
        .parent()
        .unwrap()
        .join("unsafe-parent");
    std::fs::create_dir(&unsafe_parent).unwrap();
    std::fs::set_permissions(&unsafe_parent, std::fs::Permissions::from_mode(0o777)).unwrap();

    let mut config = fixture.config();
    config.internal_dir = unsafe_parent.join("internal");
    let error = match MonolithRuntime::start(config).await {
        Ok(_) => panic!("writable parent unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InternalDirectory(_)));
    assert!(!unsafe_parent.join("internal").exists());
    fixture.assert_configured_addresses_are_unbound().await;
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_instance_lock_is_rejected_without_following_the_target() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::sqlite().await;
    prepare_internal_state(&fixture).await;
    std::fs::remove_file(fixture.internal_path("instance.lock")).unwrap();
    std::fs::remove_file(fixture.config.storage.sqlite_path.as_ref().unwrap()).unwrap();
    let lock_target = fixture._directory.path().join("lock-target");
    std::fs::write(&lock_target, "do not touch").unwrap();
    std::fs::set_permissions(&lock_target, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::os::unix::fs::symlink(&lock_target, fixture.internal_path("instance.lock")).unwrap();

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("symlinked instance lock unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InstanceLock { .. }));
    assert_eq!(
        std::fs::read_to_string(&lock_target).unwrap(),
        "do not touch"
    );
    assert_eq!(
        lock_target.metadata().unwrap().permissions().mode() & 0o777,
        0o644
    );
    assert!(
        !fixture
            .config
            .storage
            .sqlite_path
            .as_ref()
            .unwrap()
            .exists()
    );
    fixture.assert_configured_addresses_are_unbound().await;
}

#[cfg(unix)]
#[tokio::test]
async fn existing_internal_directory_without_monolith_marker_is_rejected_without_mutation() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::sqlite().await;
    let existing = fixture
        .config
        .internal_dir
        .parent()
        .unwrap()
        .join("existing-directory");
    std::fs::create_dir(&existing).unwrap();
    std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o700)).unwrap();

    let mut config = fixture.config();
    config.internal_dir = existing.clone();
    let error = match MonolithRuntime::start(config).await {
        Ok(_) => panic!("arbitrary existing directory unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InternalDirectory(_)));
    assert_eq!(
        existing.metadata().unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(!existing.join(".iot-nano-monolith-state").exists());
    fixture.assert_configured_addresses_are_unbound().await;
}

#[cfg(unix)]
#[tokio::test]
async fn dedicated_internal_directory_creates_and_reuses_its_monolith_marker() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::sqlite().await;
    let trusted_parent = fixture
        .config
        .internal_dir
        .parent()
        .unwrap()
        .join("trusted-parent");
    std::fs::create_dir(&trusted_parent).unwrap();
    std::fs::set_permissions(&trusted_parent, std::fs::Permissions::from_mode(0o700)).unwrap();

    let mut config = fixture.config();
    config.internal_dir = trusted_parent.join("internal");
    let marker = config.internal_dir.join(".iot-nano-monolith-state");

    let mut first = MonolithRuntime::start(config.clone()).await.unwrap();
    assert!(marker.is_file());
    assert_eq!(
        marker.metadata().unwrap().permissions().mode() & 0o777,
        0o600
    );
    first
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    let mut second = MonolithRuntime::start(config).await.unwrap();
    second
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn filesystem_root_cannot_be_the_internal_state_directory() {
    let fixture = Fixture::sqlite().await;
    let mut config = fixture.config();
    config.internal_dir = PathBuf::from("/");

    let error = match MonolithRuntime::start(config).await {
        Ok(_) => panic!("filesystem root unexpectedly started a runtime"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::InternalDirectory(_)));
    fixture.assert_configured_addresses_are_unbound().await;
}
