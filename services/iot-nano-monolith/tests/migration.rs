use std::{
    fs::File,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use fs2::FileExt;
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{MonolithConfig, MonolithRuntime};

struct Fixture {
    _directory: tempfile::TempDir,
    config: MonolithConfig,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let mqtt_fixtures =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../iot-nano-mqttd/tests/fixtures");

        Self {
            config: MonolithConfig {
                storage: StorageConfiguration {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(root.join("platform.sqlite")),
                    sqlite_busy_timeout_ms: 5_000,
                },
                device_token_vault_key: "test-device-token-vault-key-material-0001".to_owned(),
                internal_dir: root.join("internal"),
                public_http: reserve_address(),
                management_http: reserve_address(),
                mqtt_tcp: reserve_address(),
                mqtt_tls: reserve_address(),
                tls_cert_path: mqtt_fixtures.join("server.crt"),
                tls_key_path: mqtt_fixtures.join("server.key"),
                shutdown_deadline: Duration::from_secs(1),
            },
            _directory: directory,
        }
    }

    fn migration_command(&self) -> Command {
        let storage_path = self
            .config
            .storage
            .sqlite_path
            .as_ref()
            .unwrap()
            .display()
            .to_string();
        let internal_dir = self.config.internal_dir.display().to_string();

        let mut command = Command::new(env!("CARGO_BIN_EXE_iot-nano-monolith"));
        command
            .arg("--migrate-only")
            .env_clear()
            .env("IOT_NANO_STORAGE", "sqlite")
            .env("IOT_NANO_SQLITE_PATH", storage_path)
            .env("IOT_NANO_INTERNAL_DIR", internal_dir)
            .env(
                "IOT_DEVICE_TOKEN_VAULT_KEY",
                "test-device-token-vault-key-material-0001",
            )
            .env(
                "IOT_NANO_TLS_CERT_PATH",
                self.config.tls_cert_path.as_os_str(),
            )
            .env(
                "IOT_NANO_TLS_KEY_PATH",
                self.config.tls_key_path.as_os_str(),
            )
            .env(
                "IOT_NANO_PUBLIC_HTTP_ADDRESS",
                self.config.public_http.to_string(),
            )
            .env(
                "IOT_NANO_MANAGEMENT_ADDRESS",
                self.config.management_http.to_string(),
            )
            .env(
                "IOT_NANO_MQTT_TCP_ADDRESS",
                self.config.mqtt_tcp.to_string(),
            )
            .env(
                "IOT_NANO_MQTT_TLS_ADDRESS",
                self.config.mqtt_tls.to_string(),
            );
        command
    }

    fn assert_configured_addresses_are_unbound(&self) {
        for address in [
            self.config.public_http,
            self.config.management_http,
            self.config.mqtt_tcp,
            self.config.mqtt_tls,
        ] {
            let listener = TcpListener::bind(address).unwrap_or_else(|error| {
                panic!("{address} was bound while migrate-only was running: {error}")
            });
            drop(listener);
        }
    }

    fn remove_platform_state(&self) {
        let path = self.config.storage.sqlite_path.as_ref().unwrap();
        for suffix in ["", "-shm", "-wal"] {
            let path = PathBuf::from(format!("{}{}", path.display(), suffix));
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

#[tokio::test]
async fn migrate_only_waits_for_the_internal_instance_lock_before_migrating_or_binding() {
    let fixture = Fixture::new();
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    fixture.remove_platform_state();

    let lock_path = fixture.config.internal_dir.join("instance.lock");
    let lock = File::options()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    lock.try_lock_exclusive().unwrap();

    let mut child = fixture.migration_command().spawn().unwrap();
    wait_until_blocked(&mut child);

    assert!(
        !fixture
            .config
            .storage
            .sqlite_path
            .as_ref()
            .unwrap()
            .exists(),
        "migrate-only changed platform state while the internal instance lock was held"
    );
    fixture.assert_configured_addresses_are_unbound();

    drop(lock);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "migrate-only did not complete after the internal instance lock was released: {output:?}"
    );
    assert!(
        fixture
            .config
            .storage
            .sqlite_path
            .as_ref()
            .unwrap()
            .exists()
    );
    fixture.assert_configured_addresses_are_unbound();
}

#[tokio::test]
async fn migrate_only_creates_a_sqlite_backup_before_upgrading_an_existing_platform() {
    let fixture = Fixture::new();
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    let platform_path = fixture.config.storage.sqlite_path.as_ref().unwrap();
    let connection = rusqlite::Connection::open(platform_path).unwrap();
    connection
        .execute_batch(
            "INSERT INTO devices (device_id) VALUES ('backup-before-upgrade');
             PRAGMA user_version = 0;",
        )
        .unwrap();
    drop(connection);

    let output = fixture.migration_command().output().unwrap();
    assert!(
        output.status.success(),
        "migrate-only failed instead of preserving a pre-upgrade SQLite backup: {output:?}"
    );

    let file_name = platform_path.file_name().unwrap().to_str().unwrap();
    let backups = std::fs::read_dir(platform_path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&format!("{file_name}.backup-")))
        })
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1, "expected one pre-upgrade SQLite backup");
    assert!(backups[0].metadata().unwrap().len() > 0);
    let backup = rusqlite::Connection::open(&backups[0]).unwrap();
    let device_id: String = backup
        .query_row(
            "SELECT device_id FROM devices WHERE device_id = 'backup-before-upgrade'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(device_id, "backup-before-upgrade");

    fixture.assert_configured_addresses_are_unbound();
}

fn wait_until_blocked(child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                panic!("migrate-only exited while the internal instance lock was held: {status}")
            }
            None if Instant::now() >= deadline => return,
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
}
