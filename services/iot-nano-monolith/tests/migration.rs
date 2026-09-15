use std::{
    fs::File,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::Command,
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
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

#[tokio::test]
async fn migrate_only_loses_to_an_existing_internal_instance_lock_without_binding_listeners() {
    let fixture = Fixture::new();
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();
    runtime
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();

    let lock_path = fixture.config.internal_dir.join("instance.lock");
    let lock = File::options()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    lock.try_lock_exclusive().unwrap();

    let output = fixture.migration_command().output().unwrap();

    assert!(
        !output.status.success(),
        "migrate-only unexpectedly succeeded while instance.lock was held: {output:?}"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .to_ascii_lowercase()
            .contains("instancelocked"),
        "migrate-only did not report the instance lock: {output:?}"
    );
    fixture.assert_configured_addresses_are_unbound();
}
