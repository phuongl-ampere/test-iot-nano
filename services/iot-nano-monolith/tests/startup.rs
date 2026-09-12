use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{MonolithConfig, MonolithRuntime, StartupError};
use tempfile::TempDir;
use tokio::net::TcpListener;

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
}

impl Fixture {
    async fn sqlite() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let tls_cert_path = directory.path().join("server.crt");
        let tls_key_path = directory.path().join("server.key");
        std::fs::write(&tls_cert_path, "certificate").unwrap();
        std::fs::write(&tls_key_path, "key").unwrap();

        Self {
            config: MonolithConfig {
                storage: StorageConfiguration {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(directory.path().join("platform.sqlite")),
                    sqlite_busy_timeout_ms: 5_000,
                },
                internal_dir: directory.path().join("internal"),
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

#[tokio::test]
async fn second_runtime_cannot_acquire_the_same_internal_instance_lock() {
    let fixture = Fixture::sqlite().await;
    let mut first = MonolithRuntime::start(fixture.config()).await.unwrap();

    assert!(first.readiness().is_ready());
    assert!(fixture.internal_path("instance.lock").is_file());
    assert!(fixture.internal_path("stream.sqlite").is_file());
    assert!(fixture.internal_path("mqttd.sqlite").is_file());
    fixture.assert_configured_addresses_are_unbound().await;

    let mut locked_config = fixture.config();
    locked_config.storage.sqlite_path = None;
    let error = match MonolithRuntime::start(locked_config).await {
        Ok(_) => panic!("the second runtime unexpectedly acquired the instance lock"),
        Err(error) => error,
    };
    assert!(matches!(error, StartupError::InstanceLocked { .. }));
    fixture.assert_configured_addresses_are_unbound().await;

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
    write_corrupt_sqlite(&fixture.internal_path("stream.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("corrupt stream state unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::StreamRecovery(_)));
    assert!(!fixture.internal_path("mqttd.sqlite").exists());
    fixture.assert_configured_addresses_are_unbound().await;
}

#[tokio::test]
async fn corrupt_mqttd_state_binds_no_configured_listener() {
    let fixture = Fixture::sqlite().await;
    write_corrupt_sqlite(&fixture.internal_path("mqttd.sqlite"));

    let error = match MonolithRuntime::start(fixture.config()).await {
        Ok(_) => panic!("corrupt MQTTD state unexpectedly started"),
        Err(error) => error,
    };

    assert!(matches!(error, StartupError::MqttStorage(_)));
    fixture.assert_configured_addresses_are_unbound().await;
}
