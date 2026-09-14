use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_monolith::{MonolithConfig, MonolithRuntime};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

struct Fixture {
    _directory: TempDir,
    config: MonolithConfig,
}

impl Fixture {
    async fn new() -> Self {
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
                internal_dir: root.join("internal"),
                public_http: reserve_address().await,
                management_http: reserve_address().await,
                mqtt_tcp: reserve_address().await,
                mqtt_tls: reserve_address().await,
                tls_cert_path: mqtt_fixtures.join("server.crt"),
                tls_key_path: mqtt_fixtures.join("server.key"),
                shutdown_deadline: Duration::from_secs(2),
            },
            _directory: directory,
        }
    }
}

#[tokio::test]
async fn alpha_runtime_binds_health_and_mqtt_after_recovery() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone())
        .await
        .unwrap();

    assert!(runtime.readiness().is_ready());
    assert_health(fixture.config.public_http).await;
    assert_health(fixture.config.management_http).await;
    let mqtt = TcpStream::connect(fixture.config.mqtt_tcp).await.unwrap();
    drop(mqtt);
    let mqtt_tls = TcpStream::connect(fixture.config.mqtt_tls).await.unwrap();
    drop(mqtt_tls);

    runtime
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    for address in [
        fixture.config.public_http,
        fixture.config.management_http,
        fixture.config.mqtt_tcp,
        fixture.config.mqtt_tls,
    ] {
        let listener = TcpListener::bind(address).await.unwrap();
        drop(listener);
    }
}

async fn assert_health(address: SocketAddr) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "unexpected health response: {}",
        String::from_utf8_lossy(&response)
    );
}

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}
