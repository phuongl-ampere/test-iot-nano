use std::{
    env,
    fs::{File, OpenOptions},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use chrono::Utc;
use device_simulator::{SimulationConfig, publish_simulation};
use fs2::FileExt;
use iot_ingest::{IngestOutcome, MqttRuntime, MqttRuntimeConfig, TelemetryWriter, migrate};
use iot_stream::{GroupStart, LocalStream, StreamConfig};
use sqlx::{PgPool, Row};

struct TestBroker {
    child: Child,
    port: u16,
}

fn lock_database_file() -> File {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(env::temp_dir().join("rush-iot-nano-timescaledb-tests.lock"))
        .unwrap();
    file.lock_exclusive().unwrap();
    file
}

impl TestBroker {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let child = Command::new("mosquitto")
            .args(["-p", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Self { child, port };
            }
            thread::sleep(Duration::from_millis(20));
        }

        panic!("Mosquitto test broker did not become available");
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn simulated_telemetry_flows_from_mqtt_to_timescaledb() {
    let _database_file_lock = lock_database_file();
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the local TimescaleDB test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query("TRUNCATE command_outbox, device_tokens, telemetry, devices")
        .execute(&pool)
        .await
        .unwrap();

    let broker = TestBroker::start();
    let tempdir = tempfile::tempdir().unwrap();
    let mut stream_config = StreamConfig::for_test(8);
    stream_config.max_record_bytes = 2 * 1024;
    let stream = LocalStream::open(tempdir.path().join("stream"), stream_config).unwrap();
    let mut consumer = stream
        .join_group(
            "timescaledb-writer",
            "e2e-writer",
            GroupStart::Earliest,
            Utc::now(),
        )
        .unwrap();
    let mut runtime = MqttRuntime::new(
        MqttRuntimeConfig {
            client_id: "e2e-ingest".to_owned(),
            broker_host: "127.0.0.1".to_owned(),
            broker_port: broker.port,
        },
        stream,
    );
    runtime.subscribe().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !runtime.is_subscribed() {
            runtime.poll_once(Utc::now()).await.unwrap();
        }
    })
    .await
    .unwrap();

    let publisher = tokio::spawn(publish_simulation(SimulationConfig {
        broker_host: "127.0.0.1".to_owned(),
        broker_port: broker.port,
        devices: 2,
        messages_per_device: 1,
        interval: Duration::from_secs(1),
    }));
    let writer = TelemetryWriter::new(pool.clone(), 1_000);

    let accepted = tokio::time::timeout(Duration::from_secs(3), async {
        let mut accepted = 0;
        while accepted < 2 {
            if runtime.poll_once(Utc::now()).await.unwrap() == Some(IngestOutcome::Accepted) {
                writer.flush_once(&mut consumer, Utc::now()).await.unwrap();
                accepted += 1;
            }
        }
        accepted
    })
    .await
    .unwrap();

    publisher.await.unwrap().unwrap();
    let count = sqlx::query("SELECT COUNT(*) AS count FROM telemetry")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(accepted, 2);
    assert_eq!(count, 2);
    assert_eq!(consumer.group_stats().unwrap().total_lag(), 0);
}
