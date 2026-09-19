use std::{
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::Duration,
};

use chrono::{TimeZone, Utc};
use iot_nano_core::{
    CoreStreamConsumer, IngestOutcome, MqttRuntime, MqttRuntimeConfig, MqttStreamProducer,
};
use iot_nano_foundation::TelemetryEvent;
use iot_stream::{LocalStream, StreamConfig};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

const TOPIC: &str = "iot/v1/devices/esp-000123/telemetry";
const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

struct TestBroker {
    child: Child,
    port: u16,
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

async fn stream(dir: &TempDir) -> LocalStream {
    LocalStream::open(
        StreamConfig::sqlite(dir.path().join("stream.sqlite"))
            .with_partitions(8)
            .with_max_record_bytes(2 * 1024),
    )
    .await
    .unwrap()
}

fn telemetry_payload(device_id: &str) -> Vec<u8> {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: device_id.to_owned(),
        boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
        sequence: 1842,
        event_at: Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap(),
        measurements,
        gateway_device_id: None,
    };

    serde_json::to_vec(&event).unwrap()
}

#[tokio::test]
async fn valid_mqtt_payload_is_durably_appended_to_the_stream() {
    let tempdir = tempfile::tempdir().unwrap();
    let stream = stream(&tempdir).await;
    let consumer = MqttStreamProducer::new(TEST_TENANT_ID, Arc::new(stream.clone()));
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();

    let outcome = consumer
        .ingest(TOPIC, &telemetry_payload("esp-000123"), now)
        .await
        .unwrap();

    assert_eq!(outcome, IngestOutcome::Accepted);
    let reader = CoreStreamConsumer::new(Arc::new(stream), "mqtt-test", "reader");
    assert_eq!(reader.claim(10).await.unwrap().records().len(), 1);
}

#[tokio::test]
async fn device_id_mismatch_is_rejected_without_appending_to_the_stream() {
    let tempdir = tempfile::tempdir().unwrap();
    let stream = stream(&tempdir).await;
    let consumer = MqttStreamProducer::new(TEST_TENANT_ID, Arc::new(stream.clone()));
    let now = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 1).unwrap();

    let outcome = consumer
        .ingest(TOPIC, &telemetry_payload("esp-000456"), now)
        .await
        .unwrap();

    assert_eq!(outcome, IngestOutcome::Rejected);
    assert!(
        stream
            .stats()
            .await
            .unwrap()
            .partitions
            .iter()
            .all(|partition| partition.next_offset == 0)
    );
}

#[tokio::test]
async fn manual_ack_runtime_persists_a_qos_one_publish_before_acknowledging_it() {
    let broker = TestBroker::start();
    let tempdir = tempfile::tempdir().unwrap();
    let stream = stream(&tempdir).await;
    let mut runtime = MqttRuntime::new(
        MqttRuntimeConfig {
            tenant_id: TEST_TENANT_ID,
            client_id: "ingest-runtime-test".to_owned(),
            broker_host: "127.0.0.1".to_owned(),
            broker_port: broker.port,
        },
        Arc::new(stream.clone()),
    );
    runtime.subscribe().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while !runtime.is_subscribed() {
            runtime.poll_once(Utc::now()).await.unwrap();
        }
    })
    .await
    .unwrap();

    let mut options = MqttOptions::new("publisher-runtime-test", "127.0.0.1", broker.port);
    options.set_keep_alive(Duration::from_secs(5));
    let (publisher, mut publisher_event_loop) = AsyncClient::new(options, 10);
    tokio::spawn(async move { while publisher_event_loop.poll().await.is_ok() {} });
    publisher
        .publish(
            TOPIC,
            QoS::AtLeastOnce,
            false,
            telemetry_payload("esp-000123"),
        )
        .await
        .unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(outcome) = runtime.poll_once(Utc::now()).await.unwrap() {
                return outcome;
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(outcome, IngestOutcome::Accepted);
    let reader = CoreStreamConsumer::new(Arc::new(stream), "mqtt-runtime-test", "reader");
    assert_eq!(reader.claim(10).await.unwrap().records().len(), 1);
}
