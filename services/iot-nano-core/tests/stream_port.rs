use std::{sync::Arc, time::Duration};

use chrono::{TimeZone, Utc};
use iot_core::TelemetryEvent;
use iot_nano_core::CoreStreamConsumer;
use iot_stream::{LocalStream, StreamConfig, TelemetryMessage};
use serde_json::json;

const TEST_TENANT_ID: uuid::Uuid = uuid::Uuid::from_u128(1);

#[tokio::test]
async fn heartbeat_renews_an_active_inflight_claim_before_acknowledgement() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(
        StreamConfig::sqlite(directory.path().join("stream.sqlite"))
            .with_partitions(1)
            .with_lease_duration(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "esp-000123".to_owned(),
        boot_id: uuid::Uuid::new_v4(),
        sequence: 1,
        event_at: now,
        measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
        gateway_device_id: None,
    };
    stream
        .append(TelemetryMessage {
            tenant_id: TEST_TENANT_ID,
            topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
            payload: serde_json::to_vec(&event).unwrap(),
            event,
            received_at: now,
        })
        .await
        .unwrap();

    let consumer = CoreStreamConsumer::new(Arc::new(stream), "writer", "writer-test");
    let batch = consumer.claim(1).await.unwrap();
    assert_eq!(batch.records().len(), 1);

    tokio::time::sleep(Duration::from_millis(150)).await;
    consumer.heartbeat().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    consumer.acknowledge(&batch).await.unwrap();
}
