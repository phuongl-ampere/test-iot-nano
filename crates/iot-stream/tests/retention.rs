use std::time::Duration as StdDuration;

use chrono::{Duration, TimeZone, Utc};
use iot_core::TelemetryEvent;
use iot_stream::{GroupStart, LocalStream, StreamConfig, StreamError, TelemetryMessage};
use serde_json::json;
use uuid::Uuid;

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 5, 3, 0, 0).unwrap()
}

fn message(sequence: u64, received_at: chrono::DateTime<Utc>) -> TelemetryMessage {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));

    TelemetryMessage {
        topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
        payload: format!(r#"{{"sequence":{sequence}}}"#).into_bytes(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: "esp-000123".to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence,
            event_at: received_at,
            measurements,
            gateway_device_id: None,
        },
        received_at,
    }
}

#[test]
fn retention_deletes_only_closed_segments_and_advances_earliest_offset() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = StreamConfig::for_test(1);
    config.segment_max_bytes = 1;
    config.retention_max_age = StdDuration::from_secs(7 * 24 * 60 * 60);
    let stream = LocalStream::open(directory.path(), config).unwrap();
    let old = now() - Duration::days(8);
    stream.append(message(1, old)).unwrap();
    stream.append(message(2, old)).unwrap();
    stream.append(message(3, now())).unwrap();

    let result = stream.enforce_retention(now()).unwrap();
    let stats = stream.stats().unwrap();

    assert_eq!(result.deleted_segments, 2);
    assert_eq!(stats.partitions[0].earliest_offset, 2);
    assert_eq!(stats.partitions[0].next_offset, 3);
}

#[test]
fn append_returns_capacity_error_without_modifying_the_active_log() {
    let first = message(1, now());
    let mut config = StreamConfig::for_test(1);
    let frame_bytes = u64::try_from(serde_json::to_vec(&first).unwrap().len()).unwrap() + 8;
    config.segment_max_bytes = frame_bytes;
    config.retention_max_bytes = frame_bytes;
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), config).unwrap();

    stream.append(first).unwrap();
    let error = stream.append(message(2, now())).unwrap_err();

    assert!(matches!(error, StreamError::CapacityExceeded { .. }));
    assert_eq!(stream.stats().unwrap().partitions[0].next_offset, 1);
}

#[test]
fn lagging_group_receives_offset_out_of_range_after_retention() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = StreamConfig::for_test(1);
    config.segment_max_bytes = 1;
    config.retention_max_age = StdDuration::from_secs(7 * 24 * 60 * 60);
    let stream = LocalStream::open(directory.path(), config).unwrap();
    let old = now() - Duration::days(8);
    stream.append(message(1, old)).unwrap();
    stream.append(message(2, old)).unwrap();
    stream.append(message(3, now())).unwrap();
    let consumer = stream
        .join_group("alerts", "alerts-a", GroupStart::Earliest, now())
        .unwrap();

    stream.enforce_retention(now()).unwrap();
    let error = consumer.poll(1, now()).unwrap_err();

    assert!(matches!(
        error,
        StreamError::OffsetOutOfRange {
            requested: 0,
            earliest: 2,
            ..
        }
    ));
}
