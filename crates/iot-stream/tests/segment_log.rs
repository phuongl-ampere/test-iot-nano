use std::{fs, fs::OpenOptions, io::Write};

use chrono::{TimeZone, Utc};
use iot_core::TelemetryEvent;
use iot_stream::{LocalStream, StreamConfig, StreamError, TelemetryMessage};
use serde_json::json;
use uuid::Uuid;

const DEVICE_ID: &str = "esp-000123";
const TOPIC: &str = "iot/v1/devices/esp-000123/telemetry";

fn message_with_payload(payload: Vec<u8>) -> TelemetryMessage {
    let mut measurements = serde_json::Map::new();
    measurements.insert("temperature_c".to_owned(), json!(26.4));

    TelemetryMessage {
        topic: TOPIC.to_owned(),
        payload,
        event: TelemetryEvent {
            schema_version: 1,
            device_id: DEVICE_ID.to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence: 1,
            event_at: Utc.with_ymd_and_hms(2026, 9, 5, 1, 0, 0).unwrap(),
            measurements,
            gateway_device_id: None,
        },
        received_at: Utc.with_ymd_and_hms(2026, 9, 5, 1, 0, 1).unwrap(),
    }
}

#[test]
fn device_id_selects_a_stable_fixed_partition() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(8)).unwrap();

    assert_eq!(stream.partition_for(DEVICE_ID).get(), 4);
    assert_eq!(
        stream.partition_for(DEVICE_ID),
        stream.partition_for(DEVICE_ID)
    );
}

#[test]
fn stream_rejects_an_event_larger_than_the_record_limit() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(
        directory.path(),
        StreamConfig::for_test(1).with_max_record_bytes(32),
    )
    .unwrap();

    let error = stream
        .append(message_with_payload(vec![b'x'; 33]))
        .unwrap_err();

    assert!(matches!(error, StreamError::RecordTooLarge { .. }));
}

#[test]
fn append_survives_reopen_and_preserves_partition_offset_order() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();

    assert_eq!(
        stream
            .append(message_with_payload(br#"{"sequence":1}"#.to_vec()))
            .unwrap()
            .offset,
        0
    );
    assert_eq!(
        stream
            .append(message_with_payload(br#"{"sequence":2}"#.to_vec()))
            .unwrap()
            .offset,
        1
    );
    drop(stream);

    let reopened = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let records = reopened
        .read_partition(iot_stream::PartitionId::new(0), 0, 10)
        .unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
}

#[test]
fn open_discards_only_a_truncated_tail_frame() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    stream
        .append(message_with_payload(br#"{"sequence":1}"#.to_vec()))
        .unwrap();
    drop(stream);

    let log_path = directory
        .path()
        .join("partitions/0000/00000000000000000000.log");
    let mut log = OpenOptions::new().append(true).open(log_path).unwrap();
    log.write_all(&20_u32.to_le_bytes()).unwrap();
    log.write_all(b"partial").unwrap();
    log.sync_all().unwrap();

    let reopened = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let records = reopened
        .read_partition(iot_stream::PartitionId::new(0), 0, 10)
        .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
}

#[test]
fn open_discards_a_tail_that_only_contains_a_frame_length() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    stream
        .append(message_with_payload(br#"{"sequence":1}"#.to_vec()))
        .unwrap();
    drop(stream);

    let log_path = directory
        .path()
        .join("partitions/0000/00000000000000000000.log");
    let mut log = OpenOptions::new().append(true).open(log_path).unwrap();
    log.write_all(&20_u32.to_le_bytes()).unwrap();
    log.sync_all().unwrap();

    let reopened = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let records = reopened
        .read_partition(iot_stream::PartitionId::new(0), 0, 10)
        .unwrap();

    assert_eq!(records.len(), 1);
}

#[test]
fn append_rotates_before_writing_another_record_past_segment_limit() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = StreamConfig::for_test(1);
    config.segment_max_bytes = 1;
    let stream = LocalStream::open(directory.path(), config).unwrap();

    assert_eq!(
        stream
            .append(message_with_payload(br#"{"sequence":1}"#.to_vec()))
            .unwrap()
            .offset,
        0
    );
    assert_eq!(
        stream
            .append(message_with_payload(br#"{"sequence":2}"#.to_vec()))
            .unwrap()
            .offset,
        1
    );

    let log_count = fs::read_dir(directory.path().join("partitions/0000"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "log")
        })
        .count();
    let records = stream
        .read_partition(iot_stream::PartitionId::new(0), 0, 10)
        .unwrap();

    assert_eq!(log_count, 2);
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}
