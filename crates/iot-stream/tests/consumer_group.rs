use chrono::{Duration, TimeZone, Utc};
use iot_core::TelemetryEvent;
use iot_stream::{GroupStart, LocalStream, StreamConfig, TelemetryMessage};
use serde_json::json;
use uuid::Uuid;

fn fixed_now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 5, 2, 0, 0).unwrap()
}

fn message(sequence: u64) -> TelemetryMessage {
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
            event_at: fixed_now(),
            measurements,
            gateway_device_id: None,
        },
        received_at: fixed_now(),
    }
}

#[test]
fn two_members_receive_disjoint_partition_leases() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(8)).unwrap();
    let now = fixed_now();
    let mut first = stream
        .join_group("timescaledb-writer", "writer-a", GroupStart::Earliest, now)
        .unwrap();
    let mut second = stream
        .join_group("timescaledb-writer", "writer-b", GroupStart::Earliest, now)
        .unwrap();

    let first_partitions = first.heartbeat(now).unwrap().partitions;
    let second_partitions = second.heartbeat(now).unwrap().partitions;

    assert!(
        first_partitions
            .iter()
            .all(|partition| !second_partitions.contains(partition))
    );
    assert_eq!(first_partitions.len() + second_partitions.len(), 8);
}

#[test]
fn expired_member_lease_reassigns_partition_from_last_committed_offset() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    stream.append(message(1)).unwrap();
    stream.append(message(2)).unwrap();
    let now = fixed_now();
    let first = stream
        .join_group("timescaledb-writer", "writer-a", GroupStart::Earliest, now)
        .unwrap();

    let batch = first.poll(1, now).unwrap();
    first.commit(batch, now).unwrap();

    let mut replacement = stream
        .join_group(
            "timescaledb-writer",
            "writer-b",
            GroupStart::Earliest,
            now + Duration::seconds(301),
        )
        .unwrap();
    replacement.heartbeat(now + Duration::seconds(301)).unwrap();
    let replay = replacement.poll(10, now + Duration::seconds(301)).unwrap();

    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].offset, 1);
    assert_eq!(replay.records[0].message.event.sequence, 2);
}

#[test]
fn stale_member_cannot_commit_after_rebalance() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    stream.append(message(1)).unwrap();
    let now = fixed_now();
    let first = stream
        .join_group("timescaledb-writer", "writer-a", GroupStart::Earliest, now)
        .unwrap();
    let batch = first.poll(1, now).unwrap();
    stream
        .join_group("timescaledb-writer", "writer-b", GroupStart::Earliest, now)
        .unwrap();

    let error = first.commit(batch, now).unwrap_err();

    assert!(matches!(error, iot_stream::StreamError::StaleGeneration));
}

#[test]
fn groups_keep_independent_offsets() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    stream.append(message(1)).unwrap();
    stream.append(message(2)).unwrap();
    let now = fixed_now();
    let writer = stream
        .join_group("timescaledb-writer", "writer-a", GroupStart::Earliest, now)
        .unwrap();
    let alerts = stream
        .join_group("alerts", "alerts-a", GroupStart::Earliest, now)
        .unwrap();

    writer.commit(writer.poll(1, now).unwrap(), now).unwrap();
    let records = alerts.poll(10, now).unwrap().records;

    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}
