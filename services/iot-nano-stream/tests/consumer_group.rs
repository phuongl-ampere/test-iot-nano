use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use iot_core::TelemetryEvent;
use iot_nano_stream::{
    AcknowledgeRequest, ClaimRequest, GroupStart, HeartbeatRequest, LocalStream, PartitionCommit,
    StreamConfig, StreamError, StreamMessage, StreamPort, TelemetryMessage,
};
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

fn message(sequence: u64) -> TelemetryMessage {
    TelemetryMessage {
        topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
        payload: format!(r#"{{"sequence":{sequence}}}"#).into_bytes(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: "esp-000123".to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence,
            event_at: Utc.with_ymd_and_hms(2026, 9, 12, 8, 0, 0).unwrap(),
            measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
            gateway_device_id: None,
        },
        received_at: Utc.with_ymd_and_hms(2026, 9, 12, 8, 0, 1).unwrap(),
    }
}

fn claim(group: &str, member_id: &str) -> ClaimRequest {
    ClaimRequest {
        group: group.to_owned(),
        member_id: member_id.to_owned(),
        start: GroupStart::Earliest,
        limit: 100,
    }
}

fn test_config(path: impl AsRef<std::path::Path>) -> StreamConfig {
    StreamConfig::sqlite(path).with_lease_duration(Duration::from_millis(100))
}

#[tokio::test]
async fn unacknowledged_records_are_redelivered_until_the_caller_acknowledges() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(test_config(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    stream.append(message(1)).await.unwrap();

    let first = stream.claim(claim("writer", "writer-a")).await.unwrap();
    let replay = stream.claim(claim("writer", "writer-a")).await.unwrap();
    assert_eq!(replay[0].offset, first[0].offset);

    // The platform transaction completes before this direct stream acknowledgement.
    stream
        .acknowledge(AcknowledgeRequest::from_claims(
            "writer", "writer-a", &first,
        ))
        .await
        .unwrap();
    assert!(
        stream
            .claim(claim("writer", "writer-a"))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn acknowledge_cannot_commit_beyond_the_active_claim() {
    let directory = tempdir().unwrap();
    let stream =
        LocalStream::open(test_config(directory.path().join("stream.sqlite")).with_partitions(1))
            .await
            .unwrap();
    stream.append(message(1)).await.unwrap();
    stream.append(message(2)).await.unwrap();

    let mut request = claim("writer", "writer-a");
    request.limit = 1;
    let claimed = stream.claim(request).await.unwrap();
    let first = &claimed[0];

    assert!(matches!(
        stream
            .acknowledge(AcknowledgeRequest {
                group: "writer".to_owned(),
                member_id: "writer-a".to_owned(),
                generation: first.generation,
                commits: vec![PartitionCommit {
                    partition: first.partition,
                    next_offset: 2,
                }],
            })
            .await,
        Err(StreamError::InvalidCommit { requested: 2, .. })
    ));

    let replay = stream.claim(claim("writer", "writer-a")).await.unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

#[tokio::test]
async fn acknowledge_requires_an_active_inflight_claim() {
    let directory = tempdir().unwrap();
    let stream =
        LocalStream::open(test_config(directory.path().join("stream.sqlite")).with_partitions(1))
            .await
            .unwrap();

    assert!(
        stream
            .claim(claim("writer", "writer-a"))
            .await
            .unwrap()
            .is_empty()
    );
    let assignment = stream
        .heartbeat(HeartbeatRequest::new("writer", "writer-a"))
        .await
        .unwrap();
    stream.append(message(1)).await.unwrap();

    assert!(matches!(
        stream
            .acknowledge(AcknowledgeRequest {
                group: "writer".to_owned(),
                member_id: "writer-a".to_owned(),
                generation: assignment.generation,
                commits: vec![PartitionCommit {
                    partition: iot_nano_stream::PartitionId::new(0),
                    next_offset: 1,
                }],
            })
            .await,
        Err(StreamError::NoInflightClaim { .. })
    ));
}

#[tokio::test]
async fn expired_member_lease_reassigns_from_the_durable_offset() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(test_config(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    stream.append(message(1)).await.unwrap();
    stream.append(message(2)).await.unwrap();

    let first = stream.claim(claim("writer", "writer-a")).await.unwrap();
    stream
        .acknowledge(AcknowledgeRequest::from_claims(
            "writer",
            "writer-a",
            &first[..1],
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let replacement = stream.claim(claim("writer", "writer-b")).await.unwrap();
    assert_eq!(replacement.len(), 1);
    assert_eq!(replacement[0].offset, 1);
}

#[tokio::test]
async fn consumer_groups_keep_independent_durable_offsets() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(test_config(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    stream.append(message(1)).await.unwrap();
    stream.append(message(2)).await.unwrap();

    let writer = stream.claim(claim("writer", "writer-a")).await.unwrap();
    stream
        .acknowledge(AcknowledgeRequest::from_claims(
            "writer", "writer-a", &writer,
        ))
        .await
        .unwrap();

    assert_eq!(
        stream
            .claim(claim("alerts", "alerts-a"))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn drain_rejects_new_claims_and_waits_for_existing_claims() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(test_config(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    stream.append(message(1)).await.unwrap();
    let claimed = stream.claim(claim("writer", "writer-a")).await.unwrap();

    let timeout = stream
        .drain_until(Instant::now() + Duration::from_millis(5))
        .await
        .unwrap_err();
    assert!(matches!(timeout, StreamError::DrainTimeout { .. }));
    assert!(matches!(
        stream.claim(claim("alerts", "alerts-a")).await,
        Err(StreamError::Draining)
    ));

    stream
        .acknowledge(AcknowledgeRequest::from_claims(
            "writer", "writer-a", &claimed,
        ))
        .await
        .unwrap();
    stream
        .drain_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn drain_succeeds_immediately_when_no_claim_is_in_flight() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(test_config(directory.path().join("stream.sqlite")))
        .await
        .unwrap();

    stream
        .drain_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn stream_port_runs_the_typed_in_process_workflow() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(test_config(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    let port: &dyn StreamPort = &stream;

    port.append(StreamMessage::Telemetry(message(1)))
        .await
        .unwrap();
    let claimed = port.claim(claim("writer", "writer-a")).await.unwrap();
    let assignment = port
        .heartbeat(HeartbeatRequest::new("writer", "writer-a"))
        .await
        .unwrap();
    assert_eq!(assignment.generation, claimed[0].generation);
    port.acknowledge(AcknowledgeRequest::from_claims(
        "writer", "writer-a", &claimed,
    ))
    .await
    .unwrap();
    port.drain(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}
