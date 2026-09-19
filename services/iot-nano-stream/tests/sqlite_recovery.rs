use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use iot_nano_foundation::TelemetryEvent;
use iot_nano_stream::{
    AcknowledgeRequest, ClaimRequest, GatewayEvent, GatewayEventKind, GatewayMessage, GroupStart,
    LocalStream, StreamConfig, StreamError, StreamMessage, TelemetryMessage,
};
use rusqlite::Connection;
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

fn message(device_id: &str, sequence: u64) -> TelemetryMessage {
    let now = Utc::now();
    TelemetryMessage {
        tenant_id: TEST_TENANT_ID,
        topic: format!("iot/v1/devices/{device_id}/telemetry"),
        payload: format!(r#"{{"sequence":{sequence}}}"#).into_bytes(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: device_id.to_owned(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence,
            event_at: now,
            measurements: serde_json::Map::from_iter([("temperature_c".to_owned(), json!(26.4))]),
            gateway_device_id: None,
        },
        received_at: now,
    }
}

fn gateway_message() -> GatewayMessage {
    let now = Utc::now();
    GatewayMessage {
        tenant_id: TEST_TENANT_ID,
        topic: "iot/v1/gateways/gateway-a/events".to_owned(),
        payload: br#"{\"kind\":\"heartbeat\"}"#.to_vec(),
        gateway_event: GatewayEvent {
            schema_version: 1,
            gateway_device_id: "gateway-a".to_owned(),
            child_device_id: None,
            token_id: Uuid::from_u128(2),
            session_id: Some("session-a".to_owned()),
            event_kind: GatewayEventKind::Heartbeat,
            event_at: now,
            payload: json!({}),
            idempotency_key: "gateway-a:heartbeat:1".to_owned(),
        },
        telemetry_event: None,
        received_at: now,
    }
}

fn legacy_payload_without_tenant_id(message: StreamMessage) -> String {
    let mut payload = serde_json::to_value(message).unwrap();
    payload["message"]
        .as_object_mut()
        .unwrap()
        .remove("tenant_id");
    serde_json::to_string(&payload).unwrap()
}

fn replace_durable_payload(path: &std::path::Path, payload: String) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute("UPDATE stream_records SET payload_json = ?1", [payload])
        .unwrap();
}

fn claim(group: &str, member_id: &str) -> ClaimRequest {
    ClaimRequest {
        group: group.to_owned(),
        member_id: member_id.to_owned(),
        start: GroupStart::Earliest,
        limit: 100,
    }
}

#[tokio::test]
async fn append_is_visible_after_stream_sqlite_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let first = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    let receipt = first.append(message("device-a", 1)).await.unwrap();
    drop(first);

    let connection = Connection::open(&path).unwrap();
    let format: String = connection
        .query_row(
            "SELECT value FROM stream_metadata WHERE key = 'idempotency_key_format'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(format, "tenant_scoped_v1");
    drop(connection);

    let reopened = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    let claimed = reopened.claim(claim("writer", "writer-a")).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].offset, receipt.offset);
    assert_eq!(claimed[0].partition, receipt.partition);
    assert_eq!(claimed[0].message.tenant_id(), TEST_TENANT_ID);
}

#[tokio::test]
async fn claim_rejects_legacy_telemetry_payload_without_tenant_id() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let telemetry = message("device-a", 1);
    let legacy_payload = legacy_payload_without_tenant_id(StreamMessage::from(telemetry.clone()));
    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    stream.append(telemetry).await.unwrap();
    drop(stream);
    replace_durable_payload(&path, legacy_payload);

    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    let error = stream.claim(claim("writer", "writer-a")).await.unwrap_err();

    assert!(matches!(
        &error,
        StreamError::ResetRequiredDurableRecord {
            record_type: "telemetry"
        }
    ));
    assert!(
        error.to_string().contains("reset the development stream"),
        "unexpected stream error: {error}"
    );
    assert!(
        error.to_string().contains("telemetry"),
        "unexpected stream error: {error}"
    );
}

#[tokio::test]
async fn claim_rejects_legacy_gateway_payload_without_tenant_id() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let gateway = gateway_message();
    let legacy_payload = legacy_payload_without_tenant_id(StreamMessage::from(gateway.clone()));
    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    stream.append(gateway).await.unwrap();
    drop(stream);
    replace_durable_payload(&path, legacy_payload);

    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    let error = stream.claim(claim("writer", "writer-a")).await.unwrap_err();

    assert!(matches!(
        &error,
        StreamError::ResetRequiredDurableRecord {
            record_type: "gateway"
        }
    ));
    assert!(
        error.to_string().contains("reset the development stream"),
        "unexpected stream error: {error}"
    );
    assert!(
        error.to_string().contains("gateway"),
        "unexpected stream error: {error}"
    );
}

#[tokio::test]
async fn duplicate_idempotency_key_returns_the_original_record() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    let original = stream.append(message("device-a", 7)).await.unwrap();
    let duplicate = stream.append(message("device-a", 7)).await.unwrap();

    assert_eq!(duplicate, original);
    assert_eq!(
        stream
            .claim(claim("writer", "writer-a"))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn telemetry_idempotency_is_scoped_to_tenant() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    let tenant_a = message("device-a", 7);
    let mut tenant_b = tenant_a.clone();
    tenant_b.tenant_id = Uuid::from_u128(2);

    let first = stream.append(tenant_a.clone()).await.unwrap();
    let other_tenant = stream.append(tenant_b).await.unwrap();
    let duplicate = stream.append(tenant_a).await.unwrap();

    assert_ne!(other_tenant, first);
    assert_eq!(duplicate, first);
    assert_eq!(
        stream
            .claim(claim("writer", "writer-a"))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn gateway_idempotency_is_scoped_to_tenant() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    let tenant_a = gateway_message();
    let mut tenant_b = tenant_a.clone();
    tenant_b.tenant_id = Uuid::from_u128(2);

    let first = stream.append(tenant_a.clone()).await.unwrap();
    let other_tenant = stream.append(tenant_b).await.unwrap();
    let duplicate = stream.append(tenant_a).await.unwrap();

    assert_ne!(other_tenant, first);
    assert_eq!(duplicate, first);
    assert_eq!(
        stream
            .claim(claim("writer", "writer-a"))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn duplicate_idempotency_key_returns_the_original_receipt_after_retention() {
    let directory = tempdir().unwrap();
    let mut config = StreamConfig::sqlite(directory.path().join("stream.sqlite"));
    config.retention_max_age = Duration::from_secs(1);
    let stream = LocalStream::open(config).await.unwrap();
    let mut expired = message("device-a", 7);
    expired.received_at = Utc::now() - ChronoDuration::seconds(2);
    expired.event.event_at = expired.received_at;

    let original = stream.append(expired).await.unwrap();
    stream.enforce_retention(Utc::now()).await.unwrap();

    let duplicate = stream.append(message("device-a", 7)).await.unwrap();
    assert_eq!(duplicate, original);
}

#[tokio::test]
async fn opening_a_legacy_stream_database_preserves_tenant_scoped_idempotency_tombstones() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let mut config = StreamConfig::sqlite(&path);
    config.retention_max_age = Duration::from_secs(1);
    let stream = LocalStream::open(config.clone()).await.unwrap();
    let mut expired = message("device-a", 7);
    expired.received_at = Utc::now() - ChronoDuration::seconds(2);
    expired.event.event_at = expired.received_at;
    let original = stream.append(expired).await.unwrap();
    drop(stream);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = OFF;
            DROP TABLE stream_idempotency;
            CREATE TABLE stream_idempotency (
                idempotency_key TEXT PRIMARY KEY NOT NULL,
                partition INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                FOREIGN KEY (partition, offset)
                    REFERENCES stream_records(partition, offset) ON DELETE CASCADE
            );
            ",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO stream_idempotency(idempotency_key, partition, offset)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                format!(
                    "telemetry:{TEST_TENANT_ID}:device-a:c9c04d99-4e01-4f94-82a8-9e229e47c093:7"
                ),
                i64::from(original.partition.get()),
                i64::try_from(original.offset).unwrap(),
            ],
        )
        .unwrap();
    drop(connection);

    let reopened = LocalStream::open(config).await.unwrap();
    let connection = Connection::open(&path).unwrap();
    let mut statement = connection
        .prepare("PRAGMA foreign_key_list(stream_idempotency)")
        .unwrap();
    let foreign_key_targets = statement
        .query_map([], |row| row.get::<_, String>(2))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(statement);
    assert!(foreign_key_targets.is_empty());
    drop(connection);
    reopened.enforce_retention(Utc::now()).await.unwrap();
    let duplicate = reopened.append(message("device-a", 7)).await.unwrap();
    assert_eq!(duplicate, original);
}

#[tokio::test]
async fn opening_stream_with_unmarked_nonempty_legacy_idempotency_state_requires_reset() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let config = StreamConfig::sqlite(&path);
    let stream = LocalStream::open(config.clone()).await.unwrap();
    let original = stream.append(message("device-a", 7)).await.unwrap();
    drop(stream);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = OFF;
            DROP TABLE stream_idempotency;
            CREATE TABLE stream_idempotency (
                idempotency_key TEXT PRIMARY KEY NOT NULL,
                partition INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                FOREIGN KEY (partition, offset)
                    REFERENCES stream_records(partition, offset) ON DELETE CASCADE
            );
            ",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO stream_idempotency(idempotency_key, partition, offset)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                format!(
                    "telemetry:{TEST_TENANT_ID}:device-a:c9c04d99-4e01-4f94-82a8-9e229e47c093:7"
                ),
                i64::from(original.partition.get()),
                i64::try_from(original.offset).unwrap(),
            ],
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM stream_metadata WHERE key = 'idempotency_key_format'",
            [],
        )
        .unwrap();
    drop(connection);

    let error = match LocalStream::open(config).await {
        Err(error) => error,
        Ok(_) => panic!("unmarked idempotency state must require a stream reset"),
    };

    assert!(matches!(
        error,
        StreamError::ResetRequiredDurableRecord {
            record_type: "idempotency"
        }
    ));
    assert!(error.to_string().contains("reset the development stream"));

    let connection = Connection::open(&path).unwrap();
    let mut statement = connection
        .prepare("PRAGMA foreign_key_list(stream_idempotency)")
        .unwrap();
    let foreign_key_targets = statement
        .query_map([], |row| row.get::<_, String>(2))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(statement);
    assert_eq!(
        foreign_key_targets,
        vec!["stream_records".to_owned(), "stream_records".to_owned()]
    );
}

#[tokio::test]
async fn opening_stream_with_unmarked_empty_legacy_idempotency_schema_requires_reset() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let config = StreamConfig::sqlite(&path);
    let stream = LocalStream::open(config.clone()).await.unwrap();
    drop(stream);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = OFF;
            DROP TABLE stream_idempotency;
            CREATE TABLE stream_idempotency (
                idempotency_key TEXT PRIMARY KEY NOT NULL,
                partition INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                FOREIGN KEY (partition, offset)
                    REFERENCES stream_records(partition, offset) ON DELETE CASCADE
            );
            DELETE FROM stream_metadata WHERE key = 'idempotency_key_format';
            ",
        )
        .unwrap();
    drop(connection);

    let error = match LocalStream::open(config).await {
        Err(error) => error,
        Ok(_) => panic!("unmarked legacy idempotency schema must require a stream reset"),
    };
    assert!(matches!(
        error,
        StreamError::ResetRequiredDurableRecord {
            record_type: "idempotency"
        }
    ));

    let connection = Connection::open(&path).unwrap();
    let foreign_key_targets = connection
        .prepare("PRAGMA foreign_key_list(stream_idempotency)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(2))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        foreign_key_targets,
        vec!["stream_records".to_owned(), "stream_records".to_owned()]
    );
}

#[tokio::test]
async fn opening_stream_with_incompatible_idempotency_format_requires_reset() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let config = StreamConfig::sqlite(&path);
    let stream = LocalStream::open(config.clone()).await.unwrap();
    drop(stream);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE stream_metadata
             SET value = 'legacy_v0'
             WHERE key = 'idempotency_key_format'",
            [],
        )
        .unwrap();
    drop(connection);

    let error = match LocalStream::open(config).await {
        Err(error) => error,
        Ok(_) => panic!("incompatible idempotency format must require a stream reset"),
    };

    assert!(matches!(
        error,
        StreamError::ResetRequiredDurableRecord {
            record_type: "idempotency"
        }
    ));
}

#[tokio::test]
async fn opening_stream_with_incompatible_legacy_idempotency_schema_requires_reset() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let config = StreamConfig::sqlite(&path);
    let stream = LocalStream::open(config.clone()).await.unwrap();
    drop(stream);

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "
            PRAGMA foreign_keys = OFF;
            DROP TABLE stream_idempotency;
            CREATE TABLE stream_idempotency (
                idempotency_key TEXT PRIMARY KEY NOT NULL,
                partition INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                FOREIGN KEY (partition, offset)
                    REFERENCES stream_records(partition, offset) ON DELETE CASCADE
            );
            UPDATE stream_metadata
            SET value = 'legacy_v0'
            WHERE key = 'idempotency_key_format';
            ",
        )
        .unwrap();
    drop(connection);

    let error = match LocalStream::open(config).await {
        Err(error) => error,
        Ok(_) => panic!("incompatible legacy idempotency schema must require a stream reset"),
    };
    assert!(matches!(
        error,
        StreamError::ResetRequiredDurableRecord {
            record_type: "idempotency"
        }
    ));

    let connection = Connection::open(&path).unwrap();
    let foreign_key_targets = connection
        .prepare("PRAGMA foreign_key_list(stream_idempotency)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(2))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        foreign_key_targets,
        vec!["stream_records".to_owned(), "stream_records".to_owned()]
    );
}

#[tokio::test]
async fn capacity_rejection_preserves_unexpired_records() {
    let directory = tempdir().unwrap();
    let first = message("device-a", 1);
    let first_bytes = u64::try_from(
        serde_json::to_string(&StreamMessage::from(first.clone()))
            .unwrap()
            .len(),
    )
    .unwrap();
    let mut config = StreamConfig::sqlite(directory.path().join("stream.sqlite"));
    config.retention_max_age = Duration::from_secs(365 * 24 * 60 * 60);
    config.retention_max_bytes = first_bytes;
    let stream = LocalStream::open(config).await.unwrap();

    stream.append(first).await.unwrap();
    assert!(matches!(
        stream.append(message("device-a", 2)).await,
        Err(StreamError::CapacityExceeded { .. })
    ));

    let records = stream.claim(claim("writer", "writer-a")).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
}

#[tokio::test]
async fn committed_group_offsets_survive_reopen_independently() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    stream.append(message("device-a", 1)).await.unwrap();
    stream.append(message("device-a", 2)).await.unwrap();

    let writer_records = stream.claim(claim("writer", "writer-a")).await.unwrap();
    stream
        .acknowledge(AcknowledgeRequest::from_claims(
            "writer",
            "writer-a",
            &writer_records,
        ))
        .await
        .unwrap();
    drop(stream);

    let reopened = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    assert!(
        reopened
            .claim(claim("writer", "writer-b"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened
            .claim(claim("alerts", "alerts-a"))
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn committed_records_are_not_lost_when_the_process_stops() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    for sequence in 1..=3 {
        stream.append(message("device-a", sequence)).await.unwrap();
    }
    drop(stream);

    let restarted = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    let records = restarted.claim(claim("writer", "writer-a")).await.unwrap();

    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
}

#[tokio::test]
async fn stream_sqlite_uses_its_own_schema_and_application_marker() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("stream.sqlite");
    let stream = LocalStream::open(StreamConfig::sqlite(&path))
        .await
        .unwrap();
    drop(stream);

    let connection = Connection::open(&path).unwrap();
    let application_id: i32 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .unwrap();
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    let tables = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(application_id, 0x4953_5453);
    assert_eq!(journal_mode, "wal");
    for expected in [
        "stream_records",
        "stream_idempotency",
        "stream_groups",
        "stream_group_leases",
        "stream_group_offsets",
    ] {
        assert!(tables.iter().any(|table| table == expected));
    }
    assert!(!tables.iter().any(|table| table == "telemetry"));
}

#[tokio::test]
async fn direct_append_preserves_message_validation_without_http() {
    let directory = tempdir().unwrap();
    let stream = LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
        .await
        .unwrap();
    let mut invalid = message("device-a", 1);
    invalid.topic = "invalid/topic".to_owned();

    assert!(matches!(
        stream.append(invalid).await,
        Err(iot_nano_stream::StreamError::InvalidTelemetry(_))
    ));
}

#[tokio::test]
async fn stream_refuses_a_sqlite_file_that_already_contains_platform_tables() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("platform.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE telemetry (id INTEGER PRIMARY KEY)")
        .unwrap();

    let result = LocalStream::open(StreamConfig::sqlite(&path)).await;

    assert!(matches!(
        result,
        Err(iot_nano_stream::StreamError::ForeignTable { table }) if table == "telemetry"
    ));
}

#[tokio::test]
async fn retention_removes_expired_records_without_reusing_offsets() {
    let directory = tempdir().unwrap();
    let mut config = StreamConfig::sqlite(directory.path().join("stream.sqlite"));
    config.retention_max_age = Duration::from_secs(1);
    let stream = LocalStream::open(config).await.unwrap();
    let mut expired = message("device-a", 1);
    expired.received_at = Utc::now() - ChronoDuration::seconds(2);
    stream.append(expired).await.unwrap();
    let mut fresh = message("device-a", 2);
    fresh.received_at = Utc::now();
    fresh.event.event_at = fresh.received_at;
    stream.append(fresh).await.unwrap();

    let records = stream.claim(claim("writer", "writer-a")).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 1);
}

#[tokio::test]
async fn lagging_group_receives_offset_out_of_range_after_retention() {
    let directory = tempdir().unwrap();
    let mut config = StreamConfig::sqlite(directory.path().join("stream.sqlite"));
    config.retention_max_age = Duration::from_secs(1);
    let stream = LocalStream::open(config).await.unwrap();
    assert!(
        stream
            .claim(claim("alerts", "alerts-a"))
            .await
            .unwrap()
            .is_empty()
    );

    let mut expired = message("device-a", 1);
    expired.received_at = Utc::now() - ChronoDuration::seconds(2);
    stream.append(expired).await.unwrap();
    let mut fresh = message("device-a", 2);
    fresh.received_at = Utc::now();
    fresh.event.event_at = fresh.received_at;
    stream.append(fresh).await.unwrap();

    assert!(matches!(
        stream.claim(claim("alerts", "alerts-a")).await,
        Err(iot_nano_stream::StreamError::OffsetOutOfRange {
            requested: 0,
            earliest: 1,
            ..
        })
    ));
}
