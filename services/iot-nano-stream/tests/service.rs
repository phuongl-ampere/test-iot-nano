use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{TimeZone, Utc};
use iot_core::TelemetryEvent;
use iot_nano_stream::{
    LocalStream, StreamConfig, TelemetryMessage,
    http::{StreamHttpState, router},
};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

const MQTTD_SECRET: &str = "mqttd-stream-secret-must-have-at-least-32";
const CORE_SECRET: &str = "core-stream-secret-must-have-at-least-32-xx";

fn message() -> TelemetryMessage {
    TelemetryMessage {
        topic: "iot/v1/devices/esp-000123/telemetry".into(),
        payload: br#"{"temperature_c":26.4}"#.to_vec(),
        event: TelemetryEvent {
            schema_version: 1,
            device_id: "esp-000123".into(),
            boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
            sequence: 1,
            event_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap(),
            measurements: serde_json::Map::from_iter([("temperature_c".into(), json!(26.4))]),
            gateway_device_id: None,
        },
        received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
    }
}

#[tokio::test]
async fn authenticated_append_persists_a_telemetry_record() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let app = router(StreamHttpState::new(
        stream.clone(),
        MQTTD_SECRET,
        CORE_SECRET,
    ));
    let request = Request::builder()
        .method("POST")
        .uri("/internal/streams/telemetry/append")
        .header("x-iot-nano-mqttd-stream-secret", MQTTD_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&message()).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let records = stream
        .read_partition(iot_nano_stream::PartitionId::new(0), 0, 10)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].message.telemetry().unwrap().event.device_id,
        "esp-000123"
    );
}

#[tokio::test]
async fn authenticated_append_persists_a_gateway_record() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let app = router(StreamHttpState::new(
        stream.clone(),
        MQTTD_SECRET,
        CORE_SECRET,
    ));
    let request = Request::builder()
        .method("POST")
        .uri("/internal/streams/gateway/append")
        .header(
            "x-iot-nano-mqttd-stream-secret",
            MQTTD_SECRET,
        )
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "topic": "iot/v1/gateways/gateway-000123/events",
                "payload": [123, 34, 107, 105, 110, 100, 34, 58, 34, 104, 101, 97, 114, 116, 98, 101, 97, 116, 34, 125],
                "gateway_event": {
                    "schema_version": 1,
                    "gateway_device_id": "gateway-000123",
                    "token_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                    "session_id": "session-000123",
                    "event_kind": "heartbeat",
                    "event_at": "2026-09-10T08:00:00Z",
                    "payload": {"kind": "heartbeat"},
                    "idempotency_key": "gateway-000123:boot-1:1"
                },
                "received_at": "2026-09-10T08:00:01Z"
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let records = stream
        .read_partition(iot_nano_stream::PartitionId::new(0), 0, 10)
        .unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn gateway_append_rejects_a_heartbeat_with_child_identity() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let app = router(StreamHttpState::new(stream, MQTTD_SECRET, CORE_SECRET));
    let request = Request::builder()
        .method("POST")
        .uri("/internal/streams/gateway/append")
        .header("x-iot-nano-mqttd-stream-secret", MQTTD_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "topic": "iot/v1/gateways/gateway-000123/events",
                "payload": [123, 34, 107, 105, 110, 100, 34, 58, 34, 104, 101, 97, 114, 116, 98, 101, 97, 116, 34, 125],
                "gateway_event": {
                    "schema_version": 1,
                    "gateway_device_id": "gateway-000123",
                    "child_device_id": "child-000123",
                    "token_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                    "session_id": "session-000123",
                    "event_kind": "heartbeat",
                    "event_at": "2026-09-10T08:00:00Z",
                    "payload": {"kind": "heartbeat"},
                    "idempotency_key": "gateway-000123:boot-1:1"
                },
                "received_at": "2026-09-10T08:00:01Z"
            })
            .to_string(),
        ))
        .unwrap();

    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn consumer_group_claim_and_ack_use_durable_offsets() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let app = router(StreamHttpState::new(stream, MQTTD_SECRET, CORE_SECRET));
    let append = Request::builder()
        .method("POST")
        .uri("/internal/streams/telemetry/append")
        .header("x-iot-nano-mqttd-stream-secret", MQTTD_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&message()).unwrap()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(append).await.unwrap().status(),
        StatusCode::OK
    );

    let claim = Request::builder()
        .method("POST")
        .uri("/internal/groups/core-writer/claim")
        .header("x-iot-nano-core-stream-secret", CORE_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"member_id":"writer-a","start":"earliest","limit":10}"#,
        ))
        .unwrap();
    let response = app.clone().oneshot(claim).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["records"].as_array().unwrap().len(), 1);

    let ack = Request::builder()
        .method("POST")
        .uri("/internal/groups/core-writer/ack")
        .header("x-iot-nano-core-stream-secret", CORE_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "member_id": "writer-a",
                "generation": body["generation"],
                "commits": body["commits"],
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        app.oneshot(ack).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn append_secret_cannot_claim_a_consumer_group() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let app = router(StreamHttpState::new(stream, MQTTD_SECRET, CORE_SECRET));

    let claim = Request::builder()
        .method("POST")
        .uri("/internal/groups/core-writer/claim")
        .header("x-iot-nano-stream-secret", MQTTD_SECRET)
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"member_id":"writer-a","start":"earliest","limit":10}"#,
        ))
        .unwrap();

    assert_eq!(
        app.oneshot(claim).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn maintenance_prunes_expired_records_without_a_core_process() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = StreamConfig::for_test(1);
    config.segment_max_bytes = 1;
    config.retention_max_age = std::time::Duration::from_secs(1);
    let stream = LocalStream::open(directory.path(), config).unwrap();
    let mut expired = message();
    expired.received_at = Utc.with_ymd_and_hms(2026, 9, 10, 7, 0, 0).unwrap();
    let partition = stream.partition_for(&expired.event.device_id);
    stream.append(expired).unwrap();
    let mut fresh = message();
    fresh.received_at = Utc::now();
    fresh.event.event_at = fresh.received_at;
    fresh.event.sequence = 2;
    stream.append(fresh).unwrap();

    let result = iot_nano_stream::maintenance::enforce_retention(stream.clone())
        .await
        .unwrap();

    assert_eq!(result.deleted_segments, 1);
    assert_eq!(stream.read_partition(partition, 1, 10).unwrap().len(), 1);
}
