use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use chrono::{TimeZone, Utc};
use iot_core::{RpcMode, TelemetryEvent};
use iot_nano_core::{
    CommandTransport, HttpStreamConsumer, HttpTransportRpcClient, TransportRpcPublishRequest,
};
use iot_stream::{
    AcknowledgeRequest, ClaimRequest, GroupStart, PartitionId, StreamPort, StreamRecord,
    TelemetryMessage,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const STREAM_SECRET: &str = "stream-secret-must-have-at-least-32-ascii";
const MQTTD_SECRET: &str = "mqttd-secret-must-have-at-least-32-ascii";

#[derive(Clone)]
struct LegacyStreamState {
    response: Value,
    requests: Arc<tokio::sync::Mutex<Vec<Value>>>,
}

async fn legacy_claim(
    State(state): State<LegacyStreamState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    state.requests.lock().await.push(json!({
        "kind": "claim",
        "secret": headers
            .get("x-iot-nano-core-stream-secret")
            .and_then(|value| value.to_str().ok()),
        "request": request,
    }));
    Json(state.response)
}

async fn legacy_ack(
    State(state): State<LegacyStreamState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> StatusCode {
    state.requests.lock().await.push(json!({
        "kind": "ack",
        "secret": headers
            .get("x-iot-nano-core-stream-secret")
            .and_then(|value| value.to_str().ok()),
        "request": request,
    }));
    StatusCode::NO_CONTENT
}

#[tokio::test]
async fn legacy_http_stream_adapter_claims_and_acknowledges_through_stream_port() {
    let now = Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "esp-000123".to_owned(),
        boot_id: Uuid::new_v4(),
        sequence: 1,
        event_at: now,
        measurements: serde_json::Map::new(),
        gateway_device_id: None,
    };
    let record = StreamRecord {
        partition: PartitionId::new(0),
        offset: 0,
        message: TelemetryMessage {
            topic: "iot/v1/devices/esp-000123/telemetry".to_owned(),
            payload: serde_json::to_vec(&event).unwrap(),
            event,
            received_at: now,
        }
        .into(),
    };
    let state = LegacyStreamState {
        response: json!({
            "generation": 42,
            "records": [serde_json::to_value(record).unwrap()],
            "commits": [],
        }),
        requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/internal/groups/legacy/claim", post(legacy_claim))
        .route("/internal/groups/legacy/ack", post(legacy_ack))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let consumer = HttpStreamConsumer::new(
        format!("http://{address}"),
        STREAM_SECRET,
        "legacy",
        "core-a",
    )
    .unwrap();
    let records = StreamPort::claim(
        &consumer,
        ClaimRequest {
            group: "legacy".to_owned(),
            member_id: "core-a".to_owned(),
            start: GroupStart::Earliest,
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].generation, 42);
    StreamPort::acknowledge(
        &consumer,
        AcknowledgeRequest::from_claims("legacy", "core-a", &records),
    )
    .await
    .unwrap();

    let requests = state.requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["kind"], "claim");
    assert_eq!(requests[0]["secret"], STREAM_SECRET);
    assert_eq!(requests[0]["request"]["member_id"], "core-a");
    assert_eq!(requests[1]["kind"], "ack");
    assert_eq!(requests[1]["request"]["generation"], 42);
    assert_eq!(requests[1]["request"]["commits"][0]["next_offset"], 1);
    drop(requests);
    server.abort();
}

#[tokio::test]
async fn legacy_http_command_transport_posts_a_rpc_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1_024];
        let header_end = loop {
            let read = socket.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        while request.len() - header_end < content_length {
            let read = socket.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
        }
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        String::from_utf8(request).unwrap()
    });

    let client = HttpTransportRpcClient::new(format!("http://{address}"), MQTTD_SECRET)
        .unwrap()
        .with_timeout(Duration::from_secs(1));
    let command_id = Uuid::new_v4();
    CommandTransport::publish(
        &client,
        TransportRpcPublishRequest {
            device_id: "esp-000123".to_owned(),
            id: command_id,
            method: "sample_now".to_owned(),
            params: json!({ "source": "dashboard" }),
            mode: RpcMode::OneWay,
            issued_at: Utc.with_ymd_and_hms(2026, 9, 12, 0, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2026, 9, 12, 0, 1, 0).unwrap(),
        },
    )
    .await
    .unwrap();

    let request = server.await.unwrap();
    assert!(request.starts_with("POST /internal/rpc/publish HTTP/1.1\r\n"));
    assert!(request.contains(&format!("x-iot-nano-core-mqttd-secret: {MQTTD_SECRET}")));
    assert!(request.contains(&format!("\"id\":\"{command_id}\"")));
}
