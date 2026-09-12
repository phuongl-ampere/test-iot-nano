use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use chrono::{TimeZone, Utc};
use iot_nano_mqttd::{
    HttpRpcResponseForwarder, HttpStreamUplinkForwarder, RpcResponseForwarder,
    TransportRpcResponse, TransportUplink, UplinkForwarder,
};
use iot_nano_stream::{LocalStream, StreamConfig, http::StreamHttpState};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

const TRANSPORT_SECRET: &str = "transport-secret-must-be-at-least-32-bytes";
const STREAM_SECRET: &str = "stream-secret-must-be-at-least-32-bytes";
const CORE_STREAM_SECRET: &str = "core-stream-secret-must-be-at-least-32xx";

#[derive(Default)]
struct TestState {
    uplinks: Mutex<Vec<serde_json::Value>>,
}

#[derive(Default)]
struct RpcResponseState {
    responses: Mutex<Vec<serde_json::Value>>,
}

#[tokio::test]
async fn rpc_response_forwarder_posts_to_the_mqttd_rpc_response_endpoint() {
    let state = Arc::new(RpcResponseState::default());
    let app = Router::new()
        .route("/internal/mqttd/rpc-response", post(record_rpc_response))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let api_url = format!("http://{address}");
    let forwarder = HttpRpcResponseForwarder::new(&api_url, TRANSPORT_SECRET).unwrap();
    let command_id = Uuid::now_v7();
    let token_id = Uuid::now_v7();
    forwarder
        .forward_response(TransportRpcResponse {
            command_id,
            device_id: "device-a".to_owned(),
            token_id,
            response: json!({"ok": true}),
        })
        .await
        .unwrap();

    let responses = state.responses.lock().await;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0]["command_id"], command_id.to_string());
    assert_eq!(responses[0]["device_id"], "device-a");
    assert_eq!(responses[0]["token_id"], token_id.to_string());
    assert_eq!(responses[0]["response"], json!({"ok": true}));
}

#[tokio::test]
async fn stream_uplink_forwarder_appends_a_canonical_device_event() {
    let state = Arc::new(TestState::default());
    let app = Router::new()
        .route(
            "/internal/streams/telemetry/append",
            post(record_stream_event),
        )
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let stream_url = format!("http://{address}");
    let forwarder = HttpStreamUplinkForwarder::new(&stream_url, STREAM_SECRET).unwrap();
    forwarder
        .forward(
            "iotd_test_token",
            TransportUplink {
                device: iot_nano_mqttd::AuthenticatedDevice {
                    token_id: Uuid::now_v7(),
                    device_id: "device-a".to_owned(),
                    is_gateway: false,
                },
                topic: "v1/devices/me/telemetry".to_owned(),
                payload: serde_json::to_vec(&json!({
                    "schema_version": 1,
                    "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                    "sequence": 1,
                    "event_at": "2026-09-10T08:00:00Z",
                    "measurements": {"temperature_c": 26.4},
                }))
                .unwrap(),
                qos: rumqttc::QoS::AtLeastOnce,
                received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
            },
        )
        .await
        .unwrap();

    let events = state.uplinks.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["topic"], "iot/v1/devices/device-a/telemetry");
    assert_eq!(events[0]["event"]["device_id"], "device-a");
}

#[tokio::test]
async fn stream_uplink_forwarder_appends_to_the_real_stream_service() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let app = iot_nano_stream::http::router(StreamHttpState::new(
        stream.clone(),
        STREAM_SECRET,
        CORE_STREAM_SECRET,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let stream_url = format!("http://{address}");
    let forwarder = HttpStreamUplinkForwarder::new(&stream_url, STREAM_SECRET).unwrap();

    forwarder
        .forward(
            "iotd_test_token",
            TransportUplink {
                device: iot_nano_mqttd::AuthenticatedDevice {
                    token_id: Uuid::now_v7(),
                    device_id: "device-a".to_owned(),
                    is_gateway: false,
                },
                topic: "v1/devices/me/telemetry".to_owned(),
                payload: serde_json::to_vec(&json!({
                    "schema_version": 1,
                    "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                    "sequence": 1,
                    "event_at": "2026-09-10T08:00:00Z",
                    "measurements": {"temperature_c": 26.4},
                }))
                .unwrap(),
                qos: rumqttc::QoS::AtLeastOnce,
                received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
            },
        )
        .await
        .unwrap();

    let records = stream
        .read_partition(stream.partition_for("device-a"), 0, 10)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].message.telemetry().unwrap().event.device_id,
        "device-a"
    );
    server.abort();
}

#[tokio::test]
async fn stream_uplink_forwarder_authorizes_and_appends_gateway_child_telemetry() {
    let state = Arc::new(TestState::default());
    let app = Router::new()
        .route(
            "/internal/mqttd/gateway-authorization",
            post(authorize_gateway),
        )
        .route(
            "/internal/streams/gateway/append",
            post(record_stream_event),
        )
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base_url = format!("http://{address}");
    let forwarder = HttpStreamUplinkForwarder::new(&base_url, STREAM_SECRET)
        .unwrap()
        .with_gateway_authorization(&base_url, TRANSPORT_SECRET)
        .unwrap();
    forwarder
        .forward(
            "iotd_gateway_token",
            TransportUplink {
                device: iot_nano_mqttd::AuthenticatedDevice {
                    token_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
                    device_id: "gateway-001".to_owned(),
                    is_gateway: true,
                },
                topic: "v1/gateways/me/telemetry".to_owned(),
                payload: serde_json::to_vec(&json!({
                    "kind": "child_telemetry",
                    "schema_version": 1,
                    "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
                    "sequence": 1,
                    "event_at": "2026-09-10T08:00:00Z",
                    "child_device_id": "child-001",
                    "measurements": {"temperature_c": 26.4}
                }))
                .unwrap(),
                qos: rumqttc::QoS::AtLeastOnce,
                received_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 1).unwrap(),
            },
        )
        .await
        .unwrap();

    let events = state.uplinks.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0]["gateway_event"]["gateway_device_id"],
        "gateway-001"
    );
    assert_eq!(events[0]["gateway_event"]["child_device_id"], "child-001");
    assert_eq!(events[0]["telemetry_event"]["device_id"], "child-001");
    assert_eq!(
        events[0]["telemetry_event"]["gateway_device_id"],
        "gateway-001"
    );
}

async fn record_stream_event(
    State(state): State<Arc<TestState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<StatusCode, StatusCode> {
    if headers
        .get("x-iot-nano-mqttd-stream-secret")
        .and_then(|value| value.to_str().ok())
        != Some(STREAM_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.uplinks.lock().await.push(body);
    Ok(StatusCode::OK)
}

async fn record_rpc_response(
    State(state): State<Arc<RpcResponseState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<StatusCode, StatusCode> {
    if headers
        .get("x-iot-nano-mqttd-api-secret")
        .and_then(|value| value.to_str().ok())
        != Some(TRANSPORT_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.responses.lock().await.push(body);
    Ok(StatusCode::NO_CONTENT)
}

async fn authorize_gateway(
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if headers
        .get("x-iot-nano-mqttd-api-secret")
        .and_then(|value| value.to_str().ok())
        != Some(TRANSPORT_SECRET)
        || body["gateway_device_id"] != "gateway-001"
        || body["child_device_id"] != "child-001"
        || body["topic"] != "v1/gateways/me/telemetry"
        || body["event_kind"] != "child_telemetry"
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(json!({
        "gateway_device_id": "gateway-001",
        "token_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
        "child_device_id": "child-001",
        "topic": "v1/gateways/me/telemetry",
        "event_kind": "child_telemetry"
    })))
}
