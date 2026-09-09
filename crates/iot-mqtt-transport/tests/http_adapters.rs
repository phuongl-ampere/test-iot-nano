use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use chrono::Utc;
use iot_mqtt_transport::{
    DeviceAuthenticator, HttpDeviceAuthenticator, HttpUplinkForwarder, TransportAuthRequest,
    TransportUplink, UplinkForwarder,
};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

const TRANSPORT_SECRET: &str = "transport-secret-must-be-at-least-32-bytes";
const WEBHOOK_SECRET: &str = "webhook-secret-must-be-at-least-32-bytes";

#[derive(Default)]
struct TestState {
    uplinks: Mutex<Vec<serde_json::Value>>,
}

#[tokio::test]
async fn http_adapters_resolve_a_session_and_forward_a_webhook_envelope() {
    let state = Arc::new(TestState::default());
    let app = Router::new()
        .route(
            "/internal/mqtt-transport/session-resolution",
            post(resolve_session),
        )
        .route("/internal/nanomq/telemetry", post(record_uplink))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let api_base_url = format!("http://{address}");
    let authenticator = HttpDeviceAuthenticator::new(&api_base_url, TRANSPORT_SECRET).unwrap();
    let device = authenticator
        .authenticate(TransportAuthRequest {
            client_id: "client-a".to_owned(),
            username: "iotd_test_token".to_owned(),
            password: String::new(),
        })
        .await
        .unwrap();
    let forwarder = HttpUplinkForwarder::new(
        format!("{api_base_url}/internal/nanomq/telemetry"),
        WEBHOOK_SECRET,
    )
    .unwrap();

    forwarder
        .forward(
            "iotd_test_token",
            TransportUplink {
                device,
                topic: "v1/devices/me/telemetry".to_owned(),
                payload: br#"{"schema_version":1}"#.to_vec(),
                qos: rumqttc::QoS::AtLeastOnce,
                received_at: Utc::now(),
            },
        )
        .await
        .unwrap();

    let uplinks = state.uplinks.lock().await;
    assert_eq!(uplinks.len(), 1);
    assert_eq!(uplinks[0]["from_username"], "iotd_test_token");
    assert_eq!(uplinks[0]["topic"], "v1/devices/me/telemetry");
}

async fn resolve_session(
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if headers
        .get("x-iot-mqtt-transport-secret")
        .and_then(|value| value.to_str().ok())
        != Some(TRANSPORT_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if body["username"] != "iotd_test_token" || body["password"] != "" {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(json!({
        "device_id": "device-a",
        "token_id": Uuid::now_v7(),
        "is_gateway": false,
    })))
}

async fn record_uplink(
    State(state): State<Arc<TestState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<StatusCode, StatusCode> {
    if headers
        .get("x-iot-mqtt-transport-webhook")
        .and_then(|value| value.to_str().ok())
        != Some(WEBHOOK_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.uplinks.lock().await.push(body);
    Ok(StatusCode::OK)
}
