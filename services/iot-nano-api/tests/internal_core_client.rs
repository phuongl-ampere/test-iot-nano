use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use chrono::{TimeZone, Utc};
use iot_api::{
    CoreClient, CoreCommandCreateRequest, CoreCommandResponseRequest, CoreTelemetryBucket,
};
use iot_core::RpcMode;
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

const CORE_SECRET: &str = "core-control-secret-must-have-at-least-32";

#[derive(Default)]
struct TestState {
    received: Mutex<Vec<serde_json::Value>>,
}

#[tokio::test]
async fn client_authenticates_and_round_trips_a_core_command() {
    let state = Arc::new(TestState::default());
    let app = Router::new()
        .route("/internal/commands", post(create_command))
        .route("/internal/commands/response", post(record_response))
        .route("/internal/commands/{id}", get(get_command))
        .route("/internal/telemetry/devices/{device_id}", get(telemetry))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = CoreClient::new(format!("http://{address}"), CORE_SECRET).unwrap();
    let id = Uuid::now_v7();

    let created = client
        .create(CoreCommandCreateRequest {
            id,
            device_id: "core-client-device".to_owned(),
            method: "setRelay".to_owned(),
            params: json!({"enabled": true}),
            mode: RpcMode::TwoWay,
            issued_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 5, 0).unwrap(),
        })
        .await
        .unwrap();
    let fetched = client.get(id).await.unwrap();
    let telemetry = client
        .telemetry(
            "core-client-device",
            Utc.with_ymd_and_hms(2026, 9, 10, 7, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 9, 10, 9, 0, 0).unwrap(),
            CoreTelemetryBucket::Raw,
        )
        .await
        .unwrap();
    client
        .record_response(CoreCommandResponseRequest {
            command_id: id,
            device_id: "core-client-device".to_owned(),
            token_id: Uuid::now_v7(),
            response: json!({"ok": true}),
            responded_at: Utc.with_ymd_and_hms(2026, 9, 10, 8, 0, 30).unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(created.id, id);
    assert_eq!(fetched.device_id, "core-client-device");
    assert_eq!(fetched.state, "queued");
    assert_eq!(fetched.mode, RpcMode::TwoWay);
    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry[0].event_count, 1);
    let received = state.received.lock().await;
    assert_eq!(received.len(), 2);
    assert!(received[1].get("token_id").is_none());
    drop(received);
    server.abort();
}

async fn telemetry(
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Query(_query): Query<serde_json::Value>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    if headers
        .get("x-iot-nano-api-core-secret")
        .and_then(|value| value.to_str().ok())
        != Some(CORE_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if device_id != "core-client-device" {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(vec![json!({
        "at": "2026-09-10T08:00:00Z",
        "temperature_c": 26.4,
        "humidity_pct": 51.0,
        "event_count": 1
    })]))
}

async fn create_command(
    State(state): State<Arc<TestState>>,
    headers: HeaderMap,
    Json(command): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    if headers
        .get("x-iot-nano-api-core-secret")
        .and_then(|value| value.to_str().ok())
        != Some(CORE_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.received.lock().await.push(command.clone());
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "id": command["id"],
            "device_id": command["device_id"],
            "state": "queued",
            "expires_at": command["expires_at"],
            "mode": command["mode"],
            "response": null,
            "responded_at": null
        })),
    ))
}

async fn get_command(
    State(_state): State<Arc<TestState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if headers
        .get("x-iot-nano-api-core-secret")
        .and_then(|value| value.to_str().ok())
        != Some(CORE_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(json!({
        "id": id,
        "device_id": "core-client-device",
        "state": "queued",
        "expires_at": "2026-09-10T08:05:00Z",
        "mode": "two_way",
        "response": null,
        "responded_at": null
    })))
}

async fn record_response(
    State(state): State<Arc<TestState>>,
    headers: HeaderMap,
    Json(response): Json<serde_json::Value>,
) -> Result<StatusCode, StatusCode> {
    if headers
        .get("x-iot-nano-api-core-secret")
        .and_then(|value| value.to_str().ok())
        != Some(CORE_SECRET)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.received.lock().await.push(response);
    Ok(StatusCode::NO_CONTENT)
}
