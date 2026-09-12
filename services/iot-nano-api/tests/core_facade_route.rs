use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use iot_api::{
    CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest, CoreFacade,
    CoreFacadeError, CoreTelemetryBucket, CoreTelemetryPoint, CoreTelemetryQuery, SqliteApiState,
    bootstrap_users_sqlite, sqlite_router,
};
use iot_core::{
    DatabaseStorage, RpcMode, StorageConfiguration, generate_device_token, hash_device_token,
};
use iot_storage::SqliteStore;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

const TRANSPORT_SECRET: &str = "transport-secret-must-have-at-least-32";

#[derive(Default)]
struct CoreCalls {
    created: Vec<CoreCommandCreateRequest>,
    recorded_responses: Vec<CoreCommandResponseRequest>,
    telemetry_queries: Vec<CoreTelemetryQuery>,
}

#[derive(Clone, Default)]
struct RecordingCoreFacade {
    calls: Arc<Mutex<CoreCalls>>,
}

impl RecordingCoreFacade {
    fn calls(&self) -> std::sync::MutexGuard<'_, CoreCalls> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl CoreFacade for RecordingCoreFacade {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            let record = CoreCommandRecord {
                id: request.id,
                device_id: request.device_id.clone(),
                state: "queued".to_owned(),
                expires_at: request.expires_at,
                mode: request.mode,
                response: None,
                responded_at: None,
            };
            calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .created
                .push(request);
            Ok(record)
        })
    }

    fn get_command(
        &self,
        _id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        Box::pin(async { Err(CoreFacadeError::NotFound) })
    }

    fn record_command_response(
        &self,
        request: CoreCommandResponseRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreFacadeError>> + Send + '_>> {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .recorded_responses
                .push(request);
            Ok(())
        })
    }

    fn telemetry(
        &self,
        query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>
    {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .telemetry_queries
                .push(query);
            Ok(vec![CoreTelemetryPoint {
                at: Utc.with_ymd_and_hms(2026, 9, 13, 10, 0, 0).unwrap(),
                temperature_c: Some(24.5),
                humidity_pct: Some(48.0),
                event_count: 3,
            }])
        })
    }
}

async fn sqlite_store(path: std::path::PathBuf) -> SqliteStore {
    SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap()
}

async fn login_session(app: &axum::Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"username": "admin", "password": "NanoAdmin@1234"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    serde_json::from_slice::<serde_json::Value>(&body).unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn sqlite_routes_delegate_command_response_and_telemetry_to_core_facade() {
    let directory = tempfile::tempdir().unwrap();
    let store = sqlite_store(directory.path().join("api.db")).await;
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('facade-device')")
        .execute(store.pool())
        .await
        .unwrap();
    let token = generate_device_token();
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'facade-device', ?, ?)",
    )
    .bind(token_id.to_string())
    .bind(&token[..16])
    .bind(hash_device_token(&token).unwrap())
    .execute(store.pool())
    .await
    .unwrap();

    let facade = Arc::new(RecordingCoreFacade::default());
    let app = sqlite_router(
        SqliteApiState::new(store)
            .with_mqttd_device_transport_secret(TRANSPORT_SECRET)
            .with_core_facade(facade.clone()),
    );
    let session_id = login_session(&app).await;

    let command_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/devices/facade-device/commands")
                .header(header::AUTHORIZATION, format!("Session {session_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "method": "setRelay",
                        "params": {"enabled": true},
                        "mode": "two_way"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(command_response.status(), StatusCode::ACCEPTED);
    let command_body = to_bytes(command_response.into_body(), 16 * 1024)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&command_body).unwrap()["state"],
        "queued"
    );

    let command_id = facade.calls().created[0].id;
    let response_status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/mqttd/rpc-response")
                .header("x-iot-nano-mqttd-api-secret", TRANSPORT_SECRET)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "command_id": command_id,
                        "device_id": "facade-device",
                        "token_id": token_id,
                        "response": {"ok": true}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_status.status(), StatusCode::NO_CONTENT);

    let telemetry_response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/api/devices/facade-device/telemetry?from=2026-09-13T09%3A00%3A00Z&to=2026-09-13T11%3A00%3A00Z&bucket=raw",
                )
                .header(header::AUTHORIZATION, format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(telemetry_response.status(), StatusCode::OK);
    let telemetry_body = to_bytes(telemetry_response.into_body(), 16 * 1024)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&telemetry_body).unwrap(),
        json!([{
            "at": "2026-09-13T10:00:00Z",
            "temperature_c": 24.5,
            "humidity_pct": 48.0,
            "event_count": 3
        }])
    );

    let calls = facade.calls();
    assert_eq!(calls.created.len(), 1);
    assert_eq!(calls.created[0].device_id, "facade-device");
    assert_eq!(calls.created[0].method, "setRelay");
    assert_eq!(calls.created[0].params, json!({"enabled": true}));
    assert_eq!(calls.created[0].mode, RpcMode::TwoWay);
    assert_eq!(calls.recorded_responses.len(), 1);
    assert_eq!(calls.recorded_responses[0].command_id, command_id);
    assert_eq!(calls.recorded_responses[0].device_id, "facade-device");
    assert_eq!(calls.recorded_responses[0].response, json!({"ok": true}));
    assert_eq!(calls.telemetry_queries.len(), 1);
    assert_eq!(calls.telemetry_queries[0].device_id, "facade-device");
    assert_eq!(calls.telemetry_queries[0].bucket, CoreTelemetryBucket::Raw);
    assert_eq!(
        calls.telemetry_queries[0].from,
        Utc.with_ymd_and_hms(2026, 9, 13, 9, 0, 0).unwrap()
    );
    assert_eq!(
        calls.telemetry_queries[0].to,
        Utc.with_ymd_and_hms(2026, 9, 13, 11, 0, 0).unwrap()
    );
}
