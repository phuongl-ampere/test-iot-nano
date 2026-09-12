use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use iot_api::{
    CoreClient, CoreCommandCreateRequest, SqliteApiState, bootstrap_users_sqlite, sqlite_router,
};
use iot_core::{
    DatabaseStorage, RpcMode, StorageConfiguration, generate_device_token, hash_device_token,
};
use iot_nano_core::{CoreControlState, core_control_router};
use iot_storage::SqliteStore;
use serde_json::json;
use sqlx::Row;
use tower::ServiceExt;
use uuid::Uuid;

const CORE_SECRET: &str = "core-control-secret-must-have-at-least-32";
const TRANSPORT_SECRET: &str = "transport-secret-must-have-at-least-32";

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

#[tokio::test]
async fn public_api_creates_commands_in_the_separate_core_database() {
    let directory = tempfile::tempdir().unwrap();
    let api_store = sqlite_store(directory.path().join("api.db")).await;
    let core_store = sqlite_store(directory.path().join("core.db")).await;
    bootstrap_users_sqlite(api_store.pool()).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('api-core-device')")
        .execute(api_store.pool())
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let core_app =
        core_control_router(CoreControlState::sqlite(core_store.clone(), CORE_SECRET).unwrap());
    let core_server = tokio::spawn(async move {
        axum::serve(listener, core_app).await.unwrap();
    });
    let core_client = CoreClient::new(format!("http://{address}"), CORE_SECRET).unwrap();
    let app = sqlite_router(SqliteApiState::new(api_store.clone()).with_core_client(core_client));

    let login = Request::builder()
        .method("POST")
        .uri("/api/auth/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"username": "admin", "password": "NanoAdmin@1234"}).to_string(),
        ))
        .unwrap();
    let login = app.clone().oneshot(login).await.unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let login_body = to_bytes(login.into_body(), 16 * 1024).await.unwrap();
    let session_id =
        serde_json::from_slice::<serde_json::Value>(&login_body).unwrap()["session_id"]
            .as_str()
            .unwrap()
            .to_owned();

    let request = Request::builder()
        .method("POST")
        .uri("/api/devices/api-core-device/commands")
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
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let command_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let api_count = sqlx::query("SELECT COUNT(*) AS count FROM command_outbox")
        .fetch_one(api_store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
    let core_count = sqlx::query("SELECT COUNT(*) AS count FROM command_outbox WHERE id = ?")
        .bind(command_id)
        .fetch_one(core_store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(api_count, 0);
    assert_eq!(core_count, 1);
    core_server.abort();
}

#[tokio::test]
async fn transport_response_is_authorized_by_api_and_recorded_in_core() {
    let directory = tempfile::tempdir().unwrap();
    let api_store = sqlite_store(directory.path().join("api.db")).await;
    let core_store = sqlite_store(directory.path().join("core.db")).await;
    sqlx::query("INSERT INTO devices (device_id) VALUES ('response-device')")
        .execute(api_store.pool())
        .await
        .unwrap();
    let token = generate_device_token();
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'response-device', ?, ?)",
    )
    .bind(token_id.to_string())
    .bind(&token[..16])
    .bind(hash_device_token(&token).unwrap())
    .execute(api_store.pool())
    .await
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let core_app =
        core_control_router(CoreControlState::sqlite(core_store.clone(), CORE_SECRET).unwrap());
    let core_server = tokio::spawn(async move {
        axum::serve(listener, core_app).await.unwrap();
    });
    let core_client = CoreClient::new(format!("http://{address}"), CORE_SECRET).unwrap();
    let command_id = Uuid::now_v7();
    let now = chrono::Utc::now();
    core_client
        .create(CoreCommandCreateRequest {
            id: command_id,
            device_id: "response-device".to_owned(),
            method: "setRelay".to_owned(),
            params: json!({"enabled": true}),
            mode: RpcMode::TwoWay,
            issued_at: now,
            expires_at: now + chrono::Duration::minutes(5),
        })
        .await
        .unwrap();
    assert_eq!(
        core_store
            .claim_commands(now, now + chrono::Duration::seconds(30), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        core_store
            .mark_command_published(&command_id.to_string(), now)
            .await
            .unwrap()
            .is_some()
    );

    let app = sqlite_router(
        SqliteApiState::new(api_store)
            .with_mqttd_device_transport_secret(TRANSPORT_SECRET)
            .with_core_client(core_client.clone()),
    );
    let request = Request::builder()
        .method("POST")
        .uri("/internal/mqttd/rpc-response")
        .header("x-iot-nano-mqttd-api-secret", TRANSPORT_SECRET)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "command_id": command_id,
                "device_id": "response-device",
                "token_id": token_id,
                "response": {"ok": true}
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let command = core_client.get(command_id).await.unwrap();
    assert_eq!(command.state, "responded");
    assert_eq!(command.response, Some(json!({"ok": true})));
    core_server.abort();
}
