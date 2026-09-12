use std::sync::Arc;

use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_nano_monolith::PlatformCommandResponse;
use iot_nano_mqttd::{CommandResponseError, CommandResponsePort, TransportRpcResponse};
use iot_storage::{NewCommandOutboxEntry, PlatformStore};
use uuid::Uuid;

const STORAGE_UNAVAILABLE: &str = "platform storage unavailable";

async fn fixture() -> (tempfile::TempDir, Arc<PlatformStore>, Uuid, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("platform.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    let token_id = Uuid::now_v7();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-1')")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'device-1', 'test-token', 'unused')",
    )
    .bind(token_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store, token_id, Uuid::now_v7())
}

async fn published_command(store: &PlatformStore, command_id: Uuid, now: chrono::DateTime<Utc>) {
    store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            device_id: "device-1".to_owned(),
            method: "device.read".to_owned(),
            params: r#"{"channel":"temperature"}"#.to_owned(),
            mode: RpcMode::TwoWay,
            expires_at: now + Duration::minutes(5),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    store
        .claim_commands(now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    store
        .mark_command_published(command_id, now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
}

fn response(command_id: Uuid, device_id: &str, token_id: Uuid) -> TransportRpcResponse {
    TransportRpcResponse {
        command_id,
        device_id: device_id.to_owned(),
        token_id,
        response: serde_json::json!({"ok": true, "value": 42}),
    }
}

#[tokio::test]
async fn records_command_response_and_accepts_matching_idempotent_repeat() {
    let (_directory, store, token_id, _other_token_id) = fixture().await;
    let command_id = Uuid::now_v7();
    published_command(&store, command_id, Utc::now()).await;
    let adapter = PlatformCommandResponse::new(store.clone());
    let response = response(command_id, "device-1", token_id);

    adapter.record_response(response.clone()).await.unwrap();
    adapter.record_response(response).await.unwrap();

    let row = sqlx::query("SELECT response, state FROM command_outbox WHERE id = ?")
        .bind(command_id.to_string())
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(
        sqlx::Row::try_get::<String, _>(&row, "response").unwrap(),
        r#"{"ok":true,"value":42}"#
    );
    assert_eq!(
        sqlx::Row::try_get::<String, _>(&row, "state").unwrap(),
        "responded"
    );
}

#[tokio::test]
async fn rejects_response_with_wrong_token_or_device_as_unavailable() {
    let (_directory, store, token_id, other_token_id) = fixture().await;
    let adapter = PlatformCommandResponse::new(store.clone());
    let command_id = Uuid::now_v7();
    published_command(&store, command_id, Utc::now()).await;

    for response in [
        response(command_id, "device-1", other_token_id),
        response(command_id, "other-device", token_id),
    ] {
        assert_eq!(
            adapter.record_response(response).await,
            Err(CommandResponseError::Unavailable(
                STORAGE_UNAVAILABLE.to_owned()
            ))
        );
    }
}
