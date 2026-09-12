use chrono::{TimeZone, Utc};
use iot_api::{ApiSqliteStore, connect_api_database, migrate_api};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_nano_core::CoreSqliteStore;
use serde_json::json;
use uuid::Uuid;

fn sqlite_configuration(path: std::path::PathBuf) -> StorageConfiguration {
    StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    }
}

#[tokio::test]
async fn api_and_core_reject_each_others_sqlite_files() {
    let directory = tempfile::tempdir().unwrap();

    let core_path = directory.path().join("core.db");
    let core = CoreSqliteStore::open(&sqlite_configuration(core_path.clone()))
        .await
        .unwrap();
    drop(core);
    assert!(
        ApiSqliteStore::open(&sqlite_configuration(core_path))
            .await
            .is_err()
    );

    let api_path = directory.path().join("api.db");
    let api = ApiSqliteStore::open(&sqlite_configuration(api_path.clone()))
        .await
        .unwrap();
    drop(api);
    assert!(
        CoreSqliteStore::open(&sqlite_configuration(api_path))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn core_storage_does_not_create_api_metadata_tables() {
    let directory = tempfile::tempdir().unwrap();
    let core = CoreSqliteStore::open(&sqlite_configuration(directory.path().join("core.db")))
        .await
        .unwrap();
    let users_table = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
    )
    .fetch_optional(core.pool())
    .await
    .unwrap();

    assert_eq!(users_table, None);
}

#[tokio::test]
async fn core_records_device_runtime_state_without_api_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let core = CoreSqliteStore::open(&sqlite_configuration(directory.path().join("core.db")))
        .await
        .unwrap();
    let event_at = Utc.with_ymd_and_hms(2026, 9, 11, 8, 0, 0).unwrap();
    core.write_telemetry(
        &TelemetryEvent {
            schema_version: 1,
            device_id: "device-a".to_owned(),
            boot_id: Uuid::now_v7(),
            sequence: 1,
            event_at,
            measurements: json!({"temperature_c": 26.4}).as_object().unwrap().clone(),
            gateway_device_id: None,
        },
        event_at,
        "iot/v1/devices/device-a/telemetry",
    )
    .await
    .unwrap();

    let last_seen_at = sqlx::query_scalar::<_, String>(
        "SELECT last_seen_at FROM device_runtime_state WHERE device_id = 'device-a'",
    )
    .fetch_one(core.pool())
    .await
    .unwrap();

    assert_eq!(last_seen_at, event_at.to_rfc3339());
}

#[tokio::test]
async fn api_storage_creates_metadata_without_core_tables() {
    let directory = tempfile::tempdir().unwrap();
    let api = ApiSqliteStore::open(&sqlite_configuration(directory.path().join("api.db")))
        .await
        .unwrap();
    let users_table = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
    )
    .fetch_optional(api.pool())
    .await
    .unwrap();
    let telemetry_table = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'telemetry'",
    )
    .fetch_optional(api.pool())
    .await
    .unwrap();

    assert_eq!(users_table.as_deref(), Some("users"));
    assert_eq!(telemetry_table, None);
}

#[tokio::test]
async fn api_database_uses_metadata_schema_without_core_tables() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let pool = connect_api_database(&database_url).await.unwrap();
    migrate_api(&pool).await.unwrap();

    let api_tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano_api'
           AND table_name IN ('users', 'devices', 'device_tokens')
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(api_tables, ["device_tokens", "devices", "users"]);
    assert!(
        sqlx::query("SELECT event_at FROM telemetry")
            .fetch_optional(&pool)
            .await
            .is_err()
    );
}
