use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use iot_storage::{
    CommandOutboxState, CommandRepository, NewCommandOutboxEntry, PlatformStore,
    PlatformStoreError, TelemetryRepository, TopologyRepository,
};
use sqlx::{PgPool, Row};

fn command(device_id: &str, id: &str, params: &str) -> NewCommandOutboxEntry {
    let now = Utc::now();
    NewCommandOutboxEntry {
        id: id.to_owned(),
        device_id: device_id.to_owned(),
        method: "switch_on".to_owned(),
        params: params.to_owned(),
        mode: RpcMode::OneWay,
        expires_at: now + Duration::minutes(5),
        next_attempt_at: now,
    }
}

fn assert_unknown_device<T>(result: Result<T, PlatformStoreError>, expected_device_id: &str) {
    match result {
        Err(PlatformStoreError::UnknownDevice(device_id)) => {
            assert_eq!(device_id, expected_device_id);
        }
        _ => panic!("expected UnknownDevice({expected_device_id:?})"),
    }
}

fn telemetry(device_id: &str, sequence: u64) -> TelemetryEvent {
    TelemetryEvent {
        schema_version: 1,
        device_id: device_id.to_owned(),
        boot_id: uuid::Uuid::now_v7(),
        sequence,
        event_at: Utc::now(),
        measurements: [("temperature_c".to_owned(), serde_json::json!(23.5))]
            .into_iter()
            .collect(),
        gateway_device_id: None,
    }
}

#[tokio::test]
async fn platform_store_opens_the_complete_sqlite_platform_schema() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = PlatformStore::open(&configuration).await.unwrap();
    drop(store);

    let store = PlatformStore::open(&configuration).await.unwrap();

    let pool = store.sqlite_pool().unwrap();
    let tables = sqlx::query(
        "SELECT name
         FROM sqlite_master
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("name"))
    .collect::<Vec<_>>();

    assert_eq!(
        tables,
        [
            "alert_incidents",
            "alert_rule_event_evaluations",
            "alert_rules",
            "api_access_tokens",
            "asset_profiles",
            "assets",
            "audit_events",
            "command_outbox",
            "device_claim_codes",
            "device_profiles",
            "device_tokens",
            "devices",
            "gateway_event_receipts",
            "notification_outbox",
            "resource_shares",
            "telemetry",
            "telemetry_rollups_1h",
            "telemetry_rollups_5m",
            "user_app_grants",
            "users",
        ]
    );
}

#[tokio::test]
async fn platform_store_enqueues_a_command_for_a_registered_sqlite_device() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();
    let now = Utc::now();
    let command = CommandRepository::enqueue_command(
        &store,
        NewCommandOutboxEntry {
            id: uuid::Uuid::now_v7().to_string(),
            device_id: "platform-command-device".to_owned(),
            method: "switch_on".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::minutes(5),
            next_attempt_at: now,
        },
    )
    .await
    .unwrap();

    assert_eq!(command.state, CommandOutboxState::Queued);
    assert_eq!(command.device_id, "platform-command-device");
}

#[tokio::test]
async fn platform_store_canonicalizes_sqlite_command_values() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let canonical_id = uuid::Uuid::now_v7();
    let canonical_params = r#"{"target":"on"}"#;
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let command = CommandRepository::enqueue_command(
        &store,
        command(
            "platform-command-device",
            &canonical_id.to_string().to_uppercase(),
            "{\n  \"target\": \"on\"\n}",
        ),
    )
    .await
    .unwrap();

    assert_eq!(command.id, canonical_id.to_string());
    assert_eq!(command.params, canonical_params);

    let persisted = sqlx::query("SELECT id, params FROM command_outbox")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(persisted.get::<String, _>("id"), canonical_id.to_string());
    assert_eq!(persisted.get::<String, _>("params"), canonical_params);
}

#[tokio::test]
async fn platform_store_rejects_malformed_sqlite_commands_before_persistence() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let invalid_id = CommandRepository::enqueue_command(
        &store,
        command("platform-command-device", "not-a-uuid", "{}"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_id,
        PlatformStoreError::InvalidCommandId(ref id) if id == "not-a-uuid"
    ));

    let invalid_params = CommandRepository::enqueue_command(
        &store,
        command(
            "platform-command-device",
            &uuid::Uuid::now_v7().to_string(),
            "{not-json}",
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_params,
        PlatformStoreError::InvalidCommandParams
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn platform_store_rejects_unknown_sqlite_devices_for_commands_and_telemetry() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let device_id = "unknown-device";

    assert_unknown_device(
        CommandRepository::enqueue_command(
            &store,
            command(device_id, &uuid::Uuid::now_v7().to_string(), "{}"),
        )
        .await,
        device_id,
    );
    assert_unknown_device(
        TelemetryRepository::write_telemetry(
            &store,
            &telemetry(device_id, 1),
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await,
        device_id,
    );

    let device_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let telemetry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(device_count, 0);
    assert_eq!(telemetry_count, 0);
}

#[tokio::test]
async fn platform_store_persists_idempotent_sqlite_telemetry_via_the_repository_port() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let event = telemetry("platform-telemetry-device", 1);
    TopologyRepository::register_device(&store, &event.device_id)
        .await
        .unwrap();

    assert!(
        TelemetryRepository::write_telemetry(
            &store,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await
        .unwrap()
    );
    assert!(
        !TelemetryRepository::write_telemetry(
            &store,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn platform_store_rejects_overflowing_sqlite_telemetry_sequences() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let event = telemetry("platform-telemetry-device", (i64::MAX as u64) + 1);
    TopologyRepository::register_device(&store, &event.device_id)
        .await
        .unwrap();

    let result = TelemetryRepository::write_telemetry(
        &store,
        &event,
        Utc::now(),
        "iot/v1/devices/telemetry",
    )
    .await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::TelemetrySequenceOverflow)
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_persists_idempotent_timescale_telemetry_via_the_repository_port() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let event = telemetry("platform-telemetry-device", 1);
    TopologyRepository::register_device(&store, &event.device_id)
        .await
        .unwrap();

    assert!(
        TelemetryRepository::write_telemetry(
            &store,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await
        .unwrap()
    );
    assert!(
        !TelemetryRepository::write_telemetry(
            &store,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_enqueues_a_command_for_a_registered_timescale_device() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();
    let now = Utc::now();
    let command = CommandRepository::enqueue_command(
        &store,
        NewCommandOutboxEntry {
            id: uuid::Uuid::now_v7().to_string(),
            device_id: "platform-command-device".to_owned(),
            method: "switch_on".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::minutes(5),
            next_attempt_at: now,
        },
    )
    .await
    .unwrap();

    assert_eq!(command.state, CommandOutboxState::Queued);
    assert_eq!(command.device_id, "platform-command-device");
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_canonicalizes_timescale_command_values() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let canonical_id = uuid::Uuid::now_v7();
    let canonical_params = r#"{"target":"on"}"#;
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let command = CommandRepository::enqueue_command(
        &store,
        command(
            "platform-command-device",
            &canonical_id.to_string().to_uppercase(),
            "{\n  \"target\": \"on\"\n}",
        ),
    )
    .await
    .unwrap();

    assert_eq!(command.id, canonical_id.to_string());
    assert_eq!(command.params, canonical_params);

    let persisted = sqlx::query("SELECT id::text AS id, params FROM command_outbox")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(persisted.get::<String, _>("id"), canonical_id.to_string());
    assert_eq!(
        persisted
            .get::<sqlx::types::Json<serde_json::Value>, _>("params")
            .0,
        serde_json::from_str::<serde_json::Value>(canonical_params).unwrap()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_migrates_timescale_into_the_iot_nano_schema() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    let pool = store.timescale_pool().unwrap();
    let telemetry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(telemetry_count, 0);

    let tables = sqlx::query(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano'
           AND table_name IN (
             'users',
             'devices',
             'telemetry',
             'alert_rules',
             'notification_outbox',
             'command_outbox'
           )
         ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("table_name"))
    .collect::<Vec<_>>();

    assert_eq!(
        tables,
        [
            "alert_rules",
            "command_outbox",
            "devices",
            "notification_outbox",
            "telemetry",
            "users",
        ]
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_rejects_malformed_timescale_commands_before_persistence() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let invalid_id = CommandRepository::enqueue_command(
        &store,
        command("platform-command-device", "not-a-uuid", "{}"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_id,
        PlatformStoreError::InvalidCommandId(ref id) if id == "not-a-uuid"
    ));

    let invalid_params = CommandRepository::enqueue_command(
        &store,
        command(
            "platform-command-device",
            &uuid::Uuid::now_v7().to_string(),
            "{not-json}",
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_params,
        PlatformStoreError::InvalidCommandParams
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_rejects_unknown_timescale_devices_for_commands_and_telemetry() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let device_id = "unknown-device";

    assert_unknown_device(
        CommandRepository::enqueue_command(
            &store,
            command(device_id, &uuid::Uuid::now_v7().to_string(), "{}"),
        )
        .await,
        device_id,
    );
    assert_unknown_device(
        TelemetryRepository::write_telemetry(
            &store,
            &telemetry(device_id, 1),
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await,
        device_id,
    );

    let device_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap();
    let telemetry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(device_count, 0);
    assert_eq!(telemetry_count, 0);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_rejects_overflowing_timescale_telemetry_sequences() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let cleanup = PgPool::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&cleanup)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    cleanup.close().await;

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let event = telemetry("platform-telemetry-device", (i64::MAX as u64) + 1);
    TopologyRepository::register_device(&store, &event.device_id)
        .await
        .unwrap();

    let result = TelemetryRepository::write_telemetry(
        &store,
        &event,
        Utc::now(),
        "iot/v1/devices/telemetry",
    )
    .await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::TelemetrySequenceOverflow)
    ));
}
