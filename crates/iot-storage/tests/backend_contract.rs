use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use iot_storage::{
    CommandLifecycleRepository, CommandOutboxState, CommandRepository, NewCommandOutboxEntry,
    PlatformStore, PlatformStoreError, TelemetryRepository, TopologyRepository,
};
use sqlx::{Connection, PgConnection, PgPool, Row};

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

fn assert_command_conflict<T>(result: Result<T, PlatformStoreError>, expected_id: &str) {
    match result {
        Err(error) => assert_eq!(
            error.to_string(),
            format!("command payload conflicts with existing command ID: {expected_id:?}")
        ),
        Ok(_) => panic!("expected command conflict for {expected_id:?}"),
    }
}

fn assert_foreign_key_violation(error: sqlx::Error) {
    assert_eq!(
        error
            .as_database_error()
            .and_then(|database_error| database_error.code())
            .as_deref(),
        Some("23503")
    );
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

fn lifecycle_command(id: uuid::Uuid) -> NewCommandOutboxEntry {
    command("lifecycle-device", &id.to_string(), "{}")
}

async fn exercise_command_lifecycle(store: &PlatformStore, token_id: uuid::Uuid) {
    let now = Utc::now() + Duration::seconds(1);

    let published_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(published_id))
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    let published =
        CommandLifecycleRepository::mark_command_published(store, published_id, now).await;
    assert_eq!(
        published.unwrap().unwrap().state,
        CommandOutboxState::PublishedToBroker
    );
    assert!(
        CommandLifecycleRepository::mark_command_published(store, published_id, now)
            .await
            .unwrap()
            .is_none()
    );

    let failed_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(failed_id))
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    let failed =
        CommandLifecycleRepository::mark_command_failed(store, failed_id, "broker unavailable")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(failed.state, CommandOutboxState::Failed);
    assert!(
        CommandLifecycleRepository::mark_command_published(store, failed_id, now)
            .await
            .unwrap()
            .is_none()
    );

    let retry_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(retry_id))
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    let retried = CommandLifecycleRepository::release_command_for_retry(
        store,
        retry_id,
        "temporary broker failure",
        now + Duration::seconds(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(retried.state, CommandOutboxState::Queued);
    assert_eq!(retried.lease_until, None);

    let reclaimed_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(reclaimed_id))
        .await
        .unwrap();
    let initially_leased =
        CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 10)
            .await
            .unwrap();
    assert_eq!(
        initially_leased
            .iter()
            .find(|record| record.id == reclaimed_id.to_string())
            .unwrap()
            .attempt_count,
        1
    );
    let reclaimed = CommandLifecycleRepository::claim_commands(
        store,
        now + Duration::seconds(30),
        now + Duration::minutes(1),
        10,
    )
    .await
    .unwrap();
    let reclaimed = reclaimed
        .iter()
        .find(|record| record.id == reclaimed_id.to_string())
        .unwrap();
    assert_eq!(reclaimed.state, CommandOutboxState::Leased);
    assert_eq!(reclaimed.attempt_count, 2);

    let response_id = uuid::Uuid::now_v7();
    let mut response_command = lifecycle_command(response_id);
    response_command.mode = RpcMode::TwoWay;
    CommandRepository::enqueue_command(store, response_command)
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    CommandLifecycleRepository::mark_command_published(store, response_id, now)
        .await
        .unwrap()
        .unwrap();
    let mismatched_token_id = uuid::Uuid::now_v7();
    assert_eq!(
        CommandLifecycleRepository::mark_command_responded(
            store,
            response_id,
            "lifecycle-device",
            mismatched_token_id,
            r#"{"ok":false}"#,
            now,
        )
        .await
        .unwrap(),
        None
    );
    assert!(matches!(
        CommandLifecycleRepository::mark_command_responded(
            store,
            response_id,
            "lifecycle-device",
            token_id,
            "{not-json}",
            now,
        )
        .await,
        Err(PlatformStoreError::InvalidCommandParams)
    ));
    let responded = CommandLifecycleRepository::mark_command_responded(
        store,
        response_id,
        "lifecycle-device",
        token_id,
        r#"{ "ok": true }"#,
        now,
    )
    .await
    .unwrap()
    .unwrap();
    let response_retry = CommandLifecycleRepository::mark_command_responded(
        store,
        response_id,
        "lifecycle-device",
        token_id,
        r#"{"ok":true}"#,
        now + Duration::seconds(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response_retry, responded);

    let mut queued_expired = lifecycle_command(uuid::Uuid::now_v7());
    queued_expired.expires_at = now - Duration::seconds(1);
    queued_expired.next_attempt_at = now - Duration::seconds(2);
    CommandRepository::enqueue_command(store, queued_expired)
        .await
        .unwrap();

    let leased_id = uuid::Uuid::now_v7();
    let mut leased_expired = lifecycle_command(leased_id);
    leased_expired.expires_at = now + Duration::seconds(1);
    CommandRepository::enqueue_command(store, leased_expired)
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 10)
        .await
        .unwrap();

    let two_way_expired_id = uuid::Uuid::now_v7();
    let mut two_way_expired = lifecycle_command(two_way_expired_id);
    two_way_expired.mode = RpcMode::TwoWay;
    two_way_expired.expires_at = now + Duration::seconds(1);
    CommandRepository::enqueue_command(store, two_way_expired)
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    CommandLifecycleRepository::mark_command_published(store, two_way_expired_id, now)
        .await
        .unwrap()
        .unwrap();

    let expired = CommandLifecycleRepository::expire_commands(store, now + Duration::seconds(2))
        .await
        .unwrap();
    assert_eq!(expired.len(), 3);
    assert!(
        expired
            .iter()
            .all(|record| record.state == CommandOutboxState::Expired)
    );
    assert_eq!(
        CommandLifecycleRepository::mark_command_responded(
            store,
            two_way_expired_id,
            "lifecycle-device",
            token_id,
            r#"{"ok":true}"#,
            now + Duration::seconds(2),
        )
        .await
        .unwrap(),
        None
    );

    let revoked_id = uuid::Uuid::now_v7();
    let mut revoked_command = lifecycle_command(revoked_id);
    revoked_command.mode = RpcMode::TwoWay;
    CommandRepository::enqueue_command(store, revoked_command)
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(store, now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    CommandLifecycleRepository::mark_command_published(store, revoked_id, now)
        .await
        .unwrap()
        .unwrap();
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query("UPDATE device_tokens SET revoked_at = ? WHERE id = ?")
                .bind(now.to_rfc3339())
                .bind(token_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query("UPDATE device_tokens SET revoked_at = $1 WHERE id = $2")
                .bind(now)
                .bind(token_id)
                .execute(pool)
                .await
                .unwrap();
        }
    }
    assert_eq!(
        CommandLifecycleRepository::mark_command_responded(
            store,
            revoked_id,
            "lifecycle-device",
            token_id,
            r#"{"ok":true}"#,
            now,
        )
        .await
        .unwrap(),
        None
    );
}

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_test_store() -> (TimescaleTestLock, PlatformStore) {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:backend-contract-test'))")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut connection)
        .await
        .unwrap();

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
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
async fn platform_store_returns_the_original_sqlite_command_for_an_idempotent_retry() {
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
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let request = command(
        "platform-command-device",
        &canonical_id.to_string().to_uppercase(),
        "{\n  \"target\": \"on\"\n}",
    );
    let created = CommandRepository::enqueue_command(&store, request.clone())
        .await
        .unwrap();
    let retried = CommandRepository::enqueue_command(&store, request)
        .await
        .unwrap();

    assert_eq!(retried, created);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn platform_store_rejects_a_conflicting_sqlite_command_retry() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let command_id = uuid::Uuid::now_v7().to_string();
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let original = command("platform-command-device", &command_id, r#"{"target":"on"}"#);
    CommandRepository::enqueue_command(&store, original.clone())
        .await
        .unwrap();
    let mut conflicting = original;
    conflicting.params = r#"{"target":"off"}"#.to_owned();

    assert_command_conflict(
        CommandRepository::enqueue_command(&store, conflicting).await,
        &command_id,
    );
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
async fn platform_store_command_lifecycle_sqlite_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    TopologyRepository::register_device(&store, "lifecycle-device")
        .await
        .unwrap();
    let token_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, ?, ?, ?)",
    )
    .bind(token_id.to_string())
    .bind("lifecycle-device")
    .bind("lifecycle-token-prefix")
    .bind("unused")
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    exercise_command_lifecycle(&store, token_id).await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_command_lifecycle_timescale_contract() {
    let (_test_lock, store) = timescale_test_store().await;
    TopologyRepository::register_device(&store, "lifecycle-device")
        .await
        .unwrap();
    let token_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(token_id)
    .bind("lifecycle-device")
    .bind("lifecycle-token-prefix")
    .bind("unused")
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    exercise_command_lifecycle(&store, token_id).await;
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
    let (_test_lock, store) = timescale_test_store().await;
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
    let (_test_lock, store) = timescale_test_store().await;

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
async fn platform_store_returns_the_original_timescale_command_for_an_idempotent_retry() {
    let (_test_lock, store) = timescale_test_store().await;
    let canonical_id = uuid::Uuid::now_v7();
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let request = command(
        "platform-command-device",
        &canonical_id.to_string().to_uppercase(),
        "{\n  \"target\": \"on\"\n}",
    );
    let created = CommandRepository::enqueue_command(&store, request.clone())
        .await
        .unwrap();
    let retried = CommandRepository::enqueue_command(&store, request)
        .await
        .unwrap();

    assert_eq!(retried, created);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_rejects_a_conflicting_timescale_command_retry() {
    let (_test_lock, store) = timescale_test_store().await;
    let command_id = uuid::Uuid::now_v7().to_string();
    TopologyRepository::register_device(&store, "platform-command-device")
        .await
        .unwrap();

    let original = command("platform-command-device", &command_id, r#"{"target":"on"}"#);
    CommandRepository::enqueue_command(&store, original.clone())
        .await
        .unwrap();
    let mut conflicting = original;
    conflicting.params = r#"{"target":"off"}"#.to_owned();

    assert_command_conflict(
        CommandRepository::enqueue_command(&store, conflicting).await,
        &command_id,
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_canonicalizes_timescale_command_values() {
    let (_test_lock, store) = timescale_test_store().await;
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
    let (_test_lock, store) = timescale_test_store().await;

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
async fn platform_store_timescale_schema_enforces_device_ownership() {
    let (_test_lock, store) = timescale_test_store().await;
    let pool = store.timescale_pool().unwrap();

    let constraints = sqlx::query(
        "SELECT relation.relname AS table_name, constraint_row.conname,
                constraint_row.confdeltype::text AS confdeltype
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
         WHERE constraint_row.conname IN (
            'command_outbox_device_id_fkey',
            'device_runtime_state_device_id_fkey',
            'telemetry_device_id_fkey'
         )
         ORDER BY relation.relname",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("table_name"),
            row.get::<String, _>("conname"),
            row.get::<String, _>("confdeltype"),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(
        constraints,
        [
            (
                "command_outbox".to_owned(),
                "command_outbox_device_id_fkey".to_owned(),
                "c".to_owned(),
            ),
            (
                "device_runtime_state".to_owned(),
                "device_runtime_state_device_id_fkey".to_owned(),
                "c".to_owned(),
            ),
            (
                "telemetry".to_owned(),
                "telemetry_device_id_fkey".to_owned(),
                "a".to_owned(),
            ),
        ]
    );

    let missing_device = "missing-device";
    let command_error = sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, 'switch_on', '{}'::jsonb, 'one_way', now(), now())",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(missing_device)
    .execute(pool)
    .await
    .unwrap_err();
    assert_foreign_key_violation(command_error);

    let telemetry_error = sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES (now(), now(), $1, $2, 1, '{}'::jsonb, 'iot/v1/devices/telemetry')",
    )
    .bind(missing_device)
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await
    .unwrap_err();
    assert_foreign_key_violation(telemetry_error);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_repairs_legacy_timescale_device_ownership_foreign_keys() {
    let (_test_lock, store) = timescale_test_store().await;
    let pool = store.timescale_pool().unwrap();

    for statement in [
        "ALTER TABLE command_outbox DROP CONSTRAINT command_outbox_device_id_fkey",
        "ALTER TABLE device_runtime_state DROP CONSTRAINT device_runtime_state_device_id_fkey",
        "ALTER TABLE telemetry DROP CONSTRAINT telemetry_device_id_fkey",
    ] {
        sqlx::query(statement).execute(pool).await.unwrap();
    }
    drop(store);

    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
    let repaired_store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let repaired_pool = repaired_store.timescale_pool().unwrap();

    let constraints = sqlx::query(
        "SELECT relation.relname AS table_name, constraint_row.conname,
                constraint_row.confdeltype::text AS confdeltype
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
         WHERE constraint_row.conname IN (
            'command_outbox_device_id_fkey',
            'device_runtime_state_device_id_fkey',
            'telemetry_device_id_fkey'
         )
         ORDER BY relation.relname",
    )
    .fetch_all(repaired_pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("table_name"),
            row.get::<String, _>("conname"),
            row.get::<String, _>("confdeltype"),
        )
    })
    .collect::<Vec<_>>();

    assert_eq!(
        constraints,
        [
            (
                "command_outbox".to_owned(),
                "command_outbox_device_id_fkey".to_owned(),
                "c".to_owned(),
            ),
            (
                "device_runtime_state".to_owned(),
                "device_runtime_state_device_id_fkey".to_owned(),
                "c".to_owned(),
            ),
            (
                "telemetry".to_owned(),
                "telemetry_device_id_fkey".to_owned(),
                "a".to_owned(),
            ),
        ]
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_timescale_serializes_device_deletion_with_command_and_telemetry_writes() {
    let (_test_lock, store) = timescale_test_store().await;
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
    let deletion_pool = PgPool::connect(&database_url).await.unwrap();

    let command_device_id = "command-race-device";
    TopologyRepository::register_device(&store, command_device_id)
        .await
        .unwrap();
    let mut command_deletion = deletion_pool.begin().await.unwrap();
    sqlx::query("SELECT device_id FROM iot_nano.devices WHERE device_id = $1 FOR UPDATE")
        .bind(command_device_id)
        .execute(&mut *command_deletion)
        .await
        .unwrap();
    let command_store = store.clone();
    let command_id = uuid::Uuid::now_v7().to_string();
    let command_writer = tokio::spawn(async move {
        CommandRepository::enqueue_command(
            &command_store,
            command(command_device_id, &command_id, r#"{"target":"on"}"#),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !command_writer.is_finished(),
        "command write must wait for the device deletion lock"
    );
    sqlx::query("DELETE FROM iot_nano.devices WHERE device_id = $1")
        .bind(command_device_id)
        .execute(&mut *command_deletion)
        .await
        .unwrap();
    command_deletion.commit().await.unwrap();
    assert_unknown_device(command_writer.await.unwrap(), command_device_id);

    let telemetry_device_id = "telemetry-race-device";
    TopologyRepository::register_device(&store, telemetry_device_id)
        .await
        .unwrap();
    let mut telemetry_deletion = deletion_pool.begin().await.unwrap();
    sqlx::query("SELECT device_id FROM iot_nano.devices WHERE device_id = $1 FOR UPDATE")
        .bind(telemetry_device_id)
        .execute(&mut *telemetry_deletion)
        .await
        .unwrap();
    let telemetry_store = store.clone();
    let event = telemetry(telemetry_device_id, 1);
    let telemetry_writer = tokio::spawn(async move {
        TelemetryRepository::write_telemetry(
            &telemetry_store,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !telemetry_writer.is_finished(),
        "telemetry write must wait for the device deletion lock"
    );
    sqlx::query("DELETE FROM iot_nano.devices WHERE device_id = $1")
        .bind(telemetry_device_id)
        .execute(&mut *telemetry_deletion)
        .await
        .unwrap();
    telemetry_deletion.commit().await.unwrap();
    assert_unknown_device(telemetry_writer.await.unwrap(), telemetry_device_id);

    let command_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox WHERE device_id = $1")
            .bind(command_device_id)
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    let telemetry_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telemetry WHERE device_id = $1")
            .bind(telemetry_device_id)
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(command_count, 0);
    assert_eq!(telemetry_count, 0);
    deletion_pool.close().await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_rejects_malformed_timescale_commands_before_persistence() {
    let (_test_lock, store) = timescale_test_store().await;
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
    let (_test_lock, store) = timescale_test_store().await;
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
    let (_test_lock, store) = timescale_test_store().await;
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
