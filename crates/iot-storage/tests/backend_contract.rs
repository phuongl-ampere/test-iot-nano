use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use iot_storage::{
    CommandLifecycleRepository, CommandOutboxState, CommandRepository, NewCommandOutboxEntry,
    NewTenant, NewTenantAccount, PlatformStore, PlatformStoreError, TelemetryRepository,
    TenantIdentityRepository, TopologyRepository,
};
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Row};

mod common;

fn command(device_id: &str, id: &str, params: &str) -> NewCommandOutboxEntry {
    let now = Utc::now();
    NewCommandOutboxEntry {
        id: id.to_owned(),
        tenant_id: test_tenant_id(),
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

fn assert_check_constraint_violation(error: sqlx::Error) {
    assert_eq!(
        error
            .as_database_error()
            .and_then(|database_error| database_error.code())
            .as_deref(),
        Some("23514")
    );
}

async fn sqlite_schema_sql(pool: &sqlx::SqlitePool, object_type: &str, name: &str) -> String {
    sqlx::query_scalar(
        "SELECT sql
         FROM sqlite_master
         WHERE type = ? AND name = ?",
    )
    .bind(object_type)
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap()
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

async fn create_tenant(store: &PlatformStore, slug: &str) -> uuid::Uuid {
    TenantIdentityRepository::create_tenant_with_account(
        store,
        NewTenant {
            slug: slug.to_owned(),
            metadata: serde_json::json!({}),
        },
        NewTenantAccount {
            password_hash: format!("{slug}-password-hash"),
        },
    )
    .await
    .unwrap()
    .0
    .id
}

fn test_tenant_id() -> uuid::Uuid {
    uuid::Uuid::from_u128(1)
}

async fn ensure_test_tenant(store: &PlatformStore) {
    let tenant_id = test_tenant_id();
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT OR IGNORE INTO tenants (id, slug, status, metadata)
                 VALUES (?, 'backend-contract', 'active', '{}')",
            )
            .bind(tenant_id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO tenants (id, slug, status, metadata)
                 VALUES ($1, 'backend-contract', 'active', '{}'::jsonb)
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind(tenant_id)
            .execute(pool)
            .await
            .unwrap();
        }
    }
}

async fn register_test_device(store: &PlatformStore, device_id: &str) {
    ensure_test_tenant(store).await;
    TopologyRepository::register_device(store, test_tenant_id(), device_id)
        .await
        .unwrap();
}

async fn write_test_telemetry(
    store: &PlatformStore,
    event: &TelemetryEvent,
    received_at: chrono::DateTime<Utc>,
    topic: &str,
) -> Result<bool, PlatformStoreError> {
    ensure_test_tenant(store).await;
    TelemetryRepository::write_telemetry(store, test_tenant_id(), event, received_at, topic).await
}

fn lifecycle_command(id: uuid::Uuid) -> NewCommandOutboxEntry {
    command("lifecycle-device", &id.to_string(), "{}")
}

async fn exercise_command_lifecycle(store: &PlatformStore, token_id: uuid::Uuid) {
    let tenant_id = test_tenant_id();
    let other_tenant_id = uuid::Uuid::from_u128(2);
    let now = Utc::now() + Duration::seconds(1);

    let published_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(published_id))
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    assert!(
        CommandLifecycleRepository::mark_command_published(
            store,
            other_tenant_id,
            published_id,
            now,
        )
        .await
        .unwrap()
        .is_none()
    );
    let published =
        CommandLifecycleRepository::mark_command_published(store, tenant_id, published_id, now)
            .await;
    assert_eq!(
        published.unwrap().unwrap().state,
        CommandOutboxState::PublishedToBroker
    );
    assert!(
        CommandLifecycleRepository::mark_command_published(store, tenant_id, published_id, now)
            .await
            .unwrap()
            .is_none()
    );

    let failed_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(failed_id))
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    let failed = CommandLifecycleRepository::mark_command_failed(
        store,
        tenant_id,
        failed_id,
        "broker unavailable",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(failed.state, CommandOutboxState::Failed);
    assert!(
        CommandLifecycleRepository::mark_command_published(store, tenant_id, failed_id, now)
            .await
            .unwrap()
            .is_none()
    );

    let retry_id = uuid::Uuid::now_v7();
    CommandRepository::enqueue_command(store, lifecycle_command(retry_id))
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    let retried = CommandLifecycleRepository::release_command_for_retry(
        store,
        tenant_id,
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
    let initially_leased = CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        10,
    )
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
        tenant_id,
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
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    CommandLifecycleRepository::mark_command_published(store, tenant_id, response_id, now)
        .await
        .unwrap()
        .unwrap();
    let mismatched_token_id = uuid::Uuid::now_v7();
    assert_eq!(
        CommandLifecycleRepository::mark_command_responded(
            store,
            tenant_id,
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
            tenant_id,
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
        tenant_id,
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
        tenant_id,
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
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        10,
    )
    .await
    .unwrap();

    let two_way_expired_id = uuid::Uuid::now_v7();
    let mut two_way_expired = lifecycle_command(two_way_expired_id);
    two_way_expired.mode = RpcMode::TwoWay;
    two_way_expired.expires_at = now + Duration::seconds(1);
    CommandRepository::enqueue_command(store, two_way_expired)
        .await
        .unwrap();
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        10,
    )
    .await
    .unwrap();
    CommandLifecycleRepository::mark_command_published(store, tenant_id, two_way_expired_id, now)
        .await
        .unwrap()
        .unwrap();

    let expired = CommandLifecycleRepository::expire_due_commands(
        store,
        tenant_id,
        now + Duration::seconds(2),
        2,
    )
    .await
    .unwrap();
    assert_eq!(expired.len(), 2);
    assert!(
        expired
            .iter()
            .all(|record| record.state == CommandOutboxState::Expired)
    );
    let expired = CommandLifecycleRepository::expire_due_commands(
        store,
        tenant_id,
        now + Duration::seconds(2),
        2,
    )
    .await
    .unwrap();
    assert_eq!(expired.len(), 1);
    assert!(
        expired
            .iter()
            .all(|record| record.state == CommandOutboxState::Expired)
    );
    assert_eq!(
        CommandLifecycleRepository::mark_command_responded(
            store,
            tenant_id,
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
    CommandLifecycleRepository::claim_commands(
        store,
        tenant_id,
        now,
        now + Duration::seconds(30),
        10,
    )
    .await
    .unwrap();
    CommandLifecycleRepository::mark_command_published(store, tenant_id, revoked_id, now)
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
            tenant_id,
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
    common::reset_timescale_schema(&mut connection)
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
            "application_redirect_uris",
            "applications",
            "asset_profiles",
            "assets",
            "audit_events",
            "command_outbox",
            "device_profiles",
            "device_relations",
            "device_tokens",
            "devices",
            "gateway_event_receipts",
            "notification_outbox",
            "oauth_access_tokens",
            "oauth_authorization_codes",
            "oauth_client_secrets",
            "resource_permissions",
            "system_accounts",
            "telemetry",
            "telemetry_rollups_1h",
            "telemetry_rollups_5m",
            "tenant_accounts",
            "tenants",
            "user_app_grants",
            "user_group_members",
            "user_groups",
            "users",
        ]
    );

    let legacy_tables = sqlx::query(
        "SELECT name
         FROM sqlite_master
         WHERE type = 'table'
           AND name IN ('resource_grants', 'resource_shares')
         ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert!(legacy_tables.is_empty());

    let user_groups_sql = sqlite_schema_sql(pool, "table", "user_groups").await;
    assert!(
        user_groups_sql
            .contains("tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT")
    );
    assert!(user_groups_sql.contains("UNIQUE (id, tenant_id)"));
    assert!(user_groups_sql.contains("FOREIGN KEY (owner_user_id, tenant_id)"));
    assert!(user_groups_sql.contains("REFERENCES users(id, tenant_id) ON DELETE RESTRICT"));

    let user_group_members_sql = sqlite_schema_sql(pool, "table", "user_group_members").await;
    assert!(
        user_group_members_sql
            .contains("tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT")
    );
    assert!(user_group_members_sql.contains("PRIMARY KEY (group_id, user_id)"));
    assert!(user_group_members_sql.contains("FOREIGN KEY (group_id, tenant_id)"));
    assert!(
        user_group_members_sql.contains("REFERENCES user_groups(id, tenant_id) ON DELETE CASCADE")
    );
    assert!(user_group_members_sql.contains("FOREIGN KEY (user_id, tenant_id)"));
    assert!(user_group_members_sql.contains("REFERENCES users(id, tenant_id) ON DELETE CASCADE"));

    let user_group_members_index =
        sqlite_schema_sql(pool, "index", "user_group_members_tenant_user_group_index").await;
    assert!(
        user_group_members_index.contains("ON user_group_members (tenant_id, user_id, group_id)")
    );

    let resource_permissions_sql = sqlite_schema_sql(pool, "table", "resource_permissions").await;
    assert!(
        resource_permissions_sql
            .contains("tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT")
    );
    assert!(
        resource_permissions_sql
            .contains("permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager'))")
    );
    assert!(resource_permissions_sql.contains(
        "inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1))"
    ));
    assert!(resource_permissions_sql.contains("revoked_at TEXT"));
    assert!(resource_permissions_sql.contains(
        "CHECK (
        (subject_user_id IS NOT NULL AND subject_group_id IS NULL)
        OR (subject_user_id IS NULL AND subject_group_id IS NOT NULL)
    )"
    ));
    assert!(resource_permissions_sql.contains(
        "CHECK (
        (asset_id IS NOT NULL AND device_id IS NULL)
        OR (asset_id IS NULL AND device_id IS NOT NULL)
    )"
    ));
    assert!(resource_permissions_sql.contains("CHECK (device_id IS NULL OR inherit_children = 0)"));
    assert!(resource_permissions_sql.contains("FOREIGN KEY (subject_user_id, tenant_id)"));
    assert!(resource_permissions_sql.contains("FOREIGN KEY (subject_group_id, tenant_id)"));
    assert!(resource_permissions_sql.contains("FOREIGN KEY (asset_id, tenant_id)"));
    assert!(resource_permissions_sql.contains("FOREIGN KEY (device_id, tenant_id)"));
    assert!(resource_permissions_sql.contains("FOREIGN KEY (created_by_user_id, tenant_id)"));

    let active_permission_indexes = sqlx::query(
        "SELECT name, sql
         FROM sqlite_master
         WHERE type = 'index'
           AND tbl_name = 'resource_permissions'
           AND name LIKE 'resource_permissions_active_%'
         ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| (row.get::<String, _>("name"), row.get::<String, _>("sql")))
    .collect::<Vec<_>>();
    let expected_active_permission_indexes = [
        (
            "resource_permissions_active_asset_group_index",
            "ON resource_permissions (tenant_id, asset_id, subject_group_id)",
            "WHERE revoked_at IS NULL AND asset_id IS NOT NULL AND subject_group_id IS NOT NULL",
        ),
        (
            "resource_permissions_active_asset_user_index",
            "ON resource_permissions (tenant_id, asset_id, subject_user_id)",
            "WHERE revoked_at IS NULL AND asset_id IS NOT NULL AND subject_user_id IS NOT NULL",
        ),
        (
            "resource_permissions_active_device_group_index",
            "ON resource_permissions (tenant_id, device_id, subject_group_id)",
            "WHERE revoked_at IS NULL AND device_id IS NOT NULL AND subject_group_id IS NOT NULL",
        ),
        (
            "resource_permissions_active_device_user_index",
            "ON resource_permissions (tenant_id, device_id, subject_user_id)",
            "WHERE revoked_at IS NULL AND device_id IS NOT NULL AND subject_user_id IS NOT NULL",
        ),
    ];
    assert_eq!(
        active_permission_indexes.len(),
        expected_active_permission_indexes.len()
    );
    for ((name, index_sql), (expected_name, expected_columns, expected_active_predicate)) in
        active_permission_indexes
            .iter()
            .zip(expected_active_permission_indexes)
    {
        assert_eq!(name, expected_name);
        assert!(index_sql.contains(expected_columns));
        assert!(index_sql.contains(expected_active_predicate));
    }
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

    register_test_device(&store, "platform-command-device").await;
    let now = Utc::now();
    let command = CommandRepository::enqueue_command(
        &store,
        NewCommandOutboxEntry {
            id: uuid::Uuid::now_v7().to_string(),
            tenant_id: test_tenant_id(),
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
    register_test_device(&store, "platform-command-device").await;

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
    register_test_device(&store, "platform-command-device").await;

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
async fn platform_store_replays_sqlite_command_when_only_handler_timestamps_change() {
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
    register_test_device(&store, "platform-command-device").await;

    let original = command("platform-command-device", &command_id, r#"{"target":"on"}"#);
    let mut retry = original.clone();
    retry.next_attempt_at += Duration::seconds(1);
    retry.expires_at += Duration::seconds(1);

    let created = CommandRepository::enqueue_command(&store, original)
        .await
        .unwrap();
    let replayed = CommandRepository::enqueue_command(&store, retry)
        .await
        .unwrap();

    assert_eq!(replayed, created);
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
    register_test_device(&store, "platform-command-device").await;

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
    register_test_device(&store, "platform-command-device").await;

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
async fn platform_store_scopes_sqlite_registration_and_telemetry_to_the_explicit_tenant() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_a = create_tenant(&store, "tenant-a").await;
    let tenant_b = create_tenant(&store, "tenant-b").await;
    let event = telemetry("tenant-device", 1);

    TopologyRepository::register_device(&store, tenant_a, &event.device_id)
        .await
        .unwrap();
    assert!(
        TelemetryRepository::write_telemetry(
            &store,
            tenant_a,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await
        .unwrap()
    );
    assert_unknown_device(
        TelemetryRepository::write_telemetry(
            &store,
            tenant_a,
            &telemetry("missing-device", 1),
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await,
        "missing-device",
    );
    assert_unknown_device(
        TelemetryRepository::write_telemetry(
            &store,
            tenant_b,
            &event,
            Utc::now(),
            "iot/v1/devices/telemetry",
        )
        .await,
        &event.device_id,
    );
    assert!(matches!(
        TopologyRepository::register_device(&store, tenant_b, &event.device_id).await,
        Err(PlatformStoreError::DeviceTenantConflict { .. })
    ));
}

#[tokio::test]
async fn sqlite_registration_conflict_uses_the_active_tenant_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_a = create_tenant(&store, "tenant-race-a").await;
    let tenant_b = create_tenant(&store, "tenant-race-b").await;
    TopologyRepository::register_device(&store, tenant_b, "tenant-race-device")
        .await
        .unwrap();

    let trigger = format!(
        "CREATE TRIGGER suspend_tenant_before_registration_conflict
         BEFORE INSERT ON devices
         WHEN NEW.device_id = 'tenant-race-device'
         BEGIN
             UPDATE tenants SET status = 'suspended' WHERE id = '{tenant_a}';
         END"
    );
    sqlx::query(AssertSqlSafe(trigger))
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    assert!(matches!(
        TopologyRepository::register_device(&store, tenant_a, "tenant-race-device").await,
        Err(PlatformStoreError::DeviceTenantConflict { tenant_id, .. }) if tenant_id == tenant_a
    ));
    TenantIdentityRepository::suspend_tenant(&store, "tenant-race-a")
        .await
        .unwrap();
    assert!(matches!(
        TopologyRepository::register_device(&store, tenant_a, "suspended-device").await,
        Err(PlatformStoreError::UnknownTenant(id)) if id == tenant_a
    ));
    let suspended_device_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE device_id = 'suspended-device'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(suspended_device_count, 0);
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
        write_test_telemetry(
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
    register_test_device(&store, &event.device_id).await;

    assert!(
        write_test_telemetry(&store, &event, Utc::now(), "iot/v1/devices/telemetry",)
            .await
            .unwrap()
    );
    assert!(
        !write_test_telemetry(&store, &event, Utc::now(), "iot/v1/devices/telemetry",)
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
    register_test_device(&store, "lifecycle-device").await;
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
    register_test_device(&store, "lifecycle-device").await;
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
    register_test_device(&store, &event.device_id).await;

    let result = write_test_telemetry(&store, &event, Utc::now(), "iot/v1/devices/telemetry").await;

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
    register_test_device(&store, &event.device_id).await;

    assert!(
        write_test_telemetry(&store, &event, Utc::now(), "iot/v1/devices/telemetry",)
            .await
            .unwrap()
    );
    assert!(
        !write_test_telemetry(&store, &event, Utc::now(), "iot/v1/devices/telemetry",)
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_enqueues_a_command_for_a_registered_timescale_device() {
    let (_test_lock, store) = timescale_test_store().await;

    register_test_device(&store, "platform-command-device").await;
    let now = Utc::now();
    let command = CommandRepository::enqueue_command(
        &store,
        NewCommandOutboxEntry {
            id: uuid::Uuid::now_v7().to_string(),
            tenant_id: test_tenant_id(),
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
    register_test_device(&store, "platform-command-device").await;

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
    register_test_device(&store, "platform-command-device").await;

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
    register_test_device(&store, "platform-command-device").await;

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
             'command_outbox',
             'user_groups',
             'user_group_members',
             'resource_permissions'
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
            "resource_permissions",
            "telemetry",
            "user_group_members",
            "user_groups",
            "users",
        ]
    );

    let legacy_tables = sqlx::query(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano'
           AND table_name IN ('resource_grants', 'resource_shares')
         ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert!(legacy_tables.is_empty());

    let authorization_constraints = sqlx::query(
        "SELECT relation.relname AS table_name,
                pg_get_constraintdef(constraint_row.oid) AS definition
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
         JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = 'iot_nano'
           AND relation.relname IN (
             'user_groups',
             'user_group_members',
             'resource_permissions'
           )
         ORDER BY relation.relname, definition",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("table_name"),
            row.get::<String, _>("definition"),
        )
    })
    .collect::<Vec<_>>();
    let has_constraint = |table_name: &str, definition: &str| {
        authorization_constraints
            .iter()
            .any(|(actual_table_name, actual_definition)| {
                actual_table_name == table_name && actual_definition.contains(definition)
            })
    };

    assert!(has_constraint(
        "user_groups",
        "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint("user_groups", "UNIQUE (id, tenant_id)"));
    assert!(has_constraint(
        "user_groups",
        "FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "user_group_members",
        "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "user_group_members",
        "PRIMARY KEY (group_id, user_id)"
    ));
    assert!(has_constraint(
        "user_group_members",
        "FOREIGN KEY (group_id, tenant_id) REFERENCES user_groups(id, tenant_id) ON DELETE CASCADE"
    ));
    assert!(has_constraint(
        "user_group_members",
        "FOREIGN KEY (user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE CASCADE"
    ));
    assert!(has_constraint(
        "resource_permissions",
        "FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "resource_permissions",
        "FOREIGN KEY (subject_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "resource_permissions",
        "FOREIGN KEY (subject_group_id, tenant_id) REFERENCES user_groups(id, tenant_id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "resource_permissions",
        "FOREIGN KEY (asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "resource_permissions",
        "FOREIGN KEY (device_id, tenant_id) REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT"
    ));
    assert!(has_constraint(
        "resource_permissions",
        "FOREIGN KEY (created_by_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT"
    ));

    let authorization_indexes = sqlx::query(
        "SELECT table_relation.relname AS table_name,
                index_relation.relname AS index_name,
                pg_get_indexdef(index_relation.oid) AS definition,
                pg_get_expr(index_row.indpred, index_row.indrelid) AS predicate
         FROM pg_index AS index_row
         JOIN pg_class AS index_relation ON index_relation.oid = index_row.indexrelid
         JOIN pg_class AS table_relation ON table_relation.oid = index_row.indrelid
         JOIN pg_namespace AS namespace ON namespace.oid = table_relation.relnamespace
         WHERE namespace.nspname = 'iot_nano'
           AND index_relation.relname IN (
             'user_group_members_tenant_user_group_index',
             'resource_permissions_active_asset_group_index',
             'resource_permissions_active_asset_user_index',
             'resource_permissions_active_device_group_index',
             'resource_permissions_active_device_user_index'
           )
         ORDER BY index_relation.relname",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("table_name"),
            row.get::<String, _>("index_name"),
            row.get::<String, _>("definition"),
            row.get::<Option<String>, _>("predicate"),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(authorization_indexes.len(), 5);

    let user_group_members_index = authorization_indexes
        .iter()
        .find(|(_, index_name, _, _)| index_name == "user_group_members_tenant_user_group_index")
        .unwrap();
    assert_eq!(user_group_members_index.0, "user_group_members");
    assert!(
        user_group_members_index
            .2
            .contains("(tenant_id, user_id, group_id)")
    );
    assert!(user_group_members_index.3.is_none());

    let expected_active_permission_indexes = [
        (
            "resource_permissions_active_asset_group_index",
            "(tenant_id, asset_id, subject_group_id)",
            [
                "revoked_at IS NULL",
                "asset_id IS NOT NULL",
                "subject_group_id IS NOT NULL",
            ],
        ),
        (
            "resource_permissions_active_asset_user_index",
            "(tenant_id, asset_id, subject_user_id)",
            [
                "revoked_at IS NULL",
                "asset_id IS NOT NULL",
                "subject_user_id IS NOT NULL",
            ],
        ),
        (
            "resource_permissions_active_device_group_index",
            "(tenant_id, device_id, subject_group_id)",
            [
                "revoked_at IS NULL",
                "device_id IS NOT NULL",
                "subject_group_id IS NOT NULL",
            ],
        ),
        (
            "resource_permissions_active_device_user_index",
            "(tenant_id, device_id, subject_user_id)",
            [
                "revoked_at IS NULL",
                "device_id IS NOT NULL",
                "subject_user_id IS NOT NULL",
            ],
        ),
    ];
    for (index_name, expected_columns, expected_predicate_fragments) in
        expected_active_permission_indexes
    {
        let (_, _, definition, predicate) = authorization_indexes
            .iter()
            .find(|(_, actual_index_name, _, _)| actual_index_name == index_name)
            .unwrap();
        assert!(definition.contains(expected_columns));
        let predicate = predicate.as_deref().unwrap();
        for expected_fragment in expected_predicate_fragments {
            assert!(predicate.contains(expected_fragment));
        }
    }
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_timescale_schema_enforces_resource_permission_checks() {
    let (_test_lock, store) = timescale_test_store().await;
    ensure_test_tenant(&store).await;
    let pool = store.timescale_pool().unwrap();
    let tenant_id = test_tenant_id();
    let user_id = uuid::Uuid::now_v7();
    let group_id = uuid::Uuid::now_v7();
    let asset_id = uuid::Uuid::now_v7();
    let device_id = "resource-permission-contract-device";

    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role)
         VALUES ($1, $2, $3, 'resource-permission-contract-hash', 'member')",
    )
    .bind(user_id)
    .bind(tenant_id)
    .bind(format!("resource-permission-contract-{user_id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES ($1, $2, $3, 'resource-permission-contract-group')",
    )
    .bind(group_id)
    .bind(tenant_id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES ($1, $2, 'resource-permission-contract-asset')",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    register_test_device(&store, device_id).await;

    let invalid_permission = sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children, created_by_user_id
         ) VALUES ($1, $2, $3, $4, 'owner', FALSE, $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(user_id)
    .bind(device_id)
    .execute(pool)
    .await
    .unwrap_err();
    assert_check_constraint_violation(invalid_permission);

    let missing_subject = sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, device_id, permission, inherit_children, created_by_user_id
         ) VALUES ($1, $2, $3, 'viewer', FALSE, $4)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(device_id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap_err();
    assert_check_constraint_violation(missing_subject);

    let multiple_subjects = sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, subject_group_id, device_id, permission,
            inherit_children, created_by_user_id
         ) VALUES ($1, $2, $3, $4, $5, 'viewer', FALSE, $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(user_id)
    .bind(group_id)
    .bind(device_id)
    .execute(pool)
    .await
    .unwrap_err();
    assert_check_constraint_violation(multiple_subjects);

    let missing_scope = sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, permission, inherit_children, created_by_user_id
         ) VALUES ($1, $2, $3, 'viewer', FALSE, $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap_err();
    assert_check_constraint_violation(missing_scope);

    let multiple_scopes = sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, asset_id, device_id, permission,
            inherit_children, created_by_user_id
         ) VALUES ($1, $2, $3, $4, $5, 'viewer', FALSE, $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(user_id)
    .bind(asset_id)
    .bind(device_id)
    .execute(pool)
    .await
    .unwrap_err();
    assert_check_constraint_violation(multiple_scopes);

    let inherited_device_scope = sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children, created_by_user_id
         ) VALUES ($1, $2, $3, $4, 'viewer', TRUE, $3)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(tenant_id)
    .bind(user_id)
    .bind(device_id)
    .execute(pool)
    .await
    .unwrap_err();
    assert_check_constraint_violation(inherited_device_scope);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_timescale_schema_enforces_device_ownership() {
    let (_test_lock, store) = timescale_test_store().await;
    ensure_test_tenant(&store).await;
    let pool = store.timescale_pool().unwrap();

    let constraints = sqlx::query(
        "SELECT relation.relname AS table_name, constraint_row.conname,
                constraint_row.confdeltype::text AS confdeltype
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
         WHERE constraint_row.conname IN (
            'command_outbox_tenant_device_fkey',
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
                "command_outbox_tenant_device_fkey".to_owned(),
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
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, 'switch_on', '{}'::jsonb, 'one_way', now(), now())",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(test_tenant_id())
    .bind(missing_device)
    .execute(pool)
    .await
    .unwrap_err();
    assert_foreign_key_violation(command_error);

    let telemetry_error = sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
         ) VALUES (now(), now(), $1, $2, $3, 1, '{}'::jsonb, 'iot/v1/devices/telemetry')",
    )
    .bind(test_tenant_id())
    .bind(missing_device)
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await
    .unwrap_err();
    assert_foreign_key_violation(telemetry_error);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_rejects_timescale_schema_missing_command_outbox_tenant_device_foreign_key()
{
    let (_test_lock, store) = timescale_test_store().await;
    let pool = store.timescale_pool().unwrap().clone();
    let relation_before = sqlx::query(
        "SELECT relation.oid::text AS relation_id, relation.relfilenode::text AS storage_id
         FROM pg_class AS relation
         JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = 'iot_nano' AND relation.relname = 'command_outbox'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let relation_before = (
        relation_before.get::<String, _>("relation_id"),
        relation_before.get::<String, _>("storage_id"),
    );

    sqlx::query("ALTER TABLE command_outbox DROP CONSTRAINT command_outbox_tenant_device_fkey")
        .execute(&pool)
        .await
        .unwrap();
    drop(store);

    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
    let open_result = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await;

    match open_result {
        Err(PlatformStoreError::ResetRequiredTimescaleSchema { table }) => {
            assert_eq!(table, "command_outbox");
        }
        Err(error) => panic!("unexpected Timescale migration error: {error}"),
        Ok(store) => {
            drop(store);
            panic!("missing command_outbox tenant device foreign key was accepted");
        }
    }

    let relation_after = sqlx::query(
        "SELECT relation.oid::text AS relation_id, relation.relfilenode::text AS storage_id
         FROM pg_class AS relation
         JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = 'iot_nano' AND relation.relname = 'command_outbox'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let relation_after = (
        relation_after.get::<String, _>("relation_id"),
        relation_after.get::<String, _>("storage_id"),
    );
    assert_eq!(
        relation_after, relation_before,
        "reset-required startup must not rebuild command_outbox"
    );
    let constraint_still_missing: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1
             FROM pg_constraint AS constraint_row
             JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
             JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = 'iot_nano'
               AND relation.relname = 'command_outbox'
               AND constraint_row.conname = 'command_outbox_tenant_device_fkey'
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        constraint_still_missing,
        "reset-required startup must not mutate the incompatible schema"
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn platform_store_timescale_serializes_device_deletion_with_command_and_telemetry_writes() {
    let (_test_lock, store) = timescale_test_store().await;
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
    let deletion_pool = PgPool::connect(&database_url).await.unwrap();

    let command_device_id = "command-race-device";
    register_test_device(&store, command_device_id).await;
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
    register_test_device(&store, telemetry_device_id).await;
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
            test_tenant_id(),
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
    register_test_device(&store, "platform-command-device").await;

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
        write_test_telemetry(
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
    register_test_device(&store, &event.device_id).await;

    let result = write_test_telemetry(&store, &event, Utc::now(), "iot/v1/devices/telemetry").await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::TelemetrySequenceOverflow)
    ));
}
