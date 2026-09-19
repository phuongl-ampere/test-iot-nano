use std::env;

use chrono::{DateTime, Duration, TimeZone, Utc};
use iot_nano_foundation::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_storage::{
    CommandOutboxState, NewCommandOutboxEntry, PlatformStore, PlatformStoreError, SqliteStore,
    SqliteStoreError,
};
use sqlx::{Connection, PgConnection};

mod common;

const TIMESCALE_TEST_URL: &str = "postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform";

fn test_tenant_id() -> uuid::Uuid {
    uuid::Uuid::from_u128(1)
}

async fn store() -> (tempfile::TempDir, SqliteStore) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rush.db");
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, 'command-outbox', 'active', '{}')",
    )
    .bind(test_tenant_id().to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ('device-1', ?)")
        .bind(test_tenant_id().to_string())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES ('active-token', 'device-1', 'iotd_active_token', 'unused-in-storage-tests')",
    )
    .execute(store.pool())
    .await
    .unwrap();
    (directory, store)
}

async fn timescale_store() -> (PgConnection, PlatformStore) {
    let database_url = env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    assert_eq!(
        database_url, TIMESCALE_TEST_URL,
        "refusing to reset an unexpected Timescale test database"
    );
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        database_name, "iot_nano_test_platform",
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
    (connection, store)
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}

fn command(id: &str, now: DateTime<Utc>, expires_at: DateTime<Utc>) -> NewCommandOutboxEntry {
    NewCommandOutboxEntry {
        id: id.to_owned(),
        tenant_id: test_tenant_id(),
        device_id: "device-1".to_owned(),
        method: "sample_now".to_owned(),
        params: r#"{"source":"test"}"#.to_owned(),
        mode: RpcMode::OneWay,
        expires_at,
        next_attempt_at: now,
    }
}

async fn register_tenant_device_and_token(
    store: &SqliteStore,
    tenant_id: uuid::Uuid,
    slug: &str,
    device_id: &str,
    token_id: &str,
) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, ?, 'active', '{}')",
    )
    .bind(tenant_id.to_string())
    .bind(slug)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES (?, ?)")
        .bind(device_id)
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, ?, ?, 'unused-in-storage-tests')",
    )
    .bind(token_id)
    .bind(device_id)
    .bind(format!("iotd_{token_id}"))
    .execute(store.pool())
    .await
    .unwrap();
}

async fn assert_current_command_outbox_identifier_requires_reset(
    path: std::path::PathBuf,
    command_id: &str,
    tenant_id: &str,
) {
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    register_tenant_device_and_token(
        &store,
        test_tenant_id(),
        "command-outbox-current",
        "current-device",
        "current-token",
    )
    .await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, state, expires_at, next_attempt_at
         ) VALUES (?, ?, 'current-device', 'sample_now', '{}', 'one_way', 'queued', ?, ?)",
    )
    .bind(command_id)
    .bind(tenant_id)
    .bind("2027-01-15T08:10:00Z")
    .bind("2027-01-15T08:00:00Z")
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    drop(store);

    assert!(matches!(
        SqliteStore::open(&configuration).await,
        Err(SqliteStoreError::ResetRequired { table }) if table == "command_outbox"
    ));
    assert!(matches!(
        PlatformStore::open(&configuration).await,
        Err(PlatformStoreError::Sqlite(SqliteStoreError::ResetRequired { table }))
            if table == "command_outbox"
    ));
}

#[tokio::test]
async fn sqlite_command_outbox_claim_leases_a_command_only_once() {
    let (_directory, store) = store().await;
    let now = at(1_800_000_000);
    store
        .enqueue_command(command("command-1", now, now + Duration::minutes(5)))
        .await
        .unwrap();

    let first_store = store.clone();
    let second_store = store.clone();
    let (first, second) = tokio::join!(
        first_store.claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1),
        second_store.claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    let claimed = first.iter().chain(second.iter()).collect::<Vec<_>>();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "command-1");
    assert_eq!(claimed[0].state, CommandOutboxState::Leased);
    assert_eq!(claimed[0].attempt_count, 1);
    assert_eq!(
        store
            .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
            .await
            .unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn sqlite_command_outbox_preserves_created_at_across_lifecycle_transitions() {
    let (_directory, store) = store().await;
    let issued_at = at(1_800_000_000);
    let command_id = "command-created-at";
    let mut entry = command(command_id, issued_at, issued_at + Duration::minutes(5));
    entry.mode = RpcMode::TwoWay;

    let enqueued = store.enqueue_command(entry).await.unwrap();
    let created_at = enqueued.created_at;
    let claimed = store
        .claim_commands(
            test_tenant_id(),
            issued_at,
            issued_at + Duration::seconds(30),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let released = store
        .release_command_for_retry(
            test_tenant_id(),
            command_id,
            "broker unavailable",
            issued_at + Duration::seconds(2),
        )
        .await
        .unwrap()
        .unwrap();
    let reclaimed = store
        .claim_commands(
            test_tenant_id(),
            issued_at + Duration::seconds(2),
            issued_at + Duration::seconds(32),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let published = store
        .mark_command_published(
            test_tenant_id(),
            command_id,
            issued_at + Duration::seconds(3),
        )
        .await
        .unwrap()
        .unwrap();
    let responded = store
        .mark_command_responded(
            test_tenant_id(),
            command_id,
            "device-1",
            "active-token",
            r#"{"ok":true}"#,
            issued_at + Duration::seconds(4),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(enqueued.created_at, created_at);
    assert_eq!(claimed.created_at, created_at);
    assert_eq!(released.created_at, created_at);
    assert_eq!(reclaimed.created_at, created_at);
    assert_eq!(published.created_at, created_at);
    assert_eq!(responded.created_at, created_at);
}

#[tokio::test]
async fn sqlite_platform_store_command_lifecycle_port_claims_a_command() {
    let (_directory, sqlite) = store().await;
    let platform = PlatformStore::Sqlite(sqlite);
    let now = at(1_800_000_000);
    let command_id = uuid::Uuid::now_v7().to_string();
    platform
        .enqueue_command(command(&command_id, now, now + Duration::minutes(5)))
        .await
        .unwrap();

    let claimed = platform
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
        .await
        .unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, command_id);
    assert_eq!(claimed[0].state, CommandOutboxState::Leased);
}

#[tokio::test]
async fn sqlite_platform_store_command_lifecycle_port_accepts_typed_command_ids() {
    let (_directory, sqlite) = store().await;
    let platform = PlatformStore::Sqlite(sqlite);
    let now = at(1_800_000_000);
    let command_id = uuid::Uuid::now_v7();
    platform
        .enqueue_command(command(
            &command_id.to_string(),
            now,
            now + Duration::minutes(5),
        ))
        .await
        .unwrap();
    platform
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
        .await
        .unwrap();

    let published = platform
        .mark_command_published(test_tenant_id(), command_id, now)
        .await;

    assert_eq!(
        published.unwrap().unwrap().state,
        CommandOutboxState::PublishedToBroker
    );
}

#[tokio::test]
async fn sqlite_command_outbox_expire_marks_queued_and_leased_commands() {
    let (_directory, store) = store().await;
    let now = at(1_800_000_000);
    store
        .enqueue_command(command("queued", now - Duration::seconds(1), now))
        .await
        .unwrap();
    store
        .enqueue_command(command(
            "leased",
            now - Duration::minutes(1),
            now + Duration::seconds(1),
        ))
        .await
        .unwrap();
    let leased = store
        .claim_commands(
            test_tenant_id(),
            now - Duration::minutes(1),
            now + Duration::minutes(1),
            1,
        )
        .await
        .unwrap();
    assert_eq!(leased[0].id, "leased");

    let expired = store
        .expire_due_commands(test_tenant_id(), now + Duration::seconds(1), 2)
        .await
        .unwrap();

    assert_eq!(expired.len(), 2);
    assert!(
        expired
            .iter()
            .all(|record| record.state == CommandOutboxState::Expired)
    );
    assert_eq!(
        store
            .claim_commands(
                test_tenant_id(),
                now + Duration::seconds(1),
                now + Duration::minutes(1),
                10,
            )
            .await
            .unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn sqlite_command_outbox_expires_due_commands_with_a_bounded_deterministic_selection() {
    let (_directory, store) = store().await;
    let now = at(1_800_000_000);
    store
        .enqueue_command(command("expired-earliest", now, now - Duration::seconds(3)))
        .await
        .unwrap();
    store
        .enqueue_command(command("expired-middle", now, now - Duration::seconds(2)))
        .await
        .unwrap();
    store
        .enqueue_command(command("expired-latest", now, now - Duration::seconds(1)))
        .await
        .unwrap();

    let first = store
        .expire_due_commands(test_tenant_id(), now, 2)
        .await
        .unwrap();

    assert_eq!(first.len(), 2);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM command_outbox WHERE id = 'expired-earliest'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap(),
        "expired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM command_outbox WHERE id = 'expired-middle'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap(),
        "expired"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM command_outbox WHERE id = 'expired-latest'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap(),
        "queued"
    );

    let second = store
        .expire_due_commands(test_tenant_id(), now, 2)
        .await
        .unwrap();

    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, "expired-latest");
}

#[tokio::test]
async fn sqlite_command_outbox_reclaims_an_expired_lease_before_command_expiry() {
    let (_directory, store) = store().await;
    let now = at(1_800_000_000);
    store
        .enqueue_command(command("command-1", now, now + Duration::minutes(5)))
        .await
        .unwrap();
    let first = store
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    assert_eq!(first[0].attempt_count, 1);

    let reclaimed = store
        .claim_commands(
            test_tenant_id(),
            now + Duration::seconds(30),
            now + Duration::minutes(1),
            1,
        )
        .await
        .unwrap();

    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id, "command-1");
    assert_eq!(reclaimed[0].state, CommandOutboxState::Leased);
    assert_eq!(reclaimed[0].attempt_count, 2);
}

#[tokio::test]
async fn sqlite_command_outbox_marks_leased_commands_published_or_failed() {
    let (_directory, store) = store().await;
    let now = at(1_800_000_000);
    for id in ["published", "failed"] {
        store
            .enqueue_command(command(id, now, now + Duration::minutes(5)))
            .await
            .unwrap();
    }

    let claimed = store
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 2)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);

    let published = store
        .mark_command_published(test_tenant_id(), "published", now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    let failed = store
        .mark_command_failed(test_tenant_id(), "failed", "broker unavailable")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(published.state, CommandOutboxState::PublishedToBroker);
    assert_eq!(published.published_at, Some(now + Duration::seconds(1)));
    assert_eq!(failed.state, CommandOutboxState::Failed);
    assert_eq!(failed.last_error.as_deref(), Some("broker unavailable"));
    assert!(failed.lease_until.is_none());
    assert!(
        store
            .mark_command_published(test_tenant_id(), "failed", now + Duration::seconds(2))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_command_outbox_records_one_two_way_response_after_broker_publication() {
    let (_directory, store) = store().await;
    let now = at(1_800_000_000);
    let mut entry = command("two-way", now, now + Duration::minutes(5));
    entry.mode = RpcMode::TwoWay;
    store.enqueue_command(entry).await.unwrap();
    store
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    store
        .mark_command_published(test_tenant_id(), "two-way", now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();

    let responded = store
        .mark_command_responded(
            test_tenant_id(),
            "two-way",
            "device-1",
            "active-token",
            r#"{"ok":true,"sampled_at":"2027-01-15T08:00:01Z"}"#,
            now + Duration::seconds(2),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(responded.state, CommandOutboxState::Responded);
    assert_eq!(responded.mode, RpcMode::TwoWay);
    assert_eq!(
        responded.response.as_deref(),
        Some(r#"{"ok":true,"sampled_at":"2027-01-15T08:00:01Z"}"#)
    );
    assert_eq!(responded.responded_at, Some(now + Duration::seconds(2)));
    let retry = store
        .mark_command_responded(
            test_tenant_id(),
            "two-way",
            "device-1",
            "active-token",
            r#"{"ok":true,"sampled_at":"2027-01-15T08:00:01Z"}"#,
            now + Duration::seconds(3),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry, responded);
    assert!(
        store
            .mark_command_responded(
                test_tenant_id(),
                "two-way",
                "device-1",
                "active-token",
                r#"{"ok":false}"#,
                now + Duration::seconds(3),
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_command_outbox_scopes_issue_claim_and_response_to_tenant() {
    let (_directory, store) = store().await;
    let platform = PlatformStore::Sqlite(store.clone());
    let tenant_a = test_tenant_id();
    let tenant_b = uuid::Uuid::from_u128(2);
    register_tenant_device_and_token(
        &store,
        tenant_b,
        "command-outbox-b",
        "device-2",
        "active-token-b",
    )
    .await;
    let now = at(1_800_000_000);
    let tenant_a_token = uuid::Uuid::from_u128(3);
    sqlx::query(
        "UPDATE device_tokens
         SET id = ?, token_prefix = 'iotd_tenant_a_token'
         WHERE id = 'active-token'",
    )
    .bind(tenant_a_token.to_string())
    .execute(store.pool())
    .await
    .unwrap();

    let cross_tenant_issue_id = uuid::Uuid::now_v7();
    let mut cross_tenant_issue = command(
        &cross_tenant_issue_id.to_string(),
        now,
        now + Duration::minutes(5),
    );
    cross_tenant_issue.tenant_id = tenant_b;
    assert!(matches!(
        platform.enqueue_command(cross_tenant_issue).await,
        Err(PlatformStoreError::UnknownDevice(device_id)) if device_id == "device-1"
    ));

    let tenant_a_command_id = uuid::Uuid::now_v7();
    let mut tenant_a_command = command(
        &tenant_a_command_id.to_string(),
        now,
        now + Duration::minutes(5),
    );
    tenant_a_command.tenant_id = tenant_a;
    tenant_a_command.mode = RpcMode::TwoWay;
    platform.enqueue_command(tenant_a_command).await.unwrap();

    let tenant_b_command_id = uuid::Uuid::now_v7();
    let mut tenant_b_command = command(
        &tenant_b_command_id.to_string(),
        now,
        now + Duration::minutes(5),
    );
    tenant_b_command.tenant_id = tenant_b;
    tenant_b_command.device_id = "device-2".to_owned();
    platform.enqueue_command(tenant_b_command).await.unwrap();

    let claimed_by_b = platform
        .claim_commands(tenant_b, now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(claimed_by_b.len(), 1);
    assert_eq!(claimed_by_b[0].id, tenant_b_command_id.to_string());
    assert_eq!(claimed_by_b[0].tenant_id, tenant_b);

    let claimed_by_a = platform
        .claim_commands(tenant_a, now, now + Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(claimed_by_a.len(), 1);
    assert_eq!(claimed_by_a[0].id, tenant_a_command_id.to_string());
    assert_eq!(claimed_by_a[0].tenant_id, tenant_a);
    platform
        .mark_command_published(tenant_a, tenant_a_command_id, now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();

    assert!(
        platform
            .mark_command_responded(
                tenant_b,
                tenant_a_command_id,
                "device-1",
                tenant_a_token,
                r#"{"ok":true}"#,
                now + Duration::seconds(2),
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        platform
            .mark_command_responded(
                tenant_a,
                tenant_a_command_id,
                "device-1",
                tenant_a_token,
                r#"{"ok":true}"#,
                now + Duration::seconds(2),
            )
            .await
            .unwrap()
            .unwrap()
            .state,
        CommandOutboxState::Responded
    );
}

#[tokio::test]
async fn sqlite_open_rejects_a_legacy_command_outbox_without_tenant_scope() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    register_tenant_device_and_token(
        &store,
        test_tenant_id(),
        "command-outbox-legacy",
        "legacy-device",
        "legacy-token",
    )
    .await;
    sqlx::raw_sql(
        "DROP INDEX command_outbox_due_index;
         DROP INDEX command_outbox_expiring_index;
         DROP TABLE command_outbox;
         CREATE TABLE command_outbox (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
            method TEXT NOT NULL CHECK (trim(method) <> ''),
            params TEXT NOT NULL DEFAULT '{}',
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'leased', 'published_to_broker', 'expired', 'failed')),
            expires_at TEXT NOT NULL,
            next_attempt_at TEXT NOT NULL,
            lease_until TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            last_error TEXT,
            published_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
         );
         INSERT INTO command_outbox (
             id, device_id, method, params, state, expires_at, next_attempt_at
         ) VALUES (
             'legacy-command', 'legacy-device', 'sample_now', '{}', 'published_to_broker',
             '2027-01-15T08:10:00Z', '2027-01-15T08:00:00Z'
         );",
    )
    .execute(store.pool())
    .await
    .unwrap();
    drop(store);

    assert!(matches!(
        SqliteStore::open(&configuration).await,
        Err(SqliteStoreError::ResetRequired { table }) if table == "command_outbox"
    ));
}

#[tokio::test]
async fn sqlite_open_rejects_current_command_outbox_with_invalid_identifiers() {
    let directory = tempfile::tempdir().unwrap();
    let valid_tenant_id = test_tenant_id().to_string();
    let valid_command_id = uuid::Uuid::from_u128(2).to_string();

    assert_current_command_outbox_identifier_requires_reset(
        directory.path().join("invalid-command-id.db"),
        "invalid-command-id",
        &valid_tenant_id,
    )
    .await;
    assert_current_command_outbox_identifier_requires_reset(
        directory.path().join("invalid-tenant-id.db"),
        &valid_command_id,
        "invalid-tenant-id",
    )
    .await;
}

#[tokio::test]
async fn sqlite_store_reopens_with_the_two_way_expiry_index() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let first = SqliteStore::open(&configuration).await.unwrap();
    drop(first);

    let reopened = SqliteStore::open(&configuration).await.unwrap();
    let index_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sqlite_master
         WHERE type = 'index' AND name = 'command_outbox_expiring_index'",
    )
    .fetch_one(reopened.pool())
    .await
    .unwrap();

    assert_eq!(index_count, 1);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for iot_nano_test_platform"]
async fn timescale_command_outbox_preserves_created_at_across_lifecycle_transitions() {
    let (_connection, store) = timescale_store().await;
    let device_id = "timescale-command-device";
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES ($1, 'command-outbox', 'active', '{}'::jsonb)",
    )
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    store
        .register_device(test_tenant_id(), device_id)
        .await
        .unwrap();
    let token_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES ($1, $2, 'timescale-token-prefix', 'unused-in-storage-tests')",
    )
    .bind(token_id)
    .bind(device_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let now = Utc::now() + Duration::seconds(1);
    let response_id = uuid::Uuid::now_v7();
    let mut response_command = command(&response_id.to_string(), now, now + Duration::minutes(5));
    response_command.device_id = device_id.to_owned();
    response_command.mode = RpcMode::TwoWay;
    let enqueued = store.enqueue_command(response_command).await.unwrap();
    let created_at = enqueued.created_at;

    let claimed = store
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let released = store
        .release_command_for_retry(
            test_tenant_id(),
            response_id,
            "temporary broker failure",
            now + Duration::seconds(1),
        )
        .await
        .unwrap()
        .unwrap();
    let reclaimed = store
        .claim_commands(
            test_tenant_id(),
            now + Duration::seconds(1),
            now + Duration::seconds(31),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let published = store
        .mark_command_published(test_tenant_id(), response_id, now + Duration::seconds(2))
        .await
        .unwrap()
        .unwrap();
    let responded = store
        .mark_command_responded(
            test_tenant_id(),
            response_id,
            device_id,
            token_id,
            r#"{"ok":true}"#,
            now + Duration::seconds(3),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(claimed.created_at, created_at);
    assert_eq!(released.created_at, created_at);
    assert_eq!(reclaimed.created_at, created_at);
    assert_eq!(published.created_at, created_at);
    assert_eq!(responded.created_at, created_at);

    let failed_id = uuid::Uuid::now_v7();
    let mut failed_command = command(&failed_id.to_string(), now, now + Duration::minutes(5));
    failed_command.device_id = device_id.to_owned();
    let failed_enqueued = store.enqueue_command(failed_command).await.unwrap();
    let failed_claimed = store
        .claim_commands(test_tenant_id(), now, now + Duration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let failed = store
        .mark_command_failed(test_tenant_id(), failed_id, "broker unavailable")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed_claimed.created_at, failed_enqueued.created_at);
    assert_eq!(failed.created_at, failed_enqueued.created_at);

    let expired_id = uuid::Uuid::now_v7();
    let mut expired_command = command(
        &expired_id.to_string(),
        now - Duration::seconds(2),
        now - Duration::seconds(1),
    );
    expired_command.device_id = device_id.to_owned();
    let expired_enqueued = store.enqueue_command(expired_command).await.unwrap();
    let expired = store
        .expire_due_commands(test_tenant_id(), now, 1)
        .await
        .unwrap();
    let expired = expired
        .iter()
        .find(|record| record.id == expired_id.to_string())
        .unwrap();
    assert_eq!(expired.created_at, expired_enqueued.created_at);
}
