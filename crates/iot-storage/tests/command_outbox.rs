use chrono::{DateTime, Duration, TimeZone, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_storage::{CommandOutboxState, NewCommandOutboxEntry, SqliteStore};
use sqlx::Row;

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
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-1')")
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

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().unwrap()
}

fn command(id: &str, now: DateTime<Utc>, expires_at: DateTime<Utc>) -> NewCommandOutboxEntry {
    NewCommandOutboxEntry {
        id: id.to_owned(),
        device_id: "device-1".to_owned(),
        method: "sample_now".to_owned(),
        params: r#"{"source":"test"}"#.to_owned(),
        mode: RpcMode::OneWay,
        expires_at,
        next_attempt_at: now,
    }
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
        first_store.claim_commands(now, now + Duration::seconds(30), 1),
        second_store.claim_commands(now, now + Duration::seconds(30), 1),
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
            .claim_commands(now, now + Duration::seconds(30), 1)
            .await
            .unwrap(),
        Vec::new()
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
        .claim_commands(now - Duration::minutes(1), now + Duration::minutes(1), 1)
        .await
        .unwrap();
    assert_eq!(leased[0].id, "leased");

    let expired = store
        .expire_commands(now + Duration::seconds(1))
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
            .claim_commands(now + Duration::seconds(1), now + Duration::minutes(1), 10,)
            .await
            .unwrap(),
        Vec::new()
    );
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
        .claim_commands(now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    assert_eq!(first[0].attempt_count, 1);

    let reclaimed = store
        .claim_commands(now + Duration::seconds(30), now + Duration::minutes(1), 1)
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
        .claim_commands(now, now + Duration::seconds(30), 2)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);

    let published = store
        .mark_command_published("published", now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    let failed = store
        .mark_command_failed("failed", "broker unavailable")
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
            .mark_command_published("failed", now + Duration::seconds(2))
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
        .claim_commands(now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    store
        .mark_command_published("two-way", now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();

    let responded = store
        .mark_command_responded(
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
    assert!(
        store
            .mark_command_responded(
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
async fn sqlite_open_upgrades_a_legacy_command_outbox_without_losing_commands() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('legacy-device')")
        .execute(store.pool())
        .await
        .unwrap();
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

    let upgraded = SqliteStore::open(&configuration).await.unwrap();
    let row = sqlx::query(
        "SELECT mode, state, response, responded_at
         FROM command_outbox
         WHERE id = 'legacy-command'",
    )
    .fetch_one(upgraded.pool())
    .await
    .unwrap();

    assert_eq!(row.try_get::<String, _>("mode").unwrap(), "one_way");
    assert_eq!(
        row.try_get::<String, _>("state").unwrap(),
        "published_to_broker"
    );
    assert!(
        row.try_get::<Option<String>, _>("response")
            .unwrap()
            .is_none()
    );
    assert!(
        row.try_get::<Option<String>, _>("responded_at")
            .unwrap()
            .is_none()
    );
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
