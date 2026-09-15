use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NotificationKind, NotificationOutboxState, NotificationRepository, PlatformStore,
};
use sqlx::{Connection, PgConnection, Row};

mod common;

async fn notification_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("platform.sqlite");
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn insert_notification(store: &PlatformStore, id: &str, next_attempt_at: &str) {
    let pool = store.sqlite_pool().unwrap();
    let rule_id = uuid::Uuid::now_v7().to_string();
    let incident_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold
         ) VALUES (?, ?, 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(&rule_id)
    .bind(format!("rule-{id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at
         ) VALUES (?, ?, 'notification-device', 'open', ?)",
    )
    .bind(&incident_id)
    .bind(&rule_id)
    .bind(next_attempt_at)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES (?, ?, 'opened', ?, 'subject', 'body', ?)",
    )
    .bind(id)
    .bind(&incident_id)
    .bind(format!("dedupe-{id}"))
    .bind(next_attempt_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_notification_with_default_next_attempt_at(store: &PlatformStore, id: &str) {
    let pool = store.sqlite_pool().unwrap();
    let rule_id = uuid::Uuid::now_v7().to_string();
    let incident_id = uuid::Uuid::now_v7().to_string();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold
         ) VALUES (?, ?, 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(&rule_id)
    .bind(format!("rule-{id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at
         ) VALUES (?, ?, 'notification-device', 'open', CURRENT_TIMESTAMP)",
    )
    .bind(&incident_id)
    .bind(&rule_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body
         ) VALUES (?, ?, 'opened', ?, 'subject', 'body')",
    )
    .bind(id)
    .bind(&incident_id)
    .bind(format!("dedupe-{id}"))
    .execute(pool)
    .await
    .unwrap();
}

async fn timescale_notification_store() -> (PgConnection, PlatformStore) {
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
    (connection, store)
}

#[tokio::test]
async fn sqlite_notification_outbox_claims_and_completes_only_the_current_lease() {
    let (_directory, store) = notification_store().await;
    let now = Utc::now();
    let due = (now - Duration::seconds(1)).to_rfc3339();
    let lease_until = now + Duration::seconds(30);
    let first_id = uuid::Uuid::now_v7().to_string();
    let second_id = uuid::Uuid::now_v7().to_string();
    let third_id = uuid::Uuid::now_v7().to_string();
    insert_notification(&store, &first_id, &due).await;
    insert_notification(&store, &second_id, &due).await;
    insert_notification(&store, &third_id, &due).await;

    let claimed = NotificationRepository::claim_notifications(&store, now, lease_until, 3)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 3);
    assert!(claimed.iter().all(|record| {
        record.state == NotificationOutboxState::Leased
            && record.kind == NotificationKind::Opened
            && record.attempt_count == 1
    }));
    assert_eq!(
        NotificationRepository::claim_notifications(&store, now, lease_until, 1)
            .await
            .unwrap()
            .len(),
        0
    );

    let first = claimed
        .iter()
        .find(|record| record.id.to_string() == first_id)
        .unwrap();
    let first_lease_until = first.lease_until.unwrap();
    let sent =
        NotificationRepository::mark_notification_sent(&store, first.id, first_lease_until, now)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(sent.state, NotificationOutboxState::Sent);
    assert!(
        NotificationRepository::release_notification_for_retry(
            &store,
            sent.id,
            first_lease_until,
            "late retry",
            now,
        )
        .await
        .unwrap()
        .is_none()
    );

    let second = claimed
        .iter()
        .find(|record| record.id.to_string() == second_id)
        .unwrap();
    let released = NotificationRepository::release_notification_for_retry(
        &store,
        second.id,
        second.lease_until.unwrap(),
        "temporary failure",
        now,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(released.state, NotificationOutboxState::Pending);
    assert_eq!(released.attempt_count, 1);

    let retried = NotificationRepository::claim_notifications(&store, now, lease_until, 1)
        .await
        .unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].id.to_string(), second_id);
    assert_eq!(retried[0].attempt_count, 2);
    assert!(
        NotificationRepository::mark_notification_sent(
            &store,
            retried[0].id,
            retried[0].lease_until.unwrap(),
            now,
        )
        .await
        .unwrap()
        .is_some()
    );

    let third = NotificationRepository::claim_notifications(
        &store,
        now + Duration::seconds(31),
        now + Duration::seconds(60),
        1,
    )
    .await
    .unwrap();
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].id.to_string(), third_id);
    assert_eq!(third[0].attempt_count, 2);
    let stale_id = third[0].id;
    let stale_lease_until = third[0].lease_until.unwrap();
    assert!(
        NotificationRepository::mark_notification_sent(&store, stale_id, stale_lease_until, now)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        NotificationRepository::release_notification_for_retry(
            &store,
            stale_id,
            stale_lease_until,
            "stale",
            now + Duration::seconds(1),
        )
        .await
        .unwrap()
        .is_none()
    );

    let row = sqlx::query("SELECT state, attempt_count FROM notification_outbox WHERE id = ?")
        .bind(first_id)
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("state"), "sent");
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
}

#[tokio::test]
async fn sqlite_notification_outbox_rejects_stale_lease_finalization_after_reclaim() {
    let (_directory, store) = notification_store().await;
    let now = Utc::now();
    let notification_id = uuid::Uuid::now_v7();
    insert_notification(
        &store,
        &notification_id.to_string(),
        &(now - Duration::seconds(1)).to_rfc3339(),
    )
    .await;

    let first_lease =
        NotificationRepository::claim_notifications(&store, now, now + Duration::seconds(1), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
    let first_lease_until = first_lease.lease_until.unwrap();
    let reclaimed_at = now + Duration::seconds(2);
    let current_lease = NotificationRepository::claim_notifications(
        &store,
        reclaimed_at,
        reclaimed_at + Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let current_lease_until = current_lease.lease_until.unwrap();
    assert_eq!(current_lease.attempt_count, 2);

    assert!(
        NotificationRepository::mark_notification_sent(
            &store,
            notification_id,
            first_lease_until,
            reclaimed_at,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        NotificationRepository::release_notification_for_retry(
            &store,
            notification_id,
            first_lease_until,
            "stale worker",
            reclaimed_at,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        NotificationRepository::mark_notification_sent(
            &store,
            notification_id,
            current_lease_until,
            reclaimed_at,
        )
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn sqlite_notification_outbox_claims_rows_with_default_timestamps() {
    let (_directory, store) = notification_store().await;
    let notification_id = uuid::Uuid::now_v7();
    insert_notification_with_default_next_attempt_at(&store, &notification_id.to_string()).await;

    let claimed = NotificationRepository::claim_notifications(
        &store,
        Utc::now() + Duration::seconds(1),
        Utc::now() + Duration::seconds(31),
        1,
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, notification_id);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_notification_outbox_matches_sqlite_lease_contract() {
    let (_test_lock, store) = timescale_notification_store().await;
    let pool = store.timescale_pool().unwrap();
    let rule_id = uuid::Uuid::now_v7();
    let incident_id = uuid::Uuid::now_v7();
    let notification_id = uuid::Uuid::now_v7();
    let now = Utc::now();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold
         ) VALUES ($1, $2, 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(rule_id)
    .bind(format!("rule-{rule_id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at
         ) VALUES ($1, $2, 'notification-device', 'open', $3)",
    )
    .bind(incident_id)
    .bind(rule_id)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, 'opened', $3, 'subject', 'body', $4)",
    )
    .bind(notification_id)
    .bind(incident_id)
    .bind(format!("dedupe-{notification_id}"))
    .bind(now - Duration::seconds(1))
    .execute(pool)
    .await
    .unwrap();

    let first_lease =
        NotificationRepository::claim_notifications(&store, now, now + Duration::seconds(1), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
    assert_eq!(first_lease.attempt_count, 1);
    let first_lease_until = first_lease.lease_until.unwrap();
    let reclaimed_at = now + Duration::seconds(2);
    let current_lease = NotificationRepository::claim_notifications(
        &store,
        reclaimed_at,
        reclaimed_at + Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(current_lease.attempt_count, 2);
    let current_lease_until = current_lease.lease_until.unwrap();
    assert!(
        NotificationRepository::mark_notification_sent(
            &store,
            notification_id,
            first_lease_until,
            reclaimed_at,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        NotificationRepository::release_notification_for_retry(
            &store,
            notification_id,
            first_lease_until,
            "stale worker",
            reclaimed_at,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        NotificationRepository::mark_notification_sent(
            &store,
            notification_id,
            current_lease_until,
            reclaimed_at,
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        NotificationRepository::release_notification_for_retry(
            &store,
            notification_id,
            current_lease_until,
            "sent rows cannot be retried",
            now,
        )
        .await
        .unwrap()
        .is_none()
    );
}
