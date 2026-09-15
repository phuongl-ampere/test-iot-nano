use chrono::{Duration, Timelike, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AlertIncidentRepository, AlertIncidentStatus, NewAlertIncident, NewNotificationOutboxEntry,
    NotificationKind, PlatformStore,
};
use sqlx::{Connection, PgConnection};

mod common;

async fn store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let platform = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, platform)
}

async fn rule(store: &PlatformStore, id: uuid::Uuid) {
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold
         ) VALUES (?, 'incident-rule', 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
}

fn incident(rule_id: uuid::Uuid, id: uuid::Uuid, status: AlertIncidentStatus) -> NewAlertIncident {
    NewAlertIncident {
        id,
        rule_id,
        device_id: "incident-device".to_owned(),
        status,
        condition_started_at: Utc::now(),
        last_value: Some(31.25),
    }
}

#[tokio::test]
async fn sqlite_creates_pending_and_open_incidents_with_atomic_open_notification() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;

    let pending = AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, uuid::Uuid::now_v7(), AlertIncidentStatus::Pending),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(pending.status, AlertIncidentStatus::Pending);
    assert_eq!(pending.state_version, 0);
    let duplicate_notification = NewNotificationOutboxEntry {
        id: uuid::Uuid::now_v7(),
        kind: NotificationKind::Opened,
        dedupe_key: "pending-duplicate-notification".to_owned(),
        subject: "duplicate".to_owned(),
        body: "body".to_owned(),
        next_attempt_at: Utc::now(),
    };
    assert!(
        AlertIncidentRepository::create_incident(
            &store,
            incident(rule_id, uuid::Uuid::now_v7(), AlertIncidentStatus::Pending),
            Some(duplicate_notification),
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );

    let open_id = uuid::Uuid::now_v7();
    let opened_at = Utc::now().with_nanosecond(123_456_789).unwrap();
    let opened = AlertIncidentRepository::create_incident(
        &store,
        NewAlertIncident {
            id: open_id,
            rule_id,
            device_id: "second-device".to_owned(),
            status: AlertIncidentStatus::Open,
            condition_started_at: opened_at,
            last_value: None,
        },
        Some(NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Opened,
            dedupe_key: "incident-opened:second-device".to_owned(),
            subject: "opened".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: opened_at,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(opened.status, AlertIncidentStatus::Open);
    assert_eq!(
        opened.opened_at.unwrap().timestamp_subsec_nanos(),
        123_456_000
    );

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification_outbox WHERE incident_id = ?")
            .bind(open_id.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(count, 1);
    let notification_next_attempt_at: String =
        sqlx::query_scalar("SELECT next_attempt_at FROM notification_outbox WHERE incident_id = ?")
            .bind(open_id.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&notification_next_attempt_at)
            .unwrap()
            .timestamp_subsec_nanos(),
        123_456_000
    );
}

#[tokio::test]
async fn sqlite_rejects_stale_versions_and_incompatible_incident_transitions() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;
    let id = uuid::Uuid::now_v7();
    AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, id, AlertIncidentStatus::Pending),
        None,
    )
    .await
    .unwrap();

    let changed =
        AlertIncidentRepository::update_incident_last_value(&store, id, 0, Some(32.0), Utc::now())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(changed.state_version, 1);
    assert!(
        AlertIncidentRepository::update_incident_last_value(&store, id, 0, Some(33.0), Utc::now(),)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        AlertIncidentRepository::resolve_incident(&store, id, 1, Utc::now())
            .await
            .unwrap()
            .is_none()
    );

    let opened = AlertIncidentRepository::open_incident(&store, id, 1, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let recovery_started_at = Utc::now().with_nanosecond(222_333_444).unwrap();
    let recovering = AlertIncidentRepository::recover_incident(
        &store,
        id,
        opened.state_version,
        recovery_started_at,
    )
    .await
    .unwrap()
    .unwrap();
    let recovery_started_at = recovering.recovery_started_at;
    let resolved = AlertIncidentRepository::resolve_incident(
        &store,
        id,
        recovering.state_version,
        recovery_started_at.unwrap() + Duration::seconds(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resolved.status, AlertIncidentStatus::Resolved);
    assert_eq!(resolved.recovery_started_at, recovery_started_at);
    let persisted_recovery_started_at: String =
        sqlx::query_scalar("SELECT recovery_started_at FROM alert_incidents WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&persisted_recovery_started_at)
            .unwrap()
            .timestamp_subsec_nanos(),
        222_333_000
    );
    assert!(
        AlertIncidentRepository::remind_incident(&store, id, resolved.state_version, Utc::now(),)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_opens_incident_and_enqueues_opened_notification_atomically() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;
    let incident_id = uuid::Uuid::now_v7();
    let created = AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, incident_id, AlertIncidentStatus::Pending),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    let opened_at = Utc::now().with_nanosecond(123_456_789).unwrap();

    let opened = AlertIncidentRepository::open_incident_with_notification(
        &store,
        incident_id,
        created.state_version,
        opened_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Opened,
            dedupe_key: "incident-opened:atomic".to_owned(),
            subject: "opened".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: opened_at,
        },
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(opened.status, AlertIncidentStatus::Open);
    assert_eq!(
        opened.opened_at.unwrap().timestamp_subsec_nanos(),
        123_456_000
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox
             WHERE incident_id = ? AND dedupe_key = ?",
        )
        .bind(incident_id.to_string())
        .bind("incident-opened:atomic")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_notification_transitions_reject_stale_and_duplicate_requests_atomically() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;
    let incident_id = uuid::Uuid::now_v7();
    let created = AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, incident_id, AlertIncidentStatus::Pending),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    let opened_at = Utc::now().with_nanosecond(987_654_321).unwrap();
    let opened = AlertIncidentRepository::open_incident_with_notification(
        &store,
        incident_id,
        created.state_version,
        opened_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Opened,
            dedupe_key: "incident-opened:rollback".to_owned(),
            subject: "opened".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: opened_at,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(opened.state_version, 1);

    assert!(
        AlertIncidentRepository::open_incident_with_notification(
            &store,
            incident_id,
            created.state_version,
            opened_at,
            NewNotificationOutboxEntry {
                id: uuid::Uuid::now_v7(),
                kind: NotificationKind::Opened,
                dedupe_key: "incident-opened:stale".to_owned(),
                subject: "stale".to_owned(),
                body: "body".to_owned(),
                next_attempt_at: opened_at,
            },
        )
        .await
        .unwrap()
        .is_none()
    );

    let resolved_at = Utc::now().with_nanosecond(111_222_333).unwrap();
    assert!(
        AlertIncidentRepository::resolve_incident_with_notification(
            &store,
            incident_id,
            opened.state_version,
            resolved_at,
            NewNotificationOutboxEntry {
                id: uuid::Uuid::now_v7(),
                kind: NotificationKind::Resolved,
                dedupe_key: "incident-opened:rollback".to_owned(),
                subject: "duplicate".to_owned(),
                body: "body".to_owned(),
                next_attempt_at: resolved_at,
            },
        )
        .await
        .is_err()
    );
    let after_duplicate: (String, i64) =
        sqlx::query_as("SELECT status, state_version FROM alert_incidents WHERE id = ?")
            .bind(incident_id.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(after_duplicate, ("open".to_owned(), 1));

    let reminded_at = Utc::now().with_nanosecond(444_555_666).unwrap();
    let reminded = AlertIncidentRepository::remind_incident_with_notification(
        &store,
        incident_id,
        opened.state_version,
        reminded_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Reminder,
            dedupe_key: "incident-reminder:atomic".to_owned(),
            subject: "reminder".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: reminded_at,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        reminded.last_reminder_at.unwrap().timestamp_subsec_nanos(),
        444_555_000
    );

    let resolved = AlertIncidentRepository::resolve_incident_with_notification(
        &store,
        incident_id,
        reminded.state_version,
        resolved_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Resolved,
            dedupe_key: "incident-resolved:atomic".to_owned(),
            subject: "resolved".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: resolved_at,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        resolved.resolved_at.unwrap().timestamp_subsec_nanos(),
        111_222_000
    );
}

#[tokio::test]
async fn sqlite_direct_resolution_sets_recovery_started_at() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;
    let id = uuid::Uuid::now_v7();
    let opened = AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, id, AlertIncidentStatus::Open),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    let resolved_at = Utc::now() + Duration::seconds(1);

    let resolved =
        AlertIncidentRepository::resolve_incident(&store, id, opened.state_version, resolved_at)
            .await
            .unwrap()
            .unwrap();

    assert_eq!(resolved.status, AlertIncidentStatus::Resolved);
    assert_eq!(resolved.recovery_started_at, Some(resolved_at));
}

#[tokio::test]
async fn sqlite_enqueue_notification_returns_original_for_exact_duplicate_key() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;
    let incident_id = uuid::Uuid::now_v7();
    AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, incident_id, AlertIncidentStatus::Open),
        None,
    )
    .await
    .unwrap();
    let entry = NewNotificationOutboxEntry {
        id: uuid::Uuid::now_v7(),
        kind: NotificationKind::Reminder,
        dedupe_key: "incident-reminder:exact".to_owned(),
        subject: "subject".to_owned(),
        body: "body".to_owned(),
        next_attempt_at: Utc::now(),
    };
    let first = AlertIncidentRepository::enqueue_notification(&store, incident_id, entry.clone())
        .await
        .unwrap();
    let second = AlertIncidentRepository::enqueue_notification(&store, incident_id, entry)
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_open_incident_rolls_back_when_opened_notification_conflicts() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    rule(&store, rule_id).await;
    let dedupe_key = "incident-opened:rollback";
    let first_id = uuid::Uuid::now_v7();
    let first_incident = uuid::Uuid::now_v7();
    AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, first_incident, AlertIncidentStatus::Open),
        Some(NewNotificationOutboxEntry {
            id: first_id,
            kind: NotificationKind::Opened,
            dedupe_key: dedupe_key.to_owned(),
            subject: "first".to_owned(),
            body: "first".to_owned(),
            next_attempt_at: Utc::now(),
        }),
    )
    .await
    .unwrap();

    let second_incident = uuid::Uuid::now_v7();
    let mut second = incident(rule_id, second_incident, AlertIncidentStatus::Open);
    second.device_id = "rollback-device".to_owned();
    assert!(
        AlertIncidentRepository::create_incident(
            &store,
            second,
            Some(NewNotificationOutboxEntry {
                id: uuid::Uuid::now_v7(),
                kind: NotificationKind::Opened,
                dedupe_key: dedupe_key.to_owned(),
                subject: "second".to_owned(),
                body: "second".to_owned(),
                next_attempt_at: Utc::now(),
            }),
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents WHERE id = ?")
            .bind(second_incident.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

async fn timescale_store() -> (PgConnection, PlatformStore) {
    let url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let mut connection = PgConnection::connect(&url).await.unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(database.starts_with("iot_nano_test_"));
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (connection, store)
}

#[tokio::test]
#[ignore = "requires a disposable Timescale URL"]
async fn timescale_incident_repository_matches_sqlite_transition_and_dedupe_contract() {
    let (_lock, store) = timescale_store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold
         ) VALUES ($1, 'incident-rule', 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(rule_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let incident_id = uuid::Uuid::now_v7();
    let created = AlertIncidentRepository::create_incident(
        &store,
        incident(rule_id, incident_id, AlertIncidentStatus::Pending),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    let opened_at = Utc::now().with_nanosecond(123_456_789).unwrap();
    assert!(
        AlertIncidentRepository::create_incident(
            &store,
            incident(rule_id, uuid::Uuid::now_v7(), AlertIncidentStatus::Pending),
            Some(NewNotificationOutboxEntry {
                id: uuid::Uuid::now_v7(),
                kind: NotificationKind::Opened,
                dedupe_key: "incident-pending:timescale-duplicate".to_owned(),
                subject: "duplicate".to_owned(),
                body: "body".to_owned(),
                next_attempt_at: opened_at,
            }),
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    let opened = AlertIncidentRepository::open_incident_with_notification(
        &store,
        incident_id,
        created.state_version,
        opened_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Opened,
            dedupe_key: "incident-opened:timescale".to_owned(),
            subject: "opened".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: opened_at,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(opened.status, AlertIncidentStatus::Open);
    assert_eq!(
        opened.opened_at.unwrap().timestamp_subsec_nanos(),
        123_456_000
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT EXTRACT(MICROSECONDS FROM next_attempt_at)::BIGINT % 1000000
             FROM notification_outbox WHERE dedupe_key = $1",
        )
        .bind("incident-opened:timescale")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        123_456
    );
    assert!(
        AlertIncidentRepository::remind_incident_with_notification(
            &store,
            incident_id,
            opened.state_version,
            opened_at + Duration::seconds(1),
            NewNotificationOutboxEntry {
                id: uuid::Uuid::now_v7(),
                kind: NotificationKind::Reminder,
                dedupe_key: "incident-opened:timescale".to_owned(),
                subject: "duplicate".to_owned(),
                body: "body".to_owned(),
                next_attempt_at: opened_at + Duration::seconds(1),
            },
        )
        .await
        .is_err()
    );
    assert!(
        AlertIncidentRepository::open_incident_with_notification(
            &store,
            incident_id,
            created.state_version,
            opened_at,
            NewNotificationOutboxEntry {
                id: uuid::Uuid::now_v7(),
                kind: NotificationKind::Opened,
                dedupe_key: "incident-opened:timescale-stale".to_owned(),
                subject: "stale".to_owned(),
                body: "body".to_owned(),
                next_attempt_at: opened_at,
            },
        )
        .await
        .unwrap()
        .is_none()
    );
    let reminded_at = Utc::now().with_nanosecond(222_333_444).unwrap();
    let reminded = AlertIncidentRepository::remind_incident_with_notification(
        &store,
        incident_id,
        opened.state_version,
        reminded_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Reminder,
            dedupe_key: "incident-reminder:timescale".to_owned(),
            subject: "reminder".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: reminded_at,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        reminded.last_reminder_at.unwrap().timestamp_subsec_nanos(),
        222_333_000
    );
    let notification = NewNotificationOutboxEntry {
        id: uuid::Uuid::now_v7(),
        kind: NotificationKind::Reminder,
        dedupe_key: "incident-reminder:timescale".to_owned(),
        subject: "subject".to_owned(),
        body: "body".to_owned(),
        next_attempt_at: Utc::now(),
    };
    let first =
        AlertIncidentRepository::enqueue_notification(&store, incident_id, notification.clone())
            .await
            .unwrap();
    let second = AlertIncidentRepository::enqueue_notification(&store, incident_id, notification)
        .await
        .unwrap();
    assert_eq!(first, second);

    let direct_id = uuid::Uuid::now_v7();
    let mut direct = incident(rule_id, direct_id, AlertIncidentStatus::Open);
    direct.device_id = "direct-resolve-device".to_owned();
    let opened = AlertIncidentRepository::create_incident(&store, direct, None)
        .await
        .unwrap()
        .unwrap();
    let recovery_started_at = Utc::now().with_nanosecond(333_444_555).unwrap();
    let recovering = AlertIncidentRepository::recover_incident(
        &store,
        direct_id,
        opened.state_version,
        recovery_started_at,
    )
    .await
    .unwrap()
    .unwrap();
    let resolved_at = recovery_started_at + Duration::seconds(1);
    let resolved = AlertIncidentRepository::resolve_incident_with_notification(
        &store,
        direct_id,
        recovering.state_version,
        resolved_at,
        NewNotificationOutboxEntry {
            id: uuid::Uuid::now_v7(),
            kind: NotificationKind::Resolved,
            dedupe_key: "incident-resolved:timescale".to_owned(),
            subject: "resolved".to_owned(),
            body: "body".to_owned(),
            next_attempt_at: resolved_at,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        resolved.recovery_started_at,
        Some(recovery_started_at.with_nanosecond(333_444_000).unwrap())
    );
    let persisted_recovery_started_at: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT recovery_started_at FROM alert_incidents WHERE id = $1")
            .bind(direct_id)
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(
        persisted_recovery_started_at,
        recovery_started_at.with_nanosecond(333_444_000).unwrap()
    );
}
