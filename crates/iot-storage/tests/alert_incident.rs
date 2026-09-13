use chrono::{Duration, Timelike, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AlertIncidentRepository, AlertIncidentStatus, NewAlertIncident, NewNotificationOutboxEntry,
    NotificationKind, PlatformStore,
};
use sqlx::{Connection, PgConnection};

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
    let recovering =
        AlertIncidentRepository::recover_incident(&store, id, opened.state_version, Utc::now())
            .await
            .unwrap()
            .unwrap();
    let resolved = AlertIncidentRepository::resolve_incident(
        &store,
        id,
        recovering.state_version,
        Utc::now() + Duration::seconds(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(resolved.status, AlertIncidentStatus::Resolved);
    assert!(
        AlertIncidentRepository::remind_incident(&store, id, resolved.state_version, Utc::now(),)
            .await
            .unwrap()
            .is_none()
    );
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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:platform-storage-test'))")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut connection)
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
    let opened = AlertIncidentRepository::open_incident(
        &store,
        incident_id,
        created.state_version,
        Utc::now(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(opened.status, AlertIncidentStatus::Open);
    assert!(
        AlertIncidentRepository::open_incident(
            &store,
            incident_id,
            created.state_version,
            Utc::now()
        )
        .await
        .unwrap()
        .is_none()
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
}
