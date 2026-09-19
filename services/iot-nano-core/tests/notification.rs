use std::{future::Future, path::Path, pin::Pin, sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Duration, Utc};
use iot_nano_core::{
    EmailSender, NotificationError, PlatformNotificationDispatcher, SmtpConfig, SmtpConfigInput,
    load_live_smtp_config,
};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{NotificationRepository, PlatformStore};
use sqlx::{Connection, PgConnection, Row};
use tokio::sync::Notify;
use uuid::Uuid;

const PLATFORM_TENANT_ID: Uuid = Uuid::from_u128(1);

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_platform_store() -> (TimescaleTestLock, PlatformStore) {
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

#[derive(Clone)]
struct FailingSender;

impl EmailSender for FailingSender {
    fn send(
        &self,
        _subject: String,
        _body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        Box::pin(async { Err(NotificationError::Send("test SMTP failure".to_owned())) })
    }
}

#[derive(Clone)]
struct SuccessfulSender;

impl EmailSender for SuccessfulSender {
    fn send(
        &self,
        _subject: String,
        _body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
struct SlowSender;

impl EmailSender for SlowSender {
    fn send(
        &self,
        _subject: String,
        _body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        Box::pin(async {
            tokio::time::sleep(StdDuration::from_secs(1)).await;
            Ok(())
        })
    }
}

#[derive(Clone)]
struct InFlightFailingSender {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl EmailSender for InFlightFailingSender {
    fn send(
        &self,
        _subject: String,
        _body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            Err(NotificationError::Send("in-flight SMTP failure".to_owned()))
        })
    }
}

async fn platform_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn seed_platform_outbox(
    store: &PlatformStore,
    tenant_id: Uuid,
    id: Uuid,
    now: DateTime<Utc>,
) {
    let pool = store.sqlite_pool().unwrap();
    let rule_id = Uuid::new_v4();
    let incident_id = Uuid::new_v4();
    let device_id = format!("notification-device-{tenant_id}");
    let timestamp = now.to_rfc3339();
    sqlx::query(
        "INSERT OR IGNORE INTO tenants (id, slug, status, metadata)
         VALUES (?, ?, 'active', '{}')",
    )
    .bind(tenant_id.to_string())
    .bind(format!("notification-dispatcher-{tenant_id}"))
    .execute(pool)
    .await
    .unwrap();
    store.register_device(tenant_id, &device_id).await.unwrap();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, tenant_id, name, metric_key, rule_type, comparison, threshold
         ) VALUES (?, ?, ?, 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(rule_id.to_string())
    .bind(tenant_id.to_string())
    .bind(format!("rule-{id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at
         ) VALUES (?, ?, ?, ?, 'open', ?)",
    )
    .bind(incident_id.to_string())
    .bind(tenant_id.to_string())
    .bind(rule_id.to_string())
    .bind(&device_id)
    .bind(&timestamp)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES (?, ?, ?, 'opened', ?, 'subject', 'body', ?)",
    )
    .bind(id.to_string())
    .bind(tenant_id.to_string())
    .bind(incident_id.to_string())
    .bind(format!("dedupe-{id}"))
    .bind(timestamp)
    .execute(pool)
    .await
    .unwrap();
}

async fn register_timescale_platform_tenant(store: &PlatformStore, tenant_id: Uuid, slug: &str) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES ($1, $2, 'active', '{}'::jsonb)",
    )
    .bind(tenant_id)
    .bind(slug)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    store
        .register_device(
            tenant_id,
            &format!("timescale-notification-device-{tenant_id}"),
        )
        .await
        .unwrap();
}

async fn seed_timescale_platform_outbox(
    store: &PlatformStore,
    tenant_id: Uuid,
    id: Uuid,
    now: DateTime<Utc>,
) {
    let pool = store.timescale_pool().unwrap();
    let rule_id = Uuid::new_v4();
    let incident_id = Uuid::new_v4();
    let device_id = format!("timescale-notification-device-{tenant_id}");
    sqlx::query(
        "INSERT INTO alert_rules (
            id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold
         ) VALUES ($1, $2, $3, $4, 'temperature_c', 'event_threshold', 'gt', 30)",
    )
    .bind(rule_id)
    .bind(tenant_id)
    .bind(format!("timescale-rule-{id}"))
    .bind(&device_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at
         ) VALUES ($1, $2, $3, $4, 'open', $5)",
    )
    .bind(incident_id)
    .bind(tenant_id)
    .bind(rule_id)
    .bind(&device_id)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, 'opened', $4, 'subject', 'body', $5)",
    )
    .bind(id)
    .bind(tenant_id)
    .bind(incident_id)
    .bind(format!("timescale-dedupe-{id}"))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

async fn timescale_notification_state(
    store: &PlatformStore,
    tenant_id: Uuid,
    notification_id: Uuid,
) -> String {
    sqlx::query_scalar(
        "SELECT state
         FROM notification_outbox
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id)
    .bind(notification_id)
    .fetch_one(store.timescale_pool().unwrap())
    .await
    .unwrap()
}

#[tokio::test]
async fn platform_dispatcher_marks_a_platform_outbox_row_sent() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    seed_platform_outbox(&store, PLATFORM_TENANT_ID, Uuid::new_v4(), now).await;
    let dispatcher = PlatformNotificationDispatcher::new(Arc::new(store), SuccessfulSender, 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 1);
    assert_eq!(result.retried, 0);
}

#[tokio::test]
async fn platform_dispatcher_enforces_a_global_batch_bound_and_rotates_tenants() {
    let (_directory, store) = platform_store().await;
    let tenant_a = PLATFORM_TENANT_ID;
    let tenant_b = Uuid::from_u128(2);
    let tenant_c = Uuid::from_u128(3);
    let now = Utc::now();
    for tenant_id in [tenant_a, tenant_b, tenant_c] {
        seed_platform_outbox(&store, tenant_id, Uuid::new_v4(), now).await;
        seed_platform_outbox(&store, tenant_id, Uuid::new_v4(), now).await;
    }
    let mut non_ready_notifications = Vec::new();
    for offset in 0_u128..10 {
        let tenant_id = Uuid::from_u128(100 + offset);
        let notification_id = Uuid::new_v4();
        let leased = offset % 2 == 1;
        seed_platform_outbox(&store, tenant_id, notification_id, now).await;
        if leased {
            sqlx::query(
                "UPDATE notification_outbox
                 SET state = 'leased', lease_until = ?
                 WHERE tenant_id = ? AND id = ?",
            )
            .bind((now + Duration::hours(1)).to_rfc3339())
            .bind(tenant_id.to_string())
            .bind(notification_id.to_string())
            .execute(store.sqlite_pool().unwrap())
            .await
            .unwrap();
        } else {
            sqlx::query(
                "UPDATE notification_outbox
                 SET next_attempt_at = ?
                 WHERE tenant_id = ? AND id = ?",
            )
            .bind((now + Duration::hours(1)).to_rfc3339())
            .bind(tenant_id.to_string())
            .bind(notification_id.to_string())
            .execute(store.sqlite_pool().unwrap())
            .await
            .unwrap();
        }
        non_ready_notifications.push((
            tenant_id,
            notification_id,
            if leased { "leased" } else { "pending" },
        ));
    }
    let dispatcher =
        PlatformNotificationDispatcher::new(Arc::new(store.clone()), SuccessfulSender, 2);

    let first = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(first.claimed, 2);
    assert_eq!(first.sent, 2);
    assert!(first.claimed <= 2);
    assert!(first.sent <= 2);
    let first_sent_tenants = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT tenant_id
         FROM notification_outbox
         WHERE state = 'sent'
         ORDER BY tenant_id",
    )
    .fetch_all(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        first_sent_tenants,
        vec![tenant_a.to_string(), tenant_b.to_string()]
    );

    let second = dispatcher
        .dispatch_once(now + Duration::seconds(1))
        .await
        .unwrap();

    assert_eq!(second.claimed, 2);
    assert_eq!(second.sent, 2);
    assert!(second.claimed <= 2);
    assert!(second.sent <= 2);
    let sent_by_tenant = sqlx::query_as::<_, (String, i64)>(
        "SELECT tenant_id, COUNT(*)
         FROM notification_outbox
         WHERE state = 'sent'
         GROUP BY tenant_id
         ORDER BY tenant_id",
    )
    .fetch_all(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        sent_by_tenant,
        vec![
            (tenant_a.to_string(), 2),
            (tenant_b.to_string(), 1),
            (tenant_c.to_string(), 1),
        ]
    );
    for (tenant_id, notification_id, expected_state) in non_ready_notifications {
        let state = sqlx::query_scalar::<_, String>(
            "SELECT state
             FROM notification_outbox
             WHERE tenant_id = ? AND id = ?",
        )
        .bind(tenant_id.to_string())
        .bind(notification_id.to_string())
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
        assert_eq!(state, expected_state);
    }
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_platform_dispatcher_enforces_a_global_batch_bound_rotates_ready_tenants_and_filters_unready_leases()
 {
    let (_lock, store) = timescale_platform_store().await;
    let tenant_a = PLATFORM_TENANT_ID;
    let tenant_b = Uuid::from_u128(2);
    let tenant_c = Uuid::from_u128(3);
    for (tenant_id, slug) in [
        (tenant_a, "timescale-notification-a"),
        (tenant_b, "timescale-notification-b"),
        (tenant_c, "timescale-notification-c"),
    ] {
        register_timescale_platform_tenant(&store, tenant_id, slug).await;
    }
    let now = Utc::now();

    let reclaimable_id = Uuid::now_v7();
    seed_timescale_platform_outbox(&store, tenant_a, reclaimable_id, now).await;
    sqlx::query(
        "UPDATE notification_outbox
         SET state = 'leased', lease_until = $1
         WHERE tenant_id = $2 AND id = $3",
    )
    .bind(now - Duration::seconds(1))
    .bind(tenant_a)
    .bind(reclaimable_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let a_queued_id = Uuid::now_v7();
    seed_timescale_platform_outbox(&store, tenant_a, a_queued_id, now).await;
    sqlx::query(
        "UPDATE notification_outbox
         SET next_attempt_at = $1
         WHERE tenant_id = $2 AND id = $3",
    )
    .bind(now + Duration::milliseconds(1))
    .bind(tenant_a)
    .bind(a_queued_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let b_id = Uuid::now_v7();
    seed_timescale_platform_outbox(&store, tenant_b, b_id, now).await;
    let c_id = Uuid::now_v7();
    seed_timescale_platform_outbox(&store, tenant_c, c_id, now).await;

    let future_tenant = Uuid::from_u128(100);
    let future_id = Uuid::now_v7();
    register_timescale_platform_tenant(&store, future_tenant, "timescale-notification-future")
        .await;
    seed_timescale_platform_outbox(&store, future_tenant, future_id, now).await;
    sqlx::query(
        "UPDATE notification_outbox
         SET next_attempt_at = $1
         WHERE tenant_id = $2 AND id = $3",
    )
    .bind(now + Duration::hours(1))
    .bind(future_tenant)
    .bind(future_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let active_lease_tenant = Uuid::from_u128(101);
    let active_lease_id = Uuid::now_v7();
    register_timescale_platform_tenant(
        &store,
        active_lease_tenant,
        "timescale-notification-active-lease",
    )
    .await;
    seed_timescale_platform_outbox(&store, active_lease_tenant, active_lease_id, now).await;
    sqlx::query(
        "UPDATE notification_outbox
         SET state = 'leased', lease_until = $1
         WHERE tenant_id = $2 AND id = $3",
    )
    .bind(now + Duration::hours(1))
    .bind(active_lease_tenant)
    .bind(active_lease_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let dispatcher =
        PlatformNotificationDispatcher::new(Arc::new(store.clone()), SuccessfulSender, 2);

    let first = dispatcher.dispatch_once(now).await.unwrap();
    assert_eq!(first.claimed, 2);
    assert_eq!(first.sent, 2);
    assert!(first.claimed <= 2);
    assert!(first.sent <= 2);
    assert_eq!(
        timescale_notification_state(&store, tenant_a, reclaimable_id).await,
        "sent"
    );
    assert_eq!(
        timescale_notification_state(&store, tenant_b, b_id).await,
        "sent"
    );
    assert_eq!(
        timescale_notification_state(&store, tenant_a, a_queued_id).await,
        "pending"
    );
    assert_eq!(
        timescale_notification_state(&store, tenant_c, c_id).await,
        "pending"
    );

    let second = dispatcher
        .dispatch_once(now + Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(second.claimed, 2);
    assert_eq!(second.sent, 2);
    assert!(second.claimed <= 2);
    assert!(second.sent <= 2);
    assert_eq!(
        timescale_notification_state(&store, tenant_c, c_id).await,
        "sent"
    );
    assert_eq!(
        timescale_notification_state(&store, tenant_a, a_queued_id).await,
        "sent"
    );
    assert_eq!(
        timescale_notification_state(&store, future_tenant, future_id).await,
        "pending"
    );
    assert_eq!(
        timescale_notification_state(&store, active_lease_tenant, active_lease_id).await,
        "leased"
    );
}

#[tokio::test]
async fn platform_dispatcher_releases_failed_delivery_with_exponential_backoff() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let id = Uuid::new_v4();
    seed_platform_outbox(&store, PLATFORM_TENANT_ID, id, now).await;
    let dispatcher =
        PlatformNotificationDispatcher::new(Arc::new(store.clone()), FailingSender, 10)
            .with_delivery_policy(
                Duration::seconds(120),
                Duration::seconds(7),
                Duration::seconds(60),
            );

    let result = dispatcher.dispatch_once(now).await.unwrap();
    let row = sqlx::query(
        "SELECT state, attempt_count, next_attempt_at, lease_until, last_error
         FROM notification_outbox WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    assert_eq!(result.retried, 1);
    assert_eq!(row.get::<String, _>("state"), "pending");
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
    assert_eq!(
        row.get::<String, _>("next_attempt_at"),
        (now + Duration::seconds(7)).to_rfc3339()
    );
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert_eq!(
        row.get::<Option<String>, _>("last_error").as_deref(),
        Some("test SMTP failure")
    );
}

#[tokio::test]
async fn platform_dispatcher_does_not_count_a_retry_after_lease_reclaim() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let id = Uuid::new_v4();
    seed_platform_outbox(&store, PLATFORM_TENANT_ID, id, now).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let dispatcher = PlatformNotificationDispatcher::new(
        Arc::new(store.clone()),
        InFlightFailingSender {
            entered: entered.clone(),
            release: release.clone(),
        },
        10,
    )
    .with_delivery_policy(
        Duration::seconds(1),
        Duration::seconds(7),
        Duration::seconds(60),
    );

    let dispatch = tokio::spawn(async move { dispatcher.dispatch_once(now).await.unwrap() });
    entered.notified().await;

    let reclaimed_at = now + Duration::seconds(2);
    let reclaimed = NotificationRepository::claim_notifications(
        &store,
        PLATFORM_TENANT_ID,
        reclaimed_at,
        reclaimed_at + Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    assert_eq!(reclaimed.len(), 1);
    let current_lease_until = reclaimed[0].lease_until.unwrap();

    release.notify_one();
    let result = dispatch.await.unwrap();
    let row = sqlx::query(
        "SELECT state, attempt_count, lease_until, last_error
         FROM notification_outbox WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 0);
    assert_eq!(result.retried, 0);
    assert_eq!(row.get::<String, _>("state"), "leased");
    assert_eq!(row.get::<i64, _>("attempt_count"), 2);
    assert_eq!(
        row.get::<Option<String>, _>("lease_until").as_deref(),
        Some(current_lease_until.to_rfc3339().as_str())
    );
    assert!(row.get::<Option<String>, _>("last_error").is_none());
}

#[tokio::test]
async fn platform_dispatcher_releases_timed_out_delivery() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let id = Uuid::new_v4();
    seed_platform_outbox(&store, PLATFORM_TENANT_ID, id, now).await;
    let dispatcher = PlatformNotificationDispatcher::new(Arc::new(store.clone()), SlowSender, 10)
        .with_timeout(StdDuration::from_millis(1));

    let result = dispatcher.dispatch_once(now).await.unwrap();
    let row = sqlx::query(
        "SELECT state, lease_until, last_error
         FROM notification_outbox WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.retried, 1);
    assert_eq!(row.get::<String, _>("state"), "pending");
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert_eq!(
        row.get::<Option<String>, _>("last_error").as_deref(),
        Some("SMTP send timed out")
    );
}

#[tokio::test]
async fn platform_dispatcher_does_not_claim_a_still_leased_notification() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let id = Uuid::new_v4();
    seed_platform_outbox(&store, PLATFORM_TENANT_ID, id, now).await;
    let lease_until = now + Duration::seconds(30);
    let claimed = NotificationRepository::claim_notifications(
        &store,
        PLATFORM_TENANT_ID,
        now,
        lease_until,
        1,
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);

    let dispatcher = PlatformNotificationDispatcher::new(Arc::new(store), SuccessfulSender, 10);
    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.claimed, 0);
    assert_eq!(result.sent, 0);
    assert_eq!(result.retried, 0);
}

#[tokio::test]
async fn platform_dispatcher_reclaims_an_expired_lease() {
    let (_directory, store) = platform_store().await;
    let claim_time = Utc::now() - Duration::seconds(31);
    let id = Uuid::new_v4();
    seed_platform_outbox(&store, PLATFORM_TENANT_ID, id, claim_time).await;
    let claimed = NotificationRepository::claim_notifications(
        &store,
        PLATFORM_TENANT_ID,
        claim_time,
        claim_time + Duration::seconds(1),
        1,
    )
    .await
    .unwrap();
    assert_eq!(claimed.len(), 1);

    let dispatcher =
        PlatformNotificationDispatcher::new(Arc::new(store.clone()), SuccessfulSender, 10);
    let now = claim_time + Duration::seconds(31);
    let result = dispatcher.dispatch_once(now).await.unwrap();

    let state =
        sqlx::query_scalar::<_, String>("SELECT state FROM notification_outbox WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 1);
    assert_eq!(state, "sent");
}

#[test]
fn partial_smtp_configuration_is_rejected() {
    let result = SmtpConfig::from_input(SmtpConfigInput {
        host: Some("smtp.example.test".to_owned()),
        ..SmtpConfigInput::default()
    });

    assert!(matches!(result, Err(NotificationError::Configuration(_))));
}

#[test]
fn complete_smtp_configuration_uses_ssl_defaults() {
    let config = SmtpConfig::from_input(SmtpConfigInput {
        host: Some("smtp.example.test".to_owned()),
        username: Some("user@example.test".to_owned()),
        password: Some("secret".to_owned()),
        from: Some("user@example.test".to_owned()),
        to: Some("ops@example.test".to_owned()),
        ..SmtpConfigInput::default()
    })
    .unwrap()
    .unwrap();

    assert_eq!(config.port, 465);
    assert_eq!(config.timeout.as_secs(), 15);
}

#[test]
fn live_smtp_config_reloads_the_saved_file_without_process_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("smtp.env");
    std::fs::write(
        &path,
        "SMTP_HOST=smtp-first.example.test\nSMTP_USERNAME=alerts\nSMTP_PASSWORD=secret\nALERT_EMAIL_FROM=alerts@example.test\nALERT_EMAIL_TO=ops@example.test\n",
    )
    .unwrap();

    let first = load_live_smtp_config(&path, None).unwrap().unwrap();
    std::fs::write(
        &path,
        "SMTP_HOST=smtp-second.example.test\nSMTP_PORT=587\nSMTP_USERNAME=alerts\nSMTP_PASSWORD=secret\nALERT_EMAIL_FROM=alerts@example.test\nALERT_EMAIL_TO=ops@example.test\n",
    )
    .unwrap();
    let second = load_live_smtp_config(Path::new(&path), None)
        .unwrap()
        .unwrap();

    assert_eq!(first.host, "smtp-first.example.test");
    assert_eq!(second.host, "smtp-second.example.test");
    assert_eq!(second.port, 587);
}
