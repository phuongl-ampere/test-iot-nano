use std::{
    env,
    fs::{File, OpenOptions},
    future::Future,
    path::Path,
    pin::Pin,
    sync::{LazyLock, Mutex},
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_ingest::{
    EmailSender, NotificationDispatcher, NotificationError, SmtpConfig, SmtpConfigInput,
    SqliteNotificationDispatcher, load_live_smtp_config, migrate,
};
use iot_storage::SqliteStore;
use sqlx::{PgPool, Row, SqlitePool};
use uuid::Uuid;

static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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

async fn prepared_pool() -> PgPool {
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the local TimescaleDB test database");
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "TRUNCATE device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices",
    )
        .execute(&pool)
        .await
        .unwrap();
    pool
}

fn lock_database_file() -> File {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(env::temp_dir().join("rush-iot-nano-timescaledb-tests.lock"))
        .unwrap();
    file.lock_exclusive().unwrap();
    file
}

async fn seed_outbox(pool: &PgPool, now: DateTime<Utc>) -> Uuid {
    let rule_id = Uuid::new_v4();
    let incident_id = Uuid::new_v4();
    let outbox_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, window_seconds,
            for_seconds, resolve_after_seconds, reopen_grace_seconds, severity,
            reminder_interval_seconds
         ) VALUES ($1, 'High average', 'temperature_c', 'window_average', 'gt', 40.0, 300,
                   0, 0, 3600, 'warning', 86400)",
    )
    .bind(rule_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, state_version
         ) VALUES ($1, $2, 'esp-000123', 'open', $3, $3, 1)",
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
         ) VALUES ($1, $2, 'opened', 'notification-test', 'subject', 'body', $3)",
    )
    .bind(outbox_id)
    .bind(incident_id)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    outbox_id
}

async fn seed_sqlite_outbox(pool: &SqlitePool, now: DateTime<Utc>) -> String {
    let rule_id = Uuid::new_v4().to_string();
    let incident_id = Uuid::new_v4().to_string();
    let outbox_id = Uuid::new_v4().to_string();
    let timestamp = now.to_rfc3339();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, window_seconds,
            for_seconds, resolve_after_seconds, reopen_grace_seconds, severity,
            reminder_interval_seconds
         ) VALUES (?, 'High average', 'temperature_c', 'window_average', 'gt', 40.0, 300,
                   0, 0, 3600, 'warning', 86400)",
    )
    .bind(&rule_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, state_version
         ) VALUES (?, ?, 'esp-000123', 'open', ?, ?, 1)",
    )
    .bind(&incident_id)
    .bind(&rule_id)
    .bind(&timestamp)
    .bind(&timestamp)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES (?, ?, 'opened', 'sqlite-notification-test', 'subject', 'body', ?)",
    )
    .bind(&outbox_id)
    .bind(&incident_id)
    .bind(&timestamp)
    .execute(pool)
    .await
    .unwrap();
    outbox_id
}

#[tokio::test]
async fn sqlite_dispatcher_marks_a_sent_outbox_row_delivered() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc::now();
    let outbox_id = seed_sqlite_outbox(store.pool(), now).await;
    let dispatcher = SqliteNotificationDispatcher::new(store.pool().clone(), SuccessfulSender, 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    let row = sqlx::query(
        "SELECT state, attempt_count, sent_at, lease_until
         FROM notification_outbox
         WHERE id = ?",
    )
    .bind(outbox_id)
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 1);
    assert_eq!(result.retried, 0);
    assert_eq!(row.get::<String, _>("state"), "sent");
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
    assert!(row.get::<Option<String>, _>("sent_at").is_some());
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
}

#[tokio::test]
async fn sqlite_dispatcher_reclaims_an_expired_lease_and_delivers_it() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc::now();
    let outbox_id = seed_sqlite_outbox(store.pool(), now).await;
    sqlx::query(
        "UPDATE notification_outbox
         SET state = 'leased', attempt_count = 1, lease_until = ?
         WHERE id = ?",
    )
    .bind((now - Duration::seconds(1)).to_rfc3339())
    .bind(&outbox_id)
    .execute(store.pool())
    .await
    .unwrap();
    let dispatcher = SqliteNotificationDispatcher::new(store.pool().clone(), SuccessfulSender, 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    let row = sqlx::query(
        "SELECT state, attempt_count, sent_at, lease_until
         FROM notification_outbox
         WHERE id = ?",
    )
    .bind(outbox_id)
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 1);
    assert_eq!(result.retried, 0);
    assert_eq!(row.get::<String, _>("state"), "sent");
    assert_eq!(row.get::<i64, _>("attempt_count"), 2);
    assert!(row.get::<Option<String>, _>("sent_at").is_some());
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
}

#[tokio::test]
async fn sqlite_dispatcher_releases_a_failed_outbox_row_for_retry() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc::now();
    let outbox_id = seed_sqlite_outbox(store.pool(), now).await;
    let dispatcher = SqliteNotificationDispatcher::new(store.pool().clone(), FailingSender, 10)
        .with_delivery_policy(
            Duration::seconds(120),
            Duration::seconds(7),
            Duration::seconds(60),
        );

    let result = dispatcher.dispatch_once(now).await.unwrap();

    let row = sqlx::query(
        "SELECT state, attempt_count, next_attempt_at, lease_until, last_error
         FROM notification_outbox
         WHERE id = ?",
    )
    .bind(outbox_id)
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 0);
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
async fn sqlite_dispatcher_releases_a_timed_out_outbox_row_for_retry() {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let now = Utc::now();
    let outbox_id = seed_sqlite_outbox(store.pool(), now).await;
    let dispatcher = SqliteNotificationDispatcher::new(store.pool().clone(), SlowSender, 10)
        .with_timeout(StdDuration::from_millis(1));

    let result = dispatcher.dispatch_once(now).await.unwrap();

    let row = sqlx::query(
        "SELECT state, attempt_count, lease_until, last_error
         FROM notification_outbox
         WHERE id = ?",
    )
    .bind(outbox_id)
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.sent, 0);
    assert_eq!(result.retried, 1);
    assert_eq!(row.get::<String, _>("state"), "pending");
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert_eq!(
        row.get::<Option<String>, _>("last_error").as_deref(),
        Some("SMTP send timed out")
    );
}

#[tokio::test]
async fn failed_email_returns_leased_outbox_row_to_pending_with_backoff() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let now = Utc::now();
    let outbox_id = seed_outbox(&pool, now).await;
    let dispatcher = NotificationDispatcher::new(pool.clone(), FailingSender, 10);

    dispatcher.dispatch_once(now).await.unwrap();

    let row = sqlx::query(
        "SELECT state, attempt_count, next_attempt_at, lease_until
         FROM notification_outbox
         WHERE id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.get::<String, _>("state"), "pending");
    assert_eq!(row.get::<i32, _>("attempt_count"), 1);
    assert!(row.get::<DateTime<Utc>, _>("next_attempt_at") > now);
    assert!(row.get::<Option<DateTime<Utc>>, _>("lease_until").is_none());
}

#[tokio::test]
async fn successful_email_marks_outbox_row_sent() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let now = Utc::now();
    let outbox_id = seed_outbox(&pool, now).await;
    let dispatcher = NotificationDispatcher::new(pool.clone(), SuccessfulSender, 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    let row = sqlx::query(
        "SELECT state, sent_at, lease_until
         FROM notification_outbox
         WHERE id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(result.sent, 1);
    assert_eq!(row.get::<String, _>("state"), "sent");
    assert!(row.get::<Option<DateTime<Utc>>, _>("sent_at").is_some());
    assert!(row.get::<Option<DateTime<Utc>>, _>("lease_until").is_none());
}

#[tokio::test]
async fn notification_delivery_policy_controls_the_retry_delay() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = prepared_pool().await;
    let now = Utc::now();
    let outbox_id = seed_outbox(&pool, now).await;
    let dispatcher = NotificationDispatcher::new(pool.clone(), FailingSender, 10)
        .with_delivery_policy(
            Duration::seconds(120),
            Duration::seconds(7),
            Duration::seconds(60),
        );

    dispatcher.dispatch_once(now).await.unwrap();

    let next_attempt_at = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_attempt_at FROM notification_outbox WHERE id = $1",
    )
    .bind(outbox_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(next_attempt_at, now + Duration::seconds(7));
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
