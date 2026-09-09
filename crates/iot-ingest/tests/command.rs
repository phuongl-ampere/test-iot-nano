use std::{
    env,
    fs::{File, OpenOptions},
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
};

use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_ingest::{
    CommandDispatcher, SqliteCommandDispatcher, TransportRpcClient, TransportRpcClientError,
    TransportRpcPublishRequest, migrate,
};
use iot_storage::{NewCommandOutboxEntry, SqliteStore};
use serde_json::json;
use sqlx::{PgPool, Row};
use uuid::Uuid;

static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Clone)]
struct RecordingTransport {
    requests: Arc<tokio::sync::Mutex<Vec<TransportRpcPublishRequest>>>,
    result: Result<(), TransportRpcClientError>,
}

impl RecordingTransport {
    fn succeeds() -> Self {
        Self {
            requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Ok(()),
        }
    }

    fn fails(message: &str) -> Self {
        Self {
            requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Err(TransportRpcClientError::Unavailable(message.to_owned())),
        }
    }

    fn unavailable_session() -> Self {
        Self {
            requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Err(TransportRpcClientError::UnexpectedStatus(503)),
        }
    }
}

impl TransportRpcClient for RecordingTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportRpcClientError>> + Send + '_>> {
        Box::pin(async move {
            self.requests.lock().await.push(request);
            self.result.clone()
        })
    }
}

#[derive(Clone, Default)]
struct WaitUntilExpiredTransport {
    requests: Arc<tokio::sync::Mutex<Vec<TransportRpcPublishRequest>>>,
}

impl TransportRpcClient for WaitUntilExpiredTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportRpcClientError>> + Send + '_>> {
        Box::pin(async move {
            self.requests.lock().await.push(request.clone());
            while Utc::now() < request.expires_at {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Ok(())
        })
    }
}

async fn sqlite_store() -> (tempfile::TempDir, SqliteStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn enqueue_sqlite_command(store: &SqliteStore, command_id: Uuid, now: DateTime<Utc>) {
    enqueue_sqlite_command_with_expiry(store, command_id, now, now + Duration::seconds(30)).await;
}

async fn enqueue_sqlite_command_with_expiry(
    store: &SqliteStore,
    command_id: Uuid,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) {
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-a')")
        .execute(store.pool())
        .await
        .unwrap();
    store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: json!({ "source": "dashboard" }).to_string(),
            mode: RpcMode::OneWay,
            expires_at,
            next_attempt_at: now,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_dispatcher_claims_then_marks_published_after_transport_puback() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    enqueue_sqlite_command(&store, command_id, now).await;
    let transport = RecordingTransport::succeeds();
    let dispatcher = SqliteCommandDispatcher::new(store.clone(), transport.clone(), 10);

    let dispatched_at = now + Duration::seconds(5);
    let result = dispatcher.dispatch_once(dispatched_at).await.unwrap();

    assert_eq!(result.expired, 0);
    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 1);
    assert_eq!(result.failed, 0);
    let requests = transport.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].device_id, "device-a");
    assert_eq!(requests[0].id, command_id);
    assert_eq!(requests[0].method, "sample_now");
    assert_eq!(requests[0].params, json!({ "source": "dashboard" }));
    assert_eq!(requests[0].mode, RpcMode::OneWay);
    assert_eq!(requests[0].issued_at, now);
    assert_eq!(requests[0].expires_at, now + Duration::seconds(30));
    drop(requests);

    let row = sqlx::query(
        "SELECT state, attempt_count, lease_until, published_at
         FROM command_outbox WHERE id = ?",
    )
    .bind(command_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "published_to_broker");
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert!(row.get::<Option<String>, _>("published_at").is_some());
}

#[tokio::test]
async fn sqlite_dispatcher_forwards_a_two_way_mode_to_the_transport() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-a')")
        .execute(store.pool())
        .await
        .unwrap();
    store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::TwoWay,
            expires_at: now + Duration::seconds(30),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let transport = RecordingTransport::succeeds();
    let dispatcher = SqliteCommandDispatcher::new(store, transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.published, 1);
    assert_eq!(transport.requests.lock().await[0].mode, RpcMode::TwoWay);
}

#[tokio::test]
async fn sqlite_dispatcher_marks_transport_failure_failed_without_a_false_delivery_claim() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    enqueue_sqlite_command(&store, command_id, now).await;
    let transport = RecordingTransport::fails("device is offline");
    let dispatcher = SqliteCommandDispatcher::new(store.clone(), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 1);
    assert_eq!(transport.requests.lock().await.len(), 1);
    let row = sqlx::query(
        "SELECT state, attempt_count, lease_until, published_at, last_error
         FROM command_outbox WHERE id = ?",
    )
    .bind(command_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "failed");
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert!(row.get::<Option<String>, _>("published_at").is_none());
    assert_eq!(
        row.get::<Option<String>, _>("last_error").as_deref(),
        Some("transport is unavailable: device is offline")
    );
}

#[tokio::test]
async fn sqlite_dispatcher_retries_an_offline_session_until_the_command_expires() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    enqueue_sqlite_command_with_expiry(&store, command_id, now, now + Duration::seconds(2)).await;
    let transport = RecordingTransport::unavailable_session();
    let dispatcher = SqliteCommandDispatcher::new(store.clone(), transport.clone(), 10);

    let first = dispatcher.dispatch_once(now).await.unwrap();
    let queued = sqlx::query(
        "SELECT state, lease_until, next_attempt_at, published_at
         FROM command_outbox WHERE id = ?",
    )
    .bind(command_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let after_expiry = dispatcher
        .dispatch_once(now + Duration::seconds(3))
        .await
        .unwrap();
    let state = sqlx::query_scalar::<_, String>("SELECT state FROM command_outbox WHERE id = ?")
        .bind(command_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();

    assert_eq!(first.claimed, 1);
    assert_eq!(first.published, 0);
    assert_eq!(first.failed, 0);
    assert_eq!(transport.requests.lock().await.len(), 1);
    assert_eq!(queued.get::<String, _>("state"), "queued");
    assert!(queued.get::<Option<String>, _>("lease_until").is_none());
    assert!(queued.get::<Option<String>, _>("published_at").is_none());
    assert!(queued.get::<String, _>("next_attempt_at") > now.to_rfc3339());
    assert_eq!(after_expiry.expired, 1);
    assert_eq!(state, "expired");
}

#[tokio::test]
async fn sqlite_dispatcher_expires_command_when_transport_returns_after_its_ttl() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    enqueue_sqlite_command_with_expiry(&store, command_id, now, now + Duration::milliseconds(250))
        .await;
    let transport = WaitUntilExpiredTransport::default();
    let dispatcher = SqliteCommandDispatcher::new(store.clone(), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.expired, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 0);
    assert_eq!(transport.requests.lock().await.len(), 1);
    let row = sqlx::query(
        "SELECT state, lease_until, published_at
         FROM command_outbox WHERE id = ?",
    )
    .bind(command_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "expired");
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert!(row.get::<Option<String>, _>("published_at").is_none());
}

#[tokio::test]
async fn sqlite_dispatcher_expires_stale_commands_before_they_are_sent() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-a')")
        .execute(store.pool())
        .await
        .unwrap();
    store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now - Duration::seconds(1),
            next_attempt_at: now - Duration::seconds(2),
        })
        .await
        .unwrap();
    let transport = RecordingTransport::succeeds();
    let dispatcher = SqliteCommandDispatcher::new(store.clone(), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.expired, 1);
    assert_eq!(result.claimed, 0);
    assert_eq!(transport.requests.lock().await.len(), 0);
    let state = sqlx::query_scalar::<_, String>("SELECT state FROM command_outbox WHERE id = ?")
        .bind(command_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(state, "expired");
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

#[tokio::test]
async fn postgres_dispatcher_uses_the_command_outbox_state_machine_when_local_database_is_available()
 {
    let Ok(database_url) = env::var("DATABASE_URL") else {
        return;
    };
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
    sqlx::query("INSERT INTO devices (device_id) VALUES ($1)")
        .bind(&device_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, expires_at, next_attempt_at
         ) VALUES ($1, $2, 'sample_now', $3, $4, $5)",
    )
    .bind(command_id)
    .bind(&device_id)
    .bind(sqlx::types::Json(json!({ "source": "dashboard" })))
    .bind(now + Duration::seconds(30))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    let transport = RecordingTransport::succeeds();
    let dispatcher = CommandDispatcher::new(pool.clone(), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 1);
    assert_eq!(result.failed, 0);
    assert_eq!(transport.requests.lock().await.len(), 1);
    let row = sqlx::query(
        "SELECT state, attempt_count, lease_until, published_at
         FROM command_outbox WHERE id = $1",
    )
    .bind(command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "published_to_broker");
    assert_eq!(row.get::<i32, _>("attempt_count"), 1);
    assert!(row.get::<Option<DateTime<Utc>>, _>("lease_until").is_none());
    assert!(
        row.get::<Option<DateTime<Utc>>, _>("published_at")
            .is_some()
    );
}

#[tokio::test]
async fn postgres_dispatchers_claim_one_command_only_once() {
    let Ok(database_url) = env::var("DATABASE_URL") else {
        return;
    };
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-claim-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
    sqlx::query("INSERT INTO devices (device_id) VALUES ($1)")
        .bind(&device_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, expires_at, next_attempt_at
         ) VALUES ($1, $2, 'sample_now', $3, $4, $5)",
    )
    .bind(command_id)
    .bind(&device_id)
    .bind(sqlx::types::Json(json!({})))
    .bind(now + Duration::seconds(30))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    let transport = RecordingTransport::succeeds();
    let first = CommandDispatcher::new(pool.clone(), transport.clone(), 10);
    let second = CommandDispatcher::new(pool, transport.clone(), 10);

    let (first, second) = tokio::join!(first.dispatch_once(now), second.dispatch_once(now));

    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.claimed + second.claimed, 1);
    assert_eq!(first.published + second.published, 1);
    assert_eq!(transport.requests.lock().await.len(), 1);
}

#[tokio::test]
async fn postgres_dispatcher_retries_an_offline_session_until_the_command_expires() {
    let Ok(database_url) = env::var("DATABASE_URL") else {
        return;
    };
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-retry-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
    sqlx::query("INSERT INTO devices (device_id) VALUES ($1)")
        .bind(&device_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, expires_at, next_attempt_at
         ) VALUES ($1, $2, 'sample_now', $3, $4, $5)",
    )
    .bind(command_id)
    .bind(&device_id)
    .bind(sqlx::types::Json(json!({})))
    .bind(now + Duration::seconds(2))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    let transport = RecordingTransport::unavailable_session();
    let dispatcher = CommandDispatcher::new(pool.clone(), transport.clone(), 10);

    let first = dispatcher.dispatch_once(now).await.unwrap();
    let queued = sqlx::query(
        "SELECT state, lease_until, next_attempt_at, published_at
         FROM command_outbox WHERE id = $1",
    )
    .bind(command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let after_expiry = dispatcher
        .dispatch_once(now + Duration::seconds(3))
        .await
        .unwrap();
    let state = sqlx::query_scalar::<_, String>("SELECT state FROM command_outbox WHERE id = $1")
        .bind(command_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(first.claimed, 1);
    assert_eq!(first.published, 0);
    assert_eq!(first.failed, 0);
    assert_eq!(transport.requests.lock().await.len(), 1);
    assert_eq!(queued.get::<String, _>("state"), "queued");
    assert!(
        queued
            .get::<Option<DateTime<Utc>>, _>("lease_until")
            .is_none()
    );
    assert!(
        queued
            .get::<Option<DateTime<Utc>>, _>("published_at")
            .is_none()
    );
    assert!(queued.get::<DateTime<Utc>, _>("next_attempt_at") > now);
    assert_eq!(after_expiry.expired, 1);
    assert_eq!(state, "expired");
}

#[tokio::test]
async fn postgres_dispatcher_expires_command_when_transport_returns_after_its_ttl() {
    let Ok(database_url) = env::var("DATABASE_URL") else {
        return;
    };
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let pool = PgPool::connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
    sqlx::query("INSERT INTO devices (device_id) VALUES ($1)")
        .bind(&device_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, expires_at, next_attempt_at
         ) VALUES ($1, $2, 'sample_now', $3, $4, $5)",
    )
    .bind(command_id)
    .bind(&device_id)
    .bind(sqlx::types::Json(json!({ "source": "dashboard" })))
    .bind(now + Duration::milliseconds(250))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    let transport = WaitUntilExpiredTransport::default();
    let dispatcher = CommandDispatcher::new(pool.clone(), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.expired, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 0);
    assert_eq!(transport.requests.lock().await.len(), 1);
    let row = sqlx::query(
        "SELECT state, lease_until, published_at
         FROM command_outbox WHERE id = $1",
    )
    .bind(command_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("state"), "expired");
    assert!(row.get::<Option<DateTime<Utc>>, _>("lease_until").is_none());
    assert!(
        row.get::<Option<DateTime<Utc>>, _>("published_at")
            .is_none()
    );
}
