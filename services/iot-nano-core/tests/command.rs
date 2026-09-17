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
use iot_nano_core::{
    CommandDispatcher, CommandTransport, CommandTransportError, PlatformCommandDispatcher,
    SqliteCommandDispatcher, TransportRpcPublishRequest, connect_core_database, migrate,
};
use iot_nano_core::{CoreSqliteStore, NewCommandOutboxEntry};
use iot_storage::{NewCommandOutboxEntry as PlatformCommand, PlatformStore};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

#[derive(Clone)]
struct RecordingTransport {
    requests: Arc<tokio::sync::Mutex<Vec<TransportRpcPublishRequest>>>,
    result: Result<(), CommandTransportError>,
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
            result: Err(CommandTransportError::Unavailable(message.to_owned())),
        }
    }

    fn unavailable_session() -> Self {
        Self {
            requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Err(CommandTransportError::NoActiveSession),
        }
    }

    fn configuration_error() -> Self {
        Self {
            requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Err(CommandTransportError::Configuration(
                "invalid route".to_owned(),
            )),
        }
    }
}

impl CommandTransport for RecordingTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(async move {
            self.requests.lock().await.push(request);
            self.result.clone()
        })
    }
}

#[derive(Clone)]
struct RetryThenSuccessTransport {
    requests: Arc<tokio::sync::Mutex<Vec<TransportRpcPublishRequest>>>,
    responses: Arc<tokio::sync::Mutex<Vec<Result<(), CommandTransportError>>>>,
}

impl RetryThenSuccessTransport {
    fn new() -> Self {
        Self {
            requests: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            responses: Arc::new(tokio::sync::Mutex::new(vec![
                Err(CommandTransportError::Unavailable(
                    "broker unavailable".to_owned(),
                )),
                Ok(()),
            ])),
        }
    }
}

impl CommandTransport for RetryThenSuccessTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(async move {
            self.requests.lock().await.push(request);
            self.responses.lock().await.remove(0)
        })
    }
}

#[derive(Clone, Default)]
struct WaitUntilExpiredTransport {
    requests: Arc<tokio::sync::Mutex<Vec<TransportRpcPublishRequest>>>,
}

impl CommandTransport for WaitUntilExpiredTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(async move {
            self.requests.lock().await.push(request.clone());
            while Utc::now() < request.expires_at {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Ok(())
        })
    }
}

async fn sqlite_store() -> (tempfile::TempDir, CoreSqliteStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn enqueue_sqlite_command(store: &CoreSqliteStore, command_id: Uuid, now: DateTime<Utc>) {
    enqueue_sqlite_command_with_expiry(store, command_id, now, now + Duration::seconds(30)).await;
}

async fn enqueue_sqlite_command_with_expiry(
    store: &CoreSqliteStore,
    command_id: Uuid,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) {
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
async fn sqlite_dispatcher_accepts_an_arc_trait_object_transport() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    enqueue_sqlite_command(&store, command_id, now).await;
    let concrete = Arc::new(RecordingTransport::succeeds());
    let transport: Arc<dyn CommandTransport> = concrete.clone();
    let dispatcher = SqliteCommandDispatcher::new(store, transport, 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.published, 1);
    assert_eq!(concrete.requests.lock().await.len(), 1);
}

#[tokio::test]
async fn sqlite_dispatcher_forwards_a_two_way_mode_to_the_transport() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
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
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, 'command-dispatcher', 'active', '{}')",
    )
    .bind(TEST_TENANT_ID.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    store
        .register_device(TEST_TENANT_ID, "device-a")
        .await
        .unwrap();
    (directory, store)
}

#[tokio::test]
async fn platform_dispatcher_claims_and_publishes_through_the_platform_store() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    let enqueued = store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: json!({ "source": "dashboard" }).to_string(),
            mode: RpcMode::TwoWay,
            expires_at: now + Duration::seconds(30),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let transport = RecordingTransport::succeeds();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

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
    assert_eq!(requests[0].mode, RpcMode::TwoWay);
    assert_eq!(requests[0].issued_at, enqueued.created_at);
    assert_eq!(requests[0].expires_at, now + Duration::seconds(30));
}

#[tokio::test]
async fn platform_dispatcher_derives_and_forwards_each_command_tenant() {
    let (_directory, store) = platform_store().await;
    let tenant_b = Uuid::from_u128(2);
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, 'command-dispatcher-b', 'active', '{}')",
    )
    .bind(tenant_b.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    store.register_device(tenant_b, "device-b").await.unwrap();
    let now = Utc::now();

    for (tenant_id, device_id) in [(TEST_TENANT_ID, "device-a"), (tenant_b, "device-b")] {
        store
            .enqueue_command(PlatformCommand {
                id: Uuid::now_v7().to_string(),
                tenant_id,
                device_id: device_id.to_owned(),
                method: "sample_now".to_owned(),
                params: "{}".to_owned(),
                mode: RpcMode::OneWay,
                expires_at: now + Duration::seconds(30),
                next_attempt_at: now,
            })
            .await
            .unwrap();
    }

    let transport = RecordingTransport::succeeds();
    let result = PlatformCommandDispatcher::new(Arc::new(store), transport.clone(), 10)
        .dispatch_once(now)
        .await
        .unwrap();

    assert_eq!(result.claimed, 2);
    assert_eq!(result.published, 2);
    let requests = transport.requests.lock().await;
    assert!(
        requests
            .iter()
            .any(|request| request.tenant_id == TEST_TENANT_ID && request.device_id == "device-a")
    );
    assert!(
        requests
            .iter()
            .any(|request| request.tenant_id == tenant_b && request.device_id == "device-b")
    );
}

#[tokio::test]
async fn platform_dispatcher_releases_unavailable_commands_for_retry() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::seconds(30),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let dispatcher = PlatformCommandDispatcher::new(
        Arc::new(store.clone()),
        RecordingTransport::fails("broker unavailable"),
        10,
    );

    let result = dispatcher.dispatch_once(now).await.unwrap();
    drop(dispatcher);
    let reclaimed = store
        .claim_commands(now + Duration::seconds(2), now + Duration::seconds(32), 1)
        .await
        .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 0);
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id, command_id.to_string());
}

#[tokio::test]
async fn platform_dispatcher_marks_legacy_invalid_ids_terminal_through_the_repository() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let invalid_id = "invalid-command-id";
    sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, mode, state, expires_at, next_attempt_at
         ) VALUES (?, 'device-a', 'sample_now', '{}', 'one_way', 'queued', ?, ?)",
    )
    .bind(invalid_id)
    .bind((now + Duration::seconds(30)).to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let transport = RecordingTransport::succeeds();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();
    let row = sqlx::query(
        "SELECT state, lease_until, last_error
         FROM command_outbox WHERE id = ?",
    )
    .bind(invalid_id)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.failed, 1);
    assert_eq!(transport.requests.lock().await.len(), 0);
    assert_eq!(row.get::<String, _>("state"), "failed");
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert_eq!(
        row.get::<Option<String>, _>("last_error").as_deref(),
        Some("command ID is not a UUID")
    );
}

#[tokio::test]
async fn platform_dispatcher_preserves_original_issue_time_after_retry() {
    let (_directory, store) = platform_store().await;
    let issued_at = Utc::now();
    let command_id = Uuid::now_v7();
    let enqueued = store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: issued_at + Duration::seconds(30),
            next_attempt_at: issued_at,
        })
        .await
        .unwrap();
    let transport = RetryThenSuccessTransport::new();
    let first_dispatcher =
        PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 10);

    let first = first_dispatcher.dispatch_once(issued_at).await.unwrap();
    drop(first_dispatcher);
    let second_dispatcher = PlatformCommandDispatcher::new(Arc::new(store), transport.clone(), 10);
    let second = second_dispatcher
        .dispatch_once(issued_at + Duration::seconds(2))
        .await
        .unwrap();

    assert_eq!(first.published, 0);
    assert_eq!(first.failed, 0);
    assert_eq!(second.published, 1);
    let requests = transport.requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].issued_at, enqueued.created_at);
    assert_eq!(requests[1].issued_at, enqueued.created_at);
}

#[tokio::test]
async fn platform_dispatcher_marks_configuration_errors_terminal() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::seconds(30),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let dispatcher = PlatformCommandDispatcher::new(
        Arc::new(store.clone()),
        RecordingTransport::configuration_error(),
        10,
    );

    let result = dispatcher.dispatch_once(now).await.unwrap();
    drop(dispatcher);
    let claimed_again = store
        .claim_commands(now + Duration::seconds(2), now + Duration::seconds(32), 1)
        .await
        .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 1);
    assert!(claimed_again.is_empty());
}

#[tokio::test]
async fn platform_dispatcher_does_not_publish_after_transport_returned_past_expiry() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::milliseconds(250),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let transport = WaitUntilExpiredTransport::default();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();
    let published = store
        .mark_command_published(command_id, Utc::now())
        .await
        .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 0);
    assert_eq!(result.expired, 1);
    assert_eq!(transport.requests.lock().await.len(), 1);
    assert!(published.is_none());
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
    let pool = connect_core_database(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
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
    let pool = connect_core_database(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-claim-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
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
    let pool = connect_core_database(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-retry-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
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
    let pool = connect_core_database(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let device_id = format!("command-test-{}", Uuid::now_v7());
    let command_id = Uuid::now_v7();
    let now = Utc::now();
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
