use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration};
use iot_nano_core::{
    CommandTransport, CommandTransportError, PlatformCommandDispatcher, TransportRpcPublishRequest,
};
use iot_storage::{NewCommandOutboxEntry as PlatformCommand, PlatformStore};
use serde_json::json;
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

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

async fn register_platform_tenant(
    store: &PlatformStore,
    tenant_id: Uuid,
    slug: &str,
    device_id: &str,
) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, ?, 'active', '{}')",
    )
    .bind(tenant_id.to_string())
    .bind(slug)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    store.register_device(tenant_id, device_id).await.unwrap();
}

async fn register_timescale_platform_tenant(
    store: &PlatformStore,
    tenant_id: Uuid,
    slug: &str,
    device_id: &str,
) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES ($1, $2, 'active', '{}'::jsonb)",
    )
    .bind(tenant_id)
    .bind(slug)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    store.register_device(tenant_id, device_id).await.unwrap();
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
    register_platform_tenant(&store, TEST_TENANT_ID, "command-dispatcher", "device-a").await;
    (directory, store)
}

async fn command_state(store: &PlatformStore, tenant_id: Uuid, command_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT state
         FROM command_outbox
         WHERE tenant_id = ? AND id = ?",
    )
    .bind(tenant_id.to_string())
    .bind(command_id)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap()
}

async fn command_count_in_state(store: &PlatformStore, tenant_id: Uuid, state: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM command_outbox
         WHERE tenant_id = ? AND state = ?",
    )
    .bind(tenant_id.to_string())
    .bind(state)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap()
}

async fn timescale_command_state(
    store: &PlatformStore,
    tenant_id: Uuid,
    command_id: Uuid,
) -> String {
    sqlx::query_scalar(
        "SELECT state
         FROM command_outbox
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id)
    .bind(command_id)
    .fetch_one(store.timescale_pool().unwrap())
    .await
    .unwrap()
}

#[tokio::test]
async fn platform_dispatcher_claims_and_publishes_through_the_platform_store() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    let enqueued = store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            tenant_id: TEST_TENANT_ID,
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: json!({ "source": "dashboard" }).to_string(),
            mode: RpcMode::TwoWay,
            expires_at: now + Duration::seconds(30),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    let concrete = Arc::new(RecordingTransport::succeeds());
    let transport: Arc<dyn CommandTransport> = concrete.clone();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport, 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.expired, 0);
    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 1);
    assert_eq!(result.failed, 0);
    let requests = concrete.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tenant_id, TEST_TENANT_ID);
    assert_eq!(requests[0].device_id, "device-a");
    assert_eq!(requests[0].id, command_id);
    assert_eq!(requests[0].method, "sample_now");
    assert_eq!(requests[0].params, json!({ "source": "dashboard" }));
    assert_eq!(requests[0].mode, RpcMode::TwoWay);
    assert_eq!(requests[0].issued_at, enqueued.created_at);
    assert_eq!(requests[0].expires_at, now + Duration::seconds(30));
    drop(requests);

    let row = sqlx::query(
        "SELECT attempt_count, lease_until, published_at
         FROM command_outbox
         WHERE tenant_id = ? AND id = ?",
    )
    .bind(TEST_TENANT_ID.to_string())
    .bind(command_id.to_string())
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        command_state(&store, TEST_TENANT_ID, &command_id.to_string()).await,
        "published_to_broker"
    );
    assert_eq!(row.get::<i64, _>("attempt_count"), 1);
    assert!(row.get::<Option<String>, _>("lease_until").is_none());
    assert!(row.get::<Option<String>, _>("published_at").is_some());
}

#[tokio::test]
async fn platform_dispatcher_derives_and_forwards_each_command_tenant() {
    let (_directory, store) = platform_store().await;
    let tenant_b = Uuid::from_u128(2);
    register_platform_tenant(&store, tenant_b, "command-dispatcher-b", "device-b").await;
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
async fn platform_dispatcher_enforces_a_global_batch_bound_and_rotates_tenants() {
    let (_directory, store) = platform_store().await;
    let tenant_b = Uuid::from_u128(2);
    let tenant_c = Uuid::from_u128(3);
    register_platform_tenant(&store, tenant_b, "command-dispatcher-b", "device-b").await;
    register_platform_tenant(&store, tenant_c, "command-dispatcher-c", "device-c").await;
    let now = Utc::now();

    for (tenant_id, device_id) in [
        (TEST_TENANT_ID, "device-a"),
        (tenant_b, "device-b"),
        (tenant_c, "device-c"),
    ] {
        for _ in 0..2 {
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
    }

    let mut non_ready_commands = Vec::new();
    for offset in 0_u128..10 {
        let tenant_id = Uuid::from_u128(100 + offset);
        let device_id = format!("command-dispatcher-not-ready-{offset}");
        register_platform_tenant(
            &store,
            tenant_id,
            &format!("command-dispatcher-not-ready-{offset}"),
            &device_id,
        )
        .await;
        let command_id = Uuid::now_v7();
        let leased = offset % 2 == 1;
        store
            .enqueue_command(PlatformCommand {
                id: command_id.to_string(),
                tenant_id,
                device_id,
                method: "sample_now".to_owned(),
                params: "{}".to_owned(),
                mode: RpcMode::OneWay,
                expires_at: now + Duration::hours(2),
                next_attempt_at: if leased {
                    now
                } else {
                    now + Duration::hours(1)
                },
            })
            .await
            .unwrap();
        if leased {
            sqlx::query(
                "UPDATE command_outbox
                 SET state = 'leased', lease_until = ?
                 WHERE tenant_id = ? AND id = ?",
            )
            .bind((now + Duration::hours(1)).to_rfc3339())
            .bind(tenant_id.to_string())
            .bind(command_id.to_string())
            .execute(store.sqlite_pool().unwrap())
            .await
            .unwrap();
        }
        non_ready_commands.push((
            tenant_id,
            command_id,
            if leased { "leased" } else { "queued" },
        ));
    }

    let transport = RecordingTransport::succeeds();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 2);

    let first = dispatcher.dispatch_once(now).await.unwrap();
    let second = dispatcher
        .dispatch_once(now + Duration::seconds(1))
        .await
        .unwrap();

    assert_eq!(first.claimed, 2);
    assert_eq!(first.published, 2);
    assert_eq!(second.claimed, 2);
    assert_eq!(second.published, 2);
    assert!(first.claimed <= 2);
    assert!(second.claimed <= 2);
    let tenant_order = transport
        .requests
        .lock()
        .await
        .iter()
        .map(|request| request.tenant_id)
        .collect::<Vec<_>>();
    assert_eq!(
        tenant_order,
        vec![TEST_TENANT_ID, tenant_b, tenant_c, TEST_TENANT_ID]
    );
    for (tenant_id, command_id, expected_state) in non_ready_commands {
        assert_eq!(
            command_state(&store, tenant_id, &command_id.to_string()).await,
            expected_state
        );
    }
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_platform_dispatcher_enforces_a_global_batch_bound_rotates_ready_tenants_and_filters_unready_leases()
 {
    let (_lock, store) = timescale_platform_store().await;
    let tenant_a = TEST_TENANT_ID;
    let tenant_b = Uuid::from_u128(2);
    let tenant_c = Uuid::from_u128(3);
    for (tenant_id, slug, device_id) in [
        (tenant_a, "timescale-command-a", "timescale-device-a"),
        (tenant_b, "timescale-command-b", "timescale-device-b"),
        (tenant_c, "timescale-command-c", "timescale-device-c"),
    ] {
        register_timescale_platform_tenant(&store, tenant_id, slug, device_id).await;
    }
    let now = Utc::now();

    let reclaimable_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: reclaimable_id.to_string(),
            tenant_id: tenant_a,
            device_id: "timescale-device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::hours(2),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE command_outbox
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
    store
        .enqueue_command(PlatformCommand {
            id: a_queued_id.to_string(),
            tenant_id: tenant_a,
            device_id: "timescale-device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::hours(2),
            next_attempt_at: now + Duration::milliseconds(1),
        })
        .await
        .unwrap();
    for (tenant_id, device_id) in [
        (tenant_b, "timescale-device-b"),
        (tenant_c, "timescale-device-c"),
    ] {
        store
            .enqueue_command(PlatformCommand {
                id: Uuid::now_v7().to_string(),
                tenant_id,
                device_id: device_id.to_owned(),
                method: "sample_now".to_owned(),
                params: "{}".to_owned(),
                mode: RpcMode::OneWay,
                expires_at: now + Duration::hours(2),
                next_attempt_at: now,
            })
            .await
            .unwrap();
    }

    let future_tenant = Uuid::from_u128(100);
    let future_id = Uuid::now_v7();
    register_timescale_platform_tenant(
        &store,
        future_tenant,
        "timescale-command-future",
        "timescale-device-future",
    )
    .await;
    store
        .enqueue_command(PlatformCommand {
            id: future_id.to_string(),
            tenant_id: future_tenant,
            device_id: "timescale-device-future".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::hours(2),
            next_attempt_at: now + Duration::hours(1),
        })
        .await
        .unwrap();

    let active_lease_tenant = Uuid::from_u128(101);
    let active_lease_id = Uuid::now_v7();
    register_timescale_platform_tenant(
        &store,
        active_lease_tenant,
        "timescale-command-active-lease",
        "timescale-device-active-lease",
    )
    .await;
    store
        .enqueue_command(PlatformCommand {
            id: active_lease_id.to_string(),
            tenant_id: active_lease_tenant,
            device_id: "timescale-device-active-lease".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::hours(2),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE command_outbox
         SET state = 'leased', lease_until = $1
         WHERE tenant_id = $2 AND id = $3",
    )
    .bind(now + Duration::hours(1))
    .bind(active_lease_tenant)
    .bind(active_lease_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let transport = RecordingTransport::succeeds();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 2);

    let first = dispatcher.dispatch_once(now).await.unwrap();
    assert_eq!(first.claimed, 2);
    assert_eq!(first.published, 2);
    assert!(first.claimed <= 2);
    assert!(first.published <= 2);
    assert_eq!(
        transport
            .requests
            .lock()
            .await
            .iter()
            .map(|request| request.tenant_id)
            .collect::<Vec<_>>(),
        vec![tenant_a, tenant_b]
    );
    assert_eq!(
        timescale_command_state(&store, tenant_a, reclaimable_id).await,
        "published_to_broker"
    );
    assert_eq!(
        timescale_command_state(&store, tenant_a, a_queued_id).await,
        "queued"
    );

    let second = dispatcher
        .dispatch_once(now + Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(second.claimed, 2);
    assert_eq!(second.published, 2);
    assert!(second.claimed <= 2);
    assert!(second.published <= 2);
    assert_eq!(
        transport
            .requests
            .lock()
            .await
            .iter()
            .map(|request| request.tenant_id)
            .collect::<Vec<_>>(),
        vec![tenant_a, tenant_b, tenant_c, tenant_a]
    );
    assert_eq!(
        timescale_command_state(&store, tenant_a, a_queued_id).await,
        "published_to_broker"
    );
    assert_eq!(
        timescale_command_state(&store, future_tenant, future_id).await,
        "queued"
    );
    assert_eq!(
        timescale_command_state(&store, active_lease_tenant, active_lease_id).await,
        "leased"
    );
}

#[tokio::test]
async fn platform_dispatcher_releases_no_active_session_commands_for_retry() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            tenant_id: TEST_TENANT_ID,
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
        RecordingTransport::unavailable_session(),
        10,
    );

    let result = dispatcher.dispatch_once(now).await.unwrap();
    let denied = store
        .claim_commands(
            Uuid::from_u128(2),
            now + Duration::seconds(2),
            now + Duration::seconds(32),
            1,
        )
        .await
        .unwrap();
    let reclaimed = store
        .claim_commands(
            TEST_TENANT_ID,
            now + Duration::seconds(2),
            now + Duration::seconds(32),
            1,
        )
        .await
        .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 0);
    assert!(denied.is_empty());
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id, command_id.to_string());
}

#[tokio::test]
async fn platform_dispatcher_preserves_original_issue_time_after_retry() {
    let (_directory, store) = platform_store().await;
    let issued_at = Utc::now();
    let command_id = Uuid::now_v7();
    let enqueued = store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            tenant_id: TEST_TENANT_ID,
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
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store), transport.clone(), 10);

    let first = dispatcher.dispatch_once(issued_at).await.unwrap();
    let second = dispatcher
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
            tenant_id: TEST_TENANT_ID,
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
    let claimed_again = store
        .claim_commands(
            TEST_TENANT_ID,
            now + Duration::seconds(2),
            now + Duration::seconds(32),
            1,
        )
        .await
        .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 1);
    assert!(claimed_again.is_empty());
    assert_eq!(
        command_state(&store, TEST_TENANT_ID, &command_id.to_string()).await,
        "failed"
    );
}

#[tokio::test]
async fn platform_dispatcher_does_not_publish_after_transport_returned_past_expiry() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    let backlog_command_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            tenant_id: TEST_TENANT_ID,
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::milliseconds(250),
            next_attempt_at: now,
        })
        .await
        .unwrap();
    store
        .enqueue_command(PlatformCommand {
            id: backlog_command_id.to_string(),
            tenant_id: TEST_TENANT_ID,
            device_id: "device-a".to_owned(),
            method: "sample_now".to_owned(),
            params: "{}".to_owned(),
            mode: RpcMode::OneWay,
            expires_at: now + Duration::milliseconds(100),
            next_attempt_at: now + Duration::hours(1),
        })
        .await
        .unwrap();
    let transport = WaitUntilExpiredTransport::default();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();
    let published = store
        .mark_command_published(TEST_TENANT_ID, command_id, Utc::now())
        .await
        .unwrap();

    assert_eq!(result.claimed, 1);
    assert_eq!(result.published, 0);
    assert_eq!(result.failed, 0);
    assert_eq!(result.expired, 1);
    assert_eq!(transport.requests.lock().await.len(), 1);
    assert!(published.is_none());
    assert_eq!(
        command_state(&store, TEST_TENANT_ID, &command_id.to_string()).await,
        "expired"
    );
    assert_eq!(
        command_state(&store, TEST_TENANT_ID, &backlog_command_id.to_string()).await,
        "queued"
    );
}

#[tokio::test]
async fn platform_dispatcher_bounds_expiry_work_per_tick() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    for offset in 0..5 {
        store
            .enqueue_command(PlatformCommand {
                id: Uuid::now_v7().to_string(),
                tenant_id: TEST_TENANT_ID,
                device_id: "device-a".to_owned(),
                method: "sample_now".to_owned(),
                params: "{}".to_owned(),
                mode: RpcMode::OneWay,
                expires_at: now - Duration::seconds(5 - offset),
                next_attempt_at: now - Duration::seconds(10),
            })
            .await
            .unwrap();
    }
    let transport = RecordingTransport::succeeds();
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 2);

    let first = dispatcher.dispatch_once(now).await.unwrap();
    assert_eq!(first.expired, 2);
    assert_eq!(
        command_count_in_state(&store, TEST_TENANT_ID, "expired").await,
        2
    );
    assert_eq!(
        command_count_in_state(&store, TEST_TENANT_ID, "queued").await,
        3
    );

    let second = dispatcher
        .dispatch_once(now + Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(second.expired, 2);
    assert_eq!(
        command_count_in_state(&store, TEST_TENANT_ID, "expired").await,
        4
    );
    assert_eq!(
        command_count_in_state(&store, TEST_TENANT_ID, "queued").await,
        1
    );

    let third = dispatcher
        .dispatch_once(now + Duration::seconds(2))
        .await
        .unwrap();
    assert_eq!(third.expired, 1);
    assert_eq!(
        command_count_in_state(&store, TEST_TENANT_ID, "expired").await,
        5
    );
    assert_eq!(
        command_count_in_state(&store, TEST_TENANT_ID, "queued").await,
        0
    );
    assert!(transport.requests.lock().await.is_empty());
}

#[tokio::test]
async fn platform_dispatcher_expires_stale_commands_before_publish() {
    let (_directory, store) = platform_store().await;
    let now = Utc::now();
    let command_id = Uuid::now_v7();
    store
        .enqueue_command(PlatformCommand {
            id: command_id.to_string(),
            tenant_id: TEST_TENANT_ID,
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
    let dispatcher = PlatformCommandDispatcher::new(Arc::new(store.clone()), transport.clone(), 10);

    let result = dispatcher.dispatch_once(now).await.unwrap();

    assert_eq!(result.expired, 1);
    assert_eq!(result.claimed, 0);
    assert_eq!(transport.requests.lock().await.len(), 0);
    assert_eq!(
        command_state(&store, TEST_TENANT_ID, &command_id.to_string()).await,
        "expired"
    );
}
