use chrono::{TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_storage::{
    GatewayIngestEventKind, GatewayIngestRepository, GatewayIngestRequest,
    GatewayIngestValidationError, PlatformStore, PlatformStoreError,
};
use sqlx::{Connection, PgConnection, Row};
use tokio::time::{Duration, sleep, timeout};

mod common;

const TIMESCALE_TEST_URL: &str = "postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform";

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_test_store() -> (TimescaleTestLock, PlatformStore) {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    assert_eq!(database_url, TIMESCALE_TEST_URL);
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
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
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
}

#[derive(Clone, Copy)]
enum Backend {
    Sqlite,
    Timescale,
}

fn test_tenant_id() -> uuid::Uuid {
    uuid::Uuid::from_u128(1)
}

async fn seed_tenant(store: &PlatformStore, backend: Backend) {
    match backend {
        Backend::Sqlite => {
            sqlx::query(
                "INSERT OR IGNORE INTO tenants (id, slug, status, metadata)
                 VALUES (?, 'gateway-ingest', 'active', '{}')",
            )
            .bind(test_tenant_id().to_string())
            .execute(store.sqlite_pool().unwrap())
            .await
            .unwrap();
        }
        Backend::Timescale => {
            sqlx::query(
                "INSERT INTO tenants (id, slug, status, metadata)
                 VALUES ($1, 'gateway-ingest', 'active', '{}'::jsonb)
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind(test_tenant_id())
            .execute(store.timescale_pool().unwrap())
            .await
            .unwrap();
        }
    }
}

async fn sqlite_test_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn seed_device(
    store: &PlatformStore,
    backend: Backend,
    device_id: &str,
    is_gateway: bool,
    gateway_device_id: Option<&str>,
    deleted: bool,
) {
    seed_tenant(store, backend).await;
    match backend {
        Backend::Sqlite => {
            let deleted_at = deleted.then(|| "2026-09-13T09:00:00+00:00");
            sqlx::query(
                "INSERT INTO devices (
                     device_id, tenant_id, is_gateway, gateway_device_id, deleted_at
                 ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(device_id)
            .bind(test_tenant_id().to_string())
            .bind(i64::from(is_gateway))
            .bind(gateway_device_id)
            .bind(deleted_at)
            .execute(store.sqlite_pool().unwrap())
            .await
            .unwrap();
        }
        Backend::Timescale => {
            let deleted_at = deleted.then(|| Utc.with_ymd_and_hms(2026, 9, 13, 9, 0, 0).unwrap());
            sqlx::query(
                "INSERT INTO devices (
                     device_id, tenant_id, is_gateway, gateway_device_id, deleted_at
                 ) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(device_id)
            .bind(test_tenant_id())
            .bind(is_gateway)
            .bind(gateway_device_id)
            .bind(deleted_at)
            .execute(store.timescale_pool().unwrap())
            .await
            .unwrap();
        }
    }
}

async fn seed_gateway(store: &PlatformStore, backend: Backend, gateway_device_id: &str) {
    seed_device(store, backend, gateway_device_id, true, None, false).await;
}

async fn seed_child(
    store: &PlatformStore,
    backend: Backend,
    child_device_id: &str,
    gateway_device_id: &str,
    deleted: bool,
) {
    seed_device(
        store,
        backend,
        child_device_id,
        false,
        Some(gateway_device_id),
        deleted,
    )
    .await;
}

fn gateway_request(
    gateway_device_id: &str,
    child_device_id: Option<&str>,
    event_kind: GatewayIngestEventKind,
    event_at: chrono::DateTime<Utc>,
    idempotency_key: &str,
    telemetry_event: Option<TelemetryEvent>,
) -> GatewayIngestRequest {
    GatewayIngestRequest {
        tenant_id: test_tenant_id(),
        gateway_device_id: gateway_device_id.to_owned(),
        child_device_id: child_device_id.map(str::to_owned),
        event_kind,
        event_at,
        idempotency_key: idempotency_key.to_owned(),
        telemetry_event,
        topic: format!("iot/v1/gateways/{gateway_device_id}/events"),
        received_at: event_at,
    }
}

async fn receipt_count(
    store: &PlatformStore,
    backend: Backend,
    gateway_device_id: &str,
    idempotency_key: &str,
) -> i64 {
    match backend {
        Backend::Sqlite => sqlx::query_scalar(
            "SELECT COUNT(*) FROM gateway_event_receipts
             WHERE gateway_device_id = ? AND idempotency_key = ?",
        )
        .bind(gateway_device_id)
        .bind(idempotency_key)
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        Backend::Timescale => sqlx::query_scalar(
            "SELECT COUNT(*) FROM gateway_event_receipts
             WHERE gateway_device_id = $1 AND idempotency_key = $2",
        )
        .bind(gateway_device_id)
        .bind(idempotency_key)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
    }
}

async fn telemetry_count(store: &PlatformStore, backend: Backend) -> i64 {
    match backend {
        Backend::Sqlite => sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        Backend::Timescale => sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap(),
    }
}

async fn gateway_last_seen_at(
    store: &PlatformStore,
    backend: Backend,
    gateway_device_id: &str,
) -> Option<chrono::DateTime<Utc>> {
    match backend {
        Backend::Sqlite => {
            let value: Option<String> =
                sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE device_id = ?")
                    .bind(gateway_device_id)
                    .fetch_one(store.sqlite_pool().unwrap())
                    .await
                    .unwrap();
            value.map(|value| value.parse().unwrap())
        }
        Backend::Timescale => {
            sqlx::query_scalar("SELECT last_seen_at FROM device_runtime_state WHERE device_id = $1")
                .bind(gateway_device_id)
                .fetch_optional(store.timescale_pool().unwrap())
                .await
                .unwrap()
                .flatten()
        }
    }
}

async fn runtime_state_count(store: &PlatformStore, backend: Backend, device_id: &str) -> i64 {
    match backend {
        Backend::Sqlite => sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        Backend::Timescale => {
            sqlx::query_scalar("SELECT COUNT(*) FROM device_runtime_state WHERE device_id = $1")
                .bind(device_id)
                .fetch_one(store.timescale_pool().unwrap())
                .await
                .unwrap()
        }
    }
}

async fn child_runtime_state(
    store: &PlatformStore,
    backend: Backend,
    child_device_id: &str,
) -> (Option<chrono::DateTime<Utc>>, Option<String>) {
    match backend {
        Backend::Sqlite => {
            let row = sqlx::query(
                "SELECT gateway_last_read_at, gateway_read_quality
                 FROM devices WHERE device_id = ?",
            )
            .bind(child_device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
            let gateway_last_read_at: Option<String> = row.try_get("gateway_last_read_at").unwrap();
            (
                gateway_last_read_at.map(|value| value.parse().unwrap()),
                row.try_get("gateway_read_quality").unwrap(),
            )
        }
        Backend::Timescale => {
            let row = sqlx::query(
                "SELECT gateway_last_read_at, gateway_read_quality
                 FROM device_runtime_state WHERE device_id = $1",
            )
            .bind(child_device_id)
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
            (
                row.try_get("gateway_last_read_at").unwrap(),
                row.try_get("gateway_read_quality").unwrap(),
            )
        }
    }
}

fn telemetry_event(
    device_id: &str,
    gateway_device_id: &str,
    event_at: chrono::DateTime<Utc>,
    sequence: u64,
) -> TelemetryEvent {
    TelemetryEvent {
        schema_version: 1,
        device_id: device_id.to_owned(),
        boot_id: uuid::Uuid::parse_str("a2d0e980-b0c8-4dcb-8f7f-2f70edcb7d5f").unwrap(),
        sequence,
        event_at,
        measurements: [
            ("temperature_c".to_owned(), serde_json::json!(23.5)),
            ("humidity_pct".to_owned(), serde_json::json!(47.0)),
        ]
        .into_iter()
        .collect(),
        gateway_device_id: Some(gateway_device_id.to_owned()),
    }
}

async fn telemetry_gateway_device_id(
    store: &PlatformStore,
    backend: Backend,
    device_id: &str,
) -> Option<String> {
    match backend {
        Backend::Sqlite => {
            sqlx::query_scalar("SELECT gateway_device_id FROM telemetry WHERE device_id = ?")
                .bind(device_id)
                .fetch_one(store.sqlite_pool().unwrap())
                .await
                .unwrap()
        }
        Backend::Timescale => {
            sqlx::query_scalar("SELECT gateway_device_id FROM telemetry WHERE device_id = $1")
                .bind(device_id)
                .fetch_one(store.timescale_pool().unwrap())
                .await
                .unwrap()
        }
    }
}

async fn sqlite_rollup_counts(store: &PlatformStore, device_id: &str) -> (i64, i64) {
    let pool = store.sqlite_pool().unwrap();
    let five_minutes: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_rollups_5m WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(pool)
            .await
            .unwrap();
    let one_hour: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_rollups_1h WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(pool)
            .await
            .unwrap();
    (five_minutes, one_hour)
}

async fn exercise_connect_contract(store: &PlatformStore, backend: Backend) {
    seed_gateway(store, backend, "gateway-1").await;
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 0, 0).unwrap();

    let _: &dyn GatewayIngestRepository = store;
    let result = store
        .ingest_gateway(gateway_request(
            "gateway-1",
            None,
            GatewayIngestEventKind::Connect,
            event_at,
            "gateway-1:connect:1",
            None,
        ))
        .await
        .unwrap();

    assert!(result.receipt_inserted);
    assert!(!result.telemetry_inserted);
    match backend {
        Backend::Sqlite => {
            let pool = store.sqlite_pool().unwrap();
            let receipt_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM gateway_event_receipts
                 WHERE gateway_device_id = ? AND idempotency_key = ?",
            )
            .bind("gateway-1")
            .bind("gateway-1:connect:1")
            .fetch_one(pool)
            .await
            .unwrap();
            let last_seen_at: Option<String> =
                sqlx::query_scalar("SELECT last_seen_at FROM devices WHERE device_id = ?")
                    .bind("gateway-1")
                    .fetch_one(pool)
                    .await
                    .unwrap();
            let telemetry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(receipt_count, 1);
            assert_eq!(
                last_seen_at.as_deref(),
                Some(event_at.to_rfc3339().as_str())
            );
            assert_eq!(telemetry_count, 0);
        }
        Backend::Timescale => {
            let pool = store.timescale_pool().unwrap();
            let receipt_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM gateway_event_receipts
                 WHERE gateway_device_id = $1 AND idempotency_key = $2",
            )
            .bind("gateway-1")
            .bind("gateway-1:connect:1")
            .fetch_one(pool)
            .await
            .unwrap();
            let last_seen_at: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
                "SELECT last_seen_at FROM device_runtime_state WHERE device_id = $1",
            )
            .bind("gateway-1")
            .fetch_one(pool)
            .await
            .unwrap();
            let telemetry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(receipt_count, 1);
            assert_eq!(last_seen_at, Some(event_at));
            assert_eq!(telemetry_count, 0);
        }
    }
}

async fn exercise_receipt_retry_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 1, 0).unwrap();
    seed_gateway(store, backend, "retry-gateway").await;
    let first = store
        .ingest_gateway(gateway_request(
            "retry-gateway",
            None,
            GatewayIngestEventKind::Heartbeat,
            event_at,
            "retry-gateway:heartbeat:1",
            None,
        ))
        .await
        .unwrap();
    let retry = store
        .ingest_gateway(gateway_request(
            "retry-gateway",
            None,
            GatewayIngestEventKind::Heartbeat,
            event_at + chrono::Duration::hours(1),
            "retry-gateway:heartbeat:1",
            None,
        ))
        .await
        .unwrap();

    assert!(first.receipt_inserted);
    assert!(!first.telemetry_inserted);
    assert!(!retry.receipt_inserted);
    assert!(!retry.telemetry_inserted);
    assert_eq!(
        receipt_count(store, backend, "retry-gateway", "retry-gateway:heartbeat:1").await,
        1
    );
    assert_eq!(
        gateway_last_seen_at(store, backend, "retry-gateway").await,
        Some(event_at)
    );
}

async fn exercise_unknown_gateway_rejection_contract(store: &PlatformStore, backend: Backend) {
    seed_tenant(store, backend).await;
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 2, 0).unwrap();
    let result = store
        .ingest_gateway(gateway_request(
            "unknown-gateway",
            None,
            GatewayIngestEventKind::Connect,
            event_at,
            "unknown-gateway:connect:1",
            None,
        ))
        .await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::UnknownDevice(device_id)) if device_id == "unknown-gateway"
    ));
    assert_eq!(
        receipt_count(
            store,
            backend,
            "unknown-gateway",
            "unknown-gateway:connect:1"
        )
        .await,
        0
    );
    assert_eq!(telemetry_count(store, backend).await, 0);
    assert_eq!(
        runtime_state_count(store, backend, "unknown-gateway").await,
        0
    );
}

async fn assert_topology_rejection(
    store: &PlatformStore,
    backend: Backend,
    request: GatewayIngestRequest,
    expected_device_id: &str,
) {
    let gateway_device_id = request.gateway_device_id.clone();
    let idempotency_key = request.idempotency_key.clone();
    let result = store.ingest_gateway(request).await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::UnknownDevice(device_id)) if device_id == expected_device_id
    ));
    assert_eq!(
        receipt_count(store, backend, &gateway_device_id, &idempotency_key).await,
        0
    );
    assert_eq!(
        gateway_last_seen_at(store, backend, &gateway_device_id).await,
        None
    );
    assert_eq!(telemetry_count(store, backend).await, 0);
}

async fn exercise_child_topology_rejection_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 3, 0).unwrap();

    seed_gateway(store, backend, "child-gateway").await;
    assert_topology_rejection(
        store,
        backend,
        gateway_request(
            "child-gateway",
            Some("unknown-child"),
            GatewayIngestEventKind::Disconnect,
            event_at,
            "child-gateway:unknown-child:1",
            None,
        ),
        "unknown-child",
    )
    .await;

    seed_device(store, backend, "direct-device", false, None, false).await;
    assert_topology_rejection(
        store,
        backend,
        gateway_request(
            "direct-device",
            None,
            GatewayIngestEventKind::Connect,
            event_at,
            "direct-device:connect:1",
            None,
        ),
        "direct-device",
    )
    .await;

    seed_device(store, backend, "deleted-gateway", true, None, true).await;
    assert_topology_rejection(
        store,
        backend,
        gateway_request(
            "deleted-gateway",
            None,
            GatewayIngestEventKind::Connect,
            event_at,
            "deleted-gateway:connect:1",
            None,
        ),
        "deleted-gateway",
    )
    .await;

    seed_gateway(store, backend, "other-gateway").await;
    seed_gateway(store, backend, "wrong-parent-gateway").await;
    seed_child(store, backend, "wrong-parent-child", "other-gateway", false).await;
    assert_topology_rejection(
        store,
        backend,
        gateway_request(
            "wrong-parent-gateway",
            Some("wrong-parent-child"),
            GatewayIngestEventKind::Disconnect,
            event_at,
            "wrong-parent-gateway:wrong-parent-child:1",
            None,
        ),
        "wrong-parent-child",
    )
    .await;

    seed_gateway(store, backend, "deleted-child-gateway").await;
    seed_child(
        store,
        backend,
        "deleted-child",
        "deleted-child-gateway",
        true,
    )
    .await;
    assert_topology_rejection(
        store,
        backend,
        gateway_request(
            "deleted-child-gateway",
            Some("deleted-child"),
            GatewayIngestEventKind::Disconnect,
            event_at,
            "deleted-child-gateway:deleted-child:1",
            None,
        ),
        "deleted-child",
    )
    .await;
}

async fn exercise_disconnect_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 4, 0).unwrap();
    seed_gateway(store, backend, "disconnect-gateway").await;
    seed_child(
        store,
        backend,
        "disconnect-child",
        "disconnect-gateway",
        false,
    )
    .await;

    let result = store
        .ingest_gateway(gateway_request(
            "disconnect-gateway",
            Some("disconnect-child"),
            GatewayIngestEventKind::Disconnect,
            event_at,
            "disconnect-gateway:disconnect-child:1",
            None,
        ))
        .await
        .unwrap();

    assert!(result.receipt_inserted);
    assert!(!result.telemetry_inserted);
    assert_eq!(
        receipt_count(
            store,
            backend,
            "disconnect-gateway",
            "disconnect-gateway:disconnect-child:1"
        )
        .await,
        1
    );
    assert_eq!(
        gateway_last_seen_at(store, backend, "disconnect-gateway").await,
        Some(event_at)
    );
    assert_eq!(
        child_runtime_state(store, backend, "disconnect-child").await,
        (None, Some("unavailable".to_owned()))
    );
    assert_eq!(telemetry_count(store, backend).await, 0);
}

async fn exercise_child_telemetry_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 5, 0).unwrap();
    seed_gateway(store, backend, "telemetry-gateway").await;
    seed_child(
        store,
        backend,
        "telemetry-child",
        "telemetry-gateway",
        false,
    )
    .await;
    let mut request = gateway_request(
        "telemetry-gateway",
        Some("telemetry-child"),
        GatewayIngestEventKind::ChildTelemetry,
        event_at,
        "telemetry-gateway:telemetry-child:1",
        Some(telemetry_event(
            "telemetry-child",
            "telemetry-gateway",
            event_at,
            1,
        )),
    );
    request.received_at = event_at + chrono::Duration::seconds(30);

    let result = store.ingest_gateway(request).await.unwrap();

    assert!(result.receipt_inserted);
    assert!(result.telemetry_inserted);
    assert_eq!(telemetry_count(store, backend).await, 1);
    assert_eq!(
        telemetry_gateway_device_id(store, backend, "telemetry-child").await,
        Some("telemetry-gateway".to_owned())
    );
    assert_eq!(
        gateway_last_seen_at(store, backend, "telemetry-gateway").await,
        Some(event_at)
    );
    assert_eq!(
        child_runtime_state(store, backend, "telemetry-child").await,
        (Some(event_at), Some("good".to_owned()))
    );
    if matches!(backend, Backend::Sqlite) {
        assert_eq!(sqlite_rollup_counts(store, "telemetry-child").await, (1, 1));
    }
}

async fn assert_invalid_ingest(
    store: &PlatformStore,
    backend: Backend,
    request: GatewayIngestRequest,
    expected_error: GatewayIngestValidationError,
) {
    let gateway_device_id = request.gateway_device_id.clone();
    let idempotency_key = request.idempotency_key.clone();
    let result = store.ingest_gateway(request).await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::InvalidGatewayIngest(error)) if error == expected_error
    ));
    assert_eq!(
        receipt_count(store, backend, &gateway_device_id, &idempotency_key).await,
        0
    );
    assert_eq!(telemetry_count(store, backend).await, 0);
    assert_eq!(
        gateway_last_seen_at(store, backend, &gateway_device_id).await,
        None
    );
}

async fn exercise_telemetry_identity_rejection_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 4, 30).unwrap();
    seed_gateway(store, backend, "identity-gateway").await;
    seed_child(store, backend, "identity-child", "identity-gateway", false).await;

    let mut wrong_child = telemetry_event("identity-child", "identity-gateway", event_at, 1);
    wrong_child.device_id = "different-child".to_owned();
    assert_invalid_ingest(
        store,
        backend,
        gateway_request(
            "identity-gateway",
            Some("identity-child"),
            GatewayIngestEventKind::ChildTelemetry,
            event_at,
            "identity-gateway:identity-child:wrong-child",
            Some(wrong_child),
        ),
        GatewayIngestValidationError::TelemetryChildMismatch,
    )
    .await;

    let mut wrong_gateway = telemetry_event("identity-child", "identity-gateway", event_at, 2);
    wrong_gateway.gateway_device_id = Some("other-gateway".to_owned());
    assert_invalid_ingest(
        store,
        backend,
        gateway_request(
            "identity-gateway",
            Some("identity-child"),
            GatewayIngestEventKind::ChildTelemetry,
            event_at,
            "identity-gateway:identity-child:wrong-gateway",
            Some(wrong_gateway),
        ),
        GatewayIngestValidationError::TelemetryGatewayMismatch,
    )
    .await;
}

async fn exercise_raw_duplicate_new_receipt_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 6, 0).unwrap();
    seed_gateway(store, backend, "raw-duplicate-gateway").await;
    seed_child(
        store,
        backend,
        "raw-duplicate-child",
        "raw-duplicate-gateway",
        false,
    )
    .await;

    let event = telemetry_event("raw-duplicate-child", "raw-duplicate-gateway", event_at, 7);
    let telemetry_before: i64 = telemetry_count(store, backend).await;
    let first = store
        .ingest_gateway(gateway_request(
            "raw-duplicate-gateway",
            Some("raw-duplicate-child"),
            GatewayIngestEventKind::ChildTelemetry,
            event_at,
            "raw-duplicate:first",
            Some(event.clone()),
        ))
        .await
        .unwrap();
    let rollups_after_first = matches!(backend, Backend::Sqlite)
        .then(|| sqlite_rollup_counts(store, "raw-duplicate-child"));

    let duplicate = store
        .ingest_gateway(gateway_request(
            "raw-duplicate-gateway",
            Some("raw-duplicate-child"),
            GatewayIngestEventKind::ChildTelemetry,
            event_at + chrono::Duration::seconds(1),
            "raw-duplicate:second",
            Some(event),
        ))
        .await
        .unwrap();

    assert_eq!(
        first,
        iot_storage::GatewayIngestResult {
            receipt_inserted: true,
            telemetry_inserted: true
        }
    );
    assert_eq!(
        duplicate,
        iot_storage::GatewayIngestResult {
            receipt_inserted: true,
            telemetry_inserted: false
        }
    );
    assert_eq!(telemetry_count(store, backend).await, telemetry_before + 1);
    if matches!(backend, Backend::Sqlite) {
        assert_eq!(
            sqlite_rollup_counts(store, "raw-duplicate-child").await,
            rollups_after_first.unwrap().await
        );
    }
}

async fn exercise_out_of_order_timestamps_contract(store: &PlatformStore, backend: Backend) {
    let newer = Utc.with_ymd_and_hms(2026, 9, 13, 10, 8, 0).unwrap();
    let older = newer - chrono::Duration::minutes(5);
    seed_gateway(store, backend, "ordering-gateway").await;
    seed_child(store, backend, "ordering-child", "ordering-gateway", false).await;

    store
        .ingest_gateway(gateway_request(
            "ordering-gateway",
            Some("ordering-child"),
            GatewayIngestEventKind::ChildTelemetry,
            newer,
            "ordering:newer",
            Some(telemetry_event(
                "ordering-child",
                "ordering-gateway",
                newer,
                1,
            )),
        ))
        .await
        .unwrap();
    store
        .ingest_gateway(gateway_request(
            "ordering-gateway",
            Some("ordering-child"),
            GatewayIngestEventKind::ChildTelemetry,
            older,
            "ordering:older",
            Some(telemetry_event(
                "ordering-child",
                "ordering-gateway",
                older,
                2,
            )),
        ))
        .await
        .unwrap();

    assert_eq!(
        gateway_last_seen_at(store, backend, "ordering-gateway").await,
        Some(newer)
    );
    assert_eq!(
        child_runtime_state(store, backend, "ordering-child")
            .await
            .0,
        Some(newer)
    );
}

async fn exercise_sequence_overflow_retry_contract(store: &PlatformStore, backend: Backend) {
    let event_at = Utc.with_ymd_and_hms(2026, 9, 13, 10, 9, 0).unwrap();
    seed_gateway(store, backend, "overflow-gateway").await;
    seed_child(store, backend, "overflow-child", "overflow-gateway", false).await;
    let mut request = gateway_request(
        "overflow-gateway",
        Some("overflow-child"),
        GatewayIngestEventKind::ChildTelemetry,
        event_at,
        "overflow:retry",
        Some(telemetry_event(
            "overflow-child",
            "overflow-gateway",
            event_at,
            i64::MAX as u64 + 1,
        )),
    );

    assert!(matches!(
        store.ingest_gateway(request.clone()).await,
        Err(PlatformStoreError::TelemetrySequenceOverflow)
    ));
    let telemetry_before = telemetry_count(store, backend).await;
    assert_eq!(
        receipt_count(store, backend, "overflow-gateway", "overflow:retry").await,
        0
    );

    request.telemetry_event.as_mut().unwrap().sequence = 1;
    let retry = store.ingest_gateway(request).await.unwrap();
    assert!(retry.receipt_inserted);
    assert!(retry.telemetry_inserted);
    assert_eq!(
        receipt_count(store, backend, "overflow-gateway", "overflow:retry").await,
        1
    );
    assert_eq!(telemetry_count(store, backend).await, telemetry_before + 1);
}

#[tokio::test]
#[ignore = "requires the declared disposable Timescale test URL"]
async fn timescale_gateway_ingest_soft_delete_waits_for_inflight_validation() {
    let (_lock, store) = timescale_test_store().await;
    seed_gateway(&store, Backend::Timescale, "delete-race-gateway").await;
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
    let mut pause = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut pause)
        .await
        .unwrap();
    sqlx::query(
        "CREATE FUNCTION pause_gateway_ingest_runtime_state() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock(4242, 4242);
             RETURN NEW;
         END;
         $$",
    )
    .execute(&mut pause)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER pause_gateway_ingest_runtime_state_trigger
         BEFORE INSERT OR UPDATE ON device_runtime_state
         FOR EACH ROW EXECUTE FUNCTION pause_gateway_ingest_runtime_state()",
    )
    .execute(&mut pause)
    .await
    .unwrap();
    sqlx::query("SELECT pg_advisory_lock(4242, 4242)")
        .execute(&mut pause)
        .await
        .unwrap();

    let request = gateway_request(
        "delete-race-gateway",
        None,
        GatewayIngestEventKind::Connect,
        Utc.with_ymd_and_hms(2026, 9, 13, 10, 10, 0).unwrap(),
        "delete-race:connect",
        None,
    );
    let ingest_store = store.clone();
    let ingest = tokio::spawn(async move { ingest_store.ingest_gateway(request).await });

    let mut observer = PgConnection::connect(&database_url).await.unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND classid = 4242
                      AND objid = 4242
                      AND granted = FALSE
                 )",
            )
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if waiting {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("ingest must reach the post-validation pause");

    let mut deleter = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut deleter)
        .await
        .unwrap();
    let mut soft_delete = tokio::spawn(async move {
        sqlx::query("UPDATE devices SET deleted_at = now() WHERE device_id = $1")
            .bind("delete-race-gateway")
            .execute(&mut deleter)
            .await
    });
    let blocked = timeout(Duration::from_millis(100), &mut soft_delete).await;
    assert!(
        blocked.is_err(),
        "soft deletion must wait for the validated in-flight ingest"
    );

    sqlx::query("SELECT pg_advisory_unlock(4242, 4242)")
        .execute(&mut pause)
        .await
        .unwrap();
    let ingest = timeout(Duration::from_secs(2), ingest)
        .await
        .expect("ingest must finish after pause release")
        .unwrap()
        .unwrap();
    assert!(ingest.receipt_inserted);
    assert!(!ingest.telemetry_inserted);
    let deleted = timeout(Duration::from_secs(2), &mut soft_delete)
        .await
        .expect("soft deletion must finish after ingest commit")
        .unwrap()
        .unwrap();
    assert_eq!(deleted.rows_affected(), 1);
    assert_eq!(
        receipt_count(
            &store,
            Backend::Timescale,
            "delete-race-gateway",
            "delete-race:connect",
        )
        .await,
        1
    );
}

#[tokio::test]
async fn sqlite_gateway_ingest_contract() {
    let (_directory, store) = sqlite_test_store().await;
    exercise_connect_contract(&store, Backend::Sqlite).await;
    exercise_receipt_retry_contract(&store, Backend::Sqlite).await;
    exercise_unknown_gateway_rejection_contract(&store, Backend::Sqlite).await;
    exercise_child_topology_rejection_contract(&store, Backend::Sqlite).await;
    exercise_disconnect_contract(&store, Backend::Sqlite).await;
    exercise_telemetry_identity_rejection_contract(&store, Backend::Sqlite).await;
    exercise_child_telemetry_contract(&store, Backend::Sqlite).await;
    exercise_raw_duplicate_new_receipt_contract(&store, Backend::Sqlite).await;
    exercise_out_of_order_timestamps_contract(&store, Backend::Sqlite).await;
    exercise_sequence_overflow_retry_contract(&store, Backend::Sqlite).await;
}

#[tokio::test]
#[ignore = "requires the declared disposable Timescale test URL"]
async fn timescale_gateway_ingest_contract() {
    let (_lock, store) = timescale_test_store().await;
    exercise_connect_contract(&store, Backend::Timescale).await;
    exercise_receipt_retry_contract(&store, Backend::Timescale).await;
    exercise_unknown_gateway_rejection_contract(&store, Backend::Timescale).await;
    exercise_child_topology_rejection_contract(&store, Backend::Timescale).await;
    exercise_disconnect_contract(&store, Backend::Timescale).await;
    exercise_telemetry_identity_rejection_contract(&store, Backend::Timescale).await;
    exercise_child_telemetry_contract(&store, Backend::Timescale).await;
    exercise_raw_duplicate_new_receipt_contract(&store, Backend::Timescale).await;
    exercise_out_of_order_timestamps_contract(&store, Backend::Timescale).await;
    exercise_sequence_overflow_retry_contract(&store, Backend::Timescale).await;
}
