use std::{env, sync::Arc};

use chrono::{Duration, TimeZone, Utc};
use iot_api::{
    CoreCommandCreateRequest, CoreCommandResponseRequest, CoreFacade, CoreFacadeError,
    CoreTelemetryBucket, CoreTelemetryQuery,
};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use iot_nano_monolith::PlatformCoreFacade;
use iot_storage::{PlatformStore, TopologyRepository};
use serde_json::json;
use uuid::Uuid;

const TEST_TENANT_ID: Uuid = Uuid::from_u128(1);

async fn sqlite_store() -> (tempfile::TempDir, Arc<PlatformStore>) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("platform.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES (?, 'core-facade', 'active', '{}')",
    )
    .bind(TEST_TENANT_ID.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store)
}

fn command_request(
    id: Uuid,
    device_id: &str,
    params: serde_json::Value,
    now: chrono::DateTime<Utc>,
) -> CoreCommandCreateRequest {
    CoreCommandCreateRequest {
        id,
        tenant_id: TEST_TENANT_ID,
        device_id: device_id.to_owned(),
        method: "device_read".to_owned(),
        params,
        mode: RpcMode::TwoWay,
        issued_at: now,
        expires_at: now + Duration::minutes(5),
    }
}

async fn write_telemetry(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    sequence: u64,
    event_at: chrono::DateTime<Utc>,
    temperature_c: f64,
    humidity_pct: f64,
) {
    store
        .write_telemetry(
            tenant_id,
            &TelemetryEvent {
                schema_version: 1,
                device_id: device_id.to_owned(),
                boot_id: Uuid::now_v7(),
                sequence,
                event_at,
                measurements: [
                    ("temperature_c".to_owned(), json!(temperature_c)),
                    ("humidity_pct".to_owned(), json!(humidity_pct)),
                ]
                .into_iter()
                .collect(),
                gateway_device_id: None,
            },
            event_at,
            "v1/devices/me/telemetry",
        )
        .await
        .unwrap();
}

async fn insert_active_sqlite_token(store: &PlatformStore, device_id: &str) -> Uuid {
    let token_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, ?, ?, 'unused')",
    )
    .bind(token_id.to_string())
    .bind(device_id)
    .bind(format!("facade-token-{token_id}"))
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    token_id
}

#[tokio::test]
async fn sqlite_core_facade_replays_matching_commands_and_rejects_conflicts() {
    let (_directory, store) = sqlite_store().await;
    TopologyRepository::register_device(store.as_ref(), TEST_TENANT_ID, "facade-device")
        .await
        .unwrap();
    let facade = PlatformCoreFacade::new(store.clone());
    let now = Utc.with_ymd_and_hms(2026, 9, 14, 10, 0, 0).unwrap();
    let command_id = Uuid::now_v7();
    let request = command_request(
        command_id,
        "facade-device",
        json!({"channel": "temperature"}),
        now,
    );

    let created = facade.create_command(request.clone()).await.unwrap();
    let replay = facade.create_command(request).await.unwrap();

    assert_eq!(created.id, command_id);
    assert_eq!(created.state, "queued");
    assert_eq!(replay.id, command_id);
    assert_eq!(replay.device_id, "facade-device");
    assert_eq!(
        facade
            .get_command(TEST_TENANT_ID, command_id)
            .await
            .unwrap()
            .mode,
        RpcMode::TwoWay
    );
    assert!(matches!(
        facade.get_command(TEST_TENANT_ID, Uuid::now_v7()).await,
        Err(CoreFacadeError::NotFound)
    ));

    let conflict = facade
        .create_command(command_request(
            command_id,
            "facade-device",
            json!({"channel": "humidity"}),
            now,
        ))
        .await;
    assert!(matches!(conflict, Err(CoreFacadeError::Rejected(409))));
}

#[tokio::test]
async fn sqlite_core_facade_records_idempotent_responses_and_reads_telemetry_buckets() {
    let (_directory, store) = sqlite_store().await;
    TopologyRepository::register_device(store.as_ref(), TEST_TENANT_ID, "facade-device")
        .await
        .unwrap();
    let token_id = insert_active_sqlite_token(store.as_ref(), "facade-device").await;
    let facade = PlatformCoreFacade::new(store.clone());
    let now = Utc.with_ymd_and_hms(2026, 9, 14, 10, 1, 0).unwrap();
    let command_id = Uuid::now_v7();

    facade
        .create_command(command_request(
            command_id,
            "facade-device",
            json!({"channel": "temperature"}),
            now,
        ))
        .await
        .unwrap();
    store
        .claim_commands(now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    store
        .mark_command_published(command_id, now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();

    let response = CoreCommandResponseRequest {
        command_id,
        tenant_id: TEST_TENANT_ID,
        device_id: "facade-device".to_owned(),
        token_id,
        response: json!({"ok": true, "value": 42}),
        responded_at: now + Duration::seconds(2),
    };
    facade
        .record_command_response(response.clone())
        .await
        .unwrap();
    facade.record_command_response(response).await.unwrap();
    assert_eq!(
        facade
            .get_command(TEST_TENANT_ID, command_id)
            .await
            .unwrap()
            .response,
        Some(json!({"ok": true, "value": 42}))
    );
    assert!(matches!(
        facade
            .record_command_response(CoreCommandResponseRequest {
                command_id,
                tenant_id: TEST_TENANT_ID,
                device_id: "facade-device".to_owned(),
                token_id,
                response: json!({"ok": false}),
                responded_at: now + Duration::seconds(3),
            })
            .await,
        Err(CoreFacadeError::Rejected(409))
    ));

    write_telemetry(
        store.as_ref(),
        TEST_TENANT_ID,
        "facade-device",
        1,
        now,
        20.0,
        40.0,
    )
    .await;
    write_telemetry(
        store.as_ref(),
        TEST_TENANT_ID,
        "facade-device",
        2,
        now + Duration::minutes(2),
        24.0,
        48.0,
    )
    .await;
    let from = now - Duration::minutes(1);
    let to = now + Duration::hours(1);

    let raw = facade
        .telemetry(CoreTelemetryQuery {
            device_id: "facade-device".to_owned(),
            from,
            to,
            bucket: CoreTelemetryBucket::Raw,
        })
        .await
        .unwrap();
    assert_eq!(raw.len(), 2);
    assert_eq!(raw[0].temperature_c, Some(20.0));
    assert_eq!(raw[1].humidity_pct, Some(48.0));
    assert_eq!(raw[0].event_count, 1);

    let five_minutes = facade
        .telemetry(CoreTelemetryQuery {
            device_id: "facade-device".to_owned(),
            from,
            to,
            bucket: CoreTelemetryBucket::FiveMinutes,
        })
        .await
        .unwrap();
    assert_eq!(five_minutes.len(), 1);
    assert_eq!(five_minutes[0].temperature_c, Some(22.0));
    assert_eq!(five_minutes[0].humidity_pct, Some(44.0));
    assert_eq!(five_minutes[0].event_count, 2);

    let one_hour = facade
        .telemetry(CoreTelemetryQuery {
            device_id: "facade-device".to_owned(),
            from,
            to,
            bucket: CoreTelemetryBucket::OneHour,
        })
        .await
        .unwrap();
    assert_eq!(one_hour.len(), 1);
    assert_eq!(one_hour[0].temperature_c, Some(22.0));
    assert_eq!(one_hour[0].humidity_pct, Some(44.0));
    assert_eq!(one_hour[0].event_count, 2);
}

#[tokio::test]
async fn sqlite_core_facade_rejects_invalid_queries_and_unpublished_responses() {
    let (_directory, store) = sqlite_store().await;
    TopologyRepository::register_device(store.as_ref(), TEST_TENANT_ID, "facade-device")
        .await
        .unwrap();
    let token_id = insert_active_sqlite_token(store.as_ref(), "facade-device").await;
    let facade = PlatformCoreFacade::new(store);
    let now = Utc.with_ymd_and_hms(2026, 9, 14, 10, 0, 0).unwrap();

    assert!(matches!(
        facade
            .telemetry(CoreTelemetryQuery {
                device_id: "facade-device".to_owned(),
                from: now,
                to: now,
                bucket: CoreTelemetryBucket::Raw,
            })
            .await,
        Err(CoreFacadeError::Rejected(400))
    ));
    assert!(matches!(
        facade
            .record_command_response(CoreCommandResponseRequest {
                command_id: Uuid::now_v7(),
                tenant_id: TEST_TENANT_ID,
                device_id: "facade-device".to_owned(),
                token_id,
                response: json!({"ok": true}),
                responded_at: now,
            })
            .await,
        Err(CoreFacadeError::Rejected(409))
    ));
}

#[tokio::test]
async fn sqlite_core_facade_rejects_response_for_a_revoked_token() {
    let (_directory, store) = sqlite_store().await;
    TopologyRepository::register_device(store.as_ref(), TEST_TENANT_ID, "facade-device")
        .await
        .unwrap();
    let revoked_token_id = insert_active_sqlite_token(store.as_ref(), "facade-device").await;
    let facade = PlatformCoreFacade::new(store.clone());
    let now = Utc.with_ymd_and_hms(2026, 9, 14, 10, 0, 0).unwrap();
    let command_id = Uuid::now_v7();

    facade
        .create_command(command_request(
            command_id,
            "facade-device",
            json!({"channel": "temperature"}),
            now,
        ))
        .await
        .unwrap();
    store
        .claim_commands(now, now + Duration::seconds(30), 1)
        .await
        .unwrap();
    store
        .mark_command_published(command_id, now + Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();

    sqlx::query("UPDATE device_tokens SET revoked_at = ? WHERE id = ?")
        .bind((now + Duration::seconds(2)).to_rfc3339())
        .bind(revoked_token_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    insert_active_sqlite_token(store.as_ref(), "facade-device").await;

    assert!(matches!(
        facade
            .record_command_response(CoreCommandResponseRequest {
                command_id,
                tenant_id: TEST_TENANT_ID,
                device_id: "facade-device".to_owned(),
                token_id: revoked_token_id,
                response: json!({"ok": true}),
                responded_at: now + Duration::seconds(3),
            })
            .await,
        Err(CoreFacadeError::Rejected(409))
    ));
    let command = facade
        .get_command(TEST_TENANT_ID, command_id)
        .await
        .unwrap();
    assert_eq!(command.state, "published_to_broker");
    assert_eq!(command.response, None);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_core_facade_normalizes_malformed_and_out_of_range_raw_metrics() {
    let database_url = env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Timescale,
            database_url: Some(database_url),
            sqlite_path: None,
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    let device_id = format!("facade-metrics-{}", Uuid::now_v7());
    let tenant_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata)
         VALUES ($1, $2, 'active', '{}'::jsonb)",
    )
    .bind(tenant_id)
    .bind(format!("facade-metrics-{tenant_id}"))
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    store.register_device(tenant_id, &device_id).await.unwrap();
    let first_at = Utc::now();
    let second_at = first_at + Duration::milliseconds(1);
    let pool = store.timescale_pool().unwrap();

    for (sequence, event_at, measurements) in [
        (
            1_i64,
            first_at,
            r#"{"temperature_c":1e400,"humidity_pct":51.0}"#,
        ),
        (
            2_i64,
            second_at,
            r#"{"temperature_c":"not-a-number","humidity_pct":52.0}"#,
        ),
    ] {
        sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
             ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'iot/v1/devices/telemetry')",
        )
        .bind(event_at)
        .bind(event_at)
        .bind(tenant_id)
        .bind(&device_id)
        .bind(Uuid::now_v7())
        .bind(sequence)
        .bind(measurements)
        .execute(pool)
        .await
        .unwrap();
    }

    let points = PlatformCoreFacade::new(store)
        .telemetry(CoreTelemetryQuery {
            device_id,
            from: first_at - Duration::seconds(1),
            to: second_at + Duration::seconds(1),
            bucket: CoreTelemetryBucket::Raw,
        })
        .await
        .unwrap();

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].temperature_c, None);
    assert_eq!(points[0].humidity_pct, Some(51.0));
    assert_eq!(points[1].temperature_c, None);
    assert_eq!(points[1].humidity_pct, Some(52.0));
}
