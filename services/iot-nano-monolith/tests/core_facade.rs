use std::sync::Arc;

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
    device_id: &str,
    sequence: u64,
    event_at: chrono::DateTime<Utc>,
    temperature_c: f64,
    humidity_pct: f64,
) {
    store
        .write_telemetry(
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

#[tokio::test]
async fn sqlite_core_facade_replays_matching_commands_and_rejects_conflicts() {
    let (_directory, store) = sqlite_store().await;
    TopologyRepository::register_device(store.as_ref(), "facade-device")
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
        facade.get_command(command_id).await.unwrap().mode,
        RpcMode::TwoWay
    );
    assert!(matches!(
        facade.get_command(Uuid::now_v7()).await,
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
    TopologyRepository::register_device(store.as_ref(), "facade-device")
        .await
        .unwrap();
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
        device_id: "facade-device".to_owned(),
        response: json!({"ok": true, "value": 42}),
        responded_at: now + Duration::seconds(2),
    };
    facade
        .record_command_response(response.clone())
        .await
        .unwrap();
    facade.record_command_response(response).await.unwrap();
    assert_eq!(
        facade.get_command(command_id).await.unwrap().response,
        Some(json!({"ok": true, "value": 42}))
    );
    assert!(matches!(
        facade
            .record_command_response(CoreCommandResponseRequest {
                command_id,
                device_id: "facade-device".to_owned(),
                response: json!({"ok": false}),
                responded_at: now + Duration::seconds(3),
            })
            .await,
        Err(CoreFacadeError::Rejected(409))
    ));

    write_telemetry(store.as_ref(), "facade-device", 1, now, 20.0, 40.0).await;
    write_telemetry(
        store.as_ref(),
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
    TopologyRepository::register_device(store.as_ref(), "facade-device")
        .await
        .unwrap();
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
                device_id: "facade-device".to_owned(),
                response: json!({"ok": true}),
                responded_at: now,
            })
            .await,
        Err(CoreFacadeError::Rejected(409))
    ));
}
