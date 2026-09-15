use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ManagementChildStatus, ManagementDeviceError, ManagementDeviceRepository,
    ManagementDeviceTopology, ManagementGatewayStatus, PlatformStore, UpdateManagementDevice,
};
use serde_json::json;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-devices.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn seed_management_devices(store: &PlatformStore) -> (Uuid, Uuid) {
    let pool = store.sqlite_pool().unwrap();
    let asset_id = Uuid::now_v7();
    let device_profile_id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();
    let ten_minutes_ago = (Utc::now() - Duration::minutes(10)).to_rfc3339();

    sqlx::query("INSERT INTO assets (id, name) VALUES (?, 'management asset')")
        .bind(asset_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO device_profiles (id, name) VALUES (?, 'management profile')")
        .bind(device_profile_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO devices (
             device_id, display_name, metadata, last_seen_at, is_gateway
         ) VALUES
             ('management-gateway', 'Gateway', '{}', ?, 1),
             ('management-child', 'Child', '{}', ?, 0),
             ('management-direct', 'Direct', '{}', ?, 0),
             ('management-deleted', 'Deleted', '{}', ?, 0)",
    )
    .bind(&now)
    .bind(&now)
    .bind(&ten_minutes_ago)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices
         SET gateway_device_id = 'management-gateway',
             gateway_last_read_at = ?,
             gateway_read_quality = 'good'
         WHERE device_id = 'management-child'",
    )
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices
         SET deleted_at = ?
         WHERE device_id = 'management-deleted'",
    )
    .bind(&now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO device_tokens (id, device_id, token_prefix, token_hash)
         VALUES (?, 'management-direct', 'management-direct-token', 'unused'),
                (?, 'management-child', 'management-child-token', 'unused')",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(Uuid::now_v7().to_string())
    .execute(pool)
    .await
    .unwrap();

    (asset_id, device_profile_id)
}

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_store() -> (TimescaleTestLock, PlatformStore) {
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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:management-devices-test'))")
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

#[tokio::test]
async fn sqlite_management_device_repository_updates_lists_and_soft_deletes_devices() {
    let (_directory, store) = sqlite_store().await;
    let (asset_id, device_profile_id) = seed_management_devices(&store).await;

    let devices = ManagementDeviceRepository::list_management_devices(&store)
        .await
        .unwrap();
    assert_eq!(
        devices
            .iter()
            .map(|device| device.device_id.as_str())
            .collect::<Vec<_>>(),
        [
            "management-child",
            "management-direct",
            "management-gateway"
        ]
    );
    let gateway = devices
        .iter()
        .find(|device| device.device_id == "management-gateway")
        .unwrap();
    assert_eq!(
        gateway.health.gateway_status,
        Some(ManagementGatewayStatus::Online)
    );
    let child = devices
        .iter()
        .find(|device| device.device_id == "management-child")
        .unwrap();
    assert_eq!(
        child.health.child_status,
        Some(ManagementChildStatus::Fresh)
    );

    let updated = ManagementDeviceRepository::update_management_device(
        &store,
        "management-direct",
        UpdateManagementDevice {
            display_name: "Renamed direct".to_owned(),
            asset_id: Some(asset_id),
            device_profile_id: Some(device_profile_id),
            attributes: Some(json!({"location":"lab"})),
            topology: Some(ManagementDeviceTopology {
                is_gateway: false,
                gateway_device_id: Some("management-gateway".to_owned()),
            }),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.display_name.as_deref(), Some("Renamed direct"));
    assert_eq!(updated.asset_id, Some(asset_id));
    assert_eq!(updated.device_profile_id, Some(device_profile_id));
    assert_eq!(updated.attributes, json!({"location":"lab"}));
    assert_eq!(
        updated.topology,
        ManagementDeviceTopology {
            is_gateway: false,
            gateway_device_id: Some("management-gateway".to_owned()),
        }
    );
    assert_eq!(
        updated.health.child_status,
        Some(ManagementChildStatus::Unavailable)
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT revoked_at FROM device_tokens WHERE device_id = 'management-direct'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
        .is_some()
    );

    ManagementDeviceRepository::delete_management_device(&store, "management-child")
        .await
        .unwrap();
    assert!(
        ManagementDeviceRepository::list_management_devices(&store)
            .await
            .unwrap()
            .iter()
            .all(|device| device.device_id != "management-child")
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT revoked_at FROM device_tokens WHERE device_id = 'management-child'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn sqlite_management_device_repository_returns_typed_validation_errors() {
    let (_directory, store) = sqlite_store().await;
    seed_management_devices(&store).await;

    let scalar_attributes = ManagementDeviceRepository::update_management_device(
        &store,
        "management-direct",
        UpdateManagementDevice {
            display_name: "Direct".to_owned(),
            asset_id: None,
            device_profile_id: None,
            attributes: Some(json!("not-an-object")),
            topology: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        scalar_attributes,
        ManagementDeviceError::AttributesMustBeObject
    ));

    let missing_asset = ManagementDeviceRepository::update_management_device(
        &store,
        "management-direct",
        UpdateManagementDevice {
            display_name: "Direct".to_owned(),
            asset_id: Some(Uuid::now_v7()),
            device_profile_id: None,
            attributes: None,
            topology: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        missing_asset,
        ManagementDeviceError::AssetUnavailable(_)
    ));

    let demote_gateway = ManagementDeviceRepository::update_management_device(
        &store,
        "management-gateway",
        UpdateManagementDevice {
            display_name: "Gateway".to_owned(),
            asset_id: None,
            device_profile_id: None,
            attributes: None,
            topology: Some(ManagementDeviceTopology {
                is_gateway: false,
                gateway_device_id: None,
            }),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        demote_gateway,
        ManagementDeviceError::GatewayHasChildren
    ));

    let delete_gateway =
        ManagementDeviceRepository::delete_management_device(&store, "management-gateway")
            .await
            .unwrap_err();
    assert!(matches!(
        delete_gateway,
        ManagementDeviceError::GatewayHasChildren
    ));

    let non_gateway_parent = ManagementDeviceRepository::update_management_device(
        &store,
        "management-direct",
        UpdateManagementDevice {
            display_name: "Direct".to_owned(),
            asset_id: None,
            device_profile_id: None,
            attributes: None,
            topology: Some(ManagementDeviceTopology {
                is_gateway: false,
                gateway_device_id: Some("management-child".to_owned()),
            }),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        non_gateway_parent,
        ManagementDeviceError::GatewayIsNotGateway
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_device_repository_matches_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let asset_id = Uuid::now_v7();
    let device_profile_id = Uuid::now_v7();
    sqlx::query("INSERT INTO assets (id, name) VALUES ($1, 'timescale management asset')")
        .bind(asset_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO device_profiles (id, name)
         VALUES ($1, 'timescale management profile')",
    )
    .bind(device_profile_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, is_gateway) VALUES
             ('management-gateway', 'Gateway', TRUE),
             ('management-child', 'Child', FALSE),
             ('management-direct', 'Direct', FALSE),
             ('management-deleted', 'Deleted', FALSE)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices
         SET gateway_device_id = 'management-gateway'
         WHERE device_id = 'management-child'",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO device_runtime_state (device_id, last_seen_at, gateway_last_read_at)
         VALUES ('management-gateway', now(), NULL),
                ('management-child', now(), now()),
                ('management-direct', now() - interval '10 minutes', NULL)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET deleted_at = now() WHERE device_id = 'management-deleted'")
        .execute(pool)
        .await
        .unwrap();

    let listed = ManagementDeviceRepository::list_management_devices(&store)
        .await
        .unwrap();
    assert_eq!(listed.len(), 3);
    assert_eq!(
        listed
            .iter()
            .find(|device| device.device_id == "management-child")
            .unwrap()
            .health
            .child_status,
        Some(ManagementChildStatus::Fresh)
    );

    let updated = ManagementDeviceRepository::update_management_device(
        &store,
        "management-direct",
        UpdateManagementDevice {
            display_name: "Renamed direct".to_owned(),
            asset_id: Some(asset_id),
            device_profile_id: Some(device_profile_id),
            attributes: Some(json!({"location":"lab"})),
            topology: Some(ManagementDeviceTopology {
                is_gateway: false,
                gateway_device_id: Some("management-gateway".to_owned()),
            }),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.asset_id, Some(asset_id));
    assert_eq!(updated.device_profile_id, Some(device_profile_id));
    assert_eq!(updated.attributes, json!({"location":"lab"}));
    assert_eq!(
        updated.health.child_status,
        Some(ManagementChildStatus::Unavailable)
    );

    ManagementDeviceRepository::delete_management_device(&store, "management-child")
        .await
        .unwrap();
    assert!(
        ManagementDeviceRepository::list_management_devices(&store)
            .await
            .unwrap()
            .iter()
            .all(|device| device.device_id != "management-child")
    );
}
