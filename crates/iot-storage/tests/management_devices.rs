use chrono::{Duration, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AuditPrincipal, ManagementChildStatus, ManagementDeviceError, ManagementDeviceRepository,
    ManagementDeviceTopology, ManagementGatewayStatus, PlatformStore, UpdateManagementDevice,
};
use serde_json::json;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

mod common;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-devices.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_id = seed_tenant(store.sqlite_pool().unwrap(), "management-devices").await;
    (directory, store, tenant_id)
}

async fn seed_tenant(pool: &sqlx::SqlitePool, slug: &str) -> Uuid {
    let tenant_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, ?, 'active')")
        .bind(tenant_id.to_string())
        .bind(slug)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, password_hash, status, credential_version
         ) VALUES (?, ?, 'unused', 'active', 1)",
    )
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    tenant_id
}

fn tenant_actor(tenant_id: Uuid) -> AuditPrincipal {
    AuditPrincipal::TenantAccount(tenant_id)
}

async fn seed_management_devices(store: &PlatformStore, tenant_id: Uuid) -> (Uuid, Uuid) {
    let pool = store.sqlite_pool().unwrap();
    let asset_id = Uuid::now_v7();
    let device_profile_id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();
    let ten_minutes_ago = (Utc::now() - Duration::minutes(10)).to_rfc3339();

    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'management asset')")
        .bind(asset_id.to_string())
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO device_profiles (id, tenant_id, name) VALUES (?, ?, 'management profile')",
    )
    .bind(device_profile_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (
             device_id, tenant_id, display_name, metadata, last_seen_at, is_gateway
         ) VALUES
             ('management-gateway', ?, 'Gateway', '{}', ?, 1),
             ('management-child', ?, 'Child', '{}', ?, 0),
             ('management-direct', ?, 'Direct', '{}', ?, 0),
             ('management-deleted', ?, 'Deleted', '{}', NULL, 0)",
    )
    .bind(tenant_id.to_string())
    .bind(&now)
    .bind(tenant_id.to_string())
    .bind(&now)
    .bind(tenant_id.to_string())
    .bind(&ten_minutes_ago)
    .bind(tenant_id.to_string())
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

fn child_topology_update(gateway_device_id: Option<&str>) -> UpdateManagementDevice {
    UpdateManagementDevice {
        display_name: "Direct".to_owned(),
        asset_id: None,
        device_profile_id: None,
        attributes: None,
        topology: Some(ManagementDeviceTopology {
            is_gateway: false,
            gateway_device_id: gateway_device_id.map(str::to_owned),
        }),
    }
}

async fn sqlite_gateway_topology_version(store: &PlatformStore, device_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT gateway_topology_version
         FROM devices
         WHERE device_id = ?",
    )
    .bind(device_id)
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap()
}

async fn timescale_gateway_topology_version(pool: &sqlx::PgPool, tenant_id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT gateway_topology_version
         FROM devices
         WHERE device_id = 'timescale-version-child' AND tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn sqlite_management_devices_reject_cross_tenant_lookup_mutation_and_asset_references() {
    let (_directory, store, _default_tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_a = seed_tenant(pool, "device-tenant-a").await;
    let tenant_b = seed_tenant(pool, "device-tenant-b").await;
    let tenant_b_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, metadata) VALUES (?, ?, 'tenant-b asset', '{}')",
    )
    .bind(tenant_b_asset_id.to_string())
    .bind(tenant_b.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata, is_gateway)
         VALUES ('tenant-b-gateway', ?, 'Tenant B gateway', '{}', 1)",
    )
    .bind(tenant_b.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata)
         VALUES ('tenant-a-device', ?, 'Tenant A', '{}'),
                ('tenant-b-device', ?, 'Tenant B', '{}')",
    )
    .bind(tenant_a.to_string())
    .bind(tenant_b.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert_eq!(
        ManagementDeviceRepository::list_management_devices(&store, tenant_a)
            .await
            .unwrap()
            .iter()
            .map(|device| device.device_id.as_str())
            .collect::<Vec<_>>(),
        ["tenant-a-device"]
    );
    assert!(matches!(
        ManagementDeviceRepository::update_management_device(
            &store,
            tenant_a,
            tenant_actor(tenant_a),
            "tenant-b-device",
            UpdateManagementDevice {
                display_name: "Changed".to_owned(),
                asset_id: None,
                device_profile_id: None,
                attributes: None,
                topology: None,
            },
        )
        .await,
        Err(ManagementDeviceError::DeviceNotFound)
    ));
    assert!(matches!(
        ManagementDeviceRepository::delete_management_device(
            &store,
            tenant_a,
            tenant_actor(tenant_a),
            "tenant-b-device",
        )
        .await,
        Err(ManagementDeviceError::DeviceNotFound)
    ));
    assert!(matches!(
        ManagementDeviceRepository::update_management_device(
            &store,
            tenant_a,
            tenant_actor(tenant_a),
            "tenant-a-device",
            UpdateManagementDevice {
                display_name: "Tenant A".to_owned(),
                asset_id: Some(tenant_b_asset_id),
                device_profile_id: None,
                attributes: None,
                topology: None,
            },
        )
        .await,
        Err(ManagementDeviceError::AssetUnavailable(id)) if id == tenant_b_asset_id
    ));
    assert!(matches!(
        ManagementDeviceRepository::update_management_device(
            &store,
            tenant_a,
            tenant_actor(tenant_a),
            "tenant-a-device",
            UpdateManagementDevice {
                display_name: "Tenant A".to_owned(),
                asset_id: None,
                device_profile_id: None,
                attributes: None,
                topology: Some(ManagementDeviceTopology {
                    is_gateway: false,
                    gateway_device_id: Some("tenant-b-gateway".to_owned()),
                }),
            },
        )
        .await,
        Err(ManagementDeviceError::GatewayUnavailable)
    ));
    assert_eq!(
        sqlite_gateway_topology_version(&store, "tenant-a-device").await,
        0
    );
}

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_store() -> (TimescaleTestLock, PlatformStore, Uuid) {
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
    let tenant_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'management-devices', 'active')",
    )
    .bind(tenant_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_accounts (
            id, tenant_id, password_hash, status, credential_version
         ) VALUES ($1, $2, 'unused', 'active', 1)",
    )
    .bind(tenant_id)
    .bind(tenant_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
        tenant_id,
    )
}

#[tokio::test]
async fn sqlite_management_device_repository_updates_lists_and_soft_deletes_devices() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let (asset_id, device_profile_id) = seed_management_devices(&store, tenant_id).await;

    let devices = ManagementDeviceRepository::list_management_devices(&store, tenant_id)
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
        tenant_id,
        tenant_actor(tenant_id),
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

    ManagementDeviceRepository::delete_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-child",
    )
    .await
    .unwrap();
    assert!(
        ManagementDeviceRepository::list_management_devices(&store, tenant_id)
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
async fn sqlite_gateway_topology_version_increments_on_assignment_reassignment_and_detachment() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    seed_management_devices(&store, tenant_id).await;
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, metadata, is_gateway)
         VALUES ('management-gateway-two', ?, 'Gateway two', '{}', 1)",
    )
    .bind(tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    assert_eq!(
        sqlite_gateway_topology_version(&store, "management-direct").await,
        0
    );

    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-direct",
        child_topology_update(Some("management-gateway")),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlite_gateway_topology_version(&store, "management-direct").await,
        1
    );

    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-direct",
        child_topology_update(Some("management-gateway-two")),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlite_gateway_topology_version(&store, "management-direct").await,
        2
    );

    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-direct",
        child_topology_update(None),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlite_gateway_topology_version(&store, "management-direct").await,
        3
    );

    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-direct",
        UpdateManagementDevice {
            display_name: "Renamed direct".to_owned(),
            asset_id: None,
            device_profile_id: None,
            attributes: None,
            topology: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        sqlite_gateway_topology_version(&store, "management-direct").await,
        3
    );
}

#[tokio::test]
async fn sqlite_management_device_repository_returns_typed_validation_errors() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    seed_management_devices(&store, tenant_id).await;

    let scalar_attributes = ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
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
        tenant_id,
        tenant_actor(tenant_id),
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
        tenant_id,
        tenant_actor(tenant_id),
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

    let delete_gateway = ManagementDeviceRepository::delete_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-gateway",
    )
    .await
    .unwrap_err();
    assert!(matches!(
        delete_gateway,
        ManagementDeviceError::GatewayHasChildren
    ));

    let non_gateway_parent = ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
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
async fn sqlite_management_device_health_uses_the_five_minute_online_window() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let recently_seen = (Utc::now() - Duration::minutes(3)).to_rfc3339();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, last_seen_at)
         VALUES ('management-recent', ?, 'Recently seen', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(recently_seen)
    .execute(pool)
    .await
    .unwrap();

    let device = ManagementDeviceRepository::list_management_devices(&store, tenant_id)
        .await
        .unwrap()
        .into_iter()
        .find(|device| device.device_id == "management-recent")
        .unwrap();
    assert!(device.health.online);
}

#[test]
fn timescale_management_reference_validation_uses_boolean_exists_queries() {
    let source = include_str!("../src/management.rs");
    assert!(
        source.contains("SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)")
    );
    assert!(source.contains(
        "SELECT EXISTS(
                SELECT 1 FROM device_profiles WHERE id = $1 AND tenant_id = $2
             )"
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_device_repository_matches_sqlite_contract() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let asset_id = Uuid::now_v7();
    let device_profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name) VALUES ($1, $2, 'timescale management asset')",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO device_profiles (id, tenant_id, name)
         VALUES ($1, $2, 'timescale management profile')",
    )
    .bind(device_profile_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, is_gateway) VALUES
             ('management-gateway', $1, 'Gateway', TRUE),
             ('management-child', $1, 'Child', FALSE),
             ('management-direct', $1, 'Direct', FALSE),
             ('management-recent', $1, 'Recently seen', FALSE),
             ('management-deleted', $1, 'Deleted', FALSE)",
    )
    .bind(tenant_id)
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
                ('management-direct', now() - interval '10 minutes', NULL),
                ('management-recent', now() - interval '3 minutes', NULL)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET deleted_at = now() WHERE device_id = 'management-deleted'")
        .execute(pool)
        .await
        .unwrap();

    let listed = ManagementDeviceRepository::list_management_devices(&store, tenant_id)
        .await
        .unwrap();
    assert_eq!(listed.len(), 4);
    assert_eq!(
        listed
            .iter()
            .find(|device| device.device_id == "management-child")
            .unwrap()
            .health
            .child_status,
        Some(ManagementChildStatus::Fresh)
    );
    assert!(
        listed
            .iter()
            .find(|device| device.device_id == "management-recent")
            .unwrap()
            .health
            .online
    );

    let updated = ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
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

    ManagementDeviceRepository::delete_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "management-child",
    )
    .await
    .unwrap();
    assert!(
        ManagementDeviceRepository::list_management_devices(&store, tenant_id)
            .await
            .unwrap()
            .iter()
            .all(|device| device.device_id != "management-child")
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_gateway_topology_version_increments_on_assignment_reassignment_and_detachment() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, is_gateway) VALUES
             ('timescale-version-gateway-one', $1, 'Gateway one', TRUE),
             ('timescale-version-gateway-two', $1, 'Gateway two', TRUE),
             ('timescale-version-child', $1, 'Child', FALSE)",
    )
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();

    assert_eq!(timescale_gateway_topology_version(pool, tenant_id).await, 0);
    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "timescale-version-child",
        child_topology_update(Some("timescale-version-gateway-one")),
    )
    .await
    .unwrap();
    assert_eq!(timescale_gateway_topology_version(pool, tenant_id).await, 1);

    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "timescale-version-child",
        child_topology_update(Some("timescale-version-gateway-two")),
    )
    .await
    .unwrap();
    assert_eq!(timescale_gateway_topology_version(pool, tenant_id).await, 2);

    ManagementDeviceRepository::update_management_device(
        &store,
        tenant_id,
        tenant_actor(tenant_id),
        "timescale-version-child",
        child_topology_update(None),
    )
    .await
    .unwrap();
    assert_eq!(timescale_gateway_topology_version(pool, tenant_id).await, 3);
}
