use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    CreateManagementAssetProfile, CreateManagementDeviceProfile, ManagementAssetError,
    ManagementAssetProfileError, ManagementAssetProfileRepository, ManagementAssetRepository,
    ManagementDeviceError, ManagementDeviceProfileError, ManagementDeviceProfileRepository,
    ManagementDeviceRepository, PlatformStore, UpdateManagementAsset, UpdateManagementAssetProfile,
    UpdateManagementDevice, UpdateManagementDeviceProfile,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool, types::Json};
use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

mod common;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-profiles.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

fn device_profile(name: &str) -> CreateManagementDeviceProfile {
    CreateManagementDeviceProfile {
        name: name.to_owned(),
        telemetry_schema: json!({"temperature_c": {"type": "number"}}),
        metric_mapping: json!({"temperature_c": "temperature"}),
        reporting_settings: json!({"interval_seconds": 60}),
    }
}

fn asset_profile(name: &str) -> CreateManagementAssetProfile {
    CreateManagementAssetProfile {
        name: name.to_owned(),
        fields: json!({"location": {"type": "string"}}),
        dashboard_defaults: json!({"layout": "summary"}),
    }
}

#[tokio::test]
async fn sqlite_management_profile_repositories_create_list_update_and_delete() {
    let (_directory, store) = sqlite_store().await;

    let device_z = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Z device profile"),
    )
    .await
    .unwrap();
    let device_a = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("A device profile"),
    )
    .await
    .unwrap();
    assert_eq!(
        ManagementDeviceProfileRepository::list_management_device_profiles(&store)
            .await
            .unwrap()
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>(),
        ["A device profile", "Z device profile"]
    );
    let updated_device = ManagementDeviceProfileRepository::update_management_device_profile(
        &store,
        device_z.id,
        UpdateManagementDeviceProfile {
            name: "Renamed device profile".to_owned(),
            telemetry_schema: json!({"humidity_pct": {"type": "number"}}),
            metric_mapping: json!({"humidity_pct": "humidity"}),
            reporting_settings: json!({"interval_seconds": 300}),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated_device.name, "Renamed device profile");
    assert_eq!(
        updated_device.metric_mapping,
        json!({"humidity_pct": "humidity"})
    );
    ManagementDeviceProfileRepository::delete_management_device_profile(&store, device_a.id)
        .await
        .unwrap();
    assert_eq!(
        ManagementDeviceProfileRepository::list_management_device_profiles(&store)
            .await
            .unwrap(),
        [updated_device]
    );

    let asset_z = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("Z asset profile"),
    )
    .await
    .unwrap();
    let asset_a = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("A asset profile"),
    )
    .await
    .unwrap();
    assert_eq!(
        ManagementAssetProfileRepository::list_management_asset_profiles(&store)
            .await
            .unwrap()
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>(),
        ["A asset profile", "Z asset profile"]
    );
    let updated_asset = ManagementAssetProfileRepository::update_management_asset_profile(
        &store,
        asset_z.id,
        UpdateManagementAssetProfile {
            name: "Renamed asset profile".to_owned(),
            fields: json!({"floor": {"type": "integer"}}),
            dashboard_defaults: json!({"layout": "detail"}),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated_asset.fields, json!({"floor": {"type": "integer"}}));
    ManagementAssetProfileRepository::delete_management_asset_profile(&store, asset_a.id)
        .await
        .unwrap();
    assert_eq!(
        ManagementAssetProfileRepository::list_management_asset_profiles(&store)
            .await
            .unwrap(),
        [updated_asset]
    );
}

#[tokio::test]
async fn sqlite_management_profile_repositories_return_typed_errors_and_protect_references() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let device = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Referenced device profile"),
    )
    .await
    .unwrap();
    let asset = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("Referenced asset profile"),
    )
    .await
    .unwrap();

    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            device_profile(" "),
        )
        .await,
        Err(ManagementDeviceProfileError::InvalidName)
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            CreateManagementDeviceProfile {
                name: "Invalid device JSON".to_owned(),
                telemetry_schema: json!([]),
                metric_mapping: json!(null),
                reporting_settings: json!("hourly"),
            },
        )
        .await,
        Err(ManagementDeviceProfileError::TelemetrySchemaMustBeObject)
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            device_profile("Referenced device profile"),
        )
        .await,
        Err(ManagementDeviceProfileError::NameConflict(name))
            if name == "Referenced device profile"
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::update_management_device_profile(
            &store,
            Uuid::now_v7(),
            UpdateManagementDeviceProfile {
                name: "Missing device profile".to_owned(),
                telemetry_schema: json!({}),
                metric_mapping: json!({}),
                reporting_settings: json!({}),
            },
        )
        .await,
        Err(ManagementDeviceProfileError::DeviceProfileNotFound)
    ));
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id)
         VALUES ('profile-reference-device', 'Profile reference', ?)",
    )
    .bind(device.id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        ManagementDeviceProfileRepository::delete_management_device_profile(&store, device.id)
            .await,
        Err(ManagementDeviceProfileError::DeviceProfileInUse(id)) if id == device.id
    ));

    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            asset_profile(" "),
        )
        .await,
        Err(ManagementAssetProfileError::InvalidName)
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            CreateManagementAssetProfile {
                name: "Invalid asset JSON".to_owned(),
                fields: json!(false),
                dashboard_defaults: json!(["summary"]),
            },
        )
        .await,
        Err(ManagementAssetProfileError::FieldsMustBeObject)
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            asset_profile("Referenced asset profile"),
        )
        .await,
        Err(ManagementAssetProfileError::NameConflict(name))
            if name == "Referenced asset profile"
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::update_management_asset_profile(
            &store,
            Uuid::now_v7(),
            UpdateManagementAssetProfile {
                name: "Missing asset profile".to_owned(),
                fields: json!({}),
                dashboard_defaults: json!({}),
            },
        )
        .await,
        Err(ManagementAssetProfileError::AssetProfileNotFound)
    ));
    sqlx::query(
        "INSERT INTO assets (id, name, asset_profile_id)
         VALUES (?, 'profile-reference-asset', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(asset.id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        ManagementAssetProfileRepository::delete_management_asset_profile(&store, asset.id).await,
        Err(ManagementAssetProfileError::AssetProfileInUse(id)) if id == asset.id
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::delete_management_asset_profile(&store, Uuid::now_v7())
            .await,
        Err(ManagementAssetProfileError::AssetProfileNotFound)
    ));
}

#[tokio::test]
async fn sqlite_device_profile_deletion_preserves_active_references_and_clears_soft_deleted_ones() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let active_profile = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Active reference device profile"),
    )
    .await
    .unwrap();
    let deleted_profile = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Soft-deleted reference device profile"),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id)
         VALUES ('active-profile-reference-device', 'Active profile reference', ?)",
    )
    .bind(active_profile.id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id, deleted_at)
         VALUES (
             'soft-deleted-profile-reference-device',
             'Soft-deleted profile reference',
             ?,
             CURRENT_TIMESTAMP
         )",
    )
    .bind(deleted_profile.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert!(matches!(
        ManagementDeviceProfileRepository::delete_management_device_profile(
            &store,
            active_profile.id,
        )
        .await,
        Err(ManagementDeviceProfileError::DeviceProfileInUse(id)) if id == active_profile.id
    ));

    ManagementDeviceProfileRepository::delete_management_device_profile(&store, deleted_profile.id)
        .await
        .unwrap();
    let stale_reference: Option<String> = sqlx::query_scalar(
        "SELECT device_profile_id
         FROM devices
         WHERE device_id = 'soft-deleted-profile-reference-device'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(stale_reference, None);
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

async fn wait_for_timescale_table_lock(pool: &PgPool, table: &str, mode: &str, granted: bool) {
    timeout(Duration::from_secs(2), async {
        loop {
            let found: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'relation'
                      AND relation = $1::regclass
                      AND mode = $2
                      AND granted = $3
                )",
            )
            .bind(format!("iot_nano.{table}"))
            .bind(mode)
            .bind(granted)
            .fetch_one(pool)
            .await
            .unwrap();
            if found {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{table} did not reach {mode} granted={granted}"));
}

async fn wait_for_timescale_table_wait(pool: &PgPool, table: &str) {
    timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'relation'
                      AND relation = $1::regclass
                      AND granted = FALSE
                )",
            )
            .bind(format!("iot_nano.{table}"))
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{table} did not reach a lock wait"));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_profile_repositories_match_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let device_z = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Timescale Z device profile"),
    )
    .await
    .unwrap();
    let device_a = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Timescale A device profile"),
    )
    .await
    .unwrap();
    assert_eq!(
        ManagementDeviceProfileRepository::list_management_device_profiles(&store)
            .await
            .unwrap()
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>(),
        ["Timescale A device profile", "Timescale Z device profile"]
    );
    let updated_device = ManagementDeviceProfileRepository::update_management_device_profile(
        &store,
        device_z.id,
        UpdateManagementDeviceProfile {
            name: "Timescale renamed device profile".to_owned(),
            telemetry_schema: json!({"humidity_pct": {"type": "number"}}),
            metric_mapping: json!({"humidity_pct": "humidity"}),
            reporting_settings: json!({"interval_seconds": 300}),
        },
    )
    .await
    .unwrap();
    let stored_device_json: Json<serde_json::Value> =
        sqlx::query_scalar("SELECT telemetry_schema FROM device_profiles WHERE id = $1")
            .bind(updated_device.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(stored_device_json.0, updated_device.telemetry_schema);
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            device_profile("Timescale A device profile"),
        )
        .await,
        Err(ManagementDeviceProfileError::NameConflict(name))
            if name == "Timescale A device profile"
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            CreateManagementDeviceProfile {
                name: "Invalid Timescale device JSON".to_owned(),
                telemetry_schema: json!([]),
                metric_mapping: json!({}),
                reporting_settings: json!({}),
            },
        )
        .await,
        Err(ManagementDeviceProfileError::TelemetrySchemaMustBeObject)
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            device_profile(" "),
        )
        .await,
        Err(ManagementDeviceProfileError::InvalidName)
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            CreateManagementDeviceProfile {
                name: "Invalid Timescale device mapping".to_owned(),
                telemetry_schema: json!({}),
                metric_mapping: json!(null),
                reporting_settings: json!({}),
            },
        )
        .await,
        Err(ManagementDeviceProfileError::MetricMappingMustBeObject)
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            CreateManagementDeviceProfile {
                name: "Invalid Timescale device reporting".to_owned(),
                telemetry_schema: json!({}),
                metric_mapping: json!({}),
                reporting_settings: json!("hourly"),
            },
        )
        .await,
        Err(ManagementDeviceProfileError::ReportingSettingsMustBeObject)
    ));
    assert!(matches!(
        ManagementDeviceProfileRepository::update_management_device_profile(
            &store,
            Uuid::now_v7(),
            UpdateManagementDeviceProfile {
                name: "Missing Timescale device profile".to_owned(),
                telemetry_schema: json!({}),
                metric_mapping: json!({}),
                reporting_settings: json!({}),
            },
        )
        .await,
        Err(ManagementDeviceProfileError::DeviceProfileNotFound)
    ));
    ManagementDeviceProfileRepository::delete_management_device_profile(&store, device_a.id)
        .await
        .unwrap();
    assert_eq!(
        ManagementDeviceProfileRepository::list_management_device_profiles(&store)
            .await
            .unwrap(),
        [updated_device.clone()]
    );

    let asset_z = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("Timescale Z asset profile"),
    )
    .await
    .unwrap();
    let asset_a = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("Timescale A asset profile"),
    )
    .await
    .unwrap();
    assert_eq!(
        ManagementAssetProfileRepository::list_management_asset_profiles(&store)
            .await
            .unwrap()
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>(),
        ["Timescale A asset profile", "Timescale Z asset profile"]
    );
    let updated_asset = ManagementAssetProfileRepository::update_management_asset_profile(
        &store,
        asset_z.id,
        UpdateManagementAssetProfile {
            name: "Timescale renamed asset profile".to_owned(),
            fields: json!({"floor": {"type": "integer"}}),
            dashboard_defaults: json!({"layout": "detail"}),
        },
    )
    .await
    .unwrap();
    let stored_asset_json: Json<serde_json::Value> =
        sqlx::query_scalar("SELECT fields FROM asset_profiles WHERE id = $1")
            .bind(updated_asset.id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(stored_asset_json.0, updated_asset.fields);
    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            asset_profile("Timescale A asset profile"),
        )
        .await,
        Err(ManagementAssetProfileError::NameConflict(name))
            if name == "Timescale A asset profile"
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            CreateManagementAssetProfile {
                name: "Invalid Timescale asset JSON".to_owned(),
                fields: json!(false),
                dashboard_defaults: json!({}),
            },
        )
        .await,
        Err(ManagementAssetProfileError::FieldsMustBeObject)
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            asset_profile(" "),
        )
        .await,
        Err(ManagementAssetProfileError::InvalidName)
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::create_management_asset_profile(
            &store,
            CreateManagementAssetProfile {
                name: "Invalid Timescale asset dashboard".to_owned(),
                fields: json!({}),
                dashboard_defaults: json!(["summary"]),
            },
        )
        .await,
        Err(ManagementAssetProfileError::DashboardDefaultsMustBeObject)
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::update_management_asset_profile(
            &store,
            Uuid::now_v7(),
            UpdateManagementAssetProfile {
                name: "Missing Timescale asset profile".to_owned(),
                fields: json!({}),
                dashboard_defaults: json!({}),
            },
        )
        .await,
        Err(ManagementAssetProfileError::AssetProfileNotFound)
    ));
    ManagementAssetProfileRepository::delete_management_asset_profile(&store, asset_a.id)
        .await
        .unwrap();
    assert_eq!(
        ManagementAssetProfileRepository::list_management_asset_profiles(&store)
            .await
            .unwrap(),
        [updated_asset.clone()]
    );

    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id)
         VALUES ('timescale-profile-reference-device', 'Profile reference', $1)",
    )
    .bind(updated_device.id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, name, asset_profile_id)
         VALUES ($1, 'timescale-profile-reference-asset', $2)",
    )
    .bind(Uuid::now_v7())
    .bind(updated_asset.id)
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        ManagementDeviceProfileRepository::delete_management_device_profile(&store, updated_device.id)
            .await,
        Err(ManagementDeviceProfileError::DeviceProfileInUse(id)) if id == updated_device.id
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::delete_management_asset_profile(&store, updated_asset.id).await,
        Err(ManagementAssetProfileError::AssetProfileInUse(id)) if id == updated_asset.id
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_device_profile_deletion_handles_active_and_soft_deleted_references() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let active_profile = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Timescale active reference device profile"),
    )
    .await
    .unwrap();
    let deleted_profile = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Timescale soft-deleted reference device profile"),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id)
         VALUES ('timescale-active-profile-reference', 'Active profile reference', $1)",
    )
    .bind(active_profile.id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id, deleted_at)
         VALUES (
             'timescale-soft-deleted-profile-reference',
             'Soft-deleted profile reference',
             $1,
             now()
         )",
    )
    .bind(deleted_profile.id)
    .execute(pool)
    .await
    .unwrap();

    assert!(matches!(
        ManagementDeviceProfileRepository::delete_management_device_profile(
            &store,
            active_profile.id,
        )
        .await,
        Err(ManagementDeviceProfileError::DeviceProfileInUse(id)) if id == active_profile.id
    ));

    ManagementDeviceProfileRepository::delete_management_device_profile(&store, deleted_profile.id)
        .await
        .unwrap();
    let stale_reference: Option<Uuid> = sqlx::query_scalar(
        "SELECT device_profile_id
         FROM devices
         WHERE device_id = 'timescale-soft-deleted-profile-reference'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(stale_reference, None);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_device_profile_deletion_serializes_assignment_and_returns_typed_error() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();

    let device_profile = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Concurrent device profile"),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, display_name) VALUES ($1, $2)")
        .bind("concurrent-profile-device")
        .bind("Concurrent profile device")
        .execute(pool)
        .await
        .unwrap();
    let mut device_profile_gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    sqlx::query("BEGIN")
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM device_profiles WHERE id = $1 FOR UPDATE")
        .bind(device_profile.id)
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    let delete_store = store.clone();
    let mut device_delete = tokio::spawn(async move {
        ManagementDeviceProfileRepository::delete_management_device_profile(
            &delete_store,
            device_profile.id,
        )
        .await
    });
    wait_for_timescale_table_lock(pool, "devices", "ShareRowExclusiveLock", true).await;
    let update_store = store.clone();
    let mut device_update = tokio::spawn(async move {
        ManagementDeviceRepository::update_management_device(
            &update_store,
            "concurrent-profile-device",
            UpdateManagementDevice {
                display_name: "Concurrent profile device".to_owned(),
                asset_id: None,
                device_profile_id: Some(device_profile.id),
                attributes: Some(json!({})),
                topology: None,
            },
        )
        .await
    });
    wait_for_timescale_table_wait(pool, "devices").await;
    let mut device_probe = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut device_probe)
        .await
        .unwrap();
    sqlx::query(
        "SELECT device_id FROM devices
         WHERE device_id = 'concurrent-profile-device'
         FOR UPDATE NOWAIT",
    )
    .execute(&mut device_probe)
    .await
    .expect("profile assignment must wait on the table lock before locking its device row");
    sqlx::query("COMMIT")
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), &mut device_delete)
            .await
            .expect("device profile deletion deadlocked")
            .unwrap()
            .is_ok()
    );
    assert!(matches!(
        timeout(Duration::from_secs(2), &mut device_update)
            .await
            .expect("device profile assignment deadlocked")
            .unwrap(),
        Err(ManagementDeviceError::DeviceProfileUnavailable(id)) if id == device_profile.id
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_profile_deletion_blocks_profile_assignments_before_target_row_locks() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();

    let device_profile = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Concurrent device profile"),
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, display_name) VALUES ($1, $2)")
        .bind("concurrent-profile-device")
        .bind("Concurrent profile device")
        .execute(pool)
        .await
        .unwrap();
    let mut device_profile_gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    sqlx::query("BEGIN")
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM device_profiles WHERE id = $1 FOR UPDATE")
        .bind(device_profile.id)
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    let delete_store = store.clone();
    let mut device_delete = tokio::spawn(async move {
        ManagementDeviceProfileRepository::delete_management_device_profile(
            &delete_store,
            device_profile.id,
        )
        .await
    });
    wait_for_timescale_table_lock(pool, "devices", "ShareRowExclusiveLock", true).await;
    let update_store = store.clone();
    let mut device_update = tokio::spawn(async move {
        ManagementDeviceRepository::update_management_device(
            &update_store,
            "concurrent-profile-device",
            UpdateManagementDevice {
                display_name: "Concurrent profile device".to_owned(),
                asset_id: None,
                device_profile_id: Some(device_profile.id),
                attributes: Some(json!({})),
                topology: None,
            },
        )
        .await
    });
    wait_for_timescale_table_wait(pool, "devices").await;
    let mut device_probe = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut device_probe)
        .await
        .unwrap();
    sqlx::query(
        "SELECT device_id FROM devices
         WHERE device_id = 'concurrent-profile-device'
         FOR UPDATE NOWAIT",
    )
    .execute(&mut device_probe)
    .await
    .expect("profile assignment must wait on the table lock before locking its device row");
    sqlx::query("COMMIT")
        .execute(&mut device_profile_gate)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), &mut device_delete)
            .await
            .expect("device profile deletion deadlocked")
            .unwrap()
            .is_ok()
    );
    assert!(matches!(
        timeout(Duration::from_secs(2), &mut device_update)
            .await
            .expect("device profile assignment deadlocked")
            .unwrap(),
        Err(ManagementDeviceError::DeviceProfileUnavailable(id)) if id == device_profile.id
    ));

    let asset_profile = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("Concurrent asset profile"),
    )
    .await
    .unwrap();
    let asset_id = Uuid::now_v7();
    sqlx::query("INSERT INTO assets (id, name) VALUES ($1, $2)")
        .bind(asset_id)
        .bind("Concurrent profile asset")
        .execute(pool)
        .await
        .unwrap();
    let mut asset_profile_gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut asset_profile_gate)
        .await
        .unwrap();
    sqlx::query("BEGIN")
        .execute(&mut asset_profile_gate)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM asset_profiles WHERE id = $1 FOR UPDATE")
        .bind(asset_profile.id)
        .execute(&mut asset_profile_gate)
        .await
        .unwrap();
    let delete_store = store.clone();
    let mut asset_delete = tokio::spawn(async move {
        ManagementAssetProfileRepository::delete_management_asset_profile(
            &delete_store,
            asset_profile.id,
        )
        .await
    });
    wait_for_timescale_table_lock(pool, "assets", "ShareRowExclusiveLock", true).await;
    let update_store = store.clone();
    let mut asset_update = tokio::spawn(async move {
        ManagementAssetRepository::update_management_asset(
            &update_store,
            asset_id,
            UpdateManagementAsset {
                name: "Concurrent profile asset".to_owned(),
                asset_profile_id: Some(asset_profile.id),
                parent_asset_id: None,
                metadata: json!({}),
                attributes: Some(json!({})),
            },
        )
        .await
    });
    wait_for_timescale_table_wait(pool, "assets").await;
    let mut asset_probe = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut asset_probe)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM assets WHERE id = $1 FOR UPDATE NOWAIT")
        .bind(asset_id)
        .execute(&mut asset_probe)
        .await
        .expect("profile assignment must wait on the table lock before locking its asset row");
    sqlx::query("COMMIT")
        .execute(&mut asset_profile_gate)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), &mut asset_delete)
            .await
            .expect("asset profile deletion deadlocked")
            .unwrap()
            .is_ok()
    );
    assert!(matches!(
        timeout(Duration::from_secs(2), &mut asset_update)
            .await
            .expect("asset profile assignment deadlocked")
            .unwrap(),
        Err(ManagementAssetError::AssetProfileUnavailable(id)) if id == asset_profile.id
    ));
}
