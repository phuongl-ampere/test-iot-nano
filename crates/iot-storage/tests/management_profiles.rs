use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    CreateManagementAssetProfile, CreateManagementDeviceProfile, ManagementAssetProfileError,
    ManagementAssetProfileRepository, ManagementDeviceProfileError,
    ManagementDeviceProfileRepository, PlatformStore, UpdateManagementAssetProfile,
    UpdateManagementDeviceProfile,
};
use serde_json::json;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:management-profiles-test'))")
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
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_profile_repositories_match_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let device = ManagementDeviceProfileRepository::create_management_device_profile(
        &store,
        device_profile("Timescale device profile"),
    )
    .await
    .unwrap();
    let asset = ManagementAssetProfileRepository::create_management_asset_profile(
        &store,
        asset_profile("Timescale asset profile"),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, device_profile_id)
         VALUES ('timescale-profile-reference-device', 'Profile reference', $1)",
    )
    .bind(device.id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, name, asset_profile_id)
         VALUES ($1, 'timescale-profile-reference-asset', $2)",
    )
    .bind(Uuid::now_v7())
    .bind(asset.id)
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        ManagementDeviceProfileRepository::delete_management_device_profile(&store, device.id)
            .await,
        Err(ManagementDeviceProfileError::DeviceProfileInUse(id)) if id == device.id
    ));
    assert!(matches!(
        ManagementAssetProfileRepository::delete_management_asset_profile(&store, asset.id).await,
        Err(ManagementAssetProfileError::AssetProfileInUse(id)) if id == asset.id
    ));
}
