use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    CreateManagementAsset, ManagementAssetError, ManagementAssetRepository, PlatformStore,
    UpdateManagementAsset,
};
use serde_json::json;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-assets.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

fn asset_mutation(
    name: &str,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
) -> CreateManagementAsset {
    CreateManagementAsset {
        name: name.to_owned(),
        asset_profile_id,
        parent_asset_id,
        metadata: json!({"ignored": true}),
        attributes: Some(json!({"zone": "lab"})),
    }
}

#[tokio::test]
async fn sqlite_management_asset_repository_creates_lists_updates_and_deletes_assets() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, name)
         VALUES (?, 'management asset profile')",
    )
    .bind(profile_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Parent asset", Some(profile_id), None),
    )
    .await
    .unwrap();
    assert_eq!(parent.asset_profile_id, Some(profile_id));
    assert_eq!(parent.parent_asset_id, None);
    assert_eq!(parent.metadata, json!({"zone": "lab"}));
    assert_eq!(parent.attributes, json!({"zone": "lab"}));

    let child = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Child asset", None, Some(parent.id)),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, asset_id)
         VALUES ('management-asset-device', 'Asset device', ?)",
    )
    .bind(parent.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let listed = ManagementAssetRepository::list_management_assets(&store)
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|asset| asset.name.as_str())
            .collect::<Vec<_>>(),
        ["Child asset", "Parent asset"]
    );

    let updated = ManagementAssetRepository::update_management_asset(
        &store,
        child.id,
        UpdateManagementAsset {
            name: "Renamed child asset".to_owned(),
            asset_profile_id: Some(profile_id),
            parent_asset_id: Some(parent.id),
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.name, "Renamed child asset");
    assert_eq!(updated.asset_profile_id, Some(profile_id));
    assert_eq!(updated.metadata, json!({}));
    assert_eq!(updated.attributes, json!({}));

    ManagementAssetRepository::delete_management_asset(&store, parent.id)
        .await
        .unwrap();
    let mut detached_child = updated.clone();
    detached_child.parent_asset_id = None;
    assert_eq!(
        ManagementAssetRepository::list_management_assets(&store)
            .await
            .unwrap(),
        [detached_child]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT parent_asset_id FROM assets WHERE id = ?",)
            .bind(child.id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT asset_id FROM devices WHERE device_id = 'management-asset-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_management_asset_repository_returns_typed_validation_errors() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let root = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Root asset", None, None),
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Child asset", None, Some(root.id)),
    )
    .await
    .unwrap();

    let invalid_name =
        ManagementAssetRepository::create_management_asset(&store, asset_mutation(" ", None, None))
            .await
            .unwrap_err();
    assert!(matches!(invalid_name, ManagementAssetError::InvalidName));

    let invalid_metadata = ManagementAssetRepository::create_management_asset(
        &store,
        CreateManagementAsset {
            name: "Invalid metadata".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!("not an object"),
            attributes: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_metadata,
        ManagementAssetError::MetadataMustBeObject
    ));

    let invalid_attributes = ManagementAssetRepository::create_management_asset(
        &store,
        CreateManagementAsset {
            name: "Invalid attributes".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
            attributes: Some(json!(["not", "an", "object"])),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_attributes,
        ManagementAssetError::AttributesMustBeObject
    ));

    let unavailable_profile = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Missing profile", Some(Uuid::now_v7()), None),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        unavailable_profile,
        ManagementAssetError::AssetProfileUnavailable(_)
    ));

    let unavailable_parent = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Missing parent", None, Some(Uuid::now_v7())),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        unavailable_parent,
        ManagementAssetError::ParentAssetUnavailable(_)
    ));

    let own_parent = ManagementAssetRepository::update_management_asset(
        &store,
        root.id,
        UpdateManagementAsset {
            name: root.name.clone(),
            asset_profile_id: None,
            parent_asset_id: Some(root.id),
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        own_parent,
        ManagementAssetError::AssetCannotBeOwnParent
    ));

    let descendant_parent = ManagementAssetRepository::update_management_asset(
        &store,
        root.id,
        UpdateManagementAsset {
            name: root.name,
            asset_profile_id: None,
            parent_asset_id: Some(child.id),
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        descendant_parent,
        ManagementAssetError::AssetCannotHaveDescendantParent
    ));

    let dangling_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, asset_id)
         VALUES ('dangling-management-asset-device', 'Dangling asset device', ?)",
    )
    .bind(dangling_asset_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let missing_asset =
        ManagementAssetRepository::delete_management_asset(&store, dangling_asset_id)
            .await
            .unwrap_err();
    assert!(matches!(missing_asset, ManagementAssetError::AssetNotFound));
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT asset_id FROM devices WHERE device_id = 'dangling-management-asset-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(dangling_asset_id.to_string())
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets")
            .fetch_one(pool)
            .await
            .unwrap(),
        2
    );
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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:management-assets-test'))")
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
async fn timescale_management_asset_repository_matches_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, name)
         VALUES ($1, 'timescale management asset profile')",
    )
    .bind(profile_id)
    .execute(pool)
    .await
    .unwrap();

    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Timescale parent asset", Some(profile_id), None),
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        asset_mutation("Timescale child asset", None, Some(parent.id)),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, display_name, asset_id)
         VALUES ('timescale-management-asset-device', 'Asset device', $1)",
    )
    .bind(parent.id)
    .execute(pool)
    .await
    .unwrap();

    let cycle = ManagementAssetRepository::update_management_asset(
        &store,
        parent.id,
        UpdateManagementAsset {
            name: parent.name.clone(),
            asset_profile_id: parent.asset_profile_id,
            parent_asset_id: Some(child.id),
            metadata: parent.metadata.clone(),
            attributes: Some(parent.attributes.clone()),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        cycle,
        ManagementAssetError::AssetCannotHaveDescendantParent
    ));

    ManagementAssetRepository::delete_management_asset(&store, parent.id)
        .await
        .unwrap();
    let child = ManagementAssetRepository::list_management_assets(&store)
        .await
        .unwrap()
        .into_iter()
        .find(|asset| asset.id == child.id)
        .unwrap();
    assert_eq!(child.parent_asset_id, None);
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT asset_id FROM devices WHERE device_id = 'timescale-management-asset-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );
}
