use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    CreateManagementAsset, ManagementAsset, ManagementAssetError, ManagementAssetRepository,
    PlatformStore, UpdateManagementAsset,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool};
use tokio::{
    sync::Barrier,
    time::{Duration, timeout},
};
use uuid::Uuid;

mod common;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-assets.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_id = seed_tenant(store.sqlite_pool().unwrap(), "management-assets").await;
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
    tenant_id
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

fn asset_update(
    asset: &ManagementAsset,
    name: &str,
    parent_asset_id: Option<Uuid>,
) -> UpdateManagementAsset {
    UpdateManagementAsset {
        name: name.to_owned(),
        asset_profile_id: asset.asset_profile_id,
        parent_asset_id,
        metadata: asset.metadata.clone(),
        attributes: Some(asset.attributes.clone()),
    }
}

#[tokio::test]
async fn sqlite_management_assets_reject_cross_tenant_lookup_mutation_and_parent_references() {
    let (_directory, store, _default_tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let tenant_a = seed_tenant(pool, "asset-tenant-a").await;
    let tenant_b = seed_tenant(pool, "asset-tenant-b").await;
    let tenant_b_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, metadata) VALUES (?, ?, 'tenant-b asset', '{}')",
    )
    .bind(tenant_b_asset_id.to_string())
    .bind(tenant_b.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert!(
        ManagementAssetRepository::list_management_assets(&store, tenant_a)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        ManagementAssetRepository::update_management_asset(
            &store,
            tenant_a,
            tenant_b_asset_id,
            UpdateManagementAsset {
                name: "renamed".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
                attributes: None,
            },
        )
        .await,
        Err(ManagementAssetError::AssetNotFound)
    ));
    assert!(matches!(
        ManagementAssetRepository::delete_management_asset(&store, tenant_a, tenant_b_asset_id)
            .await,
        Err(ManagementAssetError::AssetNotFound)
    ));
    assert!(matches!(
        ManagementAssetRepository::create_management_asset(
            &store,
            tenant_a,
            asset_mutation("cross-tenant child", None, Some(tenant_b_asset_id)),
        )
        .await,
        Err(ManagementAssetError::ParentAssetUnavailable(id)) if id == tenant_b_asset_id
    ));
}

#[tokio::test]
async fn sqlite_management_asset_repository_creates_lists_updates_and_deletes_assets() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name)
         VALUES (?, ?, 'management asset profile')",
    )
    .bind(profile_id.to_string())
    .bind(tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
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
        tenant_id,
        asset_mutation("Child asset", None, Some(parent.id)),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('management-asset-device', ?, 'Asset device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(parent.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let listed = ManagementAssetRepository::list_management_assets(&store, tenant_id)
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
        tenant_id,
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

    ManagementAssetRepository::delete_management_asset(&store, tenant_id, parent.id)
        .await
        .unwrap();
    let mut detached_child = updated.clone();
    detached_child.parent_asset_id = None;
    assert_eq!(
        ManagementAssetRepository::list_management_assets(&store, tenant_id)
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
    let (_directory, store, tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let root = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Root asset", None, None),
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Child asset", None, Some(root.id)),
    )
    .await
    .unwrap();

    let invalid_name = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation(" ", None, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(invalid_name, ManagementAssetError::InvalidName));

    let invalid_metadata = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
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
        tenant_id,
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
        tenant_id,
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
        tenant_id,
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
        tenant_id,
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
        tenant_id,
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
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('dangling-management-asset-device', ?, 'Dangling asset device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(dangling_asset_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let missing_asset =
        ManagementAssetRepository::delete_management_asset(&store, tenant_id, dangling_asset_id)
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

#[tokio::test]
async fn sqlite_management_asset_repository_maps_sibling_name_conflicts_to_domain_errors() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Conflict parent", None, None),
    )
    .await
    .unwrap();
    let first = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Duplicate sibling", None, Some(parent.id)),
    )
    .await
    .unwrap();
    let second = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Other sibling", None, Some(parent.id)),
    )
    .await
    .unwrap();

    let create_conflict = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Duplicate sibling", None, Some(parent.id)),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: Some(parent_asset_id),
        } if name == "Duplicate sibling" && parent_asset_id == parent.id
    ));

    let update_conflict = ManagementAssetRepository::update_management_asset(
        &store,
        tenant_id,
        second.id,
        asset_update(&second, &first.name, Some(parent.id)),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        update_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: Some(parent_asset_id),
        } if name == "Duplicate sibling" && parent_asset_id == parent.id
    ));
}

#[tokio::test]
async fn sqlite_management_asset_repository_maps_root_name_conflicts_to_domain_errors() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let first = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Duplicate root", None, None),
    )
    .await
    .unwrap();
    let second = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Other root", None, None),
    )
    .await
    .unwrap();

    let create_conflict = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation(&first.name, None, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: None,
        } if name == "Duplicate root"
    ));

    let update_conflict = ManagementAssetRepository::update_management_asset(
        &store,
        tenant_id,
        second.id,
        asset_update(&second, &first.name, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        update_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: None,
        } if name == "Duplicate root"
    ));
}

#[tokio::test]
async fn sqlite_management_asset_delete_rejects_child_root_name_collisions_before_mutating() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let existing_root = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Promoted child", None, None),
    )
    .await
    .unwrap();
    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Delete conflict parent", None, None),
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation(&existing_root.name, None, Some(parent.id)),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('delete-conflict-device', ?, 'Delete conflict device', ?)",
    )
    .bind(tenant_id.to_string())
    .bind(parent.id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let error = ManagementAssetRepository::delete_management_asset(&store, tenant_id, parent.id)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: None,
        } if name == "Promoted child"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE id = ?")
            .bind(parent.id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT parent_asset_id FROM assets WHERE id = ?")
            .bind(child.id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
        Some(parent.id.to_string())
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT asset_id FROM devices WHERE device_id = 'delete-conflict-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(parent.id.to_string())
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
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'management-assets', 'active')",
    )
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
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_asset_repository_matches_sqlite_contract() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name)
         VALUES ($1, $2, 'timescale management asset profile')",
    )
    .bind(profile_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();

    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Timescale parent asset", Some(profile_id), None),
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Timescale child asset", None, Some(parent.id)),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('timescale-management-asset-device', $1, 'Asset device', $2)",
    )
    .bind(tenant_id)
    .bind(parent.id)
    .execute(pool)
    .await
    .unwrap();

    let cycle = ManagementAssetRepository::update_management_asset(
        &store,
        tenant_id,
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

    ManagementAssetRepository::delete_management_asset(&store, tenant_id, parent.id)
        .await
        .unwrap();
    let child = ManagementAssetRepository::list_management_assets(&store, tenant_id)
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

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_asset_repository_covers_crud_validation_and_references() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO asset_profiles (id, tenant_id, name)
         VALUES ($1, $2, 'timescale expanded management asset profile')",
    )
    .bind(profile_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();

    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Expanded Timescale parent", Some(profile_id), None),
    )
    .await
    .unwrap();
    assert_eq!(parent.asset_profile_id, Some(profile_id));
    assert_eq!(parent.parent_asset_id, None);
    assert_eq!(parent.metadata, json!({"zone": "lab"}));
    assert_eq!(parent.attributes, json!({"zone": "lab"}));

    let child = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Expanded Timescale child", None, Some(parent.id)),
    )
    .await
    .unwrap();
    assert_eq!(
        ManagementAssetRepository::list_management_assets(&store, tenant_id)
            .await
            .unwrap()
            .into_iter()
            .map(|asset| asset.name)
            .collect::<Vec<_>>(),
        ["Expanded Timescale child", "Expanded Timescale parent"]
    );

    let updated_child = ManagementAssetRepository::update_management_asset(
        &store,
        tenant_id,
        child.id,
        UpdateManagementAsset {
            name: "Expanded Timescale child".to_owned(),
            asset_profile_id: Some(profile_id),
            parent_asset_id: Some(parent.id),
            metadata: json!({"ignored": true}),
            attributes: Some(json!({"zone": "warehouse"})),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated_child.asset_profile_id, Some(profile_id));
    assert_eq!(updated_child.metadata, json!({"zone": "warehouse"}));
    assert_eq!(updated_child.attributes, json!({"zone": "warehouse"}));

    let sibling_conflict = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Expanded Timescale child", None, Some(parent.id)),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        sibling_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: Some(parent_asset_id),
        } if name == "Expanded Timescale child" && parent_asset_id == parent.id
    ));

    assert!(matches!(
        ManagementAssetRepository::create_management_asset(
            &store,
            tenant_id,
            asset_mutation(" ", None, None),
        )
        .await,
        Err(ManagementAssetError::InvalidName)
    ));
    assert!(matches!(
        ManagementAssetRepository::create_management_asset(
            &store,
            tenant_id,
            CreateManagementAsset {
                name: "Invalid Timescale metadata".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!("not an object"),
                attributes: None,
            },
        )
        .await,
        Err(ManagementAssetError::MetadataMustBeObject)
    ));
    assert!(matches!(
        ManagementAssetRepository::create_management_asset(
            &store,
            tenant_id,
            CreateManagementAsset {
                name: "Invalid Timescale attributes".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
                attributes: Some(json!(["not", "an", "object"])),
            },
        )
        .await,
        Err(ManagementAssetError::AttributesMustBeObject)
    ));
    assert!(matches!(
        ManagementAssetRepository::create_management_asset(
            &store,
            tenant_id,
            asset_mutation("Missing Timescale profile", Some(Uuid::now_v7()), None),
        )
        .await,
        Err(ManagementAssetError::AssetProfileUnavailable(_))
    ));
    assert!(matches!(
        ManagementAssetRepository::create_management_asset(
            &store,
            tenant_id,
            asset_mutation("Missing Timescale parent", None, Some(Uuid::now_v7())),
        )
        .await,
        Err(ManagementAssetError::ParentAssetUnavailable(_))
    ));
    assert!(matches!(
        ManagementAssetRepository::update_management_asset(
            &store,
            tenant_id,
            parent.id,
            asset_update(&parent, &parent.name, Some(parent.id)),
        )
        .await,
        Err(ManagementAssetError::AssetCannotBeOwnParent)
    ));
    assert!(matches!(
        ManagementAssetRepository::update_management_asset(
            &store,
            tenant_id,
            parent.id,
            asset_update(&parent, &parent.name, Some(updated_child.id)),
        )
        .await,
        Err(ManagementAssetError::AssetCannotHaveDescendantParent)
    ));

    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('expanded-timescale-management-asset-device', $1, 'Asset device', $2)",
    )
    .bind(tenant_id)
    .bind(parent.id)
    .execute(pool)
    .await
    .unwrap();
    ManagementAssetRepository::delete_management_asset(&store, tenant_id, parent.id)
        .await
        .unwrap();
    let detached_child = ManagementAssetRepository::list_management_assets(&store, tenant_id)
        .await
        .unwrap()
        .into_iter()
        .find(|asset| asset.id == updated_child.id)
        .unwrap();
    assert_eq!(detached_child.parent_asset_id, None);
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT asset_id FROM devices
             WHERE device_id = 'expanded-timescale-management-asset-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );
    assert!(matches!(
        ManagementAssetRepository::delete_management_asset(&store, tenant_id, Uuid::now_v7()).await,
        Err(ManagementAssetError::AssetNotFound)
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_asset_repository_maps_root_name_conflicts_to_domain_errors() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let first = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Duplicate Timescale root", None, None),
    )
    .await
    .unwrap();
    let second = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Other Timescale root", None, None),
    )
    .await
    .unwrap();

    let create_conflict = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation(&first.name, None, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: None,
        } if name == "Duplicate Timescale root"
    ));

    let update_conflict = ManagementAssetRepository::update_management_asset(
        &store,
        tenant_id,
        second.id,
        asset_update(&second, &first.name, None),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        update_conflict,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: None,
        } if name == "Duplicate Timescale root"
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_asset_delete_rejects_child_root_name_collisions_before_mutating() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let existing_root = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Promoted Timescale child", None, None),
    )
    .await
    .unwrap();
    let parent = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Timescale delete conflict parent", None, None),
    )
    .await
    .unwrap();
    let child = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation(&existing_root.name, None, Some(parent.id)),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, display_name, asset_id)
         VALUES ('timescale-delete-conflict-device', $1, 'Delete conflict device', $2)",
    )
    .bind(tenant_id)
    .bind(parent.id)
    .execute(pool)
    .await
    .unwrap();

    let error = ManagementAssetRepository::delete_management_asset(&store, tenant_id, parent.id)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ManagementAssetError::SiblingNameConflict {
            ref name,
            parent_asset_id: None,
        } if name == "Promoted Timescale child"
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets WHERE id = $1")
            .bind(parent.id)
            .fetch_one(pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT parent_asset_id FROM assets WHERE id = $1")
            .bind(child.id)
            .fetch_one(pool)
            .await
            .unwrap(),
        Some(parent.id)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT asset_id FROM devices WHERE device_id = 'timescale-delete-conflict-device'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(parent.id)
    );
}

async fn waiting_asset_update_barrier_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM pg_locks
         WHERE locktype = 'advisory'
           AND classid = 7103
           AND objid = 7103
           AND NOT granted",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_reciprocal_parent_updates_do_not_create_a_cycle() {
    let (_lock, store, tenant_id) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let first = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Concurrent first", None, None),
    )
    .await
    .unwrap();
    let second = ManagementAssetRepository::create_management_asset(
        &store,
        tenant_id,
        asset_mutation("Concurrent second", None, None),
    )
    .await
    .unwrap();
    let first_id = first.id;
    let second_id = second.id;

    sqlx::raw_sql(
        "CREATE FUNCTION gate_management_asset_parent_update() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock(7103, 7103);
             RETURN NEW;
         END;
         $$;
         CREATE TRIGGER gate_management_asset_parent_update_trigger
         BEFORE UPDATE OF parent_asset_id ON assets
         FOR EACH ROW EXECUTE FUNCTION gate_management_asset_parent_update();",
    )
    .execute(pool)
    .await
    .unwrap();

    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
    let mut gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(7103, 7103)")
        .execute(&mut gate)
        .await
        .unwrap();

    let barrier = std::sync::Arc::new(Barrier::new(3));
    let first_store = store.clone();
    let first_tenant_id = tenant_id;
    let first_barrier = std::sync::Arc::clone(&barrier);
    let first_update = tokio::spawn(async move {
        first_barrier.wait().await;
        ManagementAssetRepository::update_management_asset(
            &first_store,
            first_tenant_id,
            first_id,
            asset_update(&first, &first.name, Some(second_id)),
        )
        .await
    });
    let second_store = store.clone();
    let second_tenant_id = tenant_id;
    let second_barrier = std::sync::Arc::clone(&barrier);
    let second_update = tokio::spawn(async move {
        second_barrier.wait().await;
        ManagementAssetRepository::update_management_asset(
            &second_store,
            second_tenant_id,
            second_id,
            asset_update(&second, &second.name, Some(first_id)),
        )
        .await
    });

    barrier.wait().await;
    timeout(Duration::from_secs(2), async {
        loop {
            if waiting_asset_update_barrier_count(pool).await >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an update did not reach the guarded write barrier");
    let both_reached_write_barrier = timeout(Duration::from_millis(250), async {
        loop {
            if waiting_asset_update_barrier_count(pool).await >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    sqlx::query("SELECT pg_advisory_unlock(7103, 7103)")
        .execute(&mut gate)
        .await
        .unwrap();
    assert!(
        !both_reached_write_barrier,
        "reciprocal updates both passed hierarchy validation before either write committed"
    );

    let (first_result, second_result) = timeout(Duration::from_secs(5), async {
        tokio::join!(first_update, second_update)
    })
    .await
    .expect("reciprocal parent updates deadlocked");
    let results = [first_result.unwrap(), second_result.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(results.iter().any(|result| {
        matches!(
            result,
            Err(ManagementAssetError::AssetCannotHaveDescendantParent)
        )
    }));

    let assets = ManagementAssetRepository::list_management_assets(&store, tenant_id)
        .await
        .unwrap();
    let first_parent = assets
        .iter()
        .find(|asset| asset.id == first_id)
        .unwrap()
        .parent_asset_id;
    let second_parent = assets
        .iter()
        .find(|asset| asset.id == second_id)
        .unwrap()
        .parent_asset_id;
    assert!(!(first_parent == Some(second_id) && second_parent == Some(first_id)));
}
