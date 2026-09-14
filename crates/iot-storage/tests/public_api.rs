use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, NewPublicAsset, NewPublicDevice, NewPublicResourceGrant, PlatformStore,
    PublicApiRepository, PublicPrincipal, ResourcePermission,
};
use serde_json::json;
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("public-api.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

#[tokio::test]
async fn sqlite_public_repository_filters_assets_and_persists_grants() {
    let (_directory, store) = sqlite_store().await;
    let user_id = Uuid::now_v7();
    let other_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES (?, 'public-repository-user', 'unused', 'viewer', 'user'),
                (?, 'public-repository-other', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(other_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let principal = PublicPrincipal {
        user_id: Some(user_id),
        app_id: "public-repository-app".to_owned(),
        account_class: AccountClass::User,
    };
    let asset = PublicApiRepository::create_public_asset(
        &store,
        &principal,
        NewPublicAsset {
            name: "contract asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({"zone":"lab"}),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        PublicApiRepository::public_asset_permission(&store, &principal, asset.id)
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    let assets = PublicApiRepository::list_public_assets(&store, &principal, None, 10)
        .await
        .unwrap();
    assert_eq!(assets, [asset.clone()]);

    let grant = PublicApiRepository::create_public_grant(
        &store,
        &principal,
        NewPublicResourceGrant {
            resource_type: "asset".to_owned(),
            resource_id: asset.id.to_string(),
            grantee_type: "user".to_owned(),
            grantee_id: other_id.to_string(),
            permission: "viewer".to_owned(),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(grant.permission, "viewer");
    assert_eq!(
        PublicApiRepository::get_public_grant(&store, &principal, grant.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        grant.id
    );
}

#[tokio::test]
async fn sqlite_public_device_permission_denies_active_shares_and_grants_for_deleted_devices() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let owner_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES (?, 'public-deleted-device-user', 'unused', 'viewer', 'user'),
                (?, 'public-deleted-device-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, owner_user_id, deleted_at)
         VALUES ('public-deleted-device', ?, CURRENT_TIMESTAMP)",
    )
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares
            (id, resource_type, resource_id, target_user_id, permission,
             inherit_children, state, created_by_user_id)
         VALUES ('public-deleted-device-share', 'device', ?, ?, 'manager', 0, 'active', ?)",
    )
    .bind("public-deleted-device")
    .bind(user_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_grants
            (id, resource_type, resource_id, grantee_type, grantee_id, permission,
             created_by_user_id)
         VALUES (?, 'device', 'public-deleted-device', 'user', ?, 'controller', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(user_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let principal = PublicPrincipal {
        user_id: Some(user_id),
        app_id: "public-deleted-device-app".to_owned(),
        account_class: AccountClass::User,
    };
    assert_eq!(
        PublicApiRepository::public_device_permission(&store, &principal, "public-deleted-device",)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_public_device_repository_creates_updates_and_soft_deletes_owned_devices() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES (?, 'public-device-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        user_id: Some(user_id),
        app_id: "public-device-app".to_owned(),
        account_class: AccountClass::User,
    };

    let created = PublicApiRepository::create_public_device(
        &store,
        &principal,
        NewPublicDevice {
            device_id: "public-created-device".to_owned(),
            display_name: Some("Created device".to_owned()),
            metadata: serde_json::json!({"room":"lab"}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(created.device_id, "public-created-device");
    assert_eq!(created.display_name.as_deref(), Some("Created device"));
    assert_eq!(created.metadata, serde_json::json!({"room":"lab"}));

    let updated = PublicApiRepository::update_public_device(
        &store,
        &principal,
        &created.device_id,
        NewPublicDevice {
            device_id: created.device_id.clone(),
            display_name: Some("Renamed device".to_owned()),
            metadata: serde_json::json!({"room":"office"}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(updated.display_name.as_deref(), Some("Renamed device"));
    assert_eq!(updated.metadata, serde_json::json!({"room":"office"}));

    assert!(
        PublicApiRepository::delete_public_device(&store, &principal, &created.device_id)
            .await
            .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT deleted_at FROM devices WHERE device_id = ?",
        )
        .bind(&created.device_id)
        .fetch_one(pool)
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        PublicApiRepository::public_device_permission(&store, &principal, &created.device_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !PublicApiRepository::delete_public_device(&store, &principal, &created.device_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn sqlite_public_device_repository_hides_unknown_inaccessible_and_deleted_mutations() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let viewer_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES (?, 'public-device-owner-2', 'unused', 'viewer', 'user'),
                (?, 'public-device-viewer-2', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(viewer_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, owner_user_id)
         VALUES ('public-inaccessible-device', ?),
                ('public-deleted-device-2', ?)",
    )
    .bind(owner_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET deleted_at = CURRENT_TIMESTAMP WHERE device_id = ?")
        .bind("public-deleted-device-2")
        .execute(pool)
        .await
        .unwrap();
    let principal = PublicPrincipal {
        user_id: Some(viewer_id),
        app_id: "public-device-app-2".to_owned(),
        account_class: AccountClass::User,
    };
    let update = NewPublicDevice {
        device_id: "ignored".to_owned(),
        display_name: Some("should not update".to_owned()),
        metadata: serde_json::json!({}),
        asset_id: None,
        device_profile_id: None,
    };

    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &principal,
            "public-inaccessible-device",
            update.clone(),
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        !PublicApiRepository::delete_public_device(
            &store,
            &principal,
            "public-inaccessible-device",
        )
        .await
        .unwrap()
    );
    assert!(
        PublicApiRepository::update_public_device(&store, &principal, "unknown-device", update)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !PublicApiRepository::delete_public_device(&store, &principal, "public-deleted-device-2",)
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_repository_creates_and_reads_an_asset() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, 'timescale-public-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let asset = PublicApiRepository::create_public_asset(
        &store,
        &PublicPrincipal {
            user_id: Some(user_id),
            app_id: "timescale-public-app".to_owned(),
            account_class: AccountClass::User,
        },
        NewPublicAsset {
            name: "timescale contract asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        PublicApiRepository::get_public_asset(
            &store,
            &PublicPrincipal {
                user_id: Some(user_id),
                app_id: "timescale-public-app".to_owned(),
                account_class: AccountClass::User,
            },
            asset.id,
        )
        .await
        .unwrap()
        .unwrap()
        .id,
        asset.id
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_device_repository_matches_sqlite_mutation_contract() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, 'timescale-public-device-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let principal = PublicPrincipal {
        user_id: Some(user_id),
        app_id: "timescale-public-device-app".to_owned(),
        account_class: AccountClass::User,
    };
    let created = PublicApiRepository::create_public_device(
        &store,
        &principal,
        NewPublicDevice {
            device_id: "timescale-public-device".to_owned(),
            display_name: Some("Timescale device".to_owned()),
            metadata: serde_json::json!({"backend":"timescale"}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    let updated = PublicApiRepository::update_public_device(
        &store,
        &principal,
        &created.device_id,
        NewPublicDevice {
            device_id: created.device_id.clone(),
            display_name: Some("Updated Timescale device".to_owned()),
            metadata: created.metadata.clone(),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        updated.display_name.as_deref(),
        Some("Updated Timescale device")
    );
    assert!(
        PublicApiRepository::delete_public_device(&store, &principal, &created.device_id)
            .await
            .unwrap()
    );
    assert!(
        PublicApiRepository::public_device_permission(&store, &principal, &created.device_id)
            .await
            .unwrap()
            .is_none()
    );
}
