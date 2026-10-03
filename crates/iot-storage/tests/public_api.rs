use chrono::{Duration as ChronoDuration, Utc};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, AuditPrincipal, ManagementAssetRepository, NewPublicAsset, NewPublicDevice,
    PlatformStore, PublicApiRepository, PublicAssetError, PublicDeviceError, PublicPrincipal,
    ResourceAccess, ResourceAccessSource, ResourcePermission,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool};
use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

mod common;

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
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'public-api-fixture', 'active')",
    )
    .bind(test_tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store)
}

fn test_tenant_id() -> Uuid {
    Uuid::from_u128(10_001)
}

async fn timescale_store() -> (PgConnection, PlatformStore) {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
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
    (connection, store)
}

async fn seed_timescale_test_tenant(store: &PlatformStore) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES ($1, 'timescale-public-api', 'active')
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
}

#[tokio::test]
async fn sqlite_public_assets_are_invisible_across_tenants() {
    let (_directory, store) = sqlite_store().await;
    let tenant_a_id = Uuid::now_v7();
    let tenant_b_id = Uuid::now_v7();
    let tenant_a_user_id = Uuid::now_v7();
    let tenant_b_user_id = Uuid::now_v7();
    let pool = store.sqlite_pool().unwrap();

    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'public-tenant-a', 'active'),
                (?, 'public-tenant-b', 'active')",
    )
    .bind(tenant_a_id.to_string())
    .bind(tenant_b_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-tenant-a-user', 'unused', 'viewer', 'user'),
                (?, ?, 'public-tenant-b-user', 'unused', 'viewer', 'user')",
    )
    .bind(tenant_a_user_id.to_string())
    .bind(tenant_a_id.to_string())
    .bind(tenant_b_user_id.to_string())
    .bind(tenant_b_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let tenant_a_principal = PublicPrincipal {
        tenant_id: tenant_a_id,
        user_id: Some(tenant_a_user_id),
        app_id: "public-tenant-a-app".to_owned(),
        account_class: AccountClass::User,
    };
    let tenant_b_principal = PublicPrincipal {
        tenant_id: tenant_b_id,
        user_id: Some(tenant_b_user_id),
        app_id: "public-tenant-b-app".to_owned(),
        account_class: AccountClass::User,
    };
    let tenant_b_asset = PublicApiRepository::create_public_asset(
        &store,
        &tenant_b_principal,
        NewPublicAsset {
            name: "tenant-b-asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        PublicApiRepository::get_public_asset(&store, &tenant_a_principal, tenant_b_asset.id)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_tenant_account_principal_has_full_tenant_public_resource_authority() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let other_tenant_id = Uuid::now_v7();
    let other_owner_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'tenant-account-other', 'active');
         INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'tenant-account-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'tenant-account-other-owner', 'unused', 'viewer', 'user')",
    )
    .bind(other_tenant_id.to_string())
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(other_owner_id.to_string())
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let owner = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "tenant-account-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    let tenant_account = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "tenant-personal-access-token".to_owned(),
        account_class: AccountClass::Admin,
    };
    let app_only = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "ordinary-app-only-token".to_owned(),
        account_class: AccountClass::User,
    };

    let owned_asset = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "tenant-account-owned-asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let owned_device = PublicApiRepository::create_public_device(
        &store,
        &owner,
        NewPublicDevice {
            device_id: "tenant-account-owned-device".to_owned(),
            display_name: None,
            metadata: json!({}),
            asset_id: Some(owned_asset.id),
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    let other_asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id, metadata)
         VALUES (?, ?, 'tenant-account-other-asset', ?, '{}')",
    )
    .bind(other_asset_id.to_string())
    .bind(other_tenant_id.to_string())
    .bind(other_owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert_eq!(
        PublicApiRepository::list_public_assets(&store, &tenant_account, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|asset| asset.id)
            .collect::<Vec<_>>(),
        vec![owned_asset.id]
    );
    assert_eq!(
        PublicApiRepository::list_public_devices(&store, &tenant_account, None, 10)
            .await
            .unwrap()
            .iter()
            .map(|device| device.device_id.clone())
            .collect::<Vec<_>>(),
        vec![owned_device.device_id.clone()]
    );
    assert_eq!(
        PublicApiRepository::public_asset_permission(&store, &tenant_account, owned_asset.id)
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        PublicApiRepository::public_device_permission(
            &store,
            &tenant_account,
            &owned_device.device_id,
        )
        .await
        .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        PublicApiRepository::get_public_asset(&store, &tenant_account, owned_asset.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        owned_asset.id
    );
    assert_eq!(
        PublicApiRepository::get_public_device(&store, &tenant_account, &owned_device.device_id)
            .await
            .unwrap()
            .unwrap()
            .device_id,
        owned_device.device_id
    );
    assert!(
        PublicApiRepository::get_public_asset(&store, &tenant_account, other_asset_id)
            .await
            .unwrap()
            .is_none()
    );

    let created_asset = PublicApiRepository::create_public_asset(
        &store,
        &tenant_account,
        NewPublicAsset {
            name: "tenant-account-created-asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let created_device = PublicApiRepository::create_public_device(
        &store,
        &tenant_account,
        NewPublicDevice {
            device_id: "tenant-account-created-device".to_owned(),
            display_name: None,
            metadata: json!({}),
            asset_id: Some(created_asset.id),
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &tenant_account,
            owned_asset.id,
            NewPublicAsset {
                name: "tenant-account-updated-asset".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &tenant_account,
            &owned_device.device_id,
            NewPublicDevice {
                device_id: owned_device.device_id.clone(),
                display_name: Some("tenant account updated".to_owned()),
                metadata: json!({}),
                asset_id: Some(owned_asset.id),
                device_profile_id: None,
            },
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &tenant_account,
            &created_device.device_id,
            NewPublicDevice {
                device_id: created_device.device_id.clone(),
                display_name: None,
                metadata: json!({}),
                asset_id: None,
                device_profile_id: None,
            },
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        PublicApiRepository::delete_public_device(
            &store,
            &tenant_account,
            &created_device.device_id
        )
        .await
        .unwrap()
    );
    assert!(
        PublicApiRepository::delete_public_asset(&store, &tenant_account, created_asset.id)
            .await
            .unwrap()
    );

    assert!(
        PublicApiRepository::list_public_assets(&store, &app_only, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        PublicApiRepository::list_public_devices(&store, &app_only, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        PublicApiRepository::create_public_asset(
            &store,
            &app_only,
            NewPublicAsset {
                name: "ordinary-app-asset".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await,
        Err(PublicAssetError::Unauthorized)
    ));
    assert!(matches!(
        PublicApiRepository::create_public_device(
            &store,
            &app_only,
            NewPublicDevice {
                device_id: "ordinary-app-device".to_owned(),
                display_name: None,
                metadata: json!({}),
                asset_id: None,
                device_profile_id: None,
            },
        )
        .await,
        Err(PublicDeviceError::Unauthorized)
    ));
}

#[tokio::test]
async fn sqlite_public_legacy_admin_group_inherited_permission_controls_resource_flows() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let member_id = Uuid::now_v7();
    let group_id = Uuid::now_v7();
    let permission_id = Uuid::now_v7();
    let alert_rule_id = Uuid::now_v7();
    let alert_id = Uuid::now_v7();
    let device_id = format!("public-inherited-device-{}", Uuid::now_v7());
    let event_at = Utc::now();

    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'public-member', 'unused', 'admin', 'admin')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(member_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'public-shared-group')",
    )
    .bind(group_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO user_group_members (tenant_id, group_id, user_id) VALUES (?, ?, ?)")
        .bind(test_tenant_id().to_string())
        .bind(group_id.to_string())
        .bind(member_id.to_string())
        .execute(pool)
        .await
        .unwrap();

    let owner = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "public-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    let member = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(member_id),
        app_id: "public-member-app".to_owned(),
        account_class: AccountClass::Admin,
    };
    let root_asset = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "public inherited root".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let child_asset = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "public inherited child".to_owned(),
            asset_profile_id: None,
            parent_asset_id: Some(root_asset.id),
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let device = PublicApiRepository::create_public_device(
        &store,
        &owner,
        NewPublicDevice {
            device_id: device_id.clone(),
            display_name: Some("Public inherited device".to_owned()),
            metadata: json!({}),
            asset_id: Some(child_asset.id),
            device_profile_id: None,
        },
    )
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_group_id, asset_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', 1, ?)",
    )
    .bind(permission_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(group_id.to_string())
    .bind(root_asset.id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let assets = PublicApiRepository::list_public_assets(&store, &member, None, 100)
        .await
        .unwrap();
    assert!(assets.iter().any(|asset| {
        asset.id == root_asset.id
            && asset.access
                == Some(ResourceAccess {
                    permission: ResourcePermission::Viewer,
                    source: ResourceAccessSource::Group,
                })
    }));
    assert!(assets.iter().any(|asset| {
        asset.id == child_asset.id
            && asset.access
                == Some(ResourceAccess {
                    permission: ResourcePermission::Viewer,
                    source: ResourceAccessSource::InheritedGroup,
                })
    }));
    assert!(
        PublicApiRepository::get_public_asset(&store, &member, child_asset.id)
            .await
            .unwrap()
            .is_some()
    );
    let devices = PublicApiRepository::list_public_devices(&store, &member, None, 100)
        .await
        .unwrap();
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].device_id, device.device_id);
    assert_eq!(
        devices[0].access,
        Some(ResourceAccess {
            permission: ResourcePermission::Viewer,
            source: ResourceAccessSource::InheritedGroup,
        })
    );
    assert_eq!(
        PublicApiRepository::get_public_device(&store, &member, &device_id)
            .await
            .unwrap(),
        Some(device.clone())
    );

    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
         ) VALUES (?, ?, ?, ?, ?, 1, ?, 'public-inherited')",
    )
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(test_tenant_id().to_string())
    .bind(&device_id)
    .bind(Uuid::now_v7().to_string())
    .bind(json!({ "temperature_c": 26.0 }).to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold
         ) VALUES (?, ?, 'Public inherited rule', ?, 'temperature_c', 'event_threshold', 'gt', 25)",
    )
    .bind(alert_rule_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(&device_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, updated_at
         ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?)",
    )
    .bind(alert_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(alert_rule_id.to_string())
    .bind(&device_id)
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let telemetry = PublicApiRepository::list_public_telemetry(
        &store,
        &member,
        Some(&device_id),
        None,
        event_at - ChronoDuration::minutes(1),
        event_at + ChronoDuration::minutes(1),
        None,
        100,
    )
    .await
    .unwrap();
    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry[0].device_id, device_id);
    let alerts = PublicApiRepository::list_public_alerts(&store, &member, None, 100)
        .await
        .unwrap();
    assert_eq!(
        alerts.iter().map(|alert| alert.id).collect::<Vec<_>>(),
        [alert_id]
    );
    assert_eq!(
        PublicApiRepository::acknowledge_public_alert(&store, &member, alert_id, "member")
            .await
            .unwrap(),
        None
    );

    sqlx::query("UPDATE resource_permissions SET permission = 'manager' WHERE id = ?")
        .bind(permission_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(
        PublicApiRepository::acknowledge_public_alert(&store, &member, alert_id, "member")
            .await
            .unwrap()
            .map(|alert| alert.id),
        Some(alert_id)
    );

    sqlx::query("UPDATE devices SET asset_id = NULL WHERE device_id = ? AND tenant_id = ?")
        .bind(&device_id)
        .bind(test_tenant_id().to_string())
        .execute(pool)
        .await
        .unwrap();
    assert!(
        PublicApiRepository::list_public_devices(&store, &member, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        PublicApiRepository::get_public_device(&store, &member, &device_id)
            .await
            .unwrap(),
        None
    );

    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', 0, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(test_tenant_id().to_string())
    .bind(member_id.to_string())
    .bind(&device_id)
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let devices = PublicApiRepository::list_public_devices(&store, &member, None, 100)
        .await
        .unwrap();
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].asset_id, None);
    assert_eq!(
        devices[0].access,
        Some(ResourceAccess {
            permission: ResourcePermission::Viewer,
            source: ResourceAccessSource::DirectUser,
        })
    );
}

#[tokio::test]
async fn sqlite_public_legacy_admin_is_denied_unshared_resources_and_mutations() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let admin_id = Uuid::now_v7();
    let unshared_device_id = format!("public-unshared-admin-device-{}", Uuid::now_v7());
    let owned_device_id = format!("public-owned-admin-device-{}", Uuid::now_v7());
    let alert_rule_id = Uuid::now_v7();
    let alert_id = Uuid::now_v7();
    let event_at = Utc::now();

    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-unshared-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'public-legacy-admin', 'unused', 'admin', 'admin')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(admin_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let owner = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "public-unshared-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    let legacy_admin = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(admin_id),
        app_id: "public-legacy-admin-app".to_owned(),
        account_class: AccountClass::Admin,
    };
    let unshared_asset = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "unshared admin asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let unshared_device = PublicApiRepository::create_public_device(
        &store,
        &owner,
        NewPublicDevice {
            device_id: unshared_device_id.clone(),
            display_name: Some("Unshared admin device".to_owned()),
            metadata: json!({}),
            asset_id: Some(unshared_asset.id),
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    let owned_asset = PublicApiRepository::create_public_asset(
        &store,
        &legacy_admin,
        NewPublicAsset {
            name: "owned admin asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let owned_device = PublicApiRepository::create_public_device(
        &store,
        &legacy_admin,
        NewPublicDevice {
            device_id: owned_device_id.clone(),
            display_name: Some("Owned admin device".to_owned()),
            metadata: json!({}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
         ) VALUES (?, ?, ?, ?, ?, 1, ?, 'public-legacy-admin')",
    )
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(test_tenant_id().to_string())
    .bind(&unshared_device_id)
    .bind(Uuid::now_v7().to_string())
    .bind(json!({ "temperature_c": 26.0 }).to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold
         ) VALUES (?, ?, 'Public legacy admin rule', ?, 'temperature_c', 'event_threshold', 'gt', 25)",
    )
    .bind(alert_rule_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(&unshared_device_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, updated_at
         ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?)",
    )
    .bind(alert_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(alert_rule_id.to_string())
    .bind(&unshared_device_id)
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let assets = PublicApiRepository::list_public_assets(&store, &legacy_admin, None, 100)
        .await
        .unwrap();
    assert!(!assets.iter().any(|asset| asset.id == unshared_asset.id));
    assert!(assets.iter().any(|asset| {
        asset.id == owned_asset.id
            && asset.access
                == Some(ResourceAccess {
                    permission: ResourcePermission::Owner,
                    source: ResourceAccessSource::Owner,
                })
    }));
    assert!(
        PublicApiRepository::get_public_asset(&store, &legacy_admin, unshared_asset.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        PublicApiRepository::get_public_asset(&store, &legacy_admin, owned_asset.id)
            .await
            .unwrap()
            .is_some()
    );

    let devices = PublicApiRepository::list_public_devices(&store, &legacy_admin, None, 100)
        .await
        .unwrap();
    assert!(
        !devices
            .iter()
            .any(|device| device.device_id == unshared_device.device_id)
    );
    assert!(devices.iter().any(|device| {
        device.device_id == owned_device.device_id
            && device.access
                == Some(ResourceAccess {
                    permission: ResourcePermission::Owner,
                    source: ResourceAccessSource::Owner,
                })
    }));
    assert!(
        PublicApiRepository::get_public_device(&store, &legacy_admin, &unshared_device_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        PublicApiRepository::get_public_device(&store, &legacy_admin, &owned_device_id)
            .await
            .unwrap()
            .is_some()
    );

    assert!(
        PublicApiRepository::list_public_telemetry(
            &store,
            &legacy_admin,
            None,
            None,
            event_at - ChronoDuration::minutes(1),
            event_at + ChronoDuration::minutes(1),
            None,
            100,
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert!(
        PublicApiRepository::list_public_alerts(&store, &legacy_admin, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        PublicApiRepository::get_public_alert(&store, &legacy_admin, alert_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        PublicApiRepository::acknowledge_public_alert(
            &store,
            &legacy_admin,
            alert_id,
            "legacy-admin",
        )
        .await
        .unwrap()
        .is_none()
    );

    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &legacy_admin,
            &unshared_device_id,
            NewPublicDevice {
                device_id: unshared_device_id.clone(),
                display_name: Some("attempted unshared update".to_owned()),
                metadata: json!({}),
                asset_id: Some(unshared_asset.id),
                device_profile_id: None,
            },
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &legacy_admin,
            unshared_asset.id,
            NewPublicAsset {
                name: "attempted unshared update".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &legacy_admin,
            &owned_device_id,
            NewPublicDevice {
                device_id: owned_device_id.clone(),
                display_name: Some("owned admin device updated".to_owned()),
                metadata: json!({}),
                asset_id: None,
                device_profile_id: None,
            },
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &legacy_admin,
            owned_asset.id,
            NewPublicAsset {
                name: "owned admin asset updated".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn sqlite_public_asset_mutations_require_destination_access_and_valid_containment() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let source_owner_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-parent-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'public-parent-source-owner', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(source_owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let owner = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "public-parent-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    let source_owner = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(source_owner_id),
        app_id: "public-parent-source-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    let destination = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "destination".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let source = PublicApiRepository::create_public_asset(
        &store,
        &source_owner,
        NewPublicAsset {
            name: "source".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();

    let denied = PublicApiRepository::update_public_asset(
        &store,
        &source_owner,
        source.id,
        NewPublicAsset {
            name: source.name.clone(),
            asset_profile_id: None,
            parent_asset_id: Some(destination.id),
            metadata: source.metadata.clone(),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        denied,
        iot_storage::PublicAssetError::ParentUnavailable(id) if id == destination.id
    ));

    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, asset_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'manager', 0, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(test_tenant_id().to_string())
    .bind(owner_id.to_string())
    .bind(source.id.to_string())
    .bind(source_owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let self_parent = PublicApiRepository::update_public_asset(
        &store,
        &owner,
        source.id,
        NewPublicAsset {
            name: source.name.clone(),
            asset_profile_id: None,
            parent_asset_id: Some(source.id),
            metadata: source.metadata.clone(),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        self_parent,
        iot_storage::PublicAssetError::ParentUnavailable(id) if id == source.id
    ));

    let mut parent_asset_id = None;
    let mut depth_63_parent = None;
    for depth in 0..=64 {
        let asset_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO assets (id, tenant_id, name, parent_asset_id, owner_user_id)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(asset_id.to_string())
        .bind(test_tenant_id().to_string())
        .bind(format!("public-depth-{depth}"))
        .bind(parent_asset_id.map(|id: Uuid| id.to_string()))
        .bind(owner_id.to_string())
        .execute(pool)
        .await
        .unwrap();
        if depth == 63 {
            depth_63_parent = Some(asset_id);
        }
        parent_asset_id = Some(asset_id);
    }
    let too_deep_parent = parent_asset_id.unwrap();
    let too_deep = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "too deep".to_owned(),
            asset_profile_id: None,
            parent_asset_id: Some(too_deep_parent),
            metadata: json!({}),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        too_deep,
        iot_storage::PublicAssetError::ParentUnavailable(id) if id == too_deep_parent
    ));

    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, parent_asset_id, owner_user_id)
         VALUES (?, ?, 'source-child', ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(test_tenant_id().to_string())
    .bind(source.id.to_string())
    .bind(source_owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let depth_63_parent = depth_63_parent.unwrap();
    let subtree_too_deep = PublicApiRepository::update_public_asset(
        &store,
        &owner,
        source.id,
        NewPublicAsset {
            name: source.name.clone(),
            asset_profile_id: None,
            parent_asset_id: Some(depth_63_parent),
            metadata: source.metadata.clone(),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        subtree_too_deep,
        iot_storage::PublicAssetError::ParentUnavailable(id) if id == depth_63_parent
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_legacy_admin_is_denied_unshared_assets() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let admin_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'public-legacy-admin', 'active')",
    )
    .bind(test_tenant_id())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'public-timescale-owner', 'unused', 'viewer', 'user'),
                ($3, $2, 'public-timescale-admin', 'unused', 'admin', 'admin')",
    )
    .bind(owner_id)
    .bind(test_tenant_id())
    .bind(admin_id)
    .execute(pool)
    .await
    .unwrap();

    let owner = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "public-timescale-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    let legacy_admin = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(admin_id),
        app_id: "public-timescale-admin-app".to_owned(),
        account_class: AccountClass::Admin,
    };
    let unshared_asset = PublicApiRepository::create_public_asset(
        &store,
        &owner,
        NewPublicAsset {
            name: "timescale unshared admin asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();
    let owned_asset = PublicApiRepository::create_public_asset(
        &store,
        &legacy_admin,
        NewPublicAsset {
            name: "timescale owned admin asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap();

    let assets = PublicApiRepository::list_public_assets(&store, &legacy_admin, None, 100)
        .await
        .unwrap();
    assert!(!assets.iter().any(|asset| asset.id == unshared_asset.id));
    assert!(assets.iter().any(|asset| asset.id == owned_asset.id));
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &legacy_admin,
            unshared_asset.id,
            NewPublicAsset {
                name: "attempted timescale unshared update".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &legacy_admin,
            owned_asset.id,
            NewPublicAsset {
                name: "timescale owned admin asset updated".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn sqlite_public_application_principal_cannot_create_resources() {
    let (_directory, store) = sqlite_store().await;
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "public-application-only".to_owned(),
        account_class: AccountClass::User,
    };

    assert!(matches!(
        PublicApiRepository::create_public_asset(
            &store,
            &principal,
            NewPublicAsset {
                name: "application asset".to_owned(),
                asset_profile_id: None,
                parent_asset_id: None,
                metadata: json!({}),
            },
        )
        .await,
        Err(iot_storage::PublicAssetError::Unauthorized)
    ));
    assert!(matches!(
        PublicApiRepository::create_public_device(
            &store,
            &principal,
            NewPublicDevice {
                device_id: "application-device".to_owned(),
                display_name: None,
                metadata: json!({}),
                asset_id: None,
                device_profile_id: None,
            },
        )
        .await,
        Err(iot_storage::PublicDeviceError::Unauthorized)
    ));
}

#[tokio::test]
async fn sqlite_public_telemetry_requires_a_matching_telemetry_tenant() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let other_tenant_id = Uuid::now_v7();
    let device_id = format!("public-telemetry-tenant-device-{}", Uuid::now_v7());
    let event_at = Utc::now();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'public-telemetry-other', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-telemetry-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id, owner_user_id) VALUES (?, ?, ?)")
        .bind(&device_id)
        .bind(test_tenant_id().to_string())
        .bind(user_id.to_string())
        .execute(pool)
        .await
        .unwrap();

    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements, topic
         ) VALUES (?, ?, ?, ?, ?, 1, ?, 'public-telemetry-tenant')",
    )
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(other_tenant_id.to_string())
    .bind(&device_id)
    .bind(Uuid::now_v7().to_string())
    .bind(json!({ "temperature_c": 26.0 }).to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(user_id),
        app_id: "public-telemetry-owner-app".to_owned(),
        account_class: AccountClass::User,
    };
    assert!(
        PublicApiRepository::list_public_telemetry(
            &store,
            &principal,
            Some(&device_id),
            None,
            event_at - ChronoDuration::minutes(1),
            event_at + ChronoDuration::minutes(1),
            None,
            100,
        )
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test]
async fn sqlite_users_tenant_id_is_immutable() {
    let (_directory, store) = sqlite_store().await;
    let original_tenant_id = Uuid::now_v7();
    let replacement_tenant_id = Uuid::now_v7();
    let user_id = Uuid::now_v7();
    let pool = store.sqlite_pool().unwrap();

    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'immutable-user-a', 'active'),
                (?, 'immutable-user-b', 'active')",
    )
    .bind(original_tenant_id.to_string())
    .bind(replacement_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'immutable-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(original_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert!(
        sqlx::query("UPDATE users SET tenant_id = ? WHERE id = ?")
            .bind(replacement_tenant_id.to_string())
            .bind(user_id.to_string())
            .execute(pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_devices_gateway_must_belong_to_the_same_tenant() {
    let (_directory, store) = sqlite_store().await;
    let tenant_a_id = Uuid::now_v7();
    let tenant_b_id = Uuid::now_v7();
    let pool = store.sqlite_pool().unwrap();

    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'gateway-tenant-a', 'active'),
                (?, 'gateway-tenant-b', 'active')",
    )
    .bind(tenant_a_id.to_string())
    .bind(tenant_b_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway)
         VALUES ('tenant-b-gateway', ?, 1)",
    )
    .bind(tenant_b_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    assert!(
        sqlx::query(
            "INSERT INTO devices (device_id, tenant_id, gateway_device_id)
             VALUES ('tenant-a-device', ?, 'tenant-b-gateway')",
        )
        .bind(tenant_a_id.to_string())
        .execute(pool)
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_devices_gateway_must_belong_to_the_same_tenant() {
    let (_lock, store) = timescale_store().await;
    let tenant_a_id = Uuid::now_v7();
    let tenant_b_id = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();

    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'gateway-tenant-a', 'active'),
                ($2, 'gateway-tenant-b', 'active')",
    )
    .bind(tenant_a_id)
    .bind(tenant_b_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, is_gateway)
         VALUES ('tenant-b-gateway', $1, TRUE)",
    )
    .bind(tenant_b_id)
    .execute(pool)
    .await
    .unwrap();

    assert!(
        sqlx::query(
            "INSERT INTO devices (device_id, tenant_id, gateway_device_id)
             VALUES ('tenant-a-device', $1, 'tenant-b-gateway')",
        )
        .bind(tenant_a_id)
        .execute(pool)
        .await
        .is_err()
    );
}

async fn wait_for_timescale_relation_lock_count(
    pool: &PgPool,
    table: &str,
    mode: &str,
    at_least: i64,
) {
    timeout(Duration::from_secs(2), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)
                 FROM pg_locks
                 WHERE locktype = 'relation'
                   AND relation = $1::regclass
                   AND mode = $2
                   AND granted",
            )
            .bind(format!("iot_nano.{table}"))
            .bind(mode)
            .fetch_one(pool)
            .await
            .unwrap();
            if count >= at_least {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{table} did not reach {mode} count {at_least}"));
}

async fn timescale_relation_lock_is_held(pool: &PgPool, table: &str, mode: &str) -> bool {
    timeout(Duration::from_millis(250), async {
        loop {
            let held: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1
                    FROM pg_locks
                    WHERE locktype = 'relation'
                      AND relation = $1::regclass
                      AND mode = $2
                      AND granted
                )",
            )
            .bind(format!("iot_nano.{table}"))
            .bind(mode)
            .fetch_one(pool)
            .await
            .unwrap();
            if held {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn sqlite_public_repository_filters_assets() {
    let (_directory, store) = sqlite_store().await;
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-repository-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
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
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0].id, asset.id);
    assert_eq!(
        assets[0].access,
        Some(ResourceAccess {
            permission: ResourcePermission::Owner,
            source: ResourceAccessSource::Owner,
        })
    );
}

#[tokio::test]
async fn sqlite_public_device_permission_denies_direct_permissions_for_deleted_devices() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let owner_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-deleted-device-user', 'unused', 'viewer', 'user'),
                (?, ?, 'public-deleted-device-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id, deleted_at)
         VALUES ('public-deleted-device', ?, ?, CURRENT_TIMESTAMP)",
    )
    .bind(test_tenant_id().to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, device_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, 'public-deleted-device', 'manager', 0, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(test_tenant_id().to_string())
    .bind(user_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
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
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-device-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
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
async fn sqlite_public_gateway_delete_refuses_active_children() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let gateway_id = format!("public-gateway-{user_id}");
    let child_id = format!("public-gateway-child-{user_id}");
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(format!("public-gateway-owner-{user_id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id, is_gateway)
         VALUES (?, ?, ?, 1), (?, ?, ?, 0)",
    )
    .bind(&gateway_id)
    .bind(test_tenant_id().to_string())
    .bind(user_id.to_string())
    .bind(&child_id)
    .bind(test_tenant_id().to_string())
    .bind(user_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices
         SET gateway_device_id = ?
         WHERE device_id = ? AND tenant_id = ?",
    )
    .bind(&gateway_id)
    .bind(&child_id)
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(user_id),
        app_id: "public-gateway-delete-app".to_owned(),
        account_class: AccountClass::User,
    };

    assert!(
        !PublicApiRepository::delete_public_device(&store, &principal, &gateway_id)
            .await
            .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT deleted_at IS NULL
             FROM devices
             WHERE device_id = ? AND tenant_id = ?",
        )
        .bind(&gateway_id)
        .bind(test_tenant_id().to_string())
        .fetch_one(pool)
        .await
        .unwrap()
    );

    assert!(
        PublicApiRepository::delete_public_device(&store, &principal, &child_id)
            .await
            .unwrap()
    );
    assert!(
        PublicApiRepository::delete_public_device(&store, &principal, &gateway_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn sqlite_public_device_asset_assignment_requires_asset_manager_permission() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let attacker_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-asset-owner', 'unused', 'viewer', 'user'),
                (?, ?, 'public-asset-attacker', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(attacker_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id) VALUES (?, ?, 'protected', ?)",
    )
    .bind(asset_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, asset_id, permission, inherit_children,
            created_by_user_id
         ) VALUES (?, ?, ?, ?, 'viewer', 1, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(test_tenant_id().to_string())
    .bind(attacker_id.to_string())
    .bind(asset_id.to_string())
    .bind(owner_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let attacker = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(attacker_id),
        app_id: "public-asset-attacker-app".to_owned(),
        account_class: AccountClass::User,
    };

    let create_error = PublicApiRepository::create_public_device(
        &store,
        &attacker,
        NewPublicDevice {
            device_id: "public-asset-unauthorized-create".to_owned(),
            display_name: None,
            metadata: json!({}),
            asset_id: Some(asset_id),
            device_profile_id: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_error,
        PublicDeviceError::AssetUnavailable(id) if id == asset_id
    ));

    let device = PublicApiRepository::create_public_device(
        &store,
        &attacker,
        NewPublicDevice {
            device_id: "public-asset-unauthorized-update".to_owned(),
            display_name: None,
            metadata: json!({}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &attacker,
            &device.device_id,
            NewPublicDevice {
                device_id: device.device_id.clone(),
                display_name: None,
                metadata: json!({}),
                asset_id: Some(asset_id),
                device_profile_id: None,
            },
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn sqlite_public_device_create_rejects_an_unavailable_profile_atomically() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let unavailable_profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-profile-create-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(user_id),
        app_id: "public-profile-create-app".to_owned(),
        account_class: AccountClass::User,
    };

    let result = PublicApiRepository::create_public_device(
        &store,
        &principal,
        NewPublicDevice {
            device_id: "public-unavailable-profile-create".to_owned(),
            display_name: None,
            metadata: json!({}),
            asset_id: None,
            device_profile_id: Some(unavailable_profile_id),
        },
    )
    .await;

    assert_eq!(
        result.unwrap_err().to_string(),
        format!("public device profile is unavailable: {unavailable_profile_id}")
    );
    assert!(
        PublicApiRepository::get_public_device(
            &store,
            &principal,
            "public-unavailable-profile-create",
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn sqlite_public_device_update_rejects_an_unavailable_profile_atomically() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let unavailable_profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-profile-update-owner', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(user_id),
        app_id: "public-profile-update-app".to_owned(),
        account_class: AccountClass::User,
    };
    let device = PublicApiRepository::create_public_device(
        &store,
        &principal,
        NewPublicDevice {
            device_id: "public-unavailable-profile-update".to_owned(),
            display_name: Some("original".to_owned()),
            metadata: json!({"version": 1}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();

    let result = PublicApiRepository::update_public_device(
        &store,
        &principal,
        &device.device_id,
        NewPublicDevice {
            device_id: device.device_id.clone(),
            display_name: Some("updated".to_owned()),
            metadata: json!({"version": 2}),
            asset_id: None,
            device_profile_id: Some(unavailable_profile_id),
        },
    )
    .await;

    assert_eq!(
        result.unwrap_err().to_string(),
        format!("public device profile is unavailable: {unavailable_profile_id}")
    );
    assert_eq!(
        PublicApiRepository::get_public_device(&store, &principal, &device.device_id)
            .await
            .unwrap(),
        Some(device)
    );
}

#[tokio::test]
async fn sqlite_public_asset_create_rejects_a_cross_tenant_profile_atomically() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let other_tenant_id = Uuid::now_v7();
    let cross_tenant_profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'public-asset-profile-other', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-asset-profile-owner', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO asset_profiles (id, tenant_id, name) VALUES (?, ?, ?)")
        .bind(cross_tenant_profile_id.to_string())
        .bind(other_tenant_id.to_string())
        .bind("other tenant profile")
        .execute(pool)
        .await
        .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "public-asset-profile-app".to_owned(),
        account_class: AccountClass::User,
    };

    let error = PublicApiRepository::create_public_asset(
        &store,
        &principal,
        NewPublicAsset {
            name: "cross-tenant-profile-create".to_owned(),
            asset_profile_id: Some(cross_tenant_profile_id),
            parent_asset_id: None,
            metadata: json!({}),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("public asset profile is unavailable: {cross_tenant_profile_id}")
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM assets WHERE tenant_id = ? AND name = ?")
            .bind(test_tenant_id().to_string())
            .bind("cross-tenant-profile-create")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn sqlite_public_asset_update_rejects_a_cross_tenant_profile_atomically() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let other_tenant_id = Uuid::now_v7();
    let cross_tenant_profile_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (
            id, slug, status
         ) VALUES (?, 'public-asset-profile-update-other', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-asset-profile-update-owner', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO asset_profiles (id, tenant_id, name) VALUES (?, ?, ?)")
        .bind(cross_tenant_profile_id.to_string())
        .bind(other_tenant_id.to_string())
        .bind("other tenant update profile")
        .execute(pool)
        .await
        .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(owner_id),
        app_id: "public-asset-profile-update-app".to_owned(),
        account_class: AccountClass::User,
    };
    let asset = PublicApiRepository::create_public_asset(
        &store,
        &principal,
        NewPublicAsset {
            name: "cross-tenant-profile-update".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({"version": 1}),
        },
    )
    .await
    .unwrap();

    let error = PublicApiRepository::update_public_asset(
        &store,
        &principal,
        asset.id,
        NewPublicAsset {
            name: "updated cross-tenant-profile-update".to_owned(),
            asset_profile_id: Some(cross_tenant_profile_id),
            parent_asset_id: None,
            metadata: json!({"version": 2}),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("public asset profile is unavailable: {cross_tenant_profile_id}")
    );
    let current = PublicApiRepository::get_public_asset(&store, &principal, asset.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.name, "cross-tenant-profile-update");
    assert_eq!(current.asset_profile_id, None);
    assert_eq!(current.metadata, json!({"version": 1}));
}

#[tokio::test]
async fn sqlite_public_device_repository_hides_unknown_inaccessible_and_deleted_mutations() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let owner_id = Uuid::now_v7();
    let viewer_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-device-owner-2', 'unused', 'viewer', 'user'),
                (?, ?, 'public-device-viewer-2', 'unused', 'viewer', 'user')",
    )
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(viewer_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id)
         VALUES ('public-inaccessible-device', ?, ?),
                ('public-deleted-device-2', ?, ?)",
    )
    .bind(test_tenant_id().to_string())
    .bind(owner_id.to_string())
    .bind(test_tenant_id().to_string())
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
        tenant_id: test_tenant_id(),
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
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
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
    seed_timescale_test_tenant(&store).await;
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'timescale-public-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let asset = PublicApiRepository::create_public_asset(
        &store,
        &PublicPrincipal {
            tenant_id: test_tenant_id(),
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
                tenant_id: test_tenant_id(),
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
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
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
    seed_timescale_test_tenant(&store).await;
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'timescale-public-device-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
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

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_gateway_delete_refuses_active_children() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();
    let tenant_id = Uuid::now_v7();
    let user_id = Uuid::now_v7();
    let gateway_id = format!("timescale-public-gateway-{user_id}");
    let child_id = format!("timescale-public-gateway-child-{user_id}");
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES ($1, $2, 'active')")
        .bind(tenant_id)
        .bind(format!("timescale-public-gateway-{tenant_id}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, $3, 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(tenant_id)
    .bind(format!("timescale-public-gateway-owner-{user_id}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id, is_gateway)
         VALUES ($1, $2, $3, TRUE), ($4, $2, $3, FALSE)",
    )
    .bind(&gateway_id)
    .bind(tenant_id)
    .bind(user_id)
    .bind(&child_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE devices
         SET gateway_device_id = $1
         WHERE device_id = $2 AND tenant_id = $3",
    )
    .bind(&gateway_id)
    .bind(&child_id)
    .bind(tenant_id)
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id,
        user_id: Some(user_id),
        app_id: "timescale-public-gateway-delete-app".to_owned(),
        account_class: AccountClass::User,
    };

    assert!(
        !PublicApiRepository::delete_public_device(&store, &principal, &gateway_id)
            .await
            .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT deleted_at IS NULL
             FROM devices
             WHERE device_id = $1 AND tenant_id = $2",
        )
        .bind(&gateway_id)
        .bind(tenant_id)
        .fetch_one(pool)
        .await
        .unwrap()
    );

    assert!(
        PublicApiRepository::delete_public_device(&store, &principal, &child_id)
            .await
            .unwrap()
    );
    assert!(
        PublicApiRepository::delete_public_device(&store, &principal, &gateway_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_device_asset_assignment_requires_asset_manager_permission() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
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
    seed_timescale_test_tenant(&store).await;
    let owner_id = Uuid::now_v7();
    let attacker_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    let unique = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, $3, 'unused', 'viewer', 'user'),
                ($4, $2, $5, 'unused', 'viewer', 'user')",
    )
    .bind(owner_id)
    .bind(test_tenant_id())
    .bind(format!("timescale-public-asset-owner-{unique}"))
    .bind(attacker_id)
    .bind(format!("timescale-public-asset-attacker-{unique}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name, owner_user_id) VALUES ($1, $2, $3, $4)")
        .bind(asset_id)
        .bind(test_tenant_id())
        .bind(format!("timescale-public-protected-asset-{unique}"))
        .bind(owner_id)
        .execute(pool)
        .await
        .unwrap();
    let attacker = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(attacker_id),
        app_id: format!("timescale-public-asset-attacker-app-{unique}"),
        account_class: AccountClass::User,
    };

    let create_error = PublicApiRepository::create_public_device(
        &store,
        &attacker,
        NewPublicDevice {
            device_id: format!("timescale-public-asset-unauthorized-create-{unique}"),
            display_name: None,
            metadata: json!({}),
            asset_id: Some(asset_id),
            device_profile_id: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_error,
        PublicDeviceError::AssetUnavailable(id) if id == asset_id
    ));

    let device = PublicApiRepository::create_public_device(
        &store,
        &attacker,
        NewPublicDevice {
            device_id: format!("timescale-public-asset-unauthorized-update-{unique}"),
            display_name: None,
            metadata: json!({}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    assert!(
        PublicApiRepository::update_public_device(
            &store,
            &attacker,
            &device.device_id,
            NewPublicDevice {
                device_id: device.device_id.clone(),
                display_name: None,
                metadata: json!({}),
                asset_id: Some(asset_id),
                device_profile_id: None,
            },
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_device_create_rejects_an_unavailable_profile_atomically() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
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
    seed_timescale_test_tenant(&store).await;
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'timescale-unavailable-profile-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let unavailable_profile_id = Uuid::now_v7();
    let result = PublicApiRepository::create_public_device(
        &store,
        &PublicPrincipal {
            tenant_id: test_tenant_id(),
            user_id: Some(user_id),
            app_id: format!("timescale-unavailable-profile-app-{unavailable_profile_id}"),
            account_class: AccountClass::User,
        },
        NewPublicDevice {
            device_id: format!("timescale-unavailable-profile-{unavailable_profile_id}"),
            display_name: None,
            metadata: json!({}),
            asset_id: None,
            device_profile_id: Some(unavailable_profile_id),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(PublicDeviceError::DeviceProfileUnavailable(id)) if id == unavailable_profile_id
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_device_assignment_serializes_with_management_asset_deletion() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
    );
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url.clone()),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    seed_timescale_test_tenant(&store).await;
    let unique = Uuid::now_v7();
    let owner_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, $3, 'unused', 'viewer', 'user')",
    )
    .bind(owner_id)
    .bind(test_tenant_id())
    .bind(format!("timescale-asset-delete-owner-{unique}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, tenant_id, name, owner_user_id) VALUES ($1, $2, $3, $4)")
        .bind(asset_id)
        .bind(test_tenant_id())
        .bind(format!("timescale-asset-delete-target-{unique}"))
        .bind(owner_id)
        .execute(pool)
        .await
        .unwrap();

    let mut gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut gate)
        .await
        .unwrap();
    sqlx::query("BEGIN").execute(&mut gate).await.unwrap();
    sqlx::query("SELECT id FROM assets WHERE id = $1 FOR UPDATE")
        .bind(asset_id)
        .execute(&mut gate)
        .await
        .unwrap();

    let deleting_store = store.clone();
    let mut deletion = tokio::spawn(async move {
        ManagementAssetRepository::delete_management_asset(
            &deleting_store,
            test_tenant_id(),
            AuditPrincipal::User(owner_id),
            asset_id,
        )
        .await
    });
    wait_for_timescale_relation_lock_count(pool, "assets", "RowShareLock", 2).await;

    let assigning_store = store.clone();
    let mut assignment = tokio::spawn(async move {
        PublicApiRepository::create_public_device(
            &assigning_store,
            &PublicPrincipal {
                tenant_id: test_tenant_id(),
                user_id: Some(owner_id),
                app_id: format!("timescale-asset-delete-app-{unique}"),
                account_class: AccountClass::User,
            },
            NewPublicDevice {
                device_id: format!("timescale-asset-delete-device-{unique}"),
                display_name: None,
                metadata: json!({}),
                asset_id: Some(asset_id),
                device_profile_id: None,
            },
        )
        .await
    });
    wait_for_timescale_relation_lock_count(pool, "assets", "RowShareLock", 3).await;
    let assignment_locked_devices =
        timescale_relation_lock_is_held(pool, "devices", "ShareRowExclusiveLock").await;
    if assignment_locked_devices {
        assignment.abort();
        deletion.abort();
        let _ = assignment.await;
        let _ = deletion.await;
        sqlx::query("COMMIT").execute(&mut gate).await.unwrap();
        panic!("asset assignment locked devices before waiting for the asset");
    }

    sqlx::query("COMMIT").execute(&mut gate).await.unwrap();
    let deletion_result = timeout(Duration::from_secs(2), &mut deletion)
        .await
        .expect("management asset deletion deadlocked")
        .unwrap();
    assert!(
        deletion_result.is_ok(),
        "management asset deletion failed: {deletion_result:?}"
    );
    let assignment_result = timeout(Duration::from_secs(2), &mut assignment)
        .await
        .expect("public device assignment deadlocked")
        .unwrap();
    assert!(
        matches!(assignment_result, Err(PublicDeviceError::AssetUnavailable(id)) if id == asset_id),
        "unexpected public device assignment result: {assignment_result:?}"
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_device_assignment_waits_for_device_profile_lock() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
    );
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url.clone()),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    seed_timescale_test_tenant(&store).await;
    let unique = Uuid::now_v7();
    let device_profile_id = Uuid::now_v7();
    let user_id = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, $3, 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(test_tenant_id())
    .bind(format!("timescale-device-profile-lock-user-{unique}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO device_profiles (id, tenant_id, name) VALUES ($1, $2, $3)")
        .bind(device_profile_id)
        .bind(test_tenant_id())
        .bind(format!("timescale-device-profile-lock-{unique}"))
        .execute(pool)
        .await
        .unwrap();

    let mut gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut gate)
        .await
        .unwrap();
    sqlx::query("BEGIN").execute(&mut gate).await.unwrap();
    sqlx::query("SELECT id FROM device_profiles WHERE id = $1 AND tenant_id = $2 FOR UPDATE")
        .bind(device_profile_id)
        .bind(test_tenant_id())
        .execute(&mut gate)
        .await
        .unwrap();

    let assigning_store = store.clone();
    let mut assignment = tokio::spawn(async move {
        PublicApiRepository::create_public_device(
            &assigning_store,
            &PublicPrincipal {
                tenant_id: test_tenant_id(),
                user_id: Some(user_id),
                app_id: format!("timescale-device-profile-lock-app-{unique}"),
                account_class: AccountClass::User,
            },
            NewPublicDevice {
                device_id: format!("timescale-device-profile-lock-device-{unique}"),
                display_name: None,
                metadata: json!({}),
                asset_id: None,
                device_profile_id: Some(device_profile_id),
            },
        )
        .await
    });

    if let Ok(result) = timeout(Duration::from_millis(100), &mut assignment).await {
        panic!("device profile lock was bypassed: {result:?}");
    }
    sqlx::query("COMMIT").execute(&mut gate).await.unwrap();
    assert!(assignment.await.unwrap().is_ok());
}
