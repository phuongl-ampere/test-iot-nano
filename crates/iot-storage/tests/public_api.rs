use chrono::{Duration as ChronoDuration, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, ManagementAssetRepository, NewPublicAsset, NewPublicDevice,
    NewPublicResourceGrant, PlatformStore, PublicApiRepository, PublicDeviceError, PublicPrincipal,
    ResourcePermission,
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
    common::lock_timescale_schema(&mut connection)
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
async fn sqlite_public_application_grant_reads_telemetry_and_alerts() {
    let (_directory, store) = sqlite_store().await;
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "public-observer-app".to_owned(),
        account_class: AccountClass::User,
    };
    let device = PublicApiRepository::create_public_device(
        &store,
        &principal,
        NewPublicDevice {
            device_id: "public-observer-device".to_owned(),
            display_name: Some("Public observer device".to_owned()),
            metadata: json!({}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();
    let event_at = Utc::now();
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES (?, ?, ?, ?, ?, ?, 'public-observer')",
    )
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(&device.device_id)
    .bind(Uuid::now_v7().to_string())
    .bind(1_i64)
    .bind(json!({ "temperature_c": 26.0 }).to_string())
    .execute(pool)
    .await
    .unwrap();
    let rule_id = Uuid::now_v7();
    let alert_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, device_id, metric_key, rule_type, comparison, threshold
         ) VALUES (?, 'Public observer rule', ?, 'temperature_c', 'event', 'gt', 25)",
    )
    .bind(rule_id.to_string())
    .bind(&device.device_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at, updated_at
         ) VALUES (?, ?, ?, 'open', ?, ?, ?)",
    )
    .bind(alert_id.to_string())
    .bind(rule_id.to_string())
    .bind(&device.device_id)
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .bind(event_at.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();

    let telemetry = PublicApiRepository::list_public_telemetry(
        &store,
        &principal,
        None,
        event_at - ChronoDuration::minutes(1),
        event_at + ChronoDuration::minutes(1),
        None,
        100,
    )
    .await
    .unwrap();
    let alerts = PublicApiRepository::list_public_alerts(&store, &principal, None, 100)
        .await
        .unwrap();

    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry[0].device_id, device.device_id);
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].id, alert_id);
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
async fn sqlite_public_repository_filters_assets_and_persists_grants() {
    let (_directory, store) = sqlite_store().await;
    let user_id = Uuid::now_v7();
    let other_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-repository-user', 'unused', 'viewer', 'user'),
                (?, ?, 'public-repository-other', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(other_id.to_string())
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
async fn sqlite_public_grant_pagination_filters_invisible_rows_before_limit() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let user_id = Uuid::now_v7();
    let foreign_user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'public-grant-page-user', 'unused', 'viewer', 'user'),
                (?, ?, 'public-grant-page-foreign', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(foreign_user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(user_id),
        app_id: "public-grant-page-app".to_owned(),
        account_class: AccountClass::User,
    };
    let hidden_first = Uuid::from_u128(1);
    let hidden_second = Uuid::from_u128(2);
    let visible_first = Uuid::from_u128(3);
    let visible_second = Uuid::from_u128(4);
    for (id, created_by_user_id, grantee_id) in [
        (hidden_first, foreign_user_id, "foreign-grant-page-app-one"),
        (hidden_second, foreign_user_id, "foreign-grant-page-app-two"),
        (visible_first, user_id, "visible-grant-page-app-one"),
        (visible_second, user_id, "visible-grant-page-app-two"),
    ] {
        sqlx::query(
            "INSERT INTO resource_grants
                (id, tenant_id, resource_type, resource_id, grantee_type, grantee_id, permission,
                 created_by_user_id)
             VALUES (?, ?, 'device', 'public-grant-page-device', 'application', ?, 'viewer', ?)",
        )
        .bind(id.to_string())
        .bind(test_tenant_id().to_string())
        .bind(grantee_id)
        .bind(created_by_user_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    let page = PublicApiRepository::list_public_grants(&store, &principal, None, 2)
        .await
        .unwrap();
    assert_eq!(
        page.iter().map(|grant| grant.id).collect::<Vec<_>>(),
        [visible_first, visible_second]
    );
    let next_page = PublicApiRepository::list_public_grants(
        &store,
        &principal,
        Some(&visible_first.to_string()),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        next_page.iter().map(|grant| grant.id).collect::<Vec<_>>(),
        [visible_second]
    );
}

#[tokio::test]
async fn sqlite_public_device_permission_denies_active_shares_and_grants_for_deleted_devices() {
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
            (id, tenant_id, resource_type, resource_id, grantee_type, grantee_id, permission,
             created_by_user_id)
         VALUES (?, ?, 'device', 'public-deleted-device', 'user', ?, 'controller', ?)",
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
        "INSERT INTO resource_shares (
            id, resource_type, resource_id, target_user_id, permission,
            inherit_children, state, created_by_user_id
         ) VALUES (?, 'asset', ?, ?, 'viewer', 1, 'active', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(asset_id.to_string())
    .bind(attacker_id.to_string())
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
async fn sqlite_public_device_permission_requires_an_application_grant() {
    let (_directory, store) = sqlite_store().await;
    let owning_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "public-device-owning-application".to_owned(),
        account_class: AccountClass::User,
    };
    let other_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "public-device-other-application".to_owned(),
        account_class: AccountClass::User,
    };
    let created = PublicApiRepository::create_public_device(
        &store,
        &owning_application,
        NewPublicDevice {
            device_id: "public-application-owned-device".to_owned(),
            display_name: None,
            metadata: json!({}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        PublicApiRepository::public_device_permission(
            &store,
            &owning_application,
            &created.device_id,
        )
        .await
        .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        PublicApiRepository::public_device_permission(
            &store,
            &other_application,
            &created.device_id
        )
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_public_assets_and_grants_require_matching_application_grants() {
    let (_directory, store) = sqlite_store().await;
    let owning_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "public-asset-owning-application".to_owned(),
        account_class: AccountClass::User,
    };
    let other_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "public-asset-other-application".to_owned(),
        account_class: AccountClass::User,
    };
    let asset = PublicApiRepository::create_public_asset(
        &store,
        &owning_application,
        NewPublicAsset {
            name: "public-application-owned-asset".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({"zone":"lab"}),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        PublicApiRepository::public_asset_permission(&store, &owning_application, asset.id)
            .await
            .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        PublicApiRepository::public_asset_permission(&store, &other_application, asset.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        PublicApiRepository::get_public_asset(&store, &owning_application, asset.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        asset.id
    );
    assert_eq!(
        PublicApiRepository::list_public_assets(&store, &owning_application, None, 10)
            .await
            .unwrap(),
        [asset.clone()]
    );
    assert!(
        PublicApiRepository::list_public_assets(&store, &other_application, None, 10)
            .await
            .unwrap()
            .is_empty()
    );

    let denied_asset = NewPublicAsset {
        name: "other application update".to_owned(),
        asset_profile_id: None,
        parent_asset_id: None,
        metadata: json!({"zone":"guest"}),
    };
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &other_application,
            asset.id,
            denied_asset,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        !PublicApiRepository::delete_public_asset(&store, &other_application, asset.id)
            .await
            .unwrap()
    );
    assert!(
        PublicApiRepository::get_public_asset(&store, &other_application, asset.id)
            .await
            .unwrap()
            .is_none()
    );

    let grant_request = NewPublicResourceGrant {
        resource_type: "asset".to_owned(),
        resource_id: asset.id.to_string(),
        grantee_type: "application".to_owned(),
        grantee_id: "public-asset-recipient-application".to_owned(),
        permission: "viewer".to_owned(),
    };
    assert!(
        PublicApiRepository::create_public_grant(
            &store,
            &other_application,
            grant_request.clone(),
        )
        .await
        .unwrap()
        .is_none()
    );
    let grant = PublicApiRepository::create_public_grant(
        &store,
        &owning_application,
        grant_request.clone(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        PublicApiRepository::get_public_grant(&store, &owning_application, grant.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        grant.id
    );
    assert!(
        PublicApiRepository::list_public_grants(&store, &owning_application, None, 10)
            .await
            .unwrap()
            .iter()
            .any(|candidate| candidate.id == grant.id)
    );
    assert!(
        PublicApiRepository::get_public_grant(&store, &other_application, grant.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        PublicApiRepository::list_public_grants(&store, &other_application, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        PublicApiRepository::update_public_grant(
            &store,
            &other_application,
            grant.id,
            NewPublicResourceGrant {
                permission: "manager".to_owned(),
                ..grant_request.clone()
            },
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        !PublicApiRepository::delete_public_grant(&store, &other_application, grant.id)
            .await
            .unwrap()
    );

    let updated_asset = PublicApiRepository::update_public_asset(
        &store,
        &owning_application,
        asset.id,
        NewPublicAsset {
            name: "public-application-owned-asset-updated".to_owned(),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({"zone":"office"}),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(updated_asset.metadata, json!({"zone":"office"}));
    assert!(
        PublicApiRepository::delete_public_asset(&store, &owning_application, asset.id)
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
    common::lock_timescale_schema(&mut connection)
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
    common::lock_timescale_schema(&mut connection)
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
    common::lock_timescale_schema(&mut connection)
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
    let owner_id = Uuid::now_v7();
    let attacker_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    let unique = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'unused', 'viewer', 'user'),
                ($3, $4, 'unused', 'viewer', 'user')",
    )
    .bind(owner_id)
    .bind(format!("timescale-public-asset-owner-{unique}"))
    .bind(attacker_id)
    .bind(format!("timescale-public-asset-attacker-{unique}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, name, owner_user_id) VALUES ($1, $2, $3)")
        .bind(asset_id)
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
async fn timescale_public_device_permission_requires_an_application_grant() {
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
    common::lock_timescale_schema(&mut connection)
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
    let owning_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "timescale-public-device-owning-application".to_owned(),
        account_class: AccountClass::User,
    };
    let other_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: "timescale-public-device-other-application".to_owned(),
        account_class: AccountClass::User,
    };
    let created = PublicApiRepository::create_public_device(
        &store,
        &owning_application,
        NewPublicDevice {
            device_id: format!("timescale-public-application-device-{}", Uuid::now_v7()),
            display_name: None,
            metadata: json!({}),
            asset_id: None,
            device_profile_id: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        PublicApiRepository::public_device_permission(
            &store,
            &owning_application,
            &created.device_id,
        )
        .await
        .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        PublicApiRepository::public_device_permission(
            &store,
            &other_application,
            &created.device_id
        )
        .await
        .unwrap(),
        None
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
    common::lock_timescale_schema(&mut connection)
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
    let unavailable_profile_id = Uuid::now_v7();
    let result = PublicApiRepository::create_public_device(
        &store,
        &PublicPrincipal {
            tenant_id: test_tenant_id(),
            user_id: None,
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
    common::lock_timescale_schema(&mut connection)
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
    let unique = Uuid::now_v7();
    let owner_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'unused', 'viewer', 'user')",
    )
    .bind(owner_id)
    .bind(format!("timescale-asset-delete-owner-{unique}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, name, owner_user_id) VALUES ($1, $2, $3)")
        .bind(asset_id)
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
    assert!(matches!(
        timeout(Duration::from_secs(2), &mut assignment)
            .await
            .expect("public device assignment deadlocked")
            .unwrap(),
        Err(PublicDeviceError::AssetUnavailable(id)) if id == asset_id
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_device_assignment_waits_for_asset_manager_grant_lock() {
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
    common::lock_timescale_schema(&mut connection)
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
    let owner_id = Uuid::now_v7();
    let manager_id = Uuid::now_v7();
    let asset_id = Uuid::now_v7();
    let grant_id = Uuid::now_v7();
    let unique = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'unused', 'viewer', 'user'),
                ($3, $4, 'unused', 'viewer', 'user')",
    )
    .bind(owner_id)
    .bind(format!("timescale-asset-lock-owner-{unique}"))
    .bind(manager_id)
    .bind(format!("timescale-asset-lock-manager-{unique}"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO assets (id, name, owner_user_id) VALUES ($1, $2, $3)")
        .bind(asset_id)
        .bind(format!("timescale-asset-lock-target-{unique}"))
        .bind(owner_id)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_grants
            (id, resource_type, resource_id, grantee_type, grantee_id, permission)
         VALUES ($1, 'asset', $2, 'user', $3, 'manager')",
    )
    .bind(grant_id)
    .bind(asset_id.to_string())
    .bind(manager_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let mut gate = PgConnection::connect(&database_url).await.unwrap();
    sqlx::query("SET search_path TO iot_nano")
        .execute(&mut gate)
        .await
        .unwrap();
    sqlx::query("BEGIN").execute(&mut gate).await.unwrap();
    sqlx::query("SELECT id FROM resource_grants WHERE id = $1 FOR UPDATE")
        .bind(grant_id)
        .execute(&mut gate)
        .await
        .unwrap();

    let assigning_store = store.clone();
    let mut assignment = tokio::spawn(async move {
        PublicApiRepository::create_public_device(
            &assigning_store,
            &PublicPrincipal {
                tenant_id: test_tenant_id(),
                user_id: Some(manager_id),
                app_id: format!("timescale-asset-lock-app-{unique}"),
                account_class: AccountClass::User,
            },
            NewPublicDevice {
                device_id: format!("timescale-asset-lock-device-{unique}"),
                display_name: None,
                metadata: json!({}),
                asset_id: Some(asset_id),
                device_profile_id: None,
            },
        )
        .await
    });

    if let Ok(result) = timeout(Duration::from_millis(100), &mut assignment).await {
        panic!("asset manager grant lock was bypassed: {result:?}");
    }
    sqlx::query("COMMIT").execute(&mut gate).await.unwrap();
    assert!(assignment.await.unwrap().is_ok());
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
    common::lock_timescale_schema(&mut connection)
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
    let unique = Uuid::now_v7();
    let device_profile_id = Uuid::now_v7();
    let pool = store.timescale_pool().unwrap();
    sqlx::query("INSERT INTO device_profiles (id, name) VALUES ($1, $2)")
        .bind(device_profile_id)
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
    sqlx::query("SELECT id FROM device_profiles WHERE id = $1 FOR UPDATE")
        .bind(device_profile_id)
        .execute(&mut gate)
        .await
        .unwrap();

    let assigning_store = store.clone();
    let mut assignment = tokio::spawn(async move {
        PublicApiRepository::create_public_device(
            &assigning_store,
            &PublicPrincipal {
                tenant_id: test_tenant_id(),
                user_id: None,
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

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_assets_and_grants_require_matching_application_grants() {
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
    common::lock_timescale_schema(&mut connection)
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
    let unique = Uuid::now_v7();
    let owning_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: format!("timescale-public-asset-owner-{unique}"),
        account_class: AccountClass::User,
    };
    let other_application = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: None,
        app_id: format!("timescale-public-asset-other-{unique}"),
        account_class: AccountClass::User,
    };
    let asset = PublicApiRepository::create_public_asset(
        &store,
        &owning_application,
        NewPublicAsset {
            name: format!("timescale-public-application-asset-{unique}"),
            asset_profile_id: None,
            parent_asset_id: None,
            metadata: json!({"backend":"timescale"}),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        PublicApiRepository::public_asset_permission(&store, &owning_application, asset.id)
            .await
            .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        PublicApiRepository::public_asset_permission(&store, &other_application, asset.id)
            .await
            .unwrap(),
        None
    );
    assert!(
        PublicApiRepository::list_public_assets(&store, &other_application, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        PublicApiRepository::update_public_asset(
            &store,
            &other_application,
            asset.id,
            NewPublicAsset {
                name: format!("timescale-guest-update-{unique}"),
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
        !PublicApiRepository::delete_public_asset(&store, &other_application, asset.id)
            .await
            .unwrap()
    );

    let grant_request = NewPublicResourceGrant {
        resource_type: "asset".to_owned(),
        resource_id: asset.id.to_string(),
        grantee_type: "application".to_owned(),
        grantee_id: format!("timescale-public-asset-recipient-{unique}"),
        permission: "viewer".to_owned(),
    };
    assert!(
        PublicApiRepository::create_public_grant(
            &store,
            &other_application,
            grant_request.clone(),
        )
        .await
        .unwrap()
        .is_none()
    );
    let grant = PublicApiRepository::create_public_grant(
        &store,
        &owning_application,
        grant_request.clone(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        PublicApiRepository::get_public_grant(&store, &owning_application, grant.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        grant.id
    );
    assert!(
        PublicApiRepository::list_public_grants(&store, &owning_application, None, 100)
            .await
            .unwrap()
            .iter()
            .any(|candidate| candidate.id == grant.id)
    );
    assert!(
        PublicApiRepository::get_public_grant(&store, &other_application, grant.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        PublicApiRepository::list_public_grants(&store, &other_application, None, 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        PublicApiRepository::update_public_grant(
            &store,
            &other_application,
            grant.id,
            NewPublicResourceGrant {
                permission: "manager".to_owned(),
                ..grant_request.clone()
            },
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        !PublicApiRepository::delete_public_grant(&store, &other_application, grant.id)
            .await
            .unwrap()
    );

    assert!(
        PublicApiRepository::delete_public_asset(&store, &owning_application, asset.id)
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_public_grant_pagination_filters_invisible_rows_before_limit() {
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
    common::lock_timescale_schema(&mut connection)
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
    let unique = Uuid::now_v7();
    let user_id = Uuid::now_v7();
    let foreign_user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, $2, 'unused', 'viewer', 'user'),
                ($3, $4, 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(format!("timescale-public-grant-page-user-{unique}"))
    .bind(foreign_user_id)
    .bind(format!("timescale-public-grant-page-foreign-{unique}"))
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let principal = PublicPrincipal {
        tenant_id: test_tenant_id(),
        user_id: Some(user_id),
        app_id: format!("timescale-public-grant-page-app-{unique}"),
        account_class: AccountClass::User,
    };
    let hidden_first = Uuid::now_v7();
    let hidden_second = Uuid::now_v7();
    let visible_first = Uuid::now_v7();
    let visible_second = Uuid::now_v7();
    for (id, created_by_user_id, grantee_id) in [
        (
            hidden_first,
            foreign_user_id,
            format!("timescale-public-grant-hidden-one-{unique}"),
        ),
        (
            hidden_second,
            foreign_user_id,
            format!("timescale-public-grant-hidden-two-{unique}"),
        ),
        (
            visible_first,
            user_id,
            format!("timescale-public-grant-visible-one-{unique}"),
        ),
        (
            visible_second,
            user_id,
            format!("timescale-public-grant-visible-two-{unique}"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO resource_grants
                (id, resource_type, resource_id, grantee_type, grantee_id, permission,
                 created_by_user_id)
             VALUES ($1, 'device', $2, 'application', $3, 'viewer', $4)",
        )
        .bind(id)
        .bind(format!("timescale-public-grant-page-device-{unique}"))
        .bind(grantee_id)
        .bind(created_by_user_id)
        .execute(store.timescale_pool().unwrap())
        .await
        .unwrap();
    }

    let page = PublicApiRepository::list_public_grants(
        &store,
        &principal,
        Some(&hidden_first.to_string()),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        page.iter().map(|grant| grant.id).collect::<Vec<_>>(),
        [visible_first, visible_second]
    );
    let next_page = PublicApiRepository::list_public_grants(
        &store,
        &principal,
        Some(&visible_first.to_string()),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        next_page.iter().map(|grant| grant.id).collect::<Vec<_>>(),
        [visible_second]
    );
}
