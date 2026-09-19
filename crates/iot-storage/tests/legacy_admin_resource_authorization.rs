use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{AccountClass, AuthorizationSubject, PlatformStore, ResourcePermission};
use uuid::Uuid;

const TENANT_ID: Uuid = Uuid::from_u128(1);
const ADMIN_USER_ID: Uuid = Uuid::from_u128(2);
const OWNER_USER_ID: Uuid = Uuid::from_u128(3);
const UNSHARED_ASSET_ID: Uuid = Uuid::from_u128(10);
const OWNED_ASSET_ID: Uuid = Uuid::from_u128(11);
const GRANTED_ASSET_ID: Uuid = Uuid::from_u128(12);

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(
            directory
                .path()
                .join("legacy-admin-resource-authorization.sqlite"),
        ),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn seed_resources(store: &PlatformStore) {
    let pool = store.sqlite_pool().unwrap();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'legacy-admin', 'active')")
        .bind(TENANT_ID.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'legacy-admin-user', 'unused', 'admin', 'admin'),
                (?, ?, 'resource-owner', 'unused', 'viewer', 'user')",
    )
    .bind(ADMIN_USER_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .bind(TENANT_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, owner_user_id)
         VALUES (?, ?, 'unshared asset', ?),
                (?, ?, 'owned asset', ?),
                (?, ?, 'granted asset', ?)",
    )
    .bind(UNSHARED_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .bind(OWNED_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(ADMIN_USER_ID.to_string())
    .bind(GRANTED_ASSET_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, owner_user_id)
         VALUES ('unshared-device', ?, ?),
                ('owned-device', ?, ?),
                ('granted-device', ?, ?)",
    )
    .bind(TENANT_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(ADMIN_USER_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resource_permissions (
             id, tenant_id, subject_user_id, asset_id, device_id, permission,
             inherit_children, created_by_user_id
         ) VALUES ('legacy-admin-asset-grant', ?, ?, ?, NULL, 'manager', 0, ?),
                  ('legacy-admin-device-grant', ?, ?, NULL, 'granted-device', 'manager', 0, ?)",
    )
    .bind(TENANT_ID.to_string())
    .bind(ADMIN_USER_ID.to_string())
    .bind(GRANTED_ASSET_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .bind(TENANT_ID.to_string())
    .bind(ADMIN_USER_ID.to_string())
    .bind(OWNER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
}

fn can_mutate(permission: Option<ResourcePermission>) -> bool {
    permission.is_some_and(|permission| permission.allows(ResourcePermission::Manager))
}

#[tokio::test]
async fn sqlite_legacy_admin_user_requires_ownership_or_explicit_permission() {
    let (_directory, store) = sqlite_store().await;
    seed_resources(&store).await;
    let subject = store
        .authorization_subject(ADMIN_USER_ID)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        subject,
        AuthorizationSubject {
            user_id: ADMIN_USER_ID,
            tenant_id: TENANT_ID,
            account_class: AccountClass::Admin,
        }
    );

    let unshared_asset = store
        .asset_permission(&subject, UNSHARED_ASSET_ID)
        .await
        .unwrap();
    let unshared_device = store
        .device_permission(&subject, "unshared-device")
        .await
        .unwrap();
    assert_eq!(unshared_asset, None);
    assert_eq!(unshared_device, None);
    assert!(!can_mutate(unshared_asset));
    assert!(!can_mutate(unshared_device));
    assert!(
        store
            .authorized_device(&subject, "unshared-device")
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        store
            .asset_permission(&subject, OWNED_ASSET_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        store
            .device_permission(&subject, "owned-device")
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        store
            .asset_permission(&subject, GRANTED_ASSET_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        store
            .device_permission(&subject, "granted-device")
            .await
            .unwrap(),
        Some(ResourcePermission::Manager)
    );

    let devices = store
        .list_authorized_devices(&subject, None, 10)
        .await
        .unwrap();
    let device_ids = devices
        .iter()
        .map(|device| device.device_id.as_str())
        .collect::<Vec<_>>();
    assert!(!device_ids.contains(&"unshared-device"));
    assert!(device_ids.contains(&"owned-device"));
    assert!(device_ids.contains(&"granted-device"));
}
