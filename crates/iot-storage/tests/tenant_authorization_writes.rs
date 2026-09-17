use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewResourcePermission, NewUserGroup, PlatformStore, ResourcePermission,
    TenantAuthorizationError,
};
use serde_json::json;
use sqlx::SqlitePool;
use uuid::Uuid;

const TENANT_A: Uuid = Uuid::from_u128(1);
const TENANT_B: Uuid = Uuid::from_u128(2);
const OWNER_A: Uuid = Uuid::from_u128(10);
const MEMBER_A: Uuid = Uuid::from_u128(11);
const CREATOR_A: Uuid = Uuid::from_u128(12);
const USER_B: Uuid = Uuid::from_u128(20);
const ASSET_A: Uuid = Uuid::from_u128(30);
const ASSET_B: Uuid = Uuid::from_u128(31);
const DEVICE_A: &str = "device-a";
const DEVICE_B: &str = "device-b";

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn insert_user(pool: &SqlitePool, user_id: Uuid, tenant_id: Uuid, username: &str) {
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .bind(username)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_tenants(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'tenant-a', 'active'), (?, 'tenant-b', 'active')",
    )
    .bind(TENANT_A.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    insert_user(pool, OWNER_A, TENANT_A, "owner-a").await;
    insert_user(pool, MEMBER_A, TENANT_A, "member-a").await;
    insert_user(pool, CREATOR_A, TENANT_A, "creator-a").await;
    insert_user(pool, USER_B, TENANT_B, "user-b").await;
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name)
         VALUES (?, ?, 'asset-a'), (?, ?, 'asset-b')",
    )
    .bind(ASSET_A.to_string())
    .bind(TENANT_A.to_string())
    .bind(ASSET_B.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id)
         VALUES (?, ?), (?, ?)",
    )
    .bind(DEVICE_A)
    .bind(TENANT_A.to_string())
    .bind(DEVICE_B)
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
}

fn direct_device_permission() -> NewResourcePermission {
    NewResourcePermission {
        tenant_id: TENANT_A,
        subject_user_id: Some(MEMBER_A),
        subject_group_id: None,
        asset_id: None,
        device_id: Some(DEVICE_A.to_owned()),
        permission: ResourcePermission::Viewer,
        inherit_children: false,
        created_by_user_id: CREATOR_A,
    }
}

#[tokio::test]
async fn sqlite_tenant_authorization_writes_create_direct_and_group_permissions() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;

    let group = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "operators".to_owned(),
            metadata: json!({"site": "north"}),
        })
        .await
        .unwrap();
    assert_eq!(group.tenant_id, TENANT_A);
    assert_eq!(group.owner_user_id, OWNER_A);
    assert_eq!(group.name, "operators");
    assert!(
        store
            .add_user_to_group(TENANT_A, group.id, MEMBER_A)
            .await
            .unwrap()
    );

    let direct_permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();
    let group_permission = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: TENANT_A,
            subject_user_id: None,
            subject_group_id: Some(group.id),
            asset_id: Some(ASSET_A),
            device_id: None,
            permission: ResourcePermission::Manager,
            inherit_children: true,
            created_by_user_id: CREATOR_A,
        })
        .await
        .unwrap();

    assert_eq!(direct_permission.subject_user_id, Some(MEMBER_A));
    assert_eq!(direct_permission.device_id.as_deref(), Some(DEVICE_A));
    assert_eq!(group_permission.subject_group_id, Some(group.id));
    assert_eq!(group_permission.asset_id, Some(ASSET_A));
    assert!(group_permission.inherit_children);
    assert!(
        store
            .remove_user_from_group(TENANT_A, group.id, MEMBER_A)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM user_group_members WHERE group_id = ? AND user_id = ?",
        )
        .bind(group.id.to_string())
        .bind(MEMBER_A.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_tenant_authorization_writes_reject_cross_tenant_and_invalid_permission_shapes() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;
    let group_a = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "tenant-a-group".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    let group_b = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_B,
            owner_user_id: USER_B,
            name: "tenant-b-group".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();

    assert!(matches!(
        store
            .create_user_group(NewUserGroup {
                tenant_id: TENANT_A,
                owner_user_id: USER_B,
                name: "invalid-owner".to_owned(),
                metadata: json!({}),
            })
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store.add_user_to_group(TENANT_A, group_a.id, USER_B).await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_user_id: Some(USER_B),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_user_id: None,
                subject_group_id: Some(group_b.id),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::GroupNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                asset_id: Some(ASSET_B),
                device_id: None,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::AssetNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                device_id: Some(DEVICE_B.to_owned()),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::DeviceNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                created_by_user_id: USER_B,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::UserNotFound { .. })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_user_id: None,
                subject_group_id: None,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionSubject)
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                subject_group_id: Some(group_a.id),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionSubject)
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                asset_id: Some(ASSET_A),
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionResource)
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                permission: ResourcePermission::Owner,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::InvalidPermissionLevel {
            permission: ResourcePermission::Owner
        })
    ));
    assert!(matches!(
        store
            .create_resource_permission(NewResourcePermission {
                inherit_children: true,
                ..direct_device_permission()
            })
            .await,
        Err(TenantAuthorizationError::DevicePermissionCannotInherit)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_permissions")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_tenant_authorization_writes_revoke_only_the_tenant_permission() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenants(pool).await;
    let permission = store
        .create_resource_permission(direct_device_permission())
        .await
        .unwrap();

    assert!(matches!(
        store
            .revoke_resource_permission(TENANT_B, permission.id)
            .await,
        Err(TenantAuthorizationError::PermissionNotFound { .. })
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
    assert!(
        store
            .revoke_resource_permission(TENANT_A, permission.id)
            .await
            .unwrap()
    );
    assert!(
        !store
            .revoke_resource_permission(TENANT_A, permission.id)
            .await
            .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resource_permissions WHERE id = ? AND revoked_at IS NOT NULL",
        )
        .bind(permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        1
    );
}
