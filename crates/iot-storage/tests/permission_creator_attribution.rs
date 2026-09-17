use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewResourcePermission, NewUserGroup, PermissionCreator, PlatformStore, ResourcePermission,
    TenantAuthorizationError,
};
use serde_json::json;
use sqlx::SqlitePool;
use uuid::Uuid;

const TENANT_A: Uuid = Uuid::from_u128(1);
const TENANT_B: Uuid = Uuid::from_u128(2);
const OWNER_A: Uuid = Uuid::from_u128(10);
const MEMBER_A: Uuid = Uuid::from_u128(11);
const USER_CREATOR_A: Uuid = Uuid::from_u128(12);
const TENANT_ACCOUNT_A: Uuid = Uuid::from_u128(13);
const TENANT_ACCOUNT_B: Uuid = Uuid::from_u128(23);
const ASSET_A: Uuid = Uuid::from_u128(30);
const DEVICE_A: &str = "device-a";

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

async fn seed_tenant_scope(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'tenant-a', 'active'), (?, 'tenant-b', 'active')",
    )
    .bind(TENANT_A.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();
    for (id, tenant_id, username) in [
        (OWNER_A, TENANT_A, "owner-a"),
        (MEMBER_A, TENANT_A, "member-a"),
        (USER_CREATOR_A, TENANT_A, "creator-a"),
    ] {
        sqlx::query(
            "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
             VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
        )
        .bind(id.to_string())
        .bind(tenant_id.to_string())
        .bind(username)
        .execute(pool)
        .await
        .unwrap();
    }
    for (id, tenant_id) in [(TENANT_ACCOUNT_A, TENANT_A), (TENANT_ACCOUNT_B, TENANT_B)] {
        sqlx::query(
            "INSERT INTO tenant_accounts (id, tenant_id, password_hash, status, credential_version)
             VALUES (?, ?, 'unused', 'active', 1)",
        )
        .bind(id.to_string())
        .bind(tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES (?, ?, 'asset-a')")
        .bind(ASSET_A.to_string())
        .bind(TENANT_A.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES (?, ?)")
        .bind(DEVICE_A)
        .bind(TENANT_A.to_string())
        .execute(pool)
        .await
        .unwrap();
}

fn direct_device_permission(created_by: PermissionCreator) -> NewResourcePermission {
    NewResourcePermission {
        tenant_id: TENANT_A,
        subject_user_id: Some(MEMBER_A),
        subject_group_id: None,
        asset_id: None,
        device_id: Some(DEVICE_A.to_owned()),
        permission: ResourcePermission::Viewer,
        inherit_children: false,
        created_by,
    }
}

#[tokio::test]
async fn sqlite_permission_creator_accepts_tenant_account_and_user_attribution() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenant_scope(pool).await;

    let direct_permission = store
        .create_resource_permission(direct_device_permission(PermissionCreator::TenantAccount(
            TENANT_ACCOUNT_A,
        )))
        .await
        .unwrap();
    let group = store
        .create_user_group(NewUserGroup {
            tenant_id: TENANT_A,
            owner_user_id: OWNER_A,
            name: "operators".to_owned(),
            metadata: json!({}),
        })
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
            created_by: PermissionCreator::User(USER_CREATOR_A),
        })
        .await
        .unwrap();

    assert_eq!(
        direct_permission.created_by,
        PermissionCreator::TenantAccount(TENANT_ACCOUNT_A)
    );
    assert_eq!(
        group_permission.created_by,
        PermissionCreator::User(USER_CREATOR_A)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT created_by_user_id FROM resource_permissions WHERE id = ?",
        )
        .bind(direct_permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT created_by_tenant_account_id FROM resource_permissions WHERE id = ?",
        )
        .bind(direct_permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(TENANT_ACCOUNT_A.to_string())
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT created_by_user_id FROM resource_permissions WHERE id = ?",
        )
        .bind(group_permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(USER_CREATOR_A.to_string())
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT created_by_tenant_account_id FROM resource_permissions WHERE id = ?",
        )
        .bind(group_permission.id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_permission_creator_rejects_cross_tenant_tenant_account() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenant_scope(pool).await;

    let result = store
        .create_resource_permission(direct_device_permission(PermissionCreator::TenantAccount(
            TENANT_ACCOUNT_B,
        )))
        .await;

    assert!(matches!(
        result,
        Err(TenantAuthorizationError::TenantAccountNotFound {
            tenant_id: TENANT_A,
            tenant_account_id: TENANT_ACCOUNT_B,
        })
    ));
    assert!(
        sqlx::query(
            "INSERT INTO resource_permissions (
                id, tenant_id, subject_user_id, asset_id, permission, inherit_children,
                created_by_user_id, created_by_tenant_account_id
             ) VALUES (?, ?, ?, ?, 'viewer', 0, NULL, ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(TENANT_A.to_string())
        .bind(MEMBER_A.to_string())
        .bind(ASSET_A.to_string())
        .bind(TENANT_ACCOUNT_B.to_string())
        .execute(pool)
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_permissions")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_resource_permission_schema_requires_exactly_one_creator() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_tenant_scope(pool).await;

    for (created_by_user_id, created_by_tenant_account_id) in
        [(None, None), (Some(USER_CREATOR_A), Some(TENANT_ACCOUNT_A))]
    {
        let result = sqlx::query(
            "INSERT INTO resource_permissions (
                id, tenant_id, subject_user_id, asset_id, permission, inherit_children,
                created_by_user_id, created_by_tenant_account_id
             ) VALUES (?, ?, ?, ?, 'viewer', 0, ?, ?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(TENANT_A.to_string())
        .bind(MEMBER_A.to_string())
        .bind(ASSET_A.to_string())
        .bind(created_by_user_id.map(|value| value.to_string()))
        .bind(created_by_tenant_account_id.map(|value| value.to_string()))
        .execute(pool)
        .await;
        assert!(result.is_err());
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_permissions")
            .fetch_one(pool)
            .await
            .unwrap(),
        0
    );
}
