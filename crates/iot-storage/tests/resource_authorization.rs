use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{AccountClass, AuthorizationSubject, PlatformStore, ResourcePermission};
use sqlx::SqlitePool;
use uuid::Uuid;

const TENANT_A: Uuid = Uuid::from_u128(1);
const TENANT_B: Uuid = Uuid::from_u128(2);
const USER_A: Uuid = Uuid::from_u128(10);
const OTHER_USER_A: Uuid = Uuid::from_u128(11);
const LEGACY_ADMIN_A: Uuid = Uuid::from_u128(12);
const USER_B: Uuid = Uuid::from_u128(20);
const GROUP_A: Uuid = Uuid::from_u128(30);
const ROOT_ASSET_A: Uuid = Uuid::from_u128(40);
const CHILD_ASSET_A: Uuid = Uuid::from_u128(41);
const DEPTH_LEAF_ASSET_A: Uuid = Uuid::from_u128(50);

const DIRECT_DEVICE_A: &str = "device-direct-a";
const GROUP_DEVICE_A: &str = "device-group-a";
const INHERITED_DEVICE_A: &str = "device-inherited-a";
const REVOKED_DEVICE_A: &str = "device-revoked-a";
const OWNED_DEVICE_A: &str = "device-owned-a";
const UNSHARED_DEVICE_A: &str = "device-unshared-a";
const DEVICE_B: &str = "device-b";
const DEPTH_DEVICE_A: &str = "device-depth-a";

fn subject(user_id: Uuid, tenant_id: Uuid, account_class: AccountClass) -> AuthorizationSubject {
    AuthorizationSubject {
        user_id,
        tenant_id,
        account_class,
    }
}

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

async fn seed_identities(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES (?, 'tenant-a', 'active'), (?, 'tenant-b', 'active')",
    )
    .bind(TENANT_A.to_string())
    .bind(TENANT_B.to_string())
    .execute(pool)
    .await
    .unwrap();

    for (user_id, tenant_id, username, account_class) in [
        (USER_A, TENANT_A, "user-a", AccountClass::User),
        (OTHER_USER_A, TENANT_A, "other-user-a", AccountClass::User),
        (
            LEGACY_ADMIN_A,
            TENANT_A,
            "legacy-admin-a",
            AccountClass::Admin,
        ),
        (USER_B, TENANT_B, "user-b", AccountClass::User),
    ] {
        sqlx::query(
            "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
             VALUES (?, ?, ?, 'unused', 'viewer', ?)",
        )
        .bind(user_id.to_string())
        .bind(tenant_id.to_string())
        .bind(username)
        .bind(account_class.as_str())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn insert_asset(
    pool: &SqlitePool,
    asset_id: Uuid,
    tenant_id: Uuid,
    name: &str,
    parent_asset_id: Option<Uuid>,
    owner_user_id: Option<Uuid>,
) {
    sqlx::query(
        "INSERT INTO assets (id, tenant_id, name, parent_asset_id, owner_user_id)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .bind(name)
    .bind(parent_asset_id.map(|value| value.to_string()))
    .bind(owner_user_id.map(|value| value.to_string()))
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_device(
    pool: &SqlitePool,
    device_id: &str,
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    owner_user_id: Option<Uuid>,
) {
    sqlx::query(
        "INSERT INTO devices (device_id, tenant_id, asset_id, owner_user_id)
         VALUES (?, ?, ?, ?)",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .bind(asset_id.map(|value| value.to_string()))
    .bind(owner_user_id.map(|value| value.to_string()))
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_group_member(
    pool: &SqlitePool,
    tenant_id: Uuid,
    group_id: Uuid,
    owner_user_id: Uuid,
    user_id: Uuid,
) {
    sqlx::query(
        "INSERT INTO user_groups (id, tenant_id, owner_user_id, name)
         VALUES (?, ?, ?, 'operators')",
    )
    .bind(group_id.to_string())
    .bind(tenant_id.to_string())
    .bind(owner_user_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO user_group_members (tenant_id, group_id, user_id)
         VALUES (?, ?, ?)",
    )
    .bind(tenant_id.to_string())
    .bind(group_id.to_string())
    .bind(user_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn insert_permission(
    pool: &SqlitePool,
    permission_id: &str,
    tenant_id: Uuid,
    subject_user_id: Option<Uuid>,
    subject_group_id: Option<Uuid>,
    asset_id: Option<Uuid>,
    device_id: Option<&str>,
    permission: ResourcePermission,
    inherit_children: bool,
    created_by_user_id: Uuid,
    revoked_at: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO resource_permissions (
            id, tenant_id, subject_user_id, subject_group_id, asset_id, device_id,
            permission, inherit_children, created_by_user_id, revoked_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(permission_id)
    .bind(tenant_id.to_string())
    .bind(subject_user_id.map(|value| value.to_string()))
    .bind(subject_group_id.map(|value| value.to_string()))
    .bind(asset_id.map(|value| value.to_string()))
    .bind(device_id)
    .bind(permission.as_str())
    .bind(if inherit_children { 1_i64 } else { 0_i64 })
    .bind(created_by_user_id.to_string())
    .bind(revoked_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn assert_device_permission(
    store: &PlatformStore,
    subject: &AuthorizationSubject,
    device_id: &str,
    expected: Option<ResourcePermission>,
) {
    assert_eq!(
        store.device_permission(subject, device_id).await.unwrap(),
        expected
    );
}

#[tokio::test]
async fn sqlite_authorization_subject_includes_the_users_tenant() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;

    assert_eq!(
        store.authorization_subject(USER_A).await.unwrap(),
        Some(subject(USER_A, TENANT_A, AccountClass::User))
    );
    assert_eq!(
        store.authorization_subject(USER_B).await.unwrap(),
        Some(subject(USER_B, TENANT_B, AccountClass::User))
    );
}

#[tokio::test]
async fn sqlite_resolves_direct_user_and_group_permissions() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;
    insert_device(pool, DIRECT_DEVICE_A, TENANT_A, None, None).await;
    insert_device(pool, GROUP_DEVICE_A, TENANT_A, None, None).await;
    insert_group_member(pool, TENANT_A, GROUP_A, OTHER_USER_A, USER_A).await;
    insert_permission(
        pool,
        "direct-user-manager",
        TENANT_A,
        Some(USER_A),
        None,
        None,
        Some(DIRECT_DEVICE_A),
        ResourcePermission::Manager,
        false,
        OTHER_USER_A,
        None,
    )
    .await;
    insert_permission(
        pool,
        "group-viewer",
        TENANT_A,
        None,
        Some(GROUP_A),
        None,
        Some(GROUP_DEVICE_A),
        ResourcePermission::Viewer,
        false,
        OTHER_USER_A,
        None,
    )
    .await;

    let user = subject(USER_A, TENANT_A, AccountClass::User);
    assert_device_permission(
        &store,
        &user,
        DIRECT_DEVICE_A,
        Some(ResourcePermission::Manager),
    )
    .await;
    assert_device_permission(
        &store,
        &user,
        GROUP_DEVICE_A,
        Some(ResourcePermission::Viewer),
    )
    .await;
}

#[tokio::test]
async fn sqlite_inherits_asset_permission_for_assets_and_devices() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;
    insert_asset(
        pool,
        ROOT_ASSET_A,
        TENANT_A,
        "root",
        None,
        Some(OTHER_USER_A),
    )
    .await;
    insert_asset(
        pool,
        CHILD_ASSET_A,
        TENANT_A,
        "child",
        Some(ROOT_ASSET_A),
        None,
    )
    .await;
    insert_device(
        pool,
        INHERITED_DEVICE_A,
        TENANT_A,
        Some(CHILD_ASSET_A),
        None,
    )
    .await;
    insert_permission(
        pool,
        "inherited-manager",
        TENANT_A,
        Some(USER_A),
        None,
        Some(ROOT_ASSET_A),
        None,
        ResourcePermission::Manager,
        true,
        OTHER_USER_A,
        None,
    )
    .await;

    let user = subject(USER_A, TENANT_A, AccountClass::User);
    assert_eq!(
        store.asset_permission(&user, CHILD_ASSET_A).await.unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_device_permission(
        &store,
        &user,
        INHERITED_DEVICE_A,
        Some(ResourcePermission::Manager),
    )
    .await;
}

#[tokio::test]
async fn sqlite_denies_revoked_permission() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;
    insert_device(pool, REVOKED_DEVICE_A, TENANT_A, None, None).await;
    insert_permission(
        pool,
        "revoked-manager",
        TENANT_A,
        Some(USER_A),
        None,
        None,
        Some(REVOKED_DEVICE_A),
        ResourcePermission::Manager,
        false,
        OTHER_USER_A,
        Some("2026-09-18T00:00:00Z"),
    )
    .await;

    assert_device_permission(
        &store,
        &subject(USER_A, TENANT_A, AccountClass::User),
        REVOKED_DEVICE_A,
        None,
    )
    .await;
}

#[tokio::test]
async fn sqlite_owner_permission_precedes_direct_permission() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;
    insert_device(pool, OWNED_DEVICE_A, TENANT_A, None, Some(USER_A)).await;
    insert_permission(
        pool,
        "owner-viewer",
        TENANT_A,
        Some(USER_A),
        None,
        None,
        Some(OWNED_DEVICE_A),
        ResourcePermission::Viewer,
        false,
        OTHER_USER_A,
        None,
    )
    .await;

    assert_device_permission(
        &store,
        &subject(USER_A, TENANT_A, AccountClass::User),
        OWNED_DEVICE_A,
        Some(ResourcePermission::Owner),
    )
    .await;
}

#[tokio::test]
async fn sqlite_denies_cross_tenant_user_and_unshared_legacy_admin() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;
    insert_device(pool, UNSHARED_DEVICE_A, TENANT_A, None, Some(OTHER_USER_A)).await;
    insert_device(pool, DEVICE_B, TENANT_B, None, Some(USER_B)).await;

    assert_device_permission(
        &store,
        &subject(USER_A, TENANT_A, AccountClass::User),
        DEVICE_B,
        None,
    )
    .await;
    assert_device_permission(
        &store,
        &subject(LEGACY_ADMIN_A, TENANT_A, AccountClass::Admin),
        UNSHARED_DEVICE_A,
        None,
    )
    .await;
}

const fn ancestor_asset_id(depth: u128) -> Uuid {
    Uuid::from_u128(1_000 + depth)
}

#[tokio::test]
async fn sqlite_limits_asset_inheritance_to_64_ancestors() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    seed_identities(pool).await;

    for depth in (1..=65_u128).rev() {
        let parent_asset_id = (depth < 65).then(|| ancestor_asset_id(depth + 1));
        insert_asset(
            pool,
            ancestor_asset_id(depth),
            TENANT_A,
            &format!("ancestor-{depth}"),
            parent_asset_id,
            None,
        )
        .await;
    }
    insert_asset(
        pool,
        DEPTH_LEAF_ASSET_A,
        TENANT_A,
        "depth-leaf",
        Some(ancestor_asset_id(1)),
        None,
    )
    .await;
    insert_device(
        pool,
        DEPTH_DEVICE_A,
        TENANT_A,
        Some(DEPTH_LEAF_ASSET_A),
        None,
    )
    .await;
    insert_permission(
        pool,
        "depth-64-viewer",
        TENANT_A,
        Some(USER_A),
        None,
        Some(ancestor_asset_id(64)),
        None,
        ResourcePermission::Viewer,
        true,
        OTHER_USER_A,
        None,
    )
    .await;
    insert_permission(
        pool,
        "depth-65-manager",
        TENANT_A,
        Some(OTHER_USER_A),
        None,
        Some(ancestor_asset_id(65)),
        None,
        ResourcePermission::Manager,
        true,
        USER_A,
        None,
    )
    .await;

    assert_device_permission(
        &store,
        &subject(USER_A, TENANT_A, AccountClass::User),
        DEPTH_DEVICE_A,
        Some(ResourcePermission::Viewer),
    )
    .await;
    assert_device_permission(
        &store,
        &subject(OTHER_USER_A, TENANT_A, AccountClass::User),
        DEPTH_DEVICE_A,
        None,
    )
    .await;
}
