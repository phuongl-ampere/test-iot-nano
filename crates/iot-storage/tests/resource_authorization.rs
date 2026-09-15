use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, AuthorizationRepository, AuthorizationSubject, PlatformStore, ResourcePermission,
};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

mod common;

const USER_ID: Uuid = Uuid::from_u128(1);
const OTHER_USER_ID: Uuid = Uuid::from_u128(2);
const ROOT_ASSET_ID: Uuid = Uuid::from_u128(10);
const CHILD_ASSET_ID: Uuid = Uuid::from_u128(11);
const OWNED_ASSET_ID: Uuid = Uuid::from_u128(12);
const UNSHARED_ASSET_ID: Uuid = Uuid::from_u128(13);
const PENDING_ASSET_ID: Uuid = Uuid::from_u128(14);
const DEVICE_ASSET_ID: &str = "device-asset";
const DEVICE_DIRECT_ID: &str = "device-direct";
const DEVICE_OWNER_ID: &str = "device-owner";
const DEVICE_UNSHARED_ID: &str = "device-unshared";
const DEVICE_PENDING_ID: &str = "device-pending";
const DEVICE_DELETED_ID: &str = "device-deleted";
const ANCESTOR_CAP_LEAF_ID: Uuid = Uuid::from_u128(5_000);

struct AuthorizationFixtures {
    root_asset_id: Uuid,
    child_asset_id: Uuid,
    unshared_asset_id: Uuid,
    pending_asset_id: Uuid,
}

fn user_subject(account_class: AccountClass) -> AuthorizationSubject {
    AuthorizationSubject {
        user_id: USER_ID,
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

async fn seed_sqlite(store: &PlatformStore) -> AuthorizationFixtures {
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES (?, 'authorization-user', 'unused', 'viewer', 'user'),
                (?, 'authorization-other', 'unused', 'viewer', 'user')",
    )
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, name, parent_asset_id, owner_user_id)
         VALUES
            (?, 'root', NULL, ?),
            (?, 'child', ?, NULL),
            (?, 'owned', NULL, ?),
            (?, 'unshared', NULL, ?),
            (?, 'pending', NULL, ?)",
    )
    .bind(ROOT_ASSET_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(CHILD_ASSET_ID.to_string())
    .bind(ROOT_ASSET_ID.to_string())
    .bind(OWNED_ASSET_ID.to_string())
    .bind(USER_ID.to_string())
    .bind(UNSHARED_ASSET_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(PENDING_ASSET_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, asset_id, owner_user_id)
         VALUES
            (?, ?, NULL),
            (?, NULL, ?),
            (?, NULL, ?),
            (?, ?, NULL),
            (?, NULL, ?),
            (?, ?, ?)",
    )
    .bind(DEVICE_ASSET_ID)
    .bind(CHILD_ASSET_ID.to_string())
    .bind(DEVICE_DIRECT_ID)
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_OWNER_ID)
    .bind(USER_ID.to_string())
    .bind(DEVICE_UNSHARED_ID)
    .bind(UNSHARED_ASSET_ID.to_string())
    .bind(DEVICE_PENDING_ID)
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_DELETED_ID)
    .bind(CHILD_ASSET_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET deleted_at = CURRENT_TIMESTAMP WHERE device_id = ?")
        .bind(DEVICE_DELETED_ID)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares
            (id, resource_type, resource_id, target_user_id, permission,
             inherit_children, state, created_by_user_id)
         VALUES
            ('asset-inherited', 'asset', ?, ?, 'viewer', 1, 'active', ?),
            ('asset-stronger', 'asset', ?, ?, 'manager', 1, 'active', ?),
            ('device-direct', 'device', ?, ?, 'controller', 0, 'active', ?),
            ('device-pending', 'device', ?, ?, 'manager', 0, 'pending', ?),
            ('asset-pending-only', 'asset', ?, ?, 'manager', 1, 'pending', ?),
            ('device-pending-only', 'device', ?, ?, 'manager', 0, 'pending', ?),
            ('device-deleted', 'device', ?, ?, 'manager', 0, 'active', ?)",
    )
    .bind(ROOT_ASSET_ID.to_string())
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(CHILD_ASSET_ID.to_string())
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_DIRECT_ID)
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_DIRECT_ID)
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(PENDING_ASSET_ID.to_string())
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_PENDING_ID)
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_DELETED_ID)
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    AuthorizationFixtures {
        root_asset_id: ROOT_ASSET_ID,
        child_asset_id: CHILD_ASSET_ID,
        unshared_asset_id: UNSHARED_ASSET_ID,
        pending_asset_id: PENDING_ASSET_ID,
    }
}

async fn assert_approved_authorization_contract(
    store: &PlatformStore,
    fixtures: &AuthorizationFixtures,
) {
    let admin = user_subject(AccountClass::Admin);
    let user = user_subject(AccountClass::User);
    let system = user_subject(AccountClass::System);

    assert_eq!(
        store.authorization_subject(USER_ID).await.unwrap(),
        Some(user)
    );
    assert_eq!(
        store.authorization_subject(OTHER_USER_ID).await.unwrap(),
        Some(AuthorizationSubject {
            user_id: OTHER_USER_ID,
            account_class: AccountClass::User,
        })
    );

    assert_eq!(
        store
            .device_permission(&admin, DEVICE_DIRECT_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        store
            .asset_permission(&admin, fixtures.root_asset_id)
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        store
            .device_permission(&user, DEVICE_DIRECT_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Controller)
    );
    assert_eq!(
        store
            .device_permission(&user, DEVICE_OWNER_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        store.asset_permission(&user, OWNED_ASSET_ID).await.unwrap(),
        Some(ResourcePermission::Owner)
    );
    assert_eq!(
        store
            .asset_permission(&user, fixtures.child_asset_id)
            .await
            .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        store
            .device_permission(&user, DEVICE_ASSET_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Manager)
    );
    assert_eq!(
        store
            .device_permission(&system, "missing-device")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        AuthorizationRepository::device_permission(store, &user, "missing-device")
            .await
            .unwrap(),
        None
    );
    let page = store.list_authorized_devices(&user, None, 2).await.unwrap();
    assert_eq!(
        page.iter()
            .map(|device| device.device_id.as_str())
            .collect::<Vec<_>>(),
        [DEVICE_ASSET_ID, DEVICE_DIRECT_ID]
    );
    assert_eq!(
        store
            .authorized_device(&user, DEVICE_ASSET_ID)
            .await
            .unwrap()
            .unwrap()
            .device_id,
        DEVICE_ASSET_ID
    );
    assert_eq!(
        AuthorizationRepository::authorized_device(store, &user, DEVICE_DIRECT_ID)
            .await
            .unwrap()
            .unwrap()
            .device_id,
        DEVICE_DIRECT_ID
    );
    assert!(
        store
            .authorized_device(&user, DEVICE_UNSHARED_ID)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .authorized_device(&user, DEVICE_DELETED_ID)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .authorized_device(&user, "missing-device")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_resource_authorization_repository_matches_approved_storage_contract() {
    let (_directory, store) = sqlite_store().await;
    let fixtures = seed_sqlite(&store).await;
    assert_approved_authorization_contract(&store, &fixtures).await;
}

async fn assert_existing_unshared_resources_have_no_permission(
    store: &PlatformStore,
    fixtures: &AuthorizationFixtures,
) {
    let user = user_subject(AccountClass::User);
    assert_eq!(
        store
            .asset_permission(&user, fixtures.unshared_asset_id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .device_permission(&user, DEVICE_UNSHARED_ID)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_resource_authorization_returns_none_for_existing_unshared_resources() {
    let (_directory, store) = sqlite_store().await;
    let fixtures = seed_sqlite(&store).await;
    assert_existing_unshared_resources_have_no_permission(&store, &fixtures).await;
}

#[tokio::test]
async fn sqlite_authorized_device_page_uses_keyset_order_and_resource_grants() {
    let (_directory, store) = sqlite_store().await;
    let _fixtures = seed_sqlite(&store).await;
    let subject = user_subject(AccountClass::User);

    let first = store
        .list_authorized_devices(&subject, None, 2)
        .await
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|device| device.device_id.as_str())
            .collect::<Vec<_>>(),
        [DEVICE_ASSET_ID, DEVICE_DIRECT_ID]
    );

    let second = store
        .list_authorized_devices(&subject, Some(DEVICE_DIRECT_ID), 2)
        .await
        .unwrap();
    assert_eq!(
        second
            .iter()
            .map(|device| device.device_id.as_str())
            .collect::<Vec<_>>(),
        [DEVICE_OWNER_ID]
    );
}

async fn assert_pending_only_shares_have_no_permission(
    store: &PlatformStore,
    fixtures: &AuthorizationFixtures,
) {
    let user = user_subject(AccountClass::User);
    assert_eq!(
        store
            .asset_permission(&user, fixtures.pending_asset_id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .device_permission(&user, DEVICE_PENDING_ID)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_resource_authorization_ignores_pending_only_shares() {
    let (_directory, store) = sqlite_store().await;
    let fixtures = seed_sqlite(&store).await;
    assert_pending_only_shares_have_no_permission(&store, &fixtures).await;
}

async fn assert_deleted_device_direct_share_has_no_permission(store: &PlatformStore) {
    let user = user_subject(AccountClass::User);
    assert_eq!(
        store
            .device_permission(&user, DEVICE_DELETED_ID)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_resource_authorization_denies_deleted_device_direct_active_share() {
    let (_directory, store) = sqlite_store().await;
    seed_sqlite(&store).await;
    assert_deleted_device_direct_share_has_no_permission(&store).await;
}

const fn ancestor_cap_asset_id(depth: u128) -> Uuid {
    Uuid::from_u128(5_000 + depth)
}

async fn seed_sqlite_ancestor_cap(store: &PlatformStore) {
    let pool = store.sqlite_pool().unwrap();
    for depth in (1..=65_u128).rev() {
        let parent_asset_id = if depth == 65 {
            None
        } else {
            Some(ancestor_cap_asset_id(depth + 1).to_string())
        };
        sqlx::query("INSERT INTO assets (id, name, parent_asset_id) VALUES (?, ?, ?)")
            .bind(ancestor_cap_asset_id(depth).to_string())
            .bind(format!("ancestor-cap-{depth}"))
            .bind(parent_asset_id)
            .execute(pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO assets (id, name, parent_asset_id) VALUES (?, 'cap-leaf', ?)")
        .bind(ANCESTOR_CAP_LEAF_ID.to_string())
        .bind(ancestor_cap_asset_id(1).to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares
            (id, resource_type, resource_id, target_user_id, permission,
             inherit_children, state, created_by_user_id)
         VALUES
            ('cap-included', 'asset', ?, ?, 'viewer', 1, 'active', ?),
            ('cap-excluded', 'asset', ?, ?, 'manager', 1, 'active', ?)",
    )
    .bind(ancestor_cap_asset_id(64).to_string())
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(ancestor_cap_asset_id(65).to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn assert_64_ancestor_cap(store: &PlatformStore) {
    let included_subject = user_subject(AccountClass::User);
    let excluded_subject = AuthorizationSubject {
        user_id: OTHER_USER_ID,
        account_class: AccountClass::User,
    };
    assert_eq!(
        store
            .asset_permission(&included_subject, ANCESTOR_CAP_LEAF_ID)
            .await
            .unwrap(),
        Some(ResourcePermission::Viewer)
    );
    assert_eq!(
        store
            .asset_permission(&excluded_subject, ANCESTOR_CAP_LEAF_ID)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sqlite_resource_authorization_limits_inheritance_to_64_ancestors() {
    let (_directory, store) = sqlite_store().await;
    seed_sqlite(&store).await;
    seed_sqlite_ancestor_cap(&store).await;
    assert_64_ancestor_cap(&store).await;
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
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
}

async fn seed_timescale(store: &PlatformStore) -> AuthorizationFixtures {
    let pool = store.timescale_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class)
         VALUES ($1, 'authorization-user', 'unused', 'viewer', 'user'),
                ($2, 'authorization-other', 'unused', 'viewer', 'user')",
    )
    .bind(USER_ID)
    .bind(OTHER_USER_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO assets (id, name, parent_asset_id, owner_user_id)
         VALUES
            ($1, 'root', NULL, $2),
            ($3, 'child', $1, NULL),
            ($4, 'owned', NULL, $5),
            ($6, 'unshared', NULL, $2),
            ($7, 'pending', NULL, $2)",
    )
    .bind(ROOT_ASSET_ID)
    .bind(OTHER_USER_ID)
    .bind(CHILD_ASSET_ID)
    .bind(OWNED_ASSET_ID)
    .bind(USER_ID)
    .bind(UNSHARED_ASSET_ID)
    .bind(PENDING_ASSET_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, asset_id, owner_user_id)
         VALUES
            ($1, $2, NULL),
            ($3, NULL, $4),
            ($5, NULL, $6),
            ($7, $8, NULL),
            ($9, NULL, $4),
            ($10, $2, $4)",
    )
    .bind(DEVICE_ASSET_ID)
    .bind(CHILD_ASSET_ID)
    .bind(DEVICE_DIRECT_ID)
    .bind(OTHER_USER_ID)
    .bind(DEVICE_OWNER_ID)
    .bind(USER_ID)
    .bind(DEVICE_UNSHARED_ID)
    .bind(UNSHARED_ASSET_ID)
    .bind(DEVICE_PENDING_ID)
    .bind(DEVICE_DELETED_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE devices SET deleted_at = now() WHERE device_id = $1")
        .bind(DEVICE_DELETED_ID)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares
            (id, resource_type, resource_id, target_user_id, permission,
             inherit_children, state, created_by_user_id)
         VALUES
            ($1, 'asset', $2::text, $3, 'viewer', TRUE, 'active', $4),
            ($5, 'asset', $6::text, $3, 'manager', TRUE, 'active', $4),
            ($7, 'device', $8, $3, 'controller', FALSE, 'active', $4),
            ($9, 'device', $8, $3, 'manager', FALSE, 'pending', $4),
            ($10, 'asset', $11::text, $3, 'manager', TRUE, 'pending', $4),
            ($12, 'device', $13, $3, 'manager', FALSE, 'pending', $4),
            ($14, 'device', $15, $3, 'manager', FALSE, 'active', $4)",
    )
    .bind(Uuid::from_u128(101))
    .bind(ROOT_ASSET_ID)
    .bind(USER_ID)
    .bind(OTHER_USER_ID)
    .bind(Uuid::from_u128(102))
    .bind(CHILD_ASSET_ID)
    .bind(Uuid::from_u128(103))
    .bind(DEVICE_DIRECT_ID)
    .bind(Uuid::from_u128(104))
    .bind(Uuid::from_u128(105))
    .bind(PENDING_ASSET_ID)
    .bind(Uuid::from_u128(106))
    .bind(DEVICE_PENDING_ID)
    .bind(Uuid::from_u128(107))
    .bind(DEVICE_DELETED_ID)
    .execute(pool)
    .await
    .unwrap();

    AuthorizationFixtures {
        root_asset_id: ROOT_ASSET_ID,
        child_asset_id: CHILD_ASSET_ID,
        unshared_asset_id: UNSHARED_ASSET_ID,
        pending_asset_id: PENDING_ASSET_ID,
    }
}

async fn seed_timescale_ancestor_cap(store: &PlatformStore) {
    let pool = store.timescale_pool().unwrap();
    for depth in (1..=65_u128).rev() {
        let parent_asset_id = if depth == 65 {
            None
        } else {
            Some(ancestor_cap_asset_id(depth + 1))
        };
        sqlx::query("INSERT INTO assets (id, name, parent_asset_id) VALUES ($1, $2, $3)")
            .bind(ancestor_cap_asset_id(depth))
            .bind(format!("ancestor-cap-{depth}"))
            .bind(parent_asset_id)
            .execute(pool)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO assets (id, name, parent_asset_id) VALUES ($1, 'cap-leaf', $2)")
        .bind(ANCESTOR_CAP_LEAF_ID)
        .bind(ancestor_cap_asset_id(1))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resource_shares
            (id, resource_type, resource_id, target_user_id, permission,
             inherit_children, state, created_by_user_id)
         VALUES
            ($1, 'asset', $2::text, $3, 'viewer', TRUE, 'active', $4),
            ($5, 'asset', $6::text, $4, 'manager', TRUE, 'active', $3)",
    )
    .bind(Uuid::from_u128(201))
    .bind(ancestor_cap_asset_id(64))
    .bind(USER_ID)
    .bind(OTHER_USER_ID)
    .bind(Uuid::from_u128(202))
    .bind(ancestor_cap_asset_id(65))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_resource_authorization_repository_matches_approved_storage_contract() {
    let (_lock, store) = timescale_store().await;
    let fixtures = seed_timescale(&store).await;
    assert_approved_authorization_contract(&store, &fixtures).await;
    assert_existing_unshared_resources_have_no_permission(&store, &fixtures).await;
    assert_pending_only_shares_have_no_permission(&store, &fixtures).await;
    assert_deleted_device_direct_share_has_no_permission(&store).await;
    seed_timescale_ancestor_cap(&store).await;
    assert_64_ancestor_cap(&store).await;
}
