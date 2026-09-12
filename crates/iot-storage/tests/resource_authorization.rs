use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, AuthorizationRepository, AuthorizationSubject, PlatformStore, ResourcePermission,
};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

const USER_ID: Uuid = Uuid::from_u128(1);
const OTHER_USER_ID: Uuid = Uuid::from_u128(2);
const ROOT_ASSET_ID: Uuid = Uuid::from_u128(10);
const CHILD_ASSET_ID: Uuid = Uuid::from_u128(11);
const OWNED_ASSET_ID: Uuid = Uuid::from_u128(12);
const DEVICE_ASSET_ID: &str = "device-asset";
const DEVICE_DIRECT_ID: &str = "device-direct";
const DEVICE_OWNER_ID: &str = "device-owner";
const DEVICE_DELETED_ID: &str = "device-deleted";

struct AuthorizationFixtures {
    root_asset_id: Uuid,
    child_asset_id: Uuid,
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
         VALUES (?, 'root', NULL, ?), (?, 'child', ?, NULL), (?, 'owned', NULL, ?)",
    )
    .bind(ROOT_ASSET_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .bind(CHILD_ASSET_ID.to_string())
    .bind(ROOT_ASSET_ID.to_string())
    .bind(OWNED_ASSET_ID.to_string())
    .bind(USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, asset_id, owner_user_id)
         VALUES (?, ?, NULL), (?, NULL, ?), (?, NULL, ?), (?, ?, ?)",
    )
    .bind(DEVICE_ASSET_ID)
    .bind(CHILD_ASSET_ID.to_string())
    .bind(DEVICE_DIRECT_ID)
    .bind(OTHER_USER_ID.to_string())
    .bind(DEVICE_OWNER_ID)
    .bind(USER_ID.to_string())
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
    .bind(DEVICE_DELETED_ID)
    .bind(USER_ID.to_string())
    .bind(OTHER_USER_ID.to_string())
    .execute(pool)
    .await
    .unwrap();

    AuthorizationFixtures {
        root_asset_id: ROOT_ASSET_ID,
        child_asset_id: CHILD_ASSET_ID,
    }
}

async fn assert_contract(store: &PlatformStore, fixtures: AuthorizationFixtures) {
    let admin = user_subject(AccountClass::Admin);
    let user = user_subject(AccountClass::User);
    let system = user_subject(AccountClass::System);

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
            .device_permission(&user, DEVICE_DELETED_ID)
            .await
            .unwrap(),
        None
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
}

#[tokio::test]
async fn sqlite_resource_authorization_repository_matches_api_contract() {
    let (_directory, store) = sqlite_store().await;
    let fixtures = seed_sqlite(&store).await;
    assert_contract(&store, fixtures).await;
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
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:resource-authorization-test'))")
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
         VALUES ($1, 'root', NULL, $2), ($3, 'child', $1, NULL), ($4, 'owned', NULL, $5)",
    )
    .bind(ROOT_ASSET_ID)
    .bind(OTHER_USER_ID)
    .bind(CHILD_ASSET_ID)
    .bind(OWNED_ASSET_ID)
    .bind(USER_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO devices (device_id, asset_id, owner_user_id)
         VALUES ($1, $2, NULL), ($3, NULL, $4), ($5, NULL, $6), ($7, $2, $4)",
    )
    .bind(DEVICE_ASSET_ID)
    .bind(CHILD_ASSET_ID)
    .bind(DEVICE_DIRECT_ID)
    .bind(OTHER_USER_ID)
    .bind(DEVICE_OWNER_ID)
    .bind(USER_ID)
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
            ($10, 'device', $11, $3, 'manager', FALSE, 'active', $4)",
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
    .bind(DEVICE_DELETED_ID)
    .execute(pool)
    .await
    .unwrap();

    AuthorizationFixtures {
        root_asset_id: ROOT_ASSET_ID,
        child_asset_id: CHILD_ASSET_ID,
    }
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_resource_authorization_repository_matches_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let fixtures = seed_timescale(&store).await;
    assert_contract(&store, fixtures).await;
}
