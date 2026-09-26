use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, CreateManagementUser, ManagementUserError, ManagementUserRepository,
    ManagementUserRole, PlatformStore, UpdateManagementUser, UserCapability,
};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

mod common;

struct TimescaleTestLock {
    _connection: PgConnection,
}

const TEST_TENANT_ID: &str = "00000000-0000-0000-0000-000000000001";

fn test_tenant_id() -> Uuid {
    Uuid::parse_str(TEST_TENANT_ID).unwrap()
}

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-users.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata) VALUES (?, 'test', 'active', '{}')",
    )
    .bind(TEST_TENANT_ID)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store)
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
    sqlx::query(
        "INSERT INTO tenants (id, slug, status, metadata) VALUES ($1, 'test', 'active', '{}'::jsonb)",
    )
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
}

fn user_creation(username: &str) -> CreateManagementUser {
    CreateManagementUser {
        tenant_id: test_tenant_id(),
        username: username.to_owned(),
        password_hash: "stored-password-hash".to_owned(),
    }
}

#[tokio::test]
async fn sqlite_management_user_repository_creates_users_without_application_contracts() {
    let (_directory, store) = sqlite_store().await;

    let created = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();

    assert_eq!(created.username, "alice");
    assert_eq!(created.role, ManagementUserRole::Viewer);
    assert_eq!(created.account_class, AccountClass::User);
    assert_eq!(
        created.capabilities,
        [UserCapability::ClaimDevices, UserCapability::CreateAssets]
    );
    assert_eq!(
        ManagementUserRepository::list_management_users(&store, test_tenant_id())
            .await
            .unwrap(),
        [created]
    );
}

#[tokio::test]
async fn sqlite_management_user_repository_replaces_capabilities_and_clears_them_for_admins() {
    let (_directory, store) = sqlite_store().await;
    let created = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();

    let updated = ManagementUserRepository::replace_management_user_capabilities(
        &store,
        test_tenant_id(),
        "alice",
        vec![
            UserCapability::CreateDevices,
            UserCapability::ControlDevices,
            UserCapability::ShareOwnedResources,
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        updated.capabilities,
        [
            UserCapability::ControlDevices,
            UserCapability::CreateDevices,
            UserCapability::ShareOwnedResources,
        ]
    );
    assert!(
        ManagementUserRepository::user_has_management_capability(
            &store,
            test_tenant_id(),
            created.id,
            UserCapability::ControlDevices,
        )
        .await
        .unwrap()
    );

    let promoted = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(promoted.account_class, AccountClass::Admin);
    assert!(promoted.capabilities.is_empty());
}

#[tokio::test]
async fn sqlite_management_user_repository_protects_account_class_and_last_admin_invariants() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'system', 'stored-password-hash', 'viewer', 'system')",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(TEST_TENANT_ID)
    .execute(pool)
    .await
    .unwrap();

    let system_error = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "system",
        UpdateManagementUser {
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        system_error,
        ManagementUserError::SystemUserImmutable
    ));

    let _alice = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    let alice = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(alice.account_class, AccountClass::Admin);

    let last_admin_error = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        last_admin_error,
        ManagementUserError::LastAdministrator
    ));
}

#[tokio::test]
async fn sqlite_management_user_repository_reports_typed_input_errors() {
    let (_directory, store) = sqlite_store().await;

    let invalid_username = ManagementUserRepository::create_management_user(
        &store,
        user_creation("not a valid username"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_username,
        ManagementUserError::InvalidUsername(_)
    ));

    ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    let conflict = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap_err();
    assert!(matches!(conflict, ManagementUserError::UsernameConflict(_)));

    let missing = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "missing",
        UpdateManagementUser { role: None },
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, ManagementUserError::UserNotFound));
}

#[tokio::test]
async fn sqlite_management_user_repository_rejects_invalid_stored_role_or_account_class() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'bad-role', 'stored-password-hash', 'operator', 'user')",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(TEST_TENANT_ID)
    .execute(pool)
    .await
    .unwrap();

    let error = ManagementUserRepository::list_management_users(&store, test_tenant_id())
        .await
        .unwrap_err();
    assert!(matches!(error, ManagementUserError::InvalidStoredRole(_)));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL"]
async fn timescale_management_user_repository_matches_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let created = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    assert_eq!(created.account_class, AccountClass::User);
    assert_eq!(
        created.capabilities,
        [UserCapability::ClaimDevices, UserCapability::CreateAssets]
    );

    let updated = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.role, ManagementUserRole::Admin);
    assert!(updated.capabilities.is_empty());
}
