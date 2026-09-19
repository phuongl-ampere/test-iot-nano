use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AccountClass, BUILT_IN_USER_WORKSPACE, CreateManagementUser, ManagementUserError,
    ManagementUserRepository, ManagementUserRole, PlatformStore, UpdateManagementUser,
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
    seed_sqlite_applications(store.sqlite_pool().unwrap(), test_tenant_id()).await;
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
    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES ($1, 'test', 'active', '{}'::jsonb)")
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    seed_timescale_applications(store.timescale_pool().unwrap(), test_tenant_id()).await;
    (
        TimescaleTestLock {
            _connection: connection,
        },
        store,
    )
}

async fn seed_sqlite_applications(pool: &sqlx::SqlitePool, tenant_id: Uuid) {
    for app_id in ["fleet", "powermonitor", "reports", "blocked"] {
        sqlx::query(
            "INSERT INTO applications (
                app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
             ) VALUES (?, ?, 'frontend', ?, ?, '[]', 1)",
        )
        .bind(app_id)
        .bind(tenant_id.to_string())
        .bind(format!("https://example.test/{app_id}"))
        .bind(format!("{app_id}-client"))
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_timescale_applications(pool: &sqlx::PgPool, tenant_id: Uuid) {
    for app_id in ["fleet", "powermonitor", "reports", "blocked"] {
        sqlx::query(
            "INSERT INTO applications (
                app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
             ) VALUES ($1, $2, 'frontend', $3, $4, '[]'::jsonb, TRUE)",
        )
        .bind(app_id)
        .bind(tenant_id)
        .bind(format!("https://example.test/{app_id}"))
        .bind(format!("{app_id}-client"))
        .execute(pool)
        .await
        .unwrap();
    }
}

fn user_creation(username: &str) -> CreateManagementUser {
    CreateManagementUser {
        tenant_id: test_tenant_id(),
        username: username.to_owned(),
        password_hash: "stored-password-hash".to_owned(),
        default_app: "/apps/powermonitor".to_owned(),
        granted_apps: vec!["fleet".to_owned(), "powermonitor".to_owned()],
    }
}

#[tokio::test]
async fn sqlite_schema_rejects_cross_tenant_user_app_grants() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let other_tenant_id = Uuid::now_v7();
    let user_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'other', 'active')")
        .bind(other_tenant_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'grant-user', 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO applications (
            app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
         ) VALUES (?, ?, 'frontend', 'https://example.test/other', ?, '[]', 1)",
    )
    .bind("other-tenant-app")
    .bind(other_tenant_id.to_string())
    .bind("other-tenant-app-client")
    .execute(pool)
    .await
    .unwrap();

    let error =
        sqlx::query("INSERT INTO user_app_grants (user_id, tenant_id, app_key) VALUES (?, ?, ?)")
            .bind(user_id.to_string())
            .bind(test_tenant_id().to_string())
            .bind("other-tenant-app")
            .execute(pool)
            .await
            .unwrap_err();
    assert!(
        error.to_string().contains("FOREIGN KEY constraint failed"),
        "{error}"
    );
}

#[tokio::test]
async fn sqlite_management_user_repository_rejects_missing_granted_apps_before_user_creation() {
    let (_directory, store) = sqlite_store().await;

    let error = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            default_app: "/apps/unregistered".to_owned(),
            granted_apps: vec!["unregistered".to_owned()],
            ..user_creation("missing-granted-app")
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ManagementUserError::InvalidGrantedApps));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE tenant_id = ? AND username = ?")
            .bind(test_tenant_id().to_string())
            .bind("missing-granted-app")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn sqlite_management_user_repository_rejects_cross_tenant_granted_apps_before_user_creation()
{
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let other_tenant_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'grant-other-tenant', 'active')",
    )
    .bind(other_tenant_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO applications (
            app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
         ) VALUES (?, ?, 'frontend', 'https://example.test/other-grant', ?, '[]', 1)",
    )
    .bind("other-tenant-grant-app")
    .bind(other_tenant_id.to_string())
    .bind("other-tenant-grant-client")
    .execute(pool)
    .await
    .unwrap();

    let error = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            default_app: "/apps/other-tenant-grant-app".to_owned(),
            granted_apps: vec!["other-tenant-grant-app".to_owned()],
            ..user_creation("cross-tenant-granted-app")
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ManagementUserError::InvalidGrantedApps));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE tenant_id = ? AND username = ?")
            .bind(test_tenant_id().to_string())
            .bind("cross-tenant-granted-app")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn sqlite_management_user_repository_preserves_grants_when_an_update_references_a_missing_app()
 {
    let (_directory, store) = sqlite_store().await;
    let created = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();

    let error = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/unregistered".to_owned(),
            granted_apps: vec!["unregistered".to_owned()],
            role: None,
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ManagementUserError::InvalidGrantedApps));
    let current = ManagementUserRepository::list_management_users(&store, test_tenant_id())
        .await
        .unwrap();
    assert_eq!(current, [created]);
}

#[tokio::test]
async fn sqlite_management_user_repository_creates_and_lists_users() {
    let (_directory, store) = sqlite_store().await;

    let created = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    assert_eq!(created.username, "alice");
    assert_eq!(created.role, ManagementUserRole::Viewer);
    assert_eq!(created.default_app, "/apps/powermonitor");
    assert_eq!(created.granted_apps, ["fleet", "powermonitor"]);

    let listed = ManagementUserRepository::list_management_users(&store, test_tenant_id())
        .await
        .unwrap();
    assert_eq!(listed, [created]);
}

#[tokio::test]
async fn sqlite_management_user_repository_updates_a_username_and_replaces_grants() {
    let (_directory, store) = sqlite_store().await;
    ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();

    let updated = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["reports".to_owned(), "fleet".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.role, ManagementUserRole::Admin);
    assert_eq!(updated.default_app, "/apps/fleet");
    assert_eq!(updated.granted_apps, ["fleet", "reports"]);

    let grants: Vec<String> = sqlx::query_scalar(
        "SELECT app_key
         FROM user_app_grants
         WHERE user_id = ? AND tenant_id = ?
         ORDER BY app_key",
    )
    .bind(updated.id.to_string())
    .bind(test_tenant_id().to_string())
    .fetch_all(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(grants, ["fleet", "reports"]);
}

#[tokio::test]
async fn sqlite_management_user_role_changes_update_account_class_atomically() {
    let (_directory, store) = sqlite_store().await;
    ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    ManagementUserRepository::create_management_user(&store, user_creation("bob"))
        .await
        .unwrap();

    let alice = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(alice.role, ManagementUserRole::Admin);
    assert_eq!(alice.account_class, AccountClass::Admin);

    ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "bob",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    let alice = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned()],
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap();
    assert_eq!(alice.role, ManagementUserRole::Viewer);
    assert_eq!(alice.account_class, AccountClass::User);

    let stored: (String, String) =
        sqlx::query_as("SELECT role, account_class FROM users WHERE username = 'alice'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(stored, ("viewer".to_owned(), "user".to_owned()));
}

#[tokio::test]
async fn sqlite_management_user_repository_rejects_duplicate_granted_apps() {
    let (_directory, store) = sqlite_store().await;

    let error = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            tenant_id: test_tenant_id(),
            username: "alice".to_owned(),
            password_hash: "stored-password-hash".to_owned(),
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned(), "powermonitor".to_owned()],
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ManagementUserError::InvalidGrantedApps));
}

#[tokio::test]
async fn sqlite_management_user_repository_keeps_builtin_workspace_users_grant_free() {
    let (_directory, store) = sqlite_store().await;

    let workspace_user = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            tenant_id: test_tenant_id(),
            username: "workspace-user".to_owned(),
            password_hash: "stored-password-hash".to_owned(),
            default_app: BUILT_IN_USER_WORKSPACE.to_owned(),
            granted_apps: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert_eq!(workspace_user.default_app, BUILT_IN_USER_WORKSPACE);
    assert!(workspace_user.granted_apps.is_empty());

    let create_with_grant = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            tenant_id: test_tenant_id(),
            username: "workspace-with-grant".to_owned(),
            password_hash: "stored-password-hash".to_owned(),
            default_app: BUILT_IN_USER_WORKSPACE.to_owned(),
            granted_apps: vec!["fleet".to_owned()],
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_with_grant,
        ManagementUserError::InvalidGrantedApps
    ));

    let external_user =
        ManagementUserRepository::create_management_user(&store, user_creation("alice"))
            .await
            .unwrap();
    let update_with_grant = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: BUILT_IN_USER_WORKSPACE.to_owned(),
            granted_apps: vec!["fleet".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        update_with_grant,
        ManagementUserError::InvalidGrantedApps
    ));
    let alice = ManagementUserRepository::list_management_users(&store, test_tenant_id())
        .await
        .unwrap()
        .into_iter()
        .find(|user| user.username == "alice")
        .unwrap();
    assert_eq!(alice, external_user);
}

#[tokio::test]
async fn sqlite_management_user_repository_returns_typed_input_conflict_and_not_found_errors() {
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

    let invalid_app = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            default_app: "/apps/not valid".to_owned(),
            ..user_creation("alice")
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        invalid_app,
        ManagementUserError::InvalidDefaultApp(_)
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
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, ManagementUserError::UserNotFound));
}

#[tokio::test]
async fn sqlite_management_user_repository_protects_system_and_administrator_invariants_atomically()
{
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();
    let system_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (
            id, tenant_id, username, password_hash, role, account_class, default_app
         ) VALUES (?, ?, 'system', 'stored-password-hash', 'admin', 'system', '/apps/powermonitor')",
    )
    .bind(system_id.to_string())
    .bind(TEST_TENANT_ID)
    .execute(pool)
    .await
    .unwrap();

    let system_error = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "system",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        system_error,
        ManagementUserError::SystemUserImmutable
    ));

    ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    let alice = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(alice.role, ManagementUserRole::Admin);

    sqlx::query("DELETE FROM users WHERE username = 'system'")
        .execute(pool)
        .await
        .unwrap();
    let last_admin = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned()],
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(last_admin, ManagementUserError::LastAdministrator));

    sqlx::raw_sql(
        "CREATE TRIGGER management_user_grant_failure
         BEFORE INSERT ON user_app_grants
         WHEN NEW.app_key = 'blocked'
         BEGIN
             SELECT RAISE(ABORT, 'blocked management grant');
         END;",
    )
    .execute(pool)
    .await
    .unwrap();
    let storage_error = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned(), "blocked".to_owned()],
            role: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(storage_error, ManagementUserError::Storage { .. }));

    let alice_after_failure =
        ManagementUserRepository::list_management_users(&store, test_tenant_id())
            .await
            .unwrap()
            .into_iter()
            .find(|user| user.username == "alice")
            .unwrap();
    assert_eq!(alice_after_failure.role, ManagementUserRole::Admin);
    assert_eq!(alice_after_failure.default_app, "/apps/powermonitor");
    assert_eq!(alice_after_failure.granted_apps, ["powermonitor"]);
}

#[tokio::test]
async fn sqlite_management_user_repository_rejects_invalid_stored_roles_and_schema_rejects_classes()
{
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();

    sqlx::query(
        "INSERT INTO users (
            id, tenant_id, username, password_hash, role, account_class, default_app
         ) VALUES (?, ?, 'invalid-role', 'stored-password-hash', 'operator', 'user', '/apps/powermonitor')",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(TEST_TENANT_ID)
    .execute(pool)
    .await
    .unwrap();
    let invalid_role = ManagementUserRepository::list_management_users(&store, test_tenant_id())
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_role,
        ManagementUserError::InvalidStoredRole(ref role) if role == "operator"
    ));

    let invalid_class = sqlx::query(
        "INSERT INTO users (
            id, tenant_id, username, password_hash, role, account_class, default_app
         ) VALUES (?, ?, 'invalid-class', 'stored-password-hash', 'viewer', 'service', '/apps/powermonitor')",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(TEST_TENANT_ID)
    .execute(pool)
    .await
    .unwrap_err();
    assert!(
        invalid_class
            .to_string()
            .contains("CHECK constraint failed")
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_user_repository_matches_sqlite_contract() {
    let (_lock, store) = timescale_store().await;
    let pool = store.timescale_pool().unwrap();

    let created = ManagementUserRepository::create_management_user(&store, user_creation("alice"))
        .await
        .unwrap();
    assert_eq!(created.role, ManagementUserRole::Viewer);
    assert_eq!(created.granted_apps, ["fleet", "powermonitor"]);

    let workspace_user = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            tenant_id: test_tenant_id(),
            username: "workspace-user".to_owned(),
            password_hash: "stored-password-hash".to_owned(),
            default_app: BUILT_IN_USER_WORKSPACE.to_owned(),
            granted_apps: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert_eq!(workspace_user.default_app, BUILT_IN_USER_WORKSPACE);
    assert!(workspace_user.granted_apps.is_empty());

    let create_with_grant = ManagementUserRepository::create_management_user(
        &store,
        CreateManagementUser {
            tenant_id: test_tenant_id(),
            username: "workspace-with-grant".to_owned(),
            password_hash: "stored-password-hash".to_owned(),
            default_app: BUILT_IN_USER_WORKSPACE.to_owned(),
            granted_apps: vec!["fleet".to_owned()],
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        create_with_grant,
        ManagementUserError::InvalidGrantedApps
    ));

    let update_with_grant = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: BUILT_IN_USER_WORKSPACE.to_owned(),
            granted_apps: vec!["fleet".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        update_with_grant,
        ManagementUserError::InvalidGrantedApps
    ));
    let alice_after_invalid_update =
        ManagementUserRepository::list_management_users(&store, test_tenant_id())
            .await
            .unwrap()
            .into_iter()
            .find(|user| user.username == "alice")
            .unwrap();
    assert_eq!(alice_after_invalid_update, created);

    let updated = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned(), "reports".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    assert_eq!(updated.role, ManagementUserRole::Admin);
    assert_eq!(updated.account_class, AccountClass::Admin);
    assert_eq!(updated.default_app, "/apps/fleet");
    assert_eq!(updated.granted_apps, ["fleet", "reports"]);

    ManagementUserRepository::create_management_user(&store, user_creation("bob"))
        .await
        .unwrap();
    let bob = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "bob",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap();
    let demoted = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned(), "reports".to_owned()],
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap();
    assert_eq!(demoted.role, ManagementUserRole::Viewer);
    assert_eq!(demoted.account_class, AccountClass::User);

    let duplicate_apps = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned(), "fleet".to_owned()],
            role: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        duplicate_apps,
        ManagementUserError::InvalidGrantedApps
    ));

    let last_admin = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "bob",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned()],
            role: Some(ManagementUserRole::Viewer),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(last_admin, ManagementUserError::LastAdministrator));

    sqlx::query(
        "INSERT INTO users (
            id, tenant_id, username, password_hash, role, account_class, default_app
         ) VALUES ($1, $2, 'system', 'stored-password-hash', 'viewer', 'system', '/apps/powermonitor')",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(test_tenant_id())
    .execute(pool)
    .await
    .unwrap();
    let system = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "system",
        UpdateManagementUser {
            default_app: "/apps/fleet".to_owned(),
            granted_apps: vec!["fleet".to_owned()],
            role: Some(ManagementUserRole::Admin),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(system, ManagementUserError::SystemUserImmutable));

    sqlx::raw_sql(
        "CREATE FUNCTION reject_management_user_grant() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.app_key = 'blocked' THEN
                 RAISE EXCEPTION 'blocked management grant';
             END IF;
             RETURN NEW;
         END;
         $$;
         CREATE TRIGGER reject_management_user_grant_trigger
         BEFORE INSERT ON user_app_grants
         FOR EACH ROW EXECUTE FUNCTION reject_management_user_grant();",
    )
    .execute(pool)
    .await
    .unwrap();
    let rollback = ManagementUserRepository::update_management_user(
        &store,
        test_tenant_id(),
        "alice",
        UpdateManagementUser {
            default_app: "/apps/powermonitor".to_owned(),
            granted_apps: vec!["powermonitor".to_owned(), "blocked".to_owned()],
            role: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(rollback, ManagementUserError::Storage { .. }));
    let alice_after_rollback =
        ManagementUserRepository::list_management_users(&store, test_tenant_id())
            .await
            .unwrap()
            .into_iter()
            .find(|user| user.username == "alice")
            .unwrap();
    assert_eq!(alice_after_rollback.role, ManagementUserRole::Viewer);
    assert_eq!(alice_after_rollback.account_class, AccountClass::User);
    assert_eq!(alice_after_rollback.default_app, "/apps/fleet");
    assert_eq!(alice_after_rollback.granted_apps, ["fleet", "reports"]);

    let listed = ManagementUserRepository::list_management_users(&store, test_tenant_id())
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|user| user.username.as_str())
            .collect::<Vec<_>>(),
        ["alice", "bob", "system", "workspace-user"]
    );
    assert_eq!(listed[0], demoted);
    assert_eq!(listed[1], bob);
}
