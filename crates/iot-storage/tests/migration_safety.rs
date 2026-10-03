use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewResourcePermission, NewUserGroup, PermissionCreator, PlatformStore, ResourcePermission,
    TenantAuthorizationError,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool, Row, SqliteConnection};
use uuid::Uuid;

mod common;

const CREATOR_TENANT_A: Uuid = Uuid::from_u128(101);
const CREATOR_TENANT_B: Uuid = Uuid::from_u128(102);
const CREATOR_OWNER_A: Uuid = Uuid::from_u128(110);
const CREATOR_MEMBER_A: Uuid = Uuid::from_u128(111);
const CREATOR_USER_A: Uuid = Uuid::from_u128(112);
const CREATOR_TENANT_ACCOUNT_A: Uuid = Uuid::from_u128(113);
const CREATOR_TENANT_ACCOUNT_B: Uuid = Uuid::from_u128(123);
const CREATOR_ASSET_A: Uuid = Uuid::from_u128(130);
const CREATOR_DEVICE_A: &str = "creator-attribution-device-a";

fn test_tenant_id() -> uuid::Uuid {
    uuid::Uuid::from_u128(1)
}

async fn seed_test_tenant(store: &PlatformStore) {
    sqlx::query(
        "INSERT OR IGNORE INTO tenants (id, slug, status, metadata)
         VALUES (?, 'migration-safety', 'active', '{}')",
    )
    .bind(test_tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
}

#[test]
fn platform_store_owns_its_postgres_migration_source() {
    let migration = include_str!("../migrations/0001_platform.sql");
    let sqlite_source = include_str!("../src/schema/sqlite.rs");
    let (_, sqlite_schema) = sqlite_source
        .split_once("pub(crate) const SQLITE_SCHEMA: &str = r#\"")
        .expect("SQLite schema source must define SQLITE_SCHEMA");
    let (sqlite_schema, _) = sqlite_schema
        .split_once("\"#;")
        .expect("SQLite schema source must terminate SQLITE_SCHEMA");

    assert!(migration.contains("CREATE TABLE IF NOT EXISTS platform_schema"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS devices"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS telemetry"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS command_outbox"));
    assert!(migration.contains("tenant_id UUID NOT NULL"));
    assert!(migration.contains("FOREIGN KEY (device_id, tenant_id)"));
    assert!(migration.contains("FOREIGN KEY (gateway_device_id, tenant_id)"));
    assert!(migration.contains("gateway_topology_version INTEGER NOT NULL DEFAULT 0"));
    assert!(!migration.contains("ADD COLUMN IF NOT EXISTS gateway_topology_version"));
    assert!(!migration.contains("ALTER TABLE users ALTER COLUMN id SET DEFAULT"));
    assert!(!migration.contains("DROP CONSTRAINT IF EXISTS user_capabilities_capability_check"));
    assert!(!migration.contains("ADD CONSTRAINT telemetry_device_id_fkey"));
    for removed_identifier in [
        "default_app",
        "granted_apps",
        "user_app_grants",
        "api_access_tokens",
    ] {
        assert!(
            !migration.contains(removed_identifier),
            "Timescale fresh schema restored removed identifier {removed_identifier:?}"
        );
        assert!(
            !sqlite_schema.contains(removed_identifier),
            "SQLite fresh schema restored removed identifier {removed_identifier:?}"
        );
    }
}

#[test]
fn legacy_schema_paths_are_not_present() {
    let storage_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = storage_root
        .parent()
        .and_then(std::path::Path::parent)
        .expect("iot-storage must remain directly under the workspace crates directory");

    assert!(storage_root.join("migrations/0001_platform.sql").is_file());
    assert!(!workspace_root.join("db/migrations").exists());
    assert!(!workspace_root.join("crates/iot-sqldb-common").exists());
}

#[test]
fn timescale_audit_schema_rejects_truncate_without_role_configuration() {
    let migration = include_str!("../migrations/0001_platform.sql");

    assert!(migration.contains("CREATE TRIGGER audit_events_immutable_truncate"));
    assert!(migration.contains("BEFORE TRUNCATE ON audit_events"));
    assert!(
        migration.contains("FOR EACH STATEMENT EXECUTE FUNCTION prevent_audit_events_mutation();")
    );
    assert!(!migration.contains("CREATE ROLE"));
    assert!(!migration.contains("GRANT "));
}

#[tokio::test]
async fn sqlite_fresh_initialization_records_the_canonical_schema_version() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&sqlite_configuration(
        directory.path().join("platform.sqlite"),
    ))
    .await
    .unwrap();

    let marker = sqlx::query_as::<_, (i64, i64)>("SELECT singleton, version FROM platform_schema")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    assert_eq!(marker, (1, 8));
}

#[tokio::test]
async fn sqlite_open_rejects_an_unmarked_database_before_schema_initialization() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unmarked.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::query("CREATE TABLE leftover_state (id INTEGER PRIMARY KEY)")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path.clone())).await {
        Ok(_) => panic!("an unmarked database must not be initialized in place"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("leftover_state"));

    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    let initialized: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'platform_schema'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(initialized, 0);
}

#[tokio::test]
async fn sqlite_open_rejects_an_unmarked_database_with_only_a_view() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unmarked-view.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql("CREATE VIEW legacy_view AS SELECT 1 AS value;")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path.clone())).await {
        Ok(_) => panic!("an unmarked database containing a view must require reset"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("legacy_view"));

    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    let initialized: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'platform_schema'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(initialized, 0);
}

#[tokio::test]
async fn sqlite_open_rejects_an_unknown_schema_version_without_fallback() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unknown-version.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE platform_schema (
             singleton INTEGER PRIMARY KEY,
             version INTEGER NOT NULL
         );
         INSERT INTO platform_schema (singleton, version) VALUES (1, 2);",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path.clone())).await {
        Ok(_) => panic!("an unknown schema version must not be migrated in place"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("platform_schema"));

    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    let tenant_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tenants'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(tenant_table_exists, 0);
}

#[tokio::test]
async fn sqlite_open_rejects_a_marked_database_that_retains_legacy_schema() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = sqlite_configuration(directory.path().join("legacy-marker.sqlite"));
    let store = PlatformStore::open(&configuration).await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE api_access_tokens (
             role TEXT PRIMARY KEY,
             token_hash TEXT NOT NULL
         );",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    drop(store);

    let error = match PlatformStore::open(&configuration).await {
        Ok(_) => panic!("a marked database retaining legacy tables must require reset"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("api_access_tokens"));
}

#[tokio::test]
async fn sqlite_open_does_not_reapply_schema_to_a_marked_database_missing_a_table() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = sqlite_configuration(directory.path().join("missing-table.sqlite"));
    let store = PlatformStore::open(&configuration).await.unwrap();
    sqlx::query("DROP TABLE telemetry_rollups_1h")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    drop(store);

    let error = match PlatformStore::open(&configuration).await {
        Ok(_) => panic!("a marked database missing canonical tables must require reset"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("telemetry_rollups_1h"));

    let path = configuration.sqlite_path.as_ref().unwrap();
    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    let restored: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'telemetry_rollups_1h'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(restored, 0);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_a_marked_database_that_retains_legacy_schema() {
    let (database_url, connection) = isolated_timescale_connection().await;
    connection.close().await.unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url.clone()),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    };

    let store = PlatformStore::open(&configuration).await.unwrap();
    drop(store);

    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let marker =
        sqlx::query_as::<_, (i64, i64)>("SELECT singleton, version FROM iot_nano.platform_schema")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(marker, (1, 1));
    sqlx::raw_sql(
        "CREATE TABLE iot_nano.api_access_tokens (
             role TEXT PRIMARY KEY,
             token_hash TEXT NOT NULL
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let open_result = PlatformStore::open(&configuration).await;
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();

    let error = match open_result {
        Ok(_) => panic!("a marked database retaining legacy tables must require reset"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("api_access_tokens"));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_an_unmarked_platform_schema_with_only_a_view() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    sqlx::raw_sql(
        "CREATE SCHEMA iot_nano;
         CREATE VIEW iot_nano.legacy_view AS SELECT 1 AS value;",
    )
    .execute(&mut connection)
    .await
    .unwrap();

    let open_result = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await;
    let initialized: bool =
        sqlx::query_scalar("SELECT to_regclass('iot_nano.platform_schema') IS NOT NULL")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();

    let error = match open_result {
        Ok(_) => panic!("an unmarked platform schema containing a view must require reset"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("legacy_view"));
    assert!(!initialized);
}

fn sqlite_configuration(path: std::path::PathBuf) -> StorageConfiguration {
    StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    }
}

async fn isolated_timescale_connection() -> (String, PgConnection) {
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
    (database_url, connection)
}

async fn seed_creator_attribution_timescale_scope(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO tenants (id, slug, status)
         VALUES ($1, 'creator-tenant-a', 'active'), ($2, 'creator-tenant-b', 'active')",
    )
    .bind(CREATOR_TENANT_A)
    .bind(CREATOR_TENANT_B)
    .execute(pool)
    .await
    .unwrap();
    for (id, tenant_id, username) in [
        (CREATOR_OWNER_A, CREATOR_TENANT_A, "creator-owner-a"),
        (CREATOR_MEMBER_A, CREATOR_TENANT_A, "creator-member-a"),
        (CREATOR_USER_A, CREATOR_TENANT_A, "creator-user-a"),
    ] {
        sqlx::query(
            "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
             VALUES ($1, $2, $3, 'unused', 'viewer', 'user')",
        )
        .bind(id)
        .bind(tenant_id)
        .bind(username)
        .execute(pool)
        .await
        .unwrap();
    }
    for (id, tenant_id) in [
        (CREATOR_TENANT_ACCOUNT_A, CREATOR_TENANT_A),
        (CREATOR_TENANT_ACCOUNT_B, CREATOR_TENANT_B),
    ] {
        sqlx::query(
            "INSERT INTO tenant_accounts (
                 id, tenant_id, username, password_hash, status, credential_version
             ) VALUES ($1, $2, $1, 'unused', 'active', 1)",
        )
        .bind(id)
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO assets (id, tenant_id, name) VALUES ($1, $2, 'creator-asset-a')")
        .bind(CREATOR_ASSET_A)
        .bind(CREATOR_TENANT_A)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ($1, $2)")
        .bind(CREATOR_DEVICE_A)
        .bind(CREATOR_TENANT_A)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_open_rejects_pre_tenant_platform_schema_without_partial_migration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pre-tenant-platform.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE devices (
             device_id TEXT PRIMARY KEY,
             display_name TEXT
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path.clone())).await {
        Ok(_) => panic!("pre-tenant platform schema was accepted"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("devices"),
        "unexpected migration error: {error}"
    );

    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    let tenants_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tenants'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    let tenant_id_column_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pragma_table_info('devices') WHERE name = 'tenant_id'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();

    assert_eq!(tenants_table_exists, 0);
    assert_eq!(tenant_id_column_exists, 0);
}

#[tokio::test]
async fn sqlite_open_rejects_current_schema_with_legacy_default_app_column() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy-default-app.sqlite");
    let configuration = sqlite_configuration(path.clone());
    drop(PlatformStore::open(&configuration).await.unwrap());

    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    sqlx::query("ALTER TABLE users ADD COLUMN default_app TEXT")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&configuration).await {
        Ok(_) => panic!("legacy users.default_app column was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("users.default_app"));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_pre_tenant_platform_schema_without_partial_migration() {
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
    sqlx::raw_sql(
        "CREATE SCHEMA iot_nano;
         CREATE TABLE iot_nano.devices (
             device_id TEXT PRIMARY KEY,
             display_name TEXT
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();

    let error = match PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    {
        Ok(_) => panic!("pre-tenant platform schema was accepted"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("devices"),
        "unexpected migration error: {error}"
    );

    let tenants_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('iot_nano.tenants') IS NOT NULL")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let tenant_id_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'iot_nano'
               AND table_name = 'devices'
               AND column_name = 'tenant_id'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();

    assert!(!tenants_table_exists);
    assert!(!tenant_id_column_exists);
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_permission_creator_enforces_attribution_contract() {
    let (database_url, _connection) = isolated_timescale_connection().await;
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let pool = store.timescale_pool().unwrap();
    seed_creator_attribution_timescale_scope(pool).await;

    let tenant_account_permission = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: CREATOR_TENANT_A,
            subject_user_id: Some(CREATOR_MEMBER_A),
            subject_group_id: None,
            asset_id: None,
            device_id: Some(CREATOR_DEVICE_A.to_owned()),
            permission: ResourcePermission::Viewer,
            inherit_children: false,
            created_by: PermissionCreator::TenantAccount(CREATOR_TENANT_ACCOUNT_A),
        })
        .await
        .unwrap();
    let group = store
        .create_user_group(NewUserGroup {
            tenant_id: CREATOR_TENANT_A,
            owner_user_id: CREATOR_OWNER_A,
            name: "creator-operators".to_owned(),
            metadata: json!({}),
        })
        .await
        .unwrap();
    let user_permission = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: CREATOR_TENANT_A,
            subject_user_id: None,
            subject_group_id: Some(group.id),
            asset_id: Some(CREATOR_ASSET_A),
            device_id: None,
            permission: ResourcePermission::Manager,
            inherit_children: true,
            created_by: PermissionCreator::User(CREATOR_USER_A),
        })
        .await
        .unwrap();

    assert_eq!(
        tenant_account_permission.created_by,
        PermissionCreator::TenantAccount(CREATOR_TENANT_ACCOUNT_A)
    );
    assert_eq!(
        user_permission.created_by,
        PermissionCreator::User(CREATOR_USER_A)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT created_by_tenant_account_id
             FROM resource_permissions
             WHERE id = $1",
        )
        .bind(tenant_account_permission.id)
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(CREATOR_TENANT_ACCOUNT_A)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT created_by_user_id
             FROM resource_permissions
             WHERE id = $1",
        )
        .bind(user_permission.id)
        .fetch_one(pool)
        .await
        .unwrap(),
        Some(CREATOR_USER_A)
    );

    let cross_tenant_creator = store
        .create_resource_permission(NewResourcePermission {
            tenant_id: CREATOR_TENANT_A,
            subject_user_id: Some(CREATOR_MEMBER_A),
            subject_group_id: None,
            asset_id: None,
            device_id: Some(CREATOR_DEVICE_A.to_owned()),
            permission: ResourcePermission::Viewer,
            inherit_children: false,
            created_by: PermissionCreator::TenantAccount(CREATOR_TENANT_ACCOUNT_B),
        })
        .await;
    assert!(matches!(
        cross_tenant_creator,
        Err(TenantAuthorizationError::TenantAccountNotFound {
            tenant_id: CREATOR_TENANT_A,
            tenant_account_id: CREATOR_TENANT_ACCOUNT_B,
        })
    ));
    assert!(
        sqlx::query(
            "INSERT INTO resource_permissions (
                id, tenant_id, subject_user_id, asset_id, permission, inherit_children,
                created_by_user_id, created_by_tenant_account_id
             ) VALUES ($1, $2, $3, $4, 'viewer', FALSE, NULL, $5)",
        )
        .bind(Uuid::now_v7())
        .bind(CREATOR_TENANT_A)
        .bind(CREATOR_MEMBER_A)
        .bind(CREATOR_ASSET_A)
        .bind(CREATOR_TENANT_ACCOUNT_B)
        .execute(pool)
        .await
        .is_err()
    );

    for (created_by_user_id, created_by_tenant_account_id) in [
        (None, None),
        (Some(CREATOR_USER_A), Some(CREATOR_TENANT_ACCOUNT_A)),
    ] {
        let result = sqlx::query(
            "INSERT INTO resource_permissions (
                id, tenant_id, subject_user_id, asset_id, permission, inherit_children,
                created_by_user_id, created_by_tenant_account_id
             ) VALUES ($1, $2, $3, $4, 'viewer', FALSE, $5, $6)",
        )
        .bind(Uuid::now_v7())
        .bind(CREATOR_TENANT_A)
        .bind(CREATOR_MEMBER_A)
        .bind(CREATOR_ASSET_A)
        .bind(created_by_user_id)
        .bind(created_by_tenant_account_id)
        .execute(pool)
        .await;
        assert!(result.is_err());
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_permissions")
            .fetch_one(pool)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn sqlite_backup_preserves_committed_platform_data() {
    let directory = tempfile::tempdir().unwrap();
    let platform_path = directory.path().join("platform.sqlite");
    let store = PlatformStore::open(&sqlite_configuration(platform_path))
        .await
        .unwrap();
    seed_test_tenant(&store).await;
    store
        .register_device(test_tenant_id(), "backup-device")
        .await
        .unwrap();

    let backup_path = store.backup_sqlite().await.unwrap();
    assert!(backup_path.exists());
    drop(store);

    let backup = PlatformStore::open(&sqlite_configuration(backup_path))
        .await
        .unwrap();
    let device_count = sqlx::query("SELECT COUNT(*) AS count FROM devices WHERE device_id = ?")
        .bind("backup-device")
        .fetch_one(backup.sqlite_pool().unwrap())
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(device_count, 1);
}

#[tokio::test]
async fn sqlite_backup_is_a_coherent_snapshot_during_an_atomic_write() {
    let directory = tempfile::tempdir().unwrap();
    let platform_path = directory.path().join("platform.sqlite");
    let configuration = sqlite_configuration(platform_path);
    let store = PlatformStore::open(&configuration).await.unwrap();
    let device_id = "backup-snapshot-device";
    seed_test_tenant(&store).await;
    store
        .register_device(test_tenant_id(), device_id)
        .await
        .unwrap();
    sqlx::query("UPDATE devices SET display_name = 'before' WHERE device_id = ?")
        .bind(device_id)
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let command_id = uuid::Uuid::now_v7().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let mut transaction = store.sqlite_pool().unwrap().begin().await.unwrap();
    sqlx::query("UPDATE devices SET display_name = 'after' WHERE device_id = ?")
        .bind(device_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, ?, 'switch_on', '{}', 'one_way', ?, ?)",
    )
    .bind(&command_id)
    .bind(test_tenant_id().to_string())
    .bind(device_id)
    .bind(&now)
    .bind(&now)
    .execute(&mut *transaction)
    .await
    .unwrap();

    let backup_store = store.clone();
    let backup_path = tokio::spawn(async move { backup_store.backup_sqlite().await })
        .await
        .unwrap()
        .unwrap();
    transaction.commit().await.unwrap();

    let backup = PlatformStore::open(&sqlite_configuration(backup_path))
        .await
        .unwrap();
    let backup_display_name: Option<String> =
        sqlx::query_scalar("SELECT display_name FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(backup.sqlite_pool().unwrap())
            .await
            .unwrap();
    let backup_command_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox WHERE id = ?")
            .bind(&command_id)
            .fetch_one(backup.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(backup_display_name.as_deref(), Some("before"));
    assert_eq!(backup_command_count, 0);

    let primary_display_name: Option<String> =
        sqlx::query_scalar("SELECT display_name FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let primary_command_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM command_outbox WHERE id = ?")
            .bind(&command_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(primary_display_name.as_deref(), Some("after"));
    assert_eq!(primary_command_count, 1);
}

#[tokio::test]
async fn sqlite_open_rejects_pre_tenant_asset_schema_without_mutating_data() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("duplicate-root-assets.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE assets (
             id TEXT PRIMARY KEY,
             name TEXT NOT NULL,
             asset_profile_id TEXT,
             parent_asset_id TEXT,
             owner_user_id TEXT,
             metadata TEXT NOT NULL DEFAULT '{}',
             created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
             updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
             UNIQUE (parent_asset_id, name)
         );
         INSERT INTO assets (id, name) VALUES
             ('duplicate-root-one', 'Duplicate root'),
             ('duplicate-root-two', 'Duplicate root');",
    )
    .execute(&mut connection)
    .await
    .unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path)).await {
        Ok(_) => panic!("pre-tenant platform schema was accepted"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("assets"),
        "unexpected migration error: {error}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assets
             WHERE parent_asset_id IS NULL AND name = 'Duplicate root'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_pre_tenant_asset_schema_without_mutating_data() {
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
    sqlx::query("CREATE SCHEMA iot_nano")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("SET search_path TO iot_nano, public")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE assets (
             id UUID PRIMARY KEY,
             name TEXT NOT NULL,
             parent_asset_id UUID
         );
         INSERT INTO assets (id, name) VALUES
             ('00000000-0000-0000-0000-000000000001', 'Duplicate Timescale root'),
             ('00000000-0000-0000-0000-000000000002', 'Duplicate Timescale root');",
    )
    .execute(&mut connection)
    .await
    .unwrap();

    let error = match PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    {
        Ok(_) => panic!("pre-tenant platform schema was accepted"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("assets"),
        "unexpected migration error: {error}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM assets
             WHERE parent_asset_id IS NULL AND name = 'Duplicate Timescale root'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        2
    );
}
