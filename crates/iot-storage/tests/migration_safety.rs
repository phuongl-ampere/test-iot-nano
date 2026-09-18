use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    NewResourcePermission, NewUserGroup, PermissionCreator, PlatformStore, ResourcePermission,
    TenantAuthorizationError,
};
use serde_json::json;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Row, SqliteConnection};
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
    let storage_source = include_str!("../src/lib.rs");

    assert!(migration.contains("CREATE TABLE IF NOT EXISTS devices"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS telemetry"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS command_outbox"));
    assert!(migration.contains("tenant_id UUID NOT NULL"));
    assert!(migration.contains("FOREIGN KEY (device_id, tenant_id)"));
    assert!(migration.contains("FOREIGN KEY (gateway_device_id, tenant_id)"));
    assert!(migration.contains("gateway_topology_version INTEGER NOT NULL DEFAULT 0"));
    assert!(migration.contains("ADD COLUMN IF NOT EXISTS gateway_topology_version"));
    assert!(!migration.contains("ADD CONSTRAINT telemetry_device_id_fkey"));
    assert!(!storage_source.contains("services/iot-nano-api/migrations"));
    assert!(!storage_source.contains("services/iot-nano-core/migrations"));
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
            "INSERT INTO tenant_accounts (id, tenant_id, password_hash, status, credential_version)
             VALUES ($1, $2, 'unused', 'active', 1)",
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

async fn assert_timescale_reset_gate(
    connection: &mut PgConnection,
    database_url: String,
    expected_table: &str,
) {
    let open_result = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await;
    let migration_started: bool =
        sqlx::query_scalar("SELECT to_regclass('iot_nano.system_accounts') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    let error = match open_result {
        Ok(store) => {
            drop(store);
            None
        }
        Err(error) => Some(error),
    };
    common::reset_timescale_schema(connection).await.unwrap();

    let error = error.expect("partially tenant-scoped schema was accepted");
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains(expected_table),
        "unexpected migration error: {error}"
    );
    assert!(!migration_started);
}

async fn create_timescale_command_reset_gate_schema(
    connection: &mut PgConnection,
    command_outbox: &str,
) {
    let schema = format!(
        "CREATE SCHEMA iot_nano;
         SET search_path TO iot_nano, public;
         CREATE TABLE tenants (id UUID PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         {command_outbox}"
    );
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(connection)
        .await
        .unwrap();
}

async fn create_timescale_notification_reset_gate_schema(
    connection: &mut PgConnection,
    notification_outbox: &str,
) {
    let schema = format!(
        "CREATE SCHEMA iot_nano;
         SET search_path TO iot_nano, public;
         CREATE TABLE tenants (id UUID PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE alert_rules (
             id UUID NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             device_id TEXT,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE alert_incidents (
             id UUID NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             rule_id UUID NOT NULL,
             device_id TEXT NOT NULL,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (rule_id, tenant_id)
                 REFERENCES alert_rules(id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         {notification_outbox}"
    );
    sqlx::raw_sql(AssertSqlSafe(schema))
        .execute(connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_pre_migration_backup_preserves_the_legacy_source_and_is_reused_on_retry() {
    let directory = tempfile::tempdir().unwrap();
    let platform_path = directory.path().join("pre-migration.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", platform_path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "PRAGMA journal_mode = DELETE;
         PRAGMA user_version = 0;
         CREATE TABLE preserved_before_migration (value TEXT NOT NULL);
         INSERT INTO preserved_before_migration (value) VALUES ('before-migration');",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let configuration = sqlite_configuration(platform_path.clone());
    let first_backup = PlatformStore::backup_sqlite_before_migration(&configuration)
        .await
        .unwrap()
        .expect("legacy SQLite database must be backed up");
    let second_backup = PlatformStore::backup_sqlite_before_migration(&configuration)
        .await
        .unwrap()
        .expect("failed migration retry must retain the original backup");
    assert_eq!(first_backup, second_backup);

    let mut source =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rw", platform_path.display()))
            .await
            .unwrap();
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut source)
        .await
        .unwrap();
    assert_eq!(journal_mode, "delete");

    let mut backup =
        SqliteConnection::connect(&format!("sqlite://{}?mode=ro", first_backup.display()))
            .await
            .unwrap();
    let value: String = sqlx::query_scalar("SELECT value FROM preserved_before_migration LIMIT 1")
        .fetch_one(&mut backup)
        .await
        .unwrap();
    assert_eq!(value, "before-migration");
}

#[tokio::test]
async fn sqlite_current_schema_does_not_create_a_pre_migration_backup() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = sqlite_configuration(directory.path().join("current.sqlite"));
    let store = PlatformStore::open(&configuration).await.unwrap();
    drop(store);

    assert!(
        PlatformStore::backup_sqlite_before_migration(&configuration)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_open_upgrades_legacy_audit_changes_to_an_object_constraint() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = sqlite_configuration(directory.path().join("audit-events-upgrade.sqlite"));
    let store = PlatformStore::open(&configuration).await.unwrap();
    seed_test_tenant(&store).await;
    let pool = store.sqlite_pool().unwrap();
    sqlx::raw_sql(
        "DROP TABLE audit_events;
         CREATE TABLE audit_events (
             id TEXT PRIMARY KEY,
             tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
             occurred_at TEXT NOT NULL,
             actor_principal_kind TEXT NOT NULL
                 CHECK (actor_principal_kind IN ('system_account', 'tenant_account', 'user')),
             actor_principal_id TEXT NOT NULL,
             action TEXT NOT NULL,
             target_type TEXT NOT NULL,
             target_id TEXT NOT NULL,
             changes TEXT NOT NULL CHECK (json_valid(changes))
         );
         INSERT INTO audit_events (
             id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
             action, target_type, target_id, changes
         ) VALUES (
             'legacy-audit-event', '00000000-0000-0000-0000-000000000001',
             '2026-01-01T00:00:00Z', 'user', '00000000-0000-0000-0000-000000000001',
             'permission.granted', 'resource_permission', 'legacy-target', '{}'
         );",
    )
    .execute(pool)
    .await
    .unwrap();
    drop(store);

    let reopened = PlatformStore::open(&configuration).await.unwrap();
    let pool = reopened.sqlite_pool().unwrap();
    let schema: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'audit_events'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert!(
        schema.contains("json_type(changes) = 'object'"),
        "audit_events schema did not enforce object changes: {schema}"
    );
    assert!(
        sqlx::query(
            "INSERT INTO audit_events (
                id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                action, target_type, target_id, changes
             ) VALUES (?, ?, '2026-01-01T00:00:01Z', 'user', ?,
                       'permission.granted', 'resource_permission', 'array-target', '[]')",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(test_tenant_id().to_string())
        .bind(test_tenant_id().to_string())
        .execute(pool)
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT changes FROM audit_events WHERE id = 'legacy-audit-event'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        "{}"
    );
}

#[tokio::test]
async fn sqlite_open_adds_gateway_topology_version_to_an_existing_devices_table() {
    let directory = tempfile::tempdir().unwrap();
    let configuration =
        sqlite_configuration(directory.path().join("gateway-topology-version.sqlite"));
    let store = PlatformStore::open(&configuration).await.unwrap();
    sqlx::query("ALTER TABLE devices DROP COLUMN gateway_topology_version")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    drop(store);

    let reopened = PlatformStore::open(&configuration).await.unwrap();
    seed_test_tenant(&reopened).await;
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ('migration-device', ?)")
        .bind(test_tenant_id().to_string())
        .execute(reopened.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT gateway_topology_version
             FROM devices
             WHERE device_id = 'migration-device' AND tenant_id = ?",
        )
        .bind(test_tenant_id().to_string())
        .fetch_one(reopened.sqlite_pool().unwrap())
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_open_rejects_pre_creator_attribution_schema_without_partial_migration() {
    let older_schema_replacements = [
        (
            "tenant-accounts",
            "tenant_accounts",
            "PRAGMA foreign_keys = OFF;
             DROP TABLE tenant_accounts;
             CREATE TABLE tenant_accounts (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
                 password_hash TEXT NOT NULL,
                 status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
                 credential_version INTEGER NOT NULL,
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );",
        ),
        (
            "resource-permissions",
            "resource_permissions",
            "PRAGMA foreign_keys = OFF;
             DROP TABLE resource_permissions;
             CREATE TABLE resource_permissions (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
                 subject_user_id TEXT,
                 subject_group_id TEXT,
                 asset_id TEXT,
                 device_id TEXT,
                 permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
                 inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
                 created_by_user_id TEXT NOT NULL,
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 revoked_at TEXT,
                 CHECK (
                     (subject_user_id IS NOT NULL AND subject_group_id IS NULL)
                     OR (subject_user_id IS NULL AND subject_group_id IS NOT NULL)
                 ),
                 CHECK (
                     (asset_id IS NOT NULL AND device_id IS NULL)
                     OR (asset_id IS NULL AND device_id IS NOT NULL)
                 ),
                 CHECK (device_id IS NULL OR inherit_children = 0),
                 FOREIGN KEY (subject_user_id, tenant_id)
                     REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (subject_group_id, tenant_id)
                     REFERENCES user_groups(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (asset_id, tenant_id)
                     REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (device_id, tenant_id)
                     REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (created_by_user_id, tenant_id)
                     REFERENCES users(id, tenant_id) ON DELETE RESTRICT
             );",
        ),
        (
            "resource-permissions-non-null-user-creator",
            "resource_permissions",
            "PRAGMA foreign_keys = OFF;
             DROP TABLE resource_permissions;
             CREATE TABLE resource_permissions (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
                 subject_user_id TEXT,
                 subject_group_id TEXT,
                 asset_id TEXT,
                 device_id TEXT,
                 permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
                 inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
                 created_by_user_id TEXT NOT NULL,
                 created_by_tenant_account_id TEXT,
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 revoked_at TEXT,
                 CHECK (
                     (subject_user_id IS NOT NULL AND subject_group_id IS NULL)
                     OR (subject_user_id IS NULL AND subject_group_id IS NOT NULL)
                 ),
                 CHECK (
                     (asset_id IS NOT NULL AND device_id IS NULL)
                     OR (asset_id IS NULL AND device_id IS NOT NULL)
                 ),
                 CHECK (
                     (created_by_user_id IS NOT NULL AND created_by_tenant_account_id IS NULL)
                     OR (created_by_user_id IS NULL AND created_by_tenant_account_id IS NOT NULL)
                 ),
                 CHECK (device_id IS NULL OR inherit_children = 0),
                 FOREIGN KEY (subject_user_id, tenant_id)
                     REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (subject_group_id, tenant_id)
                     REFERENCES user_groups(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (asset_id, tenant_id)
                     REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (device_id, tenant_id)
                     REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (created_by_user_id, tenant_id)
                     REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (created_by_tenant_account_id, tenant_id)
                     REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
             );",
        ),
        (
            "resource-permissions-non-null-tenant-account-creator",
            "resource_permissions",
            "PRAGMA foreign_keys = OFF;
             DROP TABLE resource_permissions;
             CREATE TABLE resource_permissions (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
                 subject_user_id TEXT,
                 subject_group_id TEXT,
                 asset_id TEXT,
                 device_id TEXT,
                 permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
                 inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
                 created_by_user_id TEXT,
                 created_by_tenant_account_id TEXT NOT NULL,
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 revoked_at TEXT,
                 CHECK (
                     (subject_user_id IS NOT NULL AND subject_group_id IS NULL)
                     OR (subject_user_id IS NULL AND subject_group_id IS NOT NULL)
                 ),
                 CHECK (
                     (asset_id IS NOT NULL AND device_id IS NULL)
                     OR (asset_id IS NULL AND device_id IS NOT NULL)
                 ),
                 CHECK (
                     (created_by_user_id IS NOT NULL AND created_by_tenant_account_id IS NULL)
                     OR (created_by_user_id IS NULL AND created_by_tenant_account_id IS NOT NULL)
                 ),
                 CHECK (device_id IS NULL OR inherit_children = 0),
                 FOREIGN KEY (subject_user_id, tenant_id)
                     REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (subject_group_id, tenant_id)
                     REFERENCES user_groups(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (asset_id, tenant_id)
                     REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (device_id, tenant_id)
                     REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (created_by_user_id, tenant_id)
                     REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
                 FOREIGN KEY (created_by_tenant_account_id, tenant_id)
                     REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
             );",
        ),
    ];

    for (name, expected_table, replacement) in older_schema_replacements {
        let directory = tempfile::tempdir().unwrap();
        let configuration =
            sqlite_configuration(directory.path().join(format!("{name}-schema.sqlite")));
        let store = PlatformStore::open(&configuration).await.unwrap();
        drop(store);

        let path = configuration.sqlite_path.as_ref().unwrap();
        let mut connection =
            SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
                .await
                .unwrap();
        sqlx::raw_sql(AssertSqlSafe(replacement))
            .execute(&mut connection)
            .await
            .unwrap();
        let schema_before: String = sqlx::query_scalar(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'table' AND name = ?",
        )
        .bind(expected_table)
        .fetch_one(&mut connection)
        .await
        .unwrap();
        connection.close().await.unwrap();

        let error = match PlatformStore::open(&configuration).await {
            Ok(_) => panic!("pre-creator-attribution {expected_table} schema was accepted"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("reset the development database"),
            "unexpected migration error for {expected_table}: {error}"
        );
        assert!(
            error.to_string().contains(expected_table),
            "unexpected migration error for {expected_table}: {error}"
        );

        let mut connection =
            SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
                .await
                .unwrap();
        let schema_after: String = sqlx::query_scalar(
            "SELECT sql
             FROM sqlite_master
             WHERE type = 'table' AND name = ?",
        )
        .bind(expected_table)
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert_eq!(
            schema_after, schema_before,
            "open partially accepted the old {expected_table} schema"
        );
    }
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
async fn sqlite_open_rejects_partially_tenant_scoped_alert_schema() {
    let cases = [
        (
            "nullable-alert-rule-tenant",
            "alert_rules",
            "CREATE TABLE tenants (id TEXT PRIMARY KEY);
             CREATE TABLE alert_rules (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT,
                 device_id TEXT
             );",
        ),
        (
            "unscoped-notification-dedupe",
            "notification_outbox",
            "CREATE TABLE tenants (id TEXT PRIMARY KEY);
             CREATE TABLE notification_outbox (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL,
                 incident_id TEXT NOT NULL,
                 dedupe_key TEXT NOT NULL UNIQUE,
                 state TEXT NOT NULL,
                 next_attempt_at TEXT NOT NULL
             );",
        ),
    ];

    for (name, table, schema) in cases {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("{name}.sqlite"));
        let mut connection =
            SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
                .await
                .unwrap();
        sqlx::raw_sql(schema)
            .execute(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();

        let error = match PlatformStore::open(&sqlite_configuration(path)).await {
            Ok(_) => panic!("partially tenant-scoped {table} schema was accepted"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("reset the development database"),
            "unexpected migration error for {table}: {error}"
        );
        assert!(
            error.to_string().contains(table),
            "unexpected migration error for {table}: {error}"
        );
    }
}

#[tokio::test]
async fn sqlite_open_rejects_alert_rules_without_a_tenant_root_foreign_key() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing-alert-rule-tenant-fk.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE tenants (id TEXT PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE alert_rules (
             id TEXT NOT NULL,
             tenant_id TEXT NOT NULL,
             device_id TEXT,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path)).await {
        Ok(_) => panic!("alert rules without a tenant root foreign key were accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("alert_rules"));
}

#[tokio::test]
async fn sqlite_open_rejects_command_outbox_without_a_tenant_root_foreign_key() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing-command-tenant-fk.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE tenants (id TEXT PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE command_outbox (
             id TEXT PRIMARY KEY,
             tenant_id TEXT NOT NULL,
             device_id TEXT NOT NULL,
             mode TEXT NOT NULL,
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path)).await {
        Ok(_) => panic!("command outbox without a tenant root foreign key was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("command_outbox"));
}

#[tokio::test]
async fn sqlite_open_rejects_command_outbox_without_a_tenant_device_foreign_key() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing-command-device-fk.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE tenants (id TEXT PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE command_outbox (
             id TEXT PRIMARY KEY,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             device_id TEXT NOT NULL,
             mode TEXT NOT NULL
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path)).await {
        Ok(_) => panic!("command outbox without a tenant device foreign key was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("command_outbox"));
}

#[tokio::test]
async fn sqlite_open_rejects_notification_schema_with_a_global_dedupe_key() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("global-notification-dedupe.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE tenants (id TEXT PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE alert_rules (
             id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             device_id TEXT,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE alert_incidents (
             id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             rule_id TEXT NOT NULL,
             device_id TEXT NOT NULL,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (rule_id, tenant_id)
                 REFERENCES alert_rules(id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE notification_outbox (
             id TEXT PRIMARY KEY,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             incident_id TEXT NOT NULL,
             dedupe_key TEXT NOT NULL,
             state TEXT NOT NULL,
             next_attempt_at TEXT NOT NULL,
             UNIQUE (tenant_id, dedupe_key),
             UNIQUE (dedupe_key, state),
             FOREIGN KEY (incident_id, tenant_id)
                 REFERENCES alert_incidents(id, tenant_id)
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path)).await {
        Ok(_) => panic!("notification schema with a global dedupe key was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("notification_outbox"));
}

#[tokio::test]
async fn sqlite_open_rejects_notification_schema_with_a_partial_dedupe_key() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("partial-notification-dedupe.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE tenants (id TEXT PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE alert_rules (
             id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             device_id TEXT,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE alert_incidents (
             id TEXT NOT NULL,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             rule_id TEXT NOT NULL,
             device_id TEXT NOT NULL,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (rule_id, tenant_id)
                 REFERENCES alert_rules(id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE notification_outbox (
             id TEXT PRIMARY KEY,
             tenant_id TEXT NOT NULL REFERENCES tenants(id),
             incident_id TEXT NOT NULL,
             dedupe_key TEXT NOT NULL,
             state TEXT NOT NULL,
             next_attempt_at TEXT NOT NULL,
             UNIQUE (tenant_id, dedupe_key),
             FOREIGN KEY (incident_id, tenant_id)
                 REFERENCES alert_incidents(id, tenant_id)
         );
         CREATE UNIQUE INDEX notification_outbox_partial_dedupe_key_index
             ON notification_outbox (tenant_id, dedupe_key)
             WHERE state = 'pending';",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match PlatformStore::open(&sqlite_configuration(path)).await {
        Ok(_) => panic!("notification schema with a partial dedupe key was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("notification_outbox"));
}

#[tokio::test]
async fn sqlite_open_rejects_notification_schema_with_an_expression_dedupe_key() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = sqlite_configuration(directory.path().join("expression-dedupe.sqlite"));
    let store = PlatformStore::open(&configuration).await.unwrap();
    sqlx::raw_sql(
        "CREATE UNIQUE INDEX notification_outbox_expression_dedupe_key_index
         ON notification_outbox (lower(dedupe_key));",
    )
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    drop(store);

    let error = match PlatformStore::open(&configuration).await {
        Ok(_) => panic!("notification schema with an expression dedupe key was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("reset the development database"));
    assert!(error.to_string().contains("notification_outbox"));
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
    sqlx::query(
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
async fn timescale_open_rejects_pre_creator_attribution_tenant_account_schema() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    sqlx::raw_sql(AssertSqlSafe(
        "CREATE SCHEMA iot_nano;
         SET search_path TO iot_nano, public;
         CREATE TABLE tenants (id UUID PRIMARY KEY);
         CREATE TABLE tenant_accounts (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
             password_hash TEXT NOT NULL,
             status TEXT NOT NULL,
             credential_version INTEGER NOT NULL,
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
         );",
    ))
    .execute(&mut connection)
    .await
    .unwrap();

    assert_timescale_reset_gate(&mut connection, database_url, "tenant_accounts").await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_pre_creator_attribution_resource_permission_schema() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    sqlx::raw_sql(AssertSqlSafe(
        "CREATE SCHEMA iot_nano;
         SET search_path TO iot_nano, public;
         CREATE TABLE tenants (id UUID PRIMARY KEY);
         CREATE TABLE tenant_accounts (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
             password_hash TEXT NOT NULL,
             status TEXT NOT NULL,
             credential_version INTEGER NOT NULL,
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             UNIQUE (id, tenant_id)
         );
         CREATE TABLE users (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
             UNIQUE (id, tenant_id)
         );
         CREATE TABLE resource_permissions (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
             subject_user_id UUID,
             subject_group_id UUID,
             asset_id UUID,
             device_id TEXT,
             permission TEXT NOT NULL,
             inherit_children BOOLEAN NOT NULL DEFAULT FALSE,
             created_by_user_id UUID NOT NULL,
             created_by_tenant_account_id UUID,
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             revoked_at TIMESTAMPTZ,
             CHECK (
                 (created_by_user_id IS NOT NULL AND created_by_tenant_account_id IS NULL)
                 OR (created_by_user_id IS NULL AND created_by_tenant_account_id IS NOT NULL)
             ),
             FOREIGN KEY (created_by_user_id, tenant_id)
                 REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
             FOREIGN KEY (created_by_tenant_account_id, tenant_id)
                 REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
         );",
    ))
    .execute(&mut connection)
    .await
    .unwrap();

    assert_timescale_reset_gate(&mut connection, database_url, "resource_permissions").await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_pre_creator_attribution_non_null_tenant_account_creator_schema() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    sqlx::raw_sql(AssertSqlSafe(
        "CREATE SCHEMA iot_nano;
         SET search_path TO iot_nano, public;
         CREATE TABLE tenants (id UUID PRIMARY KEY);
         CREATE TABLE tenant_accounts (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
             password_hash TEXT NOT NULL,
             status TEXT NOT NULL,
             credential_version INTEGER NOT NULL,
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             UNIQUE (id, tenant_id)
         );
         CREATE TABLE users (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
             UNIQUE (id, tenant_id)
         );
         CREATE TABLE resource_permissions (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
             subject_user_id UUID,
             subject_group_id UUID,
             asset_id UUID,
             device_id TEXT,
             permission TEXT NOT NULL,
             inherit_children BOOLEAN NOT NULL DEFAULT FALSE,
             created_by_user_id UUID,
             created_by_tenant_account_id UUID NOT NULL,
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             revoked_at TIMESTAMPTZ,
             CHECK (
                 (created_by_user_id IS NOT NULL AND created_by_tenant_account_id IS NULL)
                 OR (created_by_user_id IS NULL AND created_by_tenant_account_id IS NOT NULL)
             ),
             FOREIGN KEY (created_by_user_id, tenant_id)
                 REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
             FOREIGN KEY (created_by_tenant_account_id, tenant_id)
                 REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
         );",
    ))
    .execute(&mut connection)
    .await
    .unwrap();

    assert_timescale_reset_gate(&mut connection, database_url, "resource_permissions").await;
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
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_notification_schema_with_a_global_dedupe_key() {
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
         SET search_path TO iot_nano, public;
         CREATE TABLE tenants (id UUID PRIMARY KEY);
         CREATE TABLE devices (
             device_id TEXT NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             PRIMARY KEY (device_id, tenant_id)
         );
         CREATE TABLE alert_rules (
             id UUID NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             device_id TEXT,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE alert_rule_event_evaluations (
             tenant_id UUID NOT NULL,
             rule_id UUID NOT NULL,
             event_at TIMESTAMPTZ NOT NULL,
             device_id TEXT NOT NULL,
             boot_id UUID NOT NULL,
             sequence BIGINT NOT NULL,
             PRIMARY KEY (tenant_id, rule_id, event_at, device_id, boot_id, sequence),
             FOREIGN KEY (tenant_id) REFERENCES tenants(id),
             FOREIGN KEY (rule_id, tenant_id) REFERENCES alert_rules(id, tenant_id),
             FOREIGN KEY (device_id, tenant_id) REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE alert_incidents (
             id UUID NOT NULL,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             rule_id UUID NOT NULL,
             device_id TEXT NOT NULL,
             PRIMARY KEY (id, tenant_id),
             FOREIGN KEY (rule_id, tenant_id) REFERENCES alert_rules(id, tenant_id),
             FOREIGN KEY (device_id, tenant_id) REFERENCES devices(device_id, tenant_id)
         );
         CREATE TABLE notification_outbox (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             incident_id UUID NOT NULL,
             dedupe_key TEXT NOT NULL,
             state TEXT NOT NULL,
             UNIQUE (id, tenant_id),
             UNIQUE (tenant_id, dedupe_key),
             UNIQUE (dedupe_key, state),
             FOREIGN KEY (incident_id, tenant_id)
                 REFERENCES alert_incidents(id, tenant_id)
         );",
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

    let migration_started: bool =
        sqlx::query_scalar("SELECT to_regclass('iot_nano.system_accounts') IS NOT NULL")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    let error = match open_result {
        Ok(store) => {
            drop(store);
            None
        }
        Err(error) => Some(error),
    };
    common::reset_timescale_schema(&mut connection)
        .await
        .unwrap();

    let error = error.expect("notification schema with a global dedupe key was accepted");
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("notification_outbox"),
        "unexpected migration error: {error}"
    );
    assert!(!migration_started);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_command_outbox_without_a_tenant_root_foreign_key() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    create_timescale_command_reset_gate_schema(
        &mut connection,
        "CREATE TABLE command_outbox (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL,
             device_id TEXT NOT NULL,
             mode TEXT NOT NULL,
             FOREIGN KEY (device_id, tenant_id)
                 REFERENCES devices(device_id, tenant_id)
         );",
    )
    .await;

    assert_timescale_reset_gate(&mut connection, database_url, "command_outbox").await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_command_outbox_without_a_tenant_device_foreign_key() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    create_timescale_command_reset_gate_schema(
        &mut connection,
        "CREATE TABLE command_outbox (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             device_id TEXT NOT NULL,
             mode TEXT NOT NULL
         );",
    )
    .await;

    assert_timescale_reset_gate(&mut connection, database_url, "command_outbox").await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_notification_schema_with_a_partial_dedupe_key() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    create_timescale_notification_reset_gate_schema(
        &mut connection,
        "CREATE TABLE notification_outbox (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             incident_id UUID NOT NULL,
             dedupe_key TEXT NOT NULL,
             state TEXT NOT NULL,
             UNIQUE (id, tenant_id),
             UNIQUE (tenant_id, dedupe_key),
             FOREIGN KEY (incident_id, tenant_id)
                 REFERENCES alert_incidents(id, tenant_id)
         );
         CREATE UNIQUE INDEX notification_outbox_partial_dedupe_key_index
             ON notification_outbox (tenant_id, dedupe_key)
             WHERE state = 'pending';",
    )
    .await;

    assert_timescale_reset_gate(&mut connection, database_url, "notification_outbox").await;
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_open_rejects_notification_schema_with_an_expression_dedupe_key() {
    let (database_url, mut connection) = isolated_timescale_connection().await;
    create_timescale_notification_reset_gate_schema(
        &mut connection,
        "CREATE TABLE notification_outbox (
             id UUID PRIMARY KEY,
             tenant_id UUID NOT NULL REFERENCES tenants(id),
             incident_id UUID NOT NULL,
             dedupe_key TEXT NOT NULL,
             state TEXT NOT NULL,
             UNIQUE (id, tenant_id),
             UNIQUE (tenant_id, dedupe_key),
             FOREIGN KEY (incident_id, tenant_id)
                 REFERENCES alert_incidents(id, tenant_id)
         );
         CREATE UNIQUE INDEX notification_outbox_expression_dedupe_key_index
             ON notification_outbox (lower(dedupe_key));",
    )
    .await;

    assert_timescale_reset_gate(&mut connection, database_url, "notification_outbox").await;
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
