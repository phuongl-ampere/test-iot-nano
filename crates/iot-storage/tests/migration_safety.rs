use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::PlatformStore;
use sqlx::{Connection, PgConnection, Row, SqliteConnection};

mod common;

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
            id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, 'switch_on', '{}', 'one_way', ?, ?)",
    )
    .bind(&command_id)
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
