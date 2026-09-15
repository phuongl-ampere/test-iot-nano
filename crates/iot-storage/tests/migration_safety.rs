use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::PlatformStore;
use sqlx::{Connection, Row, SqliteConnection};

#[test]
fn platform_store_owns_its_postgres_migration_source() {
    let migration = include_str!("../migrations/0001_platform.sql");
    let storage_source = include_str!("../src/lib.rs");

    assert!(migration.contains("CREATE TABLE IF NOT EXISTS devices"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS telemetry"));
    assert!(migration.contains("CREATE TABLE IF NOT EXISTS command_outbox"));
    assert!(migration.contains("DO $$"));
    assert!(migration.contains("command_outbox_device_id_fkey"));
    assert!(migration.contains("device_runtime_state_device_id_fkey"));
    assert!(migration.contains("telemetry_device_id_fkey"));
    assert!(migration.contains("ALTER TABLE command_outbox"));
    assert!(migration.contains("ALTER TABLE device_runtime_state"));
    assert!(migration.contains("ALTER TABLE telemetry"));
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
async fn sqlite_backup_preserves_committed_platform_data() {
    let directory = tempfile::tempdir().unwrap();
    let platform_path = directory.path().join("platform.sqlite");
    let store = PlatformStore::open(&sqlite_configuration(platform_path))
        .await
        .unwrap();
    store.register_device("backup-device").await.unwrap();

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
    store.register_device(device_id).await.unwrap();
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
async fn sqlite_migration_rejects_duplicate_root_asset_names_without_mutating_data() {
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
        Ok(_) => panic!("migration accepted duplicate root asset names"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("duplicate root asset name \"Duplicate root\""),
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
