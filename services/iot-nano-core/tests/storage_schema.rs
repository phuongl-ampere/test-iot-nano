use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_core::CoreSqliteStore;
use sqlx::{Connection, SqliteConnection};

fn sqlite_configuration(path: std::path::PathBuf) -> StorageConfiguration {
    StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path),
        sqlite_busy_timeout_ms: 5_000,
    }
}

#[tokio::test]
async fn sqlite_open_rejects_pre_tenant_core_schema_without_partial_migration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pre-tenant-core.sqlite");
    let mut connection =
        SqliteConnection::connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE telemetry (
             event_at TEXT NOT NULL,
             device_id TEXT NOT NULL,
             boot_id TEXT NOT NULL,
             sequence INTEGER NOT NULL
         );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let error = match CoreSqliteStore::open(&sqlite_configuration(path.clone())).await {
        Ok(_) => panic!("pre-tenant core schema was accepted"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("reset the development database"),
        "unexpected migration error: {error}"
    );
    assert!(
        error.to_string().contains("telemetry"),
        "unexpected migration error: {error}"
    );

    let mut connection = SqliteConnection::connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await
        .unwrap();
    let runtime_state_table_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'device_runtime_state'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    let tenant_id_column_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pragma_table_info('telemetry') WHERE name = 'tenant_id'
         )",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();

    assert_eq!(runtime_state_table_exists, 0);
    assert_eq!(tenant_id_column_exists, 0);
}

#[tokio::test]
async fn sqlite_open_initializes_a_fresh_tenant_scoped_core_schema() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreSqliteStore::open(&sqlite_configuration(directory.path().join("core.sqlite")))
        .await
        .unwrap();
    let tenant_id_column_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pragma_table_info('telemetry') WHERE name = 'tenant_id'
         )",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(tenant_id_column_exists, 1);
}
