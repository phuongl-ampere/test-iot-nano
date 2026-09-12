use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::PlatformStore;
use sqlx::{PgPool, Row};

#[tokio::test]
async fn platform_store_opens_the_complete_sqlite_platform_schema() {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    let pool = store.sqlite_pool().unwrap();
    let tables = sqlx::query(
        "SELECT name
         FROM sqlite_master
         WHERE type = 'table'
           AND name IN (
             'users',
             'devices',
             'telemetry',
             'alert_rules',
             'notification_outbox',
             'command_outbox'
           )
         ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("name"))
    .collect::<Vec<_>>();

    assert_eq!(
        tables,
        [
            "alert_rules",
            "command_outbox",
            "devices",
            "notification_outbox",
            "telemetry",
            "users",
        ]
    );
}

#[tokio::test]
async fn platform_store_migrates_timescale_into_the_iot_nano_schema() {
    let Some(database_url) = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").ok() else {
        eprintln!("skipping Timescale contract: IOT_NANO_TIMESCALE_TEST_URL is not configured");
        return;
    };

    let cleanup = PgPool::connect(&database_url).await.unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&cleanup)
        .await
        .unwrap();
    drop(cleanup);

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();

    let pool = store.timescale_pool().unwrap();
    let tables = sqlx::query(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano'
           AND table_name IN (
             'users',
             'devices',
             'telemetry',
             'alert_rules',
             'notification_outbox',
             'command_outbox'
           )
         ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("table_name"))
    .collect::<Vec<_>>();

    assert_eq!(
        tables,
        [
            "alert_rules",
            "command_outbox",
            "devices",
            "notification_outbox",
            "telemetry",
            "users",
        ]
    );
}
