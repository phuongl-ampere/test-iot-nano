use chrono::{TimeZone, Timelike, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{PlatformStore, PlatformStoreError, TelemetryAggregateRepository};
use sqlx::{Connection, PgConnection};

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

async fn insert_sqlite_telemetry(
    store: &PlatformStore,
    event_at: &str,
    sequence: i64,
    measurements: &str,
) {
    sqlx::query("INSERT INTO devices (device_id) VALUES ('aggregate-device')")
        .execute(store.sqlite_pool().unwrap())
        .await
        .ok();
    sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic
         ) VALUES (?, ?, 'aggregate-device', 'aggregate-boot', ?, ?, 'test')",
    )
    .bind(event_at)
    .bind(event_at)
    .bind(sequence)
    .bind(measurements)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
}

async fn exercise_aggregate_contract(store: &PlatformStore) {
    let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let to = Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap();

    let aggregate = TelemetryAggregateRepository::average_metric(
        store,
        "aggregate-device",
        "temperature_c",
        from,
        to,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(aggregate.average, 20.0);
    assert_eq!(aggregate.sample_count, 3);
}

#[tokio::test]
async fn sqlite_telemetry_aggregate_contract_uses_canonical_ranges_and_filters_samples() {
    let (_directory, store) = sqlite_store().await;
    seed_sqlite_aggregate_contract(&store).await;
    exercise_aggregate_contract(&store).await;
}

#[tokio::test]
async fn sqlite_telemetry_aggregate_parses_rfc3339_range_values_without_lexical_ordering() {
    let (_directory, store) = sqlite_store().await;
    for (event_at, sequence, measurements) in [
        ("2026-01-01T00:00:00+00:00", 1, r#"{"temperature_c":10}"#),
        (
            "2026-01-01T00:00:00.000000900+00:00",
            2,
            r#"{"temperature_c":20}"#,
        ),
        ("2026-01-01T00:00:00.000001Z", 3, r#"{"temperature_c":30}"#),
        (
            "2026-01-01T00:00:00.000000500Z",
            4,
            r#"{"temperature_c":"not-a-number"}"#,
        ),
        (
            "2026-01-01T00:00:00.000002100+00:00",
            5,
            r#"{"temperature_c":40}"#,
        ),
    ] {
        insert_sqlite_telemetry(&store, event_at, sequence, measurements).await;
    }

    let from = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .unwrap()
        .with_nanosecond(900)
        .unwrap();
    let to = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .unwrap()
        .with_nanosecond(1_900)
        .unwrap();
    let aggregate = TelemetryAggregateRepository::average_metric(
        &store,
        "aggregate-device",
        "temperature_c",
        from,
        to,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(aggregate.average, 20.0);
    assert_eq!(aggregate.sample_count, 3);
}

async fn seed_sqlite_aggregate_contract(store: &PlatformStore) {
    insert_sqlite_telemetry(
        store,
        "2026-01-01T00:00:00.000000Z",
        1,
        r#"{"temperature_c":10}"#,
    )
    .await;
    insert_sqlite_telemetry(
        store,
        "2026-01-01T00:00:30.000000Z",
        2,
        r#"{"temperature_c":20}"#,
    )
    .await;
    insert_sqlite_telemetry(
        store,
        "2026-01-01T00:01:00.000000Z",
        3,
        r#"{"temperature_c":30}"#,
    )
    .await;
    insert_sqlite_telemetry(
        store,
        "2026-01-01T00:00:45.000000Z",
        4,
        r#"{"temperature_c":"not-a-number","other":99}"#,
    )
    .await;
    insert_sqlite_telemetry(store, "2026-01-01T00:00:50.000000Z", 5, r#"{"other":99}"#).await;
    insert_sqlite_telemetry(
        store,
        "2026-01-01T00:00:55.000000Z",
        6,
        r#"{"temperature_c":null}"#,
    )
    .await;
}

#[tokio::test]
async fn sqlite_telemetry_aggregate_rejects_invalid_metric_identifiers() {
    let (_directory, store) = sqlite_store().await;
    let result = TelemetryAggregateRepository::average_metric(
        &store,
        "device",
        "temperature_c || measurements",
        Utc::now(),
        Utc::now(),
    )
    .await;
    assert!(matches!(
        result,
        Err(PlatformStoreError::InvalidTelemetryMetricKey(_))
    ));
}

#[tokio::test]
async fn sqlite_telemetry_aggregate_returns_none_without_numeric_samples() {
    let (_directory, store) = sqlite_store().await;
    insert_sqlite_telemetry(
        &store,
        "2026-01-01T00:00:00Z",
        1,
        r#"{"temperature_c":"NaN"}"#,
    )
    .await;
    let result = TelemetryAggregateRepository::average_metric(
        &store,
        "aggregate-device",
        "temperature_c",
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap(),
    )
    .await
    .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn sqlite_telemetry_aggregate_excludes_nonfinite_json_numbers() {
    let (_directory, store) = sqlite_store().await;
    for (event_at, sequence, measurements) in [
        ("2026-01-01T00:00:00Z", 1, r#"{"temperature_c":1.25}"#),
        ("2026-01-01T00:00:10Z", 2, r#"{"temperature_c":-2.5}"#),
        ("2026-01-01T00:00:20Z", 3, r#"{"temperature_c":9e999}"#),
        ("2026-01-01T00:00:30Z", 4, r#"{"temperature_c":-9e999}"#),
    ] {
        insert_sqlite_telemetry(&store, event_at, sequence, measurements).await;
    }

    let aggregate = TelemetryAggregateRepository::average_metric(
        &store,
        "aggregate-device",
        "temperature_c",
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(aggregate.average, -0.625);
    assert_eq!(aggregate.sample_count, 2);
}

async fn timescale_store() -> (String, PlatformStore, PgConnection) {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(database_name.starts_with("iot_nano_test_"));
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:platform-storage-test'))")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut connection)
        .await
        .unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url.clone()),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (database_url, store, connection)
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_telemetry_aggregate_matches_sqlite_contract() {
    let (_database_url, store, _connection) = timescale_store().await;
    sqlx::query("INSERT INTO devices (device_id) VALUES ('aggregate-device')")
        .execute(store.timescale_pool().unwrap())
        .await
        .unwrap();
    for (event_at, sequence, measurements) in [
        (
            "2026-01-01T00:00:00.000000900Z",
            1_i64,
            r#"{"temperature_c":10}"#,
        ),
        (
            "2026-01-01T00:00:30.000000100Z",
            2,
            r#"{"temperature_c":20}"#,
        ),
        ("2026-01-01T00:01:00Z", 3, r#"{"temperature_c":30}"#),
        (
            "2026-01-01T00:00:45Z",
            4,
            r#"{"temperature_c":"not-a-number"}"#,
        ),
        ("2026-01-01T00:00:50Z", 5, r#"{"other":99}"#),
        ("2026-01-01T00:00:55Z", 6, r#"{"temperature_c":null}"#),
    ] {
        let event_at = chrono::DateTime::parse_from_rfc3339(event_at)
            .unwrap()
            .with_timezone(&Utc);
        sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic
             ) VALUES ($1, $1, 'aggregate-device', '00000000-0000-0000-0000-000000000001',
                       $2, $3::jsonb, 'test')",
        )
        .bind(event_at)
        .bind(sequence)
        .bind(measurements)
        .execute(store.timescale_pool().unwrap())
        .await
        .unwrap();
    }
    let aggregate = TelemetryAggregateRepository::average_metric(
        &store,
        "aggregate-device",
        "temperature_c",
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(aggregate.average, 20.0);
    assert_eq!(aggregate.sample_count, 3);
}
