use chrono::{Duration, TimeZone, Timelike, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AlertComparison, AlertRepository, AlertRuleKind, AlertSeverity, PlatformStore,
    PlatformStoreError,
};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

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

fn insert_rule<'a>(
    store: &'a PlatformStore,
    id: Uuid,
    name: &'a str,
    enabled: bool,
    archived: bool,
    rule_type: &'a str,
    window_seconds: Option<i64>,
    created_at: &'a str,
) -> impl std::future::Future<Output = ()> + 'a {
    let archived_at = archived.then_some("2026-01-01T00:00:00Z");
    async move {
        sqlx::query(
            "INSERT INTO alert_rules (
                id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds, archived_at, created_at
             ) VALUES (?, ?, ?, 'device-a', 'temperature_c', ?, 'gte', 40.5, ?, 10, 20, 30,
                       0.5, 'critical', 60, ?, ?)",
        )
        .bind(id.to_string())
        .bind(name)
        .bind(enabled)
        .bind(rule_type)
        .bind(window_seconds)
        .bind(archived_at)
        .bind(created_at)
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn sqlite_alert_rules_load_active_rows_in_canonical_order_and_map_types() {
    let (_directory, store) = sqlite_store().await;
    let newest = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let oldest = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    insert_rule(
        &store,
        oldest,
        "oldest",
        true,
        false,
        "event_threshold",
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    insert_rule(
        &store,
        newest,
        "newest",
        true,
        false,
        "window_average",
        Some(300),
        "2026-02-01T00:00:00Z",
    )
    .await;
    insert_rule(
        &store,
        Uuid::now_v7(),
        "disabled",
        false,
        false,
        "event_threshold",
        None,
        "2026-03-01T00:00:00Z",
    )
    .await;
    insert_rule(
        &store,
        Uuid::now_v7(),
        "archived",
        true,
        true,
        "event_threshold",
        None,
        "2026-04-01T00:00:00Z",
    )
    .await;

    let rules = AlertRepository::load_active_rules(&store).await.unwrap();
    assert_eq!(
        rules
            .iter()
            .map(|rule| rule.name.as_str())
            .collect::<Vec<_>>(),
        ["newest", "oldest"]
    );
    assert_eq!(rules[0].id, newest);
    assert_eq!(rules[0].kind, AlertRuleKind::WindowAverage);
    assert_eq!(rules[0].comparison, AlertComparison::GreaterThanOrEqual);
    assert_eq!(rules[0].device_id.as_deref(), Some("device-a"));
    assert_eq!(rules[0].window, Some(Duration::seconds(300)));
    assert_eq!(rules[0].for_duration, Duration::seconds(10));
    assert_eq!(rules[0].resolve_after, Duration::seconds(20));
    assert_eq!(rules[0].reopen_grace, Duration::seconds(30));
    assert_eq!(rules[0].hysteresis, Some(0.5));
    assert_eq!(rules[0].severity, AlertSeverity::Critical);
    assert_eq!(rules[0].reminder_interval, Duration::seconds(60));
}

#[tokio::test]
async fn sqlite_alert_rules_reject_malformed_kind_and_duration_values() {
    let (_directory, store) = sqlite_store().await;
    insert_rule(
        &store,
        Uuid::now_v7(),
        "bad-kind",
        true,
        false,
        "bogus",
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    assert!(matches!(
        AlertRepository::load_active_rules(&store).await,
        Err(PlatformStoreError::InvalidAlertRuleKind(_))
    ));

    sqlx::query("DELETE FROM alert_rules")
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    insert_rule(
        &store,
        Uuid::now_v7(),
        "bad-duration",
        true,
        false,
        "window_average",
        Some(10),
        "2026-01-01T00:00:00Z",
    )
    .await;
    assert!(matches!(
        AlertRepository::load_active_rules(&store).await,
        Err(PlatformStoreError::InvalidAlertRuleDuration { .. })
    ));
}

#[tokio::test]
async fn sqlite_alert_rule_event_claim_is_idempotent_and_concurrency_safe() {
    let (_directory, store) = sqlite_store().await;
    let rule_id = Uuid::now_v7();
    insert_rule(
        &store,
        rule_id,
        "claimable",
        true,
        false,
        "event_threshold",
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    let event_at = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();
    let boot_id = Uuid::now_v7();

    assert!(
        AlertRepository::claim_rule_event(&store, rule_id, event_at, "device-a", boot_id, 7)
            .await
            .unwrap()
    );
    assert!(
        !AlertRepository::claim_rule_event(&store, rule_id, event_at, "device-a", boot_id, 7)
            .await
            .unwrap()
    );

    let mut attempts = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = store.clone();
        attempts.spawn(async move {
            AlertRepository::claim_rule_event(&store, rule_id, event_at, "device-a", boot_id, 8)
                .await
                .unwrap()
        });
    }
    let mut claimed = 0;
    while let Some(result) = attempts.join_next().await {
        claimed += i32::from(result.unwrap());
    }
    assert_eq!(claimed, 1);

    let count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM alert_rule_event_evaluations")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
        .get("count");
    assert_eq!(count, 2);
}

#[tokio::test]
async fn sqlite_alert_rule_event_claim_canonicalizes_submicrosecond_timestamps() {
    let (_directory, store) = sqlite_store().await;
    let rule_id = Uuid::now_v7();
    insert_rule(
        &store,
        rule_id,
        "canonical-claim",
        true,
        false,
        "event_threshold",
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    let event_at = Utc
        .with_ymd_and_hms(2026, 2, 1, 0, 0, 0)
        .unwrap()
        .with_nanosecond(123_456_100)
        .unwrap();
    let same_microsecond = event_at.with_nanosecond(123_456_900).unwrap();
    let boot_id = Uuid::now_v7();

    assert!(
        AlertRepository::claim_rule_event(&store, rule_id, event_at, "device-a", boot_id, 7)
            .await
            .unwrap()
    );
    assert!(
        !AlertRepository::claim_rule_event(
            &store,
            rule_id,
            same_microsecond,
            "device-a",
            boot_id,
            7,
        )
        .await
        .unwrap()
    );

    let count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM alert_rule_event_evaluations")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap()
        .get("count");
    assert_eq!(count, 1);
}

struct TimescaleTestLock {
    _connection: PgConnection,
}

async fn timescale_store() -> (TimescaleTestLock, PlatformStore) {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
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

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_alert_rule_repository_matches_sqlite_claim_contract() {
    let (_test_lock, store) = timescale_store().await;
    let rule_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, device_id, metric_key, rule_type, comparison, threshold,
            window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
            hysteresis, severity, reminder_interval_seconds
         ) VALUES ($1, 'timescale-rule', 'device-a', 'temperature_c', 'window_average',
                   'gte', 40.5, 300, 10, 20, 30, 0.5, 'critical', 60)",
    )
    .bind(rule_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let rules = AlertRepository::load_active_rules(&store).await.unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id, rule_id);
    assert_eq!(rules[0].kind, AlertRuleKind::WindowAverage);
    assert_eq!(rules[0].severity, AlertSeverity::Critical);

    let event_at = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();
    let boot_id = Uuid::now_v7();
    assert!(
        AlertRepository::claim_rule_event(&store, rule_id, event_at, "device-a", boot_id, 7)
            .await
            .unwrap()
    );
    assert!(
        !AlertRepository::claim_rule_event(&store, rule_id, event_at, "device-a", boot_id, 7)
            .await
            .unwrap()
    );

    let submicrosecond = event_at.with_nanosecond(100).unwrap();
    let same_microsecond = event_at.with_nanosecond(900).unwrap();
    assert!(
        AlertRepository::claim_rule_event(&store, rule_id, submicrosecond, "device-a", boot_id, 8,)
            .await
            .unwrap()
    );
    assert!(
        !AlertRepository::claim_rule_event(
            &store,
            rule_id,
            same_microsecond,
            "device-a",
            boot_id,
            8,
        )
        .await
        .unwrap()
    );
}
