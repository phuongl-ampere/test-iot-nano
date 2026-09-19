use chrono::{Duration, Utc};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{MANAGEMENT_ALERT_LIST_LIMIT, ManagementAlertRepository, PlatformStore};
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

mod common;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("management-alerts.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

async fn seed_tenant(store: &PlatformStore, slug: &str) -> Uuid {
    let tenant_id = Uuid::now_v7();
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, ?, 'active')")
                .bind(tenant_id.to_string())
                .bind(slug)
                .execute(store.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES (?, ?)")
                .bind(format!("{slug}-device"))
                .bind(tenant_id.to_string())
                .execute(store.pool())
                .await
                .unwrap();
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query("INSERT INTO tenants (id, slug, status) VALUES ($1, $2, 'active')")
                .bind(tenant_id)
                .bind(slug)
                .execute(pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ($1, $2)")
                .bind(format!("{slug}-device"))
                .bind(tenant_id)
                .execute(pool)
                .await
                .unwrap();
        }
    }
    tenant_id
}

async fn insert_alert(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    rule_name: &str,
    updated_at: chrono::DateTime<Utc>,
) {
    let rule_id = Uuid::now_v7();
    let incident_id = Uuid::now_v7();
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO alert_rules (
                    id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold, severity
                 ) VALUES (?, ?, ?, ?, 'temperature_c', 'event_threshold', 'gt', 30, 'critical')",
            )
            .bind(rule_id.to_string())
            .bind(tenant_id.to_string())
            .bind(rule_name)
            .bind(device_id)
            .execute(store.pool())
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO alert_incidents (
                    id, tenant_id, rule_id, device_id, status, condition_started_at, last_value, updated_at
                 ) VALUES (?, ?, ?, ?, 'open', ?, 42.5, ?)",
            )
            .bind(incident_id.to_string())
            .bind(tenant_id.to_string())
            .bind(rule_id.to_string())
            .bind(device_id)
            .bind(updated_at.to_rfc3339())
            .bind(updated_at.to_rfc3339())
            .execute(store.pool())
            .await
            .unwrap();
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO alert_rules (
                    id, tenant_id, name, device_id, metric_key, rule_type, comparison, threshold, severity
                 ) VALUES ($1, $2, $3, $4, 'temperature_c', 'event_threshold', 'gt', 30, 'critical')",
            )
            .bind(rule_id)
            .bind(tenant_id)
            .bind(rule_name)
            .bind(device_id)
            .execute(pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO alert_incidents (
                    id, tenant_id, rule_id, device_id, status, condition_started_at, last_value, updated_at
                 ) VALUES ($1, $2, $3, $4, 'open', $5, 42.5, $6)",
            )
            .bind(incident_id)
            .bind(tenant_id)
            .bind(rule_id)
            .bind(device_id)
            .bind(updated_at)
            .bind(updated_at)
            .execute(pool)
            .await
            .unwrap();
        }
    }
}

async fn assert_tenant_alert_list(store: &PlatformStore, suffix: &str) {
    let tenant_a = seed_tenant(store, &format!("management-alerts-a-{suffix}")).await;
    let tenant_b = seed_tenant(store, &format!("management-alerts-b-{suffix}")).await;
    let tenant_a_device = format!("management-alerts-a-{suffix}-device");
    let tenant_b_device = format!("management-alerts-b-{suffix}-device");
    let base = Utc::now() - Duration::hours(1);

    for index in 0..=MANAGEMENT_ALERT_LIST_LIMIT {
        insert_alert(
            store,
            tenant_a,
            &tenant_a_device,
            &format!("Tenant A rule {index}"),
            base + Duration::seconds(index as i64),
        )
        .await;
    }
    insert_alert(
        store,
        tenant_b,
        &tenant_b_device,
        "Tenant B newest rule",
        base + Duration::hours(1),
    )
    .await;

    let alerts = ManagementAlertRepository::list_management_alerts(store, tenant_a)
        .await
        .unwrap();

    assert_eq!(alerts.len(), MANAGEMENT_ALERT_LIST_LIMIT);
    assert!(
        alerts
            .iter()
            .all(|alert| alert.device_id == tenant_a_device)
    );
    assert_eq!(
        alerts.first().unwrap().rule_name,
        format!("Tenant A rule {MANAGEMENT_ALERT_LIST_LIMIT}")
    );
    assert_eq!(alerts.last().unwrap().rule_name, "Tenant A rule 1");
    assert_eq!(alerts.first().unwrap().severity, "critical");
    assert_eq!(alerts.first().unwrap().last_value, Some(42.5));
}

#[tokio::test]
async fn sqlite_management_alert_list_is_tenant_scoped_newest_first_and_capped() {
    let (_directory, store) = sqlite_store().await;
    assert_tenant_alert_list(&store, "sqlite").await;
}

#[tokio::test]
async fn sqlite_management_alert_list_orders_mixed_timestamp_formats_by_instant() {
    let (_directory, store) = sqlite_store().await;
    let tenant_id = seed_tenant(&store, "management-alerts-mixed-timestamps").await;
    let device_id = "management-alerts-mixed-timestamps-device";
    let older = chrono::DateTime::parse_from_rfc3339("2030-01-01T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    insert_alert(&store, tenant_id, device_id, "older", older).await;
    insert_alert(
        &store,
        tenant_id,
        device_id,
        "newer",
        older + Duration::seconds(1),
    )
    .await;
    sqlx::query(
        "UPDATE alert_incidents
         SET updated_at = '2030-01-01 12:00:01'
         WHERE rule_id = (
             SELECT id FROM alert_rules
             WHERE tenant_id = ? AND name = 'newer'
         )",
    )
    .bind(tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let alerts = ManagementAlertRepository::list_management_alerts(&store, tenant_id)
        .await
        .unwrap();
    assert_eq!(alerts.first().unwrap().rule_name, "newer");
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database"]
async fn timescale_management_alert_list_is_tenant_scoped_newest_first_and_capped() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set for ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to use non-test database {database_name:?}"
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
    assert_tenant_alert_list(&store, &Uuid::now_v7().to_string()).await;
}
