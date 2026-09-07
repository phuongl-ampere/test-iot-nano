use chrono::{TimeZone, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_storage::SqliteStore;
use serde_json::json;
use sqlx::Row;
use std::os::unix::fs::PermissionsExt;
use uuid::Uuid;

#[tokio::test]
async fn sqlite_store_creates_owner_only_parent_and_database_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("private").join("rush.db");
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(path.clone()),
        sqlite_busy_timeout_ms: 5_000,
    };

    SqliteStore::open(&configuration).await.unwrap();

    assert!(path.exists());
    assert_eq!(
        std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[tokio::test]
async fn sqlite_store_enables_wal_and_creates_platform_schema() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };

    let store = SqliteStore::open(&configuration).await.unwrap();
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sqlite_master
         WHERE type = 'table'
           AND name IN ('devices', 'telemetry', 'users', 'alert_rules', 'notification_outbox')",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(journal_mode.to_lowercase(), "wal");
    assert_eq!(tables, 5);
}

#[tokio::test]
async fn sqlite_maintenance_prunes_raw_and_rollup_rows_in_batches() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-1')")
        .execute(store.pool())
        .await
        .unwrap();
    for event_at in ["2025-01-01T00:00:00Z", "2026-09-07T00:00:00Z"] {
        sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic
             ) VALUES (?, ?, 'device-1', 'boot', 1, '{}', 'topic')",
        )
        .bind(event_at)
        .bind(event_at)
        .execute(store.pool())
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO telemetry_rollups_5m (bucket_at, device_id, event_count)
         VALUES ('2025-01-01T00:00:00Z', 'device-1', 1)",
    )
    .execute(store.pool())
    .await
    .unwrap();

    let result = store
        .enforce_retention("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", 1)
        .await
        .unwrap();
    let telemetry_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
    let rollup_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry_rollups_5m")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");

    assert_eq!(result.raw_rows, 1);
    assert_eq!(result.rollup_rows, 1);
    assert_eq!(telemetry_rows, 1);
    assert_eq!(rollup_rows, 0);
}

#[tokio::test]
async fn sqlite_maintenance_limits_each_table_to_one_batch() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    sqlx::query("INSERT INTO devices (device_id) VALUES ('device-1')")
        .execute(store.pool())
        .await
        .unwrap();
    for sequence in 1..=2 {
        sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic
             ) VALUES ('2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z',
                       'device-1', 'boot', ?, '{}', 'topic')",
        )
        .bind(sequence)
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO telemetry_rollups_5m (bucket_at, device_id, event_count)
             VALUES (?, 'device-1', 1)",
        )
        .bind(format!("2025-01-01T00:0{sequence}:00Z"))
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO telemetry_rollups_1h (bucket_at, device_id, event_count)
             VALUES (?, 'device-1', 1)",
        )
        .bind(format!("2025-01-01T0{sequence}:00:00Z"))
        .execute(store.pool())
        .await
        .unwrap();
    }

    let result = store
        .enforce_retention("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", 1)
        .await
        .unwrap();

    assert_eq!(result.raw_rows, 1);
    assert_eq!(result.rollup_rows, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM telemetry")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM telemetry_rollups_5m")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM telemetry_rollups_1h")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_maintenance_bounds_alert_history_without_pruning_active_work() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    let raw_before = "2026-01-01T00:00:00Z";
    let rollup_before = "2026-01-01T00:00:00Z";

    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold
         ) VALUES ('rule-1', 'High temperature', 'temperature_c', 'event', 'gt', 40)",
    )
    .execute(store.pool())
    .await
    .unwrap();

    for (event_at, boot_id, sequence) in [
        ("2025-01-01T00:00:00Z", "boot-old-1", "1"),
        ("2025-01-02T00:00:00Z", "boot-old-2", "2"),
        ("2026-09-07T00:00:00Z", "boot-current", "3"),
    ] {
        sqlx::query(
            "INSERT INTO alert_rule_event_evaluations (
                rule_id, event_at, device_id, boot_id, sequence
             ) VALUES ('rule-1', ?, 'device-1', ?, ?)",
        )
        .bind(event_at)
        .bind(boot_id)
        .bind(sequence)
        .execute(store.pool())
        .await
        .unwrap();
    }

    for (id, status, resolved_at) in [
        (
            "incident-resolved-blocked",
            "resolved",
            Some("2024-12-31T00:00:00Z"),
        ),
        (
            "incident-resolved-old-1",
            "resolved",
            Some("2025-01-01T00:00:00Z"),
        ),
        (
            "incident-resolved-old-2",
            "resolved",
            Some("2025-01-02T00:00:00Z"),
        ),
        ("incident-open-old", "open", None),
        (
            "incident-resolved-current",
            "resolved",
            Some("2026-09-07T00:00:00Z"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, rule_id, device_id, status, condition_started_at, resolved_at
             ) VALUES (?, 'rule-1', 'device-1', ?, '2025-01-01T00:00:00Z', ?)",
        )
        .bind(id)
        .bind(status)
        .bind(resolved_at)
        .execute(store.pool())
        .await
        .unwrap();
    }

    for (id, incident_id, state, sent_at) in [
        (
            "notification-sent-old-1",
            "incident-open-old",
            "sent",
            Some("2025-01-01T00:00:00Z"),
        ),
        (
            "notification-sent-old-2",
            "incident-open-old",
            "sent",
            Some("2025-01-02T00:00:00Z"),
        ),
        (
            "notification-sent-current",
            "incident-open-old",
            "sent",
            Some("2026-09-07T00:00:00Z"),
        ),
        (
            "notification-pending-old",
            "incident-resolved-blocked",
            "pending",
            None,
        ),
        (
            "notification-leased-old",
            "incident-resolved-blocked",
            "leased",
            None,
        ),
    ] {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, incident_id, kind, dedupe_key, subject, body, state, sent_at, created_at
             ) VALUES (?, ?, 'email', ?, 'subject', 'body', ?, ?,
                       '2025-01-01T00:00:00Z')",
        )
        .bind(id)
        .bind(incident_id)
        .bind(format!("dedupe-{id}"))
        .bind(state)
        .bind(sent_at)
        .execute(store.pool())
        .await
        .unwrap();
    }

    let result = store
        .enforce_retention(raw_before, rollup_before, 1)
        .await
        .unwrap();

    assert_eq!(result.event_evaluation_rows, 1);
    assert_eq!(result.notification_rows, 1);
    assert_eq!(result.resolved_incident_rows, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_rule_event_evaluations WHERE event_at < ?",
        )
        .bind(raw_before)
        .fetch_one(store.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox
             WHERE state = 'sent' AND sent_at < ?",
        )
        .bind(raw_before)
        .fetch_one(store.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents
             WHERE status = 'resolved' AND resolved_at < ?",
        )
        .bind(rollup_before)
        .fetch_one(store.pool())
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents WHERE id = 'incident-open-old'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents
             WHERE id = 'incident-resolved-blocked'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox
             WHERE state IN ('pending', 'leased')",
        )
        .fetch_one(store.pool())
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
async fn sqlite_writer_persists_idempotent_telemetry_and_rollups() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "device-1".to_owned(),
        boot_id: Uuid::parse_str("c9c04d99-4e01-4f94-82a8-9e229e47c093").unwrap(),
        sequence: 1,
        event_at: Utc.with_ymd_and_hms(2026, 9, 7, 1, 2, 3).unwrap(),
        measurements: serde_json::Map::from_iter([
            ("temperature_c".to_owned(), json!(26.4)),
            ("power_w".to_owned(), json!(529.9)),
        ]),
        gateway_device_id: None,
    };

    assert!(
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap()
    );
    assert!(
        !store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap()
    );
    let telemetry_rows = sqlx::query("SELECT COUNT(*) AS count FROM telemetry")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get::<i64, _>("count");
    let rollup = sqlx::query(
        "SELECT event_count, avg_temperature_c, avg_power_w
         FROM telemetry_rollups_5m
         WHERE device_id = 'device-1'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();

    assert_eq!(telemetry_rows, 1);
    assert_eq!(rollup.get::<i64, _>("event_count"), 1);
    assert_eq!(rollup.get::<f64, _>("avg_temperature_c"), 26.4);
    assert_eq!(rollup.get::<f64, _>("avg_power_w"), 529.9);
}
