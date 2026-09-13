use std::sync::Arc;

use chrono::{Duration, TimeZone, Timelike, Utc};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    AlertEvaluationEvent, AlertEvaluationRepository, AlertEvaluationResult, PlatformStore,
};
use serde_json::{Map, Value};
use sqlx::{Connection, PgConnection};
use tokio::{sync::Barrier, time::Duration as TokioDuration};

async fn store() -> (tempfile::TempDir, PlatformStore) {
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

async fn timescale_store() -> (PgConnection, PlatformStore) {
    let url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let mut connection = PgConnection::connect(&url).await.unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(database.starts_with("iot_nano_test_"));
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
        database_url: Some(url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (connection, store)
}

fn event(
    at: chrono::DateTime<Utc>,
    sequence: u64,
    value: f64,
    boot_id: uuid::Uuid,
) -> AlertEvaluationEvent {
    event_for_device(at, sequence, value, boot_id, "device-1")
}

fn event_for_device(
    at: chrono::DateTime<Utc>,
    sequence: u64,
    value: f64,
    boot_id: uuid::Uuid,
    device_id: &str,
) -> AlertEvaluationEvent {
    AlertEvaluationEvent {
        event_at: at,
        received_at: at,
        device_id: device_id.to_owned(),
        boot_id,
        sequence,
        measurements: Map::from_iter([("temperature_c".to_owned(), Value::from(value))]),
    }
}

#[tokio::test]
async fn sqlite_evaluation_opens_an_immediate_event_incident_atomically() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let at = Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap();
    let event = AlertEvaluationEvent {
        event_at: at,
        received_at: at,
        device_id: "device-1".to_owned(),
        boot_id: uuid::Uuid::now_v7(),
        sequence: 1,
        measurements: Map::from_iter([("temperature_c".to_owned(), Value::from(31.25))]),
    };

    let result = AlertEvaluationRepository::evaluate_alert_events(&store, &[event], at)
        .await
        .unwrap();

    assert_eq!(
        result,
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents WHERE status = 'open'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT subject FROM notification_outbox WHERE kind = 'opened'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        "[WARNING] temperature high opened"
    );
}

#[tokio::test]
async fn sqlite_duplicate_event_is_a_complete_noop() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let event = AlertEvaluationEvent {
        event_at: at,
        received_at: at,
        device_id: "device-1".to_owned(),
        boot_id: uuid::Uuid::now_v7(),
        sequence: 7,
        measurements: Map::from_iter([("temperature_c".to_owned(), Value::from(31.25))]),
    };
    store
        .evaluate_alert_events(&[event.clone()], at)
        .await
        .unwrap();
    let second = store.evaluate_alert_events(&[event], at).await.unwrap();

    assert_eq!(second, AlertEvaluationResult::default());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_positive_duration_breach_opens_at_the_exact_boundary() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let start = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();

    assert_eq!(
        store
            .evaluate_alert_events(&[event(start, 1, 31.25, boot_id)], start)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "pending"
    );

    let before_boundary = start + Duration::seconds(59);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(before_boundary, 2, 31.5, boot_id)], before_boundary,)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );

    let boundary = start + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(boundary, 3, 31.75, boot_id)], boundary)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "open"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'opened'"
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_normal_event_deletes_a_pending_incident() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let start = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(start, 1, 31.25, boot_id)], start)
        .await
        .unwrap();

    let normal_at = start + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(normal_at, 2, 29.0, boot_id)], normal_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_unacknowledged_open_breach_sends_a_versioned_reminder_when_due() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            reminder_interval_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_000_200, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(opened_at, 1, 31.25, boot_id)], opened_at)
        .await
        .unwrap();

    let reminder_at = opened_at + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(reminder_at, 2, 32.5, boot_id)], reminder_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            reminders: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT state_version FROM alert_incidents WHERE status = 'open'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT dedupe_key FROM notification_outbox WHERE kind = 'reminder'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        format!(
            "incident:{}:reminder:2:{}",
            sqlx::query_scalar::<_, String>("SELECT id FROM alert_incidents")
                .fetch_one(store.sqlite_pool().unwrap())
                .await
                .unwrap(),
            reminder_at.timestamp().div_euclid(60)
        )
    );
}

#[tokio::test]
async fn sqlite_open_normal_starts_recovery_and_resolves_at_the_exact_boundary() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_000_300, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(opened_at, 1, 31.25, boot_id)], opened_at)
        .await
        .unwrap();

    let recovery_started_at = opened_at + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(
                &[event(recovery_started_at, 2, 29.0, boot_id)],
                recovery_started_at,
            )
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT recovery_started_at FROM alert_incidents WHERE status = 'open'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        recovery_started_at.to_rfc3339()
    );

    let before_boundary = recovery_started_at + Duration::seconds(59);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(before_boundary, 3, 29.0, boot_id)], before_boundary,)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "open"
    );

    let resolved_at = recovery_started_at + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(resolved_at, 4, 29.0, boot_id)], resolved_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            resolved: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "resolved"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT state_version FROM alert_incidents WHERE status = 'resolved'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
async fn sqlite_reopen_within_grace_reuses_the_incident_and_beyond_grace_creates_one() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 0, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_000_400, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(opened_at, 1, 31.25, boot_id)], opened_at)
        .await
        .unwrap();
    let original_id: String = sqlx::query_scalar("SELECT id FROM alert_incidents")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE alert_incidents SET acknowledged_at = ?, acknowledged_by = 'operator' WHERE id = ?",
    )
    .bind(opened_at.to_rfc3339())
    .bind(&original_id)
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let first_resolved_at = opened_at + Duration::seconds(1);
    store
        .evaluate_alert_events(
            &[event(first_resolved_at, 2, 29.0, boot_id)],
            first_resolved_at,
        )
        .await
        .unwrap();

    let reopened_at = first_resolved_at + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(reopened_at, 3, 31.25, boot_id)], reopened_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    let (reopened_id, acknowledged_at, state_version): (String, Option<String>, i64) =
        sqlx::query_as(
            "SELECT id, acknowledged_at, state_version FROM alert_incidents WHERE status = 'open'",
        )
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(reopened_id, original_id);
    assert!(acknowledged_at.is_none());
    assert_eq!(state_version, 3);

    let second_resolved_at = reopened_at + Duration::seconds(1);
    store
        .evaluate_alert_events(
            &[event(second_resolved_at, 4, 29.0, boot_id)],
            second_resolved_at,
        )
        .await
        .unwrap();
    let beyond_grace_at = second_resolved_at + Duration::seconds(61);
    assert_eq!(
        store
            .evaluate_alert_events(
                &[event(beyond_grace_at, 5, 31.25, boot_id)],
                beyond_grace_at,
            )
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    let new_open_id: String =
        sqlx::query_scalar("SELECT id FROM alert_incidents WHERE status = 'open'")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_ne!(new_open_id, original_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn sqlite_acknowledged_open_suppresses_reminders_but_still_resolves() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reminder_interval_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 0, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_000_500, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(opened_at, 1, 31.25, boot_id)], opened_at)
        .await
        .unwrap();
    sqlx::query("UPDATE alert_incidents SET acknowledged_at = ?, acknowledged_by = 'operator'")
        .bind(opened_at.to_rfc3339())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();

    let reminder_at = opened_at + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(reminder_at, 2, 32.5, boot_id)], reminder_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );

    let resolved_at = reminder_at + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(resolved_at, 3, 29.0, boot_id)], resolved_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            resolved: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "resolved"
    );
}

#[tokio::test]
async fn sqlite_hysteresis_indeterminate_event_leaves_an_open_incident_unchanged() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, hysteresis
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 0, 2)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_000_600, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(opened_at, 1, 31.25, boot_id)], opened_at)
        .await
        .unwrap();

    let indeterminate_at = opened_at + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(
                &[event(indeterminate_at, 2, 29.0, boot_id)],
                indeterminate_at,
            )
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    let (status, last_value, recovery_started_at, state_version): (
        String,
        f64,
        Option<String>,
        i64,
    ) = sqlx::query_as(
        "SELECT status, last_value, recovery_started_at, state_version FROM alert_incidents",
    )
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(status, "open");
    assert_eq!(last_value, 31.25);
    assert!(recovery_started_at.is_none());
    assert_eq!(state_version, 1);
}

#[tokio::test]
async fn sqlite_notification_conflict_rolls_back_event_claim_and_transition() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let start = Utc.timestamp_opt(1_700_000_700, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(start, 1, 31.25, boot_id)], start)
        .await
        .unwrap();
    let incident_id: String = sqlx::query_scalar("SELECT id FROM alert_incidents")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let dedupe_key = format!("incident:{incident_id}:opened:1");
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, 'opened', ?, 'conflict', 'conflict', ?, ?)",
    )
    .bind(uuid::Uuid::now_v7().to_string())
    .bind(&incident_id)
    .bind(&dedupe_key)
    .bind(start.to_rfc3339())
    .bind(start.to_rfc3339())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();

    let boundary = start + Duration::seconds(60);
    let boundary_event = event(boundary, 2, 31.25, boot_id);
    assert!(
        store
            .evaluate_alert_events(&[boundary_event.clone()], boundary)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents WHERE id = ?")
            .bind(&incident_id)
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_rule_event_evaluations")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );

    sqlx::query("DELETE FROM notification_outbox WHERE dedupe_key = ?")
        .bind(&dedupe_key)
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(
        store
            .evaluate_alert_events(&[boundary_event], boundary)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_rule_event_evaluations")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn sqlite_evaluates_only_matching_enabled_rules_with_finite_json_numbers() {
    let (_directory, store) = store().await;
    let scoped_rule_id = uuid::Uuid::now_v7();
    let wildcard_rule_id = uuid::Uuid::now_v7();
    let disabled_rule_id = uuid::Uuid::now_v7();
    let archived_rule_id = uuid::Uuid::now_v7();
    for (id, name, device_id, enabled, archived_at) in [
        (
            scoped_rule_id,
            "scoped",
            Some("device-1"),
            1_i64,
            Option::<String>::None,
        ),
        (wildcard_rule_id, "wildcard", None, 1, None),
        (disabled_rule_id, "disabled", None, 0, None),
        (
            archived_rule_id,
            "archived",
            None,
            1,
            Some("2023-01-01T00:00:00+00:00".to_owned()),
        ),
    ] {
        sqlx::query(
            "INSERT INTO alert_rules (
                id, name, enabled, device_id, metric_key, rule_type, comparison, threshold, for_seconds,
                archived_at
             ) VALUES (?, ?, ?, ?, 'temperature_c', 'event_threshold', 'gt', 30, 0, ?)",
        )
        .bind(id.to_string())
        .bind(name)
        .bind(enabled)
        .bind(device_id)
        .bind(archived_at)
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    }
    let at = Utc.timestamp_opt(1_700_000_800, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    let mut wildcard_device_event = event(at, 1, 31.25, boot_id);
    wildcard_device_event.device_id = "device-2".to_owned();
    assert_eq!(
        store
            .evaluate_alert_events(&[wildcard_device_event], at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );

    let nonnumeric_at = at + Duration::seconds(1);
    let nonnumeric = AlertEvaluationEvent {
        event_at: nonnumeric_at,
        received_at: nonnumeric_at,
        device_id: "device-1".to_owned(),
        boot_id,
        sequence: 2,
        measurements: Map::from_iter([(
            "temperature_c".to_owned(),
            Value::String("31.25".to_owned()),
        )]),
    };
    assert_eq!(
        store
            .evaluate_alert_events(&[nonnumeric], nonnumeric_at)
            .await
            .unwrap(),
        AlertEvaluationResult::default()
    );

    let scoped_at = at + Duration::seconds(2);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(scoped_at, 3, 31.25, boot_id)], scoped_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 2,
            opened: 2,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents WHERE rule_id IN (?, ?)",
        )
        .bind(scoped_rule_id.to_string())
        .bind(wildcard_rule_id.to_string())
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents WHERE rule_id IN (?, ?)",
        )
        .bind(disabled_rule_id.to_string())
        .bind(archived_rule_id.to_string())
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn sqlite_canonicalizes_event_identity_and_keeps_legacy_u64_sequences() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let at = Utc.timestamp_opt(1_700_000_900, 123_456_789).unwrap();
    let canonical = at.with_nanosecond(123_456_000).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    let first = event(at, u64::MAX, 31.25, boot_id);
    assert_eq!(
        store.evaluate_alert_events(&[first], at).await.unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT event_at FROM alert_rule_event_evaluations")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        canonical.to_rfc3339()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT condition_started_at FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        canonical.to_rfc3339()
    );

    let duplicate_at = at.with_nanosecond(123_456_999).unwrap();
    assert_eq!(
        store
            .evaluate_alert_events(
                &[event(duplicate_at, u64::MAX, 99.0, boot_id)],
                duplicate_at,
            )
            .await
            .unwrap(),
        AlertEvaluationResult::default()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn sqlite_breach_clears_recovery_and_restarts_the_resolve_timer() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_001_000, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(opened_at, 1, 31.25, boot_id)], opened_at)
        .await
        .unwrap();
    let first_recovery_at = opened_at + Duration::seconds(1);
    store
        .evaluate_alert_events(
            &[event(first_recovery_at, 2, 29.0, boot_id)],
            first_recovery_at,
        )
        .await
        .unwrap();

    let breach_at = opened_at + Duration::seconds(30);
    store
        .evaluate_alert_events(&[event(breach_at, 3, 32.0, boot_id)], breach_at)
        .await
        .unwrap();
    let (recovery_started_at, last_value): (Option<String>, f64) = sqlx::query_as(
        "SELECT recovery_started_at, last_value FROM alert_incidents WHERE status = 'open'",
    )
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert!(recovery_started_at.is_none());
    assert_eq!(last_value, 32.0);

    let restarted_recovery_at = breach_at + Duration::seconds(1);
    store
        .evaluate_alert_events(
            &[event(restarted_recovery_at, 4, 29.0, boot_id)],
            restarted_recovery_at,
        )
        .await
        .unwrap();
    let before_restarted_boundary = restarted_recovery_at + Duration::seconds(59);
    assert_eq!(
        store
            .evaluate_alert_events(
                &[event(before_restarted_boundary, 5, 29.0, boot_id)],
                before_restarted_boundary,
            )
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        "open"
    );
    let resolved_at = restarted_recovery_at + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(resolved_at, 6, 29.0, boot_id)], resolved_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            resolved: 1,
            ..AlertEvaluationResult::default()
        }
    );
}

#[tokio::test]
async fn sqlite_within_grace_positive_duration_reopen_reuses_pending_incident() {
    let (_directory, store) = store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds
         ) VALUES (?, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 60, 0, 60)",
    )
    .bind(rule_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let started_at = Utc.timestamp_opt(1_700_001_100, 0).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    store
        .evaluate_alert_events(&[event(started_at, 1, 31.25, boot_id)], started_at)
        .await
        .unwrap();
    let incident_id: String = sqlx::query_scalar("SELECT id FROM alert_incidents")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let opened_at = started_at + Duration::seconds(60);
    store
        .evaluate_alert_events(&[event(opened_at, 2, 31.25, boot_id)], opened_at)
        .await
        .unwrap();
    let resolved_at = opened_at + Duration::seconds(1);
    store
        .evaluate_alert_events(&[event(resolved_at, 3, 29.0, boot_id)], resolved_at)
        .await
        .unwrap();

    let reopened_at = resolved_at + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(reopened_at, 4, 31.25, boot_id)], reopened_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            ..AlertEvaluationResult::default()
        }
    );
    let (reused_id, status, state_version): (String, String, i64) =
        sqlx::query_as("SELECT id, status, state_version FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(reused_id, incident_id);
    assert_eq!(status, "pending");
    assert_eq!(state_version, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        2
    );

    let reopened_boundary = reopened_at + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(
                &[event(reopened_boundary, 5, 31.25, boot_id)],
                reopened_boundary,
            )
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT state_version FROM alert_incidents")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
#[ignore = "requires a disposable Timescale URL"]
async fn timescale_concurrent_distinct_events_serialize_first_incident_transition() {
    let (_lock, store) = timescale_store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, reminder_interval_seconds
         ) VALUES ($1, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 0, 60, 60)",
    )
    .bind(rule_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let at = Utc.timestamp_opt(1_700_002_000, 123_456_789).unwrap();
    let first = event(at, 1, 31.25, uuid::Uuid::now_v7());
    let second = event(at, 2, 32.0, uuid::Uuid::now_v7());
    let first_events = [first];
    let second_events = [second];
    let first_store = store.clone();
    let second_store = store.clone();
    let (first_result, second_result) = tokio::join!(
        first_store.evaluate_alert_events(&first_events, at),
        second_store.evaluate_alert_events(&second_events, at),
    );

    let first_result = first_result.unwrap();
    let second_result = second_result.unwrap();
    assert_eq!(first_result.evaluated, 1);
    assert_eq!(second_result.evaluated, 1);
    assert_eq!(first_result.opened + second_result.opened, 1);
    assert_eq!(first_result.reminders + second_result.reminders, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents
             WHERE rule_id = $1 AND device_id = 'device-1' AND status IN ('pending', 'open')",
        )
        .bind(rule_id)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_rule_event_evaluations WHERE rule_id = $1",
        )
        .bind(rule_id)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox WHERE incident_id IN (
                SELECT id FROM alert_incidents WHERE rule_id = $1
             )",
        )
        .bind(rule_id)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        1
    );
}

#[tokio::test]
#[ignore = "requires a disposable Timescale URL"]
async fn timescale_opposite_ordered_event_batches_do_not_deadlock() {
    let (_lock, store) = timescale_store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, reminder_interval_seconds
         ) VALUES ($1, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 0, 60, 60)",
    )
    .bind(rule_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();

    let at = Utc.timestamp_opt(1_700_002_100, 123_456_789).unwrap();
    let first_events = [
        event_for_device(at, 1, 31.25, uuid::Uuid::now_v7(), "device-a"),
        event_for_device(at, 2, 31.5, uuid::Uuid::now_v7(), "device-b"),
    ];
    let second_events = [
        event_for_device(at, 3, 32.0, uuid::Uuid::now_v7(), "device-b"),
        event_for_device(at, 4, 32.25, uuid::Uuid::now_v7(), "device-a"),
    ];
    let barrier = Arc::new(Barrier::new(3));
    let first_store = store.clone();
    let first_barrier = Arc::clone(&barrier);
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_store.evaluate_alert_events(&first_events, at).await
    });
    let second_store = store.clone();
    let second_barrier = Arc::clone(&barrier);
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_store.evaluate_alert_events(&second_events, at).await
    });
    barrier.wait().await;

    let (first, second) = tokio::time::timeout(TokioDuration::from_secs(5), async {
        tokio::join!(first, second)
    })
    .await
    .expect("opposite-order batches must not deadlock");
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();
    assert_eq!(first.evaluated, 2);
    assert_eq!(second.evaluated, 2);
    assert_eq!(first.opened + second.opened, 2);
    assert_eq!(first.reminders + second.reminders, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_incidents
             WHERE rule_id = $1 AND status IN ('pending', 'open')",
        )
        .bind(rule_id)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM alert_rule_event_evaluations WHERE rule_id = $1",
        )
        .bind(rule_id)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        4
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox
             WHERE kind = 'opened' AND incident_id IN (
                 SELECT id FROM alert_incidents
                 WHERE rule_id = $1 AND status IN ('pending', 'open')
             )",
        )
        .bind(rule_id)
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        2
    );
}

#[tokio::test]
#[ignore = "requires a disposable Timescale URL"]
async fn timescale_event_evaluation_matches_sqlite_state_sequence_and_duplicate_contract() {
    let (_lock, store) = timescale_store().await;
    let rule_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, reminder_interval_seconds
         ) VALUES ($1, 'temperature high', 'temperature_c', 'event_threshold', 'gt', 30, 0, 0, 60, 60)",
    )
    .bind(rule_id)
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let opened_at = Utc.timestamp_opt(1_700_001_200, 123_456_789).unwrap();
    let boot_id = uuid::Uuid::now_v7();
    let opened_event = event(opened_at, 1, 31.25, boot_id);
    assert_eq!(
        store
            .evaluate_alert_events(&[opened_event.clone()], opened_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        store
            .evaluate_alert_events(&[opened_event], opened_at)
            .await
            .unwrap(),
        AlertEvaluationResult::default()
    );

    let reminder_at = opened_at + Duration::seconds(60);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(reminder_at, 2, 32.0, boot_id)], reminder_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            reminders: 1,
            ..AlertEvaluationResult::default()
        }
    );
    let resolved_at = reminder_at + Duration::seconds(1);
    assert_eq!(
        store
            .evaluate_alert_events(&[event(resolved_at, 3, 29.0, boot_id)], resolved_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            resolved: 1,
            ..AlertEvaluationResult::default()
        }
    );
    let reopened_at = resolved_at + Duration::seconds(1);
    let reopened_event = event(reopened_at, 4, 31.25, boot_id);
    assert_eq!(
        store
            .evaluate_alert_events(&[reopened_event.clone()], reopened_at)
            .await
            .unwrap(),
        AlertEvaluationResult {
            evaluated: 1,
            opened: 1,
            ..AlertEvaluationResult::default()
        }
    );
    assert_eq!(
        store
            .evaluate_alert_events(&[reopened_event], reopened_at)
            .await
            .unwrap(),
        AlertEvaluationResult::default()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_incidents")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap(),
        "open"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT state_version FROM alert_incidents")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification_outbox")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'opened'",
        )
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'reminder'",
        )
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM notification_outbox WHERE kind = 'resolved'",
        )
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap(),
        1
    );

    let overflow_at = reopened_at + Duration::seconds(1);
    assert!(
        store
            .evaluate_alert_events(&[event(overflow_at, u64::MAX, 32.0, boot_id)], overflow_at,)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_rule_event_evaluations")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap(),
        4
    );
}
