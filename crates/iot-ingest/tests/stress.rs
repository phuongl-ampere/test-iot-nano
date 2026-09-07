use std::{
    env,
    fs::{File, OpenOptions},
    sync::{LazyLock, Mutex},
    time::{Duration as StdDuration, Instant},
};

use chrono::{Duration, Utc};
use fs2::FileExt;
use iot_core::TelemetryEvent;
use iot_ingest::{AlertEvaluator, TelemetryWriter, migrate};
use iot_stream::{GroupStart, LocalStream, StreamConfig, TelemetryMessage};
use serde_json::json;
use sqlx::{PgPool, query, query_scalar};
use uuid::Uuid;

static DATABASE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn database_url() -> String {
    env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the local TimescaleDB test database")
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn lock_database_file() -> File {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(env::temp_dir().join("rush-iot-nano-timescaledb-tests.lock"))
        .unwrap();
    file.lock_exclusive().unwrap();
    file
}

async fn prepared_pool() -> PgPool {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    migrate(&pool).await.unwrap();
    query(
        "TRUNCATE device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices",
    )
        .execute(&pool)
        .await
        .unwrap();
    pool
}

#[tokio::test]
#[ignore = "run with scripts/stress-local.sh"]
async fn configured_events_drain_to_telemetry_and_alert_groups() {
    let _database_lock = DATABASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let _database_file_lock = lock_database_file();
    let event_count = env_usize("STRESS_EVENTS", 10_000);
    let device_count = env_usize("STRESS_DEVICES", 100);
    let started = Instant::now();
    let pool = prepared_pool().await;
    query(
        "INSERT INTO alert_rules (
            id, name, metric_key, rule_type, comparison, threshold, for_seconds,
            resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds
         ) VALUES ($1, 'Stress high temperature', 'temperature_c', 'event_threshold', 'gt',
                   25.0, 0, 300, 3600, 'warning', 86400)",
    )
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(
        tempdir.path().join("stream"),
        StreamConfig {
            partition_count: 8,
            segment_max_bytes: 8 * 1024 * 1024,
            retention_max_bytes: 512 * 1024 * 1024,
            retention_max_age: StdDuration::from_secs(24 * 60 * 60),
            max_record_bytes: 1024 * 1024,
            index_stride: 128,
        },
    )
    .unwrap();
    let now = Utc::now();
    for index in 0..event_count {
        let device_number = index % device_count;
        let device_id = format!("esp-{device_number:06}");
        let received_at = now + Duration::milliseconds(i64::try_from(index).unwrap());
        let mut measurements = serde_json::Map::new();
        measurements.insert(
            "temperature_c".to_owned(),
            json!(20.0 + f64::from(u32::try_from(device_number % 10).unwrap())),
        );
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: device_id.clone(),
            boot_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, device_id.as_bytes()),
            sequence: u64::try_from(index / device_count + 1).unwrap(),
            event_at: received_at,
            measurements,
            gateway_device_id: None,
        };
        stream
            .append(TelemetryMessage {
                topic: format!("iot/v1/devices/{device_id}/telemetry"),
                payload: serde_json::to_vec(&event).unwrap(),
                event,
                received_at,
            })
            .unwrap();
    }

    let group_started_at = Utc::now();
    let mut writer_consumer = stream
        .join_group(
            "timescaledb-writer",
            "stress-writer",
            GroupStart::Earliest,
            group_started_at,
        )
        .unwrap();
    let mut alert_consumer = stream
        .join_group(
            "alert-evaluator",
            "stress-alert",
            GroupStart::Earliest,
            group_started_at,
        )
        .unwrap();

    let writer = TelemetryWriter::new(pool.clone(), 1_000);
    let evaluator = AlertEvaluator::new(pool.clone(), 1_000);
    let deadline = Instant::now() + StdDuration::from_secs(120);
    loop {
        let evaluation_time = Utc::now();
        writer_consumer.heartbeat(evaluation_time).unwrap();
        alert_consumer.heartbeat(evaluation_time).unwrap();
        let written = writer
            .flush_once(&mut writer_consumer, evaluation_time)
            .await
            .unwrap();
        let evaluated = evaluator
            .flush_event_rules(&mut alert_consumer, evaluation_time)
            .await
            .unwrap();
        if written.read == 0 && evaluated.read == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "stress test exceeded 120 seconds"
        );
    }

    let telemetry_rows = query_scalar::<_, i64>("SELECT COUNT(*) FROM telemetry")
        .fetch_one(&pool)
        .await
        .unwrap();
    let incident_rows =
        query_scalar::<_, i64>("SELECT COUNT(*) FROM alert_incidents WHERE status = 'open'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let expected_incidents = i64::try_from(
        (0..device_count)
            .filter(|device_number| 20 + (device_number % 10) > 25)
            .count(),
    )
    .unwrap();
    let writer_lag = writer_consumer.group_stats().unwrap().total_lag();
    let alert_lag = alert_consumer.group_stats().unwrap().total_lag();
    let elapsed = started.elapsed();
    let per_second = event_count as f64 / elapsed.as_secs_f64();

    eprintln!(
        "stress events={event_count} devices={device_count} elapsed_ms={} throughput={per_second:.0} msg/s",
        elapsed.as_millis()
    );
    assert_eq!(telemetry_rows, i64::try_from(event_count).unwrap());
    assert_eq!(incident_rows, expected_incidents);
    assert_eq!(writer_lag, 0);
    assert_eq!(alert_lag, 0);
    assert!(elapsed < StdDuration::from_secs(120));
}
