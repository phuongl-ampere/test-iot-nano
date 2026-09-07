use chrono::{DateTime, Utc};
use iot_storage::{SqliteStore, SqliteStoreError};
use iot_stream::{StreamConsumer, StreamError};
use sqlx::PgPool;
use thiserror::Error;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../db/migrations/0001_telemetry.sql"),
    include_str!("../../../db/migrations/0002_alerting.sql"),
    include_str!("../../../db/migrations/0003_auth_and_rule_archive.sql"),
    include_str!("../../../db/migrations/0004_device_tokens.sql"),
    include_str!("../../../db/migrations/0005_username_password_auth.sql"),
    include_str!("../../../db/migrations/0006_domain_management.sql"),
    include_str!("../../../db/migrations/0007_generic_users_and_app_grants.sql"),
    include_str!("../../../db/migrations/0008_device_lifecycle.sql"),
    include_str!("../../../db/migrations/0009_device_token_ciphertext.sql"),
    include_str!("../../../db/migrations/0010_gateway_child_devices.sql"),
];

#[derive(Debug, Clone)]
pub struct TelemetryWriter {
    pool: PgPool,
    batch_size: usize,
}

#[derive(Clone)]
pub struct SqliteTelemetryWriter {
    store: SqliteStore,
    batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushResult {
    pub read: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub committed_partitions: usize,
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Sqlite(#[from] SqliteStoreError),
}

pub async fn migrate(pool: &PgPool) -> Result<(), WriterError> {
    for migration in MIGRATIONS {
        sqlx::raw_sql(*migration).execute(pool).await?;
    }
    Ok(())
}

impl TelemetryWriter {
    pub fn new(pool: PgPool, batch_size: usize) -> Self {
        Self {
            pool,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_once(
        &self,
        consumer: &mut StreamConsumer,
        now: DateTime<Utc>,
    ) -> Result<FlushResult, WriterError> {
        let batch = consumer.poll(self.batch_size, now)?;
        if batch.records.is_empty() {
            return Ok(FlushResult {
                read: 0,
                inserted: 0,
                duplicates: 0,
                committed_partitions: 0,
            });
        }

        let mut transaction = self.pool.begin().await?;
        let mut inserted = 0;

        for record in &batch.records {
            sqlx::query(
                "INSERT INTO devices (device_id, last_seen_at)
                 VALUES ($1, $2)
                 ON CONFLICT (device_id)
                 DO UPDATE SET last_seen_at = GREATEST(devices.last_seen_at, EXCLUDED.last_seen_at)",
            )
            .bind(&record.message.event.device_id)
            .bind(record.message.received_at)
            .execute(&mut *transaction)
            .await?;

            let measurements = serde_json::Value::Object(record.message.event.measurements.clone());
            let result = sqlx::query(
                "INSERT INTO telemetry (
                    event_at, received_at, device_id, boot_id, sequence, measurements, topic,
                    gateway_device_id
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (event_at, device_id, boot_id, sequence) DO NOTHING",
            )
            .bind(record.message.event.event_at)
            .bind(record.message.received_at)
            .bind(&record.message.event.device_id)
            .bind(record.message.event.boot_id)
            .bind(i64::try_from(record.message.event.sequence).map_err(|_| {
                sqlx::Error::Protocol("telemetry sequence does not fit PostgreSQL BIGINT".into())
            })?)
            .bind(sqlx::types::Json(measurements))
            .bind(&record.message.topic)
            .bind(&record.message.event.gateway_device_id)
            .execute(&mut *transaction)
            .await?;

            inserted += result.rows_affected() as usize;
        }
        transaction.commit().await?;
        let record_count = batch.records.len();
        let committed_partitions = batch.commits.len();
        consumer.commit(batch, now)?;

        Ok(FlushResult {
            read: record_count,
            inserted,
            duplicates: record_count - inserted,
            committed_partitions,
        })
    }
}

impl SqliteTelemetryWriter {
    pub fn new(store: SqliteStore, batch_size: usize) -> Self {
        Self {
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_once(
        &self,
        consumer: &mut StreamConsumer,
        now: DateTime<Utc>,
    ) -> Result<FlushResult, WriterError> {
        let batch = consumer.poll(self.batch_size, now)?;
        if batch.records.is_empty() {
            return Ok(FlushResult {
                read: 0,
                inserted: 0,
                duplicates: 0,
                committed_partitions: 0,
            });
        }

        let mut inserted = 0;
        for record in &batch.records {
            inserted += usize::from(
                self.store
                    .write_telemetry(
                        &record.message.event,
                        record.message.received_at,
                        &record.message.topic,
                    )
                    .await?,
            );
        }
        let record_count = batch.records.len();
        let committed_partitions = batch.commits.len();
        consumer.commit(batch, now)?;

        Ok(FlushResult {
            read: record_count,
            inserted,
            duplicates: record_count - inserted,
            committed_partitions,
        })
    }
}
