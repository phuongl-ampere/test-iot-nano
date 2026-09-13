use crate::{CoreSqliteStore, CoreSqliteStoreError};
use chrono::{DateTime, Utc};
use iot_storage::{
    GatewayIngestEventKind, GatewayIngestRepository, GatewayIngestRequest, PlatformStoreError,
    TelemetryRepository,
};
use iot_stream::{GatewayEventKind, GatewayMessage, StreamError, StreamMessage};
use sqlx::{Executor, PgPool, Postgres, Transaction, postgres::PgPoolOptions};
use thiserror::Error;

use crate::{ClaimedBatch, CoreStreamConsumer};

const MIGRATIONS: &[&str] = &[include_str!("../migrations/0001_core.sql")];

#[derive(Debug, Clone)]
pub struct TelemetryWriter {
    pool: PgPool,
    batch_size: usize,
}

#[derive(Clone)]
pub struct SqliteTelemetryWriter {
    store: CoreSqliteStore,
    batch_size: usize,
}

pub struct PlatformTelemetryWriter<S> {
    store: S,
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
    Sqlite(#[from] CoreSqliteStoreError),
    #[error(transparent)]
    Platform(#[from] PlatformStoreError),
}

pub async fn migrate(pool: &PgPool) -> Result<(), WriterError> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS iot_nano_core")
        .execute(pool)
        .await?;
    let mut connection = pool.acquire().await?;
    connection
        .execute("SET search_path TO iot_nano_core")
        .await?;
    for migration in MIGRATIONS {
        sqlx::raw_sql(*migration).execute(&mut *connection).await?;
    }
    Ok(())
}

pub async fn connect_core_database(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                connection
                    .execute("SET search_path TO iot_nano_core")
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
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
        consumer: &CoreStreamConsumer,
        _now: DateTime<Utc>,
    ) -> Result<FlushResult, WriterError> {
        let batch = consumer.claim(self.batch_size).await?;
        let mut result = self.flush_batch(&batch).await?;
        let committed_partitions = batch.committed_partitions();
        consumer.acknowledge(&batch).await?;
        result.committed_partitions = committed_partitions;
        Ok(result)
    }

    async fn flush_batch(&self, batch: &ClaimedBatch) -> Result<FlushResult, WriterError> {
        if batch.is_empty() {
            return Ok(empty_flush_result());
        }

        let mut transaction = self.pool.begin().await?;
        let mut inserted = 0;
        let mut telemetry_records = 0;

        for record in batch.records() {
            if let StreamMessage::Gateway(message) = &record.message {
                let receipt = sqlx::query(
                    "INSERT INTO gateway_event_receipts (
                        gateway_device_id, idempotency_key, event_at, received_at
                     ) VALUES ($1, $2, $3, $4)
                     ON CONFLICT DO NOTHING",
                )
                .bind(&message.gateway_event.gateway_device_id)
                .bind(&message.gateway_event.idempotency_key)
                .bind(message.gateway_event.event_at)
                .bind(message.received_at)
                .execute(&mut *transaction)
                .await?;
                if receipt.rows_affected() == 0 {
                    continue;
                }

                let seen_at = message.gateway_event.event_at;
                sqlx::query(
                    "INSERT INTO device_runtime_state (device_id, last_seen_at)
                     VALUES ($1, $2)
                     ON CONFLICT (device_id) DO UPDATE SET
                         last_seen_at = GREATEST(
                             COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
                             EXCLUDED.last_seen_at
                         )",
                )
                .bind(&message.gateway_event.gateway_device_id)
                .bind(seen_at)
                .execute(&mut *transaction)
                .await?;

                if let Some(event) = message.telemetry_event.as_ref() {
                    telemetry_records += 1;
                    inserted += usize::from(
                        write_postgres_telemetry(
                            &mut transaction,
                            event,
                            message.received_at,
                            &message.topic,
                        )
                        .await?,
                    );
                }

                match message.gateway_event.event_kind {
                    GatewayEventKind::Disconnect => {
                        if let Some(child_device_id) = &message.gateway_event.child_device_id {
                            sqlx::query(
                                "INSERT INTO device_runtime_state (
                                     device_id, gateway_read_quality
                                 ) VALUES ($1, 'unavailable')
                                 ON CONFLICT (device_id) DO UPDATE SET
                                     gateway_read_quality = EXCLUDED.gateway_read_quality",
                            )
                            .bind(child_device_id)
                            .execute(&mut *transaction)
                            .await?;
                        }
                    }
                    GatewayEventKind::ChildTelemetry => {
                        if let Some(child_device_id) = &message.gateway_event.child_device_id {
                            sqlx::query(
                                "INSERT INTO device_runtime_state (
                                     device_id, gateway_last_read_at, gateway_read_quality
                                 ) VALUES ($1, $2, 'good')
                                 ON CONFLICT (device_id) DO UPDATE SET
                                     gateway_last_read_at = GREATEST(
                                         COALESCE(
                                             device_runtime_state.gateway_last_read_at,
                                             '-infinity'::timestamptz
                                         ),
                                         EXCLUDED.gateway_last_read_at
                                     ),
                                     gateway_read_quality = EXCLUDED.gateway_read_quality",
                            )
                            .bind(child_device_id)
                            .bind(seen_at)
                            .execute(&mut *transaction)
                            .await?;
                        }
                    }
                    GatewayEventKind::Connect | GatewayEventKind::Heartbeat => {}
                }
                continue;
            }
            let Some((event, received_at, topic)) = record.message.telemetry_parts() else {
                continue;
            };
            telemetry_records += 1;
            inserted += usize::from(
                write_postgres_telemetry(&mut transaction, event, received_at, topic).await?,
            );
        }
        transaction.commit().await?;
        let record_count = batch.records().len();

        Ok(FlushResult {
            read: record_count,
            inserted,
            duplicates: telemetry_records - inserted,
            committed_partitions: 0,
        })
    }
}

async fn write_postgres_telemetry(
    transaction: &mut Transaction<'_, Postgres>,
    event: &iot_core::TelemetryEvent,
    received_at: DateTime<Utc>,
    topic: &str,
) -> Result<bool, WriterError> {
    sqlx::query(
        "INSERT INTO device_runtime_state (device_id, last_seen_at)
         VALUES ($1, $2)
         ON CONFLICT (device_id)
         DO UPDATE SET last_seen_at = GREATEST(
             COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
             EXCLUDED.last_seen_at
         )",
    )
    .bind(&event.device_id)
    .bind(received_at)
    .execute(&mut **transaction)
    .await?;

    let measurements = serde_json::Value::Object(event.measurements.clone());
    let result = sqlx::query(
        "INSERT INTO telemetry (
            event_at, received_at, device_id, boot_id, sequence, measurements, topic,
            gateway_device_id
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (event_at, device_id, boot_id, sequence) DO NOTHING",
    )
    .bind(event.event_at)
    .bind(received_at)
    .bind(&event.device_id)
    .bind(event.boot_id)
    .bind(i64::try_from(event.sequence).map_err(|_| {
        sqlx::Error::Protocol("telemetry sequence does not fit PostgreSQL BIGINT".into())
    })?)
    .bind(sqlx::types::Json(measurements))
    .bind(topic)
    .bind(&event.gateway_device_id)
    .execute(&mut **transaction)
    .await?;

    Ok(result.rows_affected() == 1)
}

impl SqliteTelemetryWriter {
    pub fn new(store: CoreSqliteStore, batch_size: usize) -> Self {
        Self {
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_once(
        &self,
        consumer: &CoreStreamConsumer,
        _now: DateTime<Utc>,
    ) -> Result<FlushResult, WriterError> {
        let batch = consumer.claim(self.batch_size).await?;
        let mut result = self.flush_batch(&batch).await?;
        let committed_partitions = batch.committed_partitions();
        consumer.acknowledge(&batch).await?;
        result.committed_partitions = committed_partitions;
        Ok(result)
    }

    async fn flush_batch(&self, batch: &ClaimedBatch) -> Result<FlushResult, WriterError> {
        if batch.is_empty() {
            return Ok(empty_flush_result());
        }

        let mut inserted = 0;
        let mut telemetry_records = 0;
        for record in batch.records() {
            if let StreamMessage::Gateway(message) = &record.message {
                let (_, telemetry_inserted) = self.write_gateway_message(message).await?;
                if message.telemetry_event.is_some() {
                    telemetry_records += 1;
                }
                inserted += usize::from(telemetry_inserted);
                continue;
            }
            let Some((event, received_at, topic)) = record.message.telemetry_parts() else {
                continue;
            };
            telemetry_records += 1;
            inserted += usize::from(
                self.store
                    .write_telemetry(event, received_at, topic)
                    .await?,
            );
        }
        let record_count = batch.records().len();

        Ok(FlushResult {
            read: record_count,
            inserted,
            duplicates: telemetry_records - inserted,
            committed_partitions: 0,
        })
    }

    async fn write_gateway_message(
        &self,
        message: &GatewayMessage,
    ) -> Result<(bool, bool), WriterError> {
        let mut transaction = self.store.pool().begin().await?;
        let receipt = sqlx::query(
            "INSERT OR IGNORE INTO gateway_event_receipts (
                gateway_device_id, idempotency_key, event_at, received_at
             ) VALUES (?, ?, ?, ?)",
        )
        .bind(&message.gateway_event.gateway_device_id)
        .bind(&message.gateway_event.idempotency_key)
        .bind(message.gateway_event.event_at.to_rfc3339())
        .bind(message.received_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        if receipt.rows_affected() == 0 {
            transaction.commit().await?;
            return Ok((false, false));
        }

        let seen_at = message.gateway_event.event_at.to_rfc3339();
        sqlx::query(
            "INSERT INTO device_runtime_state (device_id, last_seen_at)
             VALUES (?, ?)
             ON CONFLICT(device_id) DO UPDATE SET
                 last_seen_at = CASE
                     WHEN device_runtime_state.last_seen_at IS NULL
                       OR device_runtime_state.last_seen_at < excluded.last_seen_at
                     THEN excluded.last_seen_at
                     ELSE device_runtime_state.last_seen_at
                 END",
        )
        .bind(&message.gateway_event.gateway_device_id)
        .bind(&seen_at)
        .execute(&mut *transaction)
        .await?;

        let telemetry_inserted = if let Some(event) = message.telemetry_event.as_ref() {
            self.store
                .write_telemetry_in_transaction(
                    &mut transaction,
                    event,
                    message.received_at,
                    &message.topic,
                )
                .await?
        } else {
            false
        };

        match message.gateway_event.event_kind {
            GatewayEventKind::Disconnect => {
                if let Some(child_device_id) = &message.gateway_event.child_device_id {
                    sqlx::query(
                        "INSERT INTO device_runtime_state (
                             device_id, gateway_read_quality
                         ) VALUES (?, 'unavailable')
                         ON CONFLICT(device_id) DO UPDATE SET
                             gateway_read_quality = excluded.gateway_read_quality",
                    )
                    .bind(child_device_id)
                    .execute(&mut *transaction)
                    .await?;
                }
            }
            GatewayEventKind::ChildTelemetry => {
                if let Some(child_device_id) = &message.gateway_event.child_device_id {
                    sqlx::query(
                        "INSERT INTO device_runtime_state (
                             device_id, gateway_last_read_at, gateway_read_quality
                         ) VALUES (?, ?, 'good')
                         ON CONFLICT(device_id) DO UPDATE SET
                             gateway_last_read_at = CASE
                                 WHEN device_runtime_state.gateway_last_read_at IS NULL
                                   OR device_runtime_state.gateway_last_read_at
                                      < excluded.gateway_last_read_at
                                 THEN excluded.gateway_last_read_at
                                 ELSE device_runtime_state.gateway_last_read_at
                             END,
                             gateway_read_quality = excluded.gateway_read_quality",
                    )
                    .bind(child_device_id)
                    .bind(&seen_at)
                    .execute(&mut *transaction)
                    .await?;
                }
            }
            GatewayEventKind::Connect | GatewayEventKind::Heartbeat => {}
        }

        transaction.commit().await?;
        Ok((true, telemetry_inserted))
    }
}

impl<S> PlatformTelemetryWriter<S>
where
    S: TelemetryRepository + GatewayIngestRepository,
{
    pub fn new(store: S, batch_size: usize) -> Self {
        Self {
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_once(
        &self,
        consumer: &CoreStreamConsumer,
        _now: DateTime<Utc>,
    ) -> Result<FlushResult, WriterError> {
        let batch = consumer.claim(self.batch_size).await?;
        let mut result = self.flush_batch(&batch).await?;
        let committed_partitions = batch.committed_partitions();
        consumer.acknowledge(&batch).await?;
        result.committed_partitions = committed_partitions;
        Ok(result)
    }

    async fn flush_batch(&self, batch: &ClaimedBatch) -> Result<FlushResult, WriterError> {
        if batch.is_empty() {
            return Ok(empty_flush_result());
        }

        let mut inserted = 0;
        let mut telemetry_records = 0;
        for record in batch.records() {
            match &record.message {
                StreamMessage::Gateway(message) => {
                    if message.telemetry_event.is_some() {
                        telemetry_records += 1;
                    }
                    let result = self
                        .store
                        .ingest_gateway(gateway_ingest_request(message))
                        .await
                        .map_err(WriterError::Platform)?;
                    if message.telemetry_event.is_some() {
                        inserted += usize::from(result.telemetry_inserted);
                    }
                }
                StreamMessage::Telemetry(message) => {
                    telemetry_records += 1;
                    inserted += usize::from(
                        self.store
                            .write_telemetry(&message.event, message.received_at, &message.topic)
                            .await
                            .map_err(WriterError::Platform)?,
                    );
                }
            }
        }

        Ok(FlushResult {
            read: batch.records().len(),
            inserted,
            duplicates: telemetry_records - inserted,
            committed_partitions: 0,
        })
    }
}

fn gateway_ingest_request(message: &GatewayMessage) -> GatewayIngestRequest {
    GatewayIngestRequest {
        gateway_device_id: message.gateway_event.gateway_device_id.clone(),
        child_device_id: message.gateway_event.child_device_id.clone(),
        event_kind: match message.gateway_event.event_kind {
            GatewayEventKind::Connect => GatewayIngestEventKind::Connect,
            GatewayEventKind::Disconnect => GatewayIngestEventKind::Disconnect,
            GatewayEventKind::Heartbeat => GatewayIngestEventKind::Heartbeat,
            GatewayEventKind::ChildTelemetry => GatewayIngestEventKind::ChildTelemetry,
        },
        event_at: message.gateway_event.event_at,
        idempotency_key: message.gateway_event.idempotency_key.clone(),
        telemetry_event: message.telemetry_event.clone(),
        topic: message.topic.clone(),
        received_at: message.received_at,
    }
}

fn empty_flush_result() -> FlushResult {
    FlushResult {
        read: 0,
        inserted: 0,
        duplicates: 0,
        committed_partitions: 0,
    }
}
