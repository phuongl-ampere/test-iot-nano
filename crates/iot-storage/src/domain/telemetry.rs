use std::{future::Future, pin::Pin};

use chrono::{DateTime, TimeZone, Utc};
use iot_core::TelemetryEvent;
use sqlx::{PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction, types::Json};

use crate::{
    GatewayIngestEventKind, GatewayIngestRepository, GatewayIngestRequest, GatewayIngestResult,
    GatewayIngestValidationError, PlatformStore, PlatformStoreError, SqliteStore, SqliteStoreError,
    TelemetryAggregate, TelemetryAggregateRepository, TelemetryRepository,
    canonical_postgres_timestamp, timescale_tenant_device_is_locked,
};

impl PlatformStore {
    pub async fn write_telemetry(
        &self,
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, PlatformStoreError> {
        let sequence = i64::try_from(event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;

        match self {
            Self::Sqlite(store) => {
                self.require_sqlite_tenant_device(tenant_id, &event.device_id)
                    .await?;
                Ok(store
                    .write_telemetry(tenant_id, event, received_at, topic)
                    .await?)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                if !timescale_tenant_device_is_locked(&mut transaction, tenant_id, &event.device_id)
                    .await?
                {
                    return Err(PlatformStoreError::UnknownDevice(event.device_id.clone()));
                }
                sqlx::query(
                    "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (tenant_id, device_id)
                     DO UPDATE SET last_seen_at = GREATEST(
                         COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
                         EXCLUDED.last_seen_at
                     )",
                )
                .bind(tenant_id)
                .bind(&event.device_id)
                .bind(received_at)
                .execute(&mut *transaction)
                .await?;

                let result = sqlx::query(
                    "INSERT INTO telemetry (
                        event_at, received_at, tenant_id, device_id, boot_id, sequence,
                        measurements, topic, gateway_device_id
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (tenant_id, event_at, device_id, boot_id, sequence) DO NOTHING",
                )
                .bind(event.event_at)
                .bind(received_at)
                .bind(tenant_id)
                .bind(&event.device_id)
                .bind(event.boot_id)
                .bind(sequence)
                .bind(Json(serde_json::Value::Object(event.measurements.clone())))
                .bind(topic)
                .bind(&event.gateway_device_id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(result.rows_affected() == 1)
            }
        }
    }

    pub async fn ingest_gateway(
        &self,
        request: GatewayIngestRequest,
    ) -> Result<GatewayIngestResult, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => ingest_sqlite_gateway(store, request).await,
            Self::Timescale(pool) => ingest_timescale_gateway(pool, request).await,
        }
    }

    pub async fn average_metric(
        &self,
        tenant_id: uuid::Uuid,
        device_id: &str,
        metric_key: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Option<TelemetryAggregate>, PlatformStoreError> {
        validate_telemetry_metric_key(metric_key)?;
        let from = canonical_postgres_timestamp(from);
        let to = canonical_postgres_timestamp(to);
        match self {
            Self::Sqlite(store) => {
                let path = format!("$.{metric_key}");
                let row = sqlx::query(
                    "WITH canonical_telemetry AS (
                        SELECT measurements,
                               CAST(unixepoch(event_at) AS INTEGER) * 1000000
                               + CASE
                                   WHEN instr(event_at, '.') = 0 THEN 0
                                   ELSE CAST(
                                       substr(
                                           substr(
                                               event_at,
                                               instr(event_at, '.') + 1,
                                               CASE
                                                   WHEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       'Z'
                                                   ) > 0
                                                   THEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       'Z'
                                                   ) - 1
                                                   WHEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       '+'
                                                   ) > 0
                                                   THEN instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       '+'
                                                   ) - 1
                                                   ELSE instr(
                                                       substr(event_at, instr(event_at, '.') + 1),
                                                       '-'
                                                   ) - 1
                                               END
                                           ) || '000000',
                                           1,
                                           6
                                       ) AS INTEGER
                                   )
                        END AS event_at_micros
                        FROM telemetry
                        WHERE tenant_id = ? AND device_id = ?
                     )
                     SELECT AVG(json_extract(measurements, ?)) AS average,
                            COUNT(*) AS sample_count
                     FROM canonical_telemetry
                     WHERE event_at_micros >= ?
                       AND event_at_micros <= ?
                       AND json_type(measurements, ?) IN ('integer', 'real')
                       AND json_extract(measurements, ?) > -1.0e999
                       AND json_extract(measurements, ?) < 1.0e999",
                )
                .bind(tenant_id.to_string())
                .bind(device_id)
                .bind(&path)
                .bind(from.timestamp_micros())
                .bind(to.timestamp_micros())
                .bind(&path)
                .bind(&path)
                .bind(&path)
                .fetch_one(store.pool())
                .await?;
                let sample_count = row.get::<i64, _>("sample_count");
                if sample_count == 0 {
                    Ok(None)
                } else {
                    Ok(Some(TelemetryAggregate {
                        average: row.get("average"),
                        sample_count: u64::try_from(sample_count)
                            .expect("SQLite COUNT(*) is nonnegative"),
                    }))
                }
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "WITH finite_telemetry AS (
                         SELECT CASE
                                    WHEN jsonb_typeof(measurements -> $1) = 'number'
                                    THEN CASE
                                        WHEN (measurements ->> $1)::numeric BETWEEN
                                                 '-1.7976931348623157e308'::numeric
                                             AND '1.7976931348623157e308'::numeric
                                        THEN (measurements ->> $1)::numeric
                                    END
                                END AS finite_value
                         FROM telemetry
                         WHERE tenant_id = $2
                           AND device_id = $3
                           AND event_at >= $4
                           AND event_at <= $5
                     )
                     SELECT (AVG(finite_value))::double precision AS average,
                            COUNT(finite_value) AS sample_count
                     FROM finite_telemetry",
                )
                .bind(metric_key)
                .bind(tenant_id)
                .bind(device_id)
                .bind(from)
                .bind(to)
                .fetch_one(pool)
                .await?;
                let sample_count = row.get::<i64, _>("sample_count");
                if sample_count == 0 {
                    Ok(None)
                } else {
                    Ok(Some(TelemetryAggregate {
                        average: row.get("average"),
                        sample_count: u64::try_from(sample_count)
                            .expect("PostgreSQL COUNT(*) is nonnegative"),
                    }))
                }
            }
        }
    }
}

async fn ingest_sqlite_gateway(
    store: &SqliteStore,
    request: GatewayIngestRequest,
) -> Result<GatewayIngestResult, PlatformStoreError> {
    validate_gateway_ingest_request(&request)?;
    let mut transaction = store.pool().begin().await?;
    if !sqlite_active_gateway_exists(
        &mut transaction,
        request.tenant_id,
        &request.gateway_device_id,
    )
    .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(request.gateway_device_id));
    }
    if let Some(child_device_id) = request.child_device_id.as_deref()
        && !sqlite_active_owned_child_exists(
            &mut transaction,
            request.tenant_id,
            child_device_id,
            &request.gateway_device_id,
        )
        .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(
            child_device_id.to_owned(),
        ));
    }

    let receipt = sqlx::query(
        "INSERT OR IGNORE INTO gateway_event_receipts (
            tenant_id, gateway_device_id, idempotency_key, event_at, received_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(request.tenant_id.to_string())
    .bind(&request.gateway_device_id)
    .bind(&request.idempotency_key)
    .bind(request.event_at.to_rfc3339())
    .bind(request.received_at.to_rfc3339())
    .execute(transaction.as_mut())
    .await?;
    if receipt.rows_affected() == 0 {
        transaction.commit().await?;
        return Ok(GatewayIngestResult {
            receipt_inserted: false,
            telemetry_inserted: false,
        });
    }

    let event_at = request.event_at.to_rfc3339();
    sqlx::query(
        "UPDATE devices
         SET last_seen_at = CASE
             WHEN last_seen_at IS NULL OR last_seen_at < ? THEN ?
             ELSE last_seen_at
         END
         WHERE tenant_id = ? AND device_id = ?",
    )
    .bind(&event_at)
    .bind(&event_at)
    .bind(request.tenant_id.to_string())
    .bind(&request.gateway_device_id)
    .execute(transaction.as_mut())
    .await?;
    let telemetry_inserted = if let Some(telemetry_event) = request.telemetry_event.as_ref() {
        store
            .write_telemetry_in_transaction(
                &mut transaction,
                request.tenant_id,
                telemetry_event,
                request.received_at,
                &request.topic,
            )
            .await?
    } else {
        false
    };
    match request.event_kind {
        GatewayIngestEventKind::Disconnect => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "UPDATE devices
                     SET gateway_read_quality = 'unavailable'
                     WHERE tenant_id = ? AND device_id = ?",
                )
                .bind(request.tenant_id.to_string())
                .bind(child_device_id)
                .execute(transaction.as_mut())
                .await?;
            }
        }
        GatewayIngestEventKind::ChildTelemetry => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "UPDATE devices
                     SET gateway_last_read_at = CASE
                             WHEN gateway_last_read_at IS NULL OR gateway_last_read_at < ? THEN ?
                             ELSE gateway_last_read_at
                         END,
                         gateway_read_quality = 'good'
                     WHERE tenant_id = ? AND device_id = ?",
                )
                .bind(&event_at)
                .bind(&event_at)
                .bind(request.tenant_id.to_string())
                .bind(child_device_id)
                .execute(transaction.as_mut())
                .await?;
            }
        }
        GatewayIngestEventKind::Connect | GatewayIngestEventKind::Heartbeat => {}
    }

    transaction.commit().await?;
    Ok(GatewayIngestResult {
        receipt_inserted: true,
        telemetry_inserted,
    })
}

async fn ingest_timescale_gateway(
    pool: &PgPool,
    request: GatewayIngestRequest,
) -> Result<GatewayIngestResult, PlatformStoreError> {
    validate_gateway_ingest_request(&request)?;
    let mut transaction = pool.begin().await?;
    if !timescale_active_gateway_is_locked(
        &mut transaction,
        request.tenant_id,
        &request.gateway_device_id,
    )
    .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(request.gateway_device_id));
    }
    if let Some(child_device_id) = request.child_device_id.as_deref()
        && !timescale_active_owned_child_is_locked(
            &mut transaction,
            request.tenant_id,
            child_device_id,
            &request.gateway_device_id,
        )
        .await?
    {
        transaction.rollback().await?;
        return Err(PlatformStoreError::UnknownDevice(
            child_device_id.to_owned(),
        ));
    }

    let receipt = sqlx::query(
        "INSERT INTO gateway_event_receipts (
            tenant_id, gateway_device_id, idempotency_key, event_at, received_at
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tenant_id, gateway_device_id, idempotency_key) DO NOTHING",
    )
    .bind(request.tenant_id)
    .bind(&request.gateway_device_id)
    .bind(&request.idempotency_key)
    .bind(request.event_at)
    .bind(request.received_at)
    .execute(&mut *transaction)
    .await?;
    if receipt.rows_affected() == 0 {
        transaction.commit().await?;
        return Ok(GatewayIngestResult {
            receipt_inserted: false,
            telemetry_inserted: false,
        });
    }

    sqlx::query(
        "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (tenant_id, device_id)
         DO UPDATE SET last_seen_at = GREATEST(
             COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
             EXCLUDED.last_seen_at
         )",
    )
    .bind(request.tenant_id)
    .bind(&request.gateway_device_id)
    .bind(request.event_at)
    .execute(&mut *transaction)
    .await?;
    let telemetry_inserted = if let Some(telemetry_event) = request.telemetry_event.as_ref() {
        let sequence = i64::try_from(telemetry_event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;
        sqlx::query(
            "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
             VALUES ($1, $2, $3)
             ON CONFLICT (tenant_id, device_id)
             DO UPDATE SET last_seen_at = GREATEST(
                 COALESCE(device_runtime_state.last_seen_at, '-infinity'::timestamptz),
                 EXCLUDED.last_seen_at
             )",
        )
        .bind(request.tenant_id)
        .bind(&telemetry_event.device_id)
        .bind(request.received_at)
        .execute(&mut *transaction)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements,
                topic, gateway_device_id
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (tenant_id, event_at, device_id, boot_id, sequence) DO NOTHING",
        )
        .bind(telemetry_event.event_at)
        .bind(request.received_at)
        .bind(request.tenant_id)
        .bind(&telemetry_event.device_id)
        .bind(telemetry_event.boot_id)
        .bind(sequence)
        .bind(Json(serde_json::Value::Object(
            telemetry_event.measurements.clone(),
        )))
        .bind(&request.topic)
        .bind(&telemetry_event.gateway_device_id)
        .execute(&mut *transaction)
        .await?;
        inserted.rows_affected() == 1
    } else {
        false
    };
    match request.event_kind {
        GatewayIngestEventKind::Disconnect => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "INSERT INTO device_runtime_state (
                         tenant_id, device_id, gateway_read_quality
                     ) VALUES ($1, $2, 'unavailable')
                     ON CONFLICT (tenant_id, device_id)
                     DO UPDATE SET gateway_read_quality = EXCLUDED.gateway_read_quality",
                )
                .bind(request.tenant_id)
                .bind(child_device_id)
                .execute(&mut *transaction)
                .await?;
            }
        }
        GatewayIngestEventKind::ChildTelemetry => {
            if let Some(child_device_id) = request.child_device_id.as_deref() {
                sqlx::query(
                    "INSERT INTO device_runtime_state (
                         tenant_id, device_id, gateway_last_read_at, gateway_read_quality
                     ) VALUES ($1, $2, $3, 'good')
                     ON CONFLICT (tenant_id, device_id)
                     DO UPDATE SET
                         gateway_last_read_at = GREATEST(
                             COALESCE(
                                 device_runtime_state.gateway_last_read_at,
                                 '-infinity'::timestamptz
                             ),
                             EXCLUDED.gateway_last_read_at
                         ),
                         gateway_read_quality = EXCLUDED.gateway_read_quality",
                )
                .bind(request.tenant_id)
                .bind(child_device_id)
                .bind(request.event_at)
                .execute(&mut *transaction)
                .await?;
            }
        }
        GatewayIngestEventKind::Connect | GatewayIngestEventKind::Heartbeat => {}
    }

    transaction.commit().await?;
    Ok(GatewayIngestResult {
        receipt_inserted: true,
        telemetry_inserted,
    })
}

fn validate_gateway_ingest_request(
    request: &GatewayIngestRequest,
) -> Result<(), PlatformStoreError> {
    match request.event_kind {
        GatewayIngestEventKind::ChildTelemetry => {
            if request.child_device_id.is_none() {
                return Err(GatewayIngestValidationError::ChildTelemetryMissingChild.into());
            }
            if request.telemetry_event.is_none() {
                return Err(GatewayIngestValidationError::ChildTelemetryMissingTelemetry.into());
            }
        }
        GatewayIngestEventKind::Connect
        | GatewayIngestEventKind::Disconnect
        | GatewayIngestEventKind::Heartbeat => {
            if request.telemetry_event.is_some() {
                return Err(GatewayIngestValidationError::TelemetryOnNonChildEvent.into());
            }
        }
    }

    if let Some(telemetry_event) = request.telemetry_event.as_ref() {
        let Some(child_device_id) = request.child_device_id.as_deref() else {
            return Err(GatewayIngestValidationError::TelemetryMissingChild.into());
        };
        if telemetry_event.device_id != child_device_id {
            return Err(GatewayIngestValidationError::TelemetryChildMismatch.into());
        }
        if telemetry_event.gateway_device_id.as_deref() != Some(request.gateway_device_id.as_str())
        {
            return Err(GatewayIngestValidationError::TelemetryGatewayMismatch.into());
        }
        i64::try_from(telemetry_event.sequence)
            .map_err(|_| PlatformStoreError::TelemetrySequenceOverflow)?;
    }

    Ok(())
}

async fn sqlite_active_gateway_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = ? AND device_id = ? AND deleted_at IS NULL AND is_gateway = 1",
    )
    .bind(tenant_id.to_string())
    .bind(gateway_device_id)
    .fetch_optional(transaction.as_mut())
    .await
    .map(|device| device.is_some())
}

async fn sqlite_active_owned_child_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = ?
           AND device_id = ?
           AND deleted_at IS NULL
           AND is_gateway = 0
           AND gateway_device_id = ?",
    )
    .bind(tenant_id.to_string())
    .bind(child_device_id)
    .bind(gateway_device_id)
    .fetch_optional(transaction.as_mut())
    .await
    .map(|device| device.is_some())
}

async fn timescale_active_gateway_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = $1 AND device_id = $2 AND deleted_at IS NULL AND is_gateway = TRUE
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(gateway_device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map(|device| device.is_some())
}

async fn timescale_active_owned_child_is_locked(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT device_id
         FROM devices
         WHERE tenant_id = $1
           AND device_id = $2
           AND deleted_at IS NULL
           AND is_gateway = FALSE
           AND gateway_device_id = $3
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(child_device_id)
    .bind(gateway_device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map(|device| device.is_some())
}

fn validate_telemetry_metric_key(metric_key: &str) -> Result<(), PlatformStoreError> {
    let mut characters = metric_key.chars();
    let valid = matches!(characters.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(PlatformStoreError::InvalidTelemetryMetricKey(
            metric_key.to_owned(),
        ))
    }
}

impl TelemetryRepository for PlatformStore {
    fn write_telemetry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        event: &'a TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::write_telemetry(self, tenant_id, event, received_at, topic).await
        })
    }
}

impl GatewayIngestRepository for PlatformStore {
    fn ingest_gateway<'a>(
        &'a self,
        request: GatewayIngestRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayIngestResult, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::ingest_gateway(self, request).await })
    }
}

impl TelemetryAggregateRepository for PlatformStore {
    fn average_metric<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
        metric_key: &'a str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<TelemetryAggregate>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::average_metric(self, tenant_id, device_id, metric_key, from, to).await
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionResult {
    pub raw_rows: u64,
    pub rollup_rows: u64,
    pub event_evaluation_rows: u64,
    pub notification_rows: u64,
    pub resolved_incident_rows: u64,
}

impl SqliteStore {
    pub async fn write_telemetry(
        &self,
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = self
            .write_telemetry_in_transaction(&mut transaction, tenant_id, event, received_at, topic)
            .await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn write_telemetry_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let received_at = received_at.to_rfc3339();
        sqlx::query(
            "UPDATE devices
             SET last_seen_at = CASE
                 WHEN last_seen_at IS NULL OR last_seen_at < ? THEN ?
                 ELSE last_seen_at
             END
             WHERE tenant_id = ? AND device_id = ?",
        )
        .bind(&received_at)
        .bind(&received_at)
        .bind(tenant_id.to_string())
        .bind(&event.device_id)
        .execute(transaction.as_mut())
        .await?;
        let insert = sqlx::query(
            "INSERT OR IGNORE INTO telemetry (
                event_at, received_at, tenant_id, device_id, boot_id, sequence, measurements,
                topic, gateway_device_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.event_at.to_rfc3339())
        .bind(&received_at)
        .bind(tenant_id.to_string())
        .bind(&event.device_id)
        .bind(event.boot_id.to_string())
        .bind(i64::try_from(event.sequence).map_err(|_| SqliteStoreError::SequenceOverflow)?)
        .bind(serde_json::to_string(&event.measurements).map_err(SqliteStoreError::Serialization)?)
        .bind(topic)
        .bind(&event.gateway_device_id)
        .execute(transaction.as_mut())
        .await?;
        let inserted = insert.rows_affected() == 1;
        if inserted {
            let metrics = MetricAverages::from_event(event);
            upsert_rollup(
                transaction,
                RollupTable::FiveMinute,
                bucket_start(event.event_at, 5 * 60),
                tenant_id,
                &event.device_id,
                metrics,
            )
            .await?;
            upsert_rollup(
                transaction,
                RollupTable::OneHour,
                bucket_start(event.event_at, 60 * 60),
                tenant_id,
                &event.device_id,
                metrics,
            )
            .await?;
        }
        Ok(inserted)
    }

    pub async fn enforce_retention(
        &self,
        raw_before: &str,
        rollup_before: &str,
        batch_size: u64,
    ) -> Result<RetentionResult, SqliteStoreError> {
        let batch_size = batch_size.max(1).min(10_000);
        let raw_rows =
            delete_before(&self.pool, RetentionTable::Raw, raw_before, batch_size).await?;
        let rollup_rows = delete_before(
            &self.pool,
            RetentionTable::FiveMinuteRollup,
            rollup_before,
            batch_size,
        )
        .await?
            + delete_before(
                &self.pool,
                RetentionTable::OneHourRollup,
                rollup_before,
                batch_size,
            )
            .await?;
        let event_evaluation_rows = delete_before(
            &self.pool,
            RetentionTable::AlertRuleEventEvaluations,
            raw_before,
            batch_size,
        )
        .await?;
        let notification_rows = delete_before(
            &self.pool,
            RetentionTable::SentNotifications,
            raw_before,
            batch_size,
        )
        .await?;
        let resolved_incident_rows = delete_before(
            &self.pool,
            RetentionTable::ResolvedIncidents,
            rollup_before,
            batch_size,
        )
        .await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await?;
        sqlx::query("PRAGMA incremental_vacuum(64)")
            .execute(&self.pool)
            .await?;
        Ok(RetentionResult {
            raw_rows,
            rollup_rows,
            event_evaluation_rows,
            notification_rows,
            resolved_incident_rows,
        })
    }
}

async fn delete_before(
    pool: &SqlitePool,
    table: RetentionTable,
    before: &str,
    batch_size: u64,
) -> Result<u64, sqlx::Error> {
    let bound_batch_size = i64::try_from(batch_size).unwrap_or(10_000);
    let result = match table {
        RetentionTable::Raw => {
            sqlx::query(
                "DELETE FROM telemetry
                 WHERE rowid IN (
                     SELECT rowid
                     FROM telemetry
                     WHERE event_at < ?
                     ORDER BY event_at
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::FiveMinuteRollup => {
            sqlx::query(
                "DELETE FROM telemetry_rollups_5m
                 WHERE rowid IN (
                     SELECT rowid
                     FROM telemetry_rollups_5m
                     WHERE bucket_at < ?
                     ORDER BY bucket_at
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::OneHourRollup => {
            sqlx::query(
                "DELETE FROM telemetry_rollups_1h
                 WHERE rowid IN (
                     SELECT rowid
                     FROM telemetry_rollups_1h
                     WHERE bucket_at < ?
                     ORDER BY bucket_at
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::AlertRuleEventEvaluations => {
            sqlx::query(
                "DELETE FROM alert_rule_event_evaluations
                 WHERE rowid IN (
                     SELECT rowid
                     FROM alert_rule_event_evaluations
                     WHERE event_at < ?
                     ORDER BY event_at, rule_id, device_id, boot_id, sequence
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::SentNotifications => {
            sqlx::query(
                "DELETE FROM notification_outbox
                 WHERE rowid IN (
                     SELECT rowid
                     FROM notification_outbox
                     WHERE state = 'sent' AND sent_at < ?
                     ORDER BY sent_at, id
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
        RetentionTable::ResolvedIncidents => {
            sqlx::query(
                "DELETE FROM alert_incidents
                 WHERE rowid IN (
                     SELECT rowid
                     FROM alert_incidents
                     WHERE status = 'resolved'
                       AND resolved_at < ?
                       AND NOT EXISTS (
                           SELECT 1
                           FROM notification_outbox
                           WHERE notification_outbox.incident_id = alert_incidents.id
                             AND notification_outbox.state IN ('pending', 'leased')
                       )
                     ORDER BY resolved_at, id
                     LIMIT ?
                 )",
            )
            .bind(before)
            .bind(bound_batch_size)
            .execute(pool)
            .await?
        }
    };
    Ok(result.rows_affected())
}

#[derive(Clone, Copy)]
enum RetentionTable {
    Raw,
    FiveMinuteRollup,
    OneHourRollup,
    AlertRuleEventEvaluations,
    SentNotifications,
    ResolvedIncidents,
}

#[derive(Clone, Copy)]
enum RollupTable {
    FiveMinute,
    OneHour,
}

#[derive(Clone, Copy)]
struct MetricAverages {
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
    voltage_v: Option<f64>,
    current_a: Option<f64>,
    power_w: Option<f64>,
    energy_kwh: Option<f64>,
}

impl MetricAverages {
    fn from_event(event: &TelemetryEvent) -> Self {
        Self {
            temperature_c: numeric_measurement(event, "temperature_c"),
            humidity_pct: numeric_measurement(event, "humidity_pct"),
            voltage_v: numeric_measurement(event, "voltage_v"),
            current_a: numeric_measurement(event, "current_a"),
            power_w: numeric_measurement(event, "power_w"),
            energy_kwh: numeric_measurement(event, "energy_kwh"),
        }
    }
}

fn numeric_measurement(event: &TelemetryEvent, key: &str) -> Option<f64> {
    event
        .measurements
        .get(key)
        .and_then(serde_json::Value::as_f64)
}

fn bucket_start(event_at: DateTime<Utc>, seconds: i64) -> String {
    let timestamp = event_at.timestamp();
    let bucket = timestamp - timestamp.rem_euclid(seconds);
    Utc.timestamp_opt(bucket, 0)
        .single()
        .expect("valid UTC bucket timestamp")
        .to_rfc3339()
}

async fn upsert_rollup(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: RollupTable,
    bucket_at: String,
    tenant_id: uuid::Uuid,
    device_id: &str,
    metrics: MetricAverages,
) -> Result<(), sqlx::Error> {
    let query = match table {
        RollupTable::FiveMinute => {
            "INSERT INTO telemetry_rollups_5m (
                bucket_at, tenant_id, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, bucket_at, device_id) DO UPDATE SET
                avg_temperature_c = CASE
                    WHEN excluded.temperature_count = 0 THEN telemetry_rollups_5m.avg_temperature_c
                    WHEN telemetry_rollups_5m.temperature_count = 0 THEN excluded.avg_temperature_c
                    ELSE (
                        telemetry_rollups_5m.avg_temperature_c * telemetry_rollups_5m.temperature_count
                        + excluded.avg_temperature_c * excluded.temperature_count
                    ) / (telemetry_rollups_5m.temperature_count + excluded.temperature_count)
                END,
                temperature_count = telemetry_rollups_5m.temperature_count + excluded.temperature_count,
                avg_humidity_pct = CASE
                    WHEN excluded.humidity_count = 0 THEN telemetry_rollups_5m.avg_humidity_pct
                    WHEN telemetry_rollups_5m.humidity_count = 0 THEN excluded.avg_humidity_pct
                    ELSE (
                        telemetry_rollups_5m.avg_humidity_pct * telemetry_rollups_5m.humidity_count
                        + excluded.avg_humidity_pct * excluded.humidity_count
                    ) / (telemetry_rollups_5m.humidity_count + excluded.humidity_count)
                END,
                humidity_count = telemetry_rollups_5m.humidity_count + excluded.humidity_count,
                avg_voltage_v = CASE
                    WHEN excluded.voltage_count = 0 THEN telemetry_rollups_5m.avg_voltage_v
                    WHEN telemetry_rollups_5m.voltage_count = 0 THEN excluded.avg_voltage_v
                    ELSE (
                        telemetry_rollups_5m.avg_voltage_v * telemetry_rollups_5m.voltage_count
                        + excluded.avg_voltage_v * excluded.voltage_count
                    ) / (telemetry_rollups_5m.voltage_count + excluded.voltage_count)
                END,
                voltage_count = telemetry_rollups_5m.voltage_count + excluded.voltage_count,
                avg_current_a = CASE
                    WHEN excluded.current_count = 0 THEN telemetry_rollups_5m.avg_current_a
                    WHEN telemetry_rollups_5m.current_count = 0 THEN excluded.avg_current_a
                    ELSE (
                        telemetry_rollups_5m.avg_current_a * telemetry_rollups_5m.current_count
                        + excluded.avg_current_a * excluded.current_count
                    ) / (telemetry_rollups_5m.current_count + excluded.current_count)
                END,
                current_count = telemetry_rollups_5m.current_count + excluded.current_count,
                avg_power_w = CASE
                    WHEN excluded.power_count = 0 THEN telemetry_rollups_5m.avg_power_w
                    WHEN telemetry_rollups_5m.power_count = 0 THEN excluded.avg_power_w
                    ELSE (
                        telemetry_rollups_5m.avg_power_w * telemetry_rollups_5m.power_count
                        + excluded.avg_power_w * excluded.power_count
                    ) / (telemetry_rollups_5m.power_count + excluded.power_count)
                END,
                power_count = telemetry_rollups_5m.power_count + excluded.power_count,
                avg_energy_kwh = CASE
                    WHEN excluded.energy_count = 0 THEN telemetry_rollups_5m.avg_energy_kwh
                    WHEN telemetry_rollups_5m.energy_count = 0 THEN excluded.avg_energy_kwh
                    ELSE (
                        telemetry_rollups_5m.avg_energy_kwh * telemetry_rollups_5m.energy_count
                        + excluded.avg_energy_kwh * excluded.energy_count
                    ) / (telemetry_rollups_5m.energy_count + excluded.energy_count)
                END,
                energy_count = telemetry_rollups_5m.energy_count + excluded.energy_count,
                event_count = telemetry_rollups_5m.event_count + 1"
        }
        RollupTable::OneHour => {
            "INSERT INTO telemetry_rollups_1h (
                bucket_at, tenant_id, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, bucket_at, device_id) DO UPDATE SET
                avg_temperature_c = CASE
                    WHEN excluded.temperature_count = 0 THEN telemetry_rollups_1h.avg_temperature_c
                    WHEN telemetry_rollups_1h.temperature_count = 0 THEN excluded.avg_temperature_c
                    ELSE (
                        telemetry_rollups_1h.avg_temperature_c * telemetry_rollups_1h.temperature_count
                        + excluded.avg_temperature_c * excluded.temperature_count
                    ) / (telemetry_rollups_1h.temperature_count + excluded.temperature_count)
                END,
                temperature_count = telemetry_rollups_1h.temperature_count + excluded.temperature_count,
                avg_humidity_pct = CASE
                    WHEN excluded.humidity_count = 0 THEN telemetry_rollups_1h.avg_humidity_pct
                    WHEN telemetry_rollups_1h.humidity_count = 0 THEN excluded.avg_humidity_pct
                    ELSE (
                        telemetry_rollups_1h.avg_humidity_pct * telemetry_rollups_1h.humidity_count
                        + excluded.avg_humidity_pct * excluded.humidity_count
                    ) / (telemetry_rollups_1h.humidity_count + excluded.humidity_count)
                END,
                humidity_count = telemetry_rollups_1h.humidity_count + excluded.humidity_count,
                avg_voltage_v = CASE
                    WHEN excluded.voltage_count = 0 THEN telemetry_rollups_1h.avg_voltage_v
                    WHEN telemetry_rollups_1h.voltage_count = 0 THEN excluded.avg_voltage_v
                    ELSE (
                        telemetry_rollups_1h.avg_voltage_v * telemetry_rollups_1h.voltage_count
                        + excluded.avg_voltage_v * excluded.voltage_count
                    ) / (telemetry_rollups_1h.voltage_count + excluded.voltage_count)
                END,
                voltage_count = telemetry_rollups_1h.voltage_count + excluded.voltage_count,
                avg_current_a = CASE
                    WHEN excluded.current_count = 0 THEN telemetry_rollups_1h.avg_current_a
                    WHEN telemetry_rollups_1h.current_count = 0 THEN excluded.avg_current_a
                    ELSE (
                        telemetry_rollups_1h.avg_current_a * telemetry_rollups_1h.current_count
                        + excluded.avg_current_a * excluded.current_count
                    ) / (telemetry_rollups_1h.current_count + excluded.current_count)
                END,
                current_count = telemetry_rollups_1h.current_count + excluded.current_count,
                avg_power_w = CASE
                    WHEN excluded.power_count = 0 THEN telemetry_rollups_1h.avg_power_w
                    WHEN telemetry_rollups_1h.power_count = 0 THEN excluded.avg_power_w
                    ELSE (
                        telemetry_rollups_1h.avg_power_w * telemetry_rollups_1h.power_count
                        + excluded.avg_power_w * excluded.power_count
                    ) / (telemetry_rollups_1h.power_count + excluded.power_count)
                END,
                power_count = telemetry_rollups_1h.power_count + excluded.power_count,
                avg_energy_kwh = CASE
                    WHEN excluded.energy_count = 0 THEN telemetry_rollups_1h.avg_energy_kwh
                    WHEN telemetry_rollups_1h.energy_count = 0 THEN excluded.avg_energy_kwh
                    ELSE (
                        telemetry_rollups_1h.avg_energy_kwh * telemetry_rollups_1h.energy_count
                        + excluded.avg_energy_kwh * excluded.energy_count
                    ) / (telemetry_rollups_1h.energy_count + excluded.energy_count)
                END,
                energy_count = telemetry_rollups_1h.energy_count + excluded.energy_count,
                event_count = telemetry_rollups_1h.event_count + 1"
        }
    };
    let values = [
        metrics.temperature_c,
        metrics.humidity_pct,
        metrics.voltage_v,
        metrics.current_a,
        metrics.power_w,
        metrics.energy_kwh,
    ];
    let mut query = sqlx::query(query)
        .bind(bucket_at)
        .bind(tenant_id.to_string())
        .bind(device_id);
    for value in values {
        query = query.bind(value).bind(i64::from(value.is_some()));
    }
    query.execute(&mut **transaction).await?;
    Ok(())
}
