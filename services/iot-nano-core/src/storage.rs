#![forbid(unsafe_code)]

use std::{fs, path::Path, time::Duration};

use chrono::{DateTime, TimeZone, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use iot_sqldb_common::{SqlitePoolError, open_owned_sqlite_pool};
use serde_json::Value;
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;

const CORE_SQLITE_APPLICATION_ID: i64 = 0x434F_5231;

const SQLITE_SCHEMA: &str = include_str!("core_sqlite_schema.sql");

const CORE_SQLITE_SCHEMA_TABLES: &[&str] = &[
    "telemetry",
    "device_runtime_state",
    "gateway_event_receipts",
    "telemetry_rollups_5m",
    "telemetry_rollups_1h",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
];

const CORE_SQLITE_TENANT_TABLES: &[&str] = &[
    "telemetry",
    "device_runtime_state",
    "gateway_event_receipts",
    "telemetry_rollups_5m",
    "telemetry_rollups_1h",
];

#[derive(Clone)]
pub struct CoreSqliteStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutboxState {
    Queued,
    Leased,
    PublishedToBroker,
    Responded,
    Expired,
    Failed,
}

impl CommandOutboxState {
    fn from_database(value: &str) -> Result<Self, CoreSqliteStoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "published_to_broker" => Ok(Self::PublishedToBroker),
            "responded" => Ok(Self::Responded),
            "expired" => Ok(Self::Expired),
            "failed" => Ok(Self::Failed),
            _ => Err(CoreSqliteStoreError::InvalidCommandState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCommandOutboxEntry {
    pub id: String,
    pub device_id: String,
    pub method: String,
    pub params: String,
    pub mode: RpcMode,
    pub expires_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutboxRecord {
    pub id: String,
    pub device_id: String,
    pub method: String,
    pub params: String,
    pub mode: RpcMode,
    pub state: CommandOutboxState,
    pub expires_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub response: Option<String>,
    pub responded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionResult {
    pub raw_rows: u64,
    pub rollup_rows: u64,
    pub event_evaluation_rows: u64,
    pub notification_rows: u64,
    pub resolved_incident_rows: u64,
}

async fn pre_tenant_core_sqlite_table(pool: &SqlitePool) -> Result<Option<String>, sqlx::Error> {
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT name
         FROM sqlite_master
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )
    .fetch_all(pool)
    .await?;
    let core_tables = tables
        .into_iter()
        .filter(|table| CORE_SQLITE_SCHEMA_TABLES.contains(&table.as_str()))
        .collect::<Vec<_>>();
    let Some(first_table) = core_tables.first() else {
        return Ok(None);
    };
    if !core_tables
        .iter()
        .any(|table| table.as_str() == "telemetry")
    {
        return Ok(Some(first_table.clone()));
    }
    for &table in CORE_SQLITE_TENANT_TABLES {
        if core_tables
            .iter()
            .any(|existing| existing.as_str() == table)
        {
            let has_tenant_id: i64 = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pragma_table_info(?) WHERE name = 'tenant_id'
                 )",
            )
            .bind(table)
            .fetch_one(pool)
            .await?;
            if has_tenant_id == 0 {
                return Ok(Some(table.to_owned()));
            }
        }
    }
    Ok(None)
}

async fn reject_pre_tenant_core_sqlite(
    path: &Path,
    busy_timeout_ms: u64,
) -> Result<(), CoreSqliteStoreError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(SqlitePoolError::Io(error).into()),
    };
    if !metadata.is_file() {
        return Err(CoreSqliteStoreError::InvalidConfiguration);
    }
    if metadata.len() == 0 {
        return Ok(());
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .read_only(true)
        .busy_timeout(Duration::from_millis(busy_timeout_ms));
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let table = pre_tenant_core_sqlite_table(&pool).await;
    pool.close().await;
    if let Some(table) = table? {
        return Err(CoreSqliteStoreError::ResetRequired { table });
    }
    Ok(())
}

impl CoreSqliteStore {
    pub async fn open(configuration: &StorageConfiguration) -> Result<Self, CoreSqliteStoreError> {
        if configuration.storage != DatabaseStorage::Sqlite {
            return Err(CoreSqliteStoreError::InvalidConfiguration);
        }
        let path = configuration
            .sqlite_path
            .as_ref()
            .ok_or(CoreSqliteStoreError::InvalidConfiguration)?;
        reject_pre_tenant_core_sqlite(path, configuration.sqlite_busy_timeout_ms).await?;
        let pool = open_owned_sqlite_pool(
            path,
            configuration.sqlite_busy_timeout_ms,
            CORE_SQLITE_APPLICATION_ID,
        )
        .await
        .map_err(|error| match error {
            SqlitePoolError::ForeignOwnership => CoreSqliteStoreError::ForeignOwnership,
            error => CoreSqliteStoreError::Common(error),
        })?;
        sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await?;
        migrate_command_outbox_schema(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn enqueue_command(
        &self,
        command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, CoreSqliteStoreError> {
        let result = sqlx::query(
            "INSERT INTO command_outbox (
                id, device_id, method, params, mode, expires_at, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&command.id)
        .bind(&command.device_id)
        .bind(&command.method)
        .bind(&command.params)
        .bind(command_mode_value(command.mode))
        .bind(command.expires_at.to_rfc3339())
        .bind(command.next_attempt_at.to_rfc3339())
        .fetch_optional(&self.pool)
        .await;
        match result {
            Ok(Some(row)) => command_outbox_record(row),
            Ok(None) => {
                let row = sqlx::query(
                    "SELECT
                        id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                        lease_until, attempt_count, last_error, published_at, response, responded_at
                     FROM command_outbox
                     WHERE id = ?",
                )
                .bind(&command.id)
                .fetch_optional(&self.pool)
                .await?;
                let row = row.ok_or(CoreSqliteStoreError::Database(sqlx::Error::RowNotFound))?;
                let record = command_outbox_record(row)?;
                let existing_params = serde_json::from_str::<Value>(&record.params).ok();
                let requested_params = serde_json::from_str::<Value>(&command.params).ok();
                if existing_params.as_ref().is_some_and(|existing_params| {
                    requested_params.as_ref().is_some_and(|requested_params| {
                        command_payload_matches(
                            &record.device_id,
                            &record.method,
                            existing_params,
                            record.mode,
                            &command.device_id,
                            &command.method,
                            requested_params,
                            command.mode,
                        )
                    })
                }) {
                    Ok(record)
                } else {
                    Err(CoreSqliteStoreError::CommandConflict)
                }
            }
            Err(error) => Err(CoreSqliteStoreError::Database(error)),
        }
    }

    pub async fn claim_commands(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, CoreSqliteStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = now.to_rfc3339();
        // This single write statement makes claiming atomic across SQLite connections.
        let rows = sqlx::query(
            "WITH due AS (
                SELECT id
                FROM command_outbox
                WHERE expires_at > ?
                  AND (
                      (state = 'queued' AND next_attempt_at <= ?)
                      OR (state = 'leased' AND lease_until <= ?)
                  )
                ORDER BY next_attempt_at, created_at, id
                LIMIT ?
             )
             UPDATE command_outbox
             SET state = 'leased',
                 lease_until = ?,
                 attempt_count = attempt_count + 1
             WHERE id IN (SELECT id FROM due)
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn mark_command_published(
        &self,
        command_id: &str,
        published_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, CoreSqliteStoreError> {
        let published_at = published_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'published_to_broker',
                 published_at = ?,
                 lease_until = NULL
             WHERE id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&published_at)
        .bind(command_id)
        .bind(&published_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn mark_command_failed(
        &self,
        command_id: &str,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, CoreSqliteStoreError> {
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'failed',
                 last_error = ?,
                 lease_until = NULL
             WHERE id = ?
               AND state = 'leased'
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(error)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn release_command_for_retry(
        &self,
        command_id: &str,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, CoreSqliteStoreError> {
        let next_attempt_at = next_attempt_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'queued',
                 next_attempt_at = ?,
                 last_error = ?,
                 lease_until = NULL
             WHERE id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(command_id)
        .bind(&next_attempt_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn expire_commands(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CommandOutboxRecord>, CoreSqliteStoreError> {
        let rows = sqlx::query(
            "UPDATE command_outbox
             SET state = 'expired',
                 lease_until = NULL
             WHERE (
                    state IN ('queued', 'leased')
                    OR (state = 'published_to_broker' AND mode = 'two_way')
                   )
               AND expires_at <= ?
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(now.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn write_telemetry(
        &self,
        tenant_id: uuid::Uuid,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, CoreSqliteStoreError> {
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
    ) -> Result<bool, CoreSqliteStoreError> {
        let received_at = received_at.to_rfc3339();
        sqlx::query(
            "INSERT INTO device_runtime_state (tenant_id, device_id, last_seen_at)
             VALUES (?, ?, ?)
             ON CONFLICT(tenant_id, device_id) DO UPDATE SET
                 last_seen_at = CASE
                     WHEN device_runtime_state.last_seen_at IS NULL
                       OR excluded.last_seen_at > device_runtime_state.last_seen_at
                     THEN excluded.last_seen_at
                     ELSE device_runtime_state.last_seen_at
                 END",
        )
        .bind(tenant_id.to_string())
        .bind(&event.device_id)
        .bind(&received_at)
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
        .bind(i64::try_from(event.sequence).map_err(|_| CoreSqliteStoreError::SequenceOverflow)?)
        .bind(
            serde_json::to_string(&event.measurements)
                .map_err(CoreSqliteStoreError::Serialization)?,
        )
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
    ) -> Result<RetentionResult, CoreSqliteStoreError> {
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

async fn migrate_command_outbox_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let columns = sqlx::query("PRAGMA table_info(command_outbox)")
        .fetch_all(pool)
        .await?;
    for column in columns {
        if column.try_get::<String, _>("name")? == "mode" {
            refresh_command_outbox_expiring_index(pool).await?;
            return Ok(());
        }
    }

    let mut transaction = pool.begin().await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_due_index")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_expiring_index")
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(
        "CREATE TABLE command_outbox_rebuild (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            method TEXT NOT NULL CHECK (trim(method) <> ''),
            params TEXT NOT NULL DEFAULT '{}',
            mode TEXT NOT NULL DEFAULT 'one_way'
                CHECK (mode IN ('one_way', 'two_way')),
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
            expires_at TEXT NOT NULL,
            next_attempt_at TEXT NOT NULL,
            lease_until TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
            last_error TEXT,
            published_at TEXT,
            response TEXT,
            responded_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO command_outbox_rebuild (
            id, device_id, method, params, mode, state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, created_at
         )
         SELECT
            id, device_id, method, params, 'one_way', state, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, created_at
         FROM command_outbox",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query("DROP TABLE command_outbox")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("ALTER TABLE command_outbox_rebuild RENAME TO command_outbox")
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_due_index
         ON command_outbox (state, next_attempt_at)
         WHERE state = 'queued'",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    refresh_command_outbox_expiring_index(pool).await
}

async fn refresh_command_outbox_expiring_index(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut connection = pool.acquire().await?;
    sqlx::query("DROP INDEX IF EXISTS command_outbox_expiring_index")
        .execute(&mut *connection)
        .await?;
    sqlx::query(
        "CREATE INDEX command_outbox_expiring_index
         ON command_outbox (expires_at)
         WHERE state IN ('queued', 'leased')
            OR (state = 'published_to_broker' AND mode = 'two_way')",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

fn command_outbox_record(row: SqliteRow) -> Result<CommandOutboxRecord, CoreSqliteStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        method: row.try_get("method")?,
        params: row.try_get("params")?,
        mode: command_mode_from_database(&row.try_get::<String, _>("mode")?)?,
        state: CommandOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        expires_at: command_timestamp(&row, "expires_at")?,
        next_attempt_at: command_timestamp(&row, "next_attempt_at")?,
        lease_until: command_optional_timestamp(&row, "lease_until")?,
        attempt_count: row.try_get("attempt_count")?,
        last_error: row.try_get("last_error")?,
        published_at: command_optional_timestamp(&row, "published_at")?,
        response: row.try_get("response")?,
        responded_at: command_optional_timestamp(&row, "responded_at")?,
    })
}

fn command_mode_value(mode: RpcMode) -> &'static str {
    match mode {
        RpcMode::OneWay => "one_way",
        RpcMode::TwoWay => "two_way",
    }
}

fn command_mode_from_database(value: &str) -> Result<RpcMode, CoreSqliteStoreError> {
    match value {
        "one_way" => Ok(RpcMode::OneWay),
        "two_way" => Ok(RpcMode::TwoWay),
        _ => Err(CoreSqliteStoreError::InvalidCommandState(value.to_owned())),
    }
}

fn command_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, CoreSqliteStoreError> {
    let value: String = row.try_get(column)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|source| CoreSqliteStoreError::InvalidCommandTimestamp {
            column,
            value,
            source,
        })
}

fn command_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, CoreSqliteStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .map_err(|source| CoreSqliteStoreError::InvalidCommandTimestamp {
                    column,
                    value,
                    source,
                })
        })
        .transpose()
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

#[derive(Debug, Error)]
pub enum CoreSqliteStoreError {
    #[error("SQLite storage configuration is invalid")]
    InvalidConfiguration,
    #[error("SQLite file belongs to another service")]
    ForeignOwnership,
    #[error(
        "core SQLite schema table {table:?} predates tenant scoping; reset the development database before starting iot-nano"
    )]
    ResetRequired { table: String },
    #[error("invalid command outbox state: {0}")]
    InvalidCommandState(String),
    #[error("invalid command outbox {column} timestamp: {value}")]
    InvalidCommandTimestamp {
        column: &'static str,
        value: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("telemetry sequence does not fit SQLite INTEGER")]
    SequenceOverflow,
    #[error("telemetry measurements cannot be serialized")]
    Serialization(#[source] serde_json::Error),
    #[error("command payload conflicts with the existing command ID")]
    CommandConflict,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Common(#[from] SqlitePoolError),
}

pub(crate) fn command_payload_matches(
    existing_device_id: &str,
    existing_method: &str,
    existing_params: &Value,
    existing_mode: RpcMode,
    requested_device_id: &str,
    requested_method: &str,
    requested_params: &Value,
    requested_mode: RpcMode,
) -> bool {
    existing_device_id == requested_device_id
        && existing_method == requested_method
        && existing_params == requested_params
        && existing_mode == requested_mode
}
