#![forbid(unsafe_code)]

use std::{fs, time::Duration};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use chrono::{DateTime, TimeZone, Utc};
use iot_core::{DatabaseStorage, RpcMode, StorageConfiguration, TelemetryEvent};
use sqlx::{
    PgPool, Row, Sqlite, SqlitePool, Transaction,
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
};
use thiserror::Error;

const API_POSTGRES_SCHEMA: &str =
    include_str!("../../../services/iot-nano-api/migrations/0001_api.sql");
const CORE_POSTGRES_SCHEMA: &str =
    include_str!("../../../services/iot-nano-core/migrations/0001_core.sql");

const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    display_name TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT,
    asset_id TEXT,
    device_profile_id TEXT,
    deleted_at TEXT,
    is_gateway INTEGER NOT NULL DEFAULT 0,
    gateway_device_id TEXT REFERENCES devices(device_id) ON DELETE RESTRICT,
    gateway_last_read_at TEXT,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    claimed_at TEXT,
    CHECK (
        (is_gateway = 1 AND gateway_device_id IS NULL)
        OR (is_gateway = 0 AND gateway_device_id IS NOT device_id)
    )
);

CREATE TABLE IF NOT EXISTS telemetry (
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    boot_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    measurements TEXT NOT NULL,
    topic TEXT NOT NULL,
    gateway_device_id TEXT,
    UNIQUE (event_at, device_id, boot_id, sequence)
);
CREATE INDEX IF NOT EXISTS telemetry_device_event_at_index
    ON telemetry (device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_gateway_device_event_at_index
    ON telemetry (gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (gateway_device_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS telemetry_rollups_5m (
    bucket_at TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    event_count INTEGER NOT NULL,
    avg_temperature_c REAL,
    temperature_count INTEGER NOT NULL DEFAULT 0,
    avg_humidity_pct REAL,
    humidity_count INTEGER NOT NULL DEFAULT 0,
    avg_voltage_v REAL,
    voltage_count INTEGER NOT NULL DEFAULT 0,
    avg_current_a REAL,
    current_count INTEGER NOT NULL DEFAULT 0,
    avg_power_w REAL,
    power_count INTEGER NOT NULL DEFAULT 0,
    avg_energy_kwh REAL,
    energy_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_at, device_id)
);
CREATE TABLE IF NOT EXISTS telemetry_rollups_1h (
    bucket_at TEXT NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    event_count INTEGER NOT NULL,
    avg_temperature_c REAL,
    temperature_count INTEGER NOT NULL DEFAULT 0,
    avg_humidity_pct REAL,
    humidity_count INTEGER NOT NULL DEFAULT 0,
    avg_voltage_v REAL,
    voltage_count INTEGER NOT NULL DEFAULT 0,
    avg_current_a REAL,
    current_count INTEGER NOT NULL DEFAULT 0,
    avg_power_w REAL,
    power_count INTEGER NOT NULL DEFAULT 0,
    avg_energy_kwh REAL,
    energy_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_at, device_id)
);

CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user'
        CHECK (account_class IN ('system', 'admin', 'user')),
    default_app TEXT NOT NULL DEFAULT '/apps/powermonitor',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    app_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, app_key)
);

CREATE TABLE IF NOT EXISTS asset_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    fields TEXT NOT NULL DEFAULT '{}',
    dashboard_defaults TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS device_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    telemetry_schema TEXT NOT NULL DEFAULT '{}',
    metric_mapping TEXT NOT NULL DEFAULT '{}',
    reporting_settings TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    asset_profile_id TEXT REFERENCES asset_profiles(id) ON DELETE SET NULL,
    parent_asset_id TEXT REFERENCES assets(id) ON DELETE SET NULL,
    owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (parent_asset_id, name)
);
CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
    ON devices (owner_user_id)
    WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
    ON assets (owner_user_id)
    WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS resource_shares (
    id TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
    inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'active', 'declined', 'cancelled', 'expired')
    ),
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    responded_at TEXT,
    expires_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
    ON resource_shares (resource_type, resource_id, target_user_id)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS resource_shares_target_state_index
    ON resource_shares (target_user_id, state, created_at DESC);
CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index
    ON resource_shares (resource_type, resource_id, state);

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    actor_account_class TEXT NOT NULL
        CHECK (actor_account_class IN ('system', 'admin', 'user')),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_value TEXT,
    after_value TEXT,
    request_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS audit_events_resource_index
    ON audit_events (resource_type, resource_id, created_at DESC);
CREATE INDEX IF NOT EXISTS audit_events_actor_index
    ON audit_events (actor_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS device_claim_codes (
    device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    issued_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_at TEXT
);
CREATE INDEX IF NOT EXISTS device_claim_codes_expiry_index
    ON device_claim_codes (expires_at)
    WHERE used_at IS NULL;

CREATE TABLE IF NOT EXISTS device_tokens (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    token_prefix TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL,
    token_ciphertext TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS device_tokens_one_active_per_device
    ON device_tokens (device_id)
    WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS device_tokens_active_prefix_index
    ON device_tokens (token_prefix)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    device_id TEXT,
    metric_key TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    comparison TEXT NOT NULL,
    threshold REAL NOT NULL,
    window_seconds INTEGER,
    for_seconds INTEGER NOT NULL DEFAULT 300,
    resolve_after_seconds INTEGER NOT NULL DEFAULT 300,
    reopen_grace_seconds INTEGER NOT NULL DEFAULT 3600,
    hysteresis REAL,
    severity TEXT NOT NULL DEFAULT 'warning',
    reminder_interval_seconds INTEGER NOT NULL DEFAULT 86400,
    archived_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (rule_type, device_id)
    WHERE enabled = 1;

CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    event_at TEXT NOT NULL,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence TEXT NOT NULL,
    PRIMARY KEY (rule_id, event_at, device_id, boot_id, sequence)
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL,
    condition_started_at TEXT NOT NULL,
    recovery_started_at TEXT,
    opened_at TEXT,
    resolved_at TEXT,
    acknowledged_at TEXT,
    acknowledged_by TEXT,
    last_value REAL,
    last_notified_at TEXT,
    last_reminder_at TEXT,
    state_version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (rule_id, device_id)
    WHERE status IN ('pending', 'open');

CREATE TABLE IF NOT EXISTS notification_outbox (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES alert_incidents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (state, next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS command_outbox (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
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
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (state, next_attempt_at)
    WHERE state = 'queued';
"#;

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

#[derive(Clone)]
pub enum PlatformStore {
    Sqlite(SqliteStore),
    Timescale(PgPool),
}

#[derive(Debug, Error)]
pub enum PlatformStoreError {
    #[error("platform storage configuration is incomplete")]
    InvalidConfiguration,
    #[error(transparent)]
    Sqlite(#[from] SqliteStoreError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

impl PlatformStore {
    pub async fn open(configuration: &StorageConfiguration) -> Result<Self, PlatformStoreError> {
        match configuration.storage {
            DatabaseStorage::Sqlite => Ok(Self::Sqlite(SqliteStore::open(configuration).await?)),
            DatabaseStorage::Timescale => {
                let database_url = configuration
                    .database_url
                    .as_deref()
                    .ok_or(PlatformStoreError::InvalidConfiguration)?;
                let pool = PgPoolOptions::new()
                    .max_connections(8)
                    .connect(database_url)
                    .await?;
                migrate_platform_timescale(&pool).await?;
                Ok(Self::Timescale(pool))
            }
        }
    }

    pub fn sqlite_pool(&self) -> Option<&SqlitePool> {
        match self {
            Self::Sqlite(store) => Some(store.pool()),
            Self::Timescale(_) => None,
        }
    }

    pub fn timescale_pool(&self) -> Option<&PgPool> {
        match self {
            Self::Sqlite(_) => None,
            Self::Timescale(pool) => Some(pool),
        }
    }
}

async fn migrate_platform_timescale(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS iot_nano")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO iot_nano, public")
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(API_POSTGRES_SCHEMA)
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(CORE_POSTGRES_SCHEMA)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await
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
    fn from_database(value: &str) -> Result<Self, SqliteStoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "published_to_broker" => Ok(Self::PublishedToBroker),
            "responded" => Ok(Self::Responded),
            "expired" => Ok(Self::Expired),
            "failed" => Ok(Self::Failed),
            _ => Err(SqliteStoreError::InvalidCommandState(value.to_owned())),
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

impl SqliteStore {
    pub async fn open(configuration: &StorageConfiguration) -> Result<Self, SqliteStoreError> {
        if configuration.storage != DatabaseStorage::Sqlite {
            return Err(SqliteStoreError::InvalidConfiguration);
        }
        let path = configuration
            .sqlite_path
            .as_ref()
            .ok_or(SqliteStoreError::InvalidConfiguration)?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            && !parent.exists()
        {
            fs::create_dir_all(parent)?;
            #[cfg(unix)]
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_millis(configuration.sqlite_busy_timeout_ms));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&pool)
            .await?;
        sqlx::raw_sql(SQLITE_SCHEMA).execute(&pool).await?;
        migrate_command_outbox_schema(&pool).await?;
        migrate_resource_authorization_schema(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn enqueue_command(
        &self,
        command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, SqliteStoreError> {
        let row = sqlx::query(
            "INSERT INTO command_outbox (
                id, device_id, method, params, mode, expires_at, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(command.id)
        .bind(command.device_id)
        .bind(command.method)
        .bind(command.params)
        .bind(command_mode_value(command.mode))
        .bind(command.expires_at.to_rfc3339())
        .bind(command.next_attempt_at.to_rfc3339())
        .fetch_one(&self.pool)
        .await?;
        command_outbox_record(row)
    }

    pub async fn claim_commands(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, SqliteStoreError> {
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
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
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
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
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
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
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
    ) -> Result<Vec<CommandOutboxRecord>, SqliteStoreError> {
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

    pub async fn mark_command_responded(
        &self,
        command_id: &str,
        device_id: &str,
        token_id: &str,
        response: &str,
        responded_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let responded_at = responded_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE command_outbox AS command
             SET state = 'responded',
                 response = ?,
                 responded_at = ?,
                 lease_until = NULL
             WHERE command.id = ?
               AND command.device_id = ?
               AND command.mode = 'two_way'
               AND command.state = 'published_to_broker'
               AND command.expires_at > ?
               AND EXISTS (
                    SELECT 1
                    FROM device_tokens
                    WHERE id = ?
                      AND device_id = command.device_id
                      AND revoked_at IS NULL
               )
             RETURNING
                id, device_id, method, params, mode, state, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(response)
        .bind(&responded_at)
        .bind(command_id)
        .bind(device_id)
        .bind(&responded_at)
        .bind(token_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn write_telemetry(
        &self,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = self
            .write_telemetry_in_transaction(&mut transaction, event, received_at, topic)
            .await?;
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn write_telemetry_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event: &TelemetryEvent,
        received_at: DateTime<Utc>,
        topic: &str,
    ) -> Result<bool, SqliteStoreError> {
        let received_at = received_at.to_rfc3339();
        sqlx::query(
            "INSERT INTO devices (device_id, last_seen_at)
             VALUES (?, ?)
             ON CONFLICT(device_id) DO UPDATE SET
                 last_seen_at = CASE
                     WHEN devices.last_seen_at IS NULL OR excluded.last_seen_at > devices.last_seen_at
                     THEN excluded.last_seen_at
                     ELSE devices.last_seen_at
                 END",
        )
        .bind(&event.device_id)
        .bind(&received_at)
        .execute(transaction.as_mut())
        .await?;
        let insert = sqlx::query(
            "INSERT OR IGNORE INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic,
                gateway_device_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.event_at.to_rfc3339())
        .bind(&received_at)
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
                &event.device_id,
                metrics,
            )
            .await?;
            upsert_rollup(
                transaction,
                RollupTable::OneHour,
                bucket_start(event.event_at, 60 * 60),
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
            device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
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

async fn migrate_resource_authorization_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    if !sqlite_table_has_column(pool, "users", "account_class").await? {
        sqlx::query(
            "ALTER TABLE users
             ADD COLUMN account_class TEXT NOT NULL DEFAULT 'user'
             CHECK (account_class IN ('system', 'admin', 'user'))",
        )
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "UPDATE users
         SET account_class = CASE role
             WHEN 'admin' THEN 'admin'
             ELSE 'user'
         END
         WHERE account_class IS NULL
            OR account_class NOT IN ('system', 'admin', 'user')
            OR (account_class = 'user' AND role = 'admin')",
    )
    .execute(pool)
    .await?;

    if !sqlite_table_has_column(pool, "devices", "owner_user_id").await? {
        sqlx::query(
            "ALTER TABLE devices
             ADD COLUMN owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL",
        )
        .execute(pool)
        .await?;
    }
    if !sqlite_table_has_column(pool, "devices", "claimed_at").await? {
        sqlx::query("ALTER TABLE devices ADD COLUMN claimed_at TEXT")
            .execute(pool)
            .await?;
    }
    if !sqlite_table_has_column(pool, "assets", "owner_user_id").await? {
        sqlx::query(
            "ALTER TABLE assets
             ADD COLUMN owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL",
        )
        .execute(pool)
        .await?;
    }

    sqlx::raw_sql(
        "CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
             ON devices (owner_user_id)
             WHERE owner_user_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
             ON assets (owner_user_id)
             WHERE owner_user_id IS NOT NULL;
         CREATE TABLE IF NOT EXISTS resource_shares (
             id TEXT PRIMARY KEY,
             resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
             resource_id TEXT NOT NULL,
             target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
             permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
             inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
             state TEXT NOT NULL CHECK (
                 state IN ('pending', 'active', 'declined', 'cancelled', 'expired')
             ),
             created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
             created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
             responded_at TEXT,
             expires_at TEXT
         );
         CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
             ON resource_shares (resource_type, resource_id, target_user_id)
             WHERE state = 'pending';
         CREATE INDEX IF NOT EXISTS resource_shares_target_state_index
             ON resource_shares (target_user_id, state, created_at DESC);
         CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index
             ON resource_shares (resource_type, resource_id, state);
         CREATE TABLE IF NOT EXISTS audit_events (
             id TEXT PRIMARY KEY,
             actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
             actor_account_class TEXT NOT NULL
                 CHECK (actor_account_class IN ('system', 'admin', 'user')),
             resource_type TEXT NOT NULL,
             resource_id TEXT NOT NULL,
             action TEXT NOT NULL,
             before_value TEXT,
             after_value TEXT,
             request_id TEXT,
             created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
         );
         CREATE INDEX IF NOT EXISTS audit_events_resource_index
             ON audit_events (resource_type, resource_id, created_at DESC);
         CREATE INDEX IF NOT EXISTS audit_events_actor_index
             ON audit_events (actor_user_id, created_at DESC);
         CREATE TABLE IF NOT EXISTS device_claim_codes (
             device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
             code_hash TEXT NOT NULL,
             expires_at TEXT NOT NULL,
             issued_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
             issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
             used_at TEXT
         );
         CREATE INDEX IF NOT EXISTS device_claim_codes_expiry_index
             ON device_claim_codes (expires_at)
             WHERE used_at IS NULL;",
    )
    .execute(pool)
    .await
    .map(|_| ())
}

async fn sqlite_table_has_column(
    pool: &SqlitePool,
    table: &str,
    column_name: &str,
) -> Result<bool, sqlx::Error> {
    let statement = match table {
        "users" => "PRAGMA table_info(users)",
        "devices" => "PRAGMA table_info(devices)",
        "assets" => "PRAGMA table_info(assets)",
        _ => unreachable!("only audited schema table names may be queried"),
    };
    let columns = sqlx::query(statement).fetch_all(pool).await?;
    for column in columns {
        if column.try_get::<String, _>("name")? == column_name {
            return Ok(true);
        }
    }
    Ok(false)
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

fn command_outbox_record(row: SqliteRow) -> Result<CommandOutboxRecord, SqliteStoreError> {
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

fn command_mode_from_database(value: &str) -> Result<RpcMode, SqliteStoreError> {
    match value {
        "one_way" => Ok(RpcMode::OneWay),
        "two_way" => Ok(RpcMode::TwoWay),
        _ => Err(SqliteStoreError::InvalidCommandState(value.to_owned())),
    }
}

fn command_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, SqliteStoreError> {
    let value: String = row.try_get(column)?;
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|source| SqliteStoreError::InvalidCommandTimestamp {
            column,
            value,
            source,
        })
}

fn command_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, SqliteStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .map_err(|source| SqliteStoreError::InvalidCommandTimestamp {
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
    device_id: &str,
    metrics: MetricAverages,
) -> Result<(), sqlx::Error> {
    let query = match table {
        RollupTable::FiveMinute => {
            "INSERT INTO telemetry_rollups_5m (
                bucket_at, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(bucket_at, device_id) DO UPDATE SET
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
                bucket_at, device_id, event_count,
                avg_temperature_c, temperature_count,
                avg_humidity_pct, humidity_count,
                avg_voltage_v, voltage_count,
                avg_current_a, current_count,
                avg_power_w, power_count,
                avg_energy_kwh, energy_count
             ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(bucket_at, device_id) DO UPDATE SET
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
    let mut query = sqlx::query(query).bind(bucket_at).bind(device_id);
    for value in values {
        query = query.bind(value).bind(i64::from(value.is_some()));
    }
    query.execute(&mut **transaction).await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum SqliteStoreError {
    #[error("SQLite storage configuration is invalid")]
    InvalidConfiguration,
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
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Filesystem(#[from] std::io::Error),
}
