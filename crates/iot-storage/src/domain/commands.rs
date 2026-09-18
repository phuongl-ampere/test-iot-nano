use std::{future::Future, pin::Pin};

use chrono::{DateTime, NaiveDateTime, Utc};
use iot_core::RpcMode;
use sqlx::{
    PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction, postgres::PgRow, sqlite::SqliteRow,
    types::Json,
};

use crate::{
    AccountClass, CommandLifecycleRepository, CommandOutboxRecord, CommandOutboxState,
    CommandRepository, NewCommandOutboxEntry, NewNotificationOutboxEntry, NotificationKind,
    NotificationOutboxRecord, NotificationOutboxState, NotificationRepository, PlatformStore,
    PlatformStoreError, SqliteStore, SqliteStoreError, authorization_account_class,
    canonical_notification, canonical_postgres_timestamp, timescale_tenant_device_is_locked,
};

impl PlatformStore {
    pub async fn enqueue_command(
        &self,
        mut command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, PlatformStoreError> {
        let id = uuid::Uuid::parse_str(&command.id)
            .map_err(|_| PlatformStoreError::InvalidCommandId(command.id.clone()))?;
        let params = serde_json::from_str::<serde_json::Value>(&command.params)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        command.id = id.to_string();
        command.params =
            serde_json::to_string(&params).map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        command.expires_at = canonical_postgres_timestamp(command.expires_at);
        command.next_attempt_at = canonical_postgres_timestamp(command.next_attempt_at);

        match self {
            Self::Sqlite(store) => {
                self.require_sqlite_tenant_device(command.tenant_id, &command.device_id)
                    .await?;
                enqueue_sqlite_platform_command(store, &command).await
            }
            Self::Timescale(pool) => {
                enqueue_timescale_platform_command(pool, id, params, &command).await
            }
        }
    }

    pub async fn enqueue_authorized_command(
        &self,
        user_id: uuid::Uuid,
        mut command: NewCommandOutboxEntry,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        let id = uuid::Uuid::parse_str(&command.id)
            .map_err(|_| PlatformStoreError::InvalidCommandId(command.id.clone()))?;
        let params = serde_json::from_str::<serde_json::Value>(&command.params)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        command.id = id.to_string();
        command.params =
            serde_json::to_string(&params).map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        command.expires_at = canonical_postgres_timestamp(command.expires_at);
        command.next_attempt_at = canonical_postgres_timestamp(command.next_attempt_at);

        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                if !sqlite_user_can_issue_command(
                    &mut transaction,
                    user_id,
                    command.tenant_id,
                    &command.device_id,
                )
                .await?
                {
                    return Ok(None);
                }
                let record =
                    enqueue_sqlite_platform_command_in_transaction(&mut transaction, &command)
                        .await?;
                transaction.commit().await?;
                Ok(Some(record))
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    .execute(&mut *transaction)
                    .await?;
                if !timescale_user_can_issue_command(
                    &mut transaction,
                    user_id,
                    command.tenant_id,
                    &command.device_id,
                )
                .await?
                {
                    return Ok(None);
                }
                let record = enqueue_timescale_platform_command_in_transaction(
                    &mut transaction,
                    id,
                    &params,
                    &command,
                )
                .await?;
                transaction.commit().await?;
                Ok(Some(record))
            }
        }
    }

    pub async fn ready_command_tenants(
        &self,
        now: DateTime<Utc>,
        cursor: Option<uuid::Uuid>,
        limit: u32,
    ) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let range = cursor.map_or(TenantCursorRange::All, TenantCursorRange::After);
        let mut tenant_ids = match self {
            Self::Sqlite(store) => {
                sqlite_ready_command_tenants(store.pool(), now, range, limit).await?
            }
            Self::Timescale(pool) => {
                timescale_ready_command_tenants(pool, now, range, limit).await?
            }
        };
        if let Some(cursor) = cursor {
            let remaining =
                limit.saturating_sub(u32::try_from(tenant_ids.len()).unwrap_or(u32::MAX));
            if remaining > 0 {
                let wrap_range = TenantCursorRange::Through(cursor);
                let wrapped = match self {
                    Self::Sqlite(store) => {
                        sqlite_ready_command_tenants(store.pool(), now, wrap_range, remaining)
                            .await?
                    }
                    Self::Timescale(pool) => {
                        timescale_ready_command_tenants(pool, now, wrap_range, remaining).await?
                    }
                };
                tenant_ids.extend(wrapped);
            }
        }
        Ok(tenant_ids)
    }

    pub async fn claim_commands(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .claim_commands(tenant_id, now, lease_until, limit)
                .await?),
            Self::Timescale(pool) => {
                claim_timescale_commands(pool, tenant_id, now, lease_until, limit).await
            }
        }
    }

    pub async fn mark_command_published(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_published(tenant_id, &command_id.to_string(), published_at)
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_command_published(pool, tenant_id, command_id, published_at).await
            }
        }
    }

    pub async fn mark_command_failed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_failed(tenant_id, &command_id.to_string(), error)
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_command_failed(pool, tenant_id, command_id, error).await
            }
        }
    }

    pub async fn release_command_for_retry(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .release_command_for_retry(
                    tenant_id,
                    &command_id.to_string(),
                    error,
                    next_attempt_at,
                )
                .await?),
            Self::Timescale(pool) => {
                release_timescale_command_for_retry(
                    pool,
                    tenant_id,
                    command_id,
                    error,
                    next_attempt_at,
                )
                .await
            }
        }
    }

    pub async fn expire_due_commands(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        match self {
            Self::Sqlite(store) => Ok(store.expire_due_commands(tenant_id, now, limit).await?),
            Self::Timescale(pool) => {
                expire_timescale_due_commands(pool, tenant_id, now, limit).await
            }
        }
    }

    pub async fn expire_command_if_elapsed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        now: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .expire_command_if_elapsed(tenant_id, &command_id.to_string(), now)
                .await?),
            Self::Timescale(pool) => {
                expire_timescale_command_if_elapsed(pool, tenant_id, command_id, now).await
            }
        }
    }

    pub async fn mark_command_responded(
        &self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        device_id: &str,
        token_id: uuid::Uuid,
        response: &str,
        responded_at: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
        let response_value = serde_json::from_str::<serde_json::Value>(response)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        let response = serde_json::to_string(&response_value)
            .map_err(|_| PlatformStoreError::InvalidCommandParams)?;
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_command_responded(
                    tenant_id,
                    &command_id.to_string(),
                    device_id,
                    &token_id.to_string(),
                    &response,
                    responded_at,
                )
                .await?),
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "UPDATE command_outbox AS command
                     SET state = CASE
                            WHEN command.state = 'responded' THEN command.state
                            ELSE 'responded'
                         END,
                         response = CASE
                            WHEN command.state = 'responded' THEN command.response
                            ELSE $1::jsonb
                         END,
                         responded_at = CASE
                            WHEN command.state = 'responded' THEN command.responded_at
                            ELSE $2
                         END,
                         lease_until = NULL
                     WHERE command.id = $3
                       AND command.tenant_id = $4
                       AND command.device_id = $5
                       AND command.mode = 'two_way'
                       AND (
                            (command.state = 'published_to_broker' AND command.expires_at > $2)
                            OR (command.state = 'responded' AND command.response = $1::jsonb)
                           )
                       AND EXISTS (
                            SELECT 1
                            FROM device_tokens
                            JOIN devices ON devices.device_id = device_tokens.device_id
                            WHERE device_tokens.id = $6
                              AND device_tokens.device_id = command.device_id
                              AND devices.tenant_id = command.tenant_id
                              AND device_tokens.revoked_at IS NULL
                              AND devices.deleted_at IS NULL
                       )
                     RETURNING
                        id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                        lease_until, attempt_count, last_error, published_at, response, responded_at",
                )
                .bind(Json(response_value))
                .bind(responded_at)
                .bind(command_id)
                .bind(tenant_id)
                .bind(device_id)
                .bind(token_id)
                .fetch_optional(pool)
                .await?;
                row.map(postgres_command_outbox_record).transpose()
            }
        }
    }

    pub async fn claim_notifications(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .claim_notifications(tenant_id, now, lease_until, limit)
                .await?),
            Self::Timescale(pool) => {
                claim_timescale_notifications(pool, tenant_id, now, lease_until, limit).await
            }
        }
    }

    pub async fn ready_notification_tenants(
        &self,
        now: DateTime<Utc>,
        cursor: Option<uuid::Uuid>,
        limit: u32,
    ) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let range = cursor.map_or(TenantCursorRange::All, TenantCursorRange::After);
        let mut tenant_ids = match self {
            Self::Sqlite(store) => {
                sqlite_ready_notification_tenants(store.pool(), now, range, limit).await?
            }
            Self::Timescale(pool) => {
                timescale_ready_notification_tenants(pool, now, range, limit).await?
            }
        };
        if let Some(cursor) = cursor {
            let remaining =
                limit.saturating_sub(u32::try_from(tenant_ids.len()).unwrap_or(u32::MAX));
            if remaining > 0 {
                let wrap_range = TenantCursorRange::Through(cursor);
                let wrapped = match self {
                    Self::Sqlite(store) => {
                        sqlite_ready_notification_tenants(store.pool(), now, wrap_range, remaining)
                            .await?
                    }
                    Self::Timescale(pool) => {
                        timescale_ready_notification_tenants(pool, now, wrap_range, remaining)
                            .await?
                    }
                };
                tenant_ids.extend(wrapped);
            }
        }
        Ok(tenant_ids)
    }

    pub async fn mark_notification_sent(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .mark_notification_sent(
                    tenant_id,
                    &notification_id.to_string(),
                    expected_lease_until,
                    sent_at,
                )
                .await?),
            Self::Timescale(pool) => {
                mark_timescale_notification_sent(
                    pool,
                    tenant_id,
                    notification_id,
                    expected_lease_until,
                    sent_at,
                )
                .await
            }
        }
    }
    pub async fn enqueue_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Result<NotificationOutboxRecord, PlatformStoreError> {
        let notification = canonical_notification(notification);
        if notification.tenant_id != tenant_id {
            return Err(PlatformStoreError::NotificationTenantMismatch);
        }
        match self {
            Self::Sqlite(store) => Ok(store
                .enqueue_notification(tenant_id, &incident_id.to_string(), notification)
                .await?),
            Self::Timescale(pool) => {
                enqueue_timescale_notification(pool, tenant_id, incident_id, notification).await
            }
        }
    }

    pub async fn release_notification_for_retry(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .release_notification_for_retry(
                    tenant_id,
                    &notification_id.to_string(),
                    expected_lease_until,
                    error,
                    next_attempt_at,
                )
                .await?),
            Self::Timescale(pool) => {
                release_timescale_notification_for_retry(
                    pool,
                    tenant_id,
                    notification_id,
                    expected_lease_until,
                    error,
                    next_attempt_at,
                )
                .await
            }
        }
    }
}

async fn enqueue_sqlite_platform_command(
    store: &SqliteStore,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(&command.params)
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at.to_rfc3339())
    .bind(command.next_attempt_at.to_rfc3339())
    .fetch_optional(store.pool())
    .await?;
    if let Some(row) = row {
        return Ok(command_outbox_record(row)?);
    }

    let existing = sqlx::query(
        "SELECT
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = ? AND tenant_id = ?",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
    .fetch_optional(store.pool())
    .await?
    .map(command_outbox_record)
    .transpose()?;
    match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    }
}

async fn enqueue_timescale_platform_command(
    pool: &PgPool,
    id: uuid::Uuid,
    params: serde_json::Value,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    if !timescale_tenant_device_is_locked(&mut transaction, command.tenant_id, &command.device_id)
        .await?
    {
        return Err(PlatformStoreError::UnknownDevice(command.device_id.clone()));
    }

    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(id)
    .bind(command.tenant_id)
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(Json(params))
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at)
    .bind(command.next_attempt_at)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some(row) = row {
        let record = postgres_command_outbox_record(row)?;
        transaction.commit().await?;
        return Ok(record);
    }

    let existing = sqlx::query(
        "SELECT
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(id)
    .bind(command.tenant_id)
    .fetch_optional(&mut *transaction)
    .await?
    .map(postgres_command_outbox_record)
    .transpose()?;
    let result = match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    };
    transaction.commit().await?;
    result
}

async fn enqueue_sqlite_platform_command_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(&command.params)
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at.to_rfc3339())
    .bind(command.next_attempt_at.to_rfc3339())
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return Ok(command_outbox_record(row)?);
    }

    let existing = sqlx::query(
        "SELECT
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = ? AND tenant_id = ?",
    )
    .bind(&command.id)
    .bind(command.tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .map(command_outbox_record)
    .transpose()?;
    match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    }
}

async fn enqueue_timescale_platform_command_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: uuid::Uuid,
    params: &serde_json::Value,
    command: &NewCommandOutboxEntry,
) -> Result<CommandOutboxRecord, PlatformStoreError> {
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (id) DO NOTHING
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(id)
    .bind(command.tenant_id)
    .bind(&command.device_id)
    .bind(&command.method)
    .bind(Json(params.clone()))
    .bind(command_mode_value(command.mode))
    .bind(command.expires_at)
    .bind(command.next_attempt_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = row {
        return postgres_command_outbox_record(row);
    }

    let existing = sqlx::query(
        "SELECT
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at
         FROM command_outbox
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(id)
    .bind(command.tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_command_outbox_record)
    .transpose()?;
    match existing {
        Some(existing) if command_payload_matches(&existing, command) => Ok(existing),
        Some(_) | None => Err(PlatformStoreError::CommandConflict(command.id.clone())),
    }
}

async fn sqlite_user_can_issue_command(
    transaction: &mut Transaction<'_, Sqlite>,
    user_id: uuid::Uuid,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    let identity = sqlx::query_as::<_, (String, String)>(
        "SELECT tenant_id, account_class FROM users WHERE id = ?",
    )
    .bind(user_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((user_tenant_id, account_class)) = identity else {
        return Ok(false);
    };
    if user_tenant_id != tenant_id.to_string()
        || authorization_account_class(&account_class)? == AccountClass::System
    {
        return Ok(false);
    }

    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(owner) = owner else {
        return Ok(false);
    };
    let user_id = user_id.to_string();
    if owner.as_deref() == Some(user_id.as_str()) {
        return Ok(true);
    }

    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT asset_id, 0
            FROM devices
            WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL
              AND asset_id IS NOT NULL
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = ?
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT 1
         FROM resource_permissions AS permission
         WHERE permission.tenant_id = ?
           AND permission.revoked_at IS NULL
           AND permission.permission = 'manager'
           AND (
                (permission.device_id = ? AND (
                    permission.subject_user_id = ?
                    OR EXISTS (
                        SELECT 1 FROM user_group_members AS membership
                        WHERE membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                    )
                ))
                OR (permission.asset_id IN (SELECT id FROM ancestors)
                    AND permission.inherit_children = 1 AND (
                        permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                    ))
           )
         LIMIT 1",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(device_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_user_can_issue_command(
    transaction: &mut Transaction<'_, Postgres>,
    user_id: uuid::Uuid,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<bool, PlatformStoreError> {
    let identity = sqlx::query_as::<_, (uuid::Uuid, String)>(
        "SELECT tenant_id, account_class FROM users WHERE id = $1 FOR SHARE",
    )
    .bind(user_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((user_tenant_id, account_class)) = identity else {
        return Ok(false);
    };
    if user_tenant_id != tenant_id
        || authorization_account_class(&account_class)? == AccountClass::System
    {
        return Ok(false);
    }

    // Observe the asset without a row lock, then take the shared asset locks
    // before the device lock. Asset deletion uses the same asset -> device order.
    let observed_asset_id = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
        "SELECT asset_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(observed_asset_id) = observed_asset_id else {
        return Ok(false);
    };
    if let Some(asset_id) = observed_asset_id {
        lock_timescale_command_asset_ancestors(transaction, tenant_id, asset_id).await?;
    }

    let device = sqlx::query_as::<_, (Option<uuid::Uuid>, Option<uuid::Uuid>)>(
        "SELECT owner_user_id, asset_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR SHARE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((owner, asset_id)) = device else {
        return Ok(false);
    };
    if asset_id != observed_asset_id {
        return Ok(false);
    }
    if owner == Some(user_id) {
        return Ok(true);
    }
    sqlx::query(
        "SELECT group_id
         FROM user_group_members
         WHERE tenant_id = $1 AND user_id = $2
         FOR SHARE",
    )
    .bind(tenant_id)
    .bind(user_id)
    .fetch_all(&mut **transaction)
    .await?;

    Ok(sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT asset_id, 0
            FROM devices
            WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
              AND asset_id IS NOT NULL
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = $2
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT 1::bigint
         FROM resource_permissions AS permission
         WHERE permission.tenant_id = $2
           AND permission.revoked_at IS NULL
           AND permission.permission = 'manager'
           AND (
                (permission.device_id = $1 AND (
                    permission.subject_user_id = $3
                    OR EXISTS (
                        SELECT 1 FROM user_group_members AS membership
                        WHERE membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                    )
                ))
                OR (permission.asset_id IN (SELECT id FROM ancestors)
                    AND permission.inherit_children = TRUE AND (
                        permission.subject_user_id = $3
                        OR EXISTS (
                            SELECT 1 FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $3
                        )
                    ))
           )
         LIMIT 1
         FOR SHARE OF permission",
    )
    .bind(device_id)
    .bind(tenant_id)
    .bind(user_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn lock_timescale_command_asset_ancestors(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    asset_id: uuid::Uuid,
) -> Result<(), PlatformStoreError> {
    sqlx::query(
        "WITH RECURSIVE ancestors(id) AS (
             SELECT $1::uuid
             UNION
             SELECT asset.parent_asset_id
             FROM assets AS asset
             JOIN ancestors ON asset.id = ancestors.id
             WHERE asset.tenant_id = $2 AND asset.parent_asset_id IS NOT NULL
         )
         SELECT asset.id
         FROM assets AS asset
         JOIN ancestors ON asset.id = ancestors.id AND asset.tenant_id = $2
         ORDER BY asset.id
         FOR SHARE OF asset",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum TenantCursorRange {
    All,
    After(uuid::Uuid),
    Through(uuid::Uuid),
}

impl TenantCursorRange {
    fn kind(self) -> i64 {
        match self {
            Self::All => 0,
            Self::After(_) => 1,
            Self::Through(_) => 2,
        }
    }

    fn sqlite_cursor(self) -> String {
        match self {
            Self::All => String::new(),
            Self::After(cursor) | Self::Through(cursor) => cursor.to_string(),
        }
    }

    fn timescale_cursor(self) -> Option<uuid::Uuid> {
        match self {
            Self::All => None,
            Self::After(cursor) | Self::Through(cursor) => Some(cursor),
        }
    }
}

async fn sqlite_ready_command_tenants(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    let now = now.to_rfc3339();
    let range_kind = range.kind();
    let cursor = range.sqlite_cursor();
    let tenant_ids = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT tenant_id
         FROM command_outbox
         WHERE (
                (state = 'queued' AND next_attempt_at <= ?)
                OR (state = 'leased' AND lease_until <= ?)
                OR (
                    (state IN ('queued', 'leased')
                     OR (state = 'published_to_broker' AND mode = 'two_way'))
                    AND expires_at <= ?
                )
               )
           AND (
                ? = 0
                OR (? = 1 AND tenant_id > ?)
                OR (? = 2 AND tenant_id <= ?)
               )
         ORDER BY tenant_id
         LIMIT ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .bind(range_kind)
    .bind(range_kind)
    .bind(&cursor)
    .bind(range_kind)
    .bind(&cursor)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?;
    tenant_ids
        .into_iter()
        .map(|tenant_id| {
            uuid::Uuid::parse_str(&tenant_id)
                .map_err(|_| PlatformStoreError::InvalidCommandTenantId(tenant_id))
        })
        .collect()
}

async fn timescale_ready_command_tenants(
    pool: &PgPool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT DISTINCT tenant_id
         FROM command_outbox
         WHERE (
                (state = 'queued' AND next_attempt_at <= $1)
                OR (state = 'leased' AND lease_until <= $1)
                OR (
                    (state IN ('queued', 'leased')
                     OR (state = 'published_to_broker' AND mode = 'two_way'))
                    AND expires_at <= $1
                )
               )
           AND (
                $2 = 0
                OR ($2 = 1 AND tenant_id > $3)
                OR ($2 = 2 AND tenant_id <= $3)
               )
         ORDER BY tenant_id
         LIMIT $4",
    )
    .bind(now)
    .bind(range.kind())
    .bind(range.timescale_cursor())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?)
}

async fn sqlite_ready_notification_tenants(
    pool: &SqlitePool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    let now = now.to_rfc3339();
    let range_kind = range.kind();
    let cursor = range.sqlite_cursor();
    let tenant_ids = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT tenant_id
         FROM notification_outbox
         WHERE (
                (state = 'pending' AND next_attempt_at <= ?)
                OR (state = 'leased' AND lease_until <= ?)
               )
           AND (
                ? = 0
                OR (? = 1 AND tenant_id > ?)
                OR (? = 2 AND tenant_id <= ?)
               )
         ORDER BY tenant_id
         LIMIT ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(range_kind)
    .bind(range_kind)
    .bind(&cursor)
    .bind(range_kind)
    .bind(&cursor)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?;
    tenant_ids
        .into_iter()
        .map(|tenant_id| {
            uuid::Uuid::parse_str(&tenant_id)
                .map_err(|_| PlatformStoreError::InvalidNotificationTenantId(tenant_id))
        })
        .collect()
}

async fn timescale_ready_notification_tenants(
    pool: &PgPool,
    now: DateTime<Utc>,
    range: TenantCursorRange,
    limit: u32,
) -> Result<Vec<uuid::Uuid>, PlatformStoreError> {
    Ok(sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT DISTINCT tenant_id
         FROM notification_outbox
         WHERE (
                (state = 'pending' AND next_attempt_at <= $1)
                OR (state = 'leased' AND lease_until <= $1)
               )
           AND (
                $2 = 0
                OR ($2 = 1 AND tenant_id > $3)
                OR ($2 = 2 AND tenant_id <= $3)
               )
         ORDER BY tenant_id
         LIMIT $4",
    )
    .bind(now)
    .bind(range.kind())
    .bind(range.timescale_cursor())
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?)
}

async fn claim_timescale_commands(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH due AS (
            SELECT id
            FROM command_outbox
            WHERE tenant_id = $1
              AND expires_at > $2
              AND (
                    (state = 'queued' AND next_attempt_at <= $2)
                    OR (state = 'leased' AND lease_until <= $2)
                  )
            ORDER BY next_attempt_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
         )
         UPDATE command_outbox AS command
         SET state = 'leased',
             lease_until = $4,
             attempt_count = command.attempt_count + 1
         FROM due
         WHERE command.id = due.id AND command.tenant_id = $1
         RETURNING
            command.id, command.tenant_id, command.device_id, command.method, command.params, command.mode,
            command.state, command.created_at, command.expires_at, command.next_attempt_at, command.lease_until,
            command.attempt_count, command.last_error, command.published_at, command.response,
            command.responded_at",
    )
    .bind(tenant_id)
    .bind(now)
    .bind(i64::from(limit))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_command_outbox_record)
        .collect()
}

async fn expire_timescale_due_commands(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    now: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<CommandOutboxRecord>, PlatformStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH expired AS (
            SELECT id
            FROM command_outbox
            WHERE tenant_id = $1
              AND (
                    state IN ('queued', 'leased')
                    OR (state = 'published_to_broker' AND mode = 'two_way')
                  )
              AND expires_at <= $2
            ORDER BY expires_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
         )
         UPDATE command_outbox AS command
         SET state = 'expired',
             lease_until = NULL
         FROM expired
         WHERE command.id = expired.id
           AND command.tenant_id = $1
           AND (
                command.state IN ('queued', 'leased')
                OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
               )
           AND command.expires_at <= $2
         RETURNING
            command.id, command.tenant_id, command.device_id, command.method, command.params,
            command.mode, command.state, command.created_at, command.expires_at,
            command.next_attempt_at, command.lease_until, command.attempt_count,
            command.last_error, command.published_at, command.response, command.responded_at",
    )
    .bind(tenant_id)
    .bind(now)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_command_outbox_record)
        .collect()
}

async fn expire_timescale_command_if_elapsed(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    now: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox AS command
         SET state = 'expired',
             lease_until = NULL
         WHERE command.id = $1
           AND command.tenant_id = $2
           AND (
                command.state IN ('queued', 'leased')
                OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
               )
           AND command.expires_at <= $3
         RETURNING
            command.id, command.tenant_id, command.device_id, command.method, command.params,
            command.mode, command.state, command.created_at, command.expires_at,
            command.next_attempt_at, command.lease_until, command.attempt_count,
            command.last_error, command.published_at, command.response, command.responded_at",
    )
    .bind(command_id)
    .bind(tenant_id)
    .bind(now)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn claim_timescale_notifications(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH due AS (
            SELECT id
            FROM notification_outbox
            WHERE tenant_id = $1
              AND ((state = 'pending' AND next_attempt_at <= $2)
                OR (state = 'leased' AND lease_until <= $2))
            ORDER BY next_attempt_at, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $3
         )
         UPDATE notification_outbox AS notification
         SET state = 'leased',
             lease_until = $4,
             attempt_count = notification.attempt_count + 1
         FROM due
         WHERE notification.id = due.id AND notification.tenant_id = $1
         RETURNING
            notification.id, notification.tenant_id, notification.incident_id, notification.kind,
            notification.dedupe_key, notification.subject, notification.body,
            notification.state, notification.next_attempt_at, notification.lease_until,
            notification.attempt_count, notification.last_error, notification.sent_at",
    )
    .bind(tenant_id)
    .bind(now)
    .bind(i64::from(limit))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(postgres_notification_outbox_record)
        .collect()
}
async fn enqueue_timescale_notification(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    notification: NewNotificationOutboxEntry,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (tenant_id, dedupe_key) DO NOTHING",
    )
    .bind(notification.id)
    .bind(tenant_id)
    .bind(incident_id)
    .bind(notification.kind.as_str())
    .bind(&notification.dedupe_key)
    .bind(&notification.subject)
    .bind(&notification.body)
    .bind(notification.next_attempt_at)
    .execute(pool)
    .await?;
    let row = sqlx::query(
        "SELECT id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at
         FROM notification_outbox WHERE dedupe_key = $1 AND tenant_id = $2",
    )
    .bind(notification.dedupe_key)
    .bind(tenant_id)
    .fetch_one(pool)
    .await?;
    postgres_notification_outbox_record(row)
}

async fn mark_timescale_notification_sent(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    notification_id: uuid::Uuid,
    expected_lease_until: DateTime<Utc>,
    sent_at: DateTime<Utc>,
) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE notification_outbox
         SET state = 'sent', sent_at = $1, lease_until = NULL
         WHERE id = $2 AND tenant_id = $3 AND state = 'leased' AND lease_until = $4
         RETURNING
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
            next_attempt_at, lease_until, attempt_count, last_error, sent_at",
    )
    .bind(sent_at)
    .bind(notification_id)
    .bind(tenant_id)
    .bind(expected_lease_until)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_notification_outbox_record).transpose()
}

async fn release_timescale_notification_for_retry(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    notification_id: uuid::Uuid,
    expected_lease_until: DateTime<Utc>,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE notification_outbox
         SET state = 'pending', next_attempt_at = $1, last_error = $2, lease_until = NULL
         WHERE id = $3 AND tenant_id = $4 AND state = 'leased' AND lease_until = $5
         RETURNING
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
            next_attempt_at, lease_until, attempt_count, last_error, sent_at",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(notification_id)
    .bind(tenant_id)
    .bind(expected_lease_until)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_notification_outbox_record).transpose()
}

async fn mark_timescale_command_published(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    published_at: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'published_to_broker', published_at = $1, lease_until = NULL
         WHERE id = $2 AND tenant_id = $3 AND state = 'leased' AND expires_at > $1
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(published_at)
    .bind(command_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn mark_timescale_command_failed(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    error: &str,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'failed', last_error = $1, lease_until = NULL
         WHERE id = $2 AND tenant_id = $3 AND state = 'leased'
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(error)
    .bind(command_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

async fn release_timescale_command_for_retry(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    command_id: uuid::Uuid,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<Option<CommandOutboxRecord>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE command_outbox
         SET state = 'queued', next_attempt_at = $1, last_error = $2, lease_until = NULL
         WHERE id = $3 AND tenant_id = $4 AND state = 'leased' AND expires_at > $1
         RETURNING
            id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
            lease_until, attempt_count, last_error, published_at, response, responded_at",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(command_id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_command_outbox_record).transpose()
}

fn command_payload_matches(
    existing: &CommandOutboxRecord,
    command: &NewCommandOutboxEntry,
) -> bool {
    existing.id == command.id
        && existing.tenant_id == command.tenant_id
        && existing.device_id == command.device_id
        && existing.method == command.method
        && existing.params == command.params
        && existing.mode == command.mode
}
impl CommandRepository for PlatformStore {
    fn enqueue_command<'a>(
        &'a self,
        command: NewCommandOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<CommandOutboxRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::enqueue_command(self, command).await })
    }
}

impl CommandLifecycleRepository for PlatformStore {
    fn claim_commands<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move {
            PlatformStore::claim_commands(self, tenant_id, now, lease_until, limit).await
        })
    }

    fn mark_command_published<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_command_published(self, tenant_id, command_id, published_at).await
        })
    }

    fn mark_command_failed<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_command_failed(self, tenant_id, command_id, error).await
        })
    }

    fn release_command_for_retry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::release_command_for_retry(
                self,
                tenant_id,
                command_id,
                error,
                next_attempt_at,
            )
            .await
        })
    }

    fn expire_due_commands<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(
            async move { PlatformStore::expire_due_commands(self, tenant_id, now, limit).await },
        )
    }

    fn expire_command_if_elapsed<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::expire_command_if_elapsed(self, tenant_id, command_id, now).await
        })
    }

    fn mark_command_responded<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        device_id: &'a str,
        token_id: uuid::Uuid,
        response: &'a str,
        responded_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_command_responded(
                self,
                tenant_id,
                command_id,
                device_id,
                token_id,
                response,
                responded_at,
            )
            .await
        })
    }
}

impl NotificationRepository for PlatformStore {
    fn claim_notifications<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::claim_notifications(self, tenant_id, now, lease_until, limit).await
        })
    }

    fn mark_notification_sent<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::mark_notification_sent(
                self,
                tenant_id,
                notification_id,
                expected_lease_until,
                sent_at,
            )
            .await
        })
    }

    fn release_notification_for_retry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::release_notification_for_retry(
                self,
                tenant_id,
                notification_id,
                expected_lease_until,
                error,
                next_attempt_at,
            )
            .await
        })
    }
}

fn postgres_command_outbox_record(row: PgRow) -> Result<CommandOutboxRecord, PlatformStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get::<uuid::Uuid, _>("id")?.to_string(),
        tenant_id: row.try_get("tenant_id")?,
        device_id: row.try_get("device_id")?,
        method: row.try_get("method")?,
        params: row
            .try_get::<Json<serde_json::Value>, _>("params")?
            .0
            .to_string(),
        mode: command_mode_from_database(&row.try_get::<String, _>("mode")?)?,
        state: CommandOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        created_at: row.try_get("created_at")?,
        expires_at: row.try_get("expires_at")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        lease_until: row.try_get("lease_until")?,
        attempt_count: i64::from(row.try_get::<i32, _>("attempt_count")?),
        last_error: row.try_get("last_error")?,
        published_at: row.try_get("published_at")?,
        response: row
            .try_get::<Option<Json<serde_json::Value>>, _>("response")?
            .map(|response| response.0.to_string()),
        responded_at: row.try_get("responded_at")?,
    })
}

fn postgres_notification_outbox_record(
    row: PgRow,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    Ok(NotificationOutboxRecord {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        incident_id: row.try_get("incident_id")?,
        kind: NotificationKind::from_database(&row.try_get::<String, _>("kind")?)?,
        dedupe_key: row.try_get("dedupe_key")?,
        subject: row.try_get("subject")?,
        body: row.try_get("body")?,
        state: NotificationOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        lease_until: row.try_get("lease_until")?,
        attempt_count: i64::from(row.try_get::<i32, _>("attempt_count")?),
        last_error: row.try_get("last_error")?,
        sent_at: row.try_get("sent_at")?,
    })
}

impl SqliteStore {
    pub async fn enqueue_command(
        &self,
        command: NewCommandOutboxEntry,
    ) -> Result<CommandOutboxRecord, SqliteStoreError> {
        let row = sqlx::query(
            "INSERT INTO command_outbox (
                id, tenant_id, device_id, method, params, mode, expires_at, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(command.id)
        .bind(command.tenant_id.to_string())
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
        tenant_id: uuid::Uuid,
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
                WHERE tenant_id = ?
                  AND expires_at > ?
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
             WHERE tenant_id = ? AND id IN (SELECT id FROM due)
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(tenant_id.to_string())
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .bind(tenant_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn mark_command_published(
        &self,
        tenant_id: uuid::Uuid,
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
               AND tenant_id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&published_at)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(&published_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn mark_command_failed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: &str,
        error: &str,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let row = sqlx::query(
            "UPDATE command_outbox
             SET state = 'failed',
                 last_error = ?,
                 lease_until = NULL
             WHERE id = ?
               AND tenant_id = ?
               AND state = 'leased'
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(error)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn release_command_for_retry(
        &self,
        tenant_id: uuid::Uuid,
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
               AND tenant_id = ?
               AND state = 'leased'
               AND expires_at > ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(&next_attempt_at)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn claim_notifications(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<NotificationOutboxRecord>, PlatformStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = now.to_rfc3339();
        let rows = sqlx::query(
            "WITH due AS (
                SELECT id
                FROM notification_outbox
                WHERE tenant_id = ?
                  AND ((state = 'pending' AND next_attempt_at <= ?)
                    OR (state = 'leased' AND lease_until <= ?))
                ORDER BY next_attempt_at, created_at, id
                LIMIT ?
             )
             UPDATE notification_outbox
             SET state = 'leased',
                 lease_until = ?,
                 attempt_count = attempt_count + 1
             WHERE tenant_id = ? AND id IN (SELECT id FROM due)
             RETURNING
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(tenant_id.to_string())
        .bind(&now)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(lease_until.to_rfc3339())
        .bind(tenant_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(notification_outbox_record).collect()
    }
    async fn enqueue_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: &str,
        notification: NewNotificationOutboxEntry,
    ) -> Result<NotificationOutboxRecord, PlatformStoreError> {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, dedupe_key) DO NOTHING",
        )
        .bind(notification.id.to_string())
        .bind(tenant_id.to_string())
        .bind(incident_id)
        .bind(notification.kind.as_str())
        .bind(&notification.dedupe_key)
        .bind(&notification.subject)
        .bind(&notification.body)
        .bind(notification.next_attempt_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                    next_attempt_at, lease_until, attempt_count, last_error, sent_at
             FROM notification_outbox WHERE dedupe_key = ? AND tenant_id = ?",
        )
        .bind(notification.dedupe_key)
        .bind(tenant_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        notification_outbox_record(row)
    }

    pub async fn mark_notification_sent(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: &str,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        let sent_at = sent_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'sent', sent_at = ?, lease_until = NULL
             WHERE id = ? AND tenant_id = ? AND state = 'leased' AND lease_until = ?
             RETURNING
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&sent_at)
        .bind(notification_id)
        .bind(tenant_id.to_string())
        .bind(expected_lease_until.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(notification_outbox_record).transpose()
    }

    pub async fn release_notification_for_retry(
        &self,
        tenant_id: uuid::Uuid,
        notification_id: &str,
        expected_lease_until: DateTime<Utc>,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<Option<NotificationOutboxRecord>, PlatformStoreError> {
        let next_attempt_at = next_attempt_at.to_rfc3339();
        let row = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'pending', next_attempt_at = ?, last_error = ?, lease_until = NULL
             WHERE id = ? AND tenant_id = ? AND state = 'leased' AND lease_until = ?
             RETURNING
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, state,
                next_attempt_at, lease_until, attempt_count, last_error, sent_at",
        )
        .bind(&next_attempt_at)
        .bind(error)
        .bind(notification_id)
        .bind(tenant_id.to_string())
        .bind(expected_lease_until.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(notification_outbox_record).transpose()
    }

    pub async fn expire_due_commands(
        &self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CommandOutboxRecord>, SqliteStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let tenant_id = tenant_id.to_string();
        let now = now.to_rfc3339();
        // This single write statement selects and expires a bounded ordered set atomically.
        let rows = sqlx::query(
            "WITH expired AS (
                SELECT id
                FROM command_outbox
                WHERE tenant_id = ?
                  AND (
                        state IN ('queued', 'leased')
                        OR (state = 'published_to_broker' AND mode = 'two_way')
                      )
                  AND expires_at <= ?
                ORDER BY expires_at, created_at, id
                LIMIT ?
             )
             UPDATE command_outbox AS command
             SET state = 'expired',
                 lease_until = NULL
             WHERE command.tenant_id = ?
               AND command.id IN (SELECT id FROM expired)
               AND (
                    command.state IN ('queued', 'leased')
                    OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
                   )
               AND command.expires_at <= ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(&tenant_id)
        .bind(&now)
        .bind(i64::from(limit))
        .bind(&tenant_id)
        .bind(&now)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(command_outbox_record).collect()
    }

    pub async fn expire_command_if_elapsed(
        &self,
        tenant_id: uuid::Uuid,
        command_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<CommandOutboxRecord>, SqliteStoreError> {
        let row = sqlx::query(
            "UPDATE command_outbox AS command
             SET state = 'expired',
                 lease_until = NULL
             WHERE command.id = ?
               AND command.tenant_id = ?
               AND (
                    command.state IN ('queued', 'leased')
                    OR (command.state = 'published_to_broker' AND command.mode = 'two_way')
                   )
               AND command.expires_at <= ?
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(now.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }

    pub async fn mark_command_responded(
        &self,
        tenant_id: uuid::Uuid,
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
                 response = CASE
                    WHEN command.state = 'responded' THEN command.response
                    ELSE ?
                 END,
                 responded_at = CASE
                    WHEN command.state = 'responded' THEN command.responded_at
                    ELSE ?
                 END,
                 lease_until = NULL
             WHERE command.id = ?
               AND command.tenant_id = ?
               AND command.device_id = ?
               AND command.mode = 'two_way'
               AND (
                    (command.state = 'published_to_broker' AND command.expires_at > ?)
                    OR (command.state = 'responded' AND command.response = ?)
               )
               AND EXISTS (
                    SELECT 1
                    FROM device_tokens
                    JOIN devices ON devices.device_id = device_tokens.device_id
                    WHERE device_tokens.id = ?
                      AND device_tokens.device_id = command.device_id
                      AND devices.tenant_id = command.tenant_id
                      AND device_tokens.revoked_at IS NULL
                      AND devices.deleted_at IS NULL
               )
             RETURNING
                id, tenant_id, device_id, method, params, mode, state, created_at, expires_at, next_attempt_at,
                lease_until, attempt_count, last_error, published_at, response, responded_at",
        )
        .bind(response)
        .bind(&responded_at)
        .bind(command_id)
        .bind(tenant_id.to_string())
        .bind(device_id)
        .bind(&responded_at)
        .bind(response)
        .bind(token_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(command_outbox_record).transpose()
    }
}

fn command_outbox_record(row: SqliteRow) -> Result<CommandOutboxRecord, SqliteStoreError> {
    Ok(CommandOutboxRecord {
        id: row.try_get("id")?,
        tenant_id: uuid::Uuid::parse_str(&row.try_get::<String, _>("tenant_id")?)
            .map_err(|_| SqliteStoreError::InvalidCommandTenantId)?,
        device_id: row.try_get("device_id")?,
        method: row.try_get("method")?,
        params: row.try_get("params")?,
        mode: command_mode_from_database(&row.try_get::<String, _>("mode")?)?,
        state: CommandOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        created_at: command_timestamp(&row, "created_at")?,
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
fn notification_outbox_record(
    row: SqliteRow,
) -> Result<NotificationOutboxRecord, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let incident_id: String = row.try_get("incident_id")?;
    Ok(NotificationOutboxRecord {
        id: uuid::Uuid::parse_str(&id)
            .map_err(|_| PlatformStoreError::InvalidNotificationId(id))?,
        tenant_id: uuid::Uuid::parse_str(&tenant_id)
            .map_err(|_| PlatformStoreError::InvalidNotificationTenantId(tenant_id))?,
        incident_id: uuid::Uuid::parse_str(&incident_id)
            .map_err(|_| PlatformStoreError::InvalidNotificationId(incident_id))?,
        kind: NotificationKind::from_database(&row.try_get::<String, _>("kind")?)?,
        dedupe_key: row.try_get("dedupe_key")?,
        subject: row.try_get("subject")?,
        body: row.try_get("body")?,
        state: NotificationOutboxState::from_database(&row.try_get::<String, _>("state")?)?,
        next_attempt_at: notification_timestamp(&row, "next_attempt_at")?,
        lease_until: notification_optional_timestamp(&row, "lease_until")?,
        attempt_count: row.try_get("attempt_count")?,
        last_error: row.try_get("last_error")?,
        sent_at: notification_optional_timestamp(&row, "sent_at")?,
    })
}

fn notification_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    let value: String = row.try_get(column)?;
    parse_notification_timestamp(value, column)
}

fn notification_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, PlatformStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| parse_notification_timestamp(value, column))
        .transpose()
}

fn parse_notification_timestamp(
    value: String,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|timestamp| timestamp.and_utc())
        })
        .map_err(|source| PlatformStoreError::InvalidNotificationTimestamp {
            column,
            value,
            source,
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
    parse_command_timestamp(&value).map_err(|source| SqliteStoreError::InvalidCommandTimestamp {
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
            parse_command_timestamp(&value).map_err(|source| {
                SqliteStoreError::InvalidCommandTimestamp {
                    column,
                    value,
                    source,
                }
            })
        })
        .transpose()
}

fn parse_command_timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(&format!("{value} +00:00"), "%Y-%m-%d %H:%M:%S %z"))
        .map(|timestamp| timestamp.with_timezone(&Utc))
}
