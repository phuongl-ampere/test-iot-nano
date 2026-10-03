use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Sqlite, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{PlatformStore, PlatformStoreError};

/// A new personal access token whose plaintext was generated and hashed by the caller.
///
/// This storage boundary deliberately never accepts or returns a plaintext secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTenantPersonalAccessToken {
    pub id: Uuid,
    pub name: String,
    pub token_prefix: String,
    pub token_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantPersonalAccessTokenRecord {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub tenant_account_user_id: Uuid,
    pub name: String,
    pub token_prefix: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
pub enum TenantPersonalAccessTokenRepositoryError {
    #[error("personal access token name is required")]
    EmptyName,
    #[error("personal access token prefix is required")]
    EmptyTokenPrefix,
    #[error("personal access token hash must be a SHA-256 hex digest")]
    InvalidTokenHash,
    #[error("tenant account was not found or is disabled")]
    TenantAccountNotFound,
    #[error("personal access token was not found")]
    TokenNotFound,
    #[error("personal access token prefix already exists")]
    TokenPrefixConflict,
    #[error("stored personal access token timestamp is invalid")]
    InvalidStoredTimestamp,
    #[error("personal access token storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for TenantPersonalAccessTokenRepositoryError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for TenantPersonalAccessTokenRepositoryError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait TenantPersonalAccessTokenRepository: Send + Sync {
    fn active_tenant_personal_access_token<'a>(
        &'a self,
        tenant_id: Uuid,
        tenant_account_user_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<TenantPersonalAccessTokenRecord>,
                        TenantPersonalAccessTokenRepositoryError,
                    >,
                > + Send
                + 'a,
        >,
    >;

    fn rotate_tenant_personal_access_token<'a>(
        &'a self,
        tenant_id: Uuid,
        tenant_account_user_id: Uuid,
        token: NewTenantPersonalAccessToken,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        TenantPersonalAccessTokenRecord,
                        TenantPersonalAccessTokenRepositoryError,
                    >,
                > + Send
                + 'a,
        >,
    >;

    fn revoke_tenant_personal_access_token<'a>(
        &'a self,
        tenant_id: Uuid,
        tenant_account_user_id: Uuid,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), TenantPersonalAccessTokenRepositoryError>> + Send + 'a>,
    >;

    fn resolve_tenant_personal_access_token<'a>(
        &'a self,
        token_hash: &'a str,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<TenantPersonalAccessTokenRecord>,
                        TenantPersonalAccessTokenRepositoryError,
                    >,
                > + Send
                + 'a,
        >,
    >;
}

impl TenantPersonalAccessTokenRepository for PlatformStore {
    fn active_tenant_personal_access_token<'a>(
        &'a self,
        tenant_id: Uuid,
        tenant_account_user_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<TenantPersonalAccessTokenRecord>,
                        TenantPersonalAccessTokenRepositoryError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            active_tenant_personal_access_token(self, tenant_id, tenant_account_user_id).await
        })
    }

    fn rotate_tenant_personal_access_token<'a>(
        &'a self,
        tenant_id: Uuid,
        tenant_account_user_id: Uuid,
        token: NewTenantPersonalAccessToken,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        TenantPersonalAccessTokenRecord,
                        TenantPersonalAccessTokenRepositoryError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            rotate_tenant_personal_access_token(self, tenant_id, tenant_account_user_id, token, now)
                .await
        })
    }

    fn revoke_tenant_personal_access_token<'a>(
        &'a self,
        tenant_id: Uuid,
        tenant_account_user_id: Uuid,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), TenantPersonalAccessTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move {
            revoke_tenant_personal_access_token(self, tenant_id, tenant_account_user_id, now).await
        })
    }

    fn resolve_tenant_personal_access_token<'a>(
        &'a self,
        token_hash: &'a str,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<TenantPersonalAccessTokenRecord>,
                        TenantPersonalAccessTokenRepositoryError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move { resolve_tenant_personal_access_token(self, token_hash, now).await })
    }
}

async fn active_tenant_personal_access_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
) -> Result<Option<TenantPersonalAccessTokenRecord>, TenantPersonalAccessTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, tenant_id, tenant_account_user_id, name, token_prefix, created_at, last_used_at, revoked_at
             FROM tenant_personal_access_tokens
             WHERE tenant_id = ? AND tenant_account_user_id = ? AND revoked_at IS NULL",
        )
        .bind(tenant_id.to_string())
        .bind(tenant_account_user_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, tenant_id, tenant_account_user_id, name, token_prefix, created_at, last_used_at, revoked_at
             FROM tenant_personal_access_tokens
             WHERE tenant_id = $1 AND tenant_account_user_id = $2 AND revoked_at IS NULL",
        )
        .bind(tenant_id)
        .bind(tenant_account_user_id)
        .fetch_optional(pool)
        .await?
        .map(postgres_record)
        .transpose(),
    }
}

async fn rotate_tenant_personal_access_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
    token: NewTenantPersonalAccessToken,
    now: DateTime<Utc>,
) -> Result<TenantPersonalAccessTokenRecord, TenantPersonalAccessTokenRepositoryError> {
    validate_new_token(&token)?;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            ensure_sqlite_account_active(&mut transaction, tenant_id, tenant_account_user_id)
                .await?;
            sqlx::query(
                "UPDATE tenant_personal_access_tokens
                 SET revoked_at = ?
                 WHERE tenant_id = ? AND tenant_account_user_id = ? AND revoked_at IS NULL",
            )
            .bind(now.to_rfc3339())
            .bind(tenant_id.to_string())
            .bind(tenant_account_user_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_token(
                &mut transaction,
                tenant_id,
                tenant_account_user_id,
                token,
                now,
            )
            .await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            ensure_postgres_account_active(&mut transaction, tenant_id, tenant_account_user_id)
                .await?;
            sqlx::query(
                "UPDATE tenant_personal_access_tokens
                 SET revoked_at = $1
                 WHERE tenant_id = $2 AND tenant_account_user_id = $3 AND revoked_at IS NULL",
            )
            .bind(now)
            .bind(tenant_id)
            .bind(tenant_account_user_id)
            .execute(&mut *transaction)
            .await?;
            let record = insert_postgres_token(
                &mut transaction,
                tenant_id,
                tenant_account_user_id,
                token,
                now,
            )
            .await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn revoke_tenant_personal_access_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), TenantPersonalAccessTokenRepositoryError> {
    let revoked = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE tenant_personal_access_tokens
             SET revoked_at = ?
             WHERE tenant_id = ? AND tenant_account_user_id = ? AND revoked_at IS NULL",
        )
        .bind(now.to_rfc3339())
        .bind(tenant_id.to_string())
        .bind(tenant_account_user_id.to_string())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE tenant_personal_access_tokens
             SET revoked_at = $1
             WHERE tenant_id = $2 AND tenant_account_user_id = $3 AND revoked_at IS NULL",
        )
        .bind(now)
        .bind(tenant_id)
        .bind(tenant_account_user_id)
        .execute(pool)
        .await?
        .rows_affected(),
    };
    if revoked == 0 {
        Err(TenantPersonalAccessTokenRepositoryError::TokenNotFound)
    } else {
        Ok(())
    }
}

async fn resolve_tenant_personal_access_token(
    store: &PlatformStore,
    token_hash: &str,
    now: DateTime<Utc>,
) -> Result<Option<TenantPersonalAccessTokenRecord>, TenantPersonalAccessTokenRepositoryError> {
    if !is_sha256_hex(token_hash) {
        return Ok(None);
    }
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let record = sqlx::query(
                "SELECT pat.id, pat.tenant_id, pat.tenant_account_user_id, pat.name, pat.token_prefix,
                        pat.created_at, pat.last_used_at, pat.revoked_at
                 FROM tenant_personal_access_tokens pat
                 INNER JOIN tenants t ON t.id = pat.tenant_id
                 INNER JOIN tenant_accounts account
                   ON account.id = pat.tenant_account_user_id AND account.tenant_id = pat.tenant_id
                 WHERE pat.token_hash = ? AND pat.revoked_at IS NULL
                   AND t.status = 'active' AND account.status = 'active'",
            )
            .bind(token_hash)
            .fetch_optional(&mut *transaction)
            .await?
            .map(sqlite_record)
            .transpose()?;
            let Some(mut record) = record else {
                transaction.commit().await?;
                return Ok(None);
            };
            let updated = sqlx::query(
                "UPDATE tenant_personal_access_tokens
                 SET last_used_at = ?
                 WHERE id = ? AND token_hash = ? AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM tenants t
                       INNER JOIN tenant_accounts account
                         ON account.tenant_id = t.id
                       WHERE t.id = tenant_personal_access_tokens.tenant_id
                         AND account.id = tenant_personal_access_tokens.tenant_account_user_id
                         AND t.status = 'active' AND account.status = 'active'
                   )",
            )
            .bind(now.to_rfc3339())
            .bind(record.id.to_string())
            .bind(token_hash)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            transaction.commit().await?;
            if updated == 0 {
                Ok(None)
            } else {
                record.last_used_at = Some(now);
                Ok(Some(record))
            }
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            let record = sqlx::query(
                "SELECT pat.id, pat.tenant_id, pat.tenant_account_user_id, pat.name, pat.token_prefix,
                        pat.created_at, pat.last_used_at, pat.revoked_at
                 FROM tenant_personal_access_tokens pat
                 INNER JOIN tenants t ON t.id = pat.tenant_id
                 INNER JOIN tenant_accounts account
                   ON account.id = pat.tenant_account_user_id AND account.tenant_id = pat.tenant_id
                 WHERE pat.token_hash = $1 AND pat.revoked_at IS NULL
                   AND t.status = 'active' AND account.status = 'active'
                 FOR UPDATE OF pat",
            )
            .bind(token_hash)
            .fetch_optional(&mut *transaction)
            .await?
            .map(postgres_record)
            .transpose()?;
            let Some(mut record) = record else {
                transaction.commit().await?;
                return Ok(None);
            };
            let updated = sqlx::query(
                "UPDATE tenant_personal_access_tokens
                 SET last_used_at = $1
                 WHERE id = $2 AND token_hash = $3 AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM tenants t
                       INNER JOIN tenant_accounts account
                         ON account.tenant_id = t.id
                       WHERE t.id = tenant_personal_access_tokens.tenant_id
                         AND account.id = tenant_personal_access_tokens.tenant_account_user_id
                         AND t.status = 'active' AND account.status = 'active'
                   )",
            )
            .bind(now)
            .bind(record.id)
            .bind(token_hash)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            transaction.commit().await?;
            if updated == 0 {
                Ok(None)
            } else {
                record.last_used_at = Some(now);
                Ok(Some(record))
            }
        }
    }
}

async fn ensure_sqlite_account_active(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
) -> Result<(), TenantPersonalAccessTokenRepositoryError> {
    let found = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM tenant_accounts account
         INNER JOIN tenants t ON t.id = account.tenant_id
         WHERE account.id = ? AND account.tenant_id = ?
           AND account.status = 'active' AND t.status = 'active'",
    )
    .bind(tenant_account_user_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    found
        .ok_or(TenantPersonalAccessTokenRepositoryError::TenantAccountNotFound)
        .map(|_| ())
}

async fn ensure_postgres_account_active(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
) -> Result<(), TenantPersonalAccessTokenRepositoryError> {
    let found = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM tenant_accounts account
         INNER JOIN tenants t ON t.id = account.tenant_id
         WHERE account.id = $1 AND account.tenant_id = $2
           AND account.status = 'active' AND t.status = 'active'
         FOR UPDATE OF account",
    )
    .bind(tenant_account_user_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    found
        .ok_or(TenantPersonalAccessTokenRepositoryError::TenantAccountNotFound)
        .map(|_| ())
}

async fn insert_sqlite_token(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
    token: NewTenantPersonalAccessToken,
    now: DateTime<Utc>,
) -> Result<TenantPersonalAccessTokenRecord, TenantPersonalAccessTokenRepositoryError> {
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO tenant_personal_access_tokens (
             id, tenant_id, tenant_account_user_id, name, token_prefix, token_hash, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(token.id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_account_user_id.to_string())
    .bind(&token.name)
    .bind(&token.token_prefix)
    .bind(&token.token_hash)
    .bind(now.to_rfc3339())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Err(TenantPersonalAccessTokenRepositoryError::TokenPrefixConflict);
    }
    Ok(record_from_new(
        token,
        tenant_id,
        tenant_account_user_id,
        now,
    ))
}

async fn insert_postgres_token(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
    token: NewTenantPersonalAccessToken,
    now: DateTime<Utc>,
) -> Result<TenantPersonalAccessTokenRecord, TenantPersonalAccessTokenRepositoryError> {
    let inserted = sqlx::query(
        "INSERT INTO tenant_personal_access_tokens (
             id, tenant_id, tenant_account_user_id, name, token_prefix, token_hash, created_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (token_prefix) DO NOTHING
         RETURNING id",
    )
    .bind(token.id)
    .bind(tenant_id)
    .bind(tenant_account_user_id)
    .bind(&token.name)
    .bind(&token.token_prefix)
    .bind(&token.token_hash)
    .bind(now)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_none() {
        return Err(TenantPersonalAccessTokenRepositoryError::TokenPrefixConflict);
    }
    Ok(record_from_new(
        token,
        tenant_id,
        tenant_account_user_id,
        now,
    ))
}

fn record_from_new(
    token: NewTenantPersonalAccessToken,
    tenant_id: Uuid,
    tenant_account_user_id: Uuid,
    created_at: DateTime<Utc>,
) -> TenantPersonalAccessTokenRecord {
    TenantPersonalAccessTokenRecord {
        id: token.id,
        tenant_id,
        tenant_account_user_id,
        name: token.name,
        token_prefix: token.token_prefix,
        created_at,
        last_used_at: None,
        revoked_at: None,
    }
}

fn validate_new_token(
    token: &NewTenantPersonalAccessToken,
) -> Result<(), TenantPersonalAccessTokenRepositoryError> {
    if token.name.trim().is_empty() {
        return Err(TenantPersonalAccessTokenRepositoryError::EmptyName);
    }
    if token.token_prefix.is_empty() {
        return Err(TenantPersonalAccessTokenRepositoryError::EmptyTokenPrefix);
    }
    if !is_sha256_hex(&token.token_hash) {
        return Err(TenantPersonalAccessTokenRepositoryError::InvalidTokenHash);
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sqlite_record(
    row: sqlx::sqlite::SqliteRow,
) -> Result<TenantPersonalAccessTokenRecord, TenantPersonalAccessTokenRepositoryError> {
    Ok(TenantPersonalAccessTokenRecord {
        id: parse_uuid(row.try_get("id")?)?,
        tenant_id: parse_uuid(row.try_get("tenant_id")?)?,
        tenant_account_user_id: parse_uuid(row.try_get("tenant_account_user_id")?)?,
        name: row.try_get("name")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: sqlite_timestamp(row.try_get("created_at")?)?,
        last_used_at: row
            .try_get::<Option<String>, _>("last_used_at")?
            .map(sqlite_timestamp)
            .transpose()?,
        revoked_at: row
            .try_get::<Option<String>, _>("revoked_at")?
            .map(sqlite_timestamp)
            .transpose()?,
    })
}

fn postgres_record(
    row: sqlx::postgres::PgRow,
) -> Result<TenantPersonalAccessTokenRecord, TenantPersonalAccessTokenRepositoryError> {
    Ok(TenantPersonalAccessTokenRecord {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        tenant_account_user_id: row.try_get("tenant_account_user_id")?,
        name: row.try_get("name")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn parse_uuid(value: String) -> Result<Uuid, TenantPersonalAccessTokenRepositoryError> {
    value
        .parse()
        .map_err(|_| TenantPersonalAccessTokenRepositoryError::TokenNotFound)
}

fn sqlite_timestamp(
    value: String,
) -> Result<DateTime<Utc>, TenantPersonalAccessTokenRepositoryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc())
        })
        .map_err(|_| TenantPersonalAccessTokenRepositoryError::InvalidStoredTimestamp)
}
