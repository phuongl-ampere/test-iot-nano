use std::{future::Future, pin::Pin};

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rand_core::{OsRng, RngCore};
use sqlx::{Postgres, Row, Sqlite, Transaction, postgres::PgRow, sqlite::SqliteRow};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AuditAction, AuditPrincipal, AuditTargetType, PlatformStore, PlatformStoreError, audit,
};

const CLAIM_CODE_ALPHABET: &[u8] = b"0123456789";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceClaimPolicy {
    pub enabled: bool,
    pub ttl_seconds: u32,
    pub code_length: u8,
    pub max_failed_attempts: u8,
    pub request_cooldown_seconds: u32,
}

impl Default for DeviceClaimPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_seconds: 900,
            code_length: 6,
            max_failed_attempts: 5,
            request_cooldown_seconds: 30,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedDeviceClaimCode {
    pub code: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceClaimCodeStatus {
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub failed_attempts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedDevice {
    pub device_id: String,
    pub owner_user_id: Uuid,
    pub claimed_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum DeviceClaimError {
    #[error("device claim policy is invalid")]
    InvalidPolicy,
    #[error("device claim policy is disabled")]
    PolicyDisabled,
    #[error("device is unavailable for direct claim")]
    DeviceUnavailable,
    #[error("device claim request is cooling down")]
    RequestCoolingDown,
    #[error("device claim code is unavailable")]
    CodeUnavailable,
    #[error("claiming user is unavailable")]
    UserUnavailable,
    #[error("device claim storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl DeviceClaimError {
    pub const fn is_unavailable_code(&self) -> bool {
        matches!(self, Self::CodeUnavailable)
    }
}

impl From<PlatformStoreError> for DeviceClaimError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for DeviceClaimError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait DeviceClaimRepository: Send + Sync {
    fn get_device_claim_policy<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<DeviceClaimPolicy, DeviceClaimError>> + Send + 'a>>;

    fn update_device_claim_policy<'a>(
        &'a self,
        tenant_id: Uuid,
        policy: DeviceClaimPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<DeviceClaimPolicy, DeviceClaimError>> + Send + 'a>>;

    fn issue_device_claim_code<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IssuedDeviceClaimCode, DeviceClaimError>> + Send + 'a>>;

    fn issue_device_claim_code_from_console<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IssuedDeviceClaimCode, DeviceClaimError>> + Send + 'a>>;

    fn revoke_device_claim_code<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DeviceClaimError>> + Send + 'a>>;

    fn active_device_claim_code<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceClaimCodeStatus>, DeviceClaimError>>
                + Send
                + 'a,
        >,
    >;

    fn claim_device_with_code<'a>(
        &'a self,
        tenant_id: Uuid,
        user_id: Uuid,
        device_id: &'a str,
        code: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ClaimedDevice, DeviceClaimError>> + Send + 'a>>;

    fn claim_device_with_serial_number<'a>(
        &'a self,
        tenant_id: Uuid,
        user_id: Uuid,
        serial_number: &'a str,
        code: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ClaimedDevice, DeviceClaimError>> + Send + 'a>>;
}

impl DeviceClaimRepository for PlatformStore {
    fn get_device_claim_policy<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<DeviceClaimPolicy, DeviceClaimError>> + Send + 'a>>
    {
        Box::pin(async move { get_device_claim_policy(self, tenant_id).await })
    }

    fn update_device_claim_policy<'a>(
        &'a self,
        tenant_id: Uuid,
        policy: DeviceClaimPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<DeviceClaimPolicy, DeviceClaimError>> + Send + 'a>>
    {
        Box::pin(async move { update_device_claim_policy(self, tenant_id, policy).await })
    }

    fn issue_device_claim_code<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IssuedDeviceClaimCode, DeviceClaimError>> + Send + 'a>>
    {
        Box::pin(async move { issue_device_claim_code(self, tenant_id, device_id, true).await })
    }

    fn issue_device_claim_code_from_console<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IssuedDeviceClaimCode, DeviceClaimError>> + Send + 'a>>
    {
        Box::pin(async move { issue_device_claim_code(self, tenant_id, device_id, false).await })
    }

    fn revoke_device_claim_code<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DeviceClaimError>> + Send + 'a>> {
        Box::pin(async move { revoke_device_claim_code(self, tenant_id, device_id).await })
    }

    fn active_device_claim_code<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceClaimCodeStatus>, DeviceClaimError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { active_device_claim_code(self, tenant_id, device_id).await })
    }

    fn claim_device_with_code<'a>(
        &'a self,
        tenant_id: Uuid,
        user_id: Uuid,
        device_id: &'a str,
        code: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ClaimedDevice, DeviceClaimError>> + Send + 'a>> {
        Box::pin(
            async move { claim_device_with_code(self, tenant_id, user_id, device_id, code).await },
        )
    }

    fn claim_device_with_serial_number<'a>(
        &'a self,
        tenant_id: Uuid,
        user_id: Uuid,
        serial_number: &'a str,
        code: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ClaimedDevice, DeviceClaimError>> + Send + 'a>> {
        Box::pin(async move {
            claim_device_with_serial_number(self, tenant_id, user_id, serial_number, code).await
        })
    }
}

async fn get_device_claim_policy(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<DeviceClaimPolicy, DeviceClaimError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT enabled, ttl_seconds, code_length, max_failed_attempts,
                        request_cooldown_seconds
                 FROM tenant_device_claim_policies WHERE tenant_id = ?",
            )
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?;
            row.map(sqlite_policy_from_row)
                .transpose()?
                .map_or_else(|| Ok(DeviceClaimPolicy::default()), Ok)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT enabled, ttl_seconds, code_length, max_failed_attempts,
                        request_cooldown_seconds
                 FROM tenant_device_claim_policies WHERE tenant_id = $1",
            )
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?;
            row.map(timescale_policy_from_row)
                .transpose()?
                .map_or_else(|| Ok(DeviceClaimPolicy::default()), Ok)
        }
    }
}

async fn update_device_claim_policy(
    store: &PlatformStore,
    tenant_id: Uuid,
    policy: DeviceClaimPolicy,
) -> Result<DeviceClaimPolicy, DeviceClaimError> {
    validate_policy(policy)?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO tenant_device_claim_policies (
                    tenant_id, enabled, ttl_seconds, code_length, max_failed_attempts,
                    request_cooldown_seconds, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(tenant_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    ttl_seconds = excluded.ttl_seconds,
                    code_length = excluded.code_length,
                    max_failed_attempts = excluded.max_failed_attempts,
                    request_cooldown_seconds = excluded.request_cooldown_seconds,
                    updated_at = excluded.updated_at",
            )
            .bind(tenant_id.to_string())
            .bind(policy.enabled)
            .bind(i64::from(policy.ttl_seconds))
            .bind(i64::from(policy.code_length))
            .bind(i64::from(policy.max_failed_attempts))
            .bind(i64::from(policy.request_cooldown_seconds))
            .bind(sqlite_timestamp(Utc::now()))
            .execute(store.pool())
            .await?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO tenant_device_claim_policies (
                    tenant_id, enabled, ttl_seconds, code_length, max_failed_attempts,
                    request_cooldown_seconds
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT(tenant_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    ttl_seconds = excluded.ttl_seconds,
                    code_length = excluded.code_length,
                    max_failed_attempts = excluded.max_failed_attempts,
                    request_cooldown_seconds = excluded.request_cooldown_seconds,
                    updated_at = now()",
            )
            .bind(tenant_id)
            .bind(policy.enabled)
            .bind(policy.ttl_seconds as i32)
            .bind(i32::from(policy.code_length))
            .bind(i32::from(policy.max_failed_attempts))
            .bind(policy.request_cooldown_seconds as i32)
            .execute(pool)
            .await?;
        }
    }
    Ok(policy)
}

async fn issue_device_claim_code(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    enforce_request_cooldown: bool,
) -> Result<IssuedDeviceClaimCode, DeviceClaimError> {
    let policy = get_device_claim_policy(store, tenant_id).await?;
    if !policy.enabled {
        return Err(DeviceClaimError::PolicyDisabled);
    }
    let now = Utc::now();
    let expires_at = now + Duration::seconds(i64::from(policy.ttl_seconds));
    let code = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            sqlite_require_claimable_device(&mut transaction, tenant_id, device_id).await?;
            if enforce_request_cooldown {
                if let Some(issued_at) =
                    sqlite_last_claim_code_issued_at(&mut transaction, tenant_id, device_id).await?
                {
                    if now
                        < issued_at + Duration::seconds(i64::from(policy.request_cooldown_seconds))
                    {
                        return Err(DeviceClaimError::RequestCoolingDown);
                    }
                }
            }
            let code = generate_claim_code(policy.code_length);
            let code_hash = hash_claim_code(&code)?;
            sqlx::query(
                "UPDATE device_claim_codes SET revoked_at = ?
                 WHERE tenant_id = ? AND device_id = ? AND consumed_at IS NULL AND revoked_at IS NULL",
            )
            .bind(sqlite_timestamp(now))
            .bind(tenant_id.to_string())
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO device_claim_codes (
                    id, tenant_id, device_id, code_hash, issued_at, expires_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(Uuid::now_v7().to_string())
            .bind(tenant_id.to_string())
            .bind(device_id)
            .bind(code_hash)
            .bind(sqlite_timestamp(now))
            .bind(sqlite_timestamp(expires_at))
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            code
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            timescale_require_claimable_device(&mut transaction, tenant_id, device_id).await?;
            if enforce_request_cooldown {
                if let Some(issued_at) =
                    timescale_last_claim_code_issued_at(&mut transaction, tenant_id, device_id)
                        .await?
                {
                    if now
                        < issued_at + Duration::seconds(i64::from(policy.request_cooldown_seconds))
                    {
                        return Err(DeviceClaimError::RequestCoolingDown);
                    }
                }
            }
            let code = generate_claim_code(policy.code_length);
            let code_hash = hash_claim_code(&code)?;
            sqlx::query(
                "UPDATE device_claim_codes SET revoked_at = now()
                 WHERE tenant_id = $1 AND device_id = $2
                   AND consumed_at IS NULL AND revoked_at IS NULL",
            )
            .bind(tenant_id)
            .bind(device_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO device_claim_codes (
                    id, tenant_id, device_id, code_hash, issued_at, expires_at
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(Uuid::now_v7())
            .bind(tenant_id)
            .bind(device_id)
            .bind(code_hash)
            .bind(now)
            .bind(expires_at)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            code
        }
    };
    Ok(IssuedDeviceClaimCode { code, expires_at })
}

async fn revoke_device_claim_code(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<bool, DeviceClaimError> {
    let changed = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE device_claim_codes SET revoked_at = ?
             WHERE tenant_id = ? AND device_id = ? AND consumed_at IS NULL AND revoked_at IS NULL",
        )
        .bind(sqlite_timestamp(Utc::now()))
        .bind(tenant_id.to_string())
        .bind(device_id)
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE device_claim_codes SET revoked_at = now()
             WHERE tenant_id = $1 AND device_id = $2 AND consumed_at IS NULL AND revoked_at IS NULL",
        )
        .bind(tenant_id)
        .bind(device_id)
        .execute(pool)
        .await?
        .rows_affected(),
    };
    Ok(changed != 0)
}

async fn active_device_claim_code(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<Option<DeviceClaimCodeStatus>, DeviceClaimError> {
    let now = Utc::now();
    let status = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT issued_at, expires_at, failed_attempts FROM device_claim_codes
             WHERE tenant_id = ? AND device_id = ? AND consumed_at IS NULL AND revoked_at IS NULL
             ORDER BY issued_at DESC LIMIT 1",
        )
        .bind(tenant_id.to_string())
        .bind(device_id)
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_claim_status_from_row)
        .transpose()?,
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT issued_at, expires_at, failed_attempts FROM device_claim_codes
             WHERE tenant_id = $1 AND device_id = $2 AND consumed_at IS NULL AND revoked_at IS NULL
             ORDER BY issued_at DESC LIMIT 1",
        )
        .bind(tenant_id)
        .bind(device_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_claim_status_from_row)
        .transpose()?,
    };
    Ok(status.filter(|status| status.expires_at > now))
}

async fn claim_device_with_code(
    store: &PlatformStore,
    tenant_id: Uuid,
    user_id: Uuid,
    device_id: &str,
    code: &str,
) -> Result<ClaimedDevice, DeviceClaimError> {
    let policy = get_device_claim_policy(store, tenant_id).await?;
    if !policy.enabled || normalize_claim_code(code).is_empty() {
        return Err(DeviceClaimError::CodeUnavailable);
    }
    let now = Utc::now();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            sqlite_require_claiming_user(&mut transaction, tenant_id, user_id).await?;
            sqlite_require_claimable_device(&mut transaction, tenant_id, device_id).await?;
            let row = sqlx::query(
                "SELECT id, code_hash, expires_at, failed_attempts FROM device_claim_codes
                 WHERE tenant_id = ? AND device_id = ? AND consumed_at IS NULL AND revoked_at IS NULL
                 ORDER BY issued_at DESC LIMIT 1",
            )
            .bind(tenant_id.to_string())
            .bind(device_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                return Err(DeviceClaimError::CodeUnavailable);
            };
            let code_id: String = row.try_get("id")?;
            let code_hash: String = row.try_get("code_hash")?;
            let expires_at = parse_sqlite_timestamp(&row.try_get::<String, _>("expires_at")?)?;
            let failed_attempts: i64 = row.try_get("failed_attempts")?;
            if expires_at <= now {
                revoke_sqlite_code(&mut transaction, &code_id, now).await?;
                transaction.commit().await?;
                return Err(DeviceClaimError::CodeUnavailable);
            }
            if !verify_claim_code(&code_hash, code) {
                let attempts = failed_attempts + 1;
                let revoked_at = (attempts >= i64::from(policy.max_failed_attempts))
                    .then(|| sqlite_timestamp(now));
                sqlx::query(
                    "UPDATE device_claim_codes SET failed_attempts = ?, revoked_at = COALESCE(revoked_at, ?)
                     WHERE id = ?",
                )
                .bind(attempts)
                .bind(revoked_at)
                .bind(&code_id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Err(DeviceClaimError::CodeUnavailable);
            }
            let revoked_permission_count = sqlite_complete_claim(
                &mut transaction,
                tenant_id,
                user_id,
                device_id,
                &code_id,
                now,
            )
            .await?;
            audit::insert_sqlite_audit_event(
                &mut transaction,
                &audit::NewAuditEvent::new(
                    tenant_id,
                    AuditPrincipal::User(user_id),
                    AuditAction::OwnershipTransferred,
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "source": "device_claim",
                        "revoked_permission_count": revoked_permission_count,
                    }),
                ),
            )
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            timescale_require_claiming_user(&mut transaction, tenant_id, user_id).await?;
            timescale_require_claimable_device(&mut transaction, tenant_id, device_id).await?;
            let row = sqlx::query(
                "SELECT id, code_hash, expires_at, failed_attempts FROM device_claim_codes
                 WHERE tenant_id = $1 AND device_id = $2 AND consumed_at IS NULL AND revoked_at IS NULL
                 ORDER BY issued_at DESC LIMIT 1 FOR UPDATE",
            )
            .bind(tenant_id)
            .bind(device_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(row) = row else {
                return Err(DeviceClaimError::CodeUnavailable);
            };
            let code_id: Uuid = row.try_get("id")?;
            let code_hash: String = row.try_get("code_hash")?;
            let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
            let failed_attempts: i32 = row.try_get("failed_attempts")?;
            if expires_at <= now {
                sqlx::query("UPDATE device_claim_codes SET revoked_at = now() WHERE id = $1")
                    .bind(code_id)
                    .execute(&mut *transaction)
                    .await?;
                transaction.commit().await?;
                return Err(DeviceClaimError::CodeUnavailable);
            }
            if !verify_claim_code(&code_hash, code) {
                let attempts = failed_attempts + 1;
                sqlx::query(
                    "UPDATE device_claim_codes
                     SET failed_attempts = $1,
                         revoked_at = CASE WHEN $1 >= $2 THEN now() ELSE revoked_at END
                     WHERE id = $3",
                )
                .bind(attempts)
                .bind(i32::from(policy.max_failed_attempts))
                .bind(code_id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Err(DeviceClaimError::CodeUnavailable);
            }
            let revoked_permission_count =
                timescale_complete_claim(&mut transaction, tenant_id, user_id, device_id, code_id)
                    .await?;
            audit::insert_timescale_audit_event(
                &mut transaction,
                &audit::NewAuditEvent::new(
                    tenant_id,
                    AuditPrincipal::User(user_id),
                    AuditAction::OwnershipTransferred,
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "source": "device_claim",
                        "revoked_permission_count": revoked_permission_count,
                    }),
                ),
            )
            .await?;
            transaction.commit().await?;
        }
    }
    Ok(ClaimedDevice {
        device_id: device_id.to_owned(),
        owner_user_id: user_id,
        claimed_at: now,
    })
}

async fn claim_device_with_serial_number(
    store: &PlatformStore,
    tenant_id: Uuid,
    user_id: Uuid,
    serial_number: &str,
    code: &str,
) -> Result<ClaimedDevice, DeviceClaimError> {
    let serial_number = serial_number.trim().to_ascii_uppercase();
    let device_id = match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query_scalar::<_, String>(
                "SELECT device_id FROM devices
             WHERE tenant_id = ? AND upper(serial_number) = ? AND deleted_at IS NULL",
            )
            .bind(tenant_id.to_string())
            .bind(&serial_number)
            .fetch_optional(store.pool())
            .await?
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query_scalar::<_, String>(
                "SELECT device_id FROM devices
             WHERE tenant_id = $1 AND upper(serial_number) = $2 AND deleted_at IS NULL",
            )
            .bind(tenant_id)
            .bind(&serial_number)
            .fetch_optional(pool)
            .await?
        }
    }
    .ok_or(DeviceClaimError::DeviceUnavailable)?;

    claim_device_with_code(store, tenant_id, user_id, &device_id, code).await
}

async fn sqlite_complete_claim(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    user_id: Uuid,
    device_id: &str,
    code_id: &str,
    now: DateTime<Utc>,
) -> Result<u64, DeviceClaimError> {
    let now = sqlite_timestamp(now);
    let device_changed = sqlx::query(
        "UPDATE devices SET owner_user_id = ?, claimed_at = ?
         WHERE tenant_id = ? AND device_id = ? AND owner_user_id IS NULL AND deleted_at IS NULL",
    )
    .bind(user_id.to_string())
    .bind(&now)
    .bind(tenant_id.to_string())
    .bind(device_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if device_changed != 1 {
        return Err(DeviceClaimError::DeviceUnavailable);
    }
    let code_changed = sqlx::query(
        "UPDATE device_claim_codes SET consumed_at = ?
         WHERE id = ? AND consumed_at IS NULL AND revoked_at IS NULL",
    )
    .bind(&now)
    .bind(code_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if code_changed != 1 {
        return Err(DeviceClaimError::CodeUnavailable);
    }
    Ok(sqlx::query(
        "UPDATE resource_permissions SET revoked_at = ?
         WHERE tenant_id = ? AND device_id = ? AND revoked_at IS NULL",
    )
    .bind(now)
    .bind(tenant_id.to_string())
    .bind(device_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected())
}

async fn timescale_complete_claim(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    user_id: Uuid,
    device_id: &str,
    code_id: Uuid,
) -> Result<u64, DeviceClaimError> {
    let device_changed = sqlx::query(
        "UPDATE devices SET owner_user_id = $1, claimed_at = now()
         WHERE tenant_id = $2 AND device_id = $3 AND owner_user_id IS NULL AND deleted_at IS NULL",
    )
    .bind(user_id)
    .bind(tenant_id)
    .bind(device_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if device_changed != 1 {
        return Err(DeviceClaimError::DeviceUnavailable);
    }
    let code_changed = sqlx::query(
        "UPDATE device_claim_codes SET consumed_at = now()
         WHERE id = $1 AND consumed_at IS NULL AND revoked_at IS NULL",
    )
    .bind(code_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if code_changed != 1 {
        return Err(DeviceClaimError::CodeUnavailable);
    }
    Ok(sqlx::query(
        "UPDATE resource_permissions SET revoked_at = now()
         WHERE tenant_id = $1 AND device_id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant_id)
    .bind(device_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected())
}

async fn sqlite_require_claiming_user(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), DeviceClaimError> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE id = ? AND tenant_id = ? AND account_class = 'user'",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(DeviceClaimError::UserUnavailable)
    }
}

async fn timescale_require_claiming_user(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), DeviceClaimError> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2 AND account_class = 'user' FOR KEY SHARE",
    )
    .bind(user_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(DeviceClaimError::UserUnavailable)
    }
}

async fn sqlite_require_claimable_device(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceClaimError> {
    let row = sqlx::query(
        "SELECT owner_user_id, is_gateway, gateway_device_id FROM devices
         WHERE tenant_id = ? AND device_id = ? AND deleted_at IS NULL",
    )
    .bind(tenant_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Err(DeviceClaimError::DeviceUnavailable);
    };
    let owner: Option<String> = row.try_get("owner_user_id")?;
    let is_gateway: i64 = row.try_get("is_gateway")?;
    let gateway_device_id: Option<String> = row.try_get("gateway_device_id")?;
    if owner.is_some() || is_gateway != 0 || gateway_device_id.is_some() {
        Err(DeviceClaimError::DeviceUnavailable)
    } else {
        Ok(())
    }
}

async fn timescale_require_claimable_device(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceClaimError> {
    let row = sqlx::query(
        "SELECT owner_user_id, is_gateway, gateway_device_id FROM devices
         WHERE tenant_id = $1 AND device_id = $2 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Err(DeviceClaimError::DeviceUnavailable);
    };
    let owner: Option<Uuid> = row.try_get("owner_user_id")?;
    let is_gateway: bool = row.try_get("is_gateway")?;
    let gateway_device_id: Option<String> = row.try_get("gateway_device_id")?;
    if owner.is_some() || is_gateway || gateway_device_id.is_some() {
        Err(DeviceClaimError::DeviceUnavailable)
    } else {
        Ok(())
    }
}

async fn sqlite_last_claim_code_issued_at(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<Option<DateTime<Utc>>, DeviceClaimError> {
    sqlx::query_scalar::<_, String>(
        "SELECT issued_at FROM device_claim_codes WHERE tenant_id = ? AND device_id = ?
         ORDER BY issued_at DESC LIMIT 1",
    )
    .bind(tenant_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(|value| parse_sqlite_timestamp(&value))
    .transpose()
}

async fn timescale_last_claim_code_issued_at(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<Option<DateTime<Utc>>, DeviceClaimError> {
    sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT issued_at FROM device_claim_codes WHERE tenant_id = $1 AND device_id = $2
         ORDER BY issued_at DESC LIMIT 1 FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(Into::into)
}

async fn revoke_sqlite_code(
    transaction: &mut Transaction<'_, Sqlite>,
    code_id: &str,
    now: DateTime<Utc>,
) -> Result<(), DeviceClaimError> {
    sqlx::query("UPDATE device_claim_codes SET revoked_at = ? WHERE id = ?")
        .bind(sqlite_timestamp(now))
        .bind(code_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn sqlite_policy_from_row(row: SqliteRow) -> Result<DeviceClaimPolicy, DeviceClaimError> {
    let policy = DeviceClaimPolicy {
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        ttl_seconds: row
            .try_get::<i64, _>("ttl_seconds")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
        code_length: row
            .try_get::<i64, _>("code_length")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
        max_failed_attempts: row
            .try_get::<i64, _>("max_failed_attempts")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
        request_cooldown_seconds: row
            .try_get::<i64, _>("request_cooldown_seconds")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
    };
    validate_policy(policy)?;
    Ok(policy)
}

fn timescale_policy_from_row(row: PgRow) -> Result<DeviceClaimPolicy, DeviceClaimError> {
    let policy = DeviceClaimPolicy {
        enabled: row.try_get("enabled")?,
        ttl_seconds: row
            .try_get::<i32, _>("ttl_seconds")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
        code_length: row
            .try_get::<i32, _>("code_length")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
        max_failed_attempts: row
            .try_get::<i32, _>("max_failed_attempts")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
        request_cooldown_seconds: row
            .try_get::<i32, _>("request_cooldown_seconds")?
            .try_into()
            .map_err(|_| DeviceClaimError::InvalidPolicy)?,
    };
    validate_policy(policy)?;
    Ok(policy)
}

fn sqlite_claim_status_from_row(row: SqliteRow) -> Result<DeviceClaimCodeStatus, DeviceClaimError> {
    Ok(DeviceClaimCodeStatus {
        issued_at: parse_sqlite_timestamp(&row.try_get::<String, _>("issued_at")?)?,
        expires_at: parse_sqlite_timestamp(&row.try_get::<String, _>("expires_at")?)?,
        failed_attempts: row
            .try_get::<i64, _>("failed_attempts")?
            .try_into()
            .map_err(|_| DeviceClaimError::CodeUnavailable)?,
    })
}

fn timescale_claim_status_from_row(row: PgRow) -> Result<DeviceClaimCodeStatus, DeviceClaimError> {
    Ok(DeviceClaimCodeStatus {
        issued_at: row.try_get("issued_at")?,
        expires_at: row.try_get("expires_at")?,
        failed_attempts: row
            .try_get::<i32, _>("failed_attempts")?
            .try_into()
            .map_err(|_| DeviceClaimError::CodeUnavailable)?,
    })
}

fn validate_policy(policy: DeviceClaimPolicy) -> Result<(), DeviceClaimError> {
    if !(60..=86_400).contains(&policy.ttl_seconds)
        || policy.code_length != 6
        || !(1..=20).contains(&policy.max_failed_attempts)
        || !(10..=3_600).contains(&policy.request_cooldown_seconds)
    {
        return Err(DeviceClaimError::InvalidPolicy);
    }
    Ok(())
}

fn generate_claim_code(length: u8) -> String {
    let mut characters = Vec::with_capacity(usize::from(length));
    let mut random = [0_u8; 1];
    while characters.len() < usize::from(length) {
        OsRng.fill_bytes(&mut random);
        let index = usize::from(random[0]);
        let limit = 256 - (256 % CLAIM_CODE_ALPHABET.len());
        if index < limit {
            characters.push(CLAIM_CODE_ALPHABET[index % CLAIM_CODE_ALPHABET.len()] as char);
        }
    }
    characters.into_iter().collect()
}

fn hash_claim_code(code: &str) -> Result<String, DeviceClaimError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(normalize_claim_code(code).as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| DeviceClaimError::CodeUnavailable)
}

fn verify_claim_code(hash: &str, code: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(normalize_claim_code(code).as_bytes(), &parsed)
            .is_ok()
    })
}

fn normalize_claim_code(code: &str) -> String {
    code.chars()
        .filter(|character| !matches!(character, '-' | ' '))
        .map(|character| character.to_ascii_uppercase())
        .collect()
}

fn sqlite_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_sqlite_timestamp(value: &str) -> Result<DateTime<Utc>, DeviceClaimError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| DeviceClaimError::CodeUnavailable)
}
