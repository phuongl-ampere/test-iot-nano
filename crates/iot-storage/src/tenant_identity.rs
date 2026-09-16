use serde_json::Value;
use sqlx::{PgPool, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use crate::PlatformStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountStatus {
    Active,
    Disabled,
}

impl AccountStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantStatus {
    Active,
    Suspended,
    Deleted,
}

impl TenantStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Deleted => "deleted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemAccount {
    pub id: Uuid,
    pub username: String,
    pub status: AccountStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Tenant {
    pub id: Uuid,
    pub slug: String,
    pub status: TenantStatus,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAccount {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub status: AccountStatus,
    pub credential_version: i32,
}

#[derive(Debug, Clone)]
pub struct NewSystemAccount {
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug, Clone)]
pub struct NewTenant {
    pub slug: String,
    pub metadata: Value,
}

#[derive(Debug, Clone)]
pub struct NewTenantAccount {
    pub password_hash: String,
}

#[derive(Debug, Error)]
pub enum TenantIdentityError {
    #[error("system account username is required")]
    EmptySystemUsername,
    #[error("tenant slug must contain lowercase letters, digits, or hyphens")]
    InvalidTenantSlug,
    #[error("password hash is required")]
    EmptyPasswordHash,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub struct TenantIdentityRepository;

impl TenantIdentityRepository {
    pub async fn bootstrap_system_account(
        store: &PlatformStore,
        account: NewSystemAccount,
    ) -> Result<SystemAccount, TenantIdentityError> {
        validate_system_account(&account)?;
        let result = SystemAccount {
            id: Uuid::now_v7(),
            username: account.username,
            status: AccountStatus::Active,
        };
        match store {
            PlatformStore::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO system_accounts (id, username, password_hash, status)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(result.id.to_string())
                .bind(&result.username)
                .bind(account.password_hash)
                .bind(result.status.as_str())
                .execute(&store.pool)
                .await?;
            }
            PlatformStore::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO system_accounts (id, username, password_hash, status)
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(result.id)
                .bind(&result.username)
                .bind(account.password_hash)
                .bind(result.status.as_str())
                .execute(pool)
                .await?;
            }
        }
        Ok(result)
    }

    pub async fn create_tenant_with_account(
        store: &PlatformStore,
        tenant: NewTenant,
        account: NewTenantAccount,
    ) -> Result<(Tenant, TenantAccount), TenantIdentityError> {
        validate_tenant(&tenant)?;
        if account.password_hash.is_empty() {
            return Err(TenantIdentityError::EmptyPasswordHash);
        }
        let password_hash = account.password_hash;
        let tenant = Tenant {
            id: Uuid::now_v7(),
            slug: tenant.slug,
            status: TenantStatus::Active,
            metadata: tenant.metadata,
        };
        let account = TenantAccount {
            id: Uuid::now_v7(),
            tenant_id: tenant.id,
            status: AccountStatus::Active,
            credential_version: 1,
        };

        match store {
            PlatformStore::Sqlite(store) => {
                create_sqlite_tenant_with_account(&store.pool, &tenant, &account, &password_hash)
                    .await?;
            }
            PlatformStore::Timescale(pool) => {
                create_timescale_tenant_with_account(pool, &tenant, &account, &password_hash)
                    .await?;
            }
        }
        Ok((tenant, account))
    }
}

async fn create_sqlite_tenant_with_account(
    pool: &SqlitePool,
    tenant: &Tenant,
    account: &TenantAccount,
    password_hash: &str,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES (?, ?, ?, ?)")
        .bind(tenant.id.to_string())
        .bind(&tenant.slug)
        .bind(tenant.status.as_str())
        .bind(tenant.metadata.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "INSERT INTO tenant_accounts (id, tenant_id, password_hash, status, credential_version)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(account.id.to_string())
    .bind(account.tenant_id.to_string())
    .bind(password_hash)
    .bind(account.status.as_str())
    .bind(account.credential_version)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

async fn create_timescale_tenant_with_account(
    pool: &PgPool,
    tenant: &Tenant,
    account: &TenantAccount,
    password_hash: &str,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO tenants (id, slug, status, metadata) VALUES ($1, $2, $3, $4)")
        .bind(tenant.id)
        .bind(&tenant.slug)
        .bind(tenant.status.as_str())
        .bind(&tenant.metadata)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "INSERT INTO tenant_accounts (id, tenant_id, password_hash, status, credential_version)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(account.id)
    .bind(account.tenant_id)
    .bind(password_hash)
    .bind(account.status.as_str())
    .bind(account.credential_version)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await
}

fn validate_system_account(account: &NewSystemAccount) -> Result<(), TenantIdentityError> {
    if account.username.is_empty() {
        return Err(TenantIdentityError::EmptySystemUsername);
    }
    if account.password_hash.is_empty() {
        return Err(TenantIdentityError::EmptyPasswordHash);
    }
    Ok(())
}

fn validate_tenant(tenant: &NewTenant) -> Result<(), TenantIdentityError> {
    if tenant.slug.is_empty()
        || !tenant
            .slug
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(TenantIdentityError::InvalidTenantSlug);
    }
    Ok(())
}
