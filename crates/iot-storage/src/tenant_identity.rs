use serde_json::Value;
use sqlx::{PgPool, Row, SqlitePool, postgres::PgRow, sqlite::SqliteRow, types::Json};
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
pub struct TenantSummary {
    pub slug: String,
    pub status: TenantStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAccount {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub username: String,
    pub status: AccountStatus,
    pub credential_version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemAccountCredential {
    pub account: SystemAccount,
    pub password_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TenantAccountCredential {
    pub account: TenantAccount,
    pub tenant: Tenant,
    pub password_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantUserCredential {
    pub user_id: Uuid,
    pub tenant_id: Uuid,
    pub password_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlatformAccountCredential {
    System(SystemAccountCredential),
    Tenant(TenantAccountCredential),
    User(TenantUserCredential),
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
    #[error("tenant account username is required")]
    EmptyTenantAccountUsername,
    #[error("password hash is required")]
    EmptyPasswordHash,
    #[error("stored tenant identity is invalid")]
    InvalidStoredIdentity,
    #[error("tenant was not found or cannot make the requested lifecycle transition")]
    TenantLifecycleDenied,
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
        let username = tenant.slug.clone();
        Self::create_tenant_with_named_account(store, tenant, username, account).await
    }

    pub async fn create_tenant_with_named_account(
        store: &PlatformStore,
        tenant: NewTenant,
        username: String,
        account: NewTenantAccount,
    ) -> Result<(Tenant, TenantAccount), TenantIdentityError> {
        validate_tenant(&tenant)?;
        validate_tenant_account_username(&username)?;
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
            username,
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

    pub async fn system_account_credential(
        store: &PlatformStore,
        username: &str,
    ) -> Result<Option<SystemAccountCredential>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT id, username, password_hash
                     FROM system_accounts
                     WHERE username = ? AND status = 'active'",
                )
                .bind(username)
                .fetch_optional(&store.pool)
                .await?;
                row.map(system_account_credential_from_sqlite).transpose()
            }
            PlatformStore::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT id, username, password_hash
                     FROM system_accounts
                     WHERE username = $1 AND status = 'active'",
                )
                .bind(username)
                .fetch_optional(pool)
                .await?;
                row.map(system_account_credential_from_postgres).transpose()
            }
        }
    }

    pub async fn tenant_account_credential(
        store: &PlatformStore,
        tenant_slug: &str,
    ) -> Result<Option<TenantAccountCredential>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT
                        tenant_accounts.id AS account_id,
                        tenant_accounts.tenant_id AS account_tenant_id,
                        tenant_accounts.username AS account_username,
                        tenant_accounts.password_hash,
                        tenant_accounts.credential_version,
                        tenants.id AS tenant_id,
                        tenants.slug,
                        tenants.metadata
                     FROM tenant_accounts
                     JOIN tenants ON tenants.id = tenant_accounts.tenant_id
                     WHERE tenants.slug = ?
                       AND tenants.status = 'active'
                       AND tenant_accounts.status = 'active'",
                )
                .bind(tenant_slug)
                .fetch_optional(&store.pool)
                .await?;
                row.map(tenant_account_credential_from_sqlite).transpose()
            }
            PlatformStore::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT
                        tenant_accounts.id AS account_id,
                        tenant_accounts.tenant_id AS account_tenant_id,
                        tenant_accounts.username AS account_username,
                        tenant_accounts.password_hash,
                        tenant_accounts.credential_version,
                        tenants.id AS tenant_id,
                        tenants.slug,
                        tenants.metadata
                     FROM tenant_accounts
                     JOIN tenants ON tenants.id = tenant_accounts.tenant_id
                     WHERE tenants.slug = $1
                       AND tenants.status = 'active'
                       AND tenant_accounts.status = 'active'",
                )
                .bind(tenant_slug)
                .fetch_optional(pool)
                .await?;
                row.map(tenant_account_credential_from_postgres).transpose()
            }
        }
    }

    pub async fn tenant_account_credential_by_username(
        store: &PlatformStore,
        username: &str,
    ) -> Result<Option<TenantAccountCredential>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT
                        tenant_accounts.id AS account_id,
                        tenant_accounts.tenant_id AS account_tenant_id,
                        tenant_accounts.username AS account_username,
                        tenant_accounts.password_hash,
                        tenant_accounts.credential_version,
                        tenants.id AS tenant_id,
                        tenants.slug,
                        tenants.metadata
                     FROM tenant_accounts
                     JOIN tenants ON tenants.id = tenant_accounts.tenant_id
                     WHERE tenant_accounts.username = ?
                       AND tenants.status = 'active'
                       AND tenant_accounts.status = 'active'",
                )
                .bind(username)
                .fetch_optional(&store.pool)
                .await?;
                row.map(tenant_account_credential_from_sqlite).transpose()
            }
            PlatformStore::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT
                        tenant_accounts.id AS account_id,
                        tenant_accounts.tenant_id AS account_tenant_id,
                        tenant_accounts.username AS account_username,
                        tenant_accounts.password_hash,
                        tenant_accounts.credential_version,
                        tenants.id AS tenant_id,
                        tenants.slug,
                        tenants.metadata
                     FROM tenant_accounts
                     JOIN tenants ON tenants.id = tenant_accounts.tenant_id
                     WHERE tenant_accounts.username = $1
                       AND tenants.status = 'active'
                       AND tenant_accounts.status = 'active'",
                )
                .bind(username)
                .fetch_optional(pool)
                .await?;
                row.map(tenant_account_credential_from_postgres).transpose()
            }
        }
    }

    pub async fn tenant_user_credential(
        store: &PlatformStore,
        tenant_slug: &str,
        username: &str,
    ) -> Result<Option<TenantUserCredential>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT users.id, users.tenant_id, users.password_hash
                     FROM users
                     JOIN tenants ON tenants.id = users.tenant_id
                     WHERE tenants.slug = ?
                       AND tenants.status = 'active'
                       AND users.username = ?",
                )
                .bind(tenant_slug)
                .bind(username)
                .fetch_optional(&store.pool)
                .await?;
                row.map(tenant_user_credential_from_sqlite).transpose()
            }
            PlatformStore::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT users.id, users.tenant_id, users.password_hash
                     FROM users
                     JOIN tenants ON tenants.id = users.tenant_id
                     WHERE tenants.slug = $1
                       AND tenants.status = 'active'
                       AND users.username = $2",
                )
                .bind(tenant_slug)
                .bind(username)
                .fetch_optional(pool)
                .await?;
                row.map(tenant_user_credential_from_postgres).transpose()
            }
        }
    }

    pub async fn tenant_user_credential_by_username(
        store: &PlatformStore,
        username: &str,
    ) -> Result<Option<TenantUserCredential>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT users.id, users.tenant_id, users.password_hash
                     FROM users
                     JOIN tenants ON tenants.id = users.tenant_id
                     WHERE users.username = ? AND tenants.status = 'active'",
                )
                .bind(username)
                .fetch_optional(&store.pool)
                .await?;
                row.map(tenant_user_credential_from_sqlite).transpose()
            }
            PlatformStore::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT users.id, users.tenant_id, users.password_hash
                     FROM users
                     JOIN tenants ON tenants.id = users.tenant_id
                     WHERE users.username = $1 AND tenants.status = 'active'",
                )
                .bind(username)
                .fetch_optional(pool)
                .await?;
                row.map(tenant_user_credential_from_postgres).transpose()
            }
        }
    }

    pub async fn platform_account_credential(
        store: &PlatformStore,
        username: &str,
    ) -> Result<Option<PlatformAccountCredential>, TenantIdentityError> {
        if let Some(credential) = Self::system_account_credential(store, username).await? {
            return Ok(Some(PlatformAccountCredential::System(credential)));
        }
        if let Some(credential) =
            Self::tenant_account_credential_by_username(store, username).await?
        {
            return Ok(Some(PlatformAccountCredential::Tenant(credential)));
        }
        Self::tenant_user_credential_by_username(store, username)
            .await
            .map(|credential| credential.map(PlatformAccountCredential::User))
    }

    pub async fn list_tenants(store: &PlatformStore) -> Result<Vec<Tenant>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let rows = sqlx::query(
                    "SELECT id, slug, status, metadata
                     FROM tenants
                     WHERE status <> 'deleted'
                     ORDER BY slug, id",
                )
                .fetch_all(&store.pool)
                .await?;
                rows.into_iter().map(tenant_from_sqlite).collect()
            }
            PlatformStore::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT id, slug, status, metadata
                     FROM tenants
                     WHERE status <> 'deleted'
                     ORDER BY slug, id",
                )
                .fetch_all(pool)
                .await?;
                rows.into_iter().map(tenant_from_postgres).collect()
            }
        }
    }

    pub async fn list_tenant_summaries(
        store: &PlatformStore,
    ) -> Result<Vec<TenantSummary>, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let rows = sqlx::query(
                    "SELECT slug, status
                     FROM tenants
                     WHERE status <> 'deleted'
                     ORDER BY slug, id",
                )
                .fetch_all(&store.pool)
                .await?;
                rows.into_iter().map(tenant_summary_from_sqlite).collect()
            }
            PlatformStore::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT slug, status
                     FROM tenants
                     WHERE status <> 'deleted'
                     ORDER BY slug, id",
                )
                .fetch_all(pool)
                .await?;
                rows.into_iter().map(tenant_summary_from_postgres).collect()
            }
        }
    }

    pub async fn suspend_tenant(
        store: &PlatformStore,
        tenant_slug: &str,
    ) -> Result<Uuid, TenantIdentityError> {
        update_tenant_status(store, tenant_slug, TenantStatus::Suspended, "active").await
    }

    pub async fn reactivate_tenant(
        store: &PlatformStore,
        tenant_slug: &str,
    ) -> Result<Uuid, TenantIdentityError> {
        update_tenant_status(store, tenant_slug, TenantStatus::Active, "suspended").await
    }

    pub async fn delete_tenant(
        store: &PlatformStore,
        tenant_slug: &str,
    ) -> Result<Uuid, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let tenant_id = sqlx::query_scalar::<_, String>(
                    "UPDATE tenants
                     SET status = 'deleted', updated_at = CURRENT_TIMESTAMP
                     WHERE slug = ? AND status <> 'deleted'
                     RETURNING id",
                )
                .bind(tenant_slug)
                .fetch_optional(&store.pool)
                .await?
                .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
                parse_uuid(tenant_id)
            }
            PlatformStore::Timescale(pool) => {
                let tenant_id = sqlx::query_scalar::<_, Uuid>(
                    "UPDATE tenants
                     SET status = 'deleted', updated_at = now()
                     WHERE slug = $1 AND status <> 'deleted'
                     RETURNING id",
                )
                .bind(tenant_slug)
                .fetch_optional(pool)
                .await?
                .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
                Ok(tenant_id)
            }
        }
    }

    pub async fn reset_tenant_account_password(
        store: &PlatformStore,
        tenant_slug: &str,
        password_hash: String,
    ) -> Result<TenantAccount, TenantIdentityError> {
        if password_hash.is_empty() {
            return Err(TenantIdentityError::EmptyPasswordHash);
        }
        match store {
            PlatformStore::Sqlite(store) => {
                let mut transaction = store.pool.begin().await?;
                let row = sqlx::query(
                    "SELECT tenant_accounts.id, tenant_accounts.tenant_id, tenant_accounts.status,
                            tenant_accounts.username, tenant_accounts.credential_version
                     FROM tenant_accounts
                     JOIN tenants ON tenants.id = tenant_accounts.tenant_id
                     WHERE tenants.slug = ? AND tenants.status <> 'deleted'",
                )
                .bind(tenant_slug)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
                let account = tenant_account_from_sqlite(row)?;
                sqlx::query(
                    "UPDATE tenant_accounts
                     SET password_hash = ?, credential_version = credential_version + 1,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?",
                )
                .bind(password_hash)
                .bind(account.id.to_string())
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(TenantAccount {
                    credential_version: account.credential_version + 1,
                    ..account
                })
            }
            PlatformStore::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let row = sqlx::query(
                    "SELECT tenant_accounts.id, tenant_accounts.tenant_id, tenant_accounts.status,
                            tenant_accounts.username, tenant_accounts.credential_version
                     FROM tenant_accounts
                     JOIN tenants ON tenants.id = tenant_accounts.tenant_id
                     WHERE tenants.slug = $1 AND tenants.status <> 'deleted'
                     FOR UPDATE OF tenant_accounts",
                )
                .bind(tenant_slug)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
                let account = tenant_account_from_postgres(row)?;
                sqlx::query(
                    "UPDATE tenant_accounts
                     SET password_hash = $1, credential_version = credential_version + 1,
                         updated_at = now()
                     WHERE id = $2",
                )
                .bind(password_hash)
                .bind(account.id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(TenantAccount {
                    credential_version: account.credential_version + 1,
                    ..account
                })
            }
        }
    }

    pub async fn disable_tenant_account(
        store: &PlatformStore,
        tenant_slug: &str,
    ) -> Result<TenantAccount, TenantIdentityError> {
        match store {
            PlatformStore::Sqlite(store) => {
                let row = sqlx::query(
                    "UPDATE tenant_accounts
                     SET status = 'disabled', credential_version = credential_version + 1,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE tenant_id = (
                         SELECT id FROM tenants
                         WHERE slug = ? AND status <> 'deleted'
                     )
                       AND status = 'active'
                     RETURNING id, tenant_id, username, status, credential_version",
                )
                .bind(tenant_slug)
                .fetch_optional(&store.pool)
                .await?
                .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
                tenant_account_from_sqlite(row)
            }
            PlatformStore::Timescale(pool) => {
                let row = sqlx::query(
                    "UPDATE tenant_accounts
                     SET status = 'disabled', credential_version = credential_version + 1,
                         updated_at = now()
                     FROM tenants
                     WHERE tenants.id = tenant_accounts.tenant_id
                       AND tenants.slug = $1
                       AND tenants.status <> 'deleted'
                       AND tenant_accounts.status = 'active'
                     RETURNING tenant_accounts.id, tenant_accounts.tenant_id,
                               tenant_accounts.username, tenant_accounts.status,
                               tenant_accounts.credential_version",
                )
                .bind(tenant_slug)
                .fetch_optional(pool)
                .await?
                .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
                tenant_account_from_postgres(row)
            }
        }
    }
}

async fn update_tenant_status(
    store: &PlatformStore,
    tenant_slug: &str,
    next: TenantStatus,
    expected_current: &str,
) -> Result<Uuid, TenantIdentityError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let tenant_id = sqlx::query_scalar::<_, String>(
                "UPDATE tenants
                 SET status = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE slug = ? AND status = ?
                 RETURNING id",
            )
            .bind(next.as_str())
            .bind(tenant_slug)
            .bind(expected_current)
            .fetch_optional(&store.pool)
            .await?
            .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
            parse_uuid(tenant_id)
        }
        PlatformStore::Timescale(pool) => {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                "UPDATE tenants
                 SET status = $1, updated_at = now()
                 WHERE slug = $2 AND status = $3
                 RETURNING id",
            )
            .bind(next.as_str())
            .bind(tenant_slug)
            .bind(expected_current)
            .fetch_optional(pool)
            .await?
            .ok_or(TenantIdentityError::TenantLifecycleDenied)?;
            Ok(tenant_id)
        }
    }
}

fn system_account_credential_from_sqlite(
    row: SqliteRow,
) -> Result<SystemAccountCredential, TenantIdentityError> {
    Ok(SystemAccountCredential {
        account: SystemAccount {
            id: parse_uuid(row.try_get::<String, _>("id")?)?,
            username: row.try_get("username")?,
            status: AccountStatus::Active,
        },
        password_hash: row.try_get("password_hash")?,
    })
}

fn system_account_credential_from_postgres(
    row: PgRow,
) -> Result<SystemAccountCredential, TenantIdentityError> {
    Ok(SystemAccountCredential {
        account: SystemAccount {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            status: AccountStatus::Active,
        },
        password_hash: row.try_get("password_hash")?,
    })
}

fn tenant_account_credential_from_sqlite(
    row: SqliteRow,
) -> Result<TenantAccountCredential, TenantIdentityError> {
    let tenant_id = parse_uuid(row.try_get::<String, _>("tenant_id")?)?;
    let account_tenant_id = parse_uuid(row.try_get::<String, _>("account_tenant_id")?)?;
    if tenant_id != account_tenant_id {
        return Err(TenantIdentityError::InvalidStoredIdentity);
    }
    let metadata: String = row.try_get("metadata")?;
    Ok(TenantAccountCredential {
        account: TenantAccount {
            id: parse_uuid(row.try_get::<String, _>("account_id")?)?,
            tenant_id,
            username: row.try_get("account_username")?,
            status: AccountStatus::Active,
            credential_version: row.try_get("credential_version")?,
        },
        tenant: Tenant {
            id: tenant_id,
            slug: row.try_get("slug")?,
            status: TenantStatus::Active,
            metadata: serde_json::from_str(&metadata)
                .map_err(|_| TenantIdentityError::InvalidStoredIdentity)?,
        },
        password_hash: row.try_get("password_hash")?,
    })
}

fn tenant_from_sqlite(row: SqliteRow) -> Result<Tenant, TenantIdentityError> {
    let metadata: String = row.try_get("metadata")?;
    Ok(Tenant {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        slug: row.try_get("slug")?,
        status: tenant_status_from_str(&row.try_get::<String, _>("status")?)?,
        metadata: serde_json::from_str(&metadata)
            .map_err(|_| TenantIdentityError::InvalidStoredIdentity)?,
    })
}

fn tenant_summary_from_sqlite(row: SqliteRow) -> Result<TenantSummary, TenantIdentityError> {
    Ok(TenantSummary {
        slug: row.try_get("slug")?,
        status: tenant_status_from_str(&row.try_get::<String, _>("status")?)?,
    })
}

fn tenant_from_postgres(row: PgRow) -> Result<Tenant, TenantIdentityError> {
    Ok(Tenant {
        id: row.try_get("id")?,
        slug: row.try_get("slug")?,
        status: tenant_status_from_str(&row.try_get::<String, _>("status")?)?,
        metadata: row.try_get::<Json<Value>, _>("metadata")?.0,
    })
}

fn tenant_summary_from_postgres(row: PgRow) -> Result<TenantSummary, TenantIdentityError> {
    Ok(TenantSummary {
        slug: row.try_get("slug")?,
        status: tenant_status_from_str(&row.try_get::<String, _>("status")?)?,
    })
}

fn tenant_account_credential_from_postgres(
    row: PgRow,
) -> Result<TenantAccountCredential, TenantIdentityError> {
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let account_tenant_id: Uuid = row.try_get("account_tenant_id")?;
    if tenant_id != account_tenant_id {
        return Err(TenantIdentityError::InvalidStoredIdentity);
    }
    Ok(TenantAccountCredential {
        account: TenantAccount {
            id: row.try_get("account_id")?,
            tenant_id,
            username: row.try_get("account_username")?,
            status: AccountStatus::Active,
            credential_version: row.try_get("credential_version")?,
        },
        tenant: Tenant {
            id: tenant_id,
            slug: row.try_get("slug")?,
            status: TenantStatus::Active,
            metadata: row.try_get::<Json<Value>, _>("metadata")?.0,
        },
        password_hash: row.try_get("password_hash")?,
    })
}

fn tenant_user_credential_from_sqlite(
    row: SqliteRow,
) -> Result<TenantUserCredential, TenantIdentityError> {
    Ok(TenantUserCredential {
        user_id: parse_uuid(row.try_get::<String, _>("id")?)?,
        tenant_id: parse_uuid(row.try_get::<String, _>("tenant_id")?)?,
        password_hash: row.try_get("password_hash")?,
    })
}

fn tenant_user_credential_from_postgres(
    row: PgRow,
) -> Result<TenantUserCredential, TenantIdentityError> {
    Ok(TenantUserCredential {
        user_id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        password_hash: row.try_get("password_hash")?,
    })
}

fn tenant_account_from_sqlite(row: SqliteRow) -> Result<TenantAccount, TenantIdentityError> {
    Ok(TenantAccount {
        id: parse_uuid(row.try_get::<String, _>("id")?)?,
        tenant_id: parse_uuid(row.try_get::<String, _>("tenant_id")?)?,
        username: row.try_get("username")?,
        status: account_status_from_str(&row.try_get::<String, _>("status")?)?,
        credential_version: row.try_get("credential_version")?,
    })
}

fn tenant_account_from_postgres(row: PgRow) -> Result<TenantAccount, TenantIdentityError> {
    Ok(TenantAccount {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        username: row.try_get("username")?,
        status: account_status_from_str(&row.try_get::<String, _>("status")?)?,
        credential_version: row.try_get("credential_version")?,
    })
}

fn account_status_from_str(value: &str) -> Result<AccountStatus, TenantIdentityError> {
    match value {
        "active" => Ok(AccountStatus::Active),
        "disabled" => Ok(AccountStatus::Disabled),
        _ => Err(TenantIdentityError::InvalidStoredIdentity),
    }
}

fn tenant_status_from_str(value: &str) -> Result<TenantStatus, TenantIdentityError> {
    match value {
        "active" => Ok(TenantStatus::Active),
        "suspended" => Ok(TenantStatus::Suspended),
        "deleted" => Ok(TenantStatus::Deleted),
        _ => Err(TenantIdentityError::InvalidStoredIdentity),
    }
}

fn parse_uuid(value: String) -> Result<Uuid, TenantIdentityError> {
    Uuid::parse_str(&value).map_err(|_| TenantIdentityError::InvalidStoredIdentity)
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
        "INSERT INTO tenant_accounts (id, tenant_id, username, password_hash, status, credential_version)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(account.id.to_string())
    .bind(account.tenant_id.to_string())
    .bind(&account.username)
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
        "INSERT INTO tenant_accounts (id, tenant_id, username, password_hash, status, credential_version)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(account.id)
    .bind(account.tenant_id)
    .bind(&account.username)
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

fn validate_tenant_account_username(value: &str) -> Result<(), TenantIdentityError> {
    if value.is_empty() {
        return Err(TenantIdentityError::EmptyTenantAccountUsername);
    }
    Ok(())
}
