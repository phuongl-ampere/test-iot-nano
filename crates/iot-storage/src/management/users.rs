use std::{collections::BTreeSet, future::Future, pin::Pin};

use chrono::Utc;
use sqlx::{Postgres, Row, Sqlite, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{AccountClass, PlatformStore, PlatformStoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementUserRole {
    Admin,
    Viewer,
}

impl ManagementUserRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Viewer => "viewer",
        }
    }

    fn from_database(value: &str) -> Result<Self, ManagementUserError> {
        match value {
            "admin" => Ok(Self::Admin),
            "viewer" => Ok(Self::Viewer),
            _ => Err(ManagementUserError::InvalidStoredRole(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UserCapability {
    CreateAssets,
    CreateDevices,
    ClaimDevices,
    AssignDevicesToAssets,
    EditResources,
    ControlDevices,
    ShareOwnedResources,
    AssignApplicationProfiles,
    ManageDeviceTokens,
}

impl UserCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateAssets => "create_assets",
            Self::CreateDevices => "create_devices",
            Self::ClaimDevices => "claim_devices",
            Self::AssignDevicesToAssets => "assign_devices_to_assets",
            Self::EditResources => "edit_resources",
            Self::ControlDevices => "control_devices",
            Self::ShareOwnedResources => "share_owned_resources",
            Self::AssignApplicationProfiles => "assign_application_profiles",
            Self::ManageDeviceTokens => "manage_device_tokens",
        }
    }

    fn from_database(value: &str) -> Result<Self, ManagementUserError> {
        match value {
            "create_assets" => Ok(Self::CreateAssets),
            "create_devices" => Ok(Self::CreateDevices),
            "claim_devices" => Ok(Self::ClaimDevices),
            "assign_devices_to_assets" => Ok(Self::AssignDevicesToAssets),
            "edit_resources" => Ok(Self::EditResources),
            "control_devices" => Ok(Self::ControlDevices),
            "share_owned_resources" => Ok(Self::ShareOwnedResources),
            "assign_application_profiles" => Ok(Self::AssignApplicationProfiles),
            "manage_device_tokens" => Ok(Self::ManageDeviceTokens),
            _ => Err(ManagementUserError::InvalidStoredUserCapability(
                value.to_owned(),
            )),
        }
    }
}

fn management_user_account_class_for_role(role: ManagementUserRole) -> AccountClass {
    match role {
        ManagementUserRole::Admin => AccountClass::Admin,
        ManagementUserRole::Viewer => AccountClass::User,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementUser {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub username: String,
    pub role: ManagementUserRole,
    pub account_class: AccountClass,
    pub capabilities: Vec<UserCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateManagementUser {
    pub tenant_id: Uuid,
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateManagementUser {
    pub role: Option<ManagementUserRole>,
}

#[derive(Debug, Error)]
pub enum ManagementUserError {
    #[error("invalid management username: {0:?}")]
    InvalidUsername(String),
    #[error("management user password hash must not be empty")]
    EmptyPasswordHash,
    #[error("management username already exists: {0:?}")]
    UsernameConflict(String),
    #[error("management user was not found")]
    UserNotFound,
    #[error("system management users cannot be changed")]
    SystemUserImmutable,
    #[error("at least one administrator must remain")]
    LastAdministrator,
    #[error("stored management user ID is invalid")]
    InvalidStoredUserId,
    #[error("stored management user role is invalid: {0:?}")]
    InvalidStoredRole(String),
    #[error("stored management user account class is invalid: {0:?}")]
    InvalidStoredAccountClass(String),
    #[error("stored management user capability is invalid: {0:?}")]
    InvalidStoredUserCapability(String),
    #[error("capabilities can only be managed for User accounts")]
    CapabilitiesRequireUserAccount,
    #[error("management user storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementUserError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementUserError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementUserRepository: Send + Sync {
    fn list_management_users<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementUser>, ManagementUserError>> + Send + 'a>>;
    fn create_management_user<'a>(
        &'a self,
        user: CreateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>;
    fn update_management_user<'a>(
        &'a self,
        tenant_id: Uuid,
        username: &'a str,
        user: UpdateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>;
    fn replace_management_user_capabilities<'a>(
        &'a self,
        tenant_id: Uuid,
        username: &'a str,
        capabilities: Vec<UserCapability>,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>;
    fn user_has_management_capability<'a>(
        &'a self,
        tenant_id: Uuid,
        user_id: Uuid,
        capability: UserCapability,
    ) -> Pin<Box<dyn Future<Output = Result<bool, ManagementUserError>> + Send + 'a>>;
}

impl ManagementUserRepository for PlatformStore {
    fn list_management_users<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementUser>, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move { list_management_users(self, tenant_id).await })
    }

    fn create_management_user<'a>(
        &'a self,
        user: CreateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move { create_management_user(self, user).await })
    }

    fn update_management_user<'a>(
        &'a self,
        tenant_id: Uuid,
        username: &'a str,
        user: UpdateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move { update_management_user(self, tenant_id, username, user).await })
    }

    fn replace_management_user_capabilities<'a>(
        &'a self,
        tenant_id: Uuid,
        username: &'a str,
        capabilities: Vec<UserCapability>,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move {
            replace_management_user_capabilities(self, tenant_id, username, capabilities).await
        })
    }

    fn user_has_management_capability<'a>(
        &'a self,
        tenant_id: Uuid,
        user_id: Uuid,
        capability: UserCapability,
    ) -> Pin<Box<dyn Future<Output = Result<bool, ManagementUserError>> + Send + 'a>> {
        Box::pin(async move {
            user_has_management_capability(self, tenant_id, user_id, capability).await
        })
    }
}

async fn list_management_users(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementUser>, ManagementUserError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, tenant_id, username, role, account_class
                 FROM users
                 WHERE tenant_id = ?
                 ORDER BY username, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            let mut users = Vec::with_capacity(rows.len());
            for row in rows {
                users.push(sqlite_management_user_from_row(store.pool(), row).await?);
            }
            Ok(users)
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, tenant_id, username, role, account_class
                 FROM users
                 WHERE tenant_id = $1
                 ORDER BY username, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            let mut users = Vec::with_capacity(rows.len());
            for row in rows {
                users.push(timescale_management_user_from_row(pool, row).await?);
            }
            Ok(users)
        }
    }
}

async fn create_management_user(
    store: &PlatformStore,
    user: CreateManagementUser,
) -> Result<ManagementUser, ManagementUserError> {
    let user = validate_new_management_user(user)?;
    let user_id = Uuid::now_v7();
    let tenant_id = user.tenant_id;
    let username = user.username.clone();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            sqlx::query(
                "INSERT INTO users (
                    id, tenant_id, username, password_hash, role, account_class, updated_at
                 ) VALUES (?, ?, ?, ?, 'viewer', 'user', ?)",
            )
            .bind(user_id.to_string())
            .bind(tenant_id.to_string())
            .bind(&user.username)
            .bind(user.password_hash)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *transaction)
            .await
            .map_err(|error| map_management_username_conflict(error, &username))?;
            for capability in [
                UserCapability::CreateAssets,
                UserCapability::ClaimDevices,
                UserCapability::AssignDevicesToAssets,
                UserCapability::ControlDevices,
                UserCapability::ShareOwnedResources,
            ] {
                sqlx::query(
                    "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES (?, ?, ?)",
                )
                .bind(user_id.to_string())
                .bind(tenant_id.to_string())
                .bind(capability.as_str())
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "INSERT INTO users (
                    id, tenant_id, username, password_hash, role, account_class
                 ) VALUES ($1, $2, $3, $4, 'viewer', 'user')",
            )
            .bind(user_id)
            .bind(tenant_id)
            .bind(&user.username)
            .bind(user.password_hash)
            .execute(&mut *transaction)
            .await
            .map_err(|error| map_management_username_conflict(error, &username))?;
            for capability in [
                UserCapability::CreateAssets,
                UserCapability::ClaimDevices,
                UserCapability::AssignDevicesToAssets,
                UserCapability::ControlDevices,
                UserCapability::ShareOwnedResources,
            ] {
                sqlx::query(
                    "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES ($1, $2, $3)",
                )
                .bind(user_id)
                .bind(tenant_id)
                .bind(capability.as_str())
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
    }
    management_user(store, tenant_id, user_id).await
}

async fn update_management_user(
    store: &PlatformStore,
    tenant_id: Uuid,
    username: &str,
    user: UpdateManagementUser,
) -> Result<ManagementUser, ManagementUserError> {
    if !management_identifier(username) {
        return Err(ManagementUserError::InvalidUsername(username.to_owned()));
    }
    let user_id = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let (user_id, current_role, account_class) =
                sqlite_management_user_mutation_target(&mut transaction, tenant_id, username)
                    .await?;
            protect_management_user_invariants(
                &mut transaction,
                tenant_id,
                username,
                current_role,
                account_class,
                user.role,
            )
            .await?;
            sqlx::query(
                "UPDATE users
                 SET role = COALESCE(?, role),
                     account_class = COALESCE(?, account_class), updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(user.role.map(ManagementUserRole::as_str))
            .bind(
                user.role
                    .map(management_user_account_class_for_role)
                    .map(AccountClass::as_str),
            )
            .bind(Utc::now().to_rfc3339())
            .bind(user_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if user.role == Some(ManagementUserRole::Admin) {
                sqlx::query("DELETE FROM user_capabilities WHERE user_id = ? AND tenant_id = ?")
                    .bind(user_id.to_string())
                    .bind(tenant_id.to_string())
                    .execute(&mut *transaction)
                    .await?;
            }
            transaction.commit().await?;
            user_id
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "SELECT pg_advisory_xact_lock(hashtext('iot_nano:management-users-admin-role'))",
            )
            .execute(&mut *transaction)
            .await?;
            let (user_id, current_role, account_class) =
                timescale_management_user_mutation_target(&mut transaction, tenant_id, username)
                    .await?;
            protect_timescale_management_user_invariants(
                &mut transaction,
                tenant_id,
                username,
                current_role,
                account_class,
                user.role,
            )
            .await?;
            sqlx::query(
                "UPDATE users
                 SET role = COALESCE($2, role),
                     account_class = COALESCE($3, account_class), updated_at = now()
                 WHERE id = $1 AND tenant_id = $4",
            )
            .bind(user_id)
            .bind(user.role.map(ManagementUserRole::as_str))
            .bind(
                user.role
                    .map(management_user_account_class_for_role)
                    .map(AccountClass::as_str),
            )
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            if user.role == Some(ManagementUserRole::Admin) {
                sqlx::query("DELETE FROM user_capabilities WHERE user_id = $1 AND tenant_id = $2")
                    .bind(user_id)
                    .bind(tenant_id)
                    .execute(&mut *transaction)
                    .await?;
            }
            transaction.commit().await?;
            user_id
        }
    };
    management_user(store, tenant_id, user_id).await
}

async fn replace_management_user_capabilities(
    store: &PlatformStore,
    tenant_id: Uuid,
    username: &str,
    capabilities: Vec<UserCapability>,
) -> Result<ManagementUser, ManagementUserError> {
    if !management_identifier(username) {
        return Err(ManagementUserError::InvalidUsername(username.to_owned()));
    }
    let capabilities = capabilities.into_iter().collect::<BTreeSet<_>>();
    let user_id = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let (user_id, _, account_class) =
                sqlite_management_user_mutation_target(&mut transaction, tenant_id, username)
                    .await?;
            if account_class != AccountClass::User {
                return Err(ManagementUserError::CapabilitiesRequireUserAccount);
            }
            sqlx::query("DELETE FROM user_capabilities WHERE user_id = ? AND tenant_id = ?")
                .bind(user_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            for capability in capabilities {
                sqlx::query(
                    "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES (?, ?, ?)",
                )
                .bind(user_id.to_string())
                .bind(tenant_id.to_string())
                .bind(capability.as_str())
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
            user_id
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            let (user_id, _, account_class) =
                timescale_management_user_mutation_target(&mut transaction, tenant_id, username)
                    .await?;
            if account_class != AccountClass::User {
                return Err(ManagementUserError::CapabilitiesRequireUserAccount);
            }
            sqlx::query("DELETE FROM user_capabilities WHERE user_id = $1 AND tenant_id = $2")
                .bind(user_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            for capability in capabilities {
                sqlx::query(
                    "INSERT INTO user_capabilities (user_id, tenant_id, capability) VALUES ($1, $2, $3)",
                )
                .bind(user_id)
                .bind(tenant_id)
                .bind(capability.as_str())
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
            user_id
        }
    };
    management_user(store, tenant_id, user_id).await
}

async fn user_has_management_capability(
    store: &PlatformStore,
    tenant_id: Uuid,
    user_id: Uuid,
    capability: UserCapability,
) -> Result<bool, ManagementUserError> {
    match store {
        PlatformStore::Sqlite(store) => Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1
                FROM user_capabilities capabilities
                JOIN users ON users.id = capabilities.user_id
                    AND users.tenant_id = capabilities.tenant_id
                WHERE capabilities.user_id = ?
                    AND capabilities.tenant_id = ?
                    AND capabilities.capability = ?
                    AND users.account_class = 'user'
             )",
        )
        .bind(user_id.to_string())
        .bind(tenant_id.to_string())
        .bind(capability.as_str())
        .fetch_one(store.pool())
        .await?
            != 0),
        PlatformStore::Timescale(pool) => Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1
                FROM user_capabilities capabilities
                JOIN users ON users.id = capabilities.user_id
                    AND users.tenant_id = capabilities.tenant_id
                WHERE capabilities.user_id = $1
                    AND capabilities.tenant_id = $2
                    AND capabilities.capability = $3
                    AND users.account_class = 'user'
             )",
        )
        .bind(user_id)
        .bind(tenant_id)
        .bind(capability.as_str())
        .fetch_one(pool)
        .await?),
    }
}

async fn sqlite_management_user_mutation_target(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    username: &str,
) -> Result<(Uuid, ManagementUserRole, AccountClass), ManagementUserError> {
    let row = sqlx::query(
        "SELECT id, role, account_class
         FROM users
         WHERE username = ? AND tenant_id = ?",
    )
    .bind(username)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementUserError::UserNotFound)?;
    let user_id = Uuid::parse_str(&row.try_get::<String, _>("id")?)
        .map_err(|_| ManagementUserError::InvalidStoredUserId)?;
    Ok((
        user_id,
        ManagementUserRole::from_database(&row.try_get::<String, _>("role")?)?,
        management_user_account_class(row.try_get::<String, _>("account_class")?)?,
    ))
}

async fn timescale_management_user_mutation_target(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    username: &str,
) -> Result<(Uuid, ManagementUserRole, AccountClass), ManagementUserError> {
    let row = sqlx::query(
        "SELECT id, role, account_class
         FROM users
         WHERE username = $1 AND tenant_id = $2",
    )
    .bind(username)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementUserError::UserNotFound)?;
    Ok((
        row.try_get("id")?,
        ManagementUserRole::from_database(&row.try_get::<String, _>("role")?)?,
        management_user_account_class(row.try_get::<String, _>("account_class")?)?,
    ))
}

async fn protect_management_user_invariants(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    username: &str,
    current_role: ManagementUserRole,
    account_class: AccountClass,
    next_role: Option<ManagementUserRole>,
) -> Result<(), ManagementUserError> {
    if account_class == AccountClass::System {
        return Err(ManagementUserError::SystemUserImmutable);
    }
    if current_role == ManagementUserRole::Admin && next_role == Some(ManagementUserRole::Viewer) {
        let remaining_admins: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users
                 WHERE tenant_id = ? AND role = 'admin' AND username <> ?",
        )
        .bind(tenant_id.to_string())
        .bind(username)
        .fetch_one(&mut **transaction)
        .await?;
        if remaining_admins == 0 {
            return Err(ManagementUserError::LastAdministrator);
        }
    }
    Ok(())
}

async fn protect_timescale_management_user_invariants(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    username: &str,
    current_role: ManagementUserRole,
    account_class: AccountClass,
    next_role: Option<ManagementUserRole>,
) -> Result<(), ManagementUserError> {
    if account_class == AccountClass::System {
        return Err(ManagementUserError::SystemUserImmutable);
    }
    if current_role == ManagementUserRole::Admin && next_role == Some(ManagementUserRole::Viewer) {
        let remaining_admins: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users
             WHERE tenant_id = $1 AND role = 'admin' AND username <> $2",
        )
        .bind(tenant_id)
        .bind(username)
        .fetch_one(&mut **transaction)
        .await?;
        if remaining_admins == 0 {
            return Err(ManagementUserError::LastAdministrator);
        }
    }
    Ok(())
}

fn validate_new_management_user(
    user: CreateManagementUser,
) -> Result<CreateManagementUser, ManagementUserError> {
    if !management_identifier(&user.username) {
        return Err(ManagementUserError::InvalidUsername(user.username));
    }
    if user.password_hash.trim().is_empty() {
        return Err(ManagementUserError::EmptyPasswordHash);
    }
    Ok(user)
}

fn management_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|character| {
            character.is_ascii_alphanumeric() || character == b'_' || character == b'-'
        })
}

fn map_management_username_conflict(error: sqlx::Error, username: &str) -> ManagementUserError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ManagementUserError::UsernameConflict(username.to_owned())
    } else {
        ManagementUserError::from(error)
    }
}

async fn management_user(
    store: &PlatformStore,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<ManagementUser, ManagementUserError> {
    let users = list_management_users(store, tenant_id).await?;
    users
        .into_iter()
        .find(|user| user.id == user_id)
        .ok_or(ManagementUserError::InvalidStoredUserId)
}

async fn sqlite_management_user_from_row(
    pool: &sqlx::SqlitePool,
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementUser, ManagementUserError> {
    let id = Uuid::parse_str(&row.try_get::<String, _>("id")?)
        .map_err(|_| ManagementUserError::InvalidStoredUserId)?;
    let tenant_id = Uuid::parse_str(&row.try_get::<String, _>("tenant_id")?)
        .map_err(|_| ManagementUserError::InvalidStoredUserId)?;
    let capabilities = sqlite_management_user_capabilities(pool, id, tenant_id).await?;
    management_user_from_parts(
        id,
        tenant_id,
        row.try_get("username")?,
        row.try_get("role")?,
        row.try_get("account_class")?,
        capabilities,
    )
}

async fn sqlite_management_user_capabilities(
    pool: &sqlx::SqlitePool,
    user_id: Uuid,
    tenant_id: Uuid,
) -> Result<Vec<UserCapability>, ManagementUserError> {
    let capabilities = sqlx::query_scalar::<_, String>(
        "SELECT capability
         FROM user_capabilities
         WHERE user_id = ? AND tenant_id = ?
         ORDER BY capability",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_all(pool)
    .await?;
    capabilities
        .into_iter()
        .map(|capability| UserCapability::from_database(&capability))
        .collect()
}

async fn timescale_management_user_from_row(
    pool: &sqlx::PgPool,
    row: sqlx::postgres::PgRow,
) -> Result<ManagementUser, ManagementUserError> {
    let id = row.try_get("id")?;
    let tenant_id = row.try_get("tenant_id")?;
    let capabilities = timescale_management_user_capabilities(pool, id, tenant_id).await?;
    management_user_from_parts(
        id,
        tenant_id,
        row.try_get("username")?,
        row.try_get("role")?,
        row.try_get("account_class")?,
        capabilities,
    )
}

async fn timescale_management_user_capabilities(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    tenant_id: Uuid,
) -> Result<Vec<UserCapability>, ManagementUserError> {
    let capabilities = sqlx::query_scalar::<_, String>(
        "SELECT capability
         FROM user_capabilities
         WHERE user_id = $1 AND tenant_id = $2
         ORDER BY capability",
    )
    .bind(user_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await?;
    capabilities
        .into_iter()
        .map(|capability| UserCapability::from_database(&capability))
        .collect()
}

fn management_user_from_parts(
    id: Uuid,
    tenant_id: Uuid,
    username: String,
    role: String,
    account_class: String,
    capabilities: Vec<UserCapability>,
) -> Result<ManagementUser, ManagementUserError> {
    let role = ManagementUserRole::from_database(&role)?;
    let account_class = management_user_account_class(account_class)?;
    Ok(ManagementUser {
        id,
        tenant_id,
        username,
        role,
        account_class,
        capabilities,
    })
}

fn management_user_account_class(
    account_class: String,
) -> Result<AccountClass, ManagementUserError> {
    let account_class = match account_class.as_str() {
        "system" => AccountClass::System,
        "admin" => AccountClass::Admin,
        "user" => AccountClass::User,
        _ => {
            return Err(ManagementUserError::InvalidStoredAccountClass(
                account_class,
            ));
        }
    };
    Ok(account_class)
}
