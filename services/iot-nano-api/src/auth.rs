use std::str::FromStr;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use axum::http::{HeaderMap, header::AUTHORIZATION};
use chrono::{DateTime, Utc};
use iot_storage::{
    OAuthRepository, PlatformStore, PlatformStoreError, TenantIdentityError,
    TenantIdentityRepository,
};
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use sqlx::{PgPool, Row, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

const ADMIN_USERNAME: &str = "admin";
const SYSTEM_USERNAME: &str = "system";
const VIEWER_USERNAME: &str = "viewer";
const INITIAL_ADMIN_PASSWORD: &str = "NanoAdmin@1234";
const INITIAL_SYSTEM_PASSWORD: &str = "NanoSystem@1234";
const INITIAL_VIEWER_PASSWORD: &str = "NanoView@1234";
pub const DEFAULT_APP: &str = "/apps/powermonitor";
pub const POWER_MONITOR_APP: &str = "powermonitor";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Viewer,
}

impl Role {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Viewer => "viewer",
        }
    }

    pub const fn username(self) -> &'static str {
        match self {
            Self::Admin => ADMIN_USERNAME,
            Self::Viewer => VIEWER_USERNAME,
        }
    }
}

impl FromStr for Role {
    type Err = AuthError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "admin" => Ok(Self::Admin),
            "viewer" => Ok(Self::Viewer),
            _ => Err(AuthError::InvalidRole),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountClass {
    System,
    Admin,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PrincipalKind {
    System,
    Tenant,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    pub kind: PrincipalKind,
    pub principal_id: Uuid,
    pub tenant_id: Option<Uuid>,
}

impl AccountClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Admin => "admin",
            Self::User => "user",
        }
    }

    pub const fn from_legacy_role(role: Role) -> Self {
        match role {
            Role::Admin => Self::Admin,
            Role::Viewer => Self::User,
        }
    }
}

impl FromStr for AccountClass {
    type Err = AuthError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "system" => Ok(Self::System),
            "admin" => Ok(Self::Admin),
            "user" => Ok(Self::User),
            _ => Err(AuthError::InvalidAccountClass),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerAccessToken {
    pub app_id: String,
    pub user_id: Option<Uuid>,
    pub scopes: Vec<String>,
    pub expires_at: DateTime<Utc>,
}

impl BearerAccessToken {
    pub fn allows_scope(&self, required_scope: &str) -> bool {
        self.scopes.iter().any(|scope| scope == required_scope)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BearerAccessTokenError {
    Missing,
    Denied,
    Unavailable,
}

pub fn extract_bearer_access_token(headers: &HeaderMap) -> Result<&str, BearerAccessTokenError> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(BearerAccessTokenError::Missing)?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.bytes().any(|byte| byte.is_ascii_whitespace()))
        .ok_or(BearerAccessTokenError::Missing)?;
    Ok(token)
}

pub async fn validate_bearer_access_token(
    store: &impl OAuthRepository,
    headers: &HeaderMap,
    now: DateTime<Utc>,
) -> Result<BearerAccessToken, BearerAccessTokenError> {
    let token = extract_bearer_access_token(headers)?;
    let record = OAuthRepository::resolve_access_token(store, token, now)
        .await
        .map_err(|error| match error {
            PlatformStoreError::OAuthAccessTokenDenied => BearerAccessTokenError::Denied,
            _ => BearerAccessTokenError::Unavailable,
        })?;
    Ok(BearerAccessToken {
        app_id: record.app_id.as_str().to_owned(),
        user_id: record.user_id,
        scopes: record.scopes,
        expires_at: record.expires_at,
    })
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: Uuid,
    pub role: Role,
    pub account_class: AccountClass,
    pub username: String,
    pub default_app: String,
    pub granted_apps: Vec<String>,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error(
        "password must use at least eight ASCII non-whitespace characters with uppercase, lowercase, digit, and special characters"
    )]
    InvalidPasswordFormat,
    #[error("stored users are incomplete")]
    IncompleteStoredUsers,
    #[error("invalid user role")]
    InvalidRole,
    #[error("invalid account class")]
    InvalidAccountClass,
    #[error("stored password hash is invalid")]
    InvalidStoredHash,
    #[error("authentication failed")]
    AuthenticationFailed,
    #[error("tenant identity is unavailable")]
    TenantIdentity(#[source] TenantIdentityError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("failed to hash password")]
    Hashing,
}

pub fn validate_password(value: &str) -> Result<(), AuthError> {
    if value.len() < 8
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_whitespace())
        || !value.bytes().any(|byte| byte.is_ascii_uppercase())
        || !value.bytes().any(|byte| byte.is_ascii_lowercase())
        || !value.bytes().any(|byte| byte.is_ascii_digit())
        || !value.bytes().any(|byte| !byte.is_ascii_alphanumeric())
    {
        return Err(AuthError::InvalidPasswordFormat);
    }
    Ok(())
}

pub fn generate_session_id() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let mut value = String::from("session_");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    value
}

pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| AuthError::Hashing)
}

pub async fn authenticate_system_account(
    store: &PlatformStore,
    username: &str,
    password: &str,
) -> Result<AuthenticatedPrincipal, AuthError> {
    let credential = TenantIdentityRepository::system_account_credential(store, username)
        .await
        .map_err(AuthError::TenantIdentity)?
        .ok_or(AuthError::AuthenticationFailed)?;
    verify_password(password, &credential.password_hash)?;
    Ok(AuthenticatedPrincipal {
        kind: PrincipalKind::System,
        principal_id: credential.account.id,
        tenant_id: None,
    })
}

pub async fn authenticate_tenant_account(
    store: &PlatformStore,
    tenant_slug: &str,
    password: &str,
) -> Result<AuthenticatedPrincipal, AuthError> {
    let credential = TenantIdentityRepository::tenant_account_credential(store, tenant_slug)
        .await
        .map_err(AuthError::TenantIdentity)?
        .ok_or(AuthError::AuthenticationFailed)?;
    verify_password(password, &credential.password_hash)?;
    Ok(AuthenticatedPrincipal {
        kind: PrincipalKind::Tenant,
        principal_id: credential.account.id,
        tenant_id: Some(credential.tenant.id),
    })
}

fn verify_password(password: &str, password_hash: &str) -> Result<(), AuthError> {
    let password_hash =
        PasswordHash::new(password_hash).map_err(|_| AuthError::InvalidStoredHash)?;
    if Argon2::default()
        .verify_password(password.as_bytes(), &password_hash)
        .is_ok()
    {
        Ok(())
    } else {
        Err(AuthError::AuthenticationFailed)
    }
}

pub async fn bootstrap_users_sqlite(pool: &SqlitePool) -> Result<(), AuthError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query("SELECT role, username FROM users")
        .fetch_all(&mut *transaction)
        .await?;
    let bootstrap_defaults = rows.is_empty();
    if bootstrap_defaults {
        for (role, password) in [
            (Role::Admin, INITIAL_ADMIN_PASSWORD),
            (Role::Viewer, INITIAL_VIEWER_PASSWORD),
        ] {
            validate_password(password)?;
            sqlx::query(
                "INSERT INTO users (
                    id, username, password_hash, role, account_class, default_app
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(role.username())
            .bind(hash_password(password)?)
            .bind(role.as_str())
            .bind(AccountClass::from_legacy_role(role).as_str())
            .bind(DEFAULT_APP)
            .execute(&mut *transaction)
            .await?;
        }
        for username in [ADMIN_USERNAME, VIEWER_USERNAME] {
            let user_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
                .bind(username)
                .fetch_one(&mut *transaction)
                .await?;
            sqlx::query(
                "INSERT OR IGNORE INTO user_app_grants (user_id, app_key)
                 VALUES (?, ?)",
            )
            .bind(user_id)
            .bind(POWER_MONITOR_APP)
            .execute(&mut *transaction)
            .await?;
        }
    } else {
        for row in rows {
            row.try_get::<String, _>("role")?
                .parse::<Role>()
                .map_err(|_| AuthError::IncompleteStoredUsers)?;
        }
    }
    let has_system: bool = sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1 FROM users WHERE account_class = 'system'
         )",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if !has_system {
        validate_password(INITIAL_SYSTEM_PASSWORD)?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO users (
                id, username, password_hash, role, account_class, default_app
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(SYSTEM_USERNAME)
        .bind(hash_password(INITIAL_SYSTEM_PASSWORD)?)
        .bind(Role::Admin.as_str())
        .bind(AccountClass::System.as_str())
        .bind(DEFAULT_APP)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted != 1 {
            return Err(AuthError::IncompleteStoredUsers);
        }
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn authenticate_credentials(
    pool: &PgPool,
    username: &str,
    password: &str,
) -> Result<AuthenticatedUser, AuthError> {
    let row = sqlx::query(
        "SELECT id, role, account_class, password_hash, default_app
         FROM users
         WHERE username = $1",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?
    .ok_or(AuthError::AuthenticationFailed)?;
    let stored_role = row
        .try_get::<String, _>("role")?
        .parse::<Role>()
        .map_err(|_| AuthError::InvalidStoredHash)?;
    let account_class = row
        .try_get::<String, _>("account_class")?
        .parse::<AccountClass>()
        .map_err(|_| AuthError::InvalidStoredHash)?;
    let password_hash = row.try_get::<String, _>("password_hash")?;
    let password_hash =
        PasswordHash::new(&password_hash).map_err(|_| AuthError::InvalidStoredHash)?;
    if Argon2::default()
        .verify_password(password.as_bytes(), &password_hash)
        .is_ok()
    {
        let user_id = row.try_get::<Uuid, _>("id")?;
        let granted_apps = sqlx::query(
            "SELECT app_key
             FROM user_app_grants
             WHERE user_id = $1
             ORDER BY app_key",
        )
        .bind(user_id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|grant| grant.try_get("app_key"))
        .collect::<Result<Vec<String>, sqlx::Error>>()?;
        Ok(AuthenticatedUser {
            user_id,
            role: stored_role,
            account_class,
            username: username.to_owned(),
            default_app: row.try_get("default_app")?,
            granted_apps,
        })
    } else {
        Err(AuthError::AuthenticationFailed)
    }
}

pub async fn authenticate_credentials_sqlite(
    pool: &SqlitePool,
    username: &str,
    password: &str,
) -> Result<AuthenticatedUser, AuthError> {
    let row = sqlx::query(
        "SELECT id, role, account_class, password_hash, default_app
         FROM users
         WHERE username = ?",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?
    .ok_or(AuthError::AuthenticationFailed)?;
    let stored_role = row
        .try_get::<String, _>("role")?
        .parse::<Role>()
        .map_err(|_| AuthError::InvalidStoredHash)?;
    let account_class = row
        .try_get::<String, _>("account_class")?
        .parse::<AccountClass>()
        .map_err(|_| AuthError::InvalidStoredHash)?;
    let password_hash = row.try_get::<String, _>("password_hash")?;
    let password_hash =
        PasswordHash::new(&password_hash).map_err(|_| AuthError::InvalidStoredHash)?;
    if !Argon2::default()
        .verify_password(password.as_bytes(), &password_hash)
        .is_ok()
    {
        return Err(AuthError::AuthenticationFailed);
    }
    let user_id = Uuid::parse_str(&row.try_get::<String, _>("id")?)
        .map_err(|_| AuthError::InvalidStoredHash)?;
    let granted_apps = sqlx::query(
        "SELECT app_key
         FROM user_app_grants
         WHERE user_id = ?
         ORDER BY app_key",
    )
    .bind(user_id.to_string())
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|grant| grant.try_get("app_key"))
    .collect::<Result<Vec<String>, sqlx::Error>>()?;
    Ok(AuthenticatedUser {
        user_id,
        role: stored_role,
        account_class,
        username: username.to_owned(),
        default_app: row.try_get("default_app")?,
        granted_apps,
    })
}
