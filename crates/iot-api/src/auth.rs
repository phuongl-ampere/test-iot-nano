use std::str::FromStr;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use axum::{
    extract::FromRequestParts,
    http::{StatusCode, request::Parts},
};
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use sqlx::{PgPool, Row, SqlitePool};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

const ADMIN_USERNAME: &str = "admin";
const SYSTEM_USERNAME: &str = "system";
const VIEWER_USERNAME: &str = "viewer";
const INITIAL_ADMIN_PASSWORD: &str = "NanoAdmin@1234";
const INITIAL_SYSTEM_PASSWORD: &str = "NanoSystem@1234";
const INITIAL_VIEWER_PASSWORD: &str = "NanoView@1234";
pub const DEFAULT_APP: &str = "/apps/powermonitor";
pub const POWER_MONITOR_APP: &str = "powermonitor";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum AccountClass {
    System,
    Admin,
    User,
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

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: Uuid,
    pub role: Role,
    pub account_class: AccountClass,
    pub username: String,
    pub default_app: String,
    pub granted_apps: Vec<String>,
    pub session_id: String,
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

pub struct Admin;

impl<S> FromRequestParts<S> for Admin
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<AuthContext>() {
            Some(AuthContext {
                account_class: AccountClass::Admin,
                ..
            }) => Ok(Self),
            _ => Err(StatusCode::FORBIDDEN),
        }
    }
}

pub struct System;

impl<S> FromRequestParts<S> for System
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<AuthContext>() {
            Some(AuthContext {
                account_class: AccountClass::System,
                ..
            }) => Ok(Self),
            _ => Err(StatusCode::FORBIDDEN),
        }
    }
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

pub fn default_user(username: impl Into<String>, role: Role) -> AuthenticatedUser {
    AuthenticatedUser {
        user_id: Uuid::nil(),
        role,
        account_class: AccountClass::from_legacy_role(role),
        username: username.into(),
        default_app: DEFAULT_APP.to_owned(),
        granted_apps: vec![POWER_MONITOR_APP.to_owned()],
    }
}

pub(crate) fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| AuthError::Hashing)
}

pub async fn bootstrap_users(pool: &PgPool) -> Result<(), AuthError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query("SELECT role, username FROM users FOR UPDATE")
        .fetch_all(&mut *transaction)
        .await?;
    let bootstrap_defaults = rows.is_empty();

    if bootstrap_defaults {
        for (role, password) in [
            (Role::Admin, INITIAL_ADMIN_PASSWORD),
            (Role::Viewer, INITIAL_VIEWER_PASSWORD),
        ] {
            validate_password(password)?;
            let password_hash = hash_password(password)?;
            sqlx::query(
                "INSERT INTO users (username, password_hash, role, account_class, default_app)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(role.username())
            .bind(password_hash)
            .bind(role.as_str())
            .bind(AccountClass::from_legacy_role(role).as_str())
            .bind(DEFAULT_APP)
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
            "INSERT INTO users (username, password_hash, role, account_class, default_app)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (username) DO NOTHING",
        )
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

    if bootstrap_defaults {
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, app_key)
             SELECT id, $1
             FROM users
             WHERE username IN ($2, $3)
             ON CONFLICT DO NOTHING",
        )
        .bind(POWER_MONITOR_APP)
        .bind(ADMIN_USERNAME)
        .bind(VIEWER_USERNAME)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
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

pub async fn change_password(
    pool: &PgPool,
    username: &str,
    current_password: &str,
    new_password: &str,
) -> Result<(), AuthError> {
    validate_password(new_password)?;
    authenticate_credentials(pool, username, current_password).await?;
    let password_hash = hash_password(new_password)?;
    let updated = sqlx::query(
        "UPDATE users
         SET password_hash = $2, updated_at = now()
         WHERE username = $1",
    )
    .bind(username)
    .bind(password_hash)
    .execute(pool)
    .await?
    .rows_affected();
    if updated == 1 {
        Ok(())
    } else {
        Err(AuthError::IncompleteStoredUsers)
    }
}

pub async fn change_password_sqlite(
    pool: &SqlitePool,
    username: &str,
    current_password: &str,
    new_password: &str,
) -> Result<(), AuthError> {
    validate_password(new_password)?;
    authenticate_credentials_sqlite(pool, username, current_password).await?;
    let updated = sqlx::query(
        "UPDATE users
         SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
         WHERE username = ?",
    )
    .bind(hash_password(new_password)?)
    .bind(username)
    .execute(pool)
    .await?
    .rows_affected();
    if updated == 1 {
        Ok(())
    } else {
        Err(AuthError::IncompleteStoredUsers)
    }
}
