use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use axum::http::{HeaderMap, header::AUTHORIZATION};
use chrono::{DateTime, Utc};
use iot_storage::{
    OAuthRepository, PlatformAccountCredential, PlatformStore, PlatformStoreError,
    TenantIdentityError, TenantIdentityRepository,
};
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerAccessToken {
    pub app_id: String,
    pub tenant_id: Uuid,
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
        tenant_id: record.tenant_id,
        user_id: record.user_id,
        scopes: record.scopes,
        expires_at: record.expires_at,
    })
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error(
        "password must use at least eight ASCII non-whitespace characters with uppercase, lowercase, digit, and special characters"
    )]
    InvalidPasswordFormat,
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

pub async fn authenticate_platform_account(
    store: &PlatformStore,
    username: &str,
    password: &str,
) -> Result<AuthenticatedPrincipal, AuthError> {
    let credential = TenantIdentityRepository::platform_account_credential(store, username)
        .await
        .map_err(AuthError::TenantIdentity)?
        .ok_or(AuthError::AuthenticationFailed)?;
    match credential {
        PlatformAccountCredential::System(credential) => {
            verify_password(password, &credential.password_hash)?;
            Ok(AuthenticatedPrincipal {
                kind: PrincipalKind::System,
                principal_id: credential.account.id,
                tenant_id: None,
            })
        }
        PlatformAccountCredential::Tenant(credential) => {
            verify_password(password, &credential.password_hash)?;
            Ok(AuthenticatedPrincipal {
                kind: PrincipalKind::Tenant,
                principal_id: credential.account.id,
                tenant_id: Some(credential.tenant.id),
            })
        }
        PlatformAccountCredential::User(credential) => {
            verify_password(password, &credential.password_hash)?;
            Ok(AuthenticatedPrincipal {
                kind: PrincipalKind::User,
                principal_id: credential.user_id,
                tenant_id: Some(credential.tenant_id),
            })
        }
    }
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

pub async fn authenticate_user_account(
    store: &PlatformStore,
    tenant_slug: &str,
    username: &str,
    password: &str,
) -> Result<AuthenticatedPrincipal, AuthError> {
    let credential = TenantIdentityRepository::tenant_user_credential(store, tenant_slug, username)
        .await
        .map_err(AuthError::TenantIdentity)?
        .ok_or(AuthError::AuthenticationFailed)?;
    verify_password(password, &credential.password_hash)?;
    Ok(AuthenticatedPrincipal {
        kind: PrincipalKind::User,
        principal_id: credential.user_id,
        tenant_id: Some(credential.tenant_id),
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
