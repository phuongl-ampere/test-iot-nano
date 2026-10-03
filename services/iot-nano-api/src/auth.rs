use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use axum::http::{HeaderMap, header::AUTHORIZATION};
use chrono::{DateTime, Utc};
use iot_storage::{
    OAuthRepository, PlatformAccountCredential, PlatformStore, PlatformStoreError,
    TenantIdentityError, TenantIdentityRepository, TenantPersonalAccessTokenRepository,
};
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use sha2::{Digest, Sha256};
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
    pub tenant_account_id: Option<Uuid>,
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

const PERSONAL_ACCESS_TOKEN_PREFIX: &str = "iotpat_";
pub const PERSONAL_ACCESS_TOKEN_APP_ID: &str = "tenant-personal-access-token";
const PERSONAL_ACCESS_TOKEN_SCOPES: [&str; 11] = [
    "assets:read",
    "assets:write",
    "devices:read",
    "devices:write",
    "telemetry:read",
    "alerts:read",
    "alerts:write",
    "commands:read",
    "commands:write",
    "authorization:read",
    "authorization:write",
];

fn personal_access_token_scopes() -> Vec<String> {
    PERSONAL_ACCESS_TOKEN_SCOPES
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn personal_access_token_expiry() -> DateTime<Utc> {
    DateTime::from_timestamp(253_402_300_799, 0).expect("the year 9999 is representable")
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
    store: &(impl OAuthRepository + TenantPersonalAccessTokenRepository),
    headers: &HeaderMap,
    now: DateTime<Utc>,
) -> Result<BearerAccessToken, BearerAccessTokenError> {
    let token = extract_bearer_access_token(headers)?;
    match OAuthRepository::resolve_access_token(store, token, now).await {
        Ok(record) => Ok(BearerAccessToken {
            app_id: record.app_id.as_str().to_owned(),
            tenant_id: record.tenant_id,
            user_id: record.user_id,
            tenant_account_id: None,
            scopes: record.scopes,
            expires_at: record.expires_at,
        }),
        Err(PlatformStoreError::OAuthAccessTokenDenied)
            if token.starts_with(PERSONAL_ACCESS_TOKEN_PREFIX) =>
        {
            let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
            let record = TenantPersonalAccessTokenRepository::resolve_tenant_personal_access_token(
                store,
                &token_hash,
                now,
            )
            .await
            .map_err(|_| BearerAccessTokenError::Unavailable)?
            .ok_or(BearerAccessTokenError::Denied)?;
            Ok(BearerAccessToken {
                app_id: PERSONAL_ACCESS_TOKEN_APP_ID.to_owned(),
                tenant_id: record.tenant_id,
                user_id: None,
                tenant_account_id: Some(record.tenant_account_user_id),
                scopes: personal_access_token_scopes(),
                expires_at: personal_access_token_expiry(),
            })
        }
        Err(PlatformStoreError::OAuthAccessTokenDenied) => Err(BearerAccessTokenError::Denied),
        Err(_) => Err(BearerAccessTokenError::Unavailable),
    }
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
    if password_is_allowed(value, insecure_default_passwords_enabled()) {
        Ok(())
    } else {
        Err(AuthError::InvalidPasswordFormat)
    }
}

fn password_is_allowed(value: &str, allow_insecure_defaults: bool) -> bool {
    if allow_insecure_defaults && matches!(value, "systemadmin" | "tenant" | "user1" | "user2") {
        return true;
    }
    value.len() >= 8
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_whitespace())
        && value.bytes().any(|byte| byte.is_ascii_uppercase())
        && value.bytes().any(|byte| byte.is_ascii_lowercase())
        && value.bytes().any(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| !byte.is_ascii_alphanumeric())
}

fn insecure_default_passwords_enabled() -> bool {
    matches!(
        std::env::var("IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS").as_deref(),
        Ok("true") | Ok("1") | Ok("on")
    )
}

#[cfg(test)]
mod tests {
    use super::password_is_allowed;

    #[test]
    fn insecure_lab_defaults_require_explicit_opt_in() {
        assert!(!password_is_allowed("user1", false));
        assert!(password_is_allowed("systemadmin", true));
        assert!(password_is_allowed("tenant", true));
        assert!(password_is_allowed("user1", true));
        assert!(password_is_allowed("user2", true));
        assert!(!password_is_allowed("weak", true));
        assert!(password_is_allowed("StrongPassword@2026", false));
    }
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
