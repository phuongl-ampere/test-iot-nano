use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use url::Url;

use crate::PlatformStoreError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ApplicationId(String);

impl ApplicationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ApplicationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for ApplicationId {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.len() > 64 {
            return Err(PlatformStoreError::InvalidApplicationId(value.to_owned()));
        }
        let valid = value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase()
            } else {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            }
        });
        if !valid {
            return Err(PlatformStoreError::InvalidApplicationId(value.to_owned()));
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationKind {
    Frontend,
    FullStack,
}

impl ApplicationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Frontend => "frontend",
            Self::FullStack => "full_stack",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "frontend" => Ok(Self::Frontend),
            "full_stack" => Ok(Self::FullStack),
            _ => Err(PlatformStoreError::InvalidApplicationKind(value.to_owned())),
        }
    }
}

impl std::str::FromStr for ApplicationKind {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(String);

impl ClientId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for ClientId {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(PlatformStoreError::EmptyApplicationClientId);
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RedirectUri(String);

impl RedirectUri {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for RedirectUri {
    type Err = PlatformStoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(PlatformStoreError::EmptyApplicationRedirectUri);
        }
        if value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(PlatformStoreError::InvalidApplicationRedirectUri(
                value.to_owned(),
            ));
        }
        let parsed = Url::parse(value)
            .map_err(|_| PlatformStoreError::InvalidApplicationRedirectUri(value.to_owned()))?;
        if !matches!(parsed.scheme(), "https" | "http")
            || parsed.host_str().is_none()
            || parsed.fragment().is_some()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || redirect_uri_has_userinfo(value)
        {
            return Err(PlatformStoreError::InvalidApplicationRedirectUri(
                value.to_owned(),
            ));
        }
        Ok(Self(value.to_owned()))
    }
}

fn redirect_uri_has_userinfo(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, remainder)| {
        remainder
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('@'))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewApplication {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub kind: ApplicationKind,
    pub launch_url: String,
    pub client_id: ClientId,
    pub redirect_uris: Vec<RedirectUri>,
    pub allowed_scopes: Vec<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationRecord {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub kind: ApplicationKind,
    pub launch_url: String,
    pub client_id: ClientId,
    pub redirect_uris: Vec<RedirectUri>,
    pub allowed_scopes: Vec<String>,
    pub enabled: bool,
}

pub trait ApplicationRepository: Send + Sync {
    fn upsert_application<'a>(
        &'a self,
        application: NewApplication,
    ) -> Pin<Box<dyn Future<Output = Result<ApplicationRecord, PlatformStoreError>> + Send + 'a>>;
    fn list_applications_for_tenant<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ApplicationRecord>, PlatformStoreError>> + Send + 'a>>;
    fn find_application_by_app_id<'a>(
        &'a self,
        app_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    >;
    fn find_application_by_client_id<'a>(
        &'a self,
        client_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    >;
}

pub struct NewOAuthClientSecret {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub client_secret: String,
}

pub struct NewOAuthAuthorizationCode {
    pub code: String,
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub user_id: uuid::Uuid,
    pub redirect_uri: RedirectUri,
    pub code_challenge: String,
    pub scopes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct OAuthAuthorizationCodeExchange {
    pub code: String,
    pub client_id: ClientId,
    pub redirect_uri: RedirectUri,
    pub code_verifier: String,
    pub client_secret: Option<String>,
    pub access_token: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct OAuthClientCredentialsToken {
    pub client_id: ClientId,
    pub client_secret: String,
    pub access_token: String,
    pub scopes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthAccessTokenRecord {
    pub app_id: ApplicationId,
    pub tenant_id: uuid::Uuid,
    pub user_id: Option<uuid::Uuid>,
    pub scopes: Vec<String>,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub trait OAuthRepository: Send + Sync {
    fn register_client_secret<'a>(
        &'a self,
        secret: NewOAuthClientSecret,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn issue_authorization_code<'a>(
        &'a self,
        code: NewOAuthAuthorizationCode,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>>;
    fn consume_authorization_code_and_issue_access_token<'a>(
        &'a self,
        exchange: OAuthAuthorizationCodeExchange,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>;
    fn issue_client_credentials_access_token<'a>(
        &'a self,
        request: OAuthClientCredentialsToken,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>;
    fn resolve_access_token<'a>(
        &'a self,
        access_token: &'a str,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>;
}
