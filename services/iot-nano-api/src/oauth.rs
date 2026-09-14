use std::{str::FromStr, sync::Arc};

use axum::{
    Router,
    extract::{Form, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL, LOCATION, PRAGMA},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use chrono::{Duration, Utc};
use iot_storage::{
    ClientId, NewOAuthAuthorizationCode, OAuthAuthorizationCodeExchange,
    OAuthClientCredentialsToken, OAuthRepository, PlatformStore, PlatformStoreError, RedirectUri,
};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    application_registry::{ApplicationRegistry, ApplicationRegistryError},
    routes::{ApiState, SqliteApiState},
};

const AUTHORIZATION_CODE_TTL: Duration = Duration::minutes(5);
const ACCESS_TOKEN_TTL: Duration = Duration::hours(1);

pub fn public_oauth_router<S>(store: Arc<PlatformStore>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    public_oauth_router_with_optional_browser_session_verifier(store, None)
}

pub fn public_oauth_router_with_browser_session_verifier<S>(
    store: Arc<PlatformStore>,
    browser_session_verifier: Arc<dyn OAuthBrowserSessionVerifier>,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    public_oauth_router_with_optional_browser_session_verifier(
        store,
        Some(browser_session_verifier),
    )
}

fn public_oauth_router_with_optional_browser_session_verifier<S>(
    store: Arc<PlatformStore>,
    browser_session_verifier: Option<Arc<dyn OAuthBrowserSessionVerifier>>,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/oauth/authorize", get(public_authorize))
        .route("/oauth/token", post(public_token))
        .with_state(PublicOAuthState {
            store,
            browser_session_verifier,
        })
}

pub trait OAuthBrowserSessionVerifier: Send + Sync {
    fn authenticated_user_id(&self, headers: &HeaderMap) -> Option<Uuid>;
}

#[derive(Clone)]
struct PublicOAuthState {
    store: Arc<PlatformStore>,
    browser_session_verifier: Option<Arc<dyn OAuthBrowserSessionVerifier>>,
}

trait OAuthState {
    fn authenticate_browser_session(&self, headers: &HeaderMap) -> Option<Uuid>;
    fn oauth_store(&self) -> Option<Arc<PlatformStore>>;
}

impl OAuthState for ApiState {
    fn authenticate_browser_session(&self, headers: &HeaderMap) -> Option<Uuid> {
        ApiState::authenticate_browser_session(self, headers).map(|session| session.user_id)
    }

    fn oauth_store(&self) -> Option<Arc<PlatformStore>> {
        ApiState::oauth_store(self)
    }
}

impl OAuthState for SqliteApiState {
    fn authenticate_browser_session(&self, headers: &HeaderMap) -> Option<Uuid> {
        SqliteApiState::authenticate_browser_session(self, headers).map(|session| session.user_id)
    }

    fn oauth_store(&self) -> Option<Arc<PlatformStore>> {
        SqliteApiState::oauth_store(self)
    }
}

impl OAuthState for PublicOAuthState {
    fn authenticate_browser_session(&self, headers: &HeaderMap) -> Option<Uuid> {
        self.browser_session_verifier
            .as_ref()
            .and_then(|verifier| verifier.authenticated_user_id(headers))
    }

    fn oauth_store(&self) -> Option<Arc<PlatformStore>> {
        Some(Arc::clone(&self.store))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AuthorizationRequest {
    response_type: Option<String>,
    client_id: Option<String>,
    redirect_uri: Option<String>,
    scope: Option<String>,
    state: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TokenRequest {
    grant_type: Option<String>,
    code: Option<String>,
    redirect_uri: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    code_verifier: Option<String>,
    scope: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum OAuthError {
    InvalidRequest,
    InvalidScope,
    UnauthorizedClient,
    AccessDenied,
    InvalidGrant,
    InvalidClient,
    UnsupportedGrantType,
    ServerError,
}

#[derive(Serialize)]
struct OAuthErrorBody {
    error: &'static str,
    error_description: &'static str,
}

impl OAuthError {
    pub(crate) fn into_response(self) -> Response {
        let (status, error, error_description) = match self {
            Self::InvalidRequest => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "The OAuth request is invalid.",
            ),
            Self::InvalidScope => (
                StatusCode::BAD_REQUEST,
                "invalid_scope",
                "The requested scope is not allowed.",
            ),
            Self::UnauthorizedClient => (
                StatusCode::BAD_REQUEST,
                "unauthorized_client",
                "The client is not authorized for this request.",
            ),
            Self::AccessDenied => (
                StatusCode::UNAUTHORIZED,
                "access_denied",
                "A browser session is required to authorize this client.",
            ),
            Self::InvalidGrant => (
                StatusCode::BAD_REQUEST,
                "invalid_grant",
                "The authorization grant is invalid, expired, or has been consumed.",
            ),
            Self::InvalidClient => (
                StatusCode::UNAUTHORIZED,
                "invalid_client",
                "Client authentication failed.",
            ),
            Self::UnsupportedGrantType => (
                StatusCode::BAD_REQUEST,
                "unsupported_grant_type",
                "The grant type is not supported.",
            ),
            Self::ServerError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "The authorization server could not complete the request.",
            ),
        };
        let mut response = (
            status,
            axum::Json(OAuthErrorBody {
                error,
                error_description,
            }),
        )
            .into_response();
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
            .headers_mut()
            .insert(PRAGMA, HeaderValue::from_static("no-cache"));
        response
    }
}

impl From<ApplicationRegistryError> for OAuthError {
    fn from(error: ApplicationRegistryError) -> Self {
        match error {
            ApplicationRegistryError::RedirectDenied => Self::InvalidRequest,
            ApplicationRegistryError::Disabled | ApplicationRegistryError::UnknownClient => {
                Self::UnauthorizedClient
            }
            ApplicationRegistryError::ScopeDenied => Self::InvalidScope,
            _ => Self::ServerError,
        }
    }
}

pub(crate) async fn authorize(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(request): Query<AuthorizationRequest>,
) -> Response {
    authorize_response(&state, &headers, request).await
}

pub(crate) async fn sqlite_authorize(
    State(state): State<SqliteApiState>,
    headers: HeaderMap,
    Query(request): Query<AuthorizationRequest>,
) -> Response {
    authorize_response(&state, &headers, request).await
}

async fn public_authorize(
    State(state): State<PublicOAuthState>,
    headers: HeaderMap,
    Query(request): Query<AuthorizationRequest>,
) -> Response {
    authorize_response(&state, &headers, request).await
}

async fn authorize_response(
    state: &impl OAuthState,
    headers: &HeaderMap,
    request: AuthorizationRequest,
) -> Response {
    match authorize_request(state, headers, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn token(
    State(state): State<ApiState>,
    headers: HeaderMap,
    form: Result<Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Response {
    token_response_for_request(&state, &headers, form).await
}

pub(crate) async fn sqlite_token(
    State(state): State<SqliteApiState>,
    headers: HeaderMap,
    form: Result<Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Response {
    token_response_for_request(&state, &headers, form).await
}

async fn public_token(
    State(state): State<PublicOAuthState>,
    headers: HeaderMap,
    form: Result<Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Response {
    token_response_for_request(&state, &headers, form).await
}

async fn token_response_for_request(
    state: &impl OAuthState,
    headers: &HeaderMap,
    form: Result<Form<TokenRequest>, axum::extract::rejection::FormRejection>,
) -> Response {
    let Form(request) = match form {
        Ok(request) => request,
        Err(_) => return OAuthError::InvalidRequest.into_response(),
    };
    match issue_token(state, headers, request).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn authorize_request(
    state: &impl OAuthState,
    headers: &HeaderMap,
    request: AuthorizationRequest,
) -> Result<Response, OAuthError> {
    if request.response_type.as_deref() != Some("code") {
        return Err(OAuthError::InvalidRequest);
    }
    let client_id = request
        .client_id
        .as_deref()
        .ok_or(OAuthError::InvalidRequest)?;
    let redirect_uri = request
        .redirect_uri
        .as_deref()
        .ok_or(OAuthError::InvalidRequest)?;
    let state_parameter = request
        .state
        .as_deref()
        .filter(|state| !state.is_empty())
        .ok_or(OAuthError::InvalidRequest)?;
    if redirect_uri.contains('#') {
        return Err(OAuthError::InvalidRequest);
    }
    let code_challenge = request
        .code_challenge
        .as_deref()
        .filter(|challenge| is_s256_challenge(challenge))
        .ok_or(OAuthError::InvalidRequest)?;
    if request.code_challenge_method.as_deref() != Some("S256") {
        return Err(OAuthError::InvalidRequest);
    }
    let user_id = state
        .authenticate_browser_session(headers)
        .ok_or(OAuthError::AccessDenied)?;
    let store = state.oauth_store().ok_or(OAuthError::ServerError)?;
    let scopes = requested_scopes(request.scope.as_deref())?;
    let application = ApplicationRegistry::new(store.clone())
        .validate_authorization_request(client_id, redirect_uri, &scopes)
        .await?;
    let code = random_credential()?;
    let issued_at = Utc::now();
    OAuthRepository::issue_authorization_code(
        store.as_ref(),
        NewOAuthAuthorizationCode {
            code: code.clone(),
            app_id: application.app_id,
            user_id,
            redirect_uri: RedirectUri::from_str(redirect_uri)
                .map_err(|_| OAuthError::InvalidRequest)?,
            code_challenge: code_challenge.to_owned(),
            scopes,
            issued_at,
            expires_at: issued_at + AUTHORIZATION_CODE_TTL,
        },
    )
    .await
    .map_err(issue_code_error)?;

    authorization_redirect(redirect_uri, &code, Some(state_parameter))
}

fn requested_scopes(scope: Option<&str>) -> Result<Vec<String>, OAuthError> {
    let scopes = scope
        .unwrap_or_default()
        .split_ascii_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    (!scopes.is_empty())
        .then_some(scopes)
        .ok_or(OAuthError::InvalidRequest)
}

fn is_s256_challenge(challenge: &str) -> bool {
    URL_SAFE_NO_PAD
        .decode(challenge)
        .is_ok_and(|decoded| decoded.len() == 32)
}

fn is_pkce_code_verifier(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
}

fn random_credential() -> Result<String, OAuthError> {
    let mut bytes = [0_u8; 32];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| OAuthError::ServerError)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn issue_code_error(error: PlatformStoreError) -> OAuthError {
    match error {
        PlatformStoreError::OAuthScopeDenied => OAuthError::InvalidScope,
        PlatformStoreError::ApplicationDisabled(_)
        | PlatformStoreError::OAuthApplicationNotFound => OAuthError::UnauthorizedClient,
        PlatformStoreError::OAuthRedirectUriDenied => OAuthError::InvalidRequest,
        _ => OAuthError::ServerError,
    }
}

async fn issue_token(
    state: &impl OAuthState,
    headers: &HeaderMap,
    request: TokenRequest,
) -> Result<Response, OAuthError> {
    match request.grant_type.as_deref() {
        Some("authorization_code") => exchange_authorization_code(state, headers, request).await,
        Some("client_credentials") => issue_client_credentials(state, headers, request).await,
        Some(_) => Err(OAuthError::UnsupportedGrantType),
        None => Err(OAuthError::InvalidRequest),
    }
}

async fn exchange_authorization_code(
    state: &impl OAuthState,
    headers: &HeaderMap,
    request: TokenRequest,
) -> Result<Response, OAuthError> {
    let store = state.oauth_store().ok_or(OAuthError::ServerError)?;
    let client = token_client_authentication(headers, &request)?;
    let code = request.code.ok_or(OAuthError::InvalidRequest)?;
    let client_id = ClientId::from_str(&client.client_id).map_err(|_| OAuthError::InvalidClient)?;
    let redirect_uri = RedirectUri::from_str(
        request
            .redirect_uri
            .as_deref()
            .ok_or(OAuthError::InvalidRequest)?,
    )
    .map_err(|_| OAuthError::InvalidGrant)?;
    let code_verifier = request.code_verifier.ok_or(OAuthError::InvalidGrant)?;
    if !is_pkce_code_verifier(&code_verifier) {
        return Err(OAuthError::InvalidGrant);
    }
    let access_token = random_credential()?;
    let issued_at = Utc::now();
    let record = OAuthRepository::consume_authorization_code_and_issue_access_token(
        store.as_ref(),
        OAuthAuthorizationCodeExchange {
            code,
            client_id,
            redirect_uri,
            code_verifier,
            client_secret: client.client_secret,
            access_token: access_token.clone(),
            issued_at,
            expires_at: issued_at + ACCESS_TOKEN_TTL,
        },
    )
    .await
    .map_err(exchange_code_error)?;

    token_response(access_token, record.scopes)
}

fn exchange_code_error(error: PlatformStoreError) -> OAuthError {
    match error {
        PlatformStoreError::OAuthAuthorizationCodeDenied => OAuthError::InvalidGrant,
        PlatformStoreError::OAuthClientAuthenticationDenied => OAuthError::InvalidClient,
        _ => OAuthError::ServerError,
    }
}

async fn issue_client_credentials(
    state: &impl OAuthState,
    headers: &HeaderMap,
    request: TokenRequest,
) -> Result<Response, OAuthError> {
    let store = state.oauth_store().ok_or(OAuthError::ServerError)?;
    issue_client_credentials_from_store(store.as_ref(), headers, request).await
}

async fn issue_client_credentials_from_store(
    store: &PlatformStore,
    headers: &HeaderMap,
    request: TokenRequest,
) -> Result<Response, OAuthError> {
    let client = token_client_authentication(headers, &request)?;
    let client_id = ClientId::from_str(&client.client_id).map_err(|_| OAuthError::InvalidClient)?;
    let client_secret = client.client_secret.ok_or(OAuthError::InvalidClient)?;
    let access_token = random_credential()?;
    let issued_at = Utc::now();
    let record = OAuthRepository::issue_client_credentials_access_token(
        store,
        OAuthClientCredentialsToken {
            client_id,
            client_secret,
            access_token: access_token.clone(),
            scopes: requested_scopes(request.scope.as_deref())?,
            issued_at,
            expires_at: issued_at + ACCESS_TOKEN_TTL,
        },
    )
    .await
    .map_err(client_credentials_error)?;

    token_response(access_token, record.scopes)
}

fn client_credentials_error(error: PlatformStoreError) -> OAuthError {
    match error {
        PlatformStoreError::OAuthClientAuthenticationDenied => OAuthError::InvalidClient,
        PlatformStoreError::OAuthScopeDenied => OAuthError::InvalidScope,
        _ => OAuthError::ServerError,
    }
}

struct TokenClientAuthentication {
    client_id: String,
    client_secret: Option<String>,
}

fn token_client_authentication(
    headers: &HeaderMap,
    request: &TokenRequest,
) -> Result<TokenClientAuthentication, OAuthError> {
    let Some(authorization) = headers.get(AUTHORIZATION) else {
        return Ok(TokenClientAuthentication {
            client_id: request.client_id.clone().ok_or(OAuthError::InvalidClient)?,
            client_secret: request.client_secret.clone(),
        });
    };
    if request.client_id.is_some() || request.client_secret.is_some() {
        return Err(OAuthError::InvalidRequest);
    }
    let authorization = authorization
        .to_str()
        .map_err(|_| OAuthError::InvalidClient)?;
    let encoded = authorization
        .strip_prefix("Basic ")
        .ok_or(OAuthError::InvalidClient)?;
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| OAuthError::InvalidClient)?;
    let decoded = std::str::from_utf8(&decoded).map_err(|_| OAuthError::InvalidClient)?;
    let (client_id, client_secret) = decoded.split_once(':').ok_or(OAuthError::InvalidClient)?;
    if client_id.is_empty() || client_secret.is_empty() {
        return Err(OAuthError::InvalidClient);
    }
    Ok(TokenClientAuthentication {
        client_id: client_id.to_owned(),
        client_secret: Some(client_secret.to_owned()),
    })
}

fn token_response(access_token: String, scopes: Vec<String>) -> Result<Response, OAuthError> {
    #[derive(Serialize)]
    struct TokenResponse {
        access_token: String,
        token_type: &'static str,
        expires_in: i64,
        scope: String,
    }

    let mut response = axum::Json(TokenResponse {
        access_token,
        token_type: "Bearer",
        expires_in: ACCESS_TOKEN_TTL.num_seconds(),
        scope: scopes.join(" "),
    })
    .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(PRAGMA, HeaderValue::from_static("no-cache"));
    Ok(response)
}

fn authorization_redirect(
    redirect_uri: &str,
    code: &str,
    state: Option<&str>,
) -> Result<Response, OAuthError> {
    if redirect_uri.contains('#') {
        return Err(OAuthError::InvalidRequest);
    }
    let mut query = format!("code={}", encode_query_component(code));
    if let Some(state) = state {
        query.push_str("&state=");
        query.push_str(&encode_query_component(state));
    }
    let separator = if redirect_uri.contains('?') { "&" } else { "?" };
    let location = format!("{redirect_uri}{separator}{query}");
    let location = HeaderValue::from_str(&location).map_err(|_| OAuthError::InvalidRequest)?;
    let mut response = StatusCode::FOUND.into_response();
    response.headers_mut().insert(LOCATION, location);
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(PRAGMA, HeaderValue::from_static("no-cache"));
    Ok(response)
}

fn encode_query_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}
