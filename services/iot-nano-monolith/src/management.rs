use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, Path, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{COOKIE, SET_COOKIE},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use iot_api::{
    AuthError, DeviceTokenResponse, OAuthBrowserSessionVerifier, POWER_MONITOR_APP, Role,
    TokenVault, authenticate_credentials, authenticate_credentials_sqlite,
    create_platform_device_token, generate_session_id, hash_password,
    provision_platform_device_token, validate_password,
};
use iot_storage::{
    ApplicationKind, ApplicationRepository, ClientId, ManagementChildStatus,
    ManagementDevice as StorageManagementDevice, ManagementDeviceError, ManagementDeviceRepository,
    ManagementDeviceTopology, ManagementGatewayStatus, NewApplication, NewOAuthClientSecret,
    OAuthRepository, PlatformStore, PlatformStoreError, RedirectUri, UpdateManagementDevice,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

const SESSION_COOKIE: &str = "iot_nano_session";
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_LOGIN_FAILURES: u8 = 5;

#[derive(Debug, Error)]
pub enum BootstrapAdminError {
    #[error(
        "bootstrap admin username must use 3-64 ASCII letters, digits, hyphens, or underscores"
    )]
    InvalidUsername,
    #[error("bootstrap admin password is invalid")]
    InvalidPassword(#[source] AuthError),
    #[error("bootstrap admin can run only when the platform has no users")]
    AlreadyInitialized,
    #[error("platform store has no selected backend")]
    NoBackend,
    #[error("bootstrap admin storage operation failed")]
    Storage(#[source] sqlx::Error),
    #[error("bootstrap admin platform migration failed")]
    PlatformMigration(#[source] PlatformStoreError),
}

pub async fn bootstrap_admin(
    store: &PlatformStore,
    username: &str,
    password: &str,
) -> Result<(), BootstrapAdminError> {
    if !is_bootstrap_username(username) {
        return Err(BootstrapAdminError::InvalidUsername);
    }
    validate_password(password).map_err(BootstrapAdminError::InvalidPassword)?;
    let password_hash = hash_password(password).map_err(BootstrapAdminError::InvalidPassword)?;
    let user_id = Uuid::now_v7();
    if let Some(pool) = store.sqlite_pool() {
        return bootstrap_admin_sqlite(pool, user_id, username, &password_hash).await;
    }
    let pool = store
        .timescale_pool()
        .ok_or(BootstrapAdminError::NoBackend)?;
    bootstrap_admin_timescale(pool, user_id, username, &password_hash).await
}

async fn bootstrap_admin_sqlite(
    pool: &sqlx::SqlitePool,
    user_id: Uuid,
    username: &str,
    password_hash: &str,
) -> Result<(), BootstrapAdminError> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(BootstrapAdminError::Storage)?;
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    if users != 0 {
        return Err(BootstrapAdminError::AlreadyInitialized);
    }
    sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app, created_at, updated_at
         ) VALUES (?1, ?2, ?3, 'admin', 'admin', '/apps/powermonitor', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(user_id.to_string())
    .bind(username)
    .bind(password_hash)
    .execute(&mut *transaction)
    .await
    .map_err(BootstrapAdminError::Storage)?;
    sqlx::query("INSERT INTO user_app_grants (user_id, app_key) VALUES (?1, ?2)")
        .bind(user_id.to_string())
        .bind(POWER_MONITOR_APP)
        .execute(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    transaction
        .commit()
        .await
        .map_err(BootstrapAdminError::Storage)
}

async fn bootstrap_admin_timescale(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    username: &str,
    password_hash: &str,
) -> Result<(), BootstrapAdminError> {
    let mut transaction = pool.begin().await.map_err(BootstrapAdminError::Storage)?;
    sqlx::query("LOCK TABLE users IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    if users != 0 {
        return Err(BootstrapAdminError::AlreadyInitialized);
    }
    sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app, created_at, updated_at
         ) VALUES ($1, $2, $3, 'admin', 'admin', '/apps/powermonitor', now(), now())",
    )
    .bind(user_id)
    .bind(username)
    .bind(password_hash)
    .execute(&mut *transaction)
    .await
    .map_err(BootstrapAdminError::Storage)?;
    sqlx::query("INSERT INTO user_app_grants (user_id, app_key) VALUES ($1, $2)")
        .bind(user_id)
        .bind(POWER_MONITOR_APP)
        .execute(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    transaction
        .commit()
        .await
        .map_err(BootstrapAdminError::Storage)
}

fn is_bootstrap_username(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[derive(Clone)]
pub struct ManagementSessionRouter {
    pub router: Router,
    pub session_verifier: Arc<ManagementSessionVerifier>,
}

impl ManagementSessionRouter {
    pub fn new(store: Arc<PlatformStore>, token_vault: TokenVault) -> Self {
        let session_verifier = Arc::new(ManagementSessionVerifier::default());
        let state = ManagementState {
            store,
            session_verifier: Arc::clone(&session_verifier),
            token_vault,
            login_limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
        };
        let router = Router::new()
            .route("/api/auth/login", post(login))
            .route("/api/auth/logout", post(logout))
            .route("/api/auth/me", get(current_session))
            .route("/api/management/applications", post(create_application))
            .route(
                "/api/management/devices",
                get(list_management_devices).post(provision_device),
            )
            .route(
                "/api/management/devices/{device_id}",
                put(update_management_device).delete(delete_management_device),
            )
            .route(
                "/api/management/devices/{device_id}/tokens",
                post(create_device_token),
            )
            .with_state(state);
        Self {
            router,
            session_verifier,
        }
    }
}

#[derive(Default)]
pub struct ManagementSessionVerifier {
    sessions: Mutex<HashMap<String, Session>>,
}

impl ManagementSessionVerifier {
    fn issue(&self, user_id: Uuid, role: Role) -> String {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        loop {
            let session_id = generate_session_id();
            if !sessions.contains_key(&session_id) {
                sessions.insert(
                    session_id.clone(),
                    Session {
                        user_id,
                        role,
                        expires_at: Instant::now() + SESSION_TTL,
                    },
                );
                return session_id;
            }
        }
    }

    fn revoke(&self, headers: &HeaderMap) {
        let Some(session_id) = session_id(headers) else {
            return;
        };
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
    }

    fn is_admin(&self, headers: &HeaderMap) -> bool {
        let Some(session_id) = session_id(headers) else {
            return false;
        };
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        sessions
            .get(session_id)
            .is_some_and(|session| session.role == Role::Admin)
    }
}

impl OAuthBrowserSessionVerifier for ManagementSessionVerifier {
    fn authenticated_user_id(&self, headers: &HeaderMap) -> Option<Uuid> {
        let session_id = session_id(headers)?;
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        sessions.get(session_id).map(|session| session.user_id)
    }
}

#[derive(Clone)]
struct ManagementState {
    store: Arc<PlatformStore>,
    session_verifier: Arc<ManagementSessionVerifier>,
    token_vault: TokenVault,
    login_limiter: Arc<Mutex<LoginRateLimiter>>,
}

struct Session {
    user_id: Uuid,
    role: Role,
    expires_at: Instant,
}

#[derive(Default)]
struct LoginRateLimiter {
    attempts: HashMap<IpAddr, LoginAttempt>,
}

struct LoginAttempt {
    failures: u8,
    in_flight: u8,
    started_at: Instant,
}

impl LoginRateLimiter {
    fn reserve(&mut self, address: IpAddr) -> bool {
        self.attempts
            .retain(|_, attempt| attempt.started_at.elapsed() < LOGIN_WINDOW);
        let attempt = self.attempts.entry(address).or_insert(LoginAttempt {
            failures: 0,
            in_flight: 0,
            started_at: Instant::now(),
        });
        if attempt.failures.saturating_add(attempt.in_flight) >= MAX_LOGIN_FAILURES {
            return false;
        }
        attempt.in_flight = attempt.in_flight.saturating_add(1);
        true
    }

    fn record_failure(&mut self, address: IpAddr) {
        if let Some(attempt) = self.attempts.get_mut(&address) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures = attempt.failures.saturating_add(1);
        }
    }

    fn record_success(&mut self, address: IpAddr) {
        let remove = if let Some(attempt) = self.attempts.get_mut(&address) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures = 0;
            attempt.in_flight == 0
        } else {
            false
        };
        if remove {
            self.attempts.remove(&address);
        }
    }

    fn release(&mut self, address: IpAddr) {
        let remove = if let Some(attempt) = self.attempts.get_mut(&address) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures == 0 && attempt.in_flight == 0
        } else {
            false
        };
        if remove {
            self.attempts.remove(&address);
        }
    }
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    user_id: Uuid,
}

#[derive(Deserialize)]
struct CreateApplicationRequest {
    app_id: String,
    kind: String,
    launch_url: String,
    client_id: String,
    redirect_uris: Vec<String>,
    allowed_scopes: Vec<String>,
    enabled: bool,
    client_secret: Option<String>,
}

#[derive(Serialize)]
struct ApplicationResponse {
    app_id: String,
    client_id: String,
}

#[derive(Deserialize)]
struct ProvisionDeviceRequest {
    display_name: String,
}

#[derive(Deserialize)]
struct UpdateManagementDeviceRequest {
    display_name: String,
    #[serde(default)]
    asset_id: Option<Uuid>,
    #[serde(default)]
    device_profile_id: Option<Uuid>,
    #[serde(default)]
    attributes: Option<Value>,
    #[serde(default)]
    topology: Option<ManagementTopologyRequest>,
}

#[derive(Deserialize)]
struct ManagementTopologyRequest {
    is_gateway: bool,
    #[serde(default)]
    gateway_device_id: Option<String>,
}

#[derive(Serialize)]
struct ManagementDeviceResponse {
    device_id: String,
    display_name: Option<String>,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
    attributes: Value,
    online: bool,
    last_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    is_gateway: bool,
    gateway_device_id: Option<String>,
    gateway_status: Option<String>,
    child_status: Option<String>,
}

async fn login(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<SessionResponse>), ManagementSessionError> {
    let address = address.ip();
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(address)
    };
    if !reserved {
        return Err(ManagementSessionError::TooManyRequests);
    }
    let user = match authenticate(&state.store, &request).await {
        Ok(user) => user,
        Err(AuthError::AuthenticationFailed) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .record_failure(address);
            return Err(ManagementSessionError::Unauthorized);
        }
        Err(_) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .release(address);
            return Err(ManagementSessionError::Unavailable);
        }
    };
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(address);
    let session_id = state.session_verifier.issue(user.user_id, user.role);
    Ok((
        session_cookie_headers(&session_id),
        Json(SessionResponse {
            user_id: user.user_id,
        }),
    ))
}

async fn logout(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> (StatusCode, HeaderMap) {
    state.session_verifier.revoke(&headers);
    (StatusCode::NO_CONTENT, expired_session_cookie_headers())
}

async fn current_session(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<SessionResponse>, ManagementSessionError> {
    let user_id = state
        .session_verifier
        .authenticated_user_id(&headers)
        .ok_or(ManagementSessionError::Unauthorized)?;
    Ok(Json(SessionResponse { user_id }))
}

async fn create_application(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Json(request): Json<CreateApplicationRequest>,
) -> Result<(StatusCode, Json<ApplicationResponse>), ManagementSessionError> {
    if !state.session_verifier.is_admin(&headers) {
        return Err(ManagementSessionError::Forbidden);
    }
    let app_id = request
        .app_id
        .parse()
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let kind =
        ApplicationKind::from_str(&request.kind).map_err(|_| ManagementSessionError::BadRequest)?;
    let client_id =
        ClientId::from_str(&request.client_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let redirect_uris = request
        .redirect_uris
        .into_iter()
        .map(|value| RedirectUri::from_str(&value).map_err(|_| ManagementSessionError::BadRequest))
        .collect::<Result<Vec<_>, _>>()?;
    if request.launch_url.is_empty() || request.allowed_scopes.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    let application = ApplicationRepository::upsert_application(
        state.store.as_ref(),
        NewApplication {
            app_id,
            kind,
            launch_url: request.launch_url,
            client_id,
            redirect_uris,
            allowed_scopes: request.allowed_scopes,
            enabled: request.enabled,
        },
    )
    .await
    .map_err(|_| ManagementSessionError::Unavailable)?;
    if let Some(client_secret) = request.client_secret {
        if client_secret.is_empty() {
            return Err(ManagementSessionError::BadRequest);
        }
        OAuthRepository::register_client_secret(
            state.store.as_ref(),
            NewOAuthClientSecret {
                app_id: application.app_id.clone(),
                client_secret,
            },
        )
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    }
    Ok((
        StatusCode::CREATED,
        Json(ApplicationResponse {
            app_id: application.app_id.as_str().to_owned(),
            client_id: application.client_id.as_str().to_owned(),
        }),
    ))
}

async fn provision_device(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Json(request): Json<ProvisionDeviceRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ManagementSessionError> {
    if !state.session_verifier.is_admin(&headers) {
        return Err(ManagementSessionError::Forbidden);
    }
    let display_name = request.display_name.trim();
    if display_name.is_empty() || display_name.len() > 128 {
        return Err(ManagementSessionError::BadRequest);
    }
    let token = provision_platform_device_token(&state.store, &state.token_vault, display_name)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn list_management_devices(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementDeviceResponse>>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementDeviceRepository::list_management_devices(state.store.as_ref())
        .await
        .map(|devices| {
            Json(
                devices
                    .into_iter()
                    .map(management_device_response)
                    .collect(),
            )
        })
        .map_err(management_device_error)
}

async fn update_management_device(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(request): Json<UpdateManagementDeviceRequest>,
) -> Result<Json<ManagementDeviceResponse>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    let device = ManagementDeviceRepository::update_management_device(
        state.store.as_ref(),
        &device_id,
        UpdateManagementDevice {
            display_name: request.display_name,
            asset_id: request.asset_id,
            device_profile_id: request.device_profile_id,
            attributes: request.attributes,
            topology: request.topology.map(|topology| ManagementDeviceTopology {
                is_gateway: topology.is_gateway,
                gateway_device_id: topology.gateway_device_id,
            }),
        },
    )
    .await
    .map_err(management_device_error)?;
    Ok(Json(management_device_response(device)))
}

async fn delete_management_device(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementDeviceRepository::delete_management_device(state.store.as_ref(), &device_id)
        .await
        .map_err(management_device_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn management_device_response(device: StorageManagementDevice) -> ManagementDeviceResponse {
    ManagementDeviceResponse {
        device_id: device.device_id,
        display_name: device.display_name,
        asset_id: device.asset_id,
        device_profile_id: device.device_profile_id,
        attributes: device.attributes,
        online: device.health.online,
        last_seen_at: device.health.last_seen_at,
        is_gateway: device.topology.is_gateway,
        gateway_device_id: device.topology.gateway_device_id,
        gateway_status: device.health.gateway_status.map(|status| match status {
            ManagementGatewayStatus::Online => "online".to_owned(),
            ManagementGatewayStatus::Offline => "offline".to_owned(),
        }),
        child_status: device.health.child_status.map(|status| match status {
            ManagementChildStatus::Fresh => "fresh".to_owned(),
            ManagementChildStatus::Stale => "stale".to_owned(),
            ManagementChildStatus::Unavailable => "unavailable".to_owned(),
        }),
    }
}

fn management_device_error(error: ManagementDeviceError) -> ManagementSessionError {
    match error {
        ManagementDeviceError::InvalidDeviceId(_)
        | ManagementDeviceError::InvalidDisplayName
        | ManagementDeviceError::AttributesMustBeObject => ManagementSessionError::BadRequest,
        ManagementDeviceError::DeviceNotFound => ManagementSessionError::NotFound,
        ManagementDeviceError::GatewayCannotHaveParent
        | ManagementDeviceError::DeviceCannotBeOwnGateway
        | ManagementDeviceError::GatewayHasChildren
        | ManagementDeviceError::GatewayUnavailable
        | ManagementDeviceError::GatewayIsNotGateway
        | ManagementDeviceError::AssetUnavailable(_)
        | ManagementDeviceError::DeviceProfileUnavailable(_) => ManagementSessionError::Conflict,
        ManagementDeviceError::InvalidStoredAttributes
        | ManagementDeviceError::InvalidStoredTimestamp
        | ManagementDeviceError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

fn require_management_admin(
    session_verifier: &ManagementSessionVerifier,
    headers: &HeaderMap,
) -> Result<(), ManagementSessionError> {
    if session_verifier.authenticated_user_id(headers).is_none() {
        return Err(ManagementSessionError::Unauthorized);
    }
    if session_verifier.is_admin(headers) {
        Ok(())
    } else {
        Err(ManagementSessionError::Forbidden)
    }
}

async fn create_device_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ManagementSessionError> {
    if !state.session_verifier.is_admin(&headers) {
        return Err(ManagementSessionError::Forbidden);
    }
    let token = create_platform_device_token(&state.store, &state.token_vault, &device_id)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn authenticate(
    store: &PlatformStore,
    request: &LoginRequest,
) -> Result<iot_api::AuthenticatedUser, AuthError> {
    if let Some(pool) = store.sqlite_pool() {
        return authenticate_credentials_sqlite(pool, &request.username, &request.password).await;
    }
    let pool = store
        .timescale_pool()
        .ok_or_else(|| AuthError::AuthenticationFailed)?;
    authenticate_credentials(pool, &request.username, &request.password).await
}

fn session_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies
                .split(';')
                .map(str::trim)
                .find_map(|cookie| cookie.strip_prefix("iot_nano_session="))
        })
        .filter(|session_id| !session_id.is_empty())
}

fn prune_expired_sessions(sessions: &mut HashMap<String, Session>) {
    let now = Instant::now();
    sessions.retain(|_, session| session.expires_at > now);
}

fn session_cookie_headers(session_id: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::try_from(format!(
        "{SESSION_COOKIE}={session_id}; HttpOnly; Secure; SameSite=Lax; Path=/"
    ))
    .expect("generated session IDs are valid cookie values");
    headers.insert(SET_COOKIE, value);
    headers
}

fn expired_session_cookie_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        HeaderValue::from_static(
            "iot_nano_session=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0",
        ),
    );
    headers
}

enum ManagementSessionError {
    Unauthorized,
    TooManyRequests,
    Forbidden,
    BadRequest,
    NotFound,
    Conflict,
    Unavailable,
}

impl IntoResponse for ManagementSessionError {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::BadRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict => (StatusCode::CONFLICT, "conflict"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        };
        (status, Json(json!({ "error": code }))).into_response()
    }
}

#[cfg(test)]
mod unit_tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::{LoginRateLimiter, MAX_LOGIN_FAILURES};

    #[test]
    fn login_limiter_reserves_in_flight_attempts_before_authentication() {
        let mut limiter = LoginRateLimiter::default();
        let address = IpAddr::V4(Ipv4Addr::LOCALHOST);

        for _ in 0..MAX_LOGIN_FAILURES {
            assert!(limiter.reserve(address));
        }
        assert!(!limiter.reserve(address));
    }
}
