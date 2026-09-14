use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{COOKIE, SET_COOKIE},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use iot_api::{
    AuthError, OAuthBrowserSessionVerifier, authenticate_credentials,
    authenticate_credentials_sqlite, generate_session_id,
};
use iot_storage::PlatformStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

const SESSION_COOKIE: &str = "iot_nano_session";
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_LOGIN_FAILURES: u8 = 5;

#[derive(Clone)]
pub struct ManagementSessionRouter {
    pub router: Router,
    pub session_verifier: Arc<ManagementSessionVerifier>,
}

impl ManagementSessionRouter {
    pub fn new(store: Arc<PlatformStore>) -> Self {
        let session_verifier = Arc::new(ManagementSessionVerifier::default());
        let state = ManagementState {
            store,
            session_verifier: Arc::clone(&session_verifier),
            login_limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
        };
        let router = Router::new()
            .route("/api/auth/login", post(login))
            .route("/api/auth/logout", post(logout))
            .route("/api/auth/me", get(current_session))
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
    fn issue(&self, user_id: Uuid) -> String {
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
    login_limiter: Arc<Mutex<LoginRateLimiter>>,
}

struct Session {
    user_id: Uuid,
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
    let session_id = state.session_verifier.issue(user.user_id);
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
    Unavailable,
}

impl IntoResponse for ManagementSessionError {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
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
