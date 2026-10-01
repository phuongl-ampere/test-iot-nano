use super::*;

#[derive(Default)]
pub struct ManagementSessionVerifier {
    sessions: Mutex<HashMap<String, Session>>,
}

impl ManagementSessionVerifier {
    pub(super) fn issue_system(&self, system_account_id: Uuid) -> String {
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
                        user_id: None,
                        system_account_id: Some(system_account_id),
                        tenant_account_id: None,
                        tenant_id: None,
                        expires_at: Instant::now() + SESSION_TTL,
                    },
                );
                return session_id;
            }
        }
    }

    pub(super) fn issue_tenant(&self, tenant_account_id: Uuid, tenant_id: Uuid) -> String {
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
                        user_id: None,
                        system_account_id: None,
                        tenant_account_id: Some(tenant_account_id),
                        tenant_id: Some(tenant_id),
                        expires_at: Instant::now() + SESSION_TTL,
                    },
                );
                return session_id;
            }
        }
    }

    pub(super) fn issue_user(&self, user_id: Uuid, tenant_id: Uuid) -> String {
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
                        user_id: Some(user_id),
                        system_account_id: None,
                        tenant_account_id: None,
                        tenant_id: Some(tenant_id),
                        expires_at: Instant::now() + SESSION_TTL,
                    },
                );
                return session_id;
            }
        }
    }

    pub(super) fn revoke(&self, headers: &HeaderMap) {
        let Some(session_id) = session_id(headers) else {
            return;
        };
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
    }

    pub(super) fn system_authorization(&self, headers: &HeaderMap) -> ManagementAuthorization {
        let Some(session_id) = session_id(headers) else {
            return ManagementAuthorization::Unauthenticated;
        };
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        match sessions.get(session_id) {
            Some(Session {
                system_account_id: Some(_),
                ..
            }) => ManagementAuthorization::System,
            Some(_) => ManagementAuthorization::Forbidden,
            None => ManagementAuthorization::Unauthenticated,
        }
    }

    pub(super) fn tenant_session(&self, headers: &HeaderMap) -> Option<TenantSession> {
        let session_id = session_id(headers)?;
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        let session = sessions.get(session_id)?;
        Some(TenantSession {
            tenant_account_id: session.tenant_account_id?,
            tenant_id: session.tenant_id?,
        })
    }

    pub(super) fn user_session(&self, headers: &HeaderMap) -> Option<UserSession> {
        let session_id = session_id(headers)?;
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        let session = sessions.get(session_id)?;
        Some(UserSession {
            user_id: session.user_id?,
            tenant_id: session.tenant_id?,
        })
    }

    pub(super) fn platform_session(
        &self,
        headers: &HeaderMap,
    ) -> Result<PlatformUiSession, ManagementSessionError> {
        let session_id = session_id(headers).ok_or(ManagementSessionError::Unauthorized)?;
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        let session = sessions
            .get(session_id)
            .ok_or(ManagementSessionError::Unauthorized)?;

        if let Some(system_account_id) = session.system_account_id {
            return Ok(PlatformUiSession::System { system_account_id });
        }
        if session.tenant_account_id.is_some()
            && let Some(tenant_id) = session.tenant_id
        {
            return Ok(PlatformUiSession::Tenant { tenant_id });
        }
        if let (Some(user_id), Some(tenant_id)) = (session.user_id, session.tenant_id) {
            return Ok(PlatformUiSession::User { user_id, tenant_id });
        }

        Err(ManagementSessionError::Forbidden)
    }

    pub(super) fn invalidate_tenant(&self, tenant_id: Uuid) {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        sessions.retain(|_, session| session.tenant_id != Some(tenant_id));
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
        sessions.get(session_id).and_then(|session| session.user_id)
    }
}

#[derive(Clone)]
pub(super) struct ManagementState {
    pub(super) store: Arc<PlatformStore>,
    pub(super) session_verifier: Arc<ManagementSessionVerifier>,
    pub(super) token_vault: TokenVault,
    pub(super) login_limiter: Arc<Mutex<LoginRateLimiter>>,
    pub(super) authorization_gate: Arc<ManagementAuthorizationGate>,
    pub(super) infrastructure_status: SystemInfrastructureStatus,
    #[cfg(test)]
    pub(super) authorization_test_hooks: Option<Arc<ManagementAuthorizationTestHooks>>,
}

#[derive(Default)]
pub(super) struct ManagementAuthorizationGate {
    state: Mutex<ManagementAuthorizationGateState>,
}

#[derive(Default)]
pub(super) struct ManagementAuthorizationGateState {
    active_mutations: usize,
}

impl ManagementAuthorizationGate {
    pub(super) async fn acquire_mutation(&self) -> ManagementMutationLease<'_> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_mutations = state.active_mutations.saturating_add(1);
        ManagementMutationLease { gate: self }
    }
}

pub(super) struct ManagementMutationLease<'a> {
    gate: &'a ManagementAuthorizationGate,
}

impl Drop for ManagementMutationLease<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_mutations = state.active_mutations.saturating_sub(1);
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct ManagementAuthorizationTestHooks {
    pub(super) mutation_authorized: Arc<Barrier>,
    pub(super) release_mutation: Arc<Barrier>,
    pub(super) pause_mutation: Arc<AtomicBool>,
}

pub(super) struct Session {
    pub(super) user_id: Option<Uuid>,
    system_account_id: Option<Uuid>,
    pub(super) tenant_account_id: Option<Uuid>,
    pub(super) tenant_id: Option<Uuid>,
    expires_at: Instant,
}

#[derive(Clone, Copy)]
pub(super) struct TenantSession {
    pub(super) tenant_account_id: Uuid,
    pub(super) tenant_id: Uuid,
}

#[derive(Clone, Copy)]
pub(super) struct UserSession {
    pub(super) user_id: Uuid,
    pub(super) tenant_id: Uuid,
}

pub(super) enum PlatformUiSession {
    System { system_account_id: Uuid },
    Tenant { tenant_id: Uuid },
    User { user_id: Uuid, tenant_id: Uuid },
}

pub(super) enum ManagementAuthorization {
    Unauthenticated,
    System,
    Forbidden,
}

#[derive(Default)]
pub(super) struct LoginRateLimiter {
    attempts: HashMap<LoginAttemptKey, LoginAttempt>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct LoginAttemptKey {
    pub(super) address: IpAddr,
    pub(super) username: String,
}

pub(super) struct LoginAttempt {
    failures: u8,
    in_flight: u8,
    started_at: Instant,
}

impl LoginRateLimiter {
    pub(super) fn reserve(&mut self, key: &LoginAttemptKey) -> bool {
        self.attempts
            .retain(|_, attempt| attempt.started_at.elapsed() < LOGIN_WINDOW);
        let attempt = self.attempts.entry(key.clone()).or_insert(LoginAttempt {
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

    pub(super) fn record_failure(&mut self, key: &LoginAttemptKey) {
        if let Some(attempt) = self.attempts.get_mut(key) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures = attempt.failures.saturating_add(1);
        }
    }

    pub(super) fn record_success(&mut self, key: &LoginAttemptKey) {
        let remove = if let Some(attempt) = self.attempts.get_mut(key) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures = 0;
            attempt.in_flight == 0
        } else {
            false
        };
        if remove {
            self.attempts.remove(key);
        }
    }

    pub(super) fn release(&mut self, key: &LoginAttemptKey) {
        let remove = if let Some(attempt) = self.attempts.get_mut(key) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures == 0 && attempt.in_flight == 0
        } else {
            false
        };
        if remove {
            self.attempts.remove(key);
        }
    }
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

pub(super) fn session_cookie_headers(session_id: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::try_from(format!(
        "{SESSION_COOKIE}={session_id}; HttpOnly; Secure; SameSite=Lax; Path=/"
    ))
    .expect("generated session IDs are valid cookie values");
    headers.insert(SET_COOKIE, value);
    headers
}

pub(super) fn expired_session_cookie_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        HeaderValue::from_static(
            "iot_nano_session=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0",
        ),
    );
    headers
}
