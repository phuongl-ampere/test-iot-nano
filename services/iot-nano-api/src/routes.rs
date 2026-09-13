use std::{
    collections::HashMap,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant},
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, Extension, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use iot_core::{
    RpcMode, RpcRequest, SystemConfiguration, SystemConfigurationUpdate, device_token_prefix,
    generate_device_token, hash_device_token, verify_device_token,
};
use iot_storage::{CommandOutboxState, NewCommandOutboxEntry, SqliteStore, SqliteStoreError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row};
use thiserror::Error;
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::{
        Components,
        security::{ApiKey, ApiKeyValue, SecurityScheme},
    },
};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::{
    CoreClient, CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest,
    CoreFacade, CoreFacadeError, CoreTelemetryBucket, CoreTelemetryQuery, TokenVault,
    auth::{
        AccountClass, Admin, AuthContext, AuthError, AuthenticatedUser, POWER_MONITOR_APP, Role,
        System, authenticate_credentials, authenticate_credentials_sqlite, change_password,
        change_password_sqlite, default_user, generate_session_id, hash_password,
    },
    device_tokens::{
        DeviceTokenResponse, DeviceTokenStoreError, create as create_device_token,
        create_sqlite as create_device_token_sqlite, list as list_device_tokens,
        list_sqlite as list_device_tokens_sqlite, provision as provision_device_token,
        provision_owned as provision_owned_device_token,
        provision_owned_sqlite as provision_owned_device_token_sqlite,
        provision_sqlite as provision_device_token_sqlite,
        resolve_active as resolve_active_device_token,
        resolve_active_sqlite as resolve_active_device_token_sqlite, revoke as revoke_device_token,
        revoke_sqlite as revoke_device_token_sqlite, rotate as rotate_device_token,
        rotate_sqlite as rotate_device_token_sqlite,
    },
    powermonitor::{
        PowerAsset, PowerBucket, PowerDevice, PowerSummary, PowerTelemetryPoint,
        PowerTelemetryRecord, asset_telemetry as power_asset_telemetry,
        device_telemetry as power_device_telemetry,
        device_telemetry_records as power_device_telemetry_records,
        list_assets as list_power_assets, list_devices as list_power_devices,
    },
    resource_authorization::{
        ResourceKind, ResourcePermission, asset_permission, device_permission,
        sqlite_asset_permission, sqlite_device_permission,
    },
    system_config::{
        HelperSystemConfigurationService, SystemConfigurationService,
        SystemConfigurationServiceError,
    },
};

const MQTT_TRANSPORT_REVOCATION_TIMEOUT: StdDuration = StdDuration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MqttdDeviceTransportSessionRevocation {
    pub device_id: String,
    pub token_id: Uuid,
}

#[derive(Debug, Error)]
pub enum MqttdDeviceTransportSessionRevokerError {
    #[error("MQTT transport session revocation request failed")]
    Request(#[source] reqwest::Error),
    #[error("MQTT transport session revocation returned HTTP {status}")]
    UnexpectedStatus { status: u16 },
    #[error("MQTT transport session revocation is unavailable")]
    Unavailable,
}

pub trait MqttdDeviceTransportSessionRevoker: Send + Sync {
    fn revoke_session(
        &self,
        revocation: MqttdDeviceTransportSessionRevocation,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), MqttdDeviceTransportSessionRevokerError>> + Send + '_>,
    >;
}

#[derive(Clone)]
struct HttpMqttdDeviceTransportSessionRevoker {
    client: reqwest::Client,
    control_url: Arc<str>,
    secret: Arc<str>,
}

impl HttpMqttdDeviceTransportSessionRevoker {
    fn new(control_url: Arc<str>, secret: Arc<str>) -> Self {
        Self {
            client: reqwest::Client::new(),
            control_url,
            secret,
        }
    }
}

impl MqttdDeviceTransportSessionRevoker for HttpMqttdDeviceTransportSessionRevoker {
    fn revoke_session(
        &self,
        revocation: MqttdDeviceTransportSessionRevocation,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), MqttdDeviceTransportSessionRevokerError>> + Send + '_>,
    > {
        let request = self
            .client
            .post(format!(
                "{}/internal/sessions/revoke",
                self.control_url.trim_end_matches('/')
            ))
            .header("x-iot-nano-api-mqttd-secret", self.secret.as_ref())
            .json(&revocation)
            .send();
        Box::pin(async move {
            let response = tokio::time::timeout(MQTT_TRANSPORT_REVOCATION_TIMEOUT, request)
                .await
                .map_err(|_| MqttdDeviceTransportSessionRevokerError::Unavailable)?
                .map_err(MqttdDeviceTransportSessionRevokerError::Request)?;
            if response.status() == reqwest::StatusCode::NO_CONTENT {
                Ok(())
            } else {
                Err(MqttdDeviceTransportSessionRevokerError::UnexpectedStatus {
                    status: response.status().as_u16(),
                })
            }
        })
    }
}

#[derive(Clone)]
struct NoopMqttdDeviceTransportSessionRevoker;

impl MqttdDeviceTransportSessionRevoker for NoopMqttdDeviceTransportSessionRevoker {
    fn revoke_session(
        &self,
        _revocation: MqttdDeviceTransportSessionRevocation,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), MqttdDeviceTransportSessionRevokerError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
pub struct ApiState {
    pool: PgPool,
    login_limiter: Arc<Mutex<LoginRateLimiter>>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    mqttd_device_transport_secret: Option<Arc<str>>,
    mqttd_device_transport_session_revoker: Arc<dyn MqttdDeviceTransportSessionRevoker>,
    system_configuration: Arc<dyn SystemConfigurationService>,
    token_vault: TokenVault,
    core_facade: Option<Arc<dyn CoreFacade>>,
}

#[derive(Clone)]
pub struct SqliteApiState {
    store: SqliteStore,
    login_limiter: Arc<Mutex<LoginRateLimiter>>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    mqttd_device_transport_secret: Option<Arc<str>>,
    mqttd_device_transport_session_revoker: Arc<dyn MqttdDeviceTransportSessionRevoker>,
    system_configuration: Arc<dyn SystemConfigurationService>,
    token_vault: TokenVault,
    core_facade: Option<Arc<dyn CoreFacade>>,
}

#[derive(Clone)]
struct Session {
    user_id: Uuid,
    role: Role,
    account_class: AccountClass,
    username: String,
    default_app: String,
    granted_apps: Vec<String>,
    expires_at: Instant,
}

impl ApiState {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            login_limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            mqttd_device_transport_secret: None,
            mqttd_device_transport_session_revoker: Arc::new(
                NoopMqttdDeviceTransportSessionRevoker,
            ),
            system_configuration: default_system_configuration_service(),
            token_vault: TokenVault::from_key_material("iot-api-default-device-token-vault"),
            core_facade: None,
        }
    }

    pub fn with_system_configuration(
        mut self,
        system_configuration: Arc<dyn SystemConfigurationService>,
    ) -> Self {
        self.system_configuration = system_configuration;
        self
    }

    pub fn with_mqttd_device_transport_secret(mut self, secret: impl AsRef<str>) -> Self {
        self.mqttd_device_transport_secret = Some(Arc::from(secret.as_ref()));
        self
    }

    pub fn with_api_mqttd_control(
        mut self,
        control_url: impl AsRef<str>,
        secret: impl AsRef<str>,
    ) -> Self {
        self.mqttd_device_transport_session_revoker =
            Arc::new(HttpMqttdDeviceTransportSessionRevoker::new(
                Arc::from(control_url.as_ref()),
                Arc::from(secret.as_ref()),
            ));
        self
    }

    pub fn with_mqttd_device_transport_session_revoker(
        mut self,
        revoker: impl MqttdDeviceTransportSessionRevoker + 'static,
    ) -> Self {
        self.mqttd_device_transport_session_revoker = Arc::new(revoker);
        self
    }

    pub fn with_device_token_vault(mut self, token_vault: TokenVault) -> Self {
        self.token_vault = token_vault;
        self
    }

    pub fn with_core_facade(mut self, facade: Arc<dyn CoreFacade>) -> Self {
        self.core_facade = Some(facade);
        self
    }

    pub fn with_core_client(self, client: CoreClient) -> Self {
        self.with_core_facade(Arc::new(client))
    }

    pub fn with_session(
        self,
        session_id: impl Into<String>,
        username: impl Into<String>,
        role: Role,
    ) -> Self {
        let user = default_user(username, role);
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                session_id.into(),
                Session {
                    user_id: user.user_id,
                    role: user.role,
                    account_class: user.account_class,
                    username: user.username,
                    default_app: user.default_app,
                    granted_apps: user.granted_apps,
                    expires_at: Instant::now() + StdDuration::from_secs(8 * 60 * 60),
                },
            );
        self
    }

    fn require_mqttd_device_transport_secret(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        let configured = self
            .mqttd_device_transport_secret
            .as_deref()
            .ok_or(ApiError::MqttdDeviceTransportAuthenticationUnavailable)?;
        let supplied = headers
            .get("x-iot-nano-mqttd-api-secret")
            .and_then(|value| value.to_str().ok())
            .ok_or(ApiError::Unauthorized)?;
        if !constant_time_equal(configured.as_bytes(), supplied.as_bytes()) {
            return Err(ApiError::Unauthorized);
        }
        Ok(())
    }

    fn issue_session(&self, user: AuthenticatedUser) -> String {
        let session_id = generate_session_id();
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                session_id.clone(),
                Session {
                    user_id: user.user_id,
                    role: user.role,
                    account_class: user.account_class,
                    username: user.username,
                    default_app: user.default_app,
                    granted_apps: user.granted_apps,
                    expires_at: Instant::now() + StdDuration::from_secs(8 * 60 * 60),
                },
            );
        session_id
    }

    fn authenticate_session(&self, session_id: &str) -> Option<AuthContext> {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.retain(|_, session| session.expires_at > Instant::now());
        sessions.get(session_id).map(|session| AuthContext {
            user_id: session.user_id,
            role: session.role,
            account_class: session.account_class,
            username: session.username.clone(),
            default_app: session.default_app.clone(),
            granted_apps: session.granted_apps.clone(),
            session_id: session_id.to_owned(),
        })
    }

    fn revoke_session(&self, session_id: &str) {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
    }

    fn revoke_sessions_for_username(&self, username: &str) {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, session| session.username != username);
    }
}

impl SqliteApiState {
    pub fn new(store: SqliteStore) -> Self {
        Self {
            store,
            login_limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            mqttd_device_transport_secret: None,
            mqttd_device_transport_session_revoker: Arc::new(
                NoopMqttdDeviceTransportSessionRevoker,
            ),
            system_configuration: default_system_configuration_service(),
            token_vault: TokenVault::from_key_material("iot-api-default-device-token-vault"),
            core_facade: None,
        }
    }

    pub fn with_system_configuration(
        mut self,
        system_configuration: Arc<dyn SystemConfigurationService>,
    ) -> Self {
        self.system_configuration = system_configuration;
        self
    }

    pub fn with_mqttd_device_transport_secret(mut self, secret: impl AsRef<str>) -> Self {
        self.mqttd_device_transport_secret = Some(Arc::from(secret.as_ref()));
        self
    }

    pub fn with_api_mqttd_control(
        mut self,
        control_url: impl AsRef<str>,
        secret: impl AsRef<str>,
    ) -> Self {
        self.mqttd_device_transport_session_revoker =
            Arc::new(HttpMqttdDeviceTransportSessionRevoker::new(
                Arc::from(control_url.as_ref()),
                Arc::from(secret.as_ref()),
            ));
        self
    }

    pub fn with_mqttd_device_transport_session_revoker(
        mut self,
        revoker: impl MqttdDeviceTransportSessionRevoker + 'static,
    ) -> Self {
        self.mqttd_device_transport_session_revoker = Arc::new(revoker);
        self
    }

    pub fn with_device_token_vault(mut self, token_vault: TokenVault) -> Self {
        self.token_vault = token_vault;
        self
    }

    pub fn with_core_facade(mut self, facade: Arc<dyn CoreFacade>) -> Self {
        self.core_facade = Some(facade);
        self
    }

    pub fn with_core_client(self, client: CoreClient) -> Self {
        self.with_core_facade(Arc::new(client))
    }

    fn require_mqttd_device_transport_secret(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        let configured = self
            .mqttd_device_transport_secret
            .as_deref()
            .ok_or(ApiError::MqttdDeviceTransportAuthenticationUnavailable)?;
        let supplied = headers
            .get("x-iot-nano-mqttd-api-secret")
            .and_then(|value| value.to_str().ok())
            .ok_or(ApiError::Unauthorized)?;
        if !constant_time_equal(configured.as_bytes(), supplied.as_bytes()) {
            return Err(ApiError::Unauthorized);
        }
        Ok(())
    }

    fn issue_session(&self, user: AuthenticatedUser) -> String {
        let session_id = generate_session_id();
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                session_id.clone(),
                Session {
                    user_id: user.user_id,
                    role: user.role,
                    account_class: user.account_class,
                    username: user.username,
                    default_app: user.default_app,
                    granted_apps: user.granted_apps,
                    expires_at: Instant::now() + StdDuration::from_secs(8 * 60 * 60),
                },
            );
        session_id
    }

    fn authenticate_session(&self, session_id: &str) -> Option<AuthContext> {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.retain(|_, session| session.expires_at > Instant::now());
        sessions.get(session_id).map(|session| AuthContext {
            user_id: session.user_id,
            role: session.role,
            account_class: session.account_class,
            username: session.username.clone(),
            default_app: session.default_app.clone(),
            granted_apps: session.granted_apps.clone(),
            session_id: session_id.to_owned(),
        })
    }

    fn revoke_session(&self, session_id: &str) {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
    }

    fn revoke_sessions_for_username(&self, username: &str) {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, session| session.username != username);
    }
}

fn default_system_configuration_service() -> Arc<dyn SystemConfigurationService> {
    Arc::new(HelperSystemConfigurationService::new(PathBuf::from(
        "/usr/local/sbin/iot-admin-helper",
    )))
}

async fn revoke_mqttd_device_transport_session(
    revoker: &dyn MqttdDeviceTransportSessionRevoker,
    device_id: String,
    token_id: Uuid,
) -> Result<(), ApiError> {
    revoker
        .revoke_session(MqttdDeviceTransportSessionRevocation {
            device_id,
            token_id,
        })
        .await
        .map_err(|_| ApiError::MqttdDeviceTransportSessionRevocationUnavailable)
}

#[derive(Default)]
struct LoginRateLimiter {
    failures: HashMap<IpAddr, LoginAttempt>,
}

struct LoginAttempt {
    count: u8,
    started_at: Instant,
}

impl LoginRateLimiter {
    fn is_limited(&mut self, address: IpAddr) -> bool {
        let now = Instant::now();
        if self.failures.get(&address).is_some_and(|attempt| {
            now.duration_since(attempt.started_at) >= StdDuration::from_secs(60)
        }) {
            self.failures.remove(&address);
            return false;
        }
        self.failures
            .get(&address)
            .is_some_and(|attempt| attempt.count >= 5)
    }

    fn record_failure(&mut self, address: IpAddr) {
        let now = Instant::now();
        let attempt = self.failures.entry(address).or_insert(LoginAttempt {
            count: 0,
            started_at: now,
        });
        if now.duration_since(attempt.started_at) >= StdDuration::from_secs(60) {
            attempt.count = 0;
            attempt.started_at = now;
        }
        attempt.count = attempt.count.saturating_add(1);
    }

    fn clear(&mut self, address: IpAddr) {
        self.failures.remove(&address);
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Rush IoT Nano API",
        version = "0.1.0",
        description = "REST API for Rush IoT Nano device management, telemetry, alerts, and Power Monitor."
    ),
    paths(
        healthz,
        login,
        current_role,
        logout,
        change_password_handler,
        get_system_configuration,
        update_system_configuration,
        list_management_devices,
        provision_management_device,
        update_management_device,
        delete_management_device,
        issue_device_claim_code,
        list_management_assets,
        create_management_asset,
        update_management_asset,
        delete_management_asset,
        list_management_users,
        create_management_user,
        update_management_user,
        list_management_device_profiles,
        create_management_device_profile,
        update_management_device_profile,
        delete_management_device_profile,
        list_management_asset_profiles,
        create_management_asset_profile,
        update_management_asset_profile,
        delete_management_asset_profile,
        create_my_asset,
        provision_my_device,
        assign_my_device_asset,
        claim_device,
        create_asset_share,
        create_device_share,
        list_my_resource_shares,
        accept_resource_share,
        delete_resource_share,
        powermonitor_summary,
        powermonitor_assets,
        powermonitor_asset_telemetry,
        powermonitor_devices,
        powermonitor_device_telemetry,
        powermonitor_device_telemetry_records,
        list_devices,
        list_device_tokens_handler,
        create_device_token_handler,
        provision_device_token_handler,
        rotate_device_token_handler,
        revoke_device_token_handler,
        device_telemetry,
        send_command,
        get_device_command,
        list_alert_rules,
        create_alert_rule,
        update_alert_rule,
        archive_alert_rule,
        toggle_alert_rule,
        list_alert_incidents,
        acknowledge_alert_incident
    ),
    components(schemas(
        LoginRequest,
        AuthResponse,
        Role,
        AccountClass,
        SystemConfiguration,
        SystemConfigurationUpdate,
        ManagementDevice,
        ManagementAsset,
        ManagementDeviceProfile,
        ManagementAssetProfile,
        CreateManagementUserRequest,
        DeviceTokenResponse,
        PowerSummary,
        PowerAsset,
        PowerDevice,
        PowerTelemetryPoint,
        PowerTelemetryRecord,
        CommandRequest,
        CommandLifecycleResponse,
        CreateResourceShareRequest,
        ResourceShareResponse,
        ProvisionMyDeviceRequest,
        AssignMyDeviceAssetRequest,
        IssueDeviceClaimCodeRequest,
        DeviceClaimCodeResponse,
        ClaimDeviceRequest
    )),
    tags(
        (name = "Operations", description = "Health and service operations"),
        (name = "Authentication", description = "Session authentication"),
        (name = "Management", description = "Administrator entity management"),
        (name = "My resources", description = "User-owned assets and devices"),
        (name = "Resource sharing", description = "Internal username resource invitations"),
        (name = "Power Monitor", description = "Power monitoring application queries"),
        (name = "Alerts", description = "Alert rule and incident operations"),
        (name = "Devices", description = "Device telemetry, tokens, and commands")
    ),
    modifiers(&SessionSecurity)
)]
struct ApiDoc;

struct SessionSecurity;

impl Modify for SessionSecurity {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi
            .components
            .get_or_insert_with(Components::new)
            .add_security_scheme(
                "sessionAuth",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                    "Authorization",
                    "Use `Session <opaque session ID>`.",
                ))),
            );
    }
}

#[derive(Clone)]
pub struct ApiRouters {
    pub public: Router,
    pub management: Router,
}

pub fn routers(state: ApiState) -> ApiRouters {
    ApiRouters {
        public: public_router(),
        management: management_router(state),
    }
}

pub fn router(state: ApiState) -> Router {
    let ApiRouters { public, management } = routers(state);
    public.merge(management)
}

fn public_router() -> Router {
    Router::new().route("/healthz", get(healthz))
}

fn management_router(state: ApiState) -> Router {
    let management = Router::new()
        .route("/api/auth/login", post(login))
        .route(
            "/internal/mqttd/session-resolution",
            post(mqttd_device_transport_session_resolution),
        )
        .route(
            "/internal/mqttd/session-authorization",
            post(mqttd_device_transport_session_authorization),
        )
        .route(
            "/internal/mqttd/gateway-authorization",
            post(mqttd_gateway_authorization),
        )
        .route(
            "/internal/mqttd/rpc-response",
            post(mqttd_device_transport_rpc_response),
        );
    let protected = Router::new()
        .route("/api/auth/me", get(current_role))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/password", put(change_password_handler))
        .route(
            "/api/system-configuration",
            get(get_system_configuration).put(update_system_configuration),
        )
        .route(
            "/api/management/settings",
            get(get_system_configuration).put(update_system_configuration),
        )
        .route(
            "/api/management/devices",
            get(list_management_devices).post(provision_management_device),
        )
        .route(
            "/api/management/devices/{device_id}",
            put(update_management_device).delete(delete_management_device),
        )
        .route(
            "/api/management/devices/{device_id}/claim-code",
            post(issue_device_claim_code),
        )
        .route(
            "/api/management/assets",
            get(list_management_assets).post(create_management_asset),
        )
        .route(
            "/api/management/assets/{id}",
            put(update_management_asset).delete(delete_management_asset),
        )
        .route(
            "/api/management/users",
            get(list_management_users).post(create_management_user),
        )
        .route(
            "/api/management/users/{username}",
            put(update_management_user),
        )
        .route(
            "/api/management/profiles/device-profiles",
            get(list_management_device_profiles).post(create_management_device_profile),
        )
        .route(
            "/api/management/profiles/device-profiles/{id}",
            put(update_management_device_profile).delete(delete_management_device_profile),
        )
        .route(
            "/api/management/profiles/asset-profiles",
            get(list_management_asset_profiles).post(create_management_asset_profile),
        )
        .route(
            "/api/management/profiles/asset-profiles/{id}",
            put(update_management_asset_profile).delete(delete_management_asset_profile),
        )
        .route("/api/my/assets", post(create_my_asset))
        .route("/api/my/devices", post(provision_my_device))
        .route(
            "/api/my/devices/{device_id}/asset",
            put(assign_my_device_asset),
        )
        .route("/api/device-claims", post(claim_device))
        .route("/api/assets/{asset_id}/shares", post(create_asset_share))
        .route("/api/devices/{device_id}/shares", post(create_device_share))
        .route("/api/me/resource-shares", get(list_my_resource_shares))
        .route(
            "/api/resource-shares/{id}/accept",
            post(accept_resource_share),
        )
        .route(
            "/api/resource-shares/{id}",
            axum::routing::delete(delete_resource_share),
        )
        .route("/api/devices", get(list_devices))
        .route(
            "/api/devices/{device_id}/tokens",
            get(list_device_tokens_handler).post(create_device_token_handler),
        )
        .route("/api/devices/{device_id}/telemetry", get(device_telemetry))
        .route("/api/devices/{device_id}/commands", post(send_command))
        .route("/api/device-commands/{id}", get(get_device_command))
        .route("/api/device-tokens", post(provision_device_token_handler))
        .route(
            "/api/device-tokens/{id}/rotate",
            post(rotate_device_token_handler),
        )
        .route(
            "/api/device-tokens/{id}/revoke",
            post(revoke_device_token_handler),
        )
        .route(
            "/api/alert-rules",
            get(list_alert_rules).post(create_alert_rule),
        )
        .route(
            "/api/alert-rules/{id}",
            put(update_alert_rule).delete(archive_alert_rule),
        )
        .route("/api/alert-rules/{id}/toggle", post(toggle_alert_rule))
        .route("/api/alert-incidents", get(list_alert_incidents))
        .route(
            "/api/alert-incidents/{id}/acknowledge",
            post(acknowledge_alert_incident),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            authenticate_request,
        ));
    management
        .merge(protected)
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

pub fn sqlite_router(state: SqliteApiState) -> Router {
    let public = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/auth/login", post(sqlite_login))
        .route(
            "/internal/mqttd/session-resolution",
            post(sqlite_mqttd_device_transport_session_resolution),
        )
        .route(
            "/internal/mqttd/session-authorization",
            post(sqlite_mqttd_device_transport_session_authorization),
        )
        .route(
            "/internal/mqttd/gateway-authorization",
            post(sqlite_mqttd_gateway_authorization),
        )
        .route(
            "/internal/mqttd/rpc-response",
            post(sqlite_mqttd_device_transport_rpc_response),
        );
    let protected = Router::new()
        .route("/api/auth/me", get(current_role))
        .route("/api/auth/logout", post(sqlite_logout))
        .route("/api/auth/password", put(sqlite_change_password_handler))
        .route(
            "/api/system-configuration",
            get(sqlite_get_system_configuration).put(sqlite_update_system_configuration),
        )
        .route(
            "/api/management/settings",
            get(sqlite_get_system_configuration).put(sqlite_update_system_configuration),
        )
        .route(
            "/api/management/devices",
            get(sqlite_list_management_devices).post(sqlite_provision_management_device),
        )
        .route(
            "/api/management/devices/{device_id}",
            put(sqlite_update_management_device).delete(sqlite_delete_management_device),
        )
        .route(
            "/api/management/devices/{device_id}/claim-code",
            post(sqlite_issue_device_claim_code),
        )
        .route(
            "/api/management/assets",
            get(sqlite_list_management_assets).post(sqlite_create_management_asset),
        )
        .route(
            "/api/management/assets/{id}",
            put(sqlite_update_management_asset).delete(sqlite_delete_management_asset),
        )
        .route(
            "/api/management/users",
            get(sqlite_list_management_users).post(sqlite_create_management_user),
        )
        .route(
            "/api/management/users/{username}",
            put(sqlite_update_management_user),
        )
        .route(
            "/api/management/profiles/device-profiles",
            get(sqlite_list_management_device_profiles)
                .post(sqlite_create_management_device_profile),
        )
        .route(
            "/api/management/profiles/device-profiles/{id}",
            put(sqlite_update_management_device_profile)
                .delete(sqlite_delete_management_device_profile),
        )
        .route(
            "/api/management/profiles/asset-profiles",
            get(sqlite_list_management_asset_profiles).post(sqlite_create_management_asset_profile),
        )
        .route(
            "/api/management/profiles/asset-profiles/{id}",
            put(sqlite_update_management_asset_profile)
                .delete(sqlite_delete_management_asset_profile),
        )
        .route("/api/my/assets", post(sqlite_create_my_asset))
        .route("/api/my/devices", post(sqlite_provision_my_device))
        .route(
            "/api/my/devices/{device_id}/asset",
            put(sqlite_assign_my_device_asset),
        )
        .route("/api/device-claims", post(sqlite_claim_device))
        .route(
            "/api/assets/{asset_id}/shares",
            post(sqlite_create_asset_share),
        )
        .route(
            "/api/devices/{device_id}/shares",
            post(sqlite_create_device_share),
        )
        .route(
            "/api/me/resource-shares",
            get(sqlite_list_my_resource_shares),
        )
        .route(
            "/api/resource-shares/{id}/accept",
            post(sqlite_accept_resource_share),
        )
        .route(
            "/api/resource-shares/{id}",
            axum::routing::delete(sqlite_delete_resource_share),
        )
        .route("/api/devices", get(sqlite_list_devices))
        .route(
            "/api/devices/{device_id}/tokens",
            get(sqlite_list_device_tokens_handler).post(sqlite_create_device_token_handler),
        )
        .route(
            "/api/devices/{device_id}/telemetry",
            get(sqlite_device_telemetry),
        )
        .route(
            "/api/devices/{device_id}/commands",
            post(sqlite_send_command),
        )
        .route("/api/device-commands/{id}", get(sqlite_get_device_command))
        .route(
            "/api/device-tokens",
            post(sqlite_provision_device_token_handler),
        )
        .route(
            "/api/device-tokens/{id}/rotate",
            post(sqlite_rotate_device_token_handler),
        )
        .route(
            "/api/device-tokens/{id}/revoke",
            post(sqlite_revoke_device_token_handler),
        )
        .route(
            "/api/alert-rules",
            get(sqlite_list_alert_rules).post(sqlite_create_alert_rule),
        )
        .route(
            "/api/alert-rules/{id}",
            put(sqlite_update_alert_rule).delete(sqlite_archive_alert_rule),
        )
        .route(
            "/api/alert-rules/{id}/toggle",
            post(sqlite_toggle_alert_rule),
        )
        .route("/api/alert-incidents", get(sqlite_list_alert_incidents))
        .route(
            "/api/alert-incidents/{id}/acknowledge",
            post(sqlite_acknowledge_alert_incident),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            authenticate_sqlite_request,
        ));
    public
        .merge(protected)
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

#[utoipa::path(
    get,
    path = "/healthz",
    responses((status = 200, description = "Service is healthy")),
    tag = "Operations"
)]
async fn healthz() -> &'static str {
    "ok\n"
}

#[utoipa::path(
    post,
    path = "/api/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Authenticated session", body = AuthResponse),
        (status = 401, description = "Invalid credentials"),
        (status = 429, description = "Too many failed attempts")
    ),
    tag = "Authentication"
)]
async fn login(
    State(state): State<ApiState>,
    address: Option<Extension<ConnectInfo<SocketAddr>>>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let address = address
        .map(|Extension(ConnectInfo(address))| address.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if limiter.is_limited(address) {
            return Err(ApiError::TooManyRequests);
        }
    }

    match authenticate_credentials(&state.pool, &request.username, &request.password).await {
        Ok(user) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear(address);
            let session_id = state.issue_session(user.clone());
            Ok(Json(AuthResponse::from_user(user, Some(session_id))))
        }
        Err(AuthError::AuthenticationFailed) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .record_failure(address);
            Err(ApiError::Unauthorized)
        }
        Err(error) => Err(ApiError::Auth(error)),
    }
}

async fn sqlite_login(
    State(state): State<SqliteApiState>,
    address: Option<Extension<ConnectInfo<SocketAddr>>>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let address = address
        .map(|Extension(ConnectInfo(address))| address.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if limiter.is_limited(address) {
            return Err(ApiError::TooManyRequests);
        }
    }
    match authenticate_credentials_sqlite(state.store.pool(), &request.username, &request.password)
        .await
    {
        Ok(user) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear(address);
            let session_id = state.issue_session(user.clone());
            Ok(Json(AuthResponse::from_user(user, Some(session_id))))
        }
        Err(AuthError::AuthenticationFailed) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .record_failure(address);
            Err(ApiError::Unauthorized)
        }
        Err(error) => Err(ApiError::Auth(error)),
    }
}

async fn sqlite_logout(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> StatusCode {
    state.revoke_session(&context.session_id);
    StatusCode::NO_CONTENT
}

async fn sqlite_change_password_handler(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<ChangePasswordRequest>,
) -> Result<StatusCode, ApiError> {
    change_password_sqlite(
        state.store.pool(),
        &context.username,
        &request.current_password,
        &request.new_password,
    )
    .await
    .map_err(|error| match error {
        AuthError::InvalidPasswordFormat => ApiError::BadRequest("invalid password".to_owned()),
        AuthError::AuthenticationFailed => ApiError::Unauthorized,
        error => ApiError::Auth(error),
    })?;
    state.revoke_sessions_for_username(&context.username);
    Ok(StatusCode::NO_CONTENT)
}

async fn sqlite_get_system_configuration(
    State(state): State<SqliteApiState>,
    _system: System,
) -> Result<Json<SystemConfiguration>, ApiError> {
    state
        .system_configuration
        .read()
        .await
        .map(Json)
        .map_err(system_configuration_error)
}

async fn sqlite_update_system_configuration(
    State(state): State<SqliteApiState>,
    _system: System,
    Json(update): Json<SystemConfigurationUpdate>,
) -> Result<Json<SystemConfiguration>, ApiError> {
    state
        .system_configuration
        .apply(update)
        .await
        .map(Json)
        .map_err(system_configuration_error)
}

async fn sqlite_create_device_share(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Json(request): Json<CreateResourceShareRequest>,
) -> Result<(StatusCode, Json<ResourceShareResponse>), ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    sqlite_create_resource_share(&state, &context, ResourceKind::Device, device_id, request).await
}

async fn sqlite_create_asset_share(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(asset_id): Path<Uuid>,
    Json(request): Json<CreateResourceShareRequest>,
) -> Result<(StatusCode, Json<ResourceShareResponse>), ApiError> {
    sqlite_create_resource_share(
        &state,
        &context,
        ResourceKind::Asset,
        asset_id.to_string(),
        request,
    )
    .await
}

async fn sqlite_create_resource_share(
    state: &SqliteApiState,
    context: &AuthContext,
    kind: ResourceKind,
    resource_id: String,
    request: CreateResourceShareRequest,
) -> Result<(StatusCode, Json<ResourceShareResponse>), ApiError> {
    let permission = ResourcePermission::parse_share(&request.permission)
        .ok_or_else(|| ApiError::BadRequest("invalid resource permission".to_owned()))?;
    let owner = sqlite_resource_owner(state.store.pool(), kind, &resource_id).await?;
    let Some(owner) = owner else {
        return Err(ApiError::NotFound(match kind {
            ResourceKind::Asset => "asset",
            ResourceKind::Device => "device",
        }));
    };
    let access =
        sqlite_resource_permission(state.store.pool(), context, kind, &resource_id).await?;
    if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
        return Err(ApiError::Forbidden);
    }
    let target_user_id = sqlite_share_target_user_id(state.store.pool(), &request.username).await?;
    if owner == target_user_id {
        return Err(ApiError::Conflict(
            "resource owners cannot be invited".to_owned(),
        ));
    }
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT 1
         FROM resource_shares
         WHERE resource_type = ?
           AND resource_id = ?
           AND target_user_id = ?
           AND state = 'pending'",
    )
    .bind(kind.as_str())
    .bind(&resource_id)
    .bind(&target_user_id)
    .fetch_optional(state.store.pool())
    .await?
    .is_some();
    if existing {
        return Err(ApiError::Conflict(
            "a pending resource share already exists".to_owned(),
        ));
    }

    let id = Uuid::now_v7();
    let row = sqlx::query(
        "INSERT INTO resource_shares (
            id, resource_type, resource_id, target_user_id, permission,
            inherit_children, state, created_by_user_id
         ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
         RETURNING id, resource_type, resource_id, permission, inherit_children, state",
    )
    .bind(id.to_string())
    .bind(kind.as_str())
    .bind(resource_id)
    .bind(target_user_id)
    .bind(permission.as_str())
    .bind(i64::from(request.inherit_children))
    .bind(context.user_id.to_string())
    .fetch_one(state.store.pool())
    .await
    .map_err(resource_share_database_error)?;
    let response = sqlite_resource_share_from_row(row)?;
    sqlite_write_audit_event(
        state.store.pool(),
        &context,
        &response.resource_type,
        &response.resource_id,
        "resource_share.created",
        None,
        Some(&json!({
            "resource_type": response.resource_type,
            "resource_id": response.resource_id,
            "permission": response.permission,
            "inherit_children": response.inherit_children,
            "state": response.state,
        })),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn sqlite_list_my_resource_shares(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Query(query): Query<ResourceShareQuery>,
) -> Result<Json<Vec<ResourceShareResponse>>, ApiError> {
    let Some(state_filter) = query.state.as_deref() else {
        let rows = sqlx::query(
            "SELECT id, resource_type, resource_id, permission, inherit_children, state
             FROM resource_shares
             WHERE target_user_id = ?
             ORDER BY created_at DESC, id",
        )
        .bind(context.user_id.to_string())
        .fetch_all(state.store.pool())
        .await?;
        return rows
            .into_iter()
            .map(sqlite_resource_share_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map(Json);
    };
    if !is_resource_share_state(state_filter) {
        return Err(ApiError::BadRequest(
            "invalid resource share state".to_owned(),
        ));
    }
    let rows = sqlx::query(
        "SELECT id, resource_type, resource_id, permission, inherit_children, state
         FROM resource_shares
         WHERE target_user_id = ? AND state = ?
         ORDER BY created_at DESC, id",
    )
    .bind(context.user_id.to_string())
    .bind(state_filter)
    .fetch_all(state.store.pool())
    .await?;
    rows.into_iter()
        .map(sqlite_resource_share_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn sqlite_accept_resource_share(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let accepted = sqlx::query(
        "UPDATE resource_shares
         SET state = 'active', responded_at = ?
         WHERE id = ?
           AND target_user_id = ?
           AND state = 'pending'
         RETURNING resource_type, resource_id",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .bind(context.user_id.to_string())
    .fetch_optional(state.store.pool())
    .await?;
    if let Some(accepted) = accepted {
        sqlite_write_audit_event(
            state.store.pool(),
            &context,
            &accepted.try_get::<String, _>("resource_type")?,
            &accepted.try_get::<String, _>("resource_id")?,
            "resource_share.accepted",
            Some(&json!({ "state": "pending" })),
            Some(&json!({ "state": "active" })),
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Forbidden)
    }
}

async fn sqlite_delete_resource_share(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let share = sqlx::query(
        "SELECT resource_type, resource_id, target_user_id, state
         FROM resource_shares
         WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("resource share"))?;
    let target_user_id: String = share.try_get("target_user_id")?;
    let share_state: String = share.try_get("state")?;
    if target_user_id == context.user_id.to_string() {
        if share_state != "pending" {
            return Err(ApiError::Forbidden);
        }
        let updated = sqlx::query(
            "UPDATE resource_shares
             SET state = 'declined', responded_at = ?
             WHERE id = ? AND target_user_id = ? AND state = 'pending'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .bind(context.user_id.to_string())
        .execute(state.store.pool())
        .await?
        .rows_affected();
        return if updated == 1 {
            sqlite_write_audit_event(
                state.store.pool(),
                &context,
                &share.try_get::<String, _>("resource_type")?,
                &share.try_get::<String, _>("resource_id")?,
                "resource_share.declined",
                Some(&json!({ "state": "pending" })),
                Some(&json!({ "state": "declined" })),
            )
            .await?;
            Ok(StatusCode::NO_CONTENT)
        } else {
            Err(ApiError::Conflict(
                "resource share state changed concurrently".to_owned(),
            ))
        };
    }
    let kind = resource_kind(&share.try_get::<String, _>("resource_type")?)?;
    let resource_id: String = share.try_get("resource_id")?;
    let access =
        sqlite_resource_permission(state.store.pool(), &context, kind, &resource_id).await?;
    if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
        return Err(ApiError::Forbidden);
    }
    let cancelled = sqlx::query(
        "UPDATE resource_shares
         SET state = 'cancelled', responded_at = ?
         WHERE id = ? AND state IN ('pending', 'active')",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .execute(state.store.pool())
    .await?
    .rows_affected();
    if cancelled == 1 {
        sqlite_write_audit_event(
            state.store.pool(),
            &context,
            kind.as_str(),
            &resource_id,
            "resource_share.cancelled",
            Some(&json!({ "state": share_state })),
            Some(&json!({ "state": "cancelled" })),
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Conflict(
            "resource share cannot be cancelled".to_owned(),
        ))
    }
}

async fn sqlite_create_my_asset(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<CreateManagementAssetRequest>,
) -> Result<(StatusCode, Json<ManagementAsset>), ApiError> {
    require_user_account(&context)?;
    let CreateManagementAssetRequest {
        name,
        asset_profile_id,
        parent_asset_id,
        metadata,
        attributes,
    } = request;
    if let Some(parent_asset_id) = parent_asset_id {
        let access = sqlite_asset_permission(state.store.pool(), &context, parent_asset_id).await?;
        if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
            return Err(ApiError::Forbidden);
        }
    }
    let name = validated_name(&name, "asset name")?.to_owned();
    let metadata = object_value(attributes.unwrap_or(metadata), "asset attributes")?;
    let id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();
    let row = sqlx::query(
        "INSERT INTO assets (
            id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
    )
    .bind(id.to_string())
    .bind(name)
    .bind(asset_profile_id.map(|id| id.to_string()))
    .bind(parent_asset_id.map(|id| id.to_string()))
    .bind(context.user_id.to_string())
    .bind(metadata.to_string())
    .bind(&now)
    .bind(now)
    .fetch_one(state.store.pool())
    .await?;
    let asset = sqlite_management_asset_from_row(row)?;
    sqlite_write_audit_event(
        state.store.pool(),
        &context,
        "asset",
        &asset.id.to_string(),
        "asset.created",
        None,
        Some(&json!({
            "owner_user_id": context.user_id,
            "parent_asset_id": asset.parent_asset_id,
        })),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(asset)))
}

async fn sqlite_provision_my_device(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<ProvisionMyDeviceRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    require_user_account(&context)?;
    let display_name = validated_name(&request.display_name, "device name")?;
    if let Some(asset_id) = request.asset_id {
        let access = sqlite_asset_permission(state.store.pool(), &context, asset_id).await?;
        if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
            return Err(ApiError::Forbidden);
        }
    }
    let token = provision_owned_device_token_sqlite(
        state.store.pool(),
        &state.token_vault,
        display_name,
        context.user_id,
        request.asset_id,
    )
    .await
    .map_err(device_token_error)?;
    sqlite_write_audit_event(
        state.store.pool(),
        &context,
        "device",
        &token.device_id,
        "device.provisioned",
        None,
        Some(&json!({
            "owner_user_id": context.user_id,
            "asset_id": request.asset_id,
            "token_id": token.id,
        })),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn sqlite_assign_my_device_asset(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Json(request): Json<AssignMyDeviceAssetRequest>,
) -> Result<StatusCode, ApiError> {
    require_user_account(&context)?;
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    if let Some(asset_id) = request.asset_id {
        if !sqlite_asset_permission(state.store.pool(), &context, asset_id)
            .await?
            .is_some_and(|access| access.allows(ResourcePermission::Manager))
        {
            return Err(ApiError::Forbidden);
        }
    }
    let updated = sqlx::query(
        "UPDATE devices
         SET asset_id = ?, configuration_version = configuration_version + 1
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(request.asset_id.map(|id| id.to_string()))
    .bind(&device_id)
    .execute(state.store.pool())
    .await?
    .rows_affected();
    if updated == 1 {
        sqlite_write_audit_event(
            state.store.pool(),
            &context,
            "device",
            &device_id,
            "device.asset_assigned",
            None,
            Some(&json!({ "asset_id": request.asset_id })),
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound("device"))
    }
}

async fn sqlite_list_management_devices(
    State(state): State<SqliteApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementDevice>>, ApiError> {
    let rows = sqlx::query(
        "SELECT device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
         FROM devices
         WHERE deleted_at IS NULL
         ORDER BY device_id",
    )
    .fetch_all(state.store.pool())
    .await?;
    let online_after = (Utc::now() - Duration::seconds(30)).to_rfc3339();
    rows.into_iter()
        .map(|row| sqlite_management_device_from_row(row, &online_after))
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn sqlite_provision_management_device(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Json(request): Json<ProvisionDeviceTokenRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    let display_name = validated_name(&request.display_name, "device name")?;
    provision_device_token_sqlite(state.store.pool(), &state.token_vault, display_name)
        .await
        .map(|token| (StatusCode::CREATED, Json(token)))
        .map_err(device_token_error)
}

async fn sqlite_issue_device_claim_code(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    request: Option<Json<IssueDeviceClaimCodeRequest>>,
) -> Result<(StatusCode, Json<DeviceClaimCodeResponse>), ApiError> {
    if context.account_class != AccountClass::Admin {
        return Err(ApiError::Forbidden);
    }
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let expires_at =
        claim_code_expiry(request.and_then(|Json(request)| request.expires_in_seconds))?;
    let claim_code = generate_device_token();
    let code_hash = hash_device_token(&claim_code).map_err(|_| ApiError::StorageData)?;
    let mut transaction = state.store.pool().begin().await?;
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if owner.is_some() {
        return Err(ApiError::Conflict("device is already claimed".to_owned()));
    }
    sqlx::query(
        "INSERT INTO device_claim_codes (
            device_id, code_hash, expires_at, issued_by_user_id, issued_at, used_at
         ) VALUES (?, ?, ?, ?, ?, NULL)
         ON CONFLICT(device_id) DO UPDATE SET
            code_hash = excluded.code_hash,
            expires_at = excluded.expires_at,
            issued_by_user_id = excluded.issued_by_user_id,
            issued_at = excluded.issued_at,
            used_at = NULL",
    )
    .bind(&device_id)
    .bind(code_hash)
    .bind(expires_at.to_rfc3339())
    .bind(context.user_id.to_string())
    .bind(Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    sqlite_write_audit_event(
        state.store.pool(),
        &context,
        "device",
        &device_id,
        "device.claim_code_issued",
        None,
        Some(&json!({ "expires_at": expires_at })),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(DeviceClaimCodeResponse {
            device_id,
            claim_code,
            expires_at,
        }),
    ))
}

async fn sqlite_claim_device(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<ClaimDeviceRequest>,
) -> Result<StatusCode, ApiError> {
    require_user_account(&context)?;
    if !is_identifier(&request.device_id) || request.claim_code.is_empty() {
        return Err(ApiError::BadRequest("invalid device claim".to_owned()));
    }
    let mut transaction = state.store.pool().begin().await?;
    let row = sqlx::query(
        "SELECT devices.owner_user_id, claims.code_hash, claims.expires_at, claims.used_at
         FROM devices
         LEFT JOIN device_claim_codes AS claims ON claims.device_id = devices.device_id
         WHERE devices.device_id = ? AND devices.deleted_at IS NULL",
    )
    .bind(&request.device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if row.try_get::<Option<String>, _>("owner_user_id")?.is_some() {
        return Err(ApiError::Conflict("device is already claimed".to_owned()));
    }
    let code_hash = row
        .try_get::<Option<String>, _>("code_hash")?
        .ok_or(ApiError::Forbidden)?;
    let expires_at = row
        .try_get::<Option<String>, _>("expires_at")?
        .map(|value| sqlite_timestamp(&value))
        .transpose()?
        .ok_or(ApiError::Forbidden)?;
    let used_at: Option<String> = row.try_get("used_at")?;
    if used_at.is_some()
        || expires_at <= Utc::now()
        || !verify_device_token(&request.claim_code, &code_hash).unwrap_or(false)
    {
        return Err(ApiError::Forbidden);
    }
    let claimed = sqlx::query(
        "UPDATE devices
         SET owner_user_id = ?, claimed_at = ?
         WHERE device_id = ? AND owner_user_id IS NULL",
    )
    .bind(context.user_id.to_string())
    .bind(Utc::now().to_rfc3339())
    .bind(&request.device_id)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if claimed != 1 {
        return Err(ApiError::Conflict("device is already claimed".to_owned()));
    }
    sqlx::query(
        "UPDATE device_claim_codes
         SET used_at = ?
         WHERE device_id = ? AND used_at IS NULL",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(&request.device_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    sqlite_write_audit_event(
        state.store.pool(),
        &context,
        "device",
        &request.device_id,
        "device.claimed",
        Some(&json!({ "owner_user_id": null })),
        Some(&json!({ "owner_user_id": context.user_id })),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn sqlite_update_management_device(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(device_id): Path<String>,
    Json(request): Json<UpdateManagementDeviceRequest>,
) -> Result<Json<ManagementDevice>, ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let display_name = validated_name(&request.display_name, "device name")?.to_owned();
    let attributes = request
        .attributes
        .map(|value| object_value(value, "device attributes"))
        .transpose()?
        .map(|value| value.to_string());
    let mut transaction = state.store.pool().begin().await?;
    let current = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    let current_is_gateway = current.try_get::<i64, _>("is_gateway")? != 0;
    let current_gateway_device_id = current.try_get::<Option<String>, _>("gateway_device_id")?;
    let (is_gateway, gateway_device_id) = request
        .topology
        .map(|topology| (topology.is_gateway, topology.gateway_device_id))
        .unwrap_or((current_is_gateway, current_gateway_device_id));
    if is_gateway && gateway_device_id.is_some() {
        return Err(ApiError::BadRequest(
            "a gateway cannot be assigned to another gateway".to_owned(),
        ));
    }
    if gateway_device_id.as_deref() == Some(device_id.as_str()) {
        return Err(ApiError::BadRequest(
            "a device cannot be its own gateway".to_owned(),
        ));
    }
    if current_is_gateway && !is_gateway {
        let has_children = sqlx::query(
            "SELECT 1 FROM devices
             WHERE gateway_device_id = ? AND deleted_at IS NULL
             LIMIT 1",
        )
        .bind(&device_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if has_children {
            return Err(ApiError::Conflict(
                "a gateway with assigned children cannot be demoted".to_owned(),
            ));
        }
    }
    if let Some(parent_device_id) = gateway_device_id.as_deref() {
        let parent_is_gateway = sqlx::query(
            "SELECT is_gateway FROM devices
             WHERE device_id = ? AND deleted_at IS NULL",
        )
        .bind(parent_device_id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| row.try_get::<i64, _>("is_gateway"))
        .transpose()?
        .ok_or_else(|| ApiError::Conflict("gateway device is unavailable".to_owned()))?;
        if parent_is_gateway == 0 {
            return Err(ApiError::Conflict(
                "assigned gateway device is not a gateway".to_owned(),
            ));
        }
    }
    let row = sqlx::query(
        "UPDATE devices
         SET display_name = ?, asset_id = ?, device_profile_id = ?,
             metadata = COALESCE(?, metadata), is_gateway = ?, gateway_device_id = ?
         WHERE device_id = ? AND deleted_at IS NULL
         RETURNING device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                   is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality",
    )
    .bind(display_name)
    .bind(request.asset_id.map(|id| id.to_string()))
    .bind(request.device_profile_id.map(|id| id.to_string()))
    .bind(attributes)
    .bind(i64::from(is_gateway))
    .bind(&gateway_device_id)
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if gateway_device_id.is_some() {
        sqlx::query(
            "UPDATE device_tokens
             SET revoked_at = ?
             WHERE device_id = ? AND revoked_at IS NULL",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(&device_id)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    sqlite_management_device_from_row(row, &(Utc::now() - Duration::minutes(5)).to_rfc3339())
        .map(Json)
}

async fn sqlite_delete_management_device(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(device_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let mut transaction = state.store.pool().begin().await?;
    let has_children = sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = ? AND deleted_at IS NULL
         LIMIT 1",
    )
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .is_some();
    if has_children {
        return Err(ApiError::Conflict(
            "a gateway with assigned children cannot be deleted".to_owned(),
        ));
    }
    let now = Utc::now().to_rfc3339();
    let deleted = sqlx::query(
        "UPDATE devices SET deleted_at = ?
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(&now)
    .bind(&device_id)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if deleted == 0 {
        return Err(ApiError::NotFound("device"));
    }
    sqlx::query(
        "UPDATE device_tokens SET revoked_at = ?
         WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(now)
    .bind(device_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn sqlite_list_management_assets(
    State(state): State<SqliteApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementAsset>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, asset_profile_id, parent_asset_id, metadata
         FROM assets
         ORDER BY name, id",
    )
    .fetch_all(state.store.pool())
    .await?;
    rows.into_iter()
        .map(sqlite_management_asset_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn sqlite_create_management_asset(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementAssetRequest>,
) -> Result<(StatusCode, Json<ManagementAsset>), ApiError> {
    let CreateManagementAssetRequest {
        name,
        asset_profile_id,
        parent_asset_id,
        metadata,
        attributes,
    } = request;
    let name = validated_name(&name, "asset name")?.to_owned();
    let metadata = object_value(attributes.unwrap_or(metadata), "asset attributes")?;
    let id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();
    let row = sqlx::query(
        "INSERT INTO assets (
            id, name, asset_profile_id, parent_asset_id, metadata, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)
         RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
    )
    .bind(id.to_string())
    .bind(name)
    .bind(asset_profile_id.map(|id| id.to_string()))
    .bind(parent_asset_id.map(|id| id.to_string()))
    .bind(metadata.to_string())
    .bind(&now)
    .bind(now)
    .fetch_one(state.store.pool())
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(sqlite_management_asset_from_row(row)?),
    ))
}

async fn sqlite_update_management_asset(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateManagementAssetRequest>,
) -> Result<Json<ManagementAsset>, ApiError> {
    let CreateManagementAssetRequest {
        name,
        asset_profile_id,
        parent_asset_id,
        metadata,
        attributes,
    } = request;
    let name = validated_name(&name, "asset name")?.to_owned();
    let metadata = object_value(attributes.unwrap_or(metadata), "asset attributes")?;
    let id = id.to_string();
    let parent_asset_id = parent_asset_id.map(|id| id.to_string());
    let row = sqlx::query(
        "WITH RECURSIVE descendants(id) AS (
            SELECT id FROM assets WHERE parent_asset_id = ?
            UNION
            SELECT children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.id
         )
         UPDATE assets
         SET name = ?, asset_profile_id = ?, parent_asset_id = ?, metadata = ?, updated_at = ?
         WHERE id = ?
           AND (
               ? IS NULL
               OR (? <> ? AND NOT EXISTS (SELECT 1 FROM descendants WHERE id = ?))
           )
         RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
    )
    .bind(&id)
    .bind(name)
    .bind(asset_profile_id.map(|id| id.to_string()))
    .bind(&parent_asset_id)
    .bind(metadata.to_string())
    .bind(Utc::now().to_rfc3339())
    .bind(&id)
    .bind(&parent_asset_id)
    .bind(&parent_asset_id)
    .bind(&id)
    .bind(&parent_asset_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("asset"))?;
    sqlite_management_asset_from_row(row).map(Json)
}

async fn sqlite_delete_management_asset(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sqlx::query("DELETE FROM assets WHERE id = ?")
        .bind(id.to_string())
        .execute(state.store.pool())
        .await?
        .rows_affected();
    if deleted == 0 {
        Err(ApiError::NotFound("asset"))
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

async fn sqlite_list_management_users(
    State(state): State<SqliteApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementUser>>, ApiError> {
    sqlite_list_management_users_query(state.store.pool())
        .await
        .map(Json)
}

async fn sqlite_create_management_user(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementUserRequest>,
) -> Result<(StatusCode, Json<ManagementUser>), ApiError> {
    let Some(default_app_key) = app_key_from_path(&request.default_app) else {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    };
    if !is_identifier(&request.username)
        || request.granted_apps.is_empty()
        || request.granted_apps.iter().any(|app| !is_identifier(app))
        || !request
            .granted_apps
            .iter()
            .any(|app| app == default_app_key)
    {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    }
    let password_hash = hash_password(&request.password).map_err(ApiError::Auth)?;
    let mut transaction = state.store.pool().begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app, updated_at
         ) VALUES (?, ?, ?, 'viewer', 'user', ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(&request.username)
    .bind(password_hash)
    .bind(&request.default_app)
    .bind(Utc::now().to_rfc3339())
    .execute(&mut *transaction)
    .await
    .map_err(resource_share_database_error)?;
    if inserted.rows_affected() != 1 {
        return Err(ApiError::Conflict("username already exists".to_owned()));
    }
    let user_id: String = sqlx::query_scalar("SELECT id FROM users WHERE username = ?")
        .bind(&request.username)
        .fetch_one(&mut *transaction)
        .await?;
    for app_key in &request.granted_apps {
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, app_key)
             VALUES (?, ?)",
        )
        .bind(&user_id)
        .bind(app_key)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    let user = sqlite_list_management_users_query(state.store.pool())
        .await?
        .into_iter()
        .find(|user| user.username == request.username)
        .ok_or(ApiError::StorageData)?;
    Ok((StatusCode::CREATED, Json(user)))
}

async fn sqlite_update_management_user(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(username): Path<String>,
    Json(request): Json<UpdateManagementUserRequest>,
) -> Result<Json<ManagementUser>, ApiError> {
    let Some(default_app_key) = app_key_from_path(&request.default_app) else {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    };
    if !is_identifier(&username)
        || request.granted_apps.is_empty()
        || request.granted_apps.iter().any(|app| !is_identifier(app))
        || !request
            .granted_apps
            .iter()
            .any(|app| app == default_app_key)
    {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    }
    if request
        .role
        .as_deref()
        .is_some_and(|role| !matches!(role, "admin" | "viewer"))
    {
        return Err(ApiError::BadRequest("invalid user role".to_owned()));
    }
    let mut transaction = state.store.pool().begin().await?;
    if request.role.as_deref() == Some("viewer") {
        let remaining_admins: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE role = 'admin' AND username <> ?")
                .bind(&username)
                .fetch_one(&mut *transaction)
                .await?;
        if remaining_admins == 0 {
            return Err(ApiError::Conflict(
                "at least one administrator must remain".to_owned(),
            ));
        }
    }
    let user_id = sqlx::query(
        "UPDATE users
         SET default_app = ?, role = COALESCE(?, role), updated_at = ?
         WHERE username = ?
         RETURNING id",
    )
    .bind(&request.default_app)
    .bind(&request.role)
    .bind(Utc::now().to_rfc3339())
    .bind(&username)
    .fetch_optional(&mut *transaction)
    .await?
    .map(|row| row.try_get::<String, _>("id"))
    .transpose()?
    .ok_or(ApiError::NotFound("user"))?;
    sqlx::query("DELETE FROM user_app_grants WHERE user_id = ?")
        .bind(&user_id)
        .execute(&mut *transaction)
        .await?;
    for app_key in &request.granted_apps {
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, app_key)
             VALUES (?, ?)
             ON CONFLICT(user_id, app_key) DO NOTHING",
        )
        .bind(&user_id)
        .bind(app_key)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    state.revoke_sessions_for_username(&username);
    sqlite_list_management_users_query(state.store.pool())
        .await?
        .into_iter()
        .find(|user| user.username == username)
        .map(Json)
        .ok_or(ApiError::NotFound("user"))
}

async fn sqlite_list_management_device_profiles(
    State(state): State<SqliteApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementDeviceProfile>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, telemetry_schema, metric_mapping, reporting_settings
         FROM device_profiles
         ORDER BY name, id",
    )
    .fetch_all(state.store.pool())
    .await?;
    rows.into_iter()
        .map(sqlite_management_device_profile_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn sqlite_create_management_device_profile(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementDeviceProfileRequest>,
) -> Result<(StatusCode, Json<ManagementDeviceProfile>), ApiError> {
    let name = validated_name(&request.name, "device profile name")?.to_owned();
    let telemetry_schema = object_value(request.telemetry_schema, "telemetry schema")?;
    let metric_mapping = object_value(request.metric_mapping, "metric mapping")?;
    let reporting_settings = object_value(request.reporting_settings, "reporting settings")?;
    let id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();
    let row = sqlx::query(
        "INSERT INTO device_profiles (
            id, name, telemetry_schema, metric_mapping, reporting_settings, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)
         RETURNING id, name, telemetry_schema, metric_mapping, reporting_settings",
    )
    .bind(id.to_string())
    .bind(name)
    .bind(telemetry_schema.to_string())
    .bind(metric_mapping.to_string())
    .bind(reporting_settings.to_string())
    .bind(&now)
    .bind(now)
    .fetch_one(state.store.pool())
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(sqlite_management_device_profile_from_row(row)?),
    ))
}

async fn sqlite_update_management_device_profile(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateManagementDeviceProfileRequest>,
) -> Result<Json<ManagementDeviceProfile>, ApiError> {
    let name = validated_name(&request.name, "device profile name")?.to_owned();
    let telemetry_schema = object_value(request.telemetry_schema, "telemetry schema")?;
    let metric_mapping = object_value(request.metric_mapping, "metric mapping")?;
    let reporting_settings = object_value(request.reporting_settings, "reporting settings")?;
    let row = sqlx::query(
        "UPDATE device_profiles
         SET name = ?, telemetry_schema = ?, metric_mapping = ?,
             reporting_settings = ?, updated_at = ?
         WHERE id = ?
         RETURNING id, name, telemetry_schema, metric_mapping, reporting_settings",
    )
    .bind(name)
    .bind(telemetry_schema.to_string())
    .bind(metric_mapping.to_string())
    .bind(reporting_settings.to_string())
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("device profile"))?;
    sqlite_management_device_profile_from_row(row).map(Json)
}

async fn sqlite_delete_management_device_profile(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sqlx::query("DELETE FROM device_profiles WHERE id = ?")
        .bind(id.to_string())
        .execute(state.store.pool())
        .await?
        .rows_affected();
    if deleted == 0 {
        Err(ApiError::NotFound("device profile"))
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

async fn sqlite_list_management_asset_profiles(
    State(state): State<SqliteApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementAssetProfile>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, fields, dashboard_defaults
         FROM asset_profiles
         ORDER BY name, id",
    )
    .fetch_all(state.store.pool())
    .await?;
    rows.into_iter()
        .map(sqlite_management_asset_profile_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

async fn sqlite_create_management_asset_profile(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementAssetProfileRequest>,
) -> Result<(StatusCode, Json<ManagementAssetProfile>), ApiError> {
    let name = validated_name(&request.name, "asset profile name")?.to_owned();
    let fields = object_value(request.fields, "asset profile fields")?;
    let dashboard_defaults = object_value(
        request.dashboard_defaults,
        "asset profile dashboard defaults",
    )?;
    let id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();
    let row = sqlx::query(
        "INSERT INTO asset_profiles (
            id, name, fields, dashboard_defaults, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?)
         RETURNING id, name, fields, dashboard_defaults",
    )
    .bind(id.to_string())
    .bind(name)
    .bind(fields.to_string())
    .bind(dashboard_defaults.to_string())
    .bind(&now)
    .bind(now)
    .fetch_one(state.store.pool())
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(sqlite_management_asset_profile_from_row(row)?),
    ))
}

async fn sqlite_update_management_asset_profile(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateManagementAssetProfileRequest>,
) -> Result<Json<ManagementAssetProfile>, ApiError> {
    let name = validated_name(&request.name, "asset profile name")?.to_owned();
    let fields = object_value(request.fields, "asset profile fields")?;
    let dashboard_defaults = object_value(
        request.dashboard_defaults,
        "asset profile dashboard defaults",
    )?;
    let row = sqlx::query(
        "UPDATE asset_profiles
         SET name = ?, fields = ?, dashboard_defaults = ?, updated_at = ?
         WHERE id = ?
         RETURNING id, name, fields, dashboard_defaults",
    )
    .bind(name)
    .bind(fields.to_string())
    .bind(dashboard_defaults.to_string())
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("asset profile"))?;
    sqlite_management_asset_profile_from_row(row).map(Json)
}

async fn sqlite_delete_management_asset_profile(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sqlx::query("DELETE FROM asset_profiles WHERE id = ?")
        .bind(id.to_string())
        .execute(state.store.pool())
        .await?
        .rows_affected();
    if deleted == 0 {
        Err(ApiError::NotFound("asset profile"))
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

async fn sqlite_powermonitor_assets(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<PowerAsset>>, ApiError> {
    require_powermonitor(&context)?;
    let rows = sqlx::query(
        "WITH RECURSIVE descendants(root_id, asset_id) AS (
            SELECT id, id FROM assets
            UNION
            SELECT descendants.root_id, children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.asset_id
         ),
         latest AS (
            SELECT telemetry.device_id, telemetry.measurements
            FROM telemetry
            JOIN (
                SELECT device_id, MAX(event_at) AS event_at
                FROM telemetry
                GROUP BY device_id
            ) newest
              ON newest.device_id = telemetry.device_id
             AND newest.event_at = telemetry.event_at
         )
         SELECT assets.id, assets.name, assets.asset_profile_id, assets.parent_asset_id,
                assets.metadata, COUNT(devices.device_id) AS device_count,
                COALESCE(SUM(
                    CASE WHEN json_type(latest.measurements, '$.power_w') IN ('integer', 'real')
                    THEN json_extract(latest.measurements, '$.power_w') END
                ), 0.0) AS total_power_w,
                COALESCE(SUM(
                    CASE WHEN json_type(latest.measurements, '$.energy_kwh') IN ('integer', 'real')
                    THEN json_extract(latest.measurements, '$.energy_kwh') END
                ), 0.0) AS total_energy_kwh
         FROM assets
         LEFT JOIN descendants ON descendants.root_id = assets.id
         LEFT JOIN devices
           ON devices.asset_id = descendants.asset_id AND devices.deleted_at IS NULL
         LEFT JOIN latest ON latest.device_id = devices.device_id
         GROUP BY assets.id
         ORDER BY assets.name, assets.id",
    )
    .fetch_all(state.store.pool())
    .await?;
    let assets = rows
        .into_iter()
        .map(|row| {
            Ok(PowerAsset {
                id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
                name: row.try_get("name")?,
                permission: "viewer".to_owned(),
                asset_profile_id: row
                    .try_get::<Option<String>, _>("asset_profile_id")?
                    .map(|value| sqlite_uuid(&value))
                    .transpose()?,
                parent_asset_id: row
                    .try_get::<Option<String>, _>("parent_asset_id")?
                    .map(|value| sqlite_uuid(&value))
                    .transpose()?,
                metadata: serde_json::from_str(&row.try_get::<String, _>("metadata")?)
                    .map_err(ApiError::Serialization)?,
                device_count: row.try_get("device_count")?,
                total_power_w: row.try_get("total_power_w")?,
                total_energy_kwh: row.try_get("total_energy_kwh")?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let mut visible = Vec::new();
    for mut asset in assets {
        if let Some(permission) =
            sqlite_asset_permission(state.store.pool(), &context, asset.id).await?
            && permission.allows(ResourcePermission::Viewer)
        {
            asset.permission = permission.as_str().to_owned();
            visible.push(asset);
        }
    }
    Ok(Json(visible))
}

async fn sqlite_powermonitor_devices(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    require_powermonitor(&context)?;
    let online_after = (Utc::now() - Duration::seconds(30)).to_rfc3339();
    let rows = sqlx::query(
        "SELECT
            devices.device_id,
            devices.display_name,
            devices.asset_id,
            devices.device_profile_id,
            profiles.name AS device_profile_name,
            devices.last_seen_at,
            devices.is_gateway,
            devices.gateway_device_id,
            devices.gateway_last_read_at,
            devices.gateway_read_quality,
            (
                SELECT measurements
                FROM telemetry
                WHERE telemetry.device_id = devices.device_id
                ORDER BY event_at DESC
                LIMIT 1
            ) AS measurements
         FROM devices
         LEFT JOIN device_profiles AS profiles ON profiles.id = devices.device_profile_id
         WHERE devices.deleted_at IS NULL
         ORDER BY devices.device_id",
    )
    .fetch_all(state.store.pool())
    .await?;
    let mut devices = Vec::new();
    for row in rows {
        let device_id: String = row.try_get("device_id")?;
        let Some(permission) =
            sqlite_device_permission(state.store.pool(), &context, &device_id).await?
        else {
            continue;
        };
        let measurements = row
            .try_get::<Option<String>, _>("measurements")?
            .map(|value| serde_json::from_str::<serde_json::Value>(&value))
            .transpose()
            .map_err(ApiError::Serialization)?
            .unwrap_or_else(|| json!({}));
        let is_gateway = row.try_get::<i64, _>("is_gateway")? != 0;
        let gateway_device_id = row.try_get::<Option<String>, _>("gateway_device_id")?;
        let last_seen_at = row.try_get::<Option<String>, _>("last_seen_at")?;
        let gateway_last_read_at = row.try_get::<Option<String>, _>("gateway_last_read_at")?;
        let gateway_read_quality = row.try_get::<Option<String>, _>("gateway_read_quality")?;
        let (online, gateway_status, child_status, display_last_seen_at) = sqlite_device_health(
            is_gateway,
            gateway_device_id.as_deref(),
            last_seen_at.as_deref(),
            gateway_last_read_at.as_deref(),
            gateway_read_quality.as_deref(),
            &online_after,
        );
        devices.push(json!({
            "device_id": device_id,
            "display_name": row.try_get::<Option<String>, _>("display_name")?,
            "asset_id": row.try_get::<Option<String>, _>("asset_id")?,
            "device_profile_id": row.try_get::<Option<String>, _>("device_profile_id")?,
            "device_profile_name": row.try_get::<Option<String>, _>("device_profile_name")?,
            "permission": permission.as_str(),
            "online": online,
            "last_seen_at": display_last_seen_at,
            "is_gateway": is_gateway,
            "gateway_device_id": gateway_device_id,
            "gateway_status": gateway_status,
            "child_status": child_status,
            "voltage_v": sqlite_measurement_number(&measurements, "voltage_v"),
            "current_a": sqlite_measurement_number(&measurements, "current_a"),
            "power_w": sqlite_measurement_number(&measurements, "power_w"),
            "energy_kwh": sqlite_measurement_number(&measurements, "energy_kwh"),
            "frequency_hz": sqlite_measurement_number(&measurements, "frequency_hz"),
            "power_factor": sqlite_measurement_number(&measurements, "power_factor"),
            "switch_state": measurements.get("switch_state").and_then(serde_json::Value::as_bool),
            "brightness_pct": sqlite_measurement_number(&measurements, "brightness_pct"),
        }));
    }
    Ok(Json(devices))
}

async fn sqlite_powermonitor_summary(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<PowerSummary>, ApiError> {
    require_powermonitor(&context)?;
    let online_after = (Utc::now() - Duration::minutes(5)).to_rfc3339();
    let rows = sqlx::query(
        "SELECT
            devices.device_id,
            devices.last_seen_at,
            devices.is_gateway,
            devices.gateway_device_id,
            devices.gateway_last_read_at,
            devices.gateway_read_quality,
            (
                SELECT measurements
                FROM telemetry
                WHERE telemetry.device_id = devices.device_id
                ORDER BY event_at DESC, sequence DESC
                LIMIT 1
            ) AS measurements
         FROM devices
         WHERE devices.deleted_at IS NULL",
    )
    .fetch_all(state.store.pool())
    .await?;
    let mut device_count = 0_i64;
    let mut online_device_count = 0_i64;
    let mut total_power_w = 0.0_f64;
    let mut total_energy_kwh = 0.0_f64;
    for row in rows {
        let device_id: String = row.try_get("device_id")?;
        if sqlite_device_permission(state.store.pool(), &context, &device_id)
            .await?
            .is_none()
        {
            continue;
        }
        device_count += 1;
        let is_gateway = row.try_get::<i64, _>("is_gateway")? != 0;
        let gateway_device_id = row.try_get::<Option<String>, _>("gateway_device_id")?;
        let last_seen_at = row.try_get::<Option<String>, _>("last_seen_at")?;
        let gateway_last_read_at = row.try_get::<Option<String>, _>("gateway_last_read_at")?;
        let gateway_read_quality = row.try_get::<Option<String>, _>("gateway_read_quality")?;
        if sqlite_device_health(
            is_gateway,
            gateway_device_id.as_deref(),
            last_seen_at.as_deref(),
            gateway_last_read_at.as_deref(),
            gateway_read_quality.as_deref(),
            &online_after,
        )
        .0
        {
            online_device_count += 1;
        }
        let measurements = row
            .try_get::<Option<String>, _>("measurements")?
            .map(|value| serde_json::from_str::<serde_json::Value>(&value))
            .transpose()
            .map_err(ApiError::Serialization)?
            .unwrap_or_else(|| json!({}));
        total_power_w += sqlite_measurement_number(&measurements, "power_w").unwrap_or(0.0);
        total_energy_kwh += sqlite_measurement_number(&measurements, "energy_kwh").unwrap_or(0.0);
    }
    let asset_rows = sqlx::query("SELECT id FROM assets")
        .fetch_all(state.store.pool())
        .await?;
    let mut asset_count = 0_i64;
    for row in asset_rows {
        let asset_id = sqlite_uuid(&row.try_get::<String, _>("id")?)?;
        if sqlite_asset_permission(state.store.pool(), &context, asset_id)
            .await?
            .is_some()
        {
            asset_count += 1;
        }
    }
    Ok(Json(PowerSummary {
        device_count,
        online_device_count,
        asset_count,
        total_power_w,
        total_energy_kwh,
    }))
}

async fn sqlite_powermonitor_device_telemetry(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Query(query): Query<PowerTelemetryQuery>,
) -> Result<Json<Vec<PowerTelemetryPoint>>, ApiError> {
    require_powermonitor(&context)?;
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    let points = match query.validate_sqlite()? {
        PowerBucket::Raw => {
            sqlite_power_raw_points(state.store.pool(), &device_id, query.from, query.to).await?
        }
        PowerBucket::FiveMinutes => {
            sqlite_power_rollup_points(
                state.store.pool(),
                SqliteTelemetryRollup::FiveMinutes,
                &device_id,
                query.from,
                query.to,
            )
            .await?
        }
        PowerBucket::OneHour => {
            sqlite_power_rollup_points(
                state.store.pool(),
                SqliteTelemetryRollup::OneHour,
                &device_id,
                query.from,
                query.to,
            )
            .await?
        }
    };
    Ok(Json(points))
}

async fn sqlite_powermonitor_device_telemetry_records(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Query(query): Query<PowerTelemetryQuery>,
) -> Result<Json<Vec<PowerTelemetryRecord>>, ApiError> {
    require_powermonitor(&context)?;
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    query.validate_sqlite()?;
    let rows = sqlx::query(
        "SELECT event_at AS at, measurements
         FROM telemetry
         WHERE device_id = ? AND event_at >= ? AND event_at <= ?
         ORDER BY event_at DESC, sequence DESC
         LIMIT ?",
    )
    .bind(&device_id)
    .bind(query.from.to_rfc3339())
    .bind(query.to.to_rfc3339())
    .bind(SQLITE_MAX_RAW_POWER_TELEMETRY_ROWS)
    .fetch_all(state.store.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(PowerTelemetryRecord {
                at: sqlite_timestamp(&row.try_get::<String, _>("at")?)?,
                measurements: serde_json::from_str(&row.try_get::<String, _>("measurements")?)
                    .map_err(ApiError::Serialization)?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()
        .map(Json)
}

async fn sqlite_powermonitor_asset_telemetry(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(asset_id): Path<Uuid>,
    Query(query): Query<PowerTelemetryQuery>,
) -> Result<Json<Vec<PowerTelemetryPoint>>, ApiError> {
    require_powermonitor(&context)?;
    if !sqlite_asset_permission(state.store.pool(), &context, asset_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    let points = match query.validate_sqlite()? {
        PowerBucket::Raw => {
            sqlite_power_asset_raw_points(state.store.pool(), asset_id, query.from, query.to)
                .await?
        }
        PowerBucket::FiveMinutes => {
            sqlite_power_asset_rollup_points(
                state.store.pool(),
                SqliteTelemetryRollup::FiveMinutes,
                asset_id,
                query.from,
                query.to,
            )
            .await?
        }
        PowerBucket::OneHour => {
            sqlite_power_asset_rollup_points(
                state.store.pool(),
                SqliteTelemetryRollup::OneHour,
                asset_id,
                query.from,
                query.to,
            )
            .await?
        }
    };
    Ok(Json(points))
}

fn sqlite_measurement_number(measurements: &serde_json::Value, key: &str) -> Option<f64> {
    measurements.get(key).and_then(serde_json::Value::as_f64)
}

fn sqlite_device_health(
    is_gateway: bool,
    gateway_device_id: Option<&str>,
    last_seen_at: Option<&str>,
    gateway_last_read_at: Option<&str>,
    gateway_read_quality: Option<&str>,
    online_after: &str,
) -> (
    bool,
    Option<&'static str>,
    Option<&'static str>,
    Option<String>,
) {
    if is_gateway {
        let online = last_seen_at.is_some_and(|seen| seen >= online_after);
        return (
            online,
            Some(if online { "online" } else { "offline" }),
            None,
            last_seen_at.map(str::to_owned),
        );
    }
    if gateway_device_id.is_some() {
        let fresh = gateway_last_read_at.is_some_and(|read_at| read_at >= online_after);
        let unavailable = gateway_read_quality == Some("unavailable");
        let status = if unavailable {
            "unavailable"
        } else if fresh {
            "fresh"
        } else {
            "stale"
        };
        return (
            status == "fresh",
            None,
            Some(status),
            gateway_last_read_at.map(str::to_owned),
        );
    }
    let online = last_seen_at.is_some_and(|seen| seen >= online_after);
    (online, None, None, last_seen_at.map(str::to_owned))
}

async fn sqlite_list_device_tokens_handler(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
) -> Result<Json<Vec<DeviceTokenResponse>>, ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    list_device_tokens_sqlite(state.store.pool(), &state.token_vault, &device_id)
        .await
        .map(Json)
        .map_err(device_token_error)
}

async fn sqlite_create_device_token_handler(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    let prior_token_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM device_tokens
         WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(&device_id)
    .fetch_optional(state.store.pool())
    .await?;
    let token = create_device_token_sqlite(state.store.pool(), &state.token_vault, &device_id)
        .await
        .map_err(device_token_error)?;
    if let Some(prior_token_id) = prior_token_id {
        revoke_mqttd_device_transport_session(
            state.mqttd_device_transport_session_revoker.as_ref(),
            device_id,
            sqlite_uuid(&prior_token_id)?,
        )
        .await?;
    }
    Ok((StatusCode::CREATED, Json(token)))
}

async fn sqlite_provision_device_token_handler(
    State(state): State<SqliteApiState>,
    _admin: Admin,
    Json(request): Json<ProvisionDeviceTokenRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    let display_name = validated_name(&request.display_name, "device name")?;
    provision_device_token_sqlite(state.store.pool(), &state.token_vault, display_name)
        .await
        .map(|token| (StatusCode::CREATED, Json(token)))
        .map_err(device_token_error)
}

async fn sqlite_rotate_device_token_handler(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    let device_id = sqlx::query_scalar::<_, String>(
        "SELECT device_id FROM device_tokens
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("device token"))?;
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    let token = rotate_device_token_sqlite(state.store.pool(), &state.token_vault, id)
        .await
        .map_err(device_token_error)?;
    revoke_mqttd_device_transport_session(
        state.mqttd_device_transport_session_revoker.as_ref(),
        device_id,
        id,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn sqlite_revoke_device_token_handler(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let device_id = sqlx::query_scalar::<_, String>(
        "SELECT device_id FROM device_tokens
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("device token"))?;
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    revoke_device_token_sqlite(state.store.pool(), id)
        .await
        .map_err(device_token_error)?;
    revoke_mqttd_device_transport_session(
        state.mqttd_device_transport_session_revoker.as_ref(),
        device_id,
        id,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn sqlite_mqttd_device_transport_session_resolution(
    State(state): State<SqliteApiState>,
    headers: HeaderMap,
    Json(request): Json<MqttdDeviceTransportSessionResolutionRequest>,
) -> Result<Json<MqttdDeviceTransportSessionResolution>, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    request.validate_client_id()?;
    if request
        .password
        .as_deref()
        .is_some_and(|password| !password.is_empty())
    {
        return Err(ApiError::Unauthorized);
    }
    let device = resolve_active_device_token_sqlite(state.store.pool(), &request.username)
        .await
        .map_err(mqttd_device_token_error)?;
    if device.gateway_device_id.is_some() {
        return Err(ApiError::Unauthorized);
    }
    let token_prefix =
        device_token_prefix(&request.username).map_err(|_| ApiError::Unauthorized)?;
    let token_id = sqlx::query_scalar::<_, String>(
        "SELECT device_tokens.id
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.token_prefix = ?
           AND device_tokens.device_id = ?
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL",
    )
    .bind(token_prefix)
    .bind(&device.device_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::Unauthorized)?;

    Ok(Json(MqttdDeviceTransportSessionResolution {
        device_id: device.device_id,
        token_id: sqlite_uuid(&token_id)?,
        is_gateway: device.is_gateway,
    }))
}

async fn sqlite_mqttd_device_transport_session_authorization(
    State(state): State<SqliteApiState>,
    headers: HeaderMap,
    Json(request): Json<MqttdDeviceTransportSessionAuthorizationRequest>,
) -> Result<StatusCode, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    let authorized = sqlx::query_scalar::<_, i64>(
        "SELECT 1
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.id = ?
           AND device_tokens.device_id = ?
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL
           AND devices.gateway_device_id IS NULL",
    )
    .bind(request.token_id.to_string())
    .bind(&request.device_id)
    .fetch_optional(state.store.pool())
    .await?
    .is_some();
    if !authorized {
        return Err(ApiError::Unauthorized);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn sqlite_mqttd_gateway_authorization(
    State(state): State<SqliteApiState>,
    headers: HeaderMap,
    Json(request): Json<GatewayAuthorizationRequest>,
) -> Result<Json<GatewayAuthorizationResponse>, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    validate_gateway_authorization_request(&request)?;
    let authorized = sqlx::query_scalar::<_, i64>(
        "SELECT 1
         FROM device_tokens
         JOIN devices AS gateway ON gateway.device_id = device_tokens.device_id
         WHERE device_tokens.id = ?
           AND gateway.device_id = ?
           AND gateway.is_gateway = 1
           AND device_tokens.revoked_at IS NULL
           AND gateway.deleted_at IS NULL
           AND (
             ? IS NULL OR EXISTS (
               SELECT 1
               FROM devices AS child
               WHERE child.device_id = ?
                 AND child.gateway_device_id = gateway.device_id
                 AND child.deleted_at IS NULL
             )
           )",
    )
    .bind(request.token_id.to_string())
    .bind(&request.gateway_device_id)
    .bind(&request.child_device_id)
    .bind(&request.child_device_id)
    .fetch_optional(state.store.pool())
    .await?
    .is_some();
    if !authorized {
        return Err(ApiError::Unauthorized);
    }
    Ok(Json(GatewayAuthorizationResponse::from(request)))
}

async fn sqlite_mqttd_device_transport_rpc_response(
    State(state): State<SqliteApiState>,
    headers: HeaderMap,
    Json(request): Json<MqttdDeviceTransportRpcResponseRequest>,
) -> Result<StatusCode, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    let now = Utc::now();
    if let Some(facade) = &state.core_facade {
        let authorized = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM device_tokens
             WHERE id = ? AND device_id = ? AND revoked_at IS NULL",
        )
        .bind(request.token_id.to_string())
        .bind(&request.device_id)
        .fetch_optional(state.store.pool())
        .await?
        .is_some();
        if !authorized {
            return Err(ApiError::Unauthorized);
        }
        facade
            .record_command_response(CoreCommandResponseRequest {
                command_id: request.command_id,
                device_id: request.device_id,
                response: request.response,
                responded_at: now,
            })
            .await
            .map_err(core_facade_command_error)?;
        return Ok(StatusCode::NO_CONTENT);
    }
    let response = serde_json::to_string(&request.response).map_err(ApiError::Serialization)?;
    if state
        .store
        .mark_command_responded(
            &request.command_id.to_string(),
            &request.device_id,
            &request.token_id.to_string(),
            &response,
            now,
        )
        .await
        .map_err(sqlite_store_error)?
        .is_some()
    {
        return Ok(StatusCode::NO_CONTENT);
    }

    let expired = sqlx::query(
        "UPDATE command_outbox AS command
         SET state = 'expired',
             lease_until = NULL
         WHERE command.id = ?
           AND command.device_id = ?
           AND command.mode = 'two_way'
           AND command.state = 'published_to_broker'
           AND command.expires_at <= ?
           AND EXISTS (
                SELECT 1
                FROM device_tokens
                WHERE id = ?
                  AND device_id = command.device_id
                  AND revoked_at IS NULL
           )",
    )
    .bind(request.command_id.to_string())
    .bind(&request.device_id)
    .bind(now.to_rfc3339())
    .bind(request.token_id.to_string())
    .execute(state.store.pool())
    .await?
    .rows_affected();
    if expired == 1 {
        return Ok(StatusCode::NO_CONTENT);
    }

    let idempotent = sqlx::query_scalar::<_, i64>(
        "SELECT 1
         FROM command_outbox AS command
         WHERE command.id = ?
           AND command.device_id = ?
           AND command.mode = 'two_way'
           AND command.state = 'responded'
           AND EXISTS (
                SELECT 1
                FROM device_tokens
                WHERE id = ?
                  AND device_id = command.device_id
                  AND revoked_at IS NULL
           )",
    )
    .bind(request.command_id.to_string())
    .bind(&request.device_id)
    .bind(request.token_id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .is_some();
    if idempotent {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Conflict(
            "RPC response does not match an active two-way command".to_owned(),
        ))
    }
}

async fn sqlite_list_devices(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<DeviceSummary>>, ApiError> {
    let rows = sqlx::query(
        "SELECT device_id, display_name, last_seen_at
         FROM devices
         WHERE deleted_at IS NULL
         ORDER BY device_id",
    )
    .fetch_all(state.store.pool())
    .await?;
    let online_threshold = Utc::now() - Duration::minutes(5);
    let mut devices = Vec::new();
    for row in rows {
        let device_id: String = row.try_get("device_id")?;
        if sqlite_device_permission(state.store.pool(), &context, &device_id)
            .await?
            .is_none()
        {
            continue;
        }
        let last_seen_at = row
            .try_get::<Option<String>, _>("last_seen_at")?
            .map(|value| sqlite_timestamp(&value))
            .transpose()?;
        devices.push(DeviceSummary {
            device_id,
            display_name: row.try_get("display_name")?,
            online: last_seen_at.is_some_and(|value| value >= online_threshold),
            last_seen_at,
        });
    }

    Ok(Json(devices))
}

async fn sqlite_device_telemetry(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Query(query): Query<TelemetryQuery>,
) -> Result<Json<Vec<TelemetryPoint>>, ApiError> {
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    if query.from >= query.to {
        return Err(ApiError::BadRequest(
            "`from` must be earlier than `to`".to_owned(),
        ));
    }

    let bucket = query.bucket.unwrap_or(TelemetryBucket::FiveMinutes);
    let points = if let Some(facade) = &state.core_facade {
        facade
            .telemetry(CoreTelemetryQuery {
                device_id: device_id.clone(),
                from: query.from,
                to: query.to,
                bucket: match bucket {
                    TelemetryBucket::Raw => CoreTelemetryBucket::Raw,
                    TelemetryBucket::FiveMinutes => CoreTelemetryBucket::FiveMinutes,
                    TelemetryBucket::OneHour => CoreTelemetryBucket::OneHour,
                },
            })
            .await
            .map_err(core_facade_telemetry_error)?
            .into_iter()
            .map(|point| TelemetryPoint {
                at: point.at,
                temperature_c: point.temperature_c,
                humidity_pct: point.humidity_pct,
                event_count: point.event_count,
            })
            .collect()
    } else {
        match bucket {
            TelemetryBucket::Raw => {
                sqlite_raw_points(state.store.pool(), &device_id, query.from, query.to).await?
            }
            TelemetryBucket::FiveMinutes => {
                sqlite_rollup_points(
                    state.store.pool(),
                    SqliteTelemetryRollup::FiveMinutes,
                    &device_id,
                    query.from,
                    query.to,
                )
                .await?
            }
            TelemetryBucket::OneHour => {
                sqlite_rollup_points(
                    state.store.pool(),
                    SqliteTelemetryRollup::OneHour,
                    &device_id,
                    query.from,
                    query.to,
                )
                .await?
            }
        }
    };
    Ok(Json(points))
}

async fn sqlite_send_command(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Json(request): Json<CommandRequest>,
) -> Result<(StatusCode, Json<CommandLifecycleResponse>), ApiError> {
    let access = sqlite_device_permission(state.store.pool(), &context, &device_id).await?;
    if !access.is_some_and(|access| access.allows(ResourcePermission::Controller)) {
        return Err(ApiError::Forbidden);
    }
    let target = resolve_command_target_sqlite(&state, &device_id).await?;
    let command = new_rpc_request(request)?;
    let command_id = command.id;
    let expires_at = command.expires_at;
    let issued_at = command.issued_at;
    let (device_id, method, params, mode) = target.into_command_parts(command);
    if let Some(facade) = &state.core_facade {
        let record = facade
            .create_command(CoreCommandCreateRequest {
                id: command_id,
                device_id,
                method,
                params,
                mode,
                issued_at,
                expires_at,
            })
            .await
            .map_err(|_| ApiError::CoreCommandUnavailable)?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(core_command_lifecycle_response(record)?),
        ));
    }
    let record = state
        .store
        .enqueue_command(NewCommandOutboxEntry {
            id: command_id.to_string(),
            device_id,
            method,
            params: params.to_string(),
            mode,
            expires_at,
            next_attempt_at: issued_at,
        })
        .await
        .map_err(sqlite_store_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(command_lifecycle_response(
            sqlite_uuid(&record.id)?,
            sqlite_command_outbox_state(record.state),
            record.expires_at,
            rpc_mode_value(record.mode),
            record
                .response
                .map(|value| serde_json::from_str(&value))
                .transpose()
                .map_err(ApiError::Serialization)?,
            record.responded_at,
        )?),
    ))
}

async fn sqlite_get_device_command(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<Json<CommandLifecycleResponse>, ApiError> {
    if let Some(facade) = &state.core_facade {
        let record = facade
            .get_command(id)
            .await
            .map_err(core_facade_command_error)?;
        if !sqlite_device_permission(state.store.pool(), &context, &record.device_id)
            .await?
            .is_some_and(|access| access.allows(ResourcePermission::Viewer))
        {
            return Err(ApiError::Forbidden);
        }
        return core_command_lifecycle_response(record).map(Json);
    }
    let row = sqlx::query(
        "SELECT id, device_id, state, expires_at, mode, response, responded_at
         FROM command_outbox
         WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("device command"))?;
    let device_id: String = row.try_get("device_id")?;
    if !sqlite_device_permission(state.store.pool(), &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }

    Ok(Json(command_lifecycle_response(
        sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        &row.try_get::<String, _>("state")?,
        sqlite_timestamp(&row.try_get::<String, _>("expires_at")?)?,
        &row.try_get::<String, _>("mode")?,
        row.try_get::<Option<String>, _>("response")?
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(ApiError::Serialization)?,
        row.try_get::<Option<String>, _>("responded_at")?
            .map(|value| sqlite_timestamp(&value))
            .transpose()?,
    )?))
}

async fn resolve_command_target_sqlite(
    state: &SqliteApiState,
    device_id: &str,
) -> Result<CommandTarget, ApiError> {
    if !is_identifier(device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let target = sqlx::query(
        "SELECT child.gateway_device_id,
                gateway.device_id AS active_gateway_device_id,
                gateway.is_gateway AS active_gateway_is_gateway
         FROM devices AS child
         LEFT JOIN devices AS gateway
           ON gateway.device_id = child.gateway_device_id
          AND gateway.deleted_at IS NULL
         WHERE child.device_id = ? AND child.deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(state.store.pool())
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if target
        .try_get::<Option<String>, _>("gateway_device_id")?
        .is_none()
    {
        return Ok(CommandTarget::direct(device_id));
    }
    let active_gateway_device_id =
        target.try_get::<Option<String>, _>("active_gateway_device_id")?;
    let active_gateway_is_gateway =
        target.try_get::<Option<i64>, _>("active_gateway_is_gateway")?;

    let Some(active_gateway_device_id) = active_gateway_device_id else {
        return Err(ApiError::Conflict(
            "assigned gateway device is unavailable".to_owned(),
        ));
    };
    if active_gateway_is_gateway != Some(1) {
        return Err(ApiError::Conflict(
            "assigned gateway device is not a gateway".to_owned(),
        ));
    }
    Ok(CommandTarget::gateway_child(
        active_gateway_device_id,
        device_id,
    ))
}

async fn sqlite_raw_points(
    pool: &sqlx::SqlitePool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<TelemetryPoint>, ApiError> {
    let rows = sqlx::query(
        "SELECT
            event_at AS at,
            CASE
                WHEN json_type(measurements, '$.temperature_c') IN ('integer', 'real')
                THEN json_extract(measurements, '$.temperature_c')
            END AS temperature_c,
            CASE
                WHEN json_type(measurements, '$.humidity_pct') IN ('integer', 'real')
                THEN json_extract(measurements, '$.humidity_pct')
            END AS humidity_pct
         FROM telemetry
         WHERE device_id = ? AND event_at >= ? AND event_at <= ?
         ORDER BY event_at",
    )
    .bind(device_id)
    .bind(from.to_rfc3339())
    .bind(to.to_rfc3339())
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(TelemetryPoint {
                at: sqlite_timestamp(&row.try_get::<String, _>("at")?)?,
                temperature_c: row.try_get("temperature_c")?,
                humidity_pct: row.try_get("humidity_pct")?,
                event_count: 1,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum SqliteTelemetryRollup {
    FiveMinutes,
    OneHour,
}

async fn sqlite_rollup_points(
    pool: &sqlx::SqlitePool,
    rollup: SqliteTelemetryRollup,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<TelemetryPoint>, ApiError> {
    let query = match rollup {
        SqliteTelemetryRollup::FiveMinutes => {
            "SELECT bucket_at AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_rollups_5m
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
        SqliteTelemetryRollup::OneHour => {
            "SELECT bucket_at AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_rollups_1h
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
    };
    let rows = sqlx::query(query)
        .bind(device_id)
        .bind(from.to_rfc3339())
        .bind(to.to_rfc3339())
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(TelemetryPoint {
                at: sqlite_timestamp(&row.try_get::<String, _>("at")?)?,
                temperature_c: row.try_get("temperature_c")?,
                humidity_pct: row.try_get("humidity_pct")?,
                event_count: row.try_get("event_count")?,
            })
        })
        .collect()
}

async fn sqlite_power_raw_points(
    pool: &sqlx::SqlitePool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryPoint>, ApiError> {
    let rows = sqlx::query(
        "SELECT
            event_at AS at,
            CASE WHEN json_type(measurements, '$.voltage_v') IN ('integer', 'real')
                 THEN json_extract(measurements, '$.voltage_v') END AS voltage_v,
            CASE WHEN json_type(measurements, '$.current_a') IN ('integer', 'real')
                 THEN json_extract(measurements, '$.current_a') END AS current_a,
            CASE WHEN json_type(measurements, '$.power_w') IN ('integer', 'real')
                 THEN json_extract(measurements, '$.power_w') END AS power_w,
            CASE WHEN json_type(measurements, '$.energy_kwh') IN ('integer', 'real')
                 THEN json_extract(measurements, '$.energy_kwh') END AS energy_kwh,
            CASE WHEN json_type(measurements, '$.frequency_hz') IN ('integer', 'real')
                 THEN json_extract(measurements, '$.frequency_hz') END AS frequency_hz,
            CASE WHEN json_type(measurements, '$.power_factor') IN ('integer', 'real')
                 THEN json_extract(measurements, '$.power_factor') END AS power_factor,
            1 AS event_count
         FROM telemetry
         WHERE device_id = ? AND event_at >= ? AND event_at <= ?
         ORDER BY event_at, sequence
         LIMIT ?",
    )
    .bind(device_id)
    .bind(from.to_rfc3339())
    .bind(to.to_rfc3339())
    .bind(SQLITE_MAX_RAW_POWER_TELEMETRY_ROWS)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(sqlite_power_point_from_row).collect()
}

async fn sqlite_power_rollup_points(
    pool: &sqlx::SqlitePool,
    rollup: SqliteTelemetryRollup,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryPoint>, ApiError> {
    let query = match rollup {
        SqliteTelemetryRollup::FiveMinutes => {
            "SELECT
                bucket_at AS at,
                avg_voltage_v AS voltage_v,
                avg_current_a AS current_a,
                avg_power_w AS power_w,
                avg_energy_kwh AS energy_kwh,
                NULL AS frequency_hz,
                NULL AS power_factor,
                event_count
             FROM telemetry_rollups_5m
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
        SqliteTelemetryRollup::OneHour => {
            "SELECT
                bucket_at AS at,
                avg_voltage_v AS voltage_v,
                avg_current_a AS current_a,
                avg_power_w AS power_w,
                avg_energy_kwh AS energy_kwh,
                NULL AS frequency_hz,
                NULL AS power_factor,
                event_count
             FROM telemetry_rollups_1h
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
    };
    let rows = sqlx::query(query)
        .bind(device_id)
        .bind(from.to_rfc3339())
        .bind(to.to_rfc3339())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(sqlite_power_point_from_row).collect()
}

async fn sqlite_power_asset_raw_points(
    pool: &sqlx::SqlitePool,
    asset_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryPoint>, ApiError> {
    let rows = sqlx::query(
        "WITH RECURSIVE descendants(id) AS (
            SELECT id FROM assets WHERE id = ?
            UNION
            SELECT children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.id
         ),
         per_device AS (
            SELECT
                strftime(
                    '%Y-%m-%dT%H:%M:%SZ',
                    (CAST(strftime('%s', telemetry.event_at) AS INTEGER) / 60) * 60,
                    'unixepoch'
                ) AS at,
                telemetry.device_id,
                AVG(CASE WHEN json_type(telemetry.measurements, '$.voltage_v') IN ('integer', 'real')
                         THEN json_extract(telemetry.measurements, '$.voltage_v') END) AS voltage_v,
                AVG(CASE WHEN json_type(telemetry.measurements, '$.current_a') IN ('integer', 'real')
                         THEN json_extract(telemetry.measurements, '$.current_a') END) AS current_a,
                AVG(CASE WHEN json_type(telemetry.measurements, '$.power_w') IN ('integer', 'real')
                         THEN json_extract(telemetry.measurements, '$.power_w') END) AS power_w,
                MAX(CASE WHEN json_type(telemetry.measurements, '$.energy_kwh') IN ('integer', 'real')
                         THEN json_extract(telemetry.measurements, '$.energy_kwh') END) AS energy_kwh,
                AVG(CASE WHEN json_type(telemetry.measurements, '$.frequency_hz') IN ('integer', 'real')
                         THEN json_extract(telemetry.measurements, '$.frequency_hz') END) AS frequency_hz,
                AVG(CASE WHEN json_type(telemetry.measurements, '$.power_factor') IN ('integer', 'real')
                         THEN json_extract(telemetry.measurements, '$.power_factor') END) AS power_factor,
                COUNT(*) AS event_count
            FROM telemetry
            WHERE telemetry.device_id IN (
                SELECT device_id
                FROM devices
                WHERE asset_id IN (SELECT id FROM descendants)
                  AND deleted_at IS NULL
            )
              AND telemetry.event_at >= ?
              AND telemetry.event_at <= ?
            GROUP BY at, telemetry.device_id
         )
         SELECT
            at,
            AVG(voltage_v) AS voltage_v,
            AVG(current_a) AS current_a,
            SUM(power_w) AS power_w,
            SUM(energy_kwh) AS energy_kwh,
            AVG(frequency_hz) AS frequency_hz,
            AVG(power_factor) AS power_factor,
            SUM(event_count) AS event_count
         FROM per_device
         GROUP BY at
         ORDER BY at
         LIMIT ?",
    )
    .bind(asset_id.to_string())
    .bind(from.to_rfc3339())
    .bind(to.to_rfc3339())
    .bind(SQLITE_MAX_RAW_POWER_TELEMETRY_ROWS)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(sqlite_power_point_from_row).collect()
}

async fn sqlite_power_asset_rollup_points(
    pool: &sqlx::SqlitePool,
    rollup: SqliteTelemetryRollup,
    asset_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryPoint>, ApiError> {
    let query = match rollup {
        SqliteTelemetryRollup::FiveMinutes => {
            "WITH RECURSIVE descendants(id) AS (
            SELECT id FROM assets WHERE id = ?
            UNION
            SELECT children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.id
         ),
         per_device AS (
            SELECT
                rollup.bucket_at AS at,
                rollup.device_id,
                rollup.avg_voltage_v AS voltage_v,
                rollup.avg_current_a AS current_a,
                rollup.avg_power_w AS power_w,
                rollup.avg_energy_kwh AS energy_kwh,
                rollup.event_count
            FROM telemetry_rollups_5m AS rollup
            JOIN devices ON devices.device_id = rollup.device_id
            WHERE devices.asset_id IN (SELECT id FROM descendants)
              AND devices.deleted_at IS NULL
              AND rollup.bucket_at >= ?
              AND rollup.bucket_at <= ?
         )
         SELECT
            at,
            AVG(voltage_v) AS voltage_v,
            AVG(current_a) AS current_a,
            SUM(power_w) AS power_w,
            SUM(energy_kwh) AS energy_kwh,
            NULL AS frequency_hz,
            NULL AS power_factor,
            SUM(event_count) AS event_count
         FROM per_device
         GROUP BY at
         ORDER BY at"
        }
        SqliteTelemetryRollup::OneHour => {
            "WITH RECURSIVE descendants(id) AS (
            SELECT id FROM assets WHERE id = ?
            UNION
            SELECT children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.id
         ),
         per_device AS (
            SELECT
                rollup.bucket_at AS at,
                rollup.device_id,
                rollup.avg_voltage_v AS voltage_v,
                rollup.avg_current_a AS current_a,
                rollup.avg_power_w AS power_w,
                rollup.avg_energy_kwh AS energy_kwh,
                rollup.event_count
            FROM telemetry_rollups_1h AS rollup
            JOIN devices ON devices.device_id = rollup.device_id
            WHERE devices.asset_id IN (SELECT id FROM descendants)
              AND devices.deleted_at IS NULL
              AND rollup.bucket_at >= ?
              AND rollup.bucket_at <= ?
         )
         SELECT
            at,
            AVG(voltage_v) AS voltage_v,
            AVG(current_a) AS current_a,
            SUM(power_w) AS power_w,
            SUM(energy_kwh) AS energy_kwh,
            NULL AS frequency_hz,
            NULL AS power_factor,
            SUM(event_count) AS event_count
         FROM per_device
         GROUP BY at
         ORDER BY at"
        }
    };
    let rows = sqlx::query(query)
        .bind(asset_id.to_string())
        .bind(from.to_rfc3339())
        .bind(to.to_rfc3339())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(sqlite_power_point_from_row).collect()
}

fn sqlite_power_point_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<PowerTelemetryPoint, ApiError> {
    Ok(PowerTelemetryPoint {
        at: sqlite_timestamp(&row.try_get::<String, _>("at")?)?,
        voltage_v: row.try_get("voltage_v")?,
        current_a: row.try_get("current_a")?,
        power_w: row.try_get("power_w")?,
        energy_kwh: row.try_get("energy_kwh")?,
        frequency_hz: row.try_get("frequency_hz")?,
        power_factor: row.try_get("power_factor")?,
        event_count: row.try_get("event_count")?,
    })
}

async fn sqlite_list_alert_rules(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<AlertRuleResponse>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds, created_at, updated_at
         FROM alert_rules
         WHERE archived_at IS NULL
         ORDER BY created_at DESC, id",
    )
    .fetch_all(state.store.pool())
    .await?;

    let rules = rows
        .into_iter()
        .map(sqlite_alert_rule_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut visible = Vec::new();
    for rule in rules {
        if sqlite_can_read_alert(state.store.pool(), &context, rule.device_id.as_deref()).await? {
            visible.push(rule);
        }
    }
    Ok(Json(visible))
}

async fn sqlite_create_alert_rule(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<CreateAlertRuleRequest>,
) -> Result<(StatusCode, Json<AlertRuleResponse>), ApiError> {
    let rule = request.validate()?;
    sqlite_require_alert_manager(state.store.pool(), &context, rule.device_id.as_deref()).await?;
    let id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO alert_rules (
            id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
            window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
            hysteresis, severity, reminder_interval_seconds, created_at, updated_at
         ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id.to_string())
    .bind(rule.name)
    .bind(rule.device_id)
    .bind(rule.metric_key)
    .bind(rule.rule_type)
    .bind(rule.comparison)
    .bind(rule.threshold)
    .bind(rule.window_seconds)
    .bind(rule.for_seconds)
    .bind(rule.resolve_after_seconds)
    .bind(rule.reopen_grace_seconds)
    .bind(rule.hysteresis)
    .bind(rule.severity)
    .bind(rule.reminder_interval_seconds)
    .bind(&now)
    .bind(&now)
    .execute(state.store.pool())
    .await?;

    let rule = sqlite_active_alert_rule(state.store.pool(), &id.to_string())
        .await?
        .ok_or(ApiError::NotFound("alert rule not found"))?;
    Ok((StatusCode::CREATED, Json(rule)))
}

async fn sqlite_update_alert_rule(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateAlertRuleRequest>,
) -> Result<Json<AlertRuleResponse>, ApiError> {
    let current_device_id = sqlite_alert_rule_device(state.store.pool(), id).await?;
    sqlite_require_alert_manager(state.store.pool(), &context, current_device_id.as_deref())
        .await?;
    let rule = request.validate()?;
    sqlite_require_alert_manager(state.store.pool(), &context, rule.device_id.as_deref()).await?;
    let result = sqlx::query(
        "UPDATE alert_rules
         SET name = ?, device_id = ?, metric_key = ?, rule_type = ?, comparison = ?,
             threshold = ?, window_seconds = ?, for_seconds = ?,
             resolve_after_seconds = ?, reopen_grace_seconds = ?, hysteresis = ?,
             severity = ?, reminder_interval_seconds = ?, updated_at = ?
         WHERE id = ? AND archived_at IS NULL",
    )
    .bind(rule.name)
    .bind(rule.device_id)
    .bind(rule.metric_key)
    .bind(rule.rule_type)
    .bind(rule.comparison)
    .bind(rule.threshold)
    .bind(rule.window_seconds)
    .bind(rule.for_seconds)
    .bind(rule.resolve_after_seconds)
    .bind(rule.reopen_grace_seconds)
    .bind(rule.hysteresis)
    .bind(rule.severity)
    .bind(rule.reminder_interval_seconds)
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .execute(state.store.pool())
    .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::NotFound("alert rule not found"));
    }

    let rule = sqlite_active_alert_rule(state.store.pool(), &id.to_string())
        .await?
        .ok_or(ApiError::NotFound("alert rule not found"))?;
    Ok(Json(rule))
}

async fn sqlite_archive_alert_rule(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let device_id = sqlite_alert_rule_device(state.store.pool(), id).await?;
    sqlite_require_alert_manager(state.store.pool(), &context, device_id.as_deref()).await?;
    let archived_at = Utc::now().to_rfc3339();
    let rule_id = id.to_string();
    let mut transaction = state.store.pool().begin().await?;
    let rule = sqlx::query(
        "SELECT name, metric_key, threshold, severity
         FROM alert_rules
         WHERE id = ? AND archived_at IS NULL",
    )
    .bind(&rule_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("alert rule not found"))?;
    let name: String = rule.try_get("name")?;
    let metric_key: String = rule.try_get("metric_key")?;
    let threshold: f64 = rule.try_get("threshold")?;
    let severity: String = rule.try_get("severity")?;
    let incidents = sqlx::query(
        "SELECT id, device_id, last_value, state_version
         FROM alert_incidents
         WHERE rule_id = ? AND status IN ('pending', 'open')",
    )
    .bind(&rule_id)
    .fetch_all(&mut *transaction)
    .await?;

    sqlx::query(
        "UPDATE alert_rules
         SET enabled = 0, archived_at = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(&archived_at)
    .bind(&archived_at)
    .bind(&rule_id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'resolved', recovery_started_at = ?, resolved_at = ?,
             last_notified_at = ?, state_version = state_version + 1, updated_at = ?
         WHERE rule_id = ? AND status IN ('pending', 'open')",
    )
    .bind(&archived_at)
    .bind(&archived_at)
    .bind(&archived_at)
    .bind(&archived_at)
    .bind(&rule_id)
    .execute(&mut *transaction)
    .await?;

    for incident in incidents {
        let incident_id: String = incident.try_get("id")?;
        let device_id: String = incident.try_get("device_id")?;
        let value: Option<f64> = incident.try_get("last_value")?;
        let state_version: i64 = incident.try_get("state_version")?;
        let value = value
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "unavailable".to_owned());
        let state_version = state_version + 1;
        let dedupe_key = format!("incident:{incident_id}:resolved:{state_version}");
        let subject = format!("[{severity}] {name} resolved");
        let body = format!(
            "Rule: {name}\nDevice: {device_id}\nMetric: {metric_key}\nValue: {value}\nThreshold: {threshold:.3}\nState: resolved\n"
        );
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
             ) VALUES (?, ?, 'resolved', ?, ?, ?, ?, ?)
             ON CONFLICT (dedupe_key) DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(incident_id)
        .bind(dedupe_key)
        .bind(subject)
        .bind(body)
        .bind(&archived_at)
        .bind(&archived_at)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn sqlite_toggle_alert_rule(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
    Json(request): Json<ToggleAlertRuleRequest>,
) -> Result<Json<AlertRuleResponse>, ApiError> {
    let device_id = sqlite_alert_rule_device(state.store.pool(), id).await?;
    sqlite_require_alert_manager(state.store.pool(), &context, device_id.as_deref()).await?;
    let result = sqlx::query(
        "UPDATE alert_rules
         SET enabled = ?, updated_at = ?
         WHERE id = ? AND archived_at IS NULL",
    )
    .bind(i64::from(request.enabled))
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .execute(state.store.pool())
    .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::NotFound("alert rule not found"));
    }

    let rule = sqlite_active_alert_rule(state.store.pool(), &id.to_string())
        .await?
        .ok_or(ApiError::NotFound("alert rule not found"))?;
    Ok(Json(rule))
}

async fn sqlite_list_alert_incidents(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<AlertIncidentResponse>>, ApiError> {
    let rows = sqlite_alert_incident_rows(state.store.pool(), None).await?;
    let incidents = rows
        .into_iter()
        .map(sqlite_alert_incident_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut visible = Vec::new();
    for incident in incidents {
        if sqlite_can_read_alert(state.store.pool(), &context, Some(&incident.device_id)).await? {
            visible.push(incident);
        }
    }
    Ok(Json(visible))
}

async fn sqlite_acknowledge_alert_incident(
    State(state): State<SqliteApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<Json<AlertIncidentResponse>, ApiError> {
    let device_id = sqlite_alert_incident_device(state.store.pool(), id).await?;
    sqlite_require_alert_manager(state.store.pool(), &context, Some(&device_id)).await?;
    let result = sqlx::query(
        "UPDATE alert_incidents
         SET acknowledged_at = ?, acknowledged_by = 'dashboard', updated_at = ?
         WHERE id = ?
           AND EXISTS (SELECT 1 FROM alert_rules WHERE alert_rules.id = alert_incidents.rule_id)",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(Utc::now().to_rfc3339())
    .bind(id.to_string())
    .execute(state.store.pool())
    .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::NotFound("alert incident not found"));
    }

    let row = sqlite_alert_incident_rows(state.store.pool(), Some(&id.to_string()))
        .await?
        .into_iter()
        .next()
        .ok_or(ApiError::NotFound("alert incident not found"))?;
    Ok(Json(sqlite_alert_incident_from_row(row)?))
}

#[utoipa::path(
    get,
    path = "/api/auth/me",
    responses((status = 200, description = "Current session", body = AuthResponse)),
    security(("sessionAuth" = [])),
    tag = "Authentication"
)]
async fn current_role(Extension(context): Extension<AuthContext>) -> Json<AuthResponse> {
    Json(AuthResponse {
        role: context.role,
        account_class: context.account_class,
        username: context.username,
        default_app: context.default_app,
        granted_apps: context.granted_apps,
        session_id: None,
    })
}

#[utoipa::path(
    post,
    path = "/api/auth/logout",
    responses((status = 204, description = "Session revoked")),
    security(("sessionAuth" = [])),
    tag = "Authentication"
)]
async fn logout(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> StatusCode {
    state.revoke_session(&context.session_id);
    StatusCode::NO_CONTENT
}

#[utoipa::path(
    get,
    path = "/api/devices/{device_id}/tokens",
    params(("device_id" = String, Path, description = "Platform device ID")),
    responses((status = 200, description = "Device token history")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn list_device_tokens_handler(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
) -> Result<Json<Vec<DeviceTokenResponse>>, ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    list_device_tokens(&state.pool, &state.token_vault, &device_id)
        .await
        .map(Json)
        .map_err(device_token_error)
}

#[utoipa::path(
    post,
    path = "/api/devices/{device_id}/tokens",
    params(("device_id" = String, Path, description = "Platform device ID")),
    responses((status = 201, description = "Created device token")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn create_device_token_handler(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    let prior_token_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM device_tokens
         WHERE device_id = $1 AND revoked_at IS NULL",
    )
    .bind(&device_id)
    .fetch_optional(&state.pool)
    .await?;
    let token = create_device_token(&state.pool, &state.token_vault, &device_id)
        .await
        .map_err(device_token_error)?;
    if let Some(prior_token_id) = prior_token_id {
        revoke_mqttd_device_transport_session(
            state.mqttd_device_transport_session_revoker.as_ref(),
            device_id,
            prior_token_id,
        )
        .await?;
    }
    Ok((StatusCode::CREATED, Json(token)))
}

#[utoipa::path(
    post,
    path = "/api/device-tokens",
    request_body = ProvisionDeviceTokenRequest,
    responses((status = 201, description = "Provisioned device and token")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn provision_device_token_handler(
    State(state): State<ApiState>,
    _admin: Admin,
    Json(request): Json<ProvisionDeviceTokenRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    let display_name = request.display_name.trim();
    if display_name.is_empty() || display_name.len() > 128 {
        return Err(ApiError::BadRequest("invalid device name".to_owned()));
    }
    provision_device_token(&state.pool, &state.token_vault, &display_name)
        .await
        .map(|token| (StatusCode::CREATED, Json(token)))
        .map_err(device_token_error)
}

#[utoipa::path(
    post,
    path = "/api/device-tokens/{id}/rotate",
    params(("id" = Uuid, Path, description = "Device token ID")),
    responses((status = 201, description = "Rotated device token")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn rotate_device_token_handler(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    let device_id = sqlx::query_scalar::<_, String>(
        "SELECT device_id FROM device_tokens
         WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("device token"))?;
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    let token = rotate_device_token(&state.pool, &state.token_vault, id)
        .await
        .map_err(device_token_error)?;
    revoke_mqttd_device_transport_session(
        state.mqttd_device_transport_session_revoker.as_ref(),
        device_id,
        id,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(token)))
}

#[utoipa::path(
    post,
    path = "/api/device-tokens/{id}/revoke",
    params(("id" = Uuid, Path, description = "Device token ID")),
    responses((status = 204, description = "Revoked device token")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn revoke_device_token_handler(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let device_id = sqlx::query_scalar::<_, String>(
        "SELECT device_id FROM device_tokens
         WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("device token"))?;
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    revoke_device_token(&state.pool, id)
        .await
        .map_err(device_token_error)?;
    revoke_mqttd_device_transport_session(
        state.mqttd_device_transport_session_revoker.as_ref(),
        device_id,
        id,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    put,
    path = "/api/auth/password",
    request_body = serde_json::Value,
    responses((status = 204, description = "Password changed and sessions revoked")),
    security(("sessionAuth" = [])),
    tag = "Authentication"
)]
async fn change_password_handler(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<ChangePasswordRequest>,
) -> Result<StatusCode, ApiError> {
    change_password(
        &state.pool,
        &context.username,
        &request.current_password,
        &request.new_password,
    )
    .await
    .map_err(|error| match error {
        AuthError::InvalidPasswordFormat => ApiError::BadRequest("invalid password".to_owned()),
        AuthError::AuthenticationFailed => ApiError::Unauthorized,
        error => ApiError::Auth(error),
    })?;
    state.revoke_sessions_for_username(&context.username);
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/system-configuration",
    responses((status = 200, description = "System configuration", body = SystemConfiguration)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn get_system_configuration(
    State(state): State<ApiState>,
    _system: System,
) -> Result<Json<SystemConfiguration>, ApiError> {
    state
        .system_configuration
        .read()
        .await
        .map(Json)
        .map_err(system_configuration_error)
}

#[utoipa::path(
    put,
    path = "/api/system-configuration",
    request_body = SystemConfigurationUpdate,
    responses((status = 200, description = "Updated system configuration", body = SystemConfiguration)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn update_system_configuration(
    State(state): State<ApiState>,
    _system: System,
    Json(update): Json<SystemConfigurationUpdate>,
) -> Result<Json<SystemConfiguration>, ApiError> {
    state
        .system_configuration
        .apply(update)
        .await
        .map(Json)
        .map_err(system_configuration_error)
}

#[utoipa::path(
    post,
    path = "/api/devices/{device_id}/shares",
    params(("device_id" = String, Path, description = "Platform device ID")),
    request_body = CreateResourceShareRequest,
    responses((status = 201, description = "Pending device share", body = ResourceShareResponse)),
    security(("sessionAuth" = [])),
    tag = "Resource sharing"
)]
async fn create_device_share(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Json(request): Json<CreateResourceShareRequest>,
) -> Result<(StatusCode, Json<ResourceShareResponse>), ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    create_resource_share(&state, &context, ResourceKind::Device, device_id, request).await
}

#[utoipa::path(
    post,
    path = "/api/assets/{asset_id}/shares",
    params(("asset_id" = Uuid, Path, description = "Asset ID")),
    request_body = CreateResourceShareRequest,
    responses((status = 201, description = "Pending asset share", body = ResourceShareResponse)),
    security(("sessionAuth" = [])),
    tag = "Resource sharing"
)]
async fn create_asset_share(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(asset_id): Path<Uuid>,
    Json(request): Json<CreateResourceShareRequest>,
) -> Result<(StatusCode, Json<ResourceShareResponse>), ApiError> {
    create_resource_share(
        &state,
        &context,
        ResourceKind::Asset,
        asset_id.to_string(),
        request,
    )
    .await
}

async fn create_resource_share(
    state: &ApiState,
    context: &AuthContext,
    kind: ResourceKind,
    resource_id: String,
    request: CreateResourceShareRequest,
) -> Result<(StatusCode, Json<ResourceShareResponse>), ApiError> {
    let permission = ResourcePermission::parse_share(&request.permission)
        .ok_or_else(|| ApiError::BadRequest("invalid resource permission".to_owned()))?;
    let owner = resource_owner(&state.pool, kind, &resource_id).await?;
    let Some(owner) = owner else {
        return Err(ApiError::NotFound(match kind {
            ResourceKind::Asset => "asset",
            ResourceKind::Device => "device",
        }));
    };
    let access = resource_permission(&state.pool, context, kind, &resource_id).await?;
    if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
        return Err(ApiError::Forbidden);
    }
    let target_user_id = share_target_user_id(&state.pool, &request.username).await?;
    if owner == target_user_id {
        return Err(ApiError::Conflict(
            "resource owners cannot be invited".to_owned(),
        ));
    }
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT 1
         FROM resource_shares
         WHERE resource_type = $1
           AND resource_id = $2
           AND target_user_id = $3
           AND state = 'pending'",
    )
    .bind(kind.as_str())
    .bind(&resource_id)
    .bind(target_user_id)
    .fetch_optional(&state.pool)
    .await?
    .is_some();
    if existing {
        return Err(ApiError::Conflict(
            "a pending resource share already exists".to_owned(),
        ));
    }

    let row = sqlx::query(
        "INSERT INTO resource_shares (
            id, resource_type, resource_id, target_user_id, permission,
            inherit_children, state, created_by_user_id
         ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
         RETURNING id, resource_type, resource_id, permission, inherit_children, state",
    )
    .bind(Uuid::now_v7())
    .bind(kind.as_str())
    .bind(resource_id)
    .bind(target_user_id)
    .bind(permission.as_str())
    .bind(request.inherit_children)
    .bind(context.user_id)
    .fetch_one(&state.pool)
    .await
    .map_err(resource_share_database_error)?;
    let response = resource_share_from_row(row)?;
    write_audit_event(
        &state.pool,
        &context,
        &response.resource_type,
        &response.resource_id,
        "resource_share.created",
        None,
        Some(&json!({
            "resource_type": response.resource_type,
            "resource_id": response.resource_id,
            "permission": response.permission,
            "inherit_children": response.inherit_children,
            "state": response.state,
        })),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/api/me/resource-shares",
    params(("state" = Option<String>, Query, description = "pending, active, declined, cancelled, or expired")),
    responses((status = 200, description = "Current user's resource shares", body = [ResourceShareResponse])),
    security(("sessionAuth" = [])),
    tag = "Resource sharing"
)]
async fn list_my_resource_shares(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Query(query): Query<ResourceShareQuery>,
) -> Result<Json<Vec<ResourceShareResponse>>, ApiError> {
    let Some(state_filter) = query.state.as_deref() else {
        let rows = sqlx::query(
            "SELECT id, resource_type, resource_id, permission, inherit_children, state
             FROM resource_shares
             WHERE target_user_id = $1
             ORDER BY created_at DESC, id",
        )
        .bind(context.user_id)
        .fetch_all(&state.pool)
        .await?;
        return rows
            .into_iter()
            .map(resource_share_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map(Json);
    };
    if !is_resource_share_state(state_filter) {
        return Err(ApiError::BadRequest(
            "invalid resource share state".to_owned(),
        ));
    }
    let rows = sqlx::query(
        "SELECT id, resource_type, resource_id, permission, inherit_children, state
         FROM resource_shares
         WHERE target_user_id = $1 AND state = $2
         ORDER BY created_at DESC, id",
    )
    .bind(context.user_id)
    .bind(state_filter)
    .fetch_all(&state.pool)
    .await?;
    rows.into_iter()
        .map(resource_share_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

#[utoipa::path(
    post,
    path = "/api/resource-shares/{id}/accept",
    params(("id" = Uuid, Path, description = "Resource share ID")),
    responses((status = 204, description = "Resource share accepted")),
    security(("sessionAuth" = [])),
    tag = "Resource sharing"
)]
async fn accept_resource_share(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let accepted = sqlx::query(
        "UPDATE resource_shares
         SET state = 'active', responded_at = now()
         WHERE id = $1
           AND target_user_id = $2
           AND state = 'pending'
         RETURNING resource_type, resource_id",
    )
    .bind(id)
    .bind(context.user_id)
    .fetch_optional(&state.pool)
    .await?;
    if let Some(accepted) = accepted {
        write_audit_event(
            &state.pool,
            &context,
            &accepted.try_get::<String, _>("resource_type")?,
            &accepted.try_get::<String, _>("resource_id")?,
            "resource_share.accepted",
            Some(&json!({ "state": "pending" })),
            Some(&json!({ "state": "active" })),
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Forbidden)
    }
}

#[utoipa::path(
    delete,
    path = "/api/resource-shares/{id}",
    params(("id" = Uuid, Path, description = "Resource share ID")),
    responses((status = 204, description = "Resource share declined, cancelled, or revoked")),
    security(("sessionAuth" = [])),
    tag = "Resource sharing"
)]
async fn delete_resource_share(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let share = sqlx::query(
        "SELECT resource_type, resource_id, target_user_id, state
         FROM resource_shares
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("resource share"))?;
    let target_user_id: Uuid = share.try_get("target_user_id")?;
    let share_state: String = share.try_get("state")?;
    if target_user_id == context.user_id {
        if share_state != "pending" {
            return Err(ApiError::Forbidden);
        }
        let updated = sqlx::query(
            "UPDATE resource_shares
             SET state = 'declined', responded_at = now()
             WHERE id = $1 AND target_user_id = $2 AND state = 'pending'",
        )
        .bind(id)
        .bind(context.user_id)
        .execute(&state.pool)
        .await?
        .rows_affected();
        return if updated == 1 {
            write_audit_event(
                &state.pool,
                &context,
                &share.try_get::<String, _>("resource_type")?,
                &share.try_get::<String, _>("resource_id")?,
                "resource_share.declined",
                Some(&json!({ "state": "pending" })),
                Some(&json!({ "state": "declined" })),
            )
            .await?;
            Ok(StatusCode::NO_CONTENT)
        } else {
            Err(ApiError::Conflict(
                "resource share state changed concurrently".to_owned(),
            ))
        };
    }
    let kind = resource_kind(&share.try_get::<String, _>("resource_type")?)?;
    let resource_id: String = share.try_get("resource_id")?;
    let access = resource_permission(&state.pool, &context, kind, &resource_id).await?;
    if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
        return Err(ApiError::Forbidden);
    }
    let cancelled = sqlx::query(
        "UPDATE resource_shares
         SET state = 'cancelled', responded_at = now()
         WHERE id = $1 AND state IN ('pending', 'active')",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();
    if cancelled == 1 {
        write_audit_event(
            &state.pool,
            &context,
            kind.as_str(),
            &resource_id,
            "resource_share.cancelled",
            Some(&json!({ "state": share_state })),
            Some(&json!({ "state": "cancelled" })),
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Conflict(
            "resource share cannot be cancelled".to_owned(),
        ))
    }
}

#[utoipa::path(
    post,
    path = "/api/my/assets",
    request_body = CreateManagementAssetRequest,
    responses((status = 201, description = "Created user-owned asset", body = ManagementAsset)),
    security(("sessionAuth" = [])),
    tag = "My resources"
)]
async fn create_my_asset(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<CreateManagementAssetRequest>,
) -> Result<(StatusCode, Json<ManagementAsset>), ApiError> {
    require_user_account(&context)?;
    let CreateManagementAssetRequest {
        name,
        asset_profile_id,
        parent_asset_id,
        metadata,
        attributes,
    } = request;
    if let Some(parent_asset_id) = parent_asset_id {
        let access = asset_permission(&state.pool, &context, parent_asset_id).await?;
        if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
            return Err(ApiError::Forbidden);
        }
    }
    let name = validated_name(&name, "asset name")?.to_owned();
    let metadata = object_value(attributes.unwrap_or(metadata), "asset attributes")?;
    let row = sqlx::query(
        "INSERT INTO assets (
            id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata
         ) VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
    )
    .bind(Uuid::now_v7())
    .bind(name)
    .bind(asset_profile_id)
    .bind(parent_asset_id)
    .bind(context.user_id)
    .bind(metadata)
    .fetch_one(&state.pool)
    .await?;
    let asset = management_asset_from_row(row)?;
    write_audit_event(
        &state.pool,
        &context,
        "asset",
        &asset.id.to_string(),
        "asset.created",
        None,
        Some(&json!({
            "owner_user_id": context.user_id,
            "parent_asset_id": asset.parent_asset_id,
        })),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(asset)))
}

#[utoipa::path(
    post,
    path = "/api/my/devices",
    request_body = ProvisionMyDeviceRequest,
    responses((status = 201, description = "Provisioned user-owned device", body = DeviceTokenResponse)),
    security(("sessionAuth" = [])),
    tag = "My resources"
)]
async fn provision_my_device(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<ProvisionMyDeviceRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    require_user_account(&context)?;
    let display_name = validated_name(&request.display_name, "device name")?;
    if let Some(asset_id) = request.asset_id {
        let access = asset_permission(&state.pool, &context, asset_id).await?;
        if !access.is_some_and(|access| access.allows(ResourcePermission::Manager)) {
            return Err(ApiError::Forbidden);
        }
    }
    let token = provision_owned_device_token(
        &state.pool,
        &state.token_vault,
        display_name,
        context.user_id,
        request.asset_id,
    )
    .await
    .map_err(device_token_error)?;
    write_audit_event(
        &state.pool,
        &context,
        "device",
        &token.device_id,
        "device.provisioned",
        None,
        Some(&json!({
            "owner_user_id": context.user_id,
            "asset_id": request.asset_id,
            "token_id": token.id,
        })),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(token)))
}

#[utoipa::path(
    put,
    path = "/api/my/devices/{device_id}/asset",
    params(("device_id" = String, Path, description = "Platform device ID")),
    request_body = AssignMyDeviceAssetRequest,
    responses((status = 204, description = "Device asset assignment updated")),
    security(("sessionAuth" = [])),
    tag = "My resources"
)]
async fn assign_my_device_asset(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Json(request): Json<AssignMyDeviceAssetRequest>,
) -> Result<StatusCode, ApiError> {
    require_user_account(&context)?;
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    if let Some(asset_id) = request.asset_id {
        if !asset_permission(&state.pool, &context, asset_id)
            .await?
            .is_some_and(|access| access.allows(ResourcePermission::Manager))
        {
            return Err(ApiError::Forbidden);
        }
    }
    let updated = sqlx::query(
        "UPDATE devices
         SET asset_id = $1, configuration_version = configuration_version + 1
         WHERE device_id = $2 AND deleted_at IS NULL",
    )
    .bind(request.asset_id)
    .bind(&device_id)
    .execute(&state.pool)
    .await?
    .rows_affected();
    if updated == 1 {
        write_audit_event(
            &state.pool,
            &context,
            "device",
            &device_id,
            "device.asset_assigned",
            None,
            Some(&json!({ "asset_id": request.asset_id })),
        )
        .await?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound("device"))
    }
}

#[utoipa::path(
    get,
    path = "/api/management/devices",
    responses((status = 200, description = "Managed devices", body = [ManagementDevice])),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn list_management_devices(
    State(state): State<ApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementDevice>>, ApiError> {
    let rows = sqlx::query(
        "SELECT device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
         FROM devices
         WHERE deleted_at IS NULL
         ORDER BY device_id",
    )
    .fetch_all(&state.pool)
    .await?;
    let online_threshold = Utc::now() - Duration::minutes(5);
    rows.into_iter()
        .map(|row| management_device_from_row(row, online_threshold))
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

#[utoipa::path(
    post,
    path = "/api/management/devices",
    request_body = ProvisionDeviceTokenRequest,
    responses((status = 201, description = "Provisioned device", body = DeviceTokenResponse)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn provision_management_device(
    State(state): State<ApiState>,
    _admin: Admin,
    Json(request): Json<ProvisionDeviceTokenRequest>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ApiError> {
    let display_name = validated_name(&request.display_name, "device name")?.to_owned();
    provision_device_token(&state.pool, &state.token_vault, &display_name)
        .await
        .map(|token| (StatusCode::CREATED, Json(token)))
        .map_err(device_token_error)
}

#[utoipa::path(
    post,
    path = "/api/management/devices/{device_id}/claim-code",
    params(("device_id" = String, Path, description = "Platform device ID")),
    request_body = IssueDeviceClaimCodeRequest,
    responses((status = 201, description = "One-time device claim code", body = DeviceClaimCodeResponse)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn issue_device_claim_code(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    request: Option<Json<IssueDeviceClaimCodeRequest>>,
) -> Result<(StatusCode, Json<DeviceClaimCodeResponse>), ApiError> {
    if context.account_class != AccountClass::Admin {
        return Err(ApiError::Forbidden);
    }
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let expires_at =
        claim_code_expiry(request.and_then(|Json(request)| request.expires_in_seconds))?;
    let claim_code = generate_device_token();
    let code_hash = hash_device_token(&claim_code).map_err(|_| ApiError::StorageData)?;
    let mut transaction = state.pool.begin().await?;
    let owner = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if owner.is_some() {
        return Err(ApiError::Conflict("device is already claimed".to_owned()));
    }
    sqlx::query(
        "INSERT INTO device_claim_codes (
            device_id, code_hash, expires_at, issued_by_user_id, issued_at, used_at
         ) VALUES ($1, $2, $3, $4, now(), NULL)
         ON CONFLICT(device_id) DO UPDATE SET
            code_hash = excluded.code_hash,
            expires_at = excluded.expires_at,
            issued_by_user_id = excluded.issued_by_user_id,
            issued_at = now(),
            used_at = NULL",
    )
    .bind(&device_id)
    .bind(code_hash)
    .bind(expires_at)
    .bind(context.user_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    write_audit_event(
        &state.pool,
        &context,
        "device",
        &device_id,
        "device.claim_code_issued",
        None,
        Some(&json!({ "expires_at": expires_at })),
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(DeviceClaimCodeResponse {
            device_id,
            claim_code,
            expires_at,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/api/device-claims",
    request_body = ClaimDeviceRequest,
    responses((status = 204, description = "Device claimed")),
    security(("sessionAuth" = [])),
    tag = "My resources"
)]
async fn claim_device(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<ClaimDeviceRequest>,
) -> Result<StatusCode, ApiError> {
    require_user_account(&context)?;
    if !is_identifier(&request.device_id) || request.claim_code.is_empty() {
        return Err(ApiError::BadRequest("invalid device claim".to_owned()));
    }
    let mut transaction = state.pool.begin().await?;
    let row = sqlx::query(
        "SELECT devices.owner_user_id, claims.code_hash, claims.expires_at, claims.used_at
         FROM devices
         LEFT JOIN device_claim_codes AS claims ON claims.device_id = devices.device_id
         WHERE devices.device_id = $1 AND devices.deleted_at IS NULL
         FOR UPDATE OF devices",
    )
    .bind(&request.device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if row.try_get::<Option<Uuid>, _>("owner_user_id")?.is_some() {
        return Err(ApiError::Conflict("device is already claimed".to_owned()));
    }
    let code_hash = row
        .try_get::<Option<String>, _>("code_hash")?
        .ok_or(ApiError::Forbidden)?;
    let expires_at = row
        .try_get::<Option<DateTime<Utc>>, _>("expires_at")?
        .ok_or(ApiError::Forbidden)?;
    let used_at: Option<DateTime<Utc>> = row.try_get("used_at")?;
    if used_at.is_some()
        || expires_at <= Utc::now()
        || !verify_device_token(&request.claim_code, &code_hash).unwrap_or(false)
    {
        return Err(ApiError::Forbidden);
    }
    let claimed = sqlx::query(
        "UPDATE devices
         SET owner_user_id = $1, claimed_at = now()
         WHERE device_id = $2 AND owner_user_id IS NULL",
    )
    .bind(context.user_id)
    .bind(&request.device_id)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if claimed != 1 {
        return Err(ApiError::Conflict("device is already claimed".to_owned()));
    }
    sqlx::query(
        "UPDATE device_claim_codes
         SET used_at = now()
         WHERE device_id = $1 AND used_at IS NULL",
    )
    .bind(&request.device_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    write_audit_event(
        &state.pool,
        &context,
        "device",
        &request.device_id,
        "device.claimed",
        Some(&json!({ "owner_user_id": null })),
        Some(&json!({ "owner_user_id": context.user_id })),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    put,
    path = "/api/management/devices/{device_id}",
    params(("device_id" = String, Path, description = "Platform device ID")),
    request_body = UpdateManagementDeviceRequest,
    responses((status = 200, description = "Updated managed device", body = ManagementDevice)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn update_management_device(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(device_id): Path<String>,
    Json(request): Json<UpdateManagementDeviceRequest>,
) -> Result<Json<ManagementDevice>, ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let display_name = validated_name(&request.display_name, "device name")?.to_owned();
    let attributes = request
        .attributes
        .map(|value| object_value(value, "device attributes"))
        .transpose()?
        .map(sqlx::types::Json);
    let mut transaction = state.pool.begin().await?;
    let current = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    let current_is_gateway = current.try_get::<bool, _>("is_gateway")?;
    let current_gateway_device_id = current.try_get::<Option<String>, _>("gateway_device_id")?;
    let (is_gateway, gateway_device_id) = request
        .topology
        .map(|topology| (topology.is_gateway, topology.gateway_device_id))
        .unwrap_or((current_is_gateway, current_gateway_device_id));

    if is_gateway && gateway_device_id.is_some() {
        return Err(ApiError::BadRequest(
            "a gateway cannot be assigned to another gateway".to_owned(),
        ));
    }
    if gateway_device_id.as_deref() == Some(device_id.as_str()) {
        return Err(ApiError::BadRequest(
            "a device cannot be its own gateway".to_owned(),
        ));
    }
    if current_is_gateway && !is_gateway {
        let has_children = sqlx::query(
            "SELECT 1
             FROM devices
             WHERE gateway_device_id = $1 AND deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(&device_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if has_children {
            return Err(ApiError::Conflict(
                "a gateway with assigned children cannot be demoted".to_owned(),
            ));
        }
    }
    if let Some(parent_device_id) = gateway_device_id.as_deref() {
        let parent_is_gateway = sqlx::query(
            "SELECT is_gateway
             FROM devices
             WHERE device_id = $1 AND deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(parent_device_id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| row.try_get::<bool, _>("is_gateway"))
        .transpose()?
        .ok_or_else(|| ApiError::Conflict("gateway device is unavailable".to_owned()))?;
        if !parent_is_gateway {
            return Err(ApiError::Conflict(
                "assigned gateway device is not a gateway".to_owned(),
            ));
        }
    }

    let row = sqlx::query(
        "UPDATE devices
         SET display_name = $2, asset_id = $3, device_profile_id = $4,
             metadata = COALESCE($5, metadata), is_gateway = $6,
             gateway_device_id = $7
         WHERE device_id = $1 AND deleted_at IS NULL
         RETURNING device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                   is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality",
    )
    .bind(&device_id)
    .bind(display_name)
    .bind(request.asset_id)
    .bind(request.device_profile_id)
    .bind(attributes)
    .bind(is_gateway)
    .bind(&gateway_device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if gateway_device_id.is_some() {
        sqlx::query(
            "UPDATE device_tokens
             SET revoked_at = now()
             WHERE device_id = $1 AND revoked_at IS NULL",
        )
        .bind(&device_id)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    management_device_from_row(row, Utc::now() - Duration::minutes(5)).map(Json)
}

#[utoipa::path(
    delete,
    path = "/api/management/devices/{device_id}",
    params(("device_id" = String, Path, description = "Platform device ID")),
    responses((status = 204, description = "Managed device deleted")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn delete_management_device(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(device_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let mut transaction = state.pool.begin().await?;
    let has_children = sqlx::query(
        "SELECT 1
         FROM devices
         WHERE gateway_device_id = $1 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(&device_id)
    .fetch_optional(&mut *transaction)
    .await?
    .is_some();
    if has_children {
        return Err(ApiError::Conflict(
            "a gateway with assigned children cannot be deleted".to_owned(),
        ));
    }
    let deleted = sqlx::query(
        "UPDATE devices
         SET deleted_at = now()
         WHERE device_id = $1 AND deleted_at IS NULL",
    )
    .bind(&device_id)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if deleted == 0 {
        return Err(ApiError::NotFound("device"));
    }
    sqlx::query(
        "UPDATE device_tokens
         SET revoked_at = now()
         WHERE device_id = $1 AND revoked_at IS NULL",
    )
    .bind(&device_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/management/assets",
    responses((status = 200, description = "Managed assets", body = [ManagementAsset])),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn list_management_assets(
    State(state): State<ApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementAsset>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, asset_profile_id, parent_asset_id, metadata
         FROM assets
         ORDER BY name, id",
    )
    .fetch_all(&state.pool)
    .await?;
    rows.into_iter()
        .map(management_asset_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

#[utoipa::path(
    post,
    path = "/api/management/assets",
    request_body = CreateManagementAssetRequest,
    responses((status = 201, description = "Created asset", body = ManagementAsset)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn create_management_asset(
    State(state): State<ApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementAssetRequest>,
) -> Result<(StatusCode, Json<ManagementAsset>), ApiError> {
    let CreateManagementAssetRequest {
        name,
        asset_profile_id,
        parent_asset_id,
        metadata,
        attributes,
    } = request;
    let name = validated_name(&name, "asset name")?.to_owned();
    let metadata = object_value(attributes.unwrap_or(metadata), "asset attributes")?;
    let row = sqlx::query(
        "INSERT INTO assets (id, name, asset_profile_id, parent_asset_id, metadata)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
    )
    .bind(Uuid::now_v7())
    .bind(name)
    .bind(asset_profile_id)
    .bind(parent_asset_id)
    .bind(sqlx::types::Json(metadata))
    .fetch_one(&state.pool)
    .await?;
    Ok((StatusCode::CREATED, Json(management_asset_from_row(row)?)))
}

#[utoipa::path(
    put,
    path = "/api/management/assets/{id}",
    params(("id" = Uuid, Path, description = "Asset ID")),
    request_body = CreateManagementAssetRequest,
    responses((status = 200, description = "Updated asset", body = ManagementAsset)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn update_management_asset(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateManagementAssetRequest>,
) -> Result<Json<ManagementAsset>, ApiError> {
    let CreateManagementAssetRequest {
        name,
        asset_profile_id,
        parent_asset_id,
        metadata,
        attributes,
    } = request;
    let name = validated_name(&name, "asset name")?.to_owned();
    let metadata = object_value(attributes.unwrap_or(metadata), "asset attributes")?;
    let row = sqlx::query(
        "WITH RECURSIVE descendants(id) AS (
            SELECT id
            FROM assets
            WHERE parent_asset_id = $1
            UNION
            SELECT children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.id
         )
         UPDATE assets
         SET name = $2, asset_profile_id = $3, parent_asset_id = $4, metadata = $5, updated_at = now()
         WHERE id = $1
           AND (
               $4 IS NULL
               OR ($4 <> $1 AND NOT EXISTS (SELECT 1 FROM descendants WHERE id = $4))
           )
         RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
    )
    .bind(id)
    .bind(name)
    .bind(asset_profile_id)
    .bind(parent_asset_id)
    .bind(sqlx::types::Json(metadata))
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("asset"))?;
    management_asset_from_row(row).map(Json)
}

#[utoipa::path(
    delete,
    path = "/api/management/assets/{id}",
    params(("id" = Uuid, Path, description = "Asset ID")),
    responses((status = 204, description = "Asset deleted")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn delete_management_asset(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sqlx::query("DELETE FROM assets WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();
    if deleted == 0 {
        Err(ApiError::NotFound("asset"))
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

#[utoipa::path(
    get,
    path = "/api/management/users",
    responses((status = 200, description = "Platform users and app grants")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn list_management_users(
    State(state): State<ApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementUser>>, ApiError> {
    list_management_users_query(&state.pool).await.map(Json)
}

#[utoipa::path(
    post,
    path = "/api/management/users",
    request_body = CreateManagementUserRequest,
    responses((status = 201, description = "Created normal user account", body = ManagementUser)),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn create_management_user(
    State(state): State<ApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementUserRequest>,
) -> Result<(StatusCode, Json<ManagementUser>), ApiError> {
    let Some(default_app_key) = app_key_from_path(&request.default_app) else {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    };
    if !is_identifier(&request.username)
        || request.granted_apps.is_empty()
        || request.granted_apps.iter().any(|app| !is_identifier(app))
        || !request
            .granted_apps
            .iter()
            .any(|app| app == default_app_key)
    {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    }
    let password_hash = hash_password(&request.password).map_err(ApiError::Auth)?;
    let mut transaction = state.pool.begin().await?;
    let user_id = Uuid::now_v7();
    let inserted = sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app
         ) VALUES ($1, $2, $3, 'viewer', 'user', $4)",
    )
    .bind(user_id)
    .bind(&request.username)
    .bind(password_hash)
    .bind(&request.default_app)
    .execute(&mut *transaction)
    .await
    .map_err(resource_share_database_error)?;
    if inserted.rows_affected() != 1 {
        return Err(ApiError::Conflict("username already exists".to_owned()));
    }
    for app_key in &request.granted_apps {
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, app_key)
             VALUES ($1, $2)",
        )
        .bind(user_id)
        .bind(app_key)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    let user = list_management_users_query(&state.pool)
        .await?
        .into_iter()
        .find(|user| user.id == user_id)
        .ok_or(ApiError::StorageData)?;
    Ok((StatusCode::CREATED, Json(user)))
}

#[utoipa::path(
    put,
    path = "/api/management/users/{username}",
    params(("username" = String, Path, description = "Username")),
    request_body = serde_json::Value,
    responses((status = 200, description = "Updated user app grants")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn update_management_user(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(username): Path<String>,
    Json(request): Json<UpdateManagementUserRequest>,
) -> Result<Json<ManagementUser>, ApiError> {
    let Some(default_app_key) = app_key_from_path(&request.default_app) else {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    };
    if !is_identifier(&username)
        || request.granted_apps.is_empty()
        || request.granted_apps.iter().any(|app| !is_identifier(app))
        || !request
            .granted_apps
            .iter()
            .any(|app| app == default_app_key)
    {
        return Err(ApiError::BadRequest("invalid user app access".to_owned()));
    }
    if request
        .role
        .as_deref()
        .is_some_and(|role| !matches!(role, "admin" | "viewer"))
    {
        return Err(ApiError::BadRequest("invalid user role".to_owned()));
    }
    let mut transaction = state.pool.begin().await?;
    if request.role.as_deref() == Some("viewer") {
        let remaining_admins: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users WHERE role = 'admin' AND username <> $1",
        )
        .bind(&username)
        .fetch_one(&mut *transaction)
        .await?;
        if remaining_admins == 0 {
            return Err(ApiError::Conflict(
                "at least one administrator must remain".to_owned(),
            ));
        }
    }
    let user_id = sqlx::query(
        "UPDATE users
         SET default_app = $2, role = COALESCE($3, role), updated_at = now()
         WHERE username = $1
         RETURNING id",
    )
    .bind(&username)
    .bind(&request.default_app)
    .bind(&request.role)
    .fetch_optional(&mut *transaction)
    .await?
    .map(|row| row.try_get::<Uuid, _>("id"))
    .transpose()?
    .ok_or(ApiError::NotFound("user"))?;
    sqlx::query("DELETE FROM user_app_grants WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *transaction)
        .await?;
    for app_key in &request.granted_apps {
        sqlx::query(
            "INSERT INTO user_app_grants (user_id, app_key)
             VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(user_id)
        .bind(app_key)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    state.revoke_sessions_for_username(&username);

    list_management_users_query(&state.pool)
        .await?
        .into_iter()
        .find(|user| user.username == username)
        .map(Json)
        .ok_or(ApiError::NotFound("user"))
}

#[utoipa::path(
    get,
    path = "/api/management/profiles/device-profiles",
    responses((status = 200, description = "Device profiles")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn list_management_device_profiles(
    State(state): State<ApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementDeviceProfile>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, telemetry_schema, metric_mapping, reporting_settings
         FROM device_profiles
         ORDER BY name, id",
    )
    .fetch_all(&state.pool)
    .await?;
    rows.into_iter()
        .map(management_device_profile_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

#[utoipa::path(
    post,
    path = "/api/management/profiles/device-profiles",
    request_body = serde_json::Value,
    responses((status = 201, description = "Created device profile")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn create_management_device_profile(
    State(state): State<ApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementDeviceProfileRequest>,
) -> Result<(StatusCode, Json<ManagementDeviceProfile>), ApiError> {
    let name = validated_name(&request.name, "device profile name")?.to_owned();
    let telemetry_schema = object_value(request.telemetry_schema, "telemetry schema")?;
    let metric_mapping = object_value(request.metric_mapping, "metric mapping")?;
    let reporting_settings = object_value(request.reporting_settings, "reporting settings")?;
    let row = sqlx::query(
        "INSERT INTO device_profiles (
            id, name, telemetry_schema, metric_mapping, reporting_settings
         ) VALUES ($1, $2, $3, $4, $5)
         RETURNING id, name, telemetry_schema, metric_mapping, reporting_settings",
    )
    .bind(Uuid::now_v7())
    .bind(name)
    .bind(sqlx::types::Json(telemetry_schema))
    .bind(sqlx::types::Json(metric_mapping))
    .bind(sqlx::types::Json(reporting_settings))
    .fetch_one(&state.pool)
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(management_device_profile_from_row(row)?),
    ))
}

#[utoipa::path(
    put,
    path = "/api/management/profiles/device-profiles/{id}",
    params(("id" = Uuid, Path, description = "Device profile ID")),
    request_body = serde_json::Value,
    responses((status = 200, description = "Updated device profile")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn update_management_device_profile(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateManagementDeviceProfileRequest>,
) -> Result<Json<ManagementDeviceProfile>, ApiError> {
    let name = validated_name(&request.name, "device profile name")?.to_owned();
    let telemetry_schema = object_value(request.telemetry_schema, "telemetry schema")?;
    let metric_mapping = object_value(request.metric_mapping, "metric mapping")?;
    let reporting_settings = object_value(request.reporting_settings, "reporting settings")?;
    let row = sqlx::query(
        "UPDATE device_profiles
         SET name = $2, telemetry_schema = $3, metric_mapping = $4,
             reporting_settings = $5, updated_at = now()
         WHERE id = $1
         RETURNING id, name, telemetry_schema, metric_mapping, reporting_settings",
    )
    .bind(id)
    .bind(name)
    .bind(sqlx::types::Json(telemetry_schema))
    .bind(sqlx::types::Json(metric_mapping))
    .bind(sqlx::types::Json(reporting_settings))
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("device profile"))?;
    management_device_profile_from_row(row).map(Json)
}

#[utoipa::path(
    delete,
    path = "/api/management/profiles/device-profiles/{id}",
    params(("id" = Uuid, Path, description = "Device profile ID")),
    responses((status = 204, description = "Deleted device profile")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn delete_management_device_profile(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sqlx::query("DELETE FROM device_profiles WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();
    if deleted == 0 {
        Err(ApiError::NotFound("device profile"))
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

#[utoipa::path(
    get,
    path = "/api/management/profiles/asset-profiles",
    responses((status = 200, description = "Asset profiles")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn list_management_asset_profiles(
    State(state): State<ApiState>,
    _admin: Admin,
) -> Result<Json<Vec<ManagementAssetProfile>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, fields, dashboard_defaults
         FROM asset_profiles
         ORDER BY name, id",
    )
    .fetch_all(&state.pool)
    .await?;
    rows.into_iter()
        .map(management_asset_profile_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Json)
}

#[utoipa::path(
    post,
    path = "/api/management/profiles/asset-profiles",
    request_body = serde_json::Value,
    responses((status = 201, description = "Created asset profile")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn create_management_asset_profile(
    State(state): State<ApiState>,
    _admin: Admin,
    Json(request): Json<CreateManagementAssetProfileRequest>,
) -> Result<(StatusCode, Json<ManagementAssetProfile>), ApiError> {
    let name = validated_name(&request.name, "asset profile name")?.to_owned();
    let fields = object_value(request.fields, "asset fields")?;
    let dashboard_defaults = object_value(request.dashboard_defaults, "dashboard defaults")?;
    let row = sqlx::query(
        "INSERT INTO asset_profiles (id, name, fields, dashboard_defaults)
         VALUES ($1, $2, $3, $4)
         RETURNING id, name, fields, dashboard_defaults",
    )
    .bind(Uuid::now_v7())
    .bind(name)
    .bind(sqlx::types::Json(fields))
    .bind(sqlx::types::Json(dashboard_defaults))
    .fetch_one(&state.pool)
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(management_asset_profile_from_row(row)?),
    ))
}

#[utoipa::path(
    put,
    path = "/api/management/profiles/asset-profiles/{id}",
    params(("id" = Uuid, Path, description = "Asset profile ID")),
    request_body = serde_json::Value,
    responses((status = 200, description = "Updated asset profile")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn update_management_asset_profile(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateManagementAssetProfileRequest>,
) -> Result<Json<ManagementAssetProfile>, ApiError> {
    let name = validated_name(&request.name, "asset profile name")?.to_owned();
    let fields = object_value(request.fields, "asset fields")?;
    let dashboard_defaults = object_value(request.dashboard_defaults, "dashboard defaults")?;
    let row = sqlx::query(
        "UPDATE asset_profiles
         SET name = $2, fields = $3, dashboard_defaults = $4, updated_at = now()
         WHERE id = $1
         RETURNING id, name, fields, dashboard_defaults",
    )
    .bind(id)
    .bind(name)
    .bind(sqlx::types::Json(fields))
    .bind(sqlx::types::Json(dashboard_defaults))
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("asset profile"))?;
    management_asset_profile_from_row(row).map(Json)
}

#[utoipa::path(
    delete,
    path = "/api/management/profiles/asset-profiles/{id}",
    params(("id" = Uuid, Path, description = "Asset profile ID")),
    responses((status = 204, description = "Deleted asset profile")),
    security(("sessionAuth" = [])),
    tag = "Management"
)]
async fn delete_management_asset_profile(
    State(state): State<ApiState>,
    _admin: Admin,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sqlx::query("DELETE FROM asset_profiles WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?
        .rows_affected();
    if deleted == 0 {
        Err(ApiError::NotFound("asset profile"))
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

#[utoipa::path(
    get,
    path = "/api/apps/powermonitor/summary",
    responses((status = 200, description = "Power Monitor summary", body = PowerSummary)),
    security(("sessionAuth" = [])),
    tag = "Power Monitor"
)]
async fn powermonitor_summary(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<crate::powermonitor::PowerSummary>, ApiError> {
    require_powermonitor(&context)?;
    let devices = list_power_devices(&state.pool, Utc::now() - Duration::seconds(30))
        .await
        .map_err(ApiError::from)?;
    let mut device_count = 0_i64;
    let mut online_device_count = 0_i64;
    let mut total_power_w = 0.0_f64;
    let mut total_energy_kwh = 0.0_f64;
    for device in devices {
        if device_permission(&state.pool, &context, &device.device_id)
            .await?
            .is_none()
        {
            continue;
        }
        device_count += 1;
        if device.online {
            online_device_count += 1;
        }
        total_power_w += device.power_w.unwrap_or(0.0);
        total_energy_kwh += device.energy_kwh.unwrap_or(0.0);
    }
    let assets = list_power_assets(&state.pool)
        .await
        .map_err(ApiError::from)?;
    let mut asset_count = 0_i64;
    for asset in assets {
        if asset_permission(&state.pool, &context, asset.id)
            .await?
            .is_some()
        {
            asset_count += 1;
        }
    }
    Ok(Json(PowerSummary {
        device_count,
        online_device_count,
        asset_count,
        total_power_w,
        total_energy_kwh,
    }))
}

#[utoipa::path(
    get,
    path = "/api/apps/powermonitor/assets",
    responses((status = 200, description = "Power Monitor asset hierarchy", body = [PowerAsset])),
    security(("sessionAuth" = [])),
    tag = "Power Monitor"
)]
async fn powermonitor_assets(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<crate::powermonitor::PowerAsset>>, ApiError> {
    require_powermonitor(&context)?;
    let assets = list_power_assets(&state.pool)
        .await
        .map_err(ApiError::from)?;
    let mut visible = Vec::new();
    for mut asset in assets {
        if let Some(permission) = asset_permission(&state.pool, &context, asset.id).await?
            && permission.allows(ResourcePermission::Viewer)
        {
            asset.permission = permission.as_str().to_owned();
            visible.push(asset);
        }
    }
    Ok(Json(visible))
}

#[utoipa::path(
    get,
    path = "/api/apps/powermonitor/devices",
    responses((status = 200, description = "Power Monitor devices", body = [PowerDevice])),
    security(("sessionAuth" = [])),
    tag = "Power Monitor"
)]
async fn powermonitor_devices(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<crate::powermonitor::PowerDevice>>, ApiError> {
    require_powermonitor(&context)?;
    let devices = list_power_devices(&state.pool, Utc::now() - Duration::seconds(30))
        .await
        .map_err(ApiError::from)?;
    let mut visible = Vec::new();
    for mut device in devices {
        let Some(permission) = device_permission(&state.pool, &context, &device.device_id).await?
        else {
            continue;
        };
        device.permission = permission.as_str().to_owned();
        visible.push(device);
    }
    Ok(Json(visible))
}

#[utoipa::path(
    get,
    path = "/api/apps/powermonitor/devices/{device_id}/telemetry",
    params(
        ("device_id" = String, Path, description = "Platform device ID"),
        ("from" = String, Query, description = "Inclusive UTC timestamp"),
        ("to" = String, Query, description = "Inclusive UTC timestamp"),
        ("bucket" = Option<String>, Query, description = "raw, 5m, or 1h")
    ),
    responses((status = 200, description = "Bucketed Power Monitor telemetry", body = [PowerTelemetryPoint])),
    security(("sessionAuth" = [])),
    tag = "Power Monitor"
)]
async fn powermonitor_device_telemetry(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Query(query): Query<PowerTelemetryQuery>,
) -> Result<Json<Vec<PowerTelemetryPoint>>, ApiError> {
    require_powermonitor(&context)?;
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    let bucket = query.validate()?;
    power_device_telemetry(&state.pool, &device_id, query.from, query.to, bucket)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[utoipa::path(
    get,
    path = "/api/apps/powermonitor/devices/{device_id}/telemetry/records",
    params(
        ("device_id" = String, Path, description = "Platform device ID"),
        ("from" = String, Query, description = "Inclusive UTC timestamp"),
        ("to" = String, Query, description = "Inclusive UTC timestamp")
    ),
    responses((status = 200, description = "Raw telemetry records with dynamic measurements", body = [PowerTelemetryRecord])),
    security(("sessionAuth" = [])),
    tag = "Power Monitor"
)]
async fn powermonitor_device_telemetry_records(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Query(query): Query<PowerTelemetryQuery>,
) -> Result<Json<Vec<PowerTelemetryRecord>>, ApiError> {
    require_powermonitor(&context)?;
    if !is_identifier(&device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    query.validate()?;
    power_device_telemetry_records(&state.pool, &device_id, query.from, query.to)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

#[utoipa::path(
    get,
    path = "/api/apps/powermonitor/assets/{asset_id}/telemetry",
    params(
        ("asset_id" = Uuid, Path, description = "Asset ID"),
        ("from" = String, Query, description = "Inclusive UTC timestamp"),
        ("to" = String, Query, description = "Inclusive UTC timestamp"),
        ("bucket" = Option<String>, Query, description = "raw, 5m, or 1h")
    ),
    responses((status = 200, description = "Aggregated asset telemetry", body = [PowerTelemetryPoint])),
    security(("sessionAuth" = [])),
    tag = "Power Monitor"
)]
async fn powermonitor_asset_telemetry(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(asset_id): Path<Uuid>,
    Query(query): Query<PowerTelemetryQuery>,
) -> Result<Json<Vec<PowerTelemetryPoint>>, ApiError> {
    require_powermonitor(&context)?;
    if !asset_permission(&state.pool, &context, asset_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    let bucket = query.validate()?;
    power_asset_telemetry(&state.pool, asset_id, query.from, query.to, bucket)
        .await
        .map(Json)
        .map_err(ApiError::from)
}

fn system_configuration_error(error: SystemConfigurationServiceError) -> ApiError {
    match error {
        SystemConfigurationServiceError::Validation(message) => ApiError::BadRequest(message),
        error => ApiError::SystemConfiguration(error),
    }
}

async fn mqttd_device_transport_session_resolution(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<MqttdDeviceTransportSessionResolutionRequest>,
) -> Result<Json<MqttdDeviceTransportSessionResolution>, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    request.validate_client_id()?;
    if request
        .password
        .as_deref()
        .is_some_and(|password| !password.is_empty())
    {
        return Err(ApiError::Unauthorized);
    }
    let device = resolve_active_device_token(&state.pool, &request.username)
        .await
        .map_err(mqttd_device_token_error)?;
    if device.gateway_device_id.is_some() {
        return Err(ApiError::Unauthorized);
    }
    let token_prefix =
        device_token_prefix(&request.username).map_err(|_| ApiError::Unauthorized)?;
    let token_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT device_tokens.id
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.token_prefix = $1
           AND device_tokens.device_id = $2
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL",
    )
    .bind(token_prefix)
    .bind(&device.device_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::Unauthorized)?;

    Ok(Json(MqttdDeviceTransportSessionResolution {
        device_id: device.device_id,
        token_id,
        is_gateway: device.is_gateway,
    }))
}

async fn mqttd_device_transport_session_authorization(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<MqttdDeviceTransportSessionAuthorizationRequest>,
) -> Result<StatusCode, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    let authorized = sqlx::query_scalar::<_, i32>(
        "SELECT 1
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.id = $1
           AND device_tokens.device_id = $2
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL
           AND devices.gateway_device_id IS NULL",
    )
    .bind(request.token_id)
    .bind(&request.device_id)
    .fetch_optional(&state.pool)
    .await?
    .is_some();
    if !authorized {
        return Err(ApiError::Unauthorized);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn mqttd_gateway_authorization(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<GatewayAuthorizationRequest>,
) -> Result<Json<GatewayAuthorizationResponse>, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    validate_gateway_authorization_request(&request)?;
    let authorized = sqlx::query_scalar::<_, i32>(
        "SELECT 1
         FROM device_tokens
         JOIN devices AS gateway ON gateway.device_id = device_tokens.device_id
         WHERE device_tokens.id = $1
           AND gateway.device_id = $2
           AND gateway.is_gateway = TRUE
           AND device_tokens.revoked_at IS NULL
           AND gateway.deleted_at IS NULL
           AND (
             $3::TEXT IS NULL OR EXISTS (
               SELECT 1
               FROM devices AS child
               WHERE child.device_id = $3
                 AND child.gateway_device_id = gateway.device_id
                 AND child.deleted_at IS NULL
             )
           )",
    )
    .bind(request.token_id)
    .bind(&request.gateway_device_id)
    .bind(&request.child_device_id)
    .fetch_optional(&state.pool)
    .await?
    .is_some();
    if !authorized {
        return Err(ApiError::Unauthorized);
    }
    Ok(Json(GatewayAuthorizationResponse::from(request)))
}

async fn mqttd_device_transport_rpc_response(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<MqttdDeviceTransportRpcResponseRequest>,
) -> Result<StatusCode, ApiError> {
    state.require_mqttd_device_transport_secret(&headers)?;
    let now = Utc::now();
    if let Some(facade) = &state.core_facade {
        let authorized = sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM device_tokens
             WHERE id = $1 AND device_id = $2 AND revoked_at IS NULL",
        )
        .bind(request.token_id)
        .bind(&request.device_id)
        .fetch_optional(&state.pool)
        .await?
        .is_some();
        if !authorized {
            return Err(ApiError::Unauthorized);
        }
        facade
            .record_command_response(CoreCommandResponseRequest {
                command_id: request.command_id,
                device_id: request.device_id,
                response: request.response,
                responded_at: now,
            })
            .await
            .map_err(core_facade_command_error)?;
        return Ok(StatusCode::NO_CONTENT);
    }
    let response = sqlx::types::Json(request.response);
    let recorded = sqlx::query(
        "UPDATE command_outbox AS command
         SET state = 'responded',
             response = $1,
             responded_at = $2,
             lease_until = NULL
         WHERE command.id = $3
           AND command.device_id = $4
           AND command.mode = 'two_way'
           AND command.state = 'published_to_broker'
           AND command.expires_at > $2
           AND EXISTS (
                SELECT 1
                FROM device_tokens
                WHERE id = $5
                  AND device_id = command.device_id
                  AND revoked_at IS NULL
           )",
    )
    .bind(response)
    .bind(now)
    .bind(request.command_id)
    .bind(&request.device_id)
    .bind(request.token_id)
    .execute(&state.pool)
    .await?
    .rows_affected();
    if recorded == 1 {
        return Ok(StatusCode::NO_CONTENT);
    }

    let expired = sqlx::query(
        "UPDATE command_outbox AS command
         SET state = 'expired',
             lease_until = NULL
         WHERE command.id = $1
           AND command.device_id = $2
           AND command.mode = 'two_way'
           AND command.state = 'published_to_broker'
           AND command.expires_at <= $3
           AND EXISTS (
                SELECT 1
                FROM device_tokens
                WHERE id = $4
                  AND device_id = command.device_id
                  AND revoked_at IS NULL
           )",
    )
    .bind(request.command_id)
    .bind(&request.device_id)
    .bind(now)
    .bind(request.token_id)
    .execute(&state.pool)
    .await?
    .rows_affected();
    if expired == 1 {
        return Ok(StatusCode::NO_CONTENT);
    }

    let idempotent = sqlx::query_scalar::<_, i32>(
        "SELECT 1
         FROM command_outbox AS command
         WHERE command.id = $1
           AND command.device_id = $2
           AND command.mode = 'two_way'
           AND command.state = 'responded'
           AND EXISTS (
                SELECT 1
                FROM device_tokens
                WHERE id = $3
                  AND device_id = command.device_id
                  AND revoked_at IS NULL
           )",
    )
    .bind(request.command_id)
    .bind(&request.device_id)
    .bind(request.token_id)
    .fetch_optional(&state.pool)
    .await?
    .is_some();
    if idempotent {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Conflict(
            "RPC response does not match an active two-way command".to_owned(),
        ))
    }
}

async fn authenticate_request(
    State(state): State<ApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    let session_id = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Session "))
        .filter(|session_id| !session_id.is_empty());
    let Some(session_id) = session_id else {
        return ApiError::Unauthorized.into_response();
    };
    let Some(context) = state.authenticate_session(session_id) else {
        return ApiError::Unauthorized.into_response();
    };
    request.extensions_mut().insert(context);
    next.run(request).await
}

async fn authenticate_sqlite_request(
    State(state): State<SqliteApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    let session_id = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Session "))
        .filter(|session_id| !session_id.is_empty());
    let Some(session_id) = session_id else {
        return ApiError::Unauthorized.into_response();
    };
    let Some(context) = state.authenticate_session(session_id) else {
        return ApiError::Unauthorized.into_response();
    };
    request.extensions_mut().insert(context);
    next.run(request).await
}

fn device_token_error(error: DeviceTokenStoreError) -> ApiError {
    match error {
        DeviceTokenStoreError::Database(error) => ApiError::Database(error),
        DeviceTokenStoreError::NotFound => ApiError::NotFound("device token"),
        DeviceTokenStoreError::GatewayChild => {
            ApiError::Conflict("gateway child devices cannot have MQTT tokens".to_owned())
        }
        other @ (DeviceTokenStoreError::Token(_)
        | DeviceTokenStoreError::Vault(_)
        | DeviceTokenStoreError::AllocationFailed) => ApiError::DeviceToken(other),
    }
}

fn mqttd_device_token_error(error: DeviceTokenStoreError) -> ApiError {
    match error {
        DeviceTokenStoreError::Database(error) => ApiError::Database(error),
        DeviceTokenStoreError::NotFound | DeviceTokenStoreError::Token(_) => ApiError::Unauthorized,
        DeviceTokenStoreError::GatewayChild => ApiError::Unauthorized,
        error @ (DeviceTokenStoreError::Vault(_) | DeviceTokenStoreError::AllocationFailed) => {
            ApiError::DeviceToken(error)
        }
    }
}

fn sqlite_store_error(error: SqliteStoreError) -> ApiError {
    match error {
        SqliteStoreError::Database(error) => ApiError::Database(error),
        _ => ApiError::StorageData,
    }
}

#[utoipa::path(
    get,
    path = "/api/devices",
    responses((status = 200, description = "Device summaries")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn list_devices(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<DeviceSummary>>, ApiError> {
    let rows = sqlx::query(
        "SELECT device_id, display_name, last_seen_at
         FROM devices
         WHERE deleted_at IS NULL
         ORDER BY device_id",
    )
    .fetch_all(&state.pool)
    .await?;
    let online_threshold = Utc::now() - Duration::minutes(5);
    let mut devices = Vec::new();
    for row in rows {
        let device_id: String = row.try_get("device_id")?;
        if device_permission(&state.pool, &context, &device_id)
            .await?
            .is_none()
        {
            continue;
        }
        let last_seen_at = row.try_get::<Option<DateTime<Utc>>, _>("last_seen_at")?;
        devices.push(DeviceSummary {
            device_id,
            display_name: row.try_get("display_name")?,
            online: last_seen_at.is_some_and(|value| value >= online_threshold),
            last_seen_at,
        });
    }

    Ok(Json(devices))
}

#[utoipa::path(
    get,
    path = "/api/devices/{device_id}/telemetry",
    params(
        ("device_id" = String, Path, description = "Platform device ID"),
        ("from" = String, Query, description = "Inclusive UTC timestamp"),
        ("to" = String, Query, description = "Inclusive UTC timestamp"),
        ("bucket" = Option<String>, Query, description = "raw, 5m, or 1h")
    ),
    responses((status = 200, description = "Generic device telemetry")),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn device_telemetry(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Query(query): Query<TelemetryQuery>,
) -> Result<Json<Vec<TelemetryPoint>>, ApiError> {
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }
    if query.from >= query.to {
        return Err(ApiError::BadRequest(
            "`from` must be earlier than `to`".to_owned(),
        ));
    }

    let bucket = query.bucket.unwrap_or(TelemetryBucket::FiveMinutes);
    let points = if let Some(facade) = &state.core_facade {
        facade
            .telemetry(CoreTelemetryQuery {
                device_id: device_id.clone(),
                from: query.from,
                to: query.to,
                bucket: match bucket {
                    TelemetryBucket::Raw => CoreTelemetryBucket::Raw,
                    TelemetryBucket::FiveMinutes => CoreTelemetryBucket::FiveMinutes,
                    TelemetryBucket::OneHour => CoreTelemetryBucket::OneHour,
                },
            })
            .await
            .map_err(core_facade_telemetry_error)?
            .into_iter()
            .map(|point| TelemetryPoint {
                at: point.at,
                temperature_c: point.temperature_c,
                humidity_pct: point.humidity_pct,
                event_count: point.event_count,
            })
            .collect()
    } else {
        match bucket {
            TelemetryBucket::Raw => {
                raw_points(&state.pool, &device_id, query.from, query.to).await?
            }
            TelemetryBucket::FiveMinutes => {
                aggregate_points(
                    &state.pool,
                    "telemetry_5m",
                    &device_id,
                    query.from,
                    query.to,
                )
                .await?
            }
            TelemetryBucket::OneHour => {
                aggregate_points(
                    &state.pool,
                    "telemetry_1h",
                    &device_id,
                    query.from,
                    query.to,
                )
                .await?
            }
        }
    };

    Ok(Json(points))
}

#[utoipa::path(
    post,
    path = "/api/devices/{device_id}/commands",
    params(("device_id" = String, Path, description = "Platform device ID")),
    request_body = CommandRequest,
    responses((status = 202, description = "Command queued for durable dispatch", body = CommandLifecycleResponse)),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn send_command(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(device_id): Path<String>,
    Json(request): Json<CommandRequest>,
) -> Result<(StatusCode, Json<CommandLifecycleResponse>), ApiError> {
    let access = device_permission(&state.pool, &context, &device_id).await?;
    if !access.is_some_and(|access| access.allows(ResourcePermission::Controller)) {
        return Err(ApiError::Forbidden);
    }
    let target = resolve_command_target(&state, &device_id).await?;
    let command = new_rpc_request(request)?;
    let command_id = command.id;
    let expires_at = command.expires_at;
    let issued_at = command.issued_at;
    let (device_id, method, params, mode) = target.into_command_parts(command);
    if let Some(facade) = &state.core_facade {
        let record = facade
            .create_command(CoreCommandCreateRequest {
                id: command_id,
                device_id,
                method,
                params,
                mode,
                issued_at,
                expires_at,
            })
            .await
            .map_err(|_| ApiError::CoreCommandUnavailable)?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(core_command_lifecycle_response(record)?),
        ));
    }
    let row = sqlx::query(
        "INSERT INTO command_outbox (
            id, device_id, method, params, mode, expires_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, state, expires_at, mode, response, responded_at",
    )
    .bind(command_id)
    .bind(device_id)
    .bind(method)
    .bind(sqlx::types::Json(params))
    .bind(rpc_mode_value(mode))
    .bind(expires_at)
    .bind(issued_at)
    .fetch_one(&state.pool)
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(command_lifecycle_response(
            row.try_get("id")?,
            &row.try_get::<String, _>("state")?,
            row.try_get("expires_at")?,
            &row.try_get::<String, _>("mode")?,
            row.try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("response")?
                .map(|value| value.0),
            row.try_get("responded_at")?,
        )?),
    ))
}

#[utoipa::path(
    get,
    path = "/api/device-commands/{id}",
    params(("id" = Uuid, Path, description = "Server-generated command ID")),
    responses((status = 200, description = "Command lifecycle state", body = CommandLifecycleResponse)),
    security(("sessionAuth" = [])),
    tag = "Devices"
)]
async fn get_device_command(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<Json<CommandLifecycleResponse>, ApiError> {
    if let Some(facade) = &state.core_facade {
        let record = facade
            .get_command(id)
            .await
            .map_err(core_facade_command_error)?;
        if !device_permission(&state.pool, &context, &record.device_id)
            .await?
            .is_some_and(|access| access.allows(ResourcePermission::Viewer))
        {
            return Err(ApiError::Forbidden);
        }
        return core_command_lifecycle_response(record).map(Json);
    }
    let row = sqlx::query(
        "SELECT id, device_id, state, expires_at, mode, response, responded_at
         FROM command_outbox
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("device command"))?;
    let device_id: String = row.try_get("device_id")?;
    if !device_permission(&state.pool, &context, &device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Viewer))
    {
        return Err(ApiError::Forbidden);
    }

    Ok(Json(command_lifecycle_response(
        row.try_get("id")?,
        &row.try_get::<String, _>("state")?,
        row.try_get("expires_at")?,
        &row.try_get::<String, _>("mode")?,
        row.try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("response")?
            .map(|value| value.0),
        row.try_get("responded_at")?,
    )?))
}

async fn resolve_command_target(
    state: &ApiState,
    device_id: &str,
) -> Result<CommandTarget, ApiError> {
    if !is_identifier(device_id) {
        return Err(ApiError::BadRequest("invalid device ID".to_owned()));
    }
    let target = sqlx::query(
        "SELECT child.gateway_device_id,
                gateway.device_id AS active_gateway_device_id,
                gateway.is_gateway AS active_gateway_is_gateway
         FROM devices AS child
         LEFT JOIN devices AS gateway
           ON gateway.device_id = child.gateway_device_id
          AND gateway.deleted_at IS NULL
         WHERE child.device_id = $1 AND child.deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("device"))?;
    if target
        .try_get::<Option<String>, _>("gateway_device_id")?
        .is_none()
    {
        return Ok(CommandTarget::direct(device_id));
    }
    let active_gateway_device_id =
        target.try_get::<Option<String>, _>("active_gateway_device_id")?;
    let active_gateway_is_gateway =
        target.try_get::<Option<bool>, _>("active_gateway_is_gateway")?;

    let Some(active_gateway_device_id) = active_gateway_device_id else {
        return Err(ApiError::Conflict(
            "assigned gateway device is unavailable".to_owned(),
        ));
    };
    if active_gateway_is_gateway != Some(true) {
        return Err(ApiError::Conflict(
            "assigned gateway device is not a gateway".to_owned(),
        ));
    }
    Ok(CommandTarget::gateway_child(
        active_gateway_device_id,
        device_id,
    ))
}

#[utoipa::path(
    get,
    path = "/api/alert-rules",
    responses((status = 200, description = "Alert rules")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn list_alert_rules(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<AlertRuleResponse>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds, created_at, updated_at
         FROM alert_rules
         WHERE archived_at IS NULL
         ORDER BY created_at DESC, id",
    )
    .fetch_all(&state.pool)
    .await?;

    let rules = rows
        .into_iter()
        .map(alert_rule_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut visible = Vec::new();
    for rule in rules {
        if can_read_alert(&state.pool, &context, rule.device_id.as_deref()).await? {
            visible.push(rule);
        }
    }
    Ok(Json(visible))
}

#[utoipa::path(
    post,
    path = "/api/alert-rules",
    request_body = serde_json::Value,
    responses((status = 201, description = "Created alert rule")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn create_alert_rule(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Json(request): Json<CreateAlertRuleRequest>,
) -> Result<(StatusCode, Json<AlertRuleResponse>), ApiError> {
    let rule = request.validate()?;
    require_alert_manager(&state.pool, &context, rule.device_id.as_deref()).await?;
    let row = sqlx::query(
        "INSERT INTO alert_rules (
            id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
            window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
            hysteresis, severity, reminder_interval_seconds, created_at, updated_at
         ) VALUES (
            $1, $2, TRUE, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now(), now()
         )
         RETURNING id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                   window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                   hysteresis, severity, reminder_interval_seconds, created_at, updated_at",
    )
    .bind(Uuid::new_v4())
    .bind(rule.name)
    .bind(rule.device_id)
    .bind(rule.metric_key)
    .bind(rule.rule_type)
    .bind(rule.comparison)
    .bind(rule.threshold)
    .bind(rule.window_seconds)
    .bind(rule.for_seconds)
    .bind(rule.resolve_after_seconds)
    .bind(rule.reopen_grace_seconds)
    .bind(rule.hysteresis)
    .bind(rule.severity)
    .bind(rule.reminder_interval_seconds)
    .fetch_one(&state.pool)
    .await?;

    Ok((StatusCode::CREATED, Json(alert_rule_from_row(row)?)))
}

#[utoipa::path(
    put,
    path = "/api/alert-rules/{id}",
    params(("id" = Uuid, Path, description = "Alert rule ID")),
    request_body = serde_json::Value,
    responses((status = 200, description = "Updated alert rule")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn update_alert_rule(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateAlertRuleRequest>,
) -> Result<Json<AlertRuleResponse>, ApiError> {
    let current_device_id = alert_rule_device(&state.pool, id).await?;
    require_alert_manager(&state.pool, &context, current_device_id.as_deref()).await?;
    let rule = request.validate()?;
    require_alert_manager(&state.pool, &context, rule.device_id.as_deref()).await?;
    let row = sqlx::query(
        "UPDATE alert_rules
         SET name = $2, device_id = $3, metric_key = $4, rule_type = $5, comparison = $6,
             threshold = $7, window_seconds = $8, for_seconds = $9,
             resolve_after_seconds = $10, reopen_grace_seconds = $11, hysteresis = $12,
             severity = $13, reminder_interval_seconds = $14, updated_at = now()
         WHERE id = $1 AND archived_at IS NULL
         RETURNING id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                   window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                   hysteresis, severity, reminder_interval_seconds, created_at, updated_at",
    )
    .bind(id)
    .bind(rule.name)
    .bind(rule.device_id)
    .bind(rule.metric_key)
    .bind(rule.rule_type)
    .bind(rule.comparison)
    .bind(rule.threshold)
    .bind(rule.window_seconds)
    .bind(rule.for_seconds)
    .bind(rule.resolve_after_seconds)
    .bind(rule.reopen_grace_seconds)
    .bind(rule.hysteresis)
    .bind(rule.severity)
    .bind(rule.reminder_interval_seconds)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("alert rule not found"))?;

    Ok(Json(alert_rule_from_row(row)?))
}

#[utoipa::path(
    delete,
    path = "/api/alert-rules/{id}",
    params(("id" = Uuid, Path, description = "Alert rule ID")),
    responses((status = 204, description = "Archived alert rule")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn archive_alert_rule(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let device_id = alert_rule_device(&state.pool, id).await?;
    require_alert_manager(&state.pool, &context, device_id.as_deref()).await?;
    let archived_at = Utc::now();
    let mut transaction = state.pool.begin().await?;
    let rule = sqlx::query(
        "SELECT name, metric_key, threshold, severity
         FROM alert_rules
         WHERE id = $1 AND archived_at IS NULL
         FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ApiError::NotFound("alert rule not found"))?;
    let name: String = rule.try_get("name")?;
    let metric_key: String = rule.try_get("metric_key")?;
    let threshold: f64 = rule.try_get("threshold")?;
    let severity: String = rule.try_get("severity")?;

    sqlx::query(
        "UPDATE alert_rules
         SET enabled = FALSE, archived_at = $2, updated_at = $2
         WHERE id = $1",
    )
    .bind(id)
    .bind(archived_at)
    .execute(&mut *transaction)
    .await?;

    let incidents = sqlx::query(
        "UPDATE alert_incidents
         SET status = 'resolved', recovery_started_at = $2, resolved_at = $2,
             last_notified_at = $2, state_version = state_version + 1, updated_at = $2
         WHERE rule_id = $1 AND status IN ('pending', 'open')
         RETURNING id, device_id, last_value, state_version",
    )
    .bind(id)
    .bind(archived_at)
    .fetch_all(&mut *transaction)
    .await?;

    for incident in incidents {
        let incident_id: Uuid = incident.try_get("id")?;
        let device_id: String = incident.try_get("device_id")?;
        let value: Option<f64> = incident.try_get("last_value")?;
        let state_version: i32 = incident.try_get("state_version")?;
        let value = value
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "unavailable".to_owned());
        let dedupe_key = format!("incident:{incident_id}:resolved:{state_version}");
        let subject = format!("[{severity}] {name} resolved");
        let body = format!(
            "Rule: {name}\nDevice: {device_id}\nMetric: {metric_key}\nValue: {value}\nThreshold: {threshold:.3}\nState: resolved\n"
        );
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
             ) VALUES ($1, $2, 'resolved', $3, $4, $5, $6, $6)
             ON CONFLICT (dedupe_key) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(incident_id)
        .bind(dedupe_key)
        .bind(subject)
        .bind(body)
        .bind(archived_at)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/api/alert-rules/{id}/toggle",
    params(("id" = Uuid, Path, description = "Alert rule ID")),
    responses((status = 200, description = "Toggled alert rule")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn toggle_alert_rule(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
    Json(request): Json<ToggleAlertRuleRequest>,
) -> Result<Json<AlertRuleResponse>, ApiError> {
    let device_id = alert_rule_device(&state.pool, id).await?;
    require_alert_manager(&state.pool, &context, device_id.as_deref()).await?;
    let row = sqlx::query(
        "UPDATE alert_rules
         SET enabled = $2, updated_at = now()
         WHERE id = $1 AND archived_at IS NULL
         RETURNING id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                   window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                   hysteresis, severity, reminder_interval_seconds, created_at, updated_at",
    )
    .bind(id)
    .bind(request.enabled)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("alert rule not found"))?;
    Ok(Json(alert_rule_from_row(row)?))
}

#[utoipa::path(
    get,
    path = "/api/alert-incidents",
    responses((status = 200, description = "Alert incidents")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn list_alert_incidents(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
) -> Result<Json<Vec<AlertIncidentResponse>>, ApiError> {
    let rows = sqlx::query(
        "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                incidents.device_id, incidents.status, incidents.condition_started_at,
                incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                incidents.acknowledged_by, incidents.last_value, incidents.updated_at
         FROM alert_incidents AS incidents
         JOIN alert_rules AS rules ON rules.id = incidents.rule_id
         ORDER BY
            CASE incidents.status WHEN 'open' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END,
            incidents.updated_at DESC
         LIMIT 100",
    )
    .fetch_all(&state.pool)
    .await?;
    let incidents = rows
        .into_iter()
        .map(alert_incident_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut visible = Vec::new();
    for incident in incidents {
        if can_read_alert(&state.pool, &context, Some(&incident.device_id)).await? {
            visible.push(incident);
        }
    }
    Ok(Json(visible))
}

#[utoipa::path(
    post,
    path = "/api/alert-incidents/{id}/acknowledge",
    params(("id" = Uuid, Path, description = "Alert incident ID")),
    responses((status = 200, description = "Acknowledged alert incident")),
    security(("sessionAuth" = [])),
    tag = "Alerts"
)]
async fn acknowledge_alert_incident(
    State(state): State<ApiState>,
    Extension(context): Extension<AuthContext>,
    Path(id): Path<Uuid>,
) -> Result<Json<AlertIncidentResponse>, ApiError> {
    let device_id = alert_incident_device(&state.pool, id).await?;
    require_alert_manager(&state.pool, &context, Some(&device_id)).await?;
    let row = sqlx::query(
        "UPDATE alert_incidents AS incidents
         SET acknowledged_at = now(), acknowledged_by = 'dashboard', updated_at = now()
         FROM alert_rules AS rules
         WHERE incidents.id = $1 AND rules.id = incidents.rule_id
         RETURNING incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                   incidents.device_id, incidents.status, incidents.condition_started_at,
                   incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                   incidents.acknowledged_by, incidents.last_value, incidents.updated_at",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::NotFound("alert incident not found"))?;
    Ok(Json(alert_incident_from_row(row)?))
}

async fn raw_points(
    pool: &PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<TelemetryPoint>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT
            event_at AS at,
            (measurements ->> 'temperature_c')::double precision AS temperature_c,
            (measurements ->> 'humidity_pct')::double precision AS humidity_pct
         FROM telemetry
         WHERE device_id = $1 AND event_at >= $2 AND event_at <= $3
         ORDER BY event_at",
    )
    .bind(device_id)
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(TelemetryPoint {
                at: row.try_get("at")?,
                temperature_c: row.try_get("temperature_c")?,
                humidity_pct: row.try_get("humidity_pct")?,
                event_count: 1,
            })
        })
        .collect()
}

async fn aggregate_points(
    pool: &PgPool,
    view_name: &str,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<TelemetryPoint>, ApiError> {
    let query = match view_name {
        "telemetry_5m" => {
            "SELECT bucket AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_5m
             WHERE device_id = $1 AND bucket >= $2 AND bucket <= $3
             ORDER BY bucket"
        }
        "telemetry_1h" => {
            "SELECT bucket AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_1h
             WHERE device_id = $1 AND bucket >= $2 AND bucket <= $3
             ORDER BY bucket"
        }
        _ => return Err(ApiError::BadRequest("unsupported bucket".to_owned())),
    };
    let rows = sqlx::query(query)
        .bind(device_id)
        .bind(from)
        .bind(to)
        .fetch_all(pool)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(TelemetryPoint {
                at: row.try_get("at")?,
                temperature_c: row.try_get("temperature_c")?,
                humidity_pct: row.try_get("humidity_pct")?,
                event_count: row.try_get("event_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(ApiError::from)
}

#[derive(Debug, Deserialize)]
struct TelemetryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    #[serde(default)]
    bucket: Option<TelemetryBucket>,
}

const COMMAND_TTL_SECONDS: i64 = 30;

#[derive(Debug, Deserialize, ToSchema)]
struct CommandRequest {
    #[serde(alias = "command")]
    method: String,
    #[serde(alias = "parameters")]
    params: serde_json::Value,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct CommandLifecycleResponse {
    id: Uuid,
    state: String,
    expires_at: DateTime<Utc>,
    mode: String,
    response: Option<serde_json::Value>,
    responded_at: Option<DateTime<Utc>>,
}

struct CommandTarget {
    dispatch_device_id: String,
    child_device_id: Option<String>,
}

impl CommandTarget {
    fn direct(device_id: impl Into<String>) -> Self {
        Self {
            dispatch_device_id: device_id.into(),
            child_device_id: None,
        }
    }

    fn gateway_child(
        dispatch_device_id: impl Into<String>,
        child_device_id: impl Into<String>,
    ) -> Self {
        Self {
            dispatch_device_id: dispatch_device_id.into(),
            child_device_id: Some(child_device_id.into()),
        }
    }

    fn into_command_parts(
        self,
        command: RpcRequest,
    ) -> (String, String, serde_json::Value, RpcMode) {
        let mode = command.mode;
        match self.child_device_id {
            Some(child_device_id) => (
                self.dispatch_device_id,
                "gateway_child_rpc".to_owned(),
                json!({
                    "child_device_id": child_device_id,
                    "method": command.method,
                    "params": command.params,
                }),
                mode,
            ),
            None => (
                self.dispatch_device_id,
                command.method,
                command.params,
                mode,
            ),
        }
    }
}

fn new_rpc_request(request: CommandRequest) -> Result<RpcRequest, ApiError> {
    let issued_at = Utc::now();
    let mode = match request.mode.as_deref().unwrap_or("one_way") {
        "one_way" => RpcMode::OneWay,
        "two_way" => RpcMode::TwoWay,
        _ => return Err(ApiError::BadRequest("unsupported RPC mode".to_owned())),
    };
    RpcRequest::with_mode(
        Uuid::now_v7(),
        request.method,
        request.params,
        issued_at,
        issued_at + Duration::seconds(COMMAND_TTL_SECONDS),
        mode,
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))
}

fn command_lifecycle_response(
    id: Uuid,
    state: &str,
    expires_at: DateTime<Utc>,
    mode: &str,
    response: Option<serde_json::Value>,
    responded_at: Option<DateTime<Utc>>,
) -> Result<CommandLifecycleResponse, ApiError> {
    let state = match state {
        "queued" | "leased" => "queued",
        "published_to_broker" => "published_to_broker",
        "responded" => "responded",
        "expired" => "expired",
        "failed" => "failed",
        _ => return Err(ApiError::StorageData),
    };
    if !matches!(mode, "one_way" | "two_way") {
        return Err(ApiError::StorageData);
    }
    Ok(CommandLifecycleResponse {
        id,
        state: state.to_owned(),
        expires_at,
        mode: mode.to_owned(),
        response,
        responded_at,
    })
}

fn core_command_lifecycle_response(
    record: CoreCommandRecord,
) -> Result<CommandLifecycleResponse, ApiError> {
    command_lifecycle_response(
        record.id,
        &record.state,
        record.expires_at,
        rpc_mode_value(record.mode),
        record.response,
        record.responded_at,
    )
}

fn core_facade_command_error(error: CoreFacadeError) -> ApiError {
    match error {
        CoreFacadeError::NotFound => ApiError::NotFound("device command"),
        CoreFacadeError::Rejected(409) => {
            ApiError::Conflict("core command operation conflicted".to_owned())
        }
        CoreFacadeError::Rejected(_) | CoreFacadeError::Unavailable => {
            ApiError::CoreCommandUnavailable
        }
    }
}

fn core_facade_telemetry_error(error: CoreFacadeError) -> ApiError {
    match error {
        CoreFacadeError::NotFound => ApiError::NotFound("device"),
        CoreFacadeError::Rejected(409) => {
            ApiError::Conflict("core telemetry query conflicted".to_owned())
        }
        CoreFacadeError::Rejected(_) | CoreFacadeError::Unavailable => {
            ApiError::CoreCommandUnavailable
        }
    }
}

fn sqlite_command_outbox_state(state: CommandOutboxState) -> &'static str {
    match state {
        CommandOutboxState::Queued => "queued",
        CommandOutboxState::Leased => "queued",
        CommandOutboxState::PublishedToBroker => "published_to_broker",
        CommandOutboxState::Responded => "responded",
        CommandOutboxState::Expired => "expired",
        CommandOutboxState::Failed => "failed",
    }
}

fn rpc_mode_value(mode: RpcMode) -> &'static str {
    match mode {
        RpcMode::OneWay => "one_way",
        RpcMode::TwoWay => "two_way",
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct MqttdDeviceTransportSessionResolutionRequest {
    client_id: String,
    username: String,
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MqttdDeviceTransportSessionAuthorizationRequest {
    device_id: String,
    token_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct GatewayAuthorizationRequest {
    gateway_device_id: String,
    token_id: Uuid,
    child_device_id: Option<String>,
    topic: String,
    event_kind: String,
}

#[derive(Debug, Serialize)]
struct GatewayAuthorizationResponse {
    gateway_device_id: String,
    token_id: Uuid,
    child_device_id: Option<String>,
    topic: String,
    event_kind: String,
}

impl From<GatewayAuthorizationRequest> for GatewayAuthorizationResponse {
    fn from(request: GatewayAuthorizationRequest) -> Self {
        Self {
            gateway_device_id: request.gateway_device_id,
            token_id: request.token_id,
            child_device_id: request.child_device_id,
            topic: request.topic,
            event_kind: request.event_kind,
        }
    }
}

fn validate_gateway_authorization_request(
    request: &GatewayAuthorizationRequest,
) -> Result<(), ApiError> {
    let valid = match request.event_kind.as_str() {
        "heartbeat" => {
            request.child_device_id.is_none() && request.topic == "v1/gateways/me/telemetry"
        }
        "child_telemetry" => {
            request.child_device_id.is_some() && request.topic == "v1/gateways/me/telemetry"
        }
        "connect" => request.child_device_id.is_some() && request.topic == "v1/gateways/me/connect",
        "disconnect" => {
            request.child_device_id.is_some() && request.topic == "v1/gateways/me/disconnect"
        }
        _ => false,
    };
    valid.then_some(()).ok_or(ApiError::Unauthorized)
}

#[derive(Debug, Deserialize)]
struct MqttdDeviceTransportRpcResponseRequest {
    command_id: Uuid,
    device_id: String,
    token_id: Uuid,
    response: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct MqttdDeviceTransportSessionResolution {
    device_id: String,
    token_id: Uuid,
    is_gateway: bool,
}

impl MqttdDeviceTransportSessionResolutionRequest {
    fn validate_client_id(&self) -> Result<(), ApiError> {
        if self.client_id.is_empty() {
            return Err(ApiError::BadRequest("client_id is required".to_owned()));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct ProvisionDeviceTokenRequest {
    display_name: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct AuthResponse {
    role: Role,
    account_class: AccountClass,
    username: String,
    default_app: String,
    granted_apps: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
}

impl AuthResponse {
    fn from_user(user: AuthenticatedUser, session_id: Option<String>) -> Self {
        Self {
            role: user.role,
            account_class: user.account_class,
            username: user.username,
            default_app: user.default_app,
            granted_apps: user.granted_apps,
            session_id,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateManagementDeviceRequest {
    display_name: String,
    #[serde(default)]
    asset_id: Option<Uuid>,
    #[serde(default)]
    device_profile_id: Option<Uuid>,
    #[serde(default)]
    attributes: Option<serde_json::Value>,
    #[serde(default)]
    topology: Option<GatewayTopologyRequest>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct GatewayTopologyRequest {
    is_gateway: bool,
    #[serde(default)]
    gateway_device_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateManagementAssetRequest {
    name: String,
    #[serde(default)]
    asset_profile_id: Option<Uuid>,
    #[serde(default)]
    parent_asset_id: Option<Uuid>,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default)]
    attributes: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateResourceShareRequest {
    username: String,
    permission: String,
    #[serde(default)]
    inherit_children: bool,
}

#[derive(Debug, Deserialize)]
struct ResourceShareQuery {
    state: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ResourceShareResponse {
    id: Uuid,
    resource_type: String,
    resource_id: String,
    permission: String,
    inherit_children: bool,
    state: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ProvisionMyDeviceRequest {
    display_name: String,
    #[serde(default)]
    asset_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct AssignMyDeviceAssetRequest {
    #[serde(default)]
    asset_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct IssueDeviceClaimCodeRequest {
    #[serde(default)]
    expires_in_seconds: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct DeviceClaimCodeResponse {
    device_id: String,
    claim_code: String,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ClaimDeviceRequest {
    device_id: String,
    claim_code: String,
}

#[derive(Debug, Deserialize)]
struct UpdateManagementUserRequest {
    default_app: String,
    granted_apps: Vec<String>,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateManagementUserRequest {
    username: String,
    password: String,
    default_app: String,
    granted_apps: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CreateManagementDeviceProfileRequest {
    name: String,
    #[serde(default)]
    telemetry_schema: serde_json::Value,
    #[serde(default)]
    metric_mapping: serde_json::Value,
    #[serde(default)]
    reporting_settings: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct CreateManagementAssetProfileRequest {
    name: String,
    #[serde(default)]
    fields: serde_json::Value,
    #[serde(default)]
    dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct PowerTelemetryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    #[serde(default)]
    bucket: Option<PowerTelemetryBucket>,
}

const SQLITE_MAX_RAW_POWER_TELEMETRY_ROWS: i64 = 10_000;

impl PowerTelemetryQuery {
    fn validate(&self) -> Result<PowerBucket, ApiError> {
        if self.from >= self.to {
            return Err(ApiError::BadRequest(
                "`from` must be earlier than `to`".to_owned(),
            ));
        }
        Ok(
            match self.bucket.unwrap_or(PowerTelemetryBucket::FiveMinutes) {
                PowerTelemetryBucket::Raw => PowerBucket::Raw,
                PowerTelemetryBucket::FiveMinutes => PowerBucket::FiveMinutes,
                PowerTelemetryBucket::OneHour => PowerBucket::OneHour,
            },
        )
    }

    fn validate_sqlite(&self) -> Result<PowerBucket, ApiError> {
        match self.validate()? {
            PowerBucket::Raw => {
                if self.to - self.from > Duration::hours(24) {
                    return Err(ApiError::BadRequest(
                        "`bucket=raw` supports a maximum 24-hour range; use `bucket=5m` or `bucket=1h`"
                            .to_owned(),
                    ));
                }
                Ok(PowerBucket::Raw)
            }
            bucket => Ok(bucket),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PowerTelemetryBucket {
    Raw,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Debug, Serialize, ToSchema)]
struct ManagementDevice {
    device_id: String,
    display_name: Option<String>,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
    attributes: serde_json::Value,
    online: bool,
    last_seen_at: Option<DateTime<Utc>>,
    is_gateway: bool,
    gateway_device_id: Option<String>,
    gateway_status: Option<String>,
    child_status: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ManagementAsset {
    id: Uuid,
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: serde_json::Value,
    attributes: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
struct ManagementUser {
    id: Uuid,
    username: String,
    role: String,
    account_class: String,
    default_app: String,
    granted_apps: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ManagementDeviceProfile {
    id: Uuid,
    name: String,
    telemetry_schema: serde_json::Value,
    metric_mapping: serde_json::Value,
    reporting_settings: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
struct ManagementAssetProfile {
    id: Uuid,
    name: String,
    fields: serde_json::Value,
    dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct CreateAlertRuleRequest {
    name: String,
    #[serde(default)]
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    #[serde(default)]
    window_seconds: Option<i32>,
    #[serde(default)]
    for_seconds: Option<i32>,
    #[serde(default)]
    resolve_after_seconds: Option<i32>,
    #[serde(default)]
    reopen_grace_seconds: Option<i32>,
    #[serde(default)]
    hysteresis: Option<f64>,
    #[serde(default)]
    severity: Option<String>,
    #[serde(default)]
    reminder_interval_seconds: Option<i32>,
}

#[derive(Debug)]
struct ValidatedAlertRule {
    name: String,
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    window_seconds: Option<i32>,
    for_seconds: i32,
    resolve_after_seconds: i32,
    reopen_grace_seconds: i32,
    hysteresis: Option<f64>,
    severity: String,
    reminder_interval_seconds: i32,
}

impl CreateAlertRuleRequest {
    fn validate(self) -> Result<ValidatedAlertRule, ApiError> {
        let name = self.name.trim().to_owned();
        if name.is_empty() || name.len() > 120 {
            return Err(ApiError::BadRequest(
                "rule name must contain 1 to 120 characters".to_owned(),
            ));
        }
        if !is_metric_key(&self.metric_key) {
            return Err(ApiError::BadRequest("invalid metric key".to_owned()));
        }
        if let Some(device_id) = &self.device_id {
            if !is_identifier(device_id) {
                return Err(ApiError::BadRequest("invalid device ID".to_owned()));
            }
        }
        if !self.threshold.is_finite() {
            return Err(ApiError::BadRequest("threshold must be finite".to_owned()));
        }
        if !matches!(
            self.rule_type.as_str(),
            "event_threshold" | "window_average"
        ) {
            return Err(ApiError::BadRequest("invalid rule type".to_owned()));
        }
        if !matches!(self.comparison.as_str(), "gt" | "gte" | "lt" | "lte") {
            return Err(ApiError::BadRequest("invalid comparison".to_owned()));
        }
        let window_seconds = match (self.rule_type.as_str(), self.window_seconds) {
            ("event_threshold", None) => None,
            ("event_threshold", Some(_)) => {
                return Err(ApiError::BadRequest(
                    "event threshold rules must not set window_seconds".to_owned(),
                ));
            }
            ("window_average", Some(seconds)) if seconds >= 60 => Some(seconds),
            ("window_average", _) => {
                return Err(ApiError::BadRequest(
                    "window average rules require window_seconds of at least 60".to_owned(),
                ));
            }
            _ => unreachable!("rule type was validated"),
        };
        let for_seconds = self.for_seconds.unwrap_or(300);
        let resolve_after_seconds = self.resolve_after_seconds.unwrap_or(300);
        let reopen_grace_seconds = self.reopen_grace_seconds.unwrap_or(3600);
        let reminder_interval_seconds = self.reminder_interval_seconds.unwrap_or(86400);
        if [for_seconds, resolve_after_seconds, reopen_grace_seconds]
            .into_iter()
            .any(|value| value < 0)
        {
            return Err(ApiError::BadRequest(
                "rule durations must be nonnegative".to_owned(),
            ));
        }
        if reminder_interval_seconds <= 0 {
            return Err(ApiError::BadRequest(
                "reminder_interval_seconds must be greater than zero".to_owned(),
            ));
        }
        if self
            .hysteresis
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(ApiError::BadRequest(
                "hysteresis must be finite and nonnegative".to_owned(),
            ));
        }
        let severity = self.severity.unwrap_or_else(|| "warning".to_owned());
        if !matches!(severity.as_str(), "info" | "warning" | "critical") {
            return Err(ApiError::BadRequest("invalid severity".to_owned()));
        }

        Ok(ValidatedAlertRule {
            name,
            device_id: self.device_id,
            metric_key: self.metric_key,
            rule_type: self.rule_type,
            comparison: self.comparison,
            threshold: self.threshold,
            window_seconds,
            for_seconds,
            resolve_after_seconds,
            reopen_grace_seconds,
            hysteresis: self.hysteresis,
            severity,
            reminder_interval_seconds,
        })
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct ToggleAlertRuleRequest {
    enabled: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TelemetryBucket {
    Raw,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Debug, Serialize)]
struct DeviceSummary {
    device_id: String,
    display_name: Option<String>,
    online: bool,
    last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct TelemetryPoint {
    at: DateTime<Utc>,
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
    event_count: i64,
}

#[derive(Debug, Serialize)]
struct AlertRuleResponse {
    id: Uuid,
    name: String,
    enabled: bool,
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    window_seconds: Option<i32>,
    for_seconds: i32,
    resolve_after_seconds: i32,
    reopen_grace_seconds: i32,
    hysteresis: Option<f64>,
    severity: String,
    reminder_interval_seconds: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

fn alert_rule_from_row(row: sqlx::postgres::PgRow) -> Result<AlertRuleResponse, ApiError> {
    Ok(AlertRuleResponse {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        rule_type: row.try_get("rule_type")?,
        comparison: row.try_get("comparison")?,
        threshold: row.try_get("threshold")?,
        window_seconds: row.try_get("window_seconds")?,
        for_seconds: row.try_get("for_seconds")?,
        resolve_after_seconds: row.try_get("resolve_after_seconds")?,
        reopen_grace_seconds: row.try_get("reopen_grace_seconds")?,
        hysteresis: row.try_get("hysteresis")?,
        severity: row.try_get("severity")?,
        reminder_interval_seconds: row.try_get("reminder_interval_seconds")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[derive(Debug, Serialize)]
struct AlertIncidentResponse {
    id: Uuid,
    rule_id: Uuid,
    rule_name: String,
    severity: String,
    device_id: String,
    status: String,
    condition_started_at: DateTime<Utc>,
    opened_at: Option<DateTime<Utc>>,
    resolved_at: Option<DateTime<Utc>>,
    acknowledged_at: Option<DateTime<Utc>>,
    acknowledged_by: Option<String>,
    last_value: Option<f64>,
    updated_at: DateTime<Utc>,
}

fn alert_incident_from_row(row: sqlx::postgres::PgRow) -> Result<AlertIncidentResponse, ApiError> {
    Ok(AlertIncidentResponse {
        id: row.try_get("id")?,
        rule_id: row.try_get("rule_id")?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        condition_started_at: row.try_get("condition_started_at")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        last_value: row.try_get("last_value")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn sqlite_active_alert_rule(
    pool: &sqlx::SqlitePool,
    id: &str,
) -> Result<Option<AlertRuleResponse>, ApiError> {
    let row = sqlx::query(
        "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds, created_at, updated_at
         FROM alert_rules
         WHERE id = ? AND archived_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(sqlite_alert_rule_from_row).transpose()
}

fn sqlite_alert_rule_from_row(row: sqlx::sqlite::SqliteRow) -> Result<AlertRuleResponse, ApiError> {
    Ok(AlertRuleResponse {
        id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        name: row.try_get("name")?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        rule_type: row.try_get("rule_type")?,
        comparison: row.try_get("comparison")?,
        threshold: row.try_get("threshold")?,
        window_seconds: row.try_get("window_seconds")?,
        for_seconds: row.try_get("for_seconds")?,
        resolve_after_seconds: row.try_get("resolve_after_seconds")?,
        reopen_grace_seconds: row.try_get("reopen_grace_seconds")?,
        hysteresis: row.try_get("hysteresis")?,
        severity: row.try_get("severity")?,
        reminder_interval_seconds: row.try_get("reminder_interval_seconds")?,
        created_at: sqlite_timestamp(&row.try_get::<String, _>("created_at")?)?,
        updated_at: sqlite_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

async fn sqlite_alert_incident_rows(
    pool: &sqlx::SqlitePool,
    incident_id: Option<&str>,
) -> Result<Vec<sqlx::sqlite::SqliteRow>, ApiError> {
    match incident_id {
        Some(incident_id) => Ok(sqlx::query(
            "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                    incidents.device_id, incidents.status, incidents.condition_started_at,
                    incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                    incidents.acknowledged_by, incidents.last_value, incidents.updated_at
             FROM alert_incidents AS incidents
             JOIN alert_rules AS rules ON rules.id = incidents.rule_id
             WHERE incidents.id = ?",
        )
        .bind(incident_id)
        .fetch_all(pool)
        .await?),
        None => Ok(sqlx::query(
            "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                    incidents.device_id, incidents.status, incidents.condition_started_at,
                    incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                    incidents.acknowledged_by, incidents.last_value, incidents.updated_at
             FROM alert_incidents AS incidents
             JOIN alert_rules AS rules ON rules.id = incidents.rule_id
             ORDER BY
                CASE incidents.status WHEN 'open' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END,
                incidents.updated_at DESC
             LIMIT 100",
        )
        .fetch_all(pool)
        .await?),
    }
}

fn sqlite_alert_incident_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<AlertIncidentResponse, ApiError> {
    Ok(AlertIncidentResponse {
        id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        rule_id: sqlite_uuid(&row.try_get::<String, _>("rule_id")?)?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        condition_started_at: sqlite_timestamp(&row.try_get::<String, _>("condition_started_at")?)?,
        opened_at: row
            .try_get::<Option<String>, _>("opened_at")?
            .map(|value| sqlite_timestamp(&value))
            .transpose()?,
        resolved_at: row
            .try_get::<Option<String>, _>("resolved_at")?
            .map(|value| sqlite_timestamp(&value))
            .transpose()?,
        acknowledged_at: row
            .try_get::<Option<String>, _>("acknowledged_at")?
            .map(|value| sqlite_timestamp(&value))
            .transpose()?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        last_value: row.try_get("last_value")?,
        updated_at: sqlite_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn sqlite_uuid(value: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value).map_err(|_| ApiError::StorageData)
}

fn sqlite_timestamp(value: &str) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").map(|value| value.and_utc())
        })
        .map_err(|_| ApiError::StorageData)
}

async fn resource_permission(
    pool: &PgPool,
    context: &AuthContext,
    kind: ResourceKind,
    resource_id: &str,
) -> Result<Option<ResourcePermission>, ApiError> {
    match kind {
        ResourceKind::Device => device_permission(pool, context, resource_id)
            .await
            .map_err(ApiError::Database),
        ResourceKind::Asset => {
            let id = Uuid::parse_str(resource_id)
                .map_err(|_| ApiError::BadRequest("invalid asset ID".to_owned()))?;
            asset_permission(pool, context, id)
                .await
                .map_err(ApiError::Database)
        }
    }
}

async fn sqlite_resource_permission(
    pool: &sqlx::SqlitePool,
    context: &AuthContext,
    kind: ResourceKind,
    resource_id: &str,
) -> Result<Option<ResourcePermission>, ApiError> {
    match kind {
        ResourceKind::Device => sqlite_device_permission(pool, context, resource_id)
            .await
            .map_err(ApiError::Database),
        ResourceKind::Asset => {
            let id = Uuid::parse_str(resource_id)
                .map_err(|_| ApiError::BadRequest("invalid asset ID".to_owned()))?;
            sqlite_asset_permission(pool, context, id)
                .await
                .map_err(ApiError::Database)
        }
    }
}

async fn require_alert_manager(
    pool: &PgPool,
    context: &AuthContext,
    device_id: Option<&str>,
) -> Result<(), ApiError> {
    let Some(device_id) = device_id else {
        return if context.account_class == AccountClass::Admin {
            Ok(())
        } else {
            Err(ApiError::Forbidden)
        };
    };
    if !device_permission(pool, context, device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

async fn sqlite_require_alert_manager(
    pool: &sqlx::SqlitePool,
    context: &AuthContext,
    device_id: Option<&str>,
) -> Result<(), ApiError> {
    let Some(device_id) = device_id else {
        return if context.account_class == AccountClass::Admin {
            Ok(())
        } else {
            Err(ApiError::Forbidden)
        };
    };
    if !sqlite_device_permission(pool, context, device_id)
        .await?
        .is_some_and(|access| access.allows(ResourcePermission::Manager))
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

async fn can_read_alert(
    pool: &PgPool,
    context: &AuthContext,
    device_id: Option<&str>,
) -> Result<bool, ApiError> {
    match device_id {
        Some(device_id) => Ok(device_permission(pool, context, device_id)
            .await?
            .is_some_and(|access| access.allows(ResourcePermission::Viewer))),
        None => Ok(context.account_class == AccountClass::Admin),
    }
}

async fn sqlite_can_read_alert(
    pool: &sqlx::SqlitePool,
    context: &AuthContext,
    device_id: Option<&str>,
) -> Result<bool, ApiError> {
    match device_id {
        Some(device_id) => Ok(sqlite_device_permission(pool, context, device_id)
            .await?
            .is_some_and(|access| access.allows(ResourcePermission::Viewer))),
        None => Ok(context.account_class == AccountClass::Admin),
    }
}

async fn alert_rule_device(pool: &PgPool, id: Uuid) -> Result<Option<String>, ApiError> {
    sqlx::query_scalar(
        "SELECT device_id
         FROM alert_rules
         WHERE id = $1 AND archived_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::NotFound("alert rule not found"))
}

async fn sqlite_alert_rule_device(
    pool: &sqlx::SqlitePool,
    id: Uuid,
) -> Result<Option<String>, ApiError> {
    sqlx::query_scalar(
        "SELECT device_id
         FROM alert_rules
         WHERE id = ? AND archived_at IS NULL",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::NotFound("alert rule not found"))
}

async fn alert_incident_device(pool: &PgPool, id: Uuid) -> Result<String, ApiError> {
    sqlx::query_scalar(
        "SELECT device_id
         FROM alert_incidents
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::NotFound("alert incident not found"))
}

async fn sqlite_alert_incident_device(
    pool: &sqlx::SqlitePool,
    id: Uuid,
) -> Result<String, ApiError> {
    sqlx::query_scalar(
        "SELECT device_id
         FROM alert_incidents
         WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::NotFound("alert incident not found"))
}

async fn resource_owner(
    pool: &PgPool,
    kind: ResourceKind,
    resource_id: &str,
) -> Result<Option<Uuid>, ApiError> {
    match kind {
        ResourceKind::Device => sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT owner_user_id
                 FROM devices
                 WHERE device_id = $1 AND deleted_at IS NULL",
        )
        .bind(resource_id)
        .fetch_optional(pool)
        .await
        .map(|owner| owner.flatten())
        .map_err(ApiError::Database),
        ResourceKind::Asset => {
            let id = Uuid::parse_str(resource_id)
                .map_err(|_| ApiError::BadRequest("invalid asset ID".to_owned()))?;
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT owner_user_id
                 FROM assets
                 WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(pool)
            .await
            .map(|owner| owner.flatten())
            .map_err(ApiError::Database)
        }
    }
}

async fn sqlite_resource_owner(
    pool: &sqlx::SqlitePool,
    kind: ResourceKind,
    resource_id: &str,
) -> Result<Option<String>, ApiError> {
    match kind {
        ResourceKind::Device => sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_user_id
                 FROM devices
                 WHERE device_id = ? AND deleted_at IS NULL",
        )
        .bind(resource_id)
        .fetch_optional(pool)
        .await
        .map(|owner| owner.flatten())
        .map_err(ApiError::Database),
        ResourceKind::Asset => {
            let id = Uuid::parse_str(resource_id)
                .map_err(|_| ApiError::BadRequest("invalid asset ID".to_owned()))?;
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT owner_user_id
                 FROM assets
                 WHERE id = ?",
            )
            .bind(id.to_string())
            .fetch_optional(pool)
            .await
            .map(|owner| owner.flatten())
            .map_err(ApiError::Database)
        }
    }
}

async fn share_target_user_id(pool: &PgPool, username: &str) -> Result<Uuid, ApiError> {
    if !is_identifier(username) {
        return Err(ApiError::BadRequest("invalid share username".to_owned()));
    }
    sqlx::query_scalar(
        "SELECT id
         FROM users
         WHERE username = $1 AND account_class <> 'system'",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::BadRequest("share user does not exist".to_owned()))
}

async fn sqlite_share_target_user_id(
    pool: &sqlx::SqlitePool,
    username: &str,
) -> Result<String, ApiError> {
    if !is_identifier(username) {
        return Err(ApiError::BadRequest("invalid share username".to_owned()));
    }
    sqlx::query_scalar(
        "SELECT id
         FROM users
         WHERE username = ? AND account_class <> 'system'",
    )
    .bind(username)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::BadRequest("share user does not exist".to_owned()))
}

fn resource_share_from_row(row: sqlx::postgres::PgRow) -> Result<ResourceShareResponse, ApiError> {
    Ok(ResourceShareResponse {
        id: row.try_get("id")?,
        resource_type: row.try_get("resource_type")?,
        resource_id: row.try_get("resource_id")?,
        permission: row.try_get("permission")?,
        inherit_children: row.try_get("inherit_children")?,
        state: row.try_get("state")?,
    })
}

fn sqlite_resource_share_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ResourceShareResponse, ApiError> {
    Ok(ResourceShareResponse {
        id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        resource_type: row.try_get("resource_type")?,
        resource_id: row.try_get("resource_id")?,
        permission: row.try_get("permission")?,
        inherit_children: row.try_get::<i64, _>("inherit_children")? != 0,
        state: row.try_get("state")?,
    })
}

async fn write_audit_event(
    pool: &PgPool,
    context: &AuthContext,
    resource_type: &str,
    resource_id: &str,
    action: &str,
    before_value: Option<&serde_json::Value>,
    after_value: Option<&serde_json::Value>,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO audit_events (
            id, actor_user_id, actor_account_class, resource_type, resource_id,
            action, before_value, after_value, request_id
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(Uuid::now_v7())
    .bind(context.user_id)
    .bind(context.account_class.as_str())
    .bind(resource_type)
    .bind(resource_id)
    .bind(action)
    .bind(before_value.map(|value| sqlx::types::Json(value.clone())))
    .bind(after_value.map(|value| sqlx::types::Json(value.clone())))
    .bind(&context.session_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn sqlite_write_audit_event(
    pool: &sqlx::SqlitePool,
    context: &AuthContext,
    resource_type: &str,
    resource_id: &str,
    action: &str,
    before_value: Option<&serde_json::Value>,
    after_value: Option<&serde_json::Value>,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO audit_events (
            id, actor_user_id, actor_account_class, resource_type, resource_id,
            action, before_value, after_value, request_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(context.user_id.to_string())
    .bind(context.account_class.as_str())
    .bind(resource_type)
    .bind(resource_id)
    .bind(action)
    .bind(before_value.map(serde_json::Value::to_string))
    .bind(after_value.map(serde_json::Value::to_string))
    .bind(&context.session_id)
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

fn resource_kind(value: &str) -> Result<ResourceKind, ApiError> {
    match value {
        "asset" => Ok(ResourceKind::Asset),
        "device" => Ok(ResourceKind::Device),
        _ => Err(ApiError::StorageData),
    }
}

fn is_resource_share_state(value: &str) -> bool {
    matches!(
        value,
        "pending" | "active" | "declined" | "cancelled" | "expired"
    )
}

fn resource_share_database_error(error: sqlx::Error) -> ApiError {
    if error
        .as_database_error()
        .is_some_and(|database_error| database_error.is_unique_violation())
    {
        ApiError::Conflict("a pending resource share already exists".to_owned())
    } else {
        ApiError::Database(error)
    }
}

#[derive(Debug, Error)]
enum ApiError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("user authentication configuration failed")]
    Auth(#[source] AuthError),
    #[error("system configuration is unavailable")]
    SystemConfiguration(#[source] SystemConfigurationServiceError),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    NotFound(&'static str),
    #[error("authentication required")]
    Unauthorized,
    #[error("administrator access required")]
    Forbidden,
    #[error("too many failed login attempts")]
    TooManyRequests,
    #[error("MQTT transport authentication is not configured")]
    MqttdDeviceTransportAuthenticationUnavailable,
    #[error("MQTT transport session revocation failed")]
    MqttdDeviceTransportSessionRevocationUnavailable,
    #[error("core command service is unavailable")]
    CoreCommandUnavailable,
    #[error("device token operation failed")]
    DeviceToken(#[source] DeviceTokenStoreError),
    #[error("SQLite persisted data is invalid")]
    StorageData,
    #[error(transparent)]
    Serialization(serde_json::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Auth(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::SystemConfiguration(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            Self::MqttdDeviceTransportAuthenticationUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::MqttdDeviceTransportSessionRevocationUnavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::CoreCommandUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::DeviceToken(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::StorageData => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Serialization(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|character| {
            character.is_ascii_alphanumeric() || character == b'-' || character == b'_'
        })
}

fn app_key_from_path(value: &str) -> Option<&str> {
    value
        .strip_prefix("/apps/")
        .filter(|key| is_identifier(key))
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

fn is_metric_key(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic())
        && bytes.all(|character| character.is_ascii_alphanumeric() || character == b'_')
        && value.len() <= 64
}

fn require_powermonitor(context: &AuthContext) -> Result<(), ApiError> {
    if context
        .granted_apps
        .iter()
        .any(|app| app == POWER_MONITOR_APP)
    {
        Ok(())
    } else {
        Err(ApiError::Forbidden)
    }
}

fn require_user_account(context: &AuthContext) -> Result<(), ApiError> {
    if context.account_class == AccountClass::User {
        Ok(())
    } else {
        Err(ApiError::Forbidden)
    }
}

fn claim_code_expiry(expires_in_seconds: Option<i64>) -> Result<DateTime<Utc>, ApiError> {
    let seconds = expires_in_seconds.unwrap_or(24 * 60 * 60);
    if !(60..=7 * 24 * 60 * 60).contains(&seconds) {
        return Err(ApiError::BadRequest(
            "claim code expiry must be between 60 seconds and 7 days".to_owned(),
        ));
    }
    Ok(Utc::now() + Duration::seconds(seconds))
}

fn validated_name<'a>(value: &'a str, label: &str) -> Result<&'a str, ApiError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        return Err(ApiError::BadRequest(format!("invalid {label}")));
    }
    Ok(value)
}

fn object_value(value: serde_json::Value, label: &str) -> Result<serde_json::Value, ApiError> {
    if value.is_null() {
        return Ok(json!({}));
    }
    if value.is_object() {
        Ok(value)
    } else {
        Err(ApiError::BadRequest(format!("{label} must be an object")))
    }
}

fn management_device_from_row(
    row: sqlx::postgres::PgRow,
    online_after: DateTime<Utc>,
) -> Result<ManagementDevice, ApiError> {
    let last_seen_at = row.try_get::<Option<DateTime<Utc>>, _>("last_seen_at")?;
    let is_gateway = row.try_get::<bool, _>("is_gateway")?;
    let gateway_device_id = row.try_get::<Option<String>, _>("gateway_device_id")?;
    let gateway_last_read_at = row.try_get::<Option<DateTime<Utc>>, _>("gateway_last_read_at")?;
    let gateway_read_quality = row.try_get::<Option<String>, _>("gateway_read_quality")?;
    let (online, gateway_status, child_status, last_seen_at) = management_device_health(
        is_gateway,
        gateway_device_id.as_deref(),
        last_seen_at,
        gateway_last_read_at,
        gateway_read_quality.as_deref(),
        online_after,
    );
    Ok(ManagementDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        asset_id: row.try_get("asset_id")?,
        device_profile_id: row.try_get("device_profile_id")?,
        attributes: row.try_get("metadata")?,
        online,
        last_seen_at,
        is_gateway,
        gateway_device_id,
        gateway_status,
        child_status,
    })
}

fn management_device_health(
    is_gateway: bool,
    gateway_device_id: Option<&str>,
    last_seen_at: Option<DateTime<Utc>>,
    gateway_last_read_at: Option<DateTime<Utc>>,
    gateway_read_quality: Option<&str>,
    fresh_after: DateTime<Utc>,
) -> (bool, Option<String>, Option<String>, Option<DateTime<Utc>>) {
    if is_gateway {
        let status = if last_seen_at.is_some_and(|seen| seen >= fresh_after) {
            "online"
        } else {
            "offline"
        };
        return (
            status == "online",
            Some(status.to_owned()),
            None,
            last_seen_at,
        );
    }
    if gateway_device_id.is_some() {
        let unavailable_after = fresh_after - Duration::minutes(10);
        let status = if gateway_read_quality == Some("unavailable") {
            "unavailable"
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= fresh_after) {
            "fresh"
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= unavailable_after) {
            "stale"
        } else {
            "unavailable"
        };
        return (
            status == "fresh",
            None,
            Some(status.to_owned()),
            gateway_last_read_at,
        );
    }
    (
        last_seen_at.is_some_and(|seen| seen >= fresh_after),
        None,
        None,
        last_seen_at,
    )
}

fn management_asset_from_row(row: sqlx::postgres::PgRow) -> Result<ManagementAsset, ApiError> {
    let metadata: serde_json::Value = row.try_get("metadata")?;
    Ok(ManagementAsset {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        asset_profile_id: row.try_get("asset_profile_id")?,
        parent_asset_id: row.try_get("parent_asset_id")?,
        attributes: metadata.clone(),
        metadata,
    })
}

fn management_device_profile_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementDeviceProfile, ApiError> {
    Ok(ManagementDeviceProfile {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        telemetry_schema: row.try_get("telemetry_schema")?,
        metric_mapping: row.try_get("metric_mapping")?,
        reporting_settings: row.try_get("reporting_settings")?,
    })
}

fn management_asset_profile_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementAssetProfile, ApiError> {
    Ok(ManagementAssetProfile {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        fields: row.try_get("fields")?,
        dashboard_defaults: row.try_get("dashboard_defaults")?,
    })
}

fn sqlite_management_device_from_row(
    row: sqlx::sqlite::SqliteRow,
    online_after: &str,
) -> Result<ManagementDevice, ApiError> {
    let is_gateway = row.try_get::<i64, _>("is_gateway")? != 0;
    let gateway_device_id = row.try_get::<Option<String>, _>("gateway_device_id")?;
    let last_seen_at = row.try_get::<Option<String>, _>("last_seen_at")?;
    let gateway_last_read_at = row.try_get::<Option<String>, _>("gateway_last_read_at")?;
    let gateway_read_quality = row.try_get::<Option<String>, _>("gateway_read_quality")?;
    let (online, gateway_status, child_status, visible_at) = sqlite_device_health(
        is_gateway,
        gateway_device_id.as_deref(),
        last_seen_at.as_deref(),
        gateway_last_read_at.as_deref(),
        gateway_read_quality.as_deref(),
        online_after,
    );
    Ok(ManagementDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        asset_id: row
            .try_get::<Option<String>, _>("asset_id")?
            .map(|value| sqlite_uuid(&value))
            .transpose()?,
        device_profile_id: row
            .try_get::<Option<String>, _>("device_profile_id")?
            .map(|value| sqlite_uuid(&value))
            .transpose()?,
        attributes: serde_json::from_str(&row.try_get::<String, _>("metadata")?)
            .map_err(ApiError::Serialization)?,
        online,
        last_seen_at: visible_at.as_deref().map(sqlite_timestamp).transpose()?,
        is_gateway,
        gateway_device_id,
        gateway_status: gateway_status.map(str::to_owned),
        child_status: child_status.map(str::to_owned),
    })
}

fn sqlite_management_asset_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementAsset, ApiError> {
    let metadata: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("metadata")?)
        .map_err(ApiError::Serialization)?;
    Ok(ManagementAsset {
        id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        name: row.try_get("name")?,
        asset_profile_id: row
            .try_get::<Option<String>, _>("asset_profile_id")?
            .map(|value| sqlite_uuid(&value))
            .transpose()?,
        parent_asset_id: row
            .try_get::<Option<String>, _>("parent_asset_id")?
            .map(|value| sqlite_uuid(&value))
            .transpose()?,
        attributes: metadata.clone(),
        metadata,
    })
}

fn sqlite_management_device_profile_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementDeviceProfile, ApiError> {
    Ok(ManagementDeviceProfile {
        id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        name: row.try_get("name")?,
        telemetry_schema: serde_json::from_str(&row.try_get::<String, _>("telemetry_schema")?)
            .map_err(ApiError::Serialization)?,
        metric_mapping: serde_json::from_str(&row.try_get::<String, _>("metric_mapping")?)
            .map_err(ApiError::Serialization)?,
        reporting_settings: serde_json::from_str(&row.try_get::<String, _>("reporting_settings")?)
            .map_err(ApiError::Serialization)?,
    })
}

fn sqlite_management_asset_profile_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementAssetProfile, ApiError> {
    Ok(ManagementAssetProfile {
        id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
        name: row.try_get("name")?,
        fields: serde_json::from_str(&row.try_get::<String, _>("fields")?)
            .map_err(ApiError::Serialization)?,
        dashboard_defaults: serde_json::from_str(&row.try_get::<String, _>("dashboard_defaults")?)
            .map_err(ApiError::Serialization)?,
    })
}

async fn list_management_users_query(pool: &PgPool) -> Result<Vec<ManagementUser>, ApiError> {
    let rows = sqlx::query(
        "SELECT users.id, users.username, users.role, users.account_class, users.default_app,
                COALESCE(
                    array_agg(grants.app_key ORDER BY grants.app_key)
                        FILTER (WHERE grants.app_key IS NOT NULL),
                    ARRAY[]::text[]
                ) AS granted_apps
         FROM users
         LEFT JOIN user_app_grants AS grants ON grants.user_id = users.id
         GROUP BY users.id
         ORDER BY users.username",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ManagementUser {
                id: row.try_get("id")?,
                username: row.try_get("username")?,
                role: row.try_get("role")?,
                account_class: row.try_get("account_class")?,
                default_app: row.try_get("default_app")?,
                granted_apps: row.try_get("granted_apps")?,
            })
        })
        .collect()
}

async fn sqlite_list_management_users_query(
    pool: &sqlx::SqlitePool,
) -> Result<Vec<ManagementUser>, ApiError> {
    let rows = sqlx::query(
        "SELECT users.id, users.username, users.role, users.account_class, users.default_app,
                COALESCE(group_concat(grants.app_key, ','), '') AS granted_apps
         FROM users
         LEFT JOIN user_app_grants AS grants ON grants.user_id = users.id
         GROUP BY users.id
         ORDER BY users.username",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let granted_apps = row.try_get::<String, _>("granted_apps")?;
            Ok(ManagementUser {
                id: sqlite_uuid(&row.try_get::<String, _>("id")?)?,
                username: row.try_get("username")?,
                role: row.try_get("role")?,
                account_class: row.try_get("account_class")?,
                default_app: row.try_get("default_app")?,
                granted_apps: granted_apps
                    .split(',')
                    .filter(|app| !app.is_empty())
                    .map(str::to_owned)
                    .collect(),
            })
        })
        .collect()
}
