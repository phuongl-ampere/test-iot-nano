use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Form, Json, Router,
    extract::{ConnectInfo, FromRequest, Path, Query, Request, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, SET_COOKIE},
    },
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post, put},
};
use iot_api::{
    AuthError, DeviceTokenResponse, DeviceTokenStoreError, OAuthBrowserSessionVerifier,
    PrincipalKind, TokenVault, authenticate_platform_account, authenticate_system_account,
    authenticate_tenant_account, authenticate_user_account, create_platform_device_token,
    generate_session_id, hash_password, provision_management_device_token,
    provision_owned_platform_device_token, provision_platform_device_token,
    reveal_platform_device_token, rotate_platform_device_token, validate_password,
};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationDomainProfileError, ApplicationDomainProfileRepository,
    ApplicationDomainResourceKind, ApplicationKind, ApplicationRepository, AuditAction, AuditEvent,
    AuditEventCursor, AuditEventError, AuditEventRepository, AuditPrincipal, AuditTargetType,
    AuthorizationRepository, AuthorizationSubject, ClientId, CreateApplicationAssetProfileRelation,
    CreateApplicationDomainProfile, CreateDeviceAssetRelation, CreateDeviceRelation,
    CreateManagementAlertRule, CreateManagementAsset, CreateManagementAssetProfile,
    CreateManagementDeviceProfile, CreateManagementUser, DeviceClaimError, DeviceClaimPolicy,
    DeviceClaimRepository, DeviceRelationError, DeviceRelationRepository, DeviceTokenRepository,
    DeviceTokenRepositoryError, MANAGEMENT_DEVICE_TELEMETRY_LIMIT,
    ManagementAlert as StorageManagementAlert, ManagementAlertError,
    ManagementAlertIncident as StorageManagementAlertIncident, ManagementAlertIncidentError,
    ManagementAlertIncidentRepository, ManagementAlertRepository,
    ManagementAlertRule as StorageManagementAlertRule, ManagementAlertRuleError,
    ManagementAlertRuleRepository, ManagementAsset as StorageManagementAsset, ManagementAssetError,
    ManagementAssetProfile, ManagementAssetProfileError, ManagementAssetProfileRepository,
    ManagementAssetRepository, ManagementChildStatus, ManagementDevice as StorageManagementDevice,
    ManagementDeviceError, ManagementDeviceProfile, ManagementDeviceProfileError,
    ManagementDeviceProfileRepository, ManagementDeviceRepository, ManagementDeviceTelemetry,
    ManagementDeviceTelemetryRepository, ManagementDeviceTopology, ManagementGatewayStatus,
    ManagementUser, ManagementUserError, ManagementUserRepository, ManagementUserRole,
    NewApplication, NewOAuthClientSecret, NewSystemAccount, NewTenant, NewTenantAccount,
    NewUserGroup, OAuthRepository, OwnershipTransferTarget, PlatformStore, PlatformStoreError,
    ProvisionManagementDeviceError, RedirectUri, ResourceAccess, ResourceAccessSource,
    ResourceInvitationRepository, ResourcePermission, SystemAccount, TenantAuthorizationError,
    TenantAuthorizationRepository, TenantIdentityError, TenantIdentityRepository, TenantStatus,
    UpdateApplicationDomainProfile, UpdateManagementAlertRule, UpdateManagementAsset,
    UpdateManagementAssetProfile, UpdateManagementDevice, UpdateManagementDeviceProfile,
    UpdateManagementUser, UserCapability, UserDeviceActivityRepository,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use thiserror::Error;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::Readiness;

mod errors;
mod openapi;
mod operator_api;
mod routes;
mod session;

use errors::*;
use openapi::management_openapi;
use operator_api::*;
use routes::*;
pub use session::ManagementSessionVerifier;
use session::*;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use tokio::sync::Barrier;

const SESSION_COOKIE: &str = "iot_nano_session";
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_LOGIN_FAILURES: u8 = 5;
const USER_DEVICE_LIST_LIMIT: u32 = 100;
const USER_ASSET_LIST_LIMIT: u32 = 100;
const USER_DEVICE_ACTIVITY_LIMIT: u32 = 10;
const DEFAULT_TENANT_AUDIT_LIMIT: usize = 50;
const MAX_TENANT_AUDIT_LIMIT: usize = 100;

#[derive(Debug, Error)]
pub enum BootstrapSystemError {
    #[error(
        "bootstrap system username must use 3-64 ASCII letters, digits, hyphens, or underscores"
    )]
    InvalidUsername,
    #[error("bootstrap system password is invalid")]
    InvalidPassword(#[source] AuthError),
    #[error("bootstrap system identity operation failed")]
    Identity(#[from] TenantIdentityError),
    #[error("bootstrap system platform migration failed")]
    PlatformMigration(#[source] PlatformStoreError),
}

pub async fn bootstrap_system(
    store: &PlatformStore,
    username: &str,
    password: &str,
) -> Result<SystemAccount, BootstrapSystemError> {
    if !is_platform_username(username) {
        return Err(BootstrapSystemError::InvalidUsername);
    }
    validate_password(password).map_err(BootstrapSystemError::InvalidPassword)?;
    let password_hash = hash_password(password).map_err(BootstrapSystemError::InvalidPassword)?;
    TenantIdentityRepository::bootstrap_system_account(
        store,
        NewSystemAccount {
            username: username.to_owned(),
            password_hash,
        },
    )
    .await
    .map_err(BootstrapSystemError::from)
}

fn is_platform_username(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn is_tenant_slug(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[derive(Clone)]
pub struct ManagementSessionRouter {
    pub router: Router,
    pub session_verifier: Arc<ManagementSessionVerifier>,
}

impl ManagementSessionRouter {
    pub fn new(store: Arc<PlatformStore>, token_vault: TokenVault) -> Self {
        Self::new_with_infrastructure_status(
            store,
            token_vault,
            SystemInfrastructureStatus::not_running(),
        )
    }

    pub(crate) fn new_with_infrastructure_status(
        store: Arc<PlatformStore>,
        token_vault: TokenVault,
        infrastructure_status: SystemInfrastructureStatus,
    ) -> Self {
        let session_verifier = Arc::new(ManagementSessionVerifier::default());
        let state = ManagementState {
            store,
            session_verifier: Arc::clone(&session_verifier),
            token_vault,
            login_limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
            authorization_gate: Arc::new(ManagementAuthorizationGate::default()),
            infrastructure_status,
            #[cfg(test)]
            authorization_test_hooks: None,
        };
        let router = Router::new()
            .route("/", get(platform_root))
            .route("/login", get(platform_login).post(platform_login_submit))
            .route("/logout", post(platform_logout))
            .route("/system", get(platform_system))
            .route(
                "/system/infrastructure",
                get(platform_system_infrastructure),
            )
            .route(
                "/system/infrastructure/status",
                get(platform_system_infrastructure_status),
            )
            .route("/system/tenants", post(create_system_tenant_form))
            .route("/system/tenants/suspend", post(suspend_system_tenant_form))
            .route(
                "/system/tenants/reactivate",
                post(reactivate_system_tenant_form),
            )
            .route("/system/tenants/delete", post(delete_system_tenant_form))
            .route(
                "/system/tenants/tenant-account/reset",
                post(reset_system_tenant_account_form),
            )
            .route(
                "/system/tenants/tenant-account/disable",
                post(disable_system_tenant_account_form),
            )
            .route("/tenant", get(platform_tenant))
            .route(
                "/tenant/users",
                get(platform_tenant_users).post(create_tenant_user_form),
            )
            .route(
                "/tenant/groups",
                get(platform_tenant_groups).post(create_tenant_group_form),
            )
            .route("/tenant/groups/members", post(add_tenant_group_member_form))
            .route(
                "/tenant/groups/members/remove",
                post(remove_tenant_group_member_form),
            )
            .route(
                "/tenant/permissions",
                get(platform_tenant_permissions).post(create_tenant_permission_form),
            )
            .route(
                "/tenant/permissions/revoke",
                post(revoke_tenant_permission_form),
            )
            .route(
                "/tenant/assets",
                get(platform_tenant_assets).post(create_tenant_asset_form),
            )
            .route(
                "/tenant/devices",
                get(platform_tenant_devices).post(provision_tenant_device_form),
            )
            .route(
                "/tenant/devices/claim-policy",
                get(platform_tenant_device_claim_policy)
                    .post(update_tenant_device_claim_policy_form),
            )
            .route(
                "/tenant/devices/{device_id}/claim-code/revoke",
                post(revoke_tenant_device_claim_code_form),
            )
            .route("/tenant/alerts", get(platform_tenant_alerts))
            .route("/tenant/audit", get(platform_tenant_audit))
            .route(
                "/tenant/profiles/device",
                get(platform_tenant_device_profiles).post(create_tenant_device_profile_form),
            )
            .route(
                "/tenant/profiles/asset",
                get(platform_tenant_asset_profiles).post(create_tenant_asset_profile_form),
            )
            .route("/tenant/topology", get(platform_tenant_topology))
            .route(
                "/tenant/topology/assign",
                post(assign_tenant_gateway_child_form),
            )
            .route(
                "/tenant/topology/detach",
                post(detach_tenant_gateway_child_form),
            )
            .route(
                "/tenant/relations",
                get(platform_tenant_relations).post(create_tenant_relation_form),
            )
            .route(
                "/tenant/relations/delete",
                post(delete_tenant_relation_form),
            )
            .route(
                "/tenant/applications",
                get(platform_tenant_applications).post(save_tenant_application_form),
            )
            .route(
                "/tenant/applications/{app_id}",
                get(platform_tenant_application_domain),
            )
            .route("/app", get(platform_app))
            .route("/app/devices", post(create_user_device_form))
            .route("/app/devices/claim", post(claim_user_device_form))
            .route("/app/invitations", get(platform_app_invitations))
            .route(
                "/app/assets",
                get(platform_app_assets).post(create_user_asset_form),
            )
            .route(
                "/app/assets/{asset_id}",
                get(platform_app_asset_detail).post(update_user_asset_form),
            )
            .route(
                "/app/assets/{asset_id}/permissions",
                post(create_user_asset_permission_form),
            )
            .route(
                "/app/assets/{asset_id}/permissions/revoke",
                post(revoke_user_asset_permission_form),
            )
            .route(
                "/app/devices/{device_id}",
                get(platform_app_device_detail).post(update_user_device_form),
            )
            .route(
                "/app/devices/{device_id}/permissions",
                post(create_user_device_permission_form),
            )
            .route(
                "/app/devices/{device_id}/permissions/revoke",
                post(revoke_user_device_permission_form),
            )
            .route(
                "/app/invitations/{invitation_id}/accept",
                post(accept_user_resource_invitation_form),
            )
            .route(
                "/app/invitations/{invitation_id}/cancel",
                post(cancel_user_resource_invitation_form),
            )
            .route("/assets/platform-ui.css", get(platform_stylesheet))
            .route("/assets/htmx.min.js", get(platform_htmx))
            .route("/api/auth/login", post(login))
            .route("/api/auth/logout", post(logout))
            .route("/api/auth/me", get(current_session))
            .route("/api/system/auth/login", post(system_login))
            .route("/api/system/tenants", post(create_system_tenant))
            .route(
                "/api/system/tenants/{tenant_slug}/suspend",
                post(suspend_system_tenant),
            )
            .route(
                "/api/system/tenants/{tenant_slug}/reactivate",
                post(reactivate_system_tenant),
            )
            .route(
                "/api/system/tenants/{tenant_slug}/delete",
                post(delete_system_tenant),
            )
            .route(
                "/api/system/tenants/{tenant_slug}/tenant-account/reset",
                post(reset_system_tenant_account),
            )
            .route(
                "/api/system/tenants/{tenant_slug}/tenant-account/disable",
                post(disable_system_tenant_account),
            )
            .route("/api/tenant/auth/login", post(tenant_login))
            .route("/api/tenant/auth/me", get(current_tenant_session))
            .route("/api/user/auth/login", post(user_login))
            .route("/api/user/auth/me", get(current_user_session))
            .route("/api/management/applications", post(create_application))
            .route(
                "/api/management/applications/{app_id}/domain-profiles",
                get(list_management_application_domain_profiles)
                    .post(create_management_application_domain_profile),
            )
            .route(
                "/api/management/applications/{app_id}/domain-profiles/{profile_id}",
                put(update_management_application_domain_profile)
                    .delete(delete_management_application_domain_profile),
            )
            .route(
                "/api/management/applications/{app_id}/asset-profile-relations",
                get(list_management_application_asset_profile_relations)
                    .post(create_management_application_asset_profile_relation),
            )
            .route(
                "/api/management/applications/{app_id}/asset-profile-relations/{relation_id}",
                axum::routing::delete(delete_management_application_asset_profile_relation),
            )
            .route(
                "/api/management/applications/{app_id}/assets/{asset_id}/domain-profile",
                put(assign_management_asset_application_profile),
            )
            .route(
                "/api/management/applications/{app_id}/devices/{device_id}/domain-profile",
                put(assign_management_device_application_profile),
            )
            .route("/api/management/alerts", get(list_management_alerts))
            .route(
                "/api/management/alerts/summary",
                get(management_alert_summary),
            )
            .route(
                "/api/management/alert-rules",
                get(list_management_alert_rules).post(create_management_alert_rule),
            )
            .route(
                "/api/management/alert-rules/{rule_id}",
                put(update_management_alert_rule),
            )
            .route(
                "/api/management/alert-rules/{rule_id}/archive",
                post(archive_management_alert_rule),
            )
            .route(
                "/api/management/alert-incidents",
                get(list_management_alert_incidents),
            )
            .route(
                "/api/management/alert-incidents/{incident_id}/acknowledge",
                post(acknowledge_management_alert_incident),
            )
            .route("/api/management/audit", get(list_management_audit_events))
            .route(
                "/api/management/users",
                get(list_management_users).post(create_management_user),
            )
            .route(
                "/api/management/users/{username}",
                put(update_management_user),
            )
            .route(
                "/api/management/users/{username}/capabilities",
                put(update_management_user_capabilities),
            )
            .route(
                "/api/management/resource-access",
                get(list_management_resource_access),
            )
            .route(
                "/api/management/profiles/device-profiles",
                get(list_management_device_profiles).post(create_management_device_profile),
            )
            .route(
                "/api/management/profiles/device-profiles/{profile_id}",
                put(update_management_device_profile).delete(delete_management_device_profile),
            )
            .route(
                "/api/management/profiles/asset-profiles",
                get(list_management_asset_profiles).post(create_management_asset_profile),
            )
            .route(
                "/api/management/profiles/asset-profiles/{profile_id}",
                put(update_management_asset_profile).delete(delete_management_asset_profile),
            )
            .route(
                "/api/management/devices",
                get(list_management_devices).post(provision_device),
            )
            .route(
                "/api/management/devices/{device_id}",
                put(update_management_device).delete(delete_management_device),
            )
            .route(
                "/api/management/devices/{device_id}/owner",
                put(assign_management_device_owner),
            )
            .route(
                "/api/management/devices/{device_id}/telemetry",
                get(list_management_device_telemetry),
            )
            .route(
                "/api/management/assets",
                get(list_management_assets).post(create_management_asset),
            )
            .route(
                "/api/management/assets/{asset_id}",
                put(update_management_asset).delete(delete_management_asset),
            )
            .route(
                "/api/management/assets/{asset_id}/owner",
                put(assign_management_asset_owner),
            )
            .route(
                "/api/management/devices/{device_id}/tokens",
                post(create_device_token),
            )
            .route(
                "/api/management/devices/{device_id}/claim-code",
                post(issue_management_device_claim_code),
            )
            .route(
                "/api/management/devices/{device_id}/token",
                get(reveal_management_device_token),
            )
            .route(
                "/api/management/devices/{device_id}/tokens/{token_id}/rotate",
                post(rotate_management_device_token),
            )
            .with_state(state)
            .merge(
                SwaggerUi::new("/docs/")
                    .external_url_unchecked("/api-docs/openapi.json", management_openapi()),
            );
        Self {
            router,
            session_verifier,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SystemInfrastructureStatus {
    readiness: Readiness,
    snapshot: Arc<Mutex<SystemInfrastructureSnapshot>>,
}

struct SystemInfrastructureSnapshot {
    public_http_listener: String,
    management_http_listener: String,
    mqtt_plaintext_listener: String,
    mqtt_tls_listener: String,
    migration: String,
    storage: String,
    tls: String,
}

impl SystemInfrastructureStatus {
    fn not_running() -> Self {
        Self {
            readiness: Readiness::default(),
            snapshot: Arc::new(Mutex::new(SystemInfrastructureSnapshot::not_running())),
        }
    }

    pub(crate) fn starting(readiness: Readiness) -> Self {
        Self {
            readiness,
            snapshot: Arc::new(Mutex::new(SystemInfrastructureSnapshot::starting())),
        }
    }

    pub(crate) fn mark_started(
        &self,
        storage: &StorageConfiguration,
        public_http_listener: SocketAddr,
        management_http_listener: SocketAddr,
        mqtt_plaintext_listener: SocketAddr,
    ) {
        let storage = match &storage.storage {
            DatabaseStorage::Sqlite => "SQLite connected",
            DatabaseStorage::Timescale => "Timescale connected",
        };
        let mut snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *snapshot = SystemInfrastructureSnapshot {
            public_http_listener: format!("Listening on {public_http_listener}"),
            management_http_listener: format!("Listening on {management_http_listener}"),
            mqtt_plaintext_listener: format!("Listening on {mqtt_plaintext_listener}"),
            mqtt_tls_listener: "Listening (TLS endpoint bound)".to_owned(),
            migration: "Completed at startup".to_owned(),
            storage: storage.to_owned(),
            tls: "Loaded for MQTT TLS".to_owned(),
        };
    }

    fn operational_health(&self) -> &'static str {
        if self.readiness.is_ready() {
            "Ready"
        } else {
            "Not ready"
        }
    }

    fn component_status<'a>(ready: bool, value: &'a str) -> &'a str {
        if ready { value } else { "Not ready" }
    }

    fn page(&self) -> crate::SystemInfrastructurePage {
        let ready = self.readiness.is_ready();
        let snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        crate::SystemInfrastructurePage::new(
            if ready { "Ready" } else { "Not ready" },
            vec![
                crate::SystemInfrastructureStatusRow::new(
                    "Public HTTP listener",
                    Self::component_status(ready, &snapshot.public_http_listener),
                ),
                crate::SystemInfrastructureStatusRow::new(
                    "Management HTTP listener",
                    Self::component_status(ready, &snapshot.management_http_listener),
                ),
                crate::SystemInfrastructureStatusRow::new(
                    "MQTT plaintext listener",
                    Self::component_status(ready, &snapshot.mqtt_plaintext_listener),
                ),
                crate::SystemInfrastructureStatusRow::new(
                    "MQTT TLS listener",
                    Self::component_status(ready, &snapshot.mqtt_tls_listener),
                ),
            ],
            vec![
                crate::SystemInfrastructureStatusRow::new(
                    "Migrations",
                    Self::component_status(ready, &snapshot.migration),
                ),
                crate::SystemInfrastructureStatusRow::new(
                    "Storage",
                    Self::component_status(ready, &snapshot.storage),
                ),
                crate::SystemInfrastructureStatusRow::new(
                    "TLS",
                    Self::component_status(ready, &snapshot.tls),
                ),
            ],
        )
    }
}

impl SystemInfrastructureSnapshot {
    fn not_running() -> Self {
        Self {
            public_http_listener: "Not running".to_owned(),
            management_http_listener: "Not running".to_owned(),
            mqtt_plaintext_listener: "Not running".to_owned(),
            mqtt_tls_listener: "Not running".to_owned(),
            migration: "Not run by this router".to_owned(),
            storage: "Not connected by this router".to_owned(),
            tls: "Not loaded by this router".to_owned(),
        }
    }

    fn starting() -> Self {
        Self {
            public_http_listener: "Starting".to_owned(),
            management_http_listener: "Starting".to_owned(),
            mqtt_plaintext_listener: "Starting".to_owned(),
            mqtt_tls_listener: "Starting".to_owned(),
            migration: "Completed before listener startup".to_owned(),
            storage: "Connected before listener startup".to_owned(),
            tls: "Loading for MQTT TLS".to_owned(),
        }
    }
}

#[derive(Default, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    user_id: Uuid,
}

#[derive(Serialize)]
struct PlatformLoginResponse {
    principal_kind: PrincipalKind,
    principal_id: Uuid,
    tenant_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct SystemLoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct SystemSessionResponse {
    principal_kind: PrincipalKind,
    tenant_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct TenantLoginRequest {
    tenant_slug: String,
    password: String,
}

#[derive(Deserialize)]
struct UserLoginRequest {
    tenant_slug: String,
    username: String,
    password: String,
}

#[derive(Serialize)]
struct TenantSessionResponse {
    principal_kind: PrincipalKind,
    tenant_account_id: Uuid,
    tenant_id: Uuid,
}

#[derive(Serialize)]
struct UserSessionResponse {
    principal_kind: PrincipalKind,
    user_id: Uuid,
    tenant_id: Uuid,
}

#[derive(Deserialize)]
struct CreateSystemTenantRequest {
    slug: String,
    metadata: Value,
    #[serde(default)]
    tenant_account_username: String,
    tenant_account_password: String,
}

#[derive(Deserialize)]
struct ResetTenantAccountRequest {
    password: String,
}

#[derive(Deserialize)]
struct CreateSystemTenantForm {
    slug: String,
    #[serde(default)]
    tenant_account_username: String,
    tenant_account_password: String,
}

#[derive(Deserialize)]
struct SystemTenantLifecycleForm {
    slug: String,
}

#[derive(Deserialize)]
struct ResetTenantAccountForm {
    slug: String,
    password: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenantUserForm {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct CreateTenantGroupForm {
    name: String,
    owner_user_id: Uuid,
}

#[derive(Deserialize)]
struct TenantGroupMemberForm {
    group_id: Uuid,
    user_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserResourcePermissionForm {
    username: String,
    permission: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeUserResourcePermissionForm {
    permission_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserAssetForm {
    name: String,
    #[serde(default)]
    parent_asset_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserDeviceForm {
    display_name: String,
    #[serde(default)]
    asset_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserClaimDeviceForm {
    device_id: String,
    code: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenantAssetForm {
    name: String,
    #[serde(default)]
    parent_asset_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProvisionTenantDeviceForm {
    display_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantDeviceClaimPolicyForm {
    #[serde(default)]
    enabled: Option<String>,
    ttl_seconds: u32,
    code_length: u8,
    max_failed_attempts: u8,
    request_cooldown_seconds: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenantDeviceProfileForm {
    name: String,
    telemetry_schema: String,
    metric_mapping: String,
    reporting_settings: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenantAssetProfileForm {
    name: String,
    fields: String,
    dashboard_defaults: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignTenantGatewayChildForm {
    child_device_id: String,
    gateway_device_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DetachTenantGatewayChildForm {
    child_device_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenantDeviceRelationForm {
    from_device_id: String,
    target_kind: String,
    to_device_id: Option<String>,
    to_asset_id: Option<Uuid>,
    relation_type: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteTenantDeviceRelationForm {
    target_kind: String,
    relation_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantApplicationForm {
    app_id: String,
    kind: String,
    launch_url: String,
    client_id: String,
    redirect_uris: String,
    allowed_scopes: String,
    #[serde(default)]
    enabled: Option<String>,
}

#[derive(Serialize)]
struct SystemTenantResponse {
    id: Uuid,
    slug: String,
    tenant_account_id: Uuid,
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
#[serde(deny_unknown_fields)]
struct CreateManagementUserRequest {
    username: String,
    password: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateManagementUserRequest {
    #[serde(default)]
    role: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateManagementUserCapabilitiesRequest {
    capabilities: Vec<String>,
}

#[derive(Serialize)]
struct ManagementUserResponse {
    id: Uuid,
    username: String,
    role: String,
    account_class: String,
    capabilities: Vec<String>,
}

#[derive(Deserialize)]
struct ManagementResourceAccessQuery {
    scope: String,
    resource_id: String,
}

#[derive(Serialize)]
struct ManagementResourceAccessResponse {
    items: Vec<ManagementResourceAccessItem>,
}

#[derive(Serialize)]
struct ManagementResourceAccessItem {
    id: Uuid,
    user_id: Uuid,
    username: String,
    permission: String,
    inherit_children: bool,
    scope: &'static str,
    resource_id: String,
}

#[derive(Serialize)]
struct ManagementAlertResponse {
    id: Uuid,
    rule_name: String,
    severity: String,
    device_id: String,
    status: String,
    last_value: Option<f64>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementAlertRuleRequest {
    name: String,
    #[serde(default = "default_alert_rule_enabled")]
    enabled: bool,
    #[serde(default)]
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    #[serde(default)]
    window_seconds: Option<u64>,
    #[serde(default)]
    for_seconds: u64,
    #[serde(default = "default_alert_resolve_after_seconds")]
    resolve_after_seconds: u64,
    #[serde(default = "default_alert_reopen_grace_seconds")]
    reopen_grace_seconds: u64,
    #[serde(default)]
    hysteresis: Option<f64>,
    severity: String,
    #[serde(default = "default_alert_reminder_interval_seconds")]
    reminder_interval_seconds: u64,
}

fn default_alert_rule_enabled() -> bool {
    true
}

fn default_alert_resolve_after_seconds() -> u64 {
    300
}

fn default_alert_reopen_grace_seconds() -> u64 {
    3_600
}

fn default_alert_reminder_interval_seconds() -> u64 {
    86_400
}

#[derive(Serialize)]
struct ManagementAlertRuleResponse {
    id: Uuid,
    name: String,
    enabled: bool,
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    window_seconds: Option<u64>,
    for_seconds: u64,
    resolve_after_seconds: u64,
    reopen_grace_seconds: u64,
    hysteresis: Option<f64>,
    severity: String,
    reminder_interval_seconds: u64,
    archived_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct ManagementAlertIncidentResponse {
    id: Uuid,
    rule_id: Uuid,
    rule_name: String,
    severity: String,
    device_id: String,
    status: String,
    last_value: Option<f64>,
    condition_started_at: chrono::DateTime<chrono::Utc>,
    opened_at: Option<chrono::DateTime<chrono::Utc>>,
    resolved_at: Option<chrono::DateTime<chrono::Utc>>,
    acknowledged_at: Option<chrono::DateTime<chrono::Utc>>,
    acknowledged_by: Option<String>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct ManagementAlertSummaryResponse {
    open_incident_count: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantAuditQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TenantAuditCursor {
    version: u8,
    tenant_id: Uuid,
    occurred_at: chrono::DateTime<chrono::Utc>,
    id: Uuid,
}

#[derive(Serialize)]
struct ManagementAuditEventResponse {
    id: Uuid,
    occurred_at: chrono::DateTime<chrono::Utc>,
    actor_kind: &'static str,
    actor_id: Uuid,
    action: &'static str,
    target_type: &'static str,
    target_id: String,
    changes: Value,
}

#[derive(Serialize)]
struct ManagementAuditEventPage {
    items: Vec<ManagementAuditEventResponse>,
    next_cursor: Option<String>,
    has_more: bool,
}

struct TenantAuditEventPage {
    events: Vec<AuditEvent>,
    next_cursor: Option<String>,
    has_more: bool,
    limit: usize,
}

#[derive(Deserialize)]
struct ManagementDeviceProfileRequest {
    name: String,
    telemetry_schema: Value,
    metric_mapping: Value,
    reporting_settings: Value,
}

#[derive(Serialize)]
struct ManagementDeviceProfileResponse {
    id: Uuid,
    name: String,
    telemetry_schema: Value,
    metric_mapping: Value,
    reporting_settings: Value,
}

#[derive(Deserialize)]
struct ManagementAssetProfileRequest {
    name: String,
    fields: Value,
    dashboard_defaults: Value,
}

#[derive(Serialize)]
struct ManagementAssetProfileResponse {
    id: Uuid,
    name: String,
    fields: Value,
    dashboard_defaults: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementApplicationDomainProfileRequest {
    #[serde(default)]
    resource_kind: Option<String>,
    name: String,
    definition: Value,
    live_view: Value,
}

#[derive(Serialize)]
struct ManagementApplicationDomainProfileResponse {
    id: Uuid,
    resource_kind: &'static str,
    name: String,
    definition: Value,
    live_view: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementApplicationAssetProfileRelationRequest {
    parent_profile_id: Uuid,
    child_profile_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementApplicationProfileAssignmentRequest {
    profile_id: Option<Uuid>,
}

#[derive(Serialize)]
struct ManagementApplicationAssetProfileRelationResponse {
    id: Uuid,
    parent_profile_id: Uuid,
    child_profile_id: Uuid,
}

#[derive(Deserialize)]
struct ProvisionDeviceRequest {
    display_name: String,
    #[serde(default)]
    asset_id: Option<Uuid>,
    #[serde(default)]
    device_profile_id: Option<Uuid>,
    #[serde(default = "empty_json_object")]
    attributes: Value,
}

fn empty_json_object() -> Value {
    Value::Object(Map::new())
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
struct ManagementAssetRequest {
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: Value,
    attributes: Option<Value>,
}

#[derive(Deserialize)]
struct ManagementResourceOwnerRequest {
    user_id: Option<Uuid>,
}

#[derive(Serialize)]
struct ManagementAssetResponse {
    id: Uuid,
    name: String,
    owner_user_id: Option<Uuid>,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: Value,
    attributes: Value,
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
    owner_user_id: Option<Uuid>,
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

#[derive(Deserialize)]
struct ManagementDeviceTelemetryQuery {
    range: Option<String>,
}

#[derive(Serialize)]
struct ManagementDeviceTelemetryResponse {
    event_at: chrono::DateTime<chrono::Utc>,
    received_at: chrono::DateTime<chrono::Utc>,
    device_id: String,
    boot_id: String,
    sequence: i64,
    measurements: Value,
    topic: String,
}

#[derive(Serialize)]
struct ManagementDeviceTelemetryPage {
    items: Vec<ManagementDeviceTelemetryResponse>,
}

#[derive(Serialize)]
struct ManagementDeviceClaimCodeResponse {
    device_id: String,
    code: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

async fn login(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<PlatformLoginResponse>), ManagementSessionError> {
    let attempt_key = LoginAttemptKey {
        address: address.ip(),
        username: request.username.clone(),
    };
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(&attempt_key)
    };
    if !reserved {
        return Err(ManagementSessionError::TooManyRequests);
    }

    let principal =
        match authenticate_platform_account(&state.store, &request.username, &request.password)
            .await
        {
            Ok(principal) => principal,
            Err(AuthError::AuthenticationFailed) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .record_failure(&attempt_key);
                return Err(ManagementSessionError::Unauthorized);
            }
            Err(_) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .release(&attempt_key);
                return Err(ManagementSessionError::Unavailable);
            }
        };
    let session_id = match (principal.kind, principal.tenant_id) {
        (PrincipalKind::System, None) => {
            state.session_verifier.issue_system(principal.principal_id)
        }
        (PrincipalKind::Tenant, Some(tenant_id)) => state
            .session_verifier
            .issue_tenant(principal.principal_id, tenant_id),
        (PrincipalKind::User, Some(tenant_id)) => state
            .session_verifier
            .issue_user(principal.principal_id, tenant_id),
        _ => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .release(&attempt_key);
            return Err(ManagementSessionError::Unavailable);
        }
    };
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(&attempt_key);
    Ok((
        session_cookie_headers(&session_id),
        Json(PlatformLoginResponse {
            principal_kind: principal.kind,
            principal_id: principal.principal_id,
            tenant_id: principal.tenant_id,
        }),
    ))
}

async fn system_login(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<SystemLoginRequest>,
) -> Result<(HeaderMap, Json<SystemSessionResponse>), ManagementSessionError> {
    let attempt_key = LoginAttemptKey {
        address: address.ip(),
        username: request.username.clone(),
    };
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(&attempt_key)
    };
    if !reserved {
        return Err(ManagementSessionError::TooManyRequests);
    }

    let principal =
        match authenticate_system_account(&state.store, &request.username, &request.password).await
        {
            Ok(principal) => principal,
            Err(AuthError::AuthenticationFailed) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .record_failure(&attempt_key);
                return Err(ManagementSessionError::Unauthorized);
            }
            Err(_) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .release(&attempt_key);
                return Err(ManagementSessionError::Unavailable);
            }
        };
    let session_id = state.session_verifier.issue_system(principal.principal_id);
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(&attempt_key);
    Ok((
        session_cookie_headers(&session_id),
        Json(SystemSessionResponse {
            principal_kind: PrincipalKind::System,
            tenant_id: None,
        }),
    ))
}

async fn tenant_login(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<TenantLoginRequest>,
) -> Result<(HeaderMap, Json<TenantSessionResponse>), ManagementSessionError> {
    let attempt_key = LoginAttemptKey {
        address: address.ip(),
        username: request.tenant_slug.clone(),
    };
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(&attempt_key)
    };
    if !reserved {
        return Err(ManagementSessionError::TooManyRequests);
    }

    let principal =
        match authenticate_tenant_account(&state.store, &request.tenant_slug, &request.password)
            .await
        {
            Ok(principal) => principal,
            Err(AuthError::AuthenticationFailed) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .record_failure(&attempt_key);
                return Err(ManagementSessionError::Unauthorized);
            }
            Err(_) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .release(&attempt_key);
                return Err(ManagementSessionError::Unavailable);
            }
        };
    let tenant_id = principal
        .tenant_id
        .ok_or(ManagementSessionError::Unavailable)?;
    let session_id = state
        .session_verifier
        .issue_tenant(principal.principal_id, tenant_id);
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(&attempt_key);
    Ok((
        session_cookie_headers(&session_id),
        Json(TenantSessionResponse {
            principal_kind: PrincipalKind::Tenant,
            tenant_account_id: principal.principal_id,
            tenant_id,
        }),
    ))
}

async fn current_tenant_session(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<TenantSessionResponse>, ManagementSessionError> {
    let session = state
        .session_verifier
        .tenant_session(&headers)
        .ok_or(ManagementSessionError::Unauthorized)?;
    Ok(Json(TenantSessionResponse {
        principal_kind: PrincipalKind::Tenant,
        tenant_account_id: session.tenant_account_id,
        tenant_id: session.tenant_id,
    }))
}

async fn user_login(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<UserLoginRequest>,
) -> Result<(HeaderMap, Json<UserSessionResponse>), ManagementSessionError> {
    let attempt_key = LoginAttemptKey {
        address: address.ip(),
        username: format!("{}:{}", request.tenant_slug, request.username),
    };
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(&attempt_key)
    };
    if !reserved {
        return Err(ManagementSessionError::TooManyRequests);
    }

    let principal = match authenticate_user_account(
        &state.store,
        &request.tenant_slug,
        &request.username,
        &request.password,
    )
    .await
    {
        Ok(principal) => principal,
        Err(AuthError::AuthenticationFailed) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .record_failure(&attempt_key);
            return Err(ManagementSessionError::Unauthorized);
        }
        Err(_) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .release(&attempt_key);
            return Err(ManagementSessionError::Unavailable);
        }
    };
    let tenant_id = principal
        .tenant_id
        .ok_or(ManagementSessionError::Unavailable)?;
    let session_id = state
        .session_verifier
        .issue_user(principal.principal_id, tenant_id);
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(&attempt_key);
    Ok((
        session_cookie_headers(&session_id),
        Json(UserSessionResponse {
            principal_kind: PrincipalKind::User,
            user_id: principal.principal_id,
            tenant_id,
        }),
    ))
}

async fn current_user_session(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<UserSessionResponse>, ManagementSessionError> {
    let session = state
        .session_verifier
        .user_session(&headers)
        .ok_or(ManagementSessionError::Unauthorized)?;
    Ok(Json(UserSessionResponse {
        principal_kind: PrincipalKind::User,
        user_id: session.user_id,
        tenant_id: session.tenant_id,
    }))
}

async fn platform_root(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Redirect, ManagementSessionError> {
    let session = match state.session_verifier.platform_session(&headers) {
        Ok(session) => session,
        Err(ManagementSessionError::Unauthorized) => return Ok(Redirect::to("/login")),
        Err(error) => return Err(error),
    };
    match session {
        PlatformUiSession::System { .. } => Ok(Redirect::to("/system")),
        PlatformUiSession::Tenant { .. } => Ok(Redirect::to("/tenant")),
        PlatformUiSession::User { .. } => Ok(Redirect::to("/app")),
    }
}

async fn platform_login(request: Request) -> Result<Html<String>, ManagementSessionError> {
    let page = crate::PlatformLoginPage::new(login_error_requested(request.uri().query()));
    let rendered = crate::PlatformUiRenderer::render_login(&page)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

async fn platform_login_submit(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Form(request): Form<LoginRequest>,
) -> (HeaderMap, Redirect) {
    let attempt_key = LoginAttemptKey {
        address: address.ip(),
        username: request.username.clone(),
    };
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(&attempt_key)
    };
    if !reserved {
        return (HeaderMap::new(), Redirect::to("/login?error=invalid"));
    }

    let principal = match authenticate_platform_account(
        state.store.as_ref(),
        &request.username,
        &request.password,
    )
    .await
    {
        Ok(principal) => principal,
        Err(AuthError::AuthenticationFailed) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .record_failure(&attempt_key);
            return (HeaderMap::new(), Redirect::to("/login?error=invalid"));
        }
        Err(_) => {
            state
                .login_limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .release(&attempt_key);
            return (HeaderMap::new(), Redirect::to("/login?error=invalid"));
        }
    };

    let (session_id, destination) = match principal.kind {
        PrincipalKind::System => (
            state.session_verifier.issue_system(principal.principal_id),
            "/system",
        ),
        PrincipalKind::Tenant => {
            let tenant_id = if let Some(tenant_id) = principal.tenant_id {
                tenant_id
            } else {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .release(&attempt_key);
                return (HeaderMap::new(), Redirect::to("/login?error=invalid"));
            };
            (
                state
                    .session_verifier
                    .issue_tenant(principal.principal_id, tenant_id),
                "/tenant",
            )
        }
        PrincipalKind::User => {
            let tenant_id = if let Some(tenant_id) = principal.tenant_id {
                tenant_id
            } else {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .release(&attempt_key);
                return (HeaderMap::new(), Redirect::to("/login?error=invalid"));
            };
            (
                state
                    .session_verifier
                    .issue_user(principal.principal_id, tenant_id),
                "/app",
            )
        }
    };
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(&attempt_key);
    (
        session_cookie_headers(&session_id),
        Redirect::to(destination),
    )
}

async fn platform_logout(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> (HeaderMap, Redirect) {
    state.session_verifier.revoke(&headers);
    (expired_session_cookie_headers(), Redirect::to("/login"))
}

fn login_error_requested(query: Option<&str>) -> bool {
    query.is_some_and(|query| {
        query
            .split('&')
            .any(|parameter| parameter == "error=invalid")
    })
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
