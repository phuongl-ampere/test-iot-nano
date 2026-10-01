use std::{collections::HashMap, sync::Arc};

use axum::{
    Extension, Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use chrono::Utc;
use iot_nano_foundation::RpcMode;
use iot_storage::{
    ApplicationDomainResourceKind, AuthorizationRepository, CreateManagementAlertRule,
    DeviceClaimError, DeviceClaimRepository, ManagementAlertRule, ManagementAlertRuleError,
    ManagementAlertRuleRepository, ManagementAssetProfileRepository, ManagementAssetRepository,
    ManagementDeviceProfileRepository, ManagementDeviceRepository, ManagementUserRepository,
    NewPublicAsset, NewPublicDevice, OwnershipTransferTarget, PlatformStore, PublicAlert,
    PublicApiRepository, PublicAsset, PublicAssetError, PublicDevice, PublicDeviceError,
    PublicPrincipal, PublicTelemetry, ResourceInvitation, ResourceInvitationRepository,
    ResourcePermission, TenantProfileRepository, UpdateManagementAlertRule, UserCapability,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    CoreAuthorizedCommandCreateRequest, CoreCommandCreateRequest, CoreFacade, CoreFacadeError,
    DeviceTokenResponse, DeviceTokenStoreError, TokenVault,
    auth::{BearerAccessTokenError, extract_bearer_access_token, validate_bearer_access_token},
    reveal_platform_device_token, rotate_platform_device_token,
};

#[derive(Clone)]
pub(crate) struct PublicApiContext {
    pub(crate) store: Option<Arc<PlatformStore>>,
    pub(crate) token_vault: TokenVault,
    pub(crate) core_facade: Option<Arc<dyn CoreFacade>>,
}

impl PublicApiContext {
    pub(crate) fn new(
        store: Option<Arc<PlatformStore>>,
        token_vault: TokenVault,
        core_facade: Option<Arc<dyn CoreFacade>>,
    ) -> Self {
        Self {
            store,
            token_vault,
            core_facade,
        }
    }
}

pub(crate) fn router<S>(context: PublicApiContext) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/v1/tenant-profile/profiles", get(list_tenant_profiles))
        .route("/api/v1/user-capabilities", get(list_user_capabilities))
        .route("/api/v1/asset-profiles", get(list_asset_profiles))
        .route("/api/v1/assets", get(list_assets).post(create_asset))
        .route(
            "/api/v1/assets/{asset_id}",
            get(get_asset).patch(update_asset).delete(delete_asset),
        )
        .route(
            "/api/v1/assets/{asset_id}/resource-invitations",
            axum::routing::post(create_asset_resource_invitation),
        )
        .route(
            "/api/v1/assets/{asset_id}/live-view",
            get(get_asset_live_view),
        )
        .route(
            "/api/v1/assets/{asset_id}/tenant-profile",
            put(assign_asset_tenant_profile),
        )
        .route("/api/v1/telemetry", get(list_telemetry))
        .route("/api/v1/telemetry/{device_id}", get(get_telemetry))
        .route("/api/v1/alerts", get(list_alerts))
        .route("/api/v1/alerts/{alert_id}", get(get_alert))
        .route(
            "/api/v1/alerts/{alert_id}/acknowledge",
            axum::routing::post(acknowledge_alert),
        )
        .route("/api/v1/device-profiles", get(list_device_profiles))
        .route("/api/v1/devices", get(list_devices).post(create_device))
        .route("/api/v1/devices/claim", axum::routing::post(claim_device))
        .route(
            "/api/v1/devices/{device_id}/token",
            get(reveal_device_token).post(rotate_device_token),
        )
        .route(
            "/api/v1/devices/{device_id}/alert-rules",
            get(list_device_alert_rules).post(create_device_alert_rule),
        )
        .route(
            "/api/v1/devices/{device_id}/alert-rules/{rule_id}",
            put(update_device_alert_rule).delete(archive_device_alert_rule),
        )
        .route(
            "/api/v1/devices/{device_id}",
            get(get_device).patch(update_device).delete(delete_device),
        )
        .route(
            "/api/v1/devices/{device_id}/resource-invitations",
            axum::routing::post(create_device_resource_invitation),
        )
        .route(
            "/api/v1/devices/{device_id}/live-view",
            get(get_device_live_view),
        )
        .route(
            "/api/v1/devices/{device_id}/tenant-profile",
            put(assign_device_tenant_profile),
        )
        .route(
            "/api/v1/devices/{device_id}/commands",
            axum::routing::post(create_command),
        )
        .route("/api/v1/commands/{command_id}", get(get_command))
        .route(
            "/api/v1/resource-invitations",
            get(list_resource_invitations),
        )
        .route(
            "/api/v1/resource-invitations/{invitation_id}/accept",
            axum::routing::post(accept_resource_invitation),
        )
        .route(
            "/api/v1/resource-invitations/{invitation_id}/cancel",
            axum::routing::post(cancel_resource_invitation),
        )
        .layer(Extension(context))
}

pub fn public_v1_router<S>(
    store: Arc<PlatformStore>,
    token_vault: TokenVault,
    core_facade: Arc<dyn CoreFacade>,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router(PublicApiContext::new(
        Some(store),
        token_vault,
        Some(core_facade),
    ))
}

#[derive(Debug, Clone, Copy)]
enum PublicApiError {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    Unavailable,
}

impl From<BearerAccessTokenError> for PublicApiError {
    fn from(error: BearerAccessTokenError) -> Self {
        match error {
            BearerAccessTokenError::Missing | BearerAccessTokenError::Denied => Self::Unauthorized,
            BearerAccessTokenError::Unavailable => Self::Unavailable,
        }
    }
}

impl IntoResponse for PublicApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::BadRequest => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the request is invalid",
            ),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "OAuth bearer authentication is required",
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "the access token is not authorized for this resource",
            ),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "the requested resource was not found",
            ),
            Self::Conflict => (
                StatusCode::CONFLICT,
                "conflict",
                "the request conflicts with an existing resource",
            ),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                "the service is temporarily unavailable",
            ),
        };
        (
            status,
            Json(json!({
                "code": code,
                "message": message,
                "request_id": Uuid::now_v7().to_string(),
                "details": Value::Null,
            })),
        )
            .into_response()
    }
}

#[derive(Debug, Deserialize)]
struct PublicPageQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct PublicPage<T> {
    items: Vec<T>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct AssetResponse {
    id: Uuid,
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: Value,
    attributes: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_permission: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_source: Option<&'static str>,
}

#[derive(Debug, Deserialize)]
struct CreateAssetRequest {
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    #[serde(default)]
    metadata: Value,
    attributes: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct UpdateAssetRequest {
    name: Option<String>,
    #[serde(default)]
    asset_profile_id: OptionalUpdate<Uuid>,
    #[serde(default)]
    parent_asset_id: OptionalUpdate<Uuid>,
    metadata: Option<Value>,
    attributes: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AssetCursor {
    version: u8,
    subject_user_id: Option<Uuid>,
    app_id: String,
    asset_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct CreateDeviceRequest {
    device_id: String,
    display_name: Option<String>,
    #[serde(default = "default_metadata")]
    metadata: Value,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct UpdateDeviceRequest {
    display_name: Option<String>,
    metadata: Option<Value>,
    #[serde(default)]
    asset_id: OptionalUpdate<Uuid>,
    #[serde(default)]
    device_profile_id: OptionalUpdate<Uuid>,
}

#[derive(Debug)]
enum OptionalUpdate<T> {
    Absent,
    Value(Option<T>),
}

impl<T> Default for OptionalUpdate<T> {
    fn default() -> Self {
        Self::Absent
    }
}

impl<'de, T> Deserialize<'de> for OptionalUpdate<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::Value(Option::<T>::deserialize(deserializer)?))
    }
}

impl<T> OptionalUpdate<T> {
    fn is_present(&self) -> bool {
        matches!(self, Self::Value(_))
    }

    fn resolve(self, current: Option<T>) -> Option<T> {
        match self {
            Self::Absent => current,
            Self::Value(value) => value,
        }
    }
}

#[derive(Debug, Serialize)]
struct ProfileCatalogEntryResponse {
    id: Uuid,
    name: String,
}

#[derive(Debug, Deserialize)]
struct TenantProfileQuery {
    kind: String,
}

#[derive(Debug, Deserialize)]
struct TenantProfileAssignmentRequest {
    profile_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct TenantProfileAssignmentResponse {
    profile_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct DeviceResponse {
    device_id: String,
    serial_number: Option<String>,
    display_name: Option<String>,
    metadata: Value,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
    online: bool,
    last_seen_at: Option<chrono::DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_permission: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_source: Option<&'static str>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimDeviceRequest {
    serial_number: String,
    code: String,
}

#[derive(Debug, Serialize)]
struct UserCapabilitiesResponse {
    capabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicDeviceAlertRuleRequest {
    name: String,
    #[serde(default = "default_alert_rule_enabled")]
    enabled: bool,
    metric_key: String,
    #[serde(default = "default_alert_rule_type")]
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

#[derive(Debug, Serialize)]
struct PublicDeviceAlertRuleResponse {
    id: Uuid,
    name: String,
    enabled: bool,
    device_id: String,
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
    updated_at: chrono::DateTime<chrono::Utc>,
}

fn default_alert_rule_enabled() -> bool {
    true
}

fn default_alert_rule_type() -> String {
    "event_threshold".to_owned()
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

#[derive(Debug, Deserialize)]
struct CreateResourceInvitationRequest {
    username: String,
    permission: String,
}

#[derive(Debug, Serialize)]
struct ResourceInvitationResponse {
    id: Uuid,
    resource_kind: &'static str,
    resource_id: String,
    resource_name: String,
    sender_username: String,
    permission: &'static str,
}

const DEFAULT_PUBLIC_LIMIT: usize = 50;
const MAX_PUBLIC_LIMIT: usize = 100;

async fn authenticate(
    context: &PublicApiContext,
    headers: &HeaderMap,
    scope: &str,
) -> Result<(Arc<PlatformStore>, PublicPrincipal), PublicApiError> {
    extract_bearer_access_token(headers).map_err(PublicApiError::from)?;
    let store = context.store.clone().ok_or(PublicApiError::Unavailable)?;
    let token = validate_bearer_access_token(store.as_ref(), headers, Utc::now()).await?;
    if !token.allows_scope(scope) {
        return Err(PublicApiError::Forbidden);
    }
    let user_id = token.user_id.ok_or(PublicApiError::Forbidden)?;
    let tenant_id = PublicApiRepository::public_user_tenant_id(store.as_ref(), user_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    if tenant_id != token.tenant_id {
        return Err(PublicApiError::Forbidden);
    }
    let account_class = AuthorizationRepository::authorization_subject(store.as_ref(), user_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?
        .account_class;
    Ok((
        store,
        PublicPrincipal {
            tenant_id,
            user_id: Some(user_id),
            app_id: token.app_id,
            account_class,
        },
    ))
}

async fn require_user_capability(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    capability: UserCapability,
) -> Result<(), PublicApiError> {
    let user_id = principal.user_id.ok_or(PublicApiError::Forbidden)?;
    let enabled = ManagementUserRepository::user_has_management_capability(
        store,
        principal.tenant_id,
        user_id,
        capability,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    if enabled {
        Ok(())
    } else {
        Err(PublicApiError::Forbidden)
    }
}

async fn list_user_capabilities(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
) -> Result<Json<UserCapabilitiesResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:read").await?;
    let user_id = principal.user_id.ok_or(PublicApiError::Forbidden)?;
    let user = ManagementUserRepository::list_management_users(store.as_ref(), principal.tenant_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .into_iter()
        .find(|user| user.id == user_id)
        .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(UserCapabilitiesResponse {
        capabilities: user
            .capabilities
            .into_iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
    }))
}

fn limit(value: Option<usize>) -> Result<usize, PublicApiError> {
    let value = value.unwrap_or(DEFAULT_PUBLIC_LIMIT);
    if !(1..=MAX_PUBLIC_LIMIT).contains(&value) {
        return Err(PublicApiError::BadRequest);
    }
    Ok(value)
}

fn decode_asset_cursor(
    vault: &TokenVault,
    principal: &PublicPrincipal,
    cursor: Option<&str>,
) -> Result<Option<String>, PublicApiError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let plaintext = vault
        .decrypt(cursor)
        .map_err(|_| PublicApiError::BadRequest)?;
    let cursor: AssetCursor =
        serde_json::from_str(&plaintext).map_err(|_| PublicApiError::BadRequest)?;
    if cursor.version != 1
        || cursor.subject_user_id != principal.user_id
        || cursor.app_id != principal.app_id
    {
        return Err(PublicApiError::BadRequest);
    }
    Ok(Some(cursor.asset_id.to_string()))
}

fn encode_asset_cursor(
    vault: &TokenVault,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<String, PublicApiError> {
    let plaintext = serde_json::to_string(&AssetCursor {
        version: 1,
        subject_user_id: principal.user_id,
        app_id: principal.app_id.clone(),
        asset_id,
    })
    .map_err(|_| PublicApiError::Unavailable)?;
    vault
        .encrypt(&plaintext)
        .map_err(|_| PublicApiError::Unavailable)
}

async fn list_assets(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Query(query): Query<PublicPageQuery>,
) -> Result<Json<PublicPage<AssetResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:read").await?;
    let limit = limit(query.limit)?;
    let after = decode_asset_cursor(&context.token_vault, &principal, query.after.as_deref())?;
    let mut assets = PublicApiRepository::list_public_assets(
        store.as_ref(),
        &principal,
        after.as_deref(),
        u32::try_from(limit.saturating_add(1)).map_err(|_| PublicApiError::BadRequest)?,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    let has_more = assets.len() > limit;
    assets.truncate(limit);
    let next_cursor = if has_more {
        assets
            .last()
            .map(|asset| encode_asset_cursor(&context.token_vault, &principal, asset.id))
            .transpose()?
    } else {
        None
    };
    Ok(Json(PublicPage {
        items: assets.into_iter().map(asset_response).collect(),
        next_cursor,
        has_more,
    }))
}

async fn list_tenant_profiles(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Query(query): Query<TenantProfileQuery>,
) -> Result<Json<Vec<ProfileCatalogEntryResponse>>, PublicApiError> {
    let resource_kind =
        ApplicationDomainResourceKind::parse(&query.kind).ok_or(PublicApiError::BadRequest)?;
    let scope = match resource_kind {
        ApplicationDomainResourceKind::Asset => "assets:read",
        ApplicationDomainResourceKind::Device => "devices:read",
    };
    let (store, principal) = authenticate(&context, &headers, scope).await?;
    let profiles = TenantProfileRepository::list_tenant_profile_definitions(
        store.as_ref(),
        principal.tenant_id,
        Some(resource_kind),
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    Ok(Json(
        profiles
            .into_iter()
            .map(|profile| ProfileCatalogEntryResponse {
                id: profile.id,
                name: profile.name,
            })
            .collect(),
    ))
}

async fn list_asset_profiles(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
) -> Result<Json<Vec<ProfileCatalogEntryResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:read").await?;
    let profiles = ManagementAssetProfileRepository::list_management_asset_profiles(
        store.as_ref(),
        principal.tenant_id,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    Ok(Json(
        profiles
            .into_iter()
            .map(|profile| ProfileCatalogEntryResponse {
                id: profile.id,
                name: profile.name,
            })
            .collect(),
    ))
}

async fn get_asset(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
) -> Result<Json<AssetResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:read").await?;
    let asset_id = asset_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let asset = PublicApiRepository::get_public_asset(store.as_ref(), &principal, asset_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(asset_response(asset)))
}

async fn get_asset_live_view(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
) -> Result<Json<LiveViewResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:read").await?;
    let asset_id = asset_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    PublicApiRepository::get_public_asset(store.as_ref(), &principal, asset_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    let Some(profile) = TenantProfileRepository::tenant_profile_assignment(
        store.as_ref(),
        principal.tenant_id,
        ApplicationDomainResourceKind::Asset,
        &asset_id.to_string(),
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?
    else {
        return Ok(Json(LiveViewResponse::empty()));
    };
    Ok(Json(LiveViewResponse {
        profile: Some(LiveViewProfileResponse {
            id: profile.id,
            name: profile.name,
        }),
        charts: live_charts(&profile.live_view),
    }))
}

async fn assign_asset_tenant_profile(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
    Json(request): Json<TenantProfileAssignmentRequest>,
) -> Result<Json<TenantProfileAssignmentResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:write").await?;
    require_user_capability(
        store.as_ref(),
        &principal,
        UserCapability::AssignApplicationProfiles,
    )
    .await?;
    let asset_id = asset_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    if !PublicApiRepository::public_asset_permission(store.as_ref(), &principal, asset_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Err(PublicApiError::Forbidden);
    }
    TenantProfileRepository::assign_tenant_profile(
        store.as_ref(),
        principal.tenant_id,
        ApplicationDomainResourceKind::Asset,
        &asset_id.to_string(),
        request.profile_id,
    )
    .await
    .map_err(|error| match error {
        iot_storage::ApplicationDomainProfileError::ProfileNotFound
        | iot_storage::ApplicationDomainProfileError::ProfileKindMismatch
        | iot_storage::ApplicationDomainProfileError::ResourceNotFound => PublicApiError::Conflict,
        _ => PublicApiError::Unavailable,
    })?;
    Ok(Json(TenantProfileAssignmentResponse {
        profile_id: request.profile_id,
    }))
}

async fn create_asset(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Json(request): Json<CreateAssetRequest>,
) -> Result<(axum::http::StatusCode, Json<AssetResponse>), PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:write").await?;
    require_user_capability(store.as_ref(), &principal, UserCapability::CreateAssets).await?;
    let name = validate_name(request.name)?;
    let metadata = request.attributes.unwrap_or(request.metadata);
    if !metadata.is_object() {
        return Err(PublicApiError::BadRequest);
    }
    let asset = PublicApiRepository::create_public_asset(
        store.as_ref(),
        &principal,
        NewPublicAsset {
            name,
            asset_profile_id: request.asset_profile_id,
            parent_asset_id: request.parent_asset_id,
            metadata,
        },
    )
    .await
    .map_err(public_asset_error)?;
    Ok((axum::http::StatusCode::CREATED, Json(asset_response(asset))))
}

async fn update_asset(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
    Json(request): Json<UpdateAssetRequest>,
) -> Result<Json<AssetResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:write").await?;
    require_user_capability(store.as_ref(), &principal, UserCapability::EditResources).await?;
    let asset_id = asset_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let current = PublicApiRepository::get_public_asset(store.as_ref(), &principal, asset_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    let metadata = request
        .attributes
        .or(request.metadata)
        .unwrap_or_else(|| current.metadata.clone());
    if !metadata.is_object() {
        return Err(PublicApiError::BadRequest);
    }
    let asset = PublicApiRepository::update_public_asset(
        store.as_ref(),
        &principal,
        asset_id,
        NewPublicAsset {
            name: validate_name(request.name.unwrap_or(current.name))?,
            asset_profile_id: request.asset_profile_id.resolve(current.asset_profile_id),
            parent_asset_id: request.parent_asset_id.resolve(current.parent_asset_id),
            metadata,
        },
    )
    .await
    .map_err(public_asset_error)?
    .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(asset_response(asset)))
}

async fn delete_asset(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
) -> Result<axum::http::StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:write").await?;
    require_user_capability(store.as_ref(), &principal, UserCapability::EditResources).await?;
    let asset_id = asset_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let deleted = PublicApiRepository::delete_public_asset(store.as_ref(), &principal, asset_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?;
    if deleted {
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(PublicApiError::Forbidden)
    }
}

fn validate_name(name: String) -> Result<String, PublicApiError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 200 {
        return Err(PublicApiError::BadRequest);
    }
    Ok(name.to_owned())
}

fn default_metadata() -> Value {
    Value::Object(serde_json::Map::new())
}

fn validate_device_id(device_id: String) -> Result<String, PublicApiError> {
    let device_id = device_id.trim();
    if device_id.is_empty() || device_id.len() > 200 {
        return Err(PublicApiError::BadRequest);
    }
    Ok(device_id.to_owned())
}

fn validate_serial_number(serial_number: String) -> Result<String, PublicApiError> {
    let serial_number = serial_number.trim().to_ascii_uppercase();
    if serial_number.is_empty()
        || serial_number.len() > 128
        || !serial_number.bytes().all(|value| {
            value.is_ascii_alphanumeric() || value == b'-' || value == b'_' || value == b'.'
        })
    {
        return Err(PublicApiError::BadRequest);
    }
    Ok(serial_number)
}

fn validate_display_name(display_name: Option<String>) -> Result<Option<String>, PublicApiError> {
    display_name
        .map(|display_name| validate_name(display_name))
        .transpose()
}

fn validate_metadata(metadata: Value) -> Result<Value, PublicApiError> {
    if metadata.is_object() {
        Ok(metadata)
    } else {
        Err(PublicApiError::BadRequest)
    }
}

fn asset_response(asset: PublicAsset) -> AssetResponse {
    let access = asset.access;
    AssetResponse {
        id: asset.id,
        name: asset.name,
        asset_profile_id: asset.asset_profile_id,
        parent_asset_id: asset.parent_asset_id,
        attributes: asset.metadata.clone(),
        metadata: asset.metadata,
        effective_permission: access.map(|access| access.permission.as_str()),
        access_source: access.map(|access| access.source.as_str()),
    }
}

async fn create_asset_resource_invitation(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
    Json(request): Json<CreateResourceInvitationRequest>,
) -> Result<(StatusCode, Json<ResourceInvitationResponse>), PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let asset_id = asset_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let invitation = create_resource_invitation(
        store.as_ref(),
        &principal,
        OwnershipTransferTarget::Asset(asset_id),
        request,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(resource_invitation_response(store.as_ref(), invitation).await?),
    ))
}

async fn create_device(
    Extension(context): Extension<PublicApiContext>,
    request: Request,
) -> Result<(axum::http::StatusCode, Json<DeviceResponse>), PublicApiError> {
    let headers = request.headers().clone();
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    require_user_capability(store.as_ref(), &principal, UserCapability::CreateDevices).await?;
    let request: CreateDeviceRequest = serde_json::from_slice(
        &to_bytes(request.into_body(), 1024 * 1024)
            .await
            .map_err(|_| PublicApiError::BadRequest)?,
    )
    .map_err(|_| PublicApiError::BadRequest)?;
    if request.device_profile_id.is_some() {
        return Err(PublicApiError::Forbidden);
    }
    let device = PublicApiRepository::create_public_device(
        store.as_ref(),
        &principal,
        NewPublicDevice {
            device_id: validate_device_id(request.device_id)?,
            display_name: validate_display_name(request.display_name)?,
            metadata: validate_metadata(request.metadata)?,
            asset_id: request.asset_id,
            device_profile_id: request.device_profile_id,
        },
    )
    .await
    .map_err(public_device_error)?;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(device_response(device, None)),
    ))
}

async fn claim_device(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Json(request): Json<ClaimDeviceRequest>,
) -> Result<Json<DeviceResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    require_user_capability(store.as_ref(), &principal, UserCapability::ClaimDevices).await?;
    let user_id = principal.user_id.ok_or(PublicApiError::Forbidden)?;
    let serial_number = validate_serial_number(request.serial_number)?;
    let claimed = DeviceClaimRepository::claim_device_with_serial_number(
        store.as_ref(),
        principal.tenant_id,
        user_id,
        &serial_number,
        &request.code,
    )
    .await
    .map_err(public_device_claim_error)?;
    let device =
        PublicApiRepository::get_public_device(store.as_ref(), &principal, &claimed.device_id)
            .await
            .map_err(|_| PublicApiError::Unavailable)?
            .ok_or(PublicApiError::Forbidden)?;
    let health = device_health_by_id(store.as_ref(), principal.tenant_id).await?;
    Ok(Json(device_response(
        device,
        health.get(&claimed.device_id),
    )))
}

async fn list_devices(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Query(query): Query<PublicPageQuery>,
) -> Result<Json<PublicPage<DeviceResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:read").await?;
    let limit = limit(query.limit)?;
    let after = decode_cursor(
        &context.token_vault,
        &principal,
        "device",
        query.after.as_deref(),
    )?;
    let mut devices = PublicApiRepository::list_public_devices(
        store.as_ref(),
        &principal,
        after.as_deref(),
        u32::try_from(limit.saturating_add(1)).map_err(|_| PublicApiError::BadRequest)?,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    let has_more = devices.len() > limit;
    devices.truncate(limit);
    let device_health = device_health_by_id(store.as_ref(), principal.tenant_id).await?;
    let next_cursor = if has_more {
        devices
            .last()
            .map(|device| {
                encode_cursor(
                    &context.token_vault,
                    &principal,
                    "device",
                    device.device_id.clone(),
                )
            })
            .transpose()?
    } else {
        None
    };
    Ok(Json(PublicPage {
        items: devices
            .into_iter()
            .map(|device| {
                let health = device_health.get(&device.device_id);
                device_response(device, health)
            })
            .collect(),
        next_cursor,
        has_more,
    }))
}

async fn list_device_profiles(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
) -> Result<Json<Vec<ProfileCatalogEntryResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:read").await?;
    let profiles = ManagementDeviceProfileRepository::list_management_device_profiles(
        store.as_ref(),
        principal.tenant_id,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    Ok(Json(
        profiles
            .into_iter()
            .map(|profile| ProfileCatalogEntryResponse {
                id: profile.id,
                name: profile.name,
            })
            .collect(),
    ))
}

async fn get_device(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<DeviceResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:read").await?;
    if !PublicApiRepository::public_device_permission(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(ResourcePermission::Viewer))
    {
        return Err(PublicApiError::Forbidden);
    }
    let device = PublicApiRepository::get_public_device(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    let device_health = device_health_by_id(store.as_ref(), principal.tenant_id).await?;
    Ok(Json(device_response(device, device_health.get(&device_id))))
}

async fn get_device_live_view(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<LiveViewResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:read").await?;
    PublicApiRepository::get_public_device(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    let Some(profile) = TenantProfileRepository::tenant_profile_assignment(
        store.as_ref(),
        principal.tenant_id,
        ApplicationDomainResourceKind::Device,
        &device_id,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?
    else {
        return Ok(Json(LiveViewResponse::empty()));
    };
    Ok(Json(LiveViewResponse {
        profile: Some(LiveViewProfileResponse {
            id: profile.id,
            name: profile.name,
        }),
        charts: live_charts(&profile.live_view),
    }))
}

async fn assign_device_tenant_profile(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(request): Json<TenantProfileAssignmentRequest>,
) -> Result<Json<TenantProfileAssignmentResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    require_user_capability(
        store.as_ref(),
        &principal,
        UserCapability::AssignApplicationProfiles,
    )
    .await?;
    if !PublicApiRepository::public_device_permission(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Err(PublicApiError::Forbidden);
    }
    TenantProfileRepository::assign_tenant_profile(
        store.as_ref(),
        principal.tenant_id,
        ApplicationDomainResourceKind::Device,
        &device_id,
        request.profile_id,
    )
    .await
    .map_err(|error| match error {
        iot_storage::ApplicationDomainProfileError::ProfileNotFound
        | iot_storage::ApplicationDomainProfileError::ProfileKindMismatch
        | iot_storage::ApplicationDomainProfileError::ResourceNotFound => PublicApiError::Conflict,
        _ => PublicApiError::Unavailable,
    })?;
    Ok(Json(TenantProfileAssignmentResponse {
        profile_id: request.profile_id,
    }))
}

async fn reveal_device_token(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<DeviceTokenResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    require_user_capability(
        store.as_ref(),
        &principal,
        UserCapability::ManageDeviceTokens,
    )
    .await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Owner,
    )
    .await?;
    let token = reveal_platform_device_token(
        store.as_ref(),
        &context.token_vault,
        principal.tenant_id,
        &device_id,
    )
    .await
    .map_err(public_device_token_error)?;
    Ok(Json(token))
}

async fn rotate_device_token(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<DeviceTokenResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    require_user_capability(
        store.as_ref(),
        &principal,
        UserCapability::ManageDeviceTokens,
    )
    .await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Owner,
    )
    .await?;
    let active = reveal_platform_device_token(
        store.as_ref(),
        &context.token_vault,
        principal.tenant_id,
        &device_id,
    )
    .await
    .map_err(public_device_token_error)?;
    let token = rotate_platform_device_token(
        store.as_ref(),
        &context.token_vault,
        principal.tenant_id,
        active.id,
    )
    .await
    .map_err(public_device_token_error)?;
    Ok(Json(token))
}

async fn list_device_alert_rules(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<PublicPage<PublicDeviceAlertRuleResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:read").await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Manager,
    )
    .await?;
    let items = ManagementAlertRuleRepository::list_management_alert_rules(
        store.as_ref(),
        principal.tenant_id,
    )
    .await
    .map_err(public_alert_rule_error)?
    .into_iter()
    .filter(|rule| rule.device_id.as_deref() == Some(device_id.as_str()))
    .map(public_device_alert_rule_response)
    .collect();
    Ok(Json(PublicPage {
        items,
        next_cursor: None,
        has_more: false,
    }))
}

async fn create_device_alert_rule(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(request): Json<PublicDeviceAlertRuleRequest>,
) -> Result<(StatusCode, Json<PublicDeviceAlertRuleResponse>), PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:write").await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Manager,
    )
    .await?;
    let rule = ManagementAlertRuleRepository::create_management_alert_rule(
        store.as_ref(),
        principal.tenant_id,
        create_device_alert_rule_input(request, device_id),
    )
    .await
    .map_err(public_alert_rule_error)?;
    Ok((
        StatusCode::CREATED,
        Json(public_device_alert_rule_response(rule)),
    ))
}

async fn update_device_alert_rule(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path((device_id, rule_id)): Path<(String, Uuid)>,
    Json(request): Json<PublicDeviceAlertRuleRequest>,
) -> Result<Json<PublicDeviceAlertRuleResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:write").await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Manager,
    )
    .await?;
    ensure_device_alert_rule(store.as_ref(), principal.tenant_id, &device_id, rule_id).await?;
    let rule = ManagementAlertRuleRepository::update_management_alert_rule(
        store.as_ref(),
        principal.tenant_id,
        rule_id,
        update_device_alert_rule_input(request, device_id),
    )
    .await
    .map_err(public_alert_rule_error)?;
    Ok(Json(public_device_alert_rule_response(rule)))
}

async fn archive_device_alert_rule(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path((device_id, rule_id)): Path<(String, Uuid)>,
) -> Result<StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:write").await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Manager,
    )
    .await?;
    ensure_device_alert_rule(store.as_ref(), principal.tenant_id, &device_id, rule_id).await?;
    ManagementAlertRuleRepository::archive_management_alert_rule(
        store.as_ref(),
        principal.tenant_id,
        rule_id,
    )
    .await
    .map_err(public_alert_rule_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn require_device_permission(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: &str,
    required: ResourcePermission,
) -> Result<(), PublicApiError> {
    if PublicApiRepository::public_device_permission(store, principal, device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(required))
    {
        Ok(())
    } else {
        Err(PublicApiError::Forbidden)
    }
}

async fn ensure_device_alert_rule(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    rule_id: Uuid,
) -> Result<(), PublicApiError> {
    let rule = ManagementAlertRuleRepository::list_management_alert_rules(store, tenant_id)
        .await
        .map_err(public_alert_rule_error)?
        .into_iter()
        .find(|rule| rule.id == rule_id && rule.device_id.as_deref() == Some(device_id));
    if rule.is_some() {
        Ok(())
    } else {
        Err(PublicApiError::NotFound)
    }
}

fn create_device_alert_rule_input(
    request: PublicDeviceAlertRuleRequest,
    device_id: String,
) -> CreateManagementAlertRule {
    CreateManagementAlertRule {
        name: request.name,
        enabled: request.enabled,
        device_id: Some(device_id),
        metric_key: request.metric_key,
        rule_type: request.rule_type,
        comparison: request.comparison,
        threshold: request.threshold,
        window_seconds: request.window_seconds,
        for_seconds: request.for_seconds,
        resolve_after_seconds: request.resolve_after_seconds,
        reopen_grace_seconds: request.reopen_grace_seconds,
        hysteresis: request.hysteresis,
        severity: request.severity,
        reminder_interval_seconds: request.reminder_interval_seconds,
    }
}

fn update_device_alert_rule_input(
    request: PublicDeviceAlertRuleRequest,
    device_id: String,
) -> UpdateManagementAlertRule {
    UpdateManagementAlertRule {
        name: request.name,
        enabled: request.enabled,
        device_id: Some(device_id),
        metric_key: request.metric_key,
        rule_type: request.rule_type,
        comparison: request.comparison,
        threshold: request.threshold,
        window_seconds: request.window_seconds,
        for_seconds: request.for_seconds,
        resolve_after_seconds: request.resolve_after_seconds,
        reopen_grace_seconds: request.reopen_grace_seconds,
        hysteresis: request.hysteresis,
        severity: request.severity,
        reminder_interval_seconds: request.reminder_interval_seconds,
    }
}

fn public_device_alert_rule_response(rule: ManagementAlertRule) -> PublicDeviceAlertRuleResponse {
    PublicDeviceAlertRuleResponse {
        id: rule.id,
        name: rule.name,
        enabled: rule.enabled,
        device_id: rule.device_id.unwrap_or_default(),
        metric_key: rule.metric_key,
        rule_type: rule.rule_type,
        comparison: rule.comparison,
        threshold: rule.threshold,
        window_seconds: rule.window_seconds,
        for_seconds: rule.for_seconds,
        resolve_after_seconds: rule.resolve_after_seconds,
        reopen_grace_seconds: rule.reopen_grace_seconds,
        hysteresis: rule.hysteresis,
        severity: rule.severity,
        reminder_interval_seconds: rule.reminder_interval_seconds,
        updated_at: rule.updated_at,
    }
}

fn public_device_token_error(error: DeviceTokenStoreError) -> PublicApiError {
    match error {
        DeviceTokenStoreError::NotFound => PublicApiError::NotFound,
        DeviceTokenStoreError::GatewayChild => PublicApiError::Conflict,
        _ => PublicApiError::Unavailable,
    }
}

fn public_alert_rule_error(error: ManagementAlertRuleError) -> PublicApiError {
    match error {
        ManagementAlertRuleError::InvalidName
        | ManagementAlertRuleError::InvalidMetricKey
        | ManagementAlertRuleError::InvalidRuleType
        | ManagementAlertRuleError::InvalidComparison
        | ManagementAlertRuleError::InvalidThreshold
        | ManagementAlertRuleError::InvalidWindow
        | ManagementAlertRuleError::InvalidDuration
        | ManagementAlertRuleError::InvalidHysteresis
        | ManagementAlertRuleError::InvalidSeverity => PublicApiError::BadRequest,
        ManagementAlertRuleError::DeviceUnavailable(_) => PublicApiError::Conflict,
        ManagementAlertRuleError::RuleNotFound | ManagementAlertRuleError::RuleArchived => {
            PublicApiError::NotFound
        }
        ManagementAlertRuleError::InvalidStoredRule | ManagementAlertRuleError::Storage { .. } => {
            PublicApiError::Unavailable
        }
    }
}

async fn update_device(
    Extension(context): Extension<PublicApiContext>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Json<DeviceResponse>, PublicApiError> {
    let headers = request.headers().clone();
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    if !PublicApiRepository::public_device_permission(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Err(PublicApiError::Forbidden);
    }
    let current = PublicApiRepository::get_public_device(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    let request: UpdateDeviceRequest = serde_json::from_slice(
        &to_bytes(request.into_body(), 1024 * 1024)
            .await
            .map_err(|_| PublicApiError::BadRequest)?,
    )
    .map_err(|_| PublicApiError::BadRequest)?;
    if request.device_profile_id.is_present() {
        return Err(PublicApiError::Forbidden);
    }
    if request.asset_id.is_present() {
        require_user_capability(
            store.as_ref(),
            &principal,
            UserCapability::AssignDevicesToAssets,
        )
        .await?;
        if let OptionalUpdate::Value(Some(asset_id)) = &request.asset_id {
            if !PublicApiRepository::public_asset_permission(store.as_ref(), &principal, *asset_id)
                .await
                .map_err(|_| PublicApiError::Unavailable)?
                .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
            {
                return Err(PublicApiError::Forbidden);
            }
        }
    }
    if request.display_name.is_some() || request.metadata.is_some() {
        require_user_capability(store.as_ref(), &principal, UserCapability::EditResources).await?;
    }
    let device = PublicApiRepository::update_public_device(
        store.as_ref(),
        &principal,
        &device_id,
        NewPublicDevice {
            device_id: device_id.clone(),
            display_name: validate_display_name(request.display_name)?.or(current.display_name),
            metadata: validate_metadata(request.metadata.unwrap_or(current.metadata))?,
            asset_id: request.asset_id.resolve(current.asset_id),
            device_profile_id: request.device_profile_id.resolve(current.device_profile_id),
        },
    )
    .await
    .map_err(public_device_error)?
    .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(device_response(device, None)))
}

async fn delete_device(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<axum::http::StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    require_user_capability(store.as_ref(), &principal, UserCapability::EditResources).await?;
    if PublicApiRepository::delete_public_device(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
    {
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(PublicApiError::Forbidden)
    }
}

type DeviceHealth = (bool, Option<chrono::DateTime<Utc>>);

async fn device_health_by_id(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<HashMap<String, DeviceHealth>, PublicApiError> {
    ManagementDeviceRepository::list_management_devices(store, tenant_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)
        .map(|devices| {
            devices
                .into_iter()
                .map(|device| {
                    (
                        device.device_id,
                        (device.health.online, device.health.last_seen_at),
                    )
                })
                .collect()
        })
}

fn device_response(device: PublicDevice, health: Option<&DeviceHealth>) -> DeviceResponse {
    let access = device.access;
    let (online, last_seen_at) = health.cloned().unwrap_or((false, None));
    DeviceResponse {
        device_id: device.device_id,
        serial_number: device.serial_number,
        display_name: device.display_name,
        metadata: device.metadata,
        asset_id: device.asset_id,
        device_profile_id: device.device_profile_id,
        online,
        last_seen_at,
        effective_permission: access.map(|access| access.permission.as_str()),
        access_source: access.map(|access| access.source.as_str()),
    }
}

fn public_device_claim_error(error: DeviceClaimError) -> PublicApiError {
    match error {
        DeviceClaimError::InvalidPolicy => PublicApiError::BadRequest,
        DeviceClaimError::UserUnavailable => PublicApiError::Forbidden,
        DeviceClaimError::PolicyDisabled
        | DeviceClaimError::DeviceUnavailable
        | DeviceClaimError::RequestCoolingDown
        | DeviceClaimError::CodeUnavailable => PublicApiError::Conflict,
        DeviceClaimError::Storage { .. } => PublicApiError::Unavailable,
    }
}

async fn create_device_resource_invitation(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(request): Json<CreateResourceInvitationRequest>,
) -> Result<(StatusCode, Json<ResourceInvitationResponse>), PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let invitation = create_resource_invitation(
        store.as_ref(),
        &principal,
        OwnershipTransferTarget::Device(device_id),
        request,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(resource_invitation_response(store.as_ref(), invitation).await?),
    ))
}

async fn create_resource_invitation(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    target: OwnershipTransferTarget,
    request: CreateResourceInvitationRequest,
) -> Result<ResourceInvitation, PublicApiError> {
    let sender_user_id = principal.user_id.ok_or(PublicApiError::Forbidden)?;
    let username = request.username.trim();
    if username.is_empty() || username.len() > 64 {
        return Err(PublicApiError::BadRequest);
    }
    let permission = match request.permission.as_str() {
        "viewer" => ResourcePermission::Viewer,
        "manager" => ResourcePermission::Manager,
        _ => return Err(PublicApiError::BadRequest),
    };
    let recipient_user_id =
        ManagementUserRepository::list_management_users(store, principal.tenant_id)
            .await
            .map_err(|_| PublicApiError::Unavailable)?
            .into_iter()
            .find(|user| user.username == username)
            .map(|user| user.id)
            .ok_or(PublicApiError::Forbidden)?;
    ResourceInvitationRepository::create_owner_resource_invitation(
        store,
        principal.tenant_id,
        sender_user_id,
        recipient_user_id,
        target,
        permission,
    )
    .await
    .map_err(|_| PublicApiError::Forbidden)
}

async fn list_resource_invitations(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
) -> Result<Json<PublicPage<ResourceInvitationResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:read").await?;
    let recipient_user_id = principal.user_id.ok_or(PublicApiError::Forbidden)?;
    let invitations = ResourceInvitationRepository::list_pending_resource_invitations(
        store.as_ref(),
        principal.tenant_id,
        recipient_user_id,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    let mut items = Vec::with_capacity(invitations.len());
    for invitation in invitations {
        items.push(resource_invitation_response(store.as_ref(), invitation).await?);
    }
    Ok(Json(PublicPage {
        has_more: false,
        items,
        next_cursor: None,
    }))
}

async fn accept_resource_invitation(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(invitation_id): Path<String>,
) -> Result<StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let invitation_id = invitation_id
        .parse()
        .map_err(|_| PublicApiError::BadRequest)?;
    ResourceInvitationRepository::accept_resource_invitation(
        store.as_ref(),
        principal.tenant_id,
        principal.user_id.ok_or(PublicApiError::Forbidden)?,
        invitation_id,
    )
    .await
    .map_err(|_| PublicApiError::Forbidden)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn cancel_resource_invitation(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(invitation_id): Path<String>,
) -> Result<StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let invitation_id = invitation_id
        .parse()
        .map_err(|_| PublicApiError::BadRequest)?;
    ResourceInvitationRepository::cancel_resource_invitation(
        store.as_ref(),
        principal.tenant_id,
        principal.user_id.ok_or(PublicApiError::Forbidden)?,
        invitation_id,
    )
    .await
    .map_err(|_| PublicApiError::Forbidden)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resource_invitation_response(
    store: &PlatformStore,
    invitation: ResourceInvitation,
) -> Result<ResourceInvitationResponse, PublicApiError> {
    let users = ManagementUserRepository::list_management_users(store, invitation.tenant_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?;
    let sender_username = users
        .iter()
        .find(|user| user.id == invitation.sender_user_id)
        .map(|user| user.username.clone())
        .unwrap_or_else(|| invitation.sender_user_id.to_string());
    let (resource_kind, resource_id, resource_name) =
        match (invitation.asset_id, invitation.device_id.as_deref()) {
            (Some(asset_id), None) => {
                let asset_name =
                    ManagementAssetRepository::list_management_assets(store, invitation.tenant_id)
                        .await
                        .map_err(|_| PublicApiError::Unavailable)?
                        .into_iter()
                        .find(|asset| asset.id == asset_id)
                        .map(|asset| asset.name)
                        .unwrap_or_else(|| asset_id.to_string());
                ("asset", asset_id.to_string(), asset_name)
            }
            (None, Some(device_id)) => {
                let device_name = ManagementDeviceRepository::list_management_devices(
                    store,
                    invitation.tenant_id,
                )
                .await
                .map_err(|_| PublicApiError::Unavailable)?
                .into_iter()
                .find(|device| device.device_id == device_id)
                .and_then(|device| device.display_name)
                .unwrap_or_else(|| device_id.to_owned());
                ("device", device_id.to_owned(), device_name)
            }
            _ => return Err(PublicApiError::Unavailable),
        };
    Ok(ResourceInvitationResponse {
        id: invitation.id,
        resource_kind,
        resource_id,
        resource_name,
        sender_username,
        permission: invitation.permission.as_str(),
    })
}

fn public_device_error(error: PublicDeviceError) -> PublicApiError {
    match error {
        PublicDeviceError::Unauthorized => PublicApiError::Forbidden,
        PublicDeviceError::AssetUnavailable(_) | PublicDeviceError::DeviceProfileUnavailable(_) => {
            PublicApiError::Conflict
        }
        PublicDeviceError::Storage { .. } => PublicApiError::Unavailable,
    }
}

fn public_asset_error(error: PublicAssetError) -> PublicApiError {
    match error {
        PublicAssetError::Unauthorized => PublicApiError::Forbidden,
        PublicAssetError::ParentUnavailable(_) | PublicAssetError::AssetProfileUnavailable(_) => {
            PublicApiError::Conflict
        }
        PublicAssetError::Storage { .. } => PublicApiError::Unavailable,
    }
}

#[derive(Debug, Deserialize)]
struct TelemetryQuery {
    device_id: Option<String>,
    asset_id: Option<Uuid>,
    aggregate: Option<String>,
    from: Option<chrono::DateTime<Utc>>,
    to: Option<chrono::DateTime<Utc>>,
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct LiveViewResponse {
    profile: Option<LiveViewProfileResponse>,
    charts: Vec<LiveChartResponse>,
}

impl LiveViewResponse {
    fn empty() -> Self {
        Self {
            profile: None,
            charts: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
struct LiveViewProfileResponse {
    id: Uuid,
    name: String,
}

#[derive(Debug, Serialize)]
struct LiveChartResponse {
    metric: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    color: Option<String>,
    aggregation: String,
}

fn live_charts(settings: &Value) -> Vec<LiveChartResponse> {
    let Some(charts) = settings.get("live_charts").and_then(Value::as_array) else {
        return Vec::new();
    };
    charts
        .iter()
        .filter_map(|chart| {
            let chart = chart.as_object()?;
            let metric = chart.get("metric")?.as_str()?.trim();
            if metric.is_empty() || metric.len() > 128 {
                return None;
            }
            let aggregation = match chart
                .get("aggregation")
                .and_then(Value::as_str)
                .unwrap_or("last")
            {
                "last" | "sum" | "avg" | "min" | "max" => chart
                    .get("aggregation")
                    .and_then(Value::as_str)
                    .unwrap_or("last")
                    .to_owned(),
                _ => return None,
            };
            let label = chart
                .get("label")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|label| !label.is_empty() && label.len() <= 128)
                .unwrap_or(metric)
                .to_owned();
            let unit = chart
                .get("unit")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|unit| !unit.is_empty() && unit.len() <= 32)
                .map(ToOwned::to_owned);
            let color = chart
                .get("color")
                .and_then(Value::as_str)
                .filter(|color| is_hex_color(color))
                .map(ToOwned::to_owned);
            Some(LiveChartResponse {
                metric: metric.to_owned(),
                label,
                unit,
                color,
                aggregation,
            })
        })
        .collect()
}

fn is_hex_color(value: &str) -> bool {
    value.len() == 7
        && value.starts_with('#')
        && value.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit)
}

#[derive(Debug, Serialize)]
struct TelemetryResponse {
    event_at: chrono::DateTime<Utc>,
    received_at: chrono::DateTime<Utc>,
    device_id: String,
    boot_id: String,
    sequence: i64,
    measurements: Value,
    topic: String,
}

#[derive(Debug, Serialize)]
struct AlertResponse {
    id: Uuid,
    rule_id: Uuid,
    rule_name: String,
    severity: String,
    device_id: String,
    status: String,
    condition_started_at: chrono::DateTime<Utc>,
    opened_at: Option<chrono::DateTime<Utc>>,
    resolved_at: Option<chrono::DateTime<Utc>>,
    acknowledged_at: Option<chrono::DateTime<Utc>>,
    acknowledged_by: Option<String>,
    last_value: Option<f64>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PublicCursor {
    version: u8,
    kind: String,
    subject_user_id: Option<Uuid>,
    app_id: String,
    value: String,
}

fn encode_cursor(
    vault: &TokenVault,
    principal: &PublicPrincipal,
    kind: &str,
    value: String,
) -> Result<String, PublicApiError> {
    let plaintext = serde_json::to_string(&PublicCursor {
        version: 1,
        kind: kind.to_owned(),
        subject_user_id: principal.user_id,
        app_id: principal.app_id.clone(),
        value,
    })
    .map_err(|_| PublicApiError::Unavailable)?;
    vault
        .encrypt(&plaintext)
        .map_err(|_| PublicApiError::Unavailable)
}

fn decode_cursor(
    vault: &TokenVault,
    principal: &PublicPrincipal,
    kind: &str,
    cursor: Option<&str>,
) -> Result<Option<String>, PublicApiError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let plaintext = vault
        .decrypt(cursor)
        .map_err(|_| PublicApiError::BadRequest)?;
    let cursor: PublicCursor =
        serde_json::from_str(&plaintext).map_err(|_| PublicApiError::BadRequest)?;
    if cursor.version != 1
        || cursor.kind != kind
        || cursor.subject_user_id != principal.user_id
        || cursor.app_id != principal.app_id
    {
        return Err(PublicApiError::BadRequest);
    }
    Ok(Some(cursor.value))
}

async fn list_telemetry(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Query(query): Query<TelemetryQuery>,
) -> Result<Json<PublicPage<TelemetryResponse>>, PublicApiError> {
    list_telemetry_for_device(context, headers, query, None).await
}

async fn get_telemetry(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Query(mut query): Query<TelemetryQuery>,
) -> Result<Json<PublicPage<TelemetryResponse>>, PublicApiError> {
    query.device_id = Some(device_id.clone());
    list_telemetry_for_device(context, headers, query, Some(device_id)).await
}

async fn list_telemetry_for_device(
    context: PublicApiContext,
    headers: HeaderMap,
    query: TelemetryQuery,
    path_device_id: Option<String>,
) -> Result<Json<PublicPage<TelemetryResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "telemetry:read").await?;
    let limit = limit(query.limit)?;
    let from = query
        .from
        .unwrap_or_else(|| Utc::now() - chrono::Duration::days(30));
    let to = query.to.unwrap_or_else(Utc::now);
    if from >= to {
        return Err(PublicApiError::BadRequest);
    }
    let device_id = path_device_id.or(query.device_id);
    let asset_id = query.asset_id;
    if device_id.is_some() && asset_id.is_some() {
        return Err(PublicApiError::BadRequest);
    }
    if asset_id.is_some() && query.aggregate.as_deref() != Some("asset") {
        return Err(PublicApiError::BadRequest);
    }
    if asset_id.is_none() && query.aggregate.is_some() {
        return Err(PublicApiError::BadRequest);
    }
    if let Some(device_id) = device_id.as_deref()
        && !PublicApiRepository::public_device_permission(store.as_ref(), &principal, device_id)
            .await
            .map_err(|_| PublicApiError::Unavailable)?
            .is_some_and(|permission| permission.allows(ResourcePermission::Viewer))
    {
        return Err(PublicApiError::Forbidden);
    }
    if let Some(asset_id) = asset_id
        && !PublicApiRepository::public_asset_permission(store.as_ref(), &principal, asset_id)
            .await
            .map_err(|_| PublicApiError::Unavailable)?
            .is_some_and(|permission| permission.allows(ResourcePermission::Viewer))
    {
        return Err(PublicApiError::Forbidden);
    }
    let cursor_kind = match (device_id.as_deref(), asset_id) {
        (Some(device_id), None) => format!("telemetry:device:{device_id}"),
        (None, Some(asset_id)) => format!("telemetry:asset:{asset_id}"),
        (None, None) => "telemetry".to_owned(),
        (Some(_), Some(_)) => return Err(PublicApiError::BadRequest),
    };
    let after = decode_cursor(
        &context.token_vault,
        &principal,
        &cursor_kind,
        query.after.as_deref(),
    )?;
    let mut rows = PublicApiRepository::list_public_telemetry(
        store.as_ref(),
        &principal,
        device_id.as_deref(),
        asset_id,
        from,
        to,
        after.as_deref(),
        u32::try_from(limit.saturating_add(1)).map_err(|_| PublicApiError::BadRequest)?,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = rows
        .last()
        .map(|row| {
            encode_cursor(
                &context.token_vault,
                &principal,
                &cursor_kind,
                format!(
                    "{}|{}|{}",
                    row.event_at.to_rfc3339(),
                    row.device_id,
                    row.sequence
                ),
            )
        })
        .transpose()?;
    Ok(Json(PublicPage {
        items: rows.into_iter().map(telemetry_response).collect(),
        next_cursor: if has_more { next_cursor } else { None },
        has_more,
    }))
}

async fn list_alerts(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Query(query): Query<PublicPageQuery>,
) -> Result<Json<PublicPage<AlertResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:read").await?;
    let limit = limit(query.limit)?;
    let after = decode_cursor(
        &context.token_vault,
        &principal,
        "alerts",
        query.after.as_deref(),
    )?;
    let mut rows = PublicApiRepository::list_public_alerts(
        store.as_ref(),
        &principal,
        after.as_deref(),
        u32::try_from(limit.saturating_add(1)).map_err(|_| PublicApiError::BadRequest)?,
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = rows
        .last()
        .map(|row| {
            encode_cursor(
                &context.token_vault,
                &principal,
                "alerts",
                row.id.to_string(),
            )
        })
        .transpose()?;
    Ok(Json(PublicPage {
        items: rows.into_iter().map(alert_response).collect(),
        next_cursor: if has_more { next_cursor } else { None },
        has_more,
    }))
}

async fn get_alert(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(alert_id): Path<String>,
) -> Result<Json<AlertResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:read").await?;
    let alert_id = alert_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let alert = PublicApiRepository::get_public_alert(store.as_ref(), &principal, alert_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(alert_response(alert)))
}

async fn acknowledge_alert(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(alert_id): Path<String>,
) -> Result<Json<AlertResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "alerts:write").await?;
    let alert_id = alert_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let actor = principal
        .user_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| principal.app_id.clone());
    let alert =
        PublicApiRepository::acknowledge_public_alert(store.as_ref(), &principal, alert_id, &actor)
            .await
            .map_err(|_| PublicApiError::Unavailable)?
            .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(alert_response(alert)))
}

fn telemetry_response(row: PublicTelemetry) -> TelemetryResponse {
    TelemetryResponse {
        event_at: row.event_at,
        received_at: row.received_at,
        device_id: row.device_id,
        boot_id: row.boot_id,
        sequence: row.sequence,
        measurements: row.measurements,
        topic: row.topic,
    }
}

fn alert_response(row: PublicAlert) -> AlertResponse {
    AlertResponse {
        id: row.id,
        rule_id: row.rule_id,
        rule_name: row.rule_name,
        severity: row.severity,
        device_id: row.device_id,
        status: row.status,
        condition_started_at: row.condition_started_at,
        opened_at: row.opened_at,
        resolved_at: row.resolved_at,
        acknowledged_at: row.acknowledged_at,
        acknowledged_by: row.acknowledged_by,
        last_value: row.last_value,
        updated_at: row.updated_at,
    }
}

#[derive(Debug, Deserialize)]
struct CommandRequest {
    method: String,
    params: Value,
    mode: Option<String>,
}

#[derive(Debug, Serialize)]
struct CommandResponse {
    id: Uuid,
    state: String,
    expires_at: chrono::DateTime<Utc>,
    mode: String,
    response: Option<Value>,
    responded_at: Option<chrono::DateTime<Utc>>,
}

async fn create_command(
    Extension(context): Extension<PublicApiContext>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<(axum::http::StatusCode, Json<CommandResponse>), PublicApiError> {
    let headers = request.headers().clone();
    let (store, principal) = authenticate(&context, &headers, "commands:write").await?;
    require_device_permission(
        store.as_ref(),
        &principal,
        &device_id,
        ResourcePermission::Manager,
    )
    .await?;
    let request: CommandRequest = serde_json::from_slice(
        &to_bytes(request.into_body(), 1024 * 1024)
            .await
            .map_err(|_| PublicApiError::BadRequest)?,
    )
    .map_err(|_| PublicApiError::BadRequest)?;
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(PublicApiError::BadRequest)?;
    let mode = match request.mode.as_deref().unwrap_or("one_way") {
        "one_way" => RpcMode::OneWay,
        "two_way" => RpcMode::TwoWay,
        _ => return Err(PublicApiError::BadRequest),
    };
    if request.method.trim().is_empty() {
        return Err(PublicApiError::BadRequest);
    }
    let namespace = format!(
        "iot-nano/public-v1/command/{}/{}/{}",
        principal.app_id,
        principal
            .user_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "application".to_owned()),
        idempotency_key,
    );
    let command_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, namespace.as_bytes());
    let issued_at = Utc::now();
    let facade = context
        .core_facade
        .as_ref()
        .ok_or(PublicApiError::Unavailable)?;
    let record = facade
        .create_authorized_command(CoreAuthorizedCommandCreateRequest {
            user_id: principal.user_id.ok_or(PublicApiError::Forbidden)?,
            command: CoreCommandCreateRequest {
                id: command_id,
                tenant_id: principal.tenant_id,
                device_id,
                method: request.method,
                params: request.params,
                mode,
                issued_at,
                expires_at: issued_at + chrono::Duration::seconds(30),
            },
        })
        .await
        .map_err(public_command_error)?;
    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(command_response(record)),
    ))
}

async fn get_command(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(command_id): Path<String>,
) -> Result<Json<CommandResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "commands:read").await?;
    let command_id = command_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let facade = context
        .core_facade
        .as_ref()
        .ok_or(PublicApiError::Unavailable)?;
    let record = facade
        .get_command(principal.tenant_id, command_id)
        .await
        .map_err(|error| match error {
            CoreFacadeError::NotFound => PublicApiError::Forbidden,
            other => public_command_error(other),
        })?;
    if record.tenant_id != principal.tenant_id {
        return Err(PublicApiError::Forbidden);
    }
    if !PublicApiRepository::public_device_permission(store.as_ref(), &principal, &record.device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(ResourcePermission::Viewer))
    {
        return Err(PublicApiError::Forbidden);
    }
    Ok(Json(command_response(record)))
}

fn public_command_error(error: CoreFacadeError) -> PublicApiError {
    match error {
        CoreFacadeError::Rejected(409) => PublicApiError::Conflict,
        CoreFacadeError::NotFound => PublicApiError::Forbidden,
        CoreFacadeError::Rejected(_) | CoreFacadeError::Unavailable => PublicApiError::Unavailable,
    }
}

fn command_response(record: crate::CoreCommandRecord) -> CommandResponse {
    CommandResponse {
        id: record.id,
        state: record.state,
        expires_at: record.expires_at,
        mode: match record.mode {
            RpcMode::OneWay => "one_way".to_owned(),
            RpcMode::TwoWay => "two_way".to_owned(),
        },
        response: record.response,
        responded_at: record.responded_at,
    }
}
