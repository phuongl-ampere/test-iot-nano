use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::Utc;
use iot_core::RpcMode;
use iot_storage::{
    AuthorizationRepository, NewPublicAsset, NewPublicDevice, PlatformStore, PublicAlert,
    PublicApiRepository, PublicAsset, PublicAssetError, PublicDevice, PublicDeviceError,
    PublicPrincipal, PublicTelemetry, ResourcePermission,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    CoreAuthorizedCommandCreateRequest, CoreCommandCreateRequest, CoreFacade, CoreFacadeError,
    TokenVault,
    auth::{BearerAccessTokenError, extract_bearer_access_token, validate_bearer_access_token},
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
        .route("/api/v1/assets", get(list_assets).post(create_asset))
        .route(
            "/api/v1/assets/{asset_id}",
            get(get_asset).patch(update_asset).delete(delete_asset),
        )
        .route("/api/v1/telemetry", get(list_telemetry))
        .route("/api/v1/telemetry/{device_id}", get(get_telemetry))
        .route("/api/v1/alerts", get(list_alerts))
        .route("/api/v1/alerts/{alert_id}", get(get_alert))
        .route(
            "/api/v1/alerts/{alert_id}/acknowledge",
            axum::routing::post(acknowledge_alert),
        )
        .route("/api/v1/devices", get(list_devices).post(create_device))
        .route(
            "/api/v1/devices/{device_id}",
            get(get_device).patch(update_device).delete(delete_device),
        )
        .route(
            "/api/v1/devices/{device_id}/commands",
            axum::routing::post(create_command),
        )
        .route("/api/v1/commands/{command_id}", get(get_command))
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
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
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
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
struct DeviceResponse {
    device_id: String,
    display_name: Option<String>,
    metadata: Value,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_permission: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_source: Option<&'static str>,
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

async fn create_asset(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Json(request): Json<CreateAssetRequest>,
) -> Result<(axum::http::StatusCode, Json<AssetResponse>), PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "assets:write").await?;
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
            asset_profile_id: request.asset_profile_id.or(current.asset_profile_id),
            parent_asset_id: request.parent_asset_id.or(current.parent_asset_id),
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

async fn create_device(
    Extension(context): Extension<PublicApiContext>,
    request: Request,
) -> Result<(axum::http::StatusCode, Json<DeviceResponse>), PublicApiError> {
    let headers = request.headers().clone();
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    let request: CreateDeviceRequest = serde_json::from_slice(
        &to_bytes(request.into_body(), 1024 * 1024)
            .await
            .map_err(|_| PublicApiError::BadRequest)?,
    )
    .map_err(|_| PublicApiError::BadRequest)?;
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
        Json(device_response(device)),
    ))
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
        items: devices.into_iter().map(device_response).collect(),
        next_cursor,
        has_more,
    }))
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
    Ok(Json(device_response(device)))
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
    let device = PublicApiRepository::update_public_device(
        store.as_ref(),
        &principal,
        &device_id,
        NewPublicDevice {
            device_id: device_id.clone(),
            display_name: validate_display_name(request.display_name)?.or(current.display_name),
            metadata: validate_metadata(request.metadata.unwrap_or(current.metadata))?,
            asset_id: request.asset_id.or(current.asset_id),
            device_profile_id: request.device_profile_id.or(current.device_profile_id),
        },
    )
    .await
    .map_err(public_device_error)?
    .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(device_response(device)))
}

async fn delete_device(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<axum::http::StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "devices:write").await?;
    if PublicApiRepository::delete_public_device(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
    {
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(PublicApiError::Forbidden)
    }
}

fn device_response(device: PublicDevice) -> DeviceResponse {
    let access = device.access;
    DeviceResponse {
        device_id: device.device_id,
        display_name: device.display_name,
        metadata: device.metadata,
        asset_id: device.asset_id,
        device_profile_id: device.device_profile_id,
        effective_permission: access.map(|access| access.permission.as_str()),
        access_source: access.map(|access| access.source.as_str()),
    }
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
    from: Option<chrono::DateTime<Utc>>,
    to: Option<chrono::DateTime<Utc>>,
    after: Option<String>,
    limit: Option<usize>,
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
    if let Some(device_id) = device_id.as_deref()
        && !PublicApiRepository::public_device_permission(store.as_ref(), &principal, device_id)
            .await
            .map_err(|_| PublicApiError::Unavailable)?
            .is_some_and(|permission| permission.allows(ResourcePermission::Viewer))
    {
        return Err(PublicApiError::Forbidden);
    }
    let after = decode_cursor(
        &context.token_vault,
        &principal,
        "telemetry",
        query.after.as_deref(),
    )?;
    let mut rows = PublicApiRepository::list_public_telemetry(
        store.as_ref(),
        &principal,
        device_id.as_deref(),
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
                "telemetry",
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
    let (_store, principal) = authenticate(&context, &headers, "commands:write").await?;
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
