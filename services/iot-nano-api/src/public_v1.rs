use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request},
    http::HeaderMap,
    routing::get,
};
use chrono::Utc;
use iot_core::RpcMode;
use iot_storage::{
    AccountClass, AuthorizationRepository, NewPublicAsset, NewPublicResourceGrant, PlatformStore,
    PublicAlert, PublicApiRepository, PublicAsset, PublicPrincipal, PublicResourceGrant,
    PublicTelemetry, ResourcePermission,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    CoreCommandCreateRequest, CoreFacade, CoreFacadeError, TokenVault,
    auth::{extract_bearer_access_token, validate_bearer_access_token},
    routes::PublicApiError,
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
        .route(
            "/api/v1/devices/{device_id}/commands",
            axum::routing::post(create_command),
        )
        .route("/api/v1/commands/{command_id}", get(get_command))
        .route(
            "/api/v1/resource-grants",
            get(list_grants).post(create_grant),
        )
        .route(
            "/api/v1/resource-grants/{grant_id}",
            get(get_grant).patch(update_grant).delete(delete_grant),
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
    let account_class = match token.user_id {
        Some(user_id) => {
            AuthorizationRepository::authorization_subject(store.as_ref(), user_id)
                .await
                .map_err(|_| PublicApiError::Unavailable)?
                .ok_or(PublicApiError::Forbidden)?
                .account_class
        }
        None => AccountClass::User,
    };
    Ok((
        store,
        PublicPrincipal {
            user_id: token.user_id,
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
    .map_err(|_| PublicApiError::Unavailable)?;
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
    .map_err(|_| PublicApiError::Unavailable)?
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

fn asset_response(asset: PublicAsset) -> AssetResponse {
    AssetResponse {
        id: asset.id,
        name: asset.name,
        asset_profile_id: asset.asset_profile_id,
        parent_asset_id: asset.parent_asset_id,
        attributes: asset.metadata.clone(),
        metadata: asset.metadata,
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

#[derive(Debug, Serialize)]
struct GrantResponse {
    id: Uuid,
    resource_type: String,
    resource_id: String,
    grantee_type: String,
    grantee_id: String,
    permission: String,
    created_by_user_id: Option<Uuid>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct GrantRequest {
    resource_type: String,
    resource_id: String,
    grantee_type: String,
    grantee_id: String,
    permission: String,
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

async fn list_grants(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Query(query): Query<PublicPageQuery>,
) -> Result<Json<PublicPage<GrantResponse>>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:read").await?;
    let limit = limit(query.limit)?;
    let after = decode_cursor(
        &context.token_vault,
        &principal,
        "grants",
        query.after.as_deref(),
    )?;
    let mut rows = PublicApiRepository::list_public_grants(
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
                "grants",
                row.id.to_string(),
            )
        })
        .transpose()?;
    Ok(Json(PublicPage {
        items: rows.into_iter().map(grant_response).collect(),
        next_cursor: if has_more { next_cursor } else { None },
        has_more,
    }))
}

async fn get_grant(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(grant_id): Path<String>,
) -> Result<Json<GrantResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:read").await?;
    let grant_id = grant_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let grant = PublicApiRepository::get_public_grant(store.as_ref(), &principal, grant_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(grant_response(grant)))
}

async fn create_grant(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Json(request): Json<GrantRequest>,
) -> Result<(axum::http::StatusCode, Json<GrantResponse>), PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let grant = PublicApiRepository::create_public_grant(
        store.as_ref(),
        &principal,
        NewPublicResourceGrant {
            resource_type: request.resource_type,
            resource_id: request.resource_id,
            grantee_type: request.grantee_type,
            grantee_id: request.grantee_id,
            permission: request.permission,
        },
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?
    .ok_or(PublicApiError::Forbidden)?;
    Ok((axum::http::StatusCode::CREATED, Json(grant_response(grant))))
}

async fn update_grant(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(grant_id): Path<String>,
    Json(request): Json<GrantRequest>,
) -> Result<Json<GrantResponse>, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let grant_id = grant_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    let grant = PublicApiRepository::update_public_grant(
        store.as_ref(),
        &principal,
        grant_id,
        NewPublicResourceGrant {
            resource_type: request.resource_type,
            resource_id: request.resource_id,
            grantee_type: request.grantee_type,
            grantee_id: request.grantee_id,
            permission: request.permission,
        },
    )
    .await
    .map_err(|_| PublicApiError::Unavailable)?
    .ok_or(PublicApiError::Forbidden)?;
    Ok(Json(grant_response(grant)))
}

async fn delete_grant(
    Extension(context): Extension<PublicApiContext>,
    headers: HeaderMap,
    Path(grant_id): Path<String>,
) -> Result<axum::http::StatusCode, PublicApiError> {
    let (store, principal) = authenticate(&context, &headers, "authorization:write").await?;
    let grant_id = grant_id.parse().map_err(|_| PublicApiError::BadRequest)?;
    if PublicApiRepository::delete_public_grant(store.as_ref(), &principal, grant_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
    {
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(PublicApiError::Forbidden)
    }
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

fn grant_response(row: PublicResourceGrant) -> GrantResponse {
    GrantResponse {
        id: row.id,
        resource_type: row.resource_type,
        resource_id: row.resource_id,
        grantee_type: row.grantee_type,
        grantee_id: row.grantee_id,
        permission: row.permission,
        created_by_user_id: row.created_by_user_id,
        created_at: row.created_at,
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
    if !PublicApiRepository::public_device_permission(store.as_ref(), &principal, &device_id)
        .await
        .map_err(|_| PublicApiError::Unavailable)?
        .is_some_and(|permission| permission.allows(ResourcePermission::Controller))
    {
        return Err(PublicApiError::Forbidden);
    }
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
        .create_command(CoreCommandCreateRequest {
            id: command_id,
            device_id,
            method: request.method,
            params: request.params,
            mode,
            issued_at,
            expires_at: issued_at + chrono::Duration::seconds(30),
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
        .get_command(command_id)
        .await
        .map_err(|error| match error {
            CoreFacadeError::NotFound => PublicApiError::Forbidden,
            other => public_command_error(other),
        })?;
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
