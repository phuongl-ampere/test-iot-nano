use super::*;
use qrcodegen::{QrCode, QrCodeEcc};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

#[derive(Deserialize)]
struct OtaPolicyRequest {
    require_matching_device_profile: bool,
    require_newer_version: bool,
}

#[derive(Serialize)]
pub(super) struct OtaArtifactResponse {
    id: Uuid,
    device_profile_id: Uuid,
    version: String,
    filename: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Deserialize)]
struct PersonalAccessTokenRequest {
    name: String,
}

#[derive(Serialize)]
pub(super) struct PersonalAccessTokenMetadataResponse {
    name: String,
    prefix: String,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize)]
pub(super) struct PersonalAccessTokenGetResponse {
    token: Option<PersonalAccessTokenMetadataResponse>,
}

#[derive(Serialize)]
pub(super) struct PersonalAccessTokenCreateResponse {
    token: PersonalAccessTokenMetadataResponse,
    secret: String,
}

pub(super) fn personal_access_token_metadata_response(
    token: iot_storage::TenantPersonalAccessTokenRecord,
) -> PersonalAccessTokenMetadataResponse {
    PersonalAccessTokenMetadataResponse {
        name: token.name,
        prefix: token.token_prefix,
        created_at: token.created_at,
        last_used_at: token.last_used_at,
    }
}

pub(super) async fn get_personal_access_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<PersonalAccessTokenGetResponse>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let token =
        iot_storage::TenantPersonalAccessTokenRepository::active_tenant_personal_access_token(
            state.store.as_ref(),
            tenant.tenant_id,
            tenant.tenant_account_id,
        )
        .await
        .map_err(personal_access_token_error)?
        .map(personal_access_token_metadata_response);
    Ok(Json(PersonalAccessTokenGetResponse { token }))
}

pub(super) async fn create_personal_access_token(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, HeaderValue); 1],
        Json<PersonalAccessTokenCreateResponse>,
    ),
    ManagementSessionError,
> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: PersonalAccessTokenRequest = management_request_json(&state, request).await?;
    let name = request.name.trim();
    if name.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    let secret = format!(
        "iotpat_{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    );
    let token_hash = format!("{:x}", Sha256::digest(secret.as_bytes()));
    let token = iot_storage::NewTenantPersonalAccessToken {
        id: Uuid::now_v7(),
        name: name.to_owned(),
        token_prefix: secret.chars().take(15).collect(),
        token_hash,
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let token =
        iot_storage::TenantPersonalAccessTokenRepository::rotate_tenant_personal_access_token(
            state.store.as_ref(),
            tenant.tenant_id,
            tenant.tenant_account_id,
            token,
            chrono::Utc::now(),
        )
        .await
        .map_err(personal_access_token_error)?;
    Ok((
        StatusCode::CREATED,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(PersonalAccessTokenCreateResponse {
            token: personal_access_token_metadata_response(token),
            secret,
        }),
    ))
}

pub(super) async fn revoke_personal_access_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    iot_storage::TenantPersonalAccessTokenRepository::revoke_tenant_personal_access_token(
        state.store.as_ref(),
        tenant.tenant_id,
        tenant.tenant_account_id,
        chrono::Utc::now(),
    )
    .await
    .map_err(personal_access_token_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn personal_access_token_error(
    error: iot_storage::TenantPersonalAccessTokenRepositoryError,
) -> ManagementSessionError {
    match error {
        iot_storage::TenantPersonalAccessTokenRepositoryError::EmptyName
        | iot_storage::TenantPersonalAccessTokenRepositoryError::EmptyTokenPrefix
        | iot_storage::TenantPersonalAccessTokenRepositoryError::InvalidTokenHash => {
            ManagementSessionError::BadRequest
        }
        iot_storage::TenantPersonalAccessTokenRepositoryError::TenantAccountNotFound
        | iot_storage::TenantPersonalAccessTokenRepositoryError::TokenNotFound => {
            ManagementSessionError::NotFound
        }
        iot_storage::TenantPersonalAccessTokenRepositoryError::TokenPrefixConflict => {
            ManagementSessionError::Conflict
        }
        iot_storage::TenantPersonalAccessTokenRepositoryError::InvalidStoredTimestamp
        | iot_storage::TenantPersonalAccessTokenRepositoryError::Storage { .. } => {
            ManagementSessionError::Unavailable
        }
    }
}

pub(super) async fn list_ota_artifacts(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<OtaArtifactResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    state
        .store
        .list_ota_artifacts(tenant.tenant_id)
        .await
        .map(|artifacts| {
            Json(
                artifacts
                    .into_iter()
                    .map(|artifact| OtaArtifactResponse {
                        id: artifact.id,
                        device_profile_id: artifact.device_profile_id,
                        version: artifact.version,
                        filename: artifact.filename,
                        sha256: artifact.sha256,
                        size_bytes: artifact.size_bytes,
                    })
                    .collect(),
            )
        })
        .map_err(|_| ManagementSessionError::Unavailable)
}

pub(super) async fn get_ota_policy(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<iot_storage::OtaPolicy>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    state
        .store
        .ota_policy(tenant.tenant_id)
        .await
        .map(Json)
        .map_err(|_| ManagementSessionError::Unavailable)
}

pub(super) async fn update_ota_policy(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<StatusCode, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let policy: OtaPolicyRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    state
        .store
        .set_ota_policy(
            tenant.tenant_id,
            iot_storage::OtaPolicy {
                require_matching_device_profile: policy.require_matching_device_profile,
                require_newer_version: policy.require_newer_version,
            },
        )
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn create_application(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ApplicationResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateApplicationRequest = management_request_json(&state, request).await?;
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
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let application = ApplicationRepository::upsert_application(
        state.store.as_ref(),
        NewApplication {
            app_id,
            tenant_id: tenant.tenant_id,
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
                tenant_id: application.tenant_id,
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

pub(super) async fn upload_ota_artifact(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<serde_json::Value>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let profile_id = headers
        .get("x-ota-device-profile-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ManagementSessionError::BadRequest)?;
    let version = headers
        .get("x-ota-version")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 64)
        .ok_or(ManagementSessionError::BadRequest)?;
    let filename = headers
        .get("x-ota-filename")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128 && !value.contains('/'))
        .ok_or(ManagementSessionError::BadRequest)?;
    let profile_exists = ManagementDeviceProfileRepository::list_management_device_profiles(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(|_| ManagementSessionError::Unavailable)?
    .into_iter()
    .any(|profile| profile.id == profile_id);
    if !profile_exists {
        return Err(ManagementSessionError::BadRequest);
    }
    let body = axum::body::to_bytes(request.into_body(), 64 * 1024 * 1024)
        .await
        .map_err(|_| ManagementSessionError::PayloadTooLarge)?;
    if body.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let artifact_id = Uuid::now_v7();
    let ota_root =
        std::env::var("IOT_NANO_INTERNAL_DIR").map_err(|_| ManagementSessionError::Unavailable)?;
    let ota_dir = std::path::Path::new(&ota_root)
        .parent()
        .ok_or(ManagementSessionError::Unavailable)?
        .join("ota")
        .join(tenant.tenant_id.to_string());
    tokio::fs::create_dir_all(&ota_dir)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let storage_path = ota_dir.join(artifact_id.to_string());
    tokio::fs::write(&storage_path, &body)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let sha256 = format!("{:x}", Sha256::digest(&body));
    let artifact = iot_storage::OtaArtifact {
        id: artifact_id,
        tenant_id: tenant.tenant_id,
        device_profile_id: profile_id,
        version: version.to_owned(),
        filename: filename.to_owned(),
        storage_path: storage_path.to_string_lossy().into_owned(),
        sha256: sha256.clone(),
        size_bytes: body.len() as u64,
    };
    if state.store.create_ota_artifact(&artifact).await.is_err() {
        let _ = tokio::fs::remove_file(&storage_path).await;
        return Err(ManagementSessionError::Conflict);
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": artifact_id,
            "device_profile_id": profile_id,
            "version": version,
            "filename": filename,
            "sha256": sha256,
            "size_bytes": body.len(),
        })),
    ))
}

pub(super) async fn provision_device(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, HeaderValue); 1],
        Json<DeviceTokenResponse>,
    ),
    ManagementSessionError,
> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ProvisionDeviceRequest = management_request_json(&state, request).await?;
    let serial_number = request.serial_number.trim();
    let display_name = request.display_name.trim();
    if serial_number.len() > 128 || display_name.is_empty() || display_name.len() > 128 {
        return Err(ManagementSessionError::BadRequest);
    }
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let token = provision_management_device_token(
        &state.store,
        &state.token_vault,
        tenant.tenant_id,
        serial_number,
        display_name,
        request.asset_id,
        request.device_profile_id,
        request.attributes,
    )
    .await
    .map_err(management_device_token_error)?;
    Ok((
        StatusCode::CREATED,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(token),
    ))
}

pub(super) async fn list_management_users(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementUserResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementUserRepository::list_management_users(state.store.as_ref(), tenant.tenant_id)
        .await
        .map(|users| Json(users.into_iter().map(management_user_response).collect()))
        .map_err(management_user_error)
}

pub(super) async fn list_management_resource_access(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Query(query): Query<ManagementResourceAccessQuery>,
) -> Result<Json<ManagementResourceAccessResponse>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let scope = management_resource_access_scope(&query)?;
    let users =
        ManagementUserRepository::list_management_users(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_user_error)?;
    let usernames: HashMap<Uuid, String> = users
        .into_iter()
        .filter(|user| user.account_class.as_str() == "user")
        .map(|user| (user.id, user.username))
        .collect();
    let items = TenantAuthorizationRepository::list_active_resource_permissions(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(tenant_authorization_error)?
    .into_iter()
    .filter(|permission| scope.matches_permission(permission))
    .filter_map(|permission| {
        let user_id = permission.subject_user_id?;
        let username = usernames.get(&user_id)?.clone();
        Some(ManagementResourceAccessItem {
            id: permission.id,
            user_id,
            username,
            permission: permission.permission.as_str().to_owned(),
            inherit_children: permission.inherit_children,
            scope: scope.name(),
            resource_id: scope.resource_id(),
        })
    })
    .collect();
    Ok(Json(ManagementResourceAccessResponse { items }))
}

#[derive(Clone)]
enum ManagementResourceAccessScope {
    Asset(Uuid),
    Device(String),
}

impl ManagementResourceAccessScope {
    fn matches_permission(&self, permission: &iot_storage::ResourcePermissionRecord) -> bool {
        match self {
            Self::Asset(asset_id) => permission.asset_id == Some(*asset_id),
            Self::Device(device_id) => permission.device_id.as_deref() == Some(device_id),
        }
    }

    const fn name(&self) -> &'static str {
        match self {
            Self::Asset(_) => "asset",
            Self::Device(_) => "device",
        }
    }

    fn resource_id(&self) -> String {
        match self {
            Self::Asset(asset_id) => asset_id.to_string(),
            Self::Device(device_id) => device_id.clone(),
        }
    }
}

fn management_resource_access_scope(
    query: &ManagementResourceAccessQuery,
) -> Result<ManagementResourceAccessScope, ManagementSessionError> {
    match query.scope.as_str() {
        "asset" => Uuid::parse_str(&query.resource_id)
            .map(ManagementResourceAccessScope::Asset)
            .map_err(|_| ManagementSessionError::BadRequest),
        "device" if !query.resource_id.trim().is_empty() => Ok(
            ManagementResourceAccessScope::Device(query.resource_id.clone()),
        ),
        _ => Err(ManagementSessionError::BadRequest),
    }
}

pub(super) async fn list_management_alerts(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAlertResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementAlertRepository::list_management_alerts(state.store.as_ref(), tenant.tenant_id)
        .await
        .map(|alerts| Json(alerts.into_iter().map(management_alert_response).collect()))
        .map_err(management_alert_error)
}

pub(super) async fn management_alert_summary(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<ManagementAlertSummaryResponse>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let open_incident_count =
        ManagementAlertIncidentRepository::open_management_alert_incident_count(
            state.store.as_ref(),
            tenant.tenant_id,
        )
        .await
        .map_err(management_alert_incident_error)?;
    Ok(Json(ManagementAlertSummaryResponse {
        open_incident_count,
    }))
}

pub(super) async fn list_management_alert_rules(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAlertRuleResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementAlertRuleRepository::list_management_alert_rules(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map(|rules| {
        Json(
            rules
                .into_iter()
                .map(management_alert_rule_response)
                .collect(),
        )
    })
    .map_err(management_alert_rule_error)
}

pub(super) async fn create_management_alert_rule(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementAlertRuleResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementAlertRuleRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let rule = ManagementAlertRuleRepository::create_management_alert_rule(
        state.store.as_ref(),
        tenant.tenant_id,
        CreateManagementAlertRule {
            name: request.name,
            enabled: request.enabled,
            device_id: request.device_id,
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
        },
    )
    .await
    .map_err(management_alert_rule_error)?;
    Ok((
        StatusCode::CREATED,
        Json(management_alert_rule_response(rule)),
    ))
}

pub(super) async fn update_management_alert_rule(
    State(state): State<ManagementState>,
    Path(rule_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementAlertRuleResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let rule_id = Uuid::parse_str(&rule_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let request: ManagementAlertRuleRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let rule = ManagementAlertRuleRepository::update_management_alert_rule(
        state.store.as_ref(),
        tenant.tenant_id,
        rule_id,
        UpdateManagementAlertRule {
            name: request.name,
            enabled: request.enabled,
            device_id: request.device_id,
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
        },
    )
    .await
    .map_err(management_alert_rule_error)?;
    Ok(Json(management_alert_rule_response(rule)))
}

pub(super) async fn archive_management_alert_rule(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(rule_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let rule_id = Uuid::parse_str(&rule_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    ManagementAlertRuleRepository::archive_management_alert_rule(
        state.store.as_ref(),
        tenant.tenant_id,
        rule_id,
    )
    .await
    .map_err(management_alert_rule_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_management_alert_incidents(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAlertIncidentResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementAlertIncidentRepository::list_management_alert_incidents(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map(|incidents| {
        Json(
            incidents
                .into_iter()
                .map(management_alert_incident_response)
                .collect(),
        )
    })
    .map_err(management_alert_incident_error)
}

pub(super) async fn acknowledge_management_alert_incident(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(incident_id): Path<String>,
) -> Result<Json<ManagementAlertIncidentResponse>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let incident_id =
        Uuid::parse_str(&incident_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let incident = ManagementAlertIncidentRepository::acknowledge_management_alert_incident(
        state.store.as_ref(),
        tenant.tenant_id,
        incident_id,
        tenant.tenant_account_id.to_string(),
    )
    .await
    .map_err(management_alert_incident_error)?;
    Ok(Json(management_alert_incident_response(incident)))
}

pub(super) async fn list_management_audit_events(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Query(query): Query<TenantAuditQuery>,
) -> Result<Json<ManagementAuditEventPage>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let page = tenant_audit_event_page(&state, tenant.tenant_id, &query).await?;
    Ok(Json(ManagementAuditEventPage {
        items: page
            .events
            .into_iter()
            .map(management_audit_event_response)
            .collect(),
        next_cursor: page.next_cursor,
        has_more: page.has_more,
    }))
}

pub(super) async fn create_management_user(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementUserResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    #[cfg(test)]
    pause_after_mutation_authorization(&state).await;
    let request: CreateManagementUserRequest = management_request_json(&state, request).await?;
    validate_password(&request.password).map_err(|_| ManagementSessionError::BadRequest)?;
    let password_hash =
        hash_password(&request.password).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let user = ManagementUserRepository::create_management_user(
        state.store.as_ref(),
        CreateManagementUser {
            tenant_id: tenant.tenant_id,
            username: request.username,
            password_hash,
        },
    )
    .await
    .map_err(management_user_error)?;
    Ok((StatusCode::CREATED, Json(management_user_response(user))))
}

pub(super) async fn update_management_user(
    State(state): State<ManagementState>,
    Path(username): Path<String>,
    request: Request,
) -> Result<Json<ManagementUserResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: UpdateManagementUserRequest = management_request_json(&state, request).await?;
    let role = request
        .role
        .as_deref()
        .map(management_user_role)
        .transpose()?;
    let update = UpdateManagementUser { role };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let user = ManagementUserRepository::update_management_user(
        state.store.as_ref(),
        tenant.tenant_id,
        &username,
        update,
    )
    .await
    .map_err(management_user_error)?;
    Ok(Json(management_user_response(user)))
}

pub(super) async fn update_management_user_capabilities(
    State(state): State<ManagementState>,
    Path(username): Path<String>,
    request: Request,
) -> Result<Json<ManagementUserResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: UpdateManagementUserCapabilitiesRequest =
        management_request_json(&state, request).await?;
    let capabilities = request
        .capabilities
        .iter()
        .map(|value| management_user_capability(value))
        .collect::<Result<Vec<_>, _>>()?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let user = ManagementUserRepository::replace_management_user_capabilities(
        state.store.as_ref(),
        tenant.tenant_id,
        &username,
        capabilities,
    )
    .await
    .map_err(management_user_error)?;
    Ok(Json(management_user_response(user)))
}

pub(super) async fn list_management_device_profiles(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementDeviceProfileResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementDeviceProfileRepository::list_management_device_profiles(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map(|profiles| {
        Json(
            profiles
                .into_iter()
                .map(management_device_profile_response)
                .collect(),
        )
    })
    .map_err(management_device_profile_error)
}

pub(super) async fn create_management_device_profile(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementDeviceProfileResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementDeviceProfileRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let profile = ManagementDeviceProfileRepository::create_management_device_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        CreateManagementDeviceProfile {
            name: request.name,
            telemetry_schema: request.telemetry_schema,
            metric_mapping: request.metric_mapping,
            reporting_settings: request.reporting_settings,
        },
    )
    .await
    .map_err(management_device_profile_error)?;
    Ok((
        StatusCode::CREATED,
        Json(management_device_profile_response(profile)),
    ))
}

pub(super) async fn update_management_device_profile(
    State(state): State<ManagementState>,
    Path(profile_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementDeviceProfileResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementDeviceProfileRequest = management_request_json(&state, request).await?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let profile = ManagementDeviceProfileRepository::update_management_device_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        profile_id,
        UpdateManagementDeviceProfile {
            name: request.name,
            telemetry_schema: request.telemetry_schema,
            metric_mapping: request.metric_mapping,
            reporting_settings: request.reporting_settings,
        },
    )
    .await
    .map_err(management_device_profile_error)?;
    Ok(Json(management_device_profile_response(profile)))
}

pub(super) async fn delete_management_device_profile(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    ManagementDeviceProfileRepository::delete_management_device_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        profile_id,
    )
    .await
    .map_err(management_device_profile_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_management_asset_profiles(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAssetProfileResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementAssetProfileRepository::list_management_asset_profiles(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map(|profiles| {
        Json(
            profiles
                .into_iter()
                .map(management_asset_profile_response)
                .collect(),
        )
    })
    .map_err(management_asset_profile_error)
}

pub(super) async fn create_management_asset_profile(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementAssetProfileResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementAssetProfileRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let profile = ManagementAssetProfileRepository::create_management_asset_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        CreateManagementAssetProfile {
            name: request.name,
            fields: request.fields,
            dashboard_defaults: request.dashboard_defaults,
        },
    )
    .await
    .map_err(management_asset_profile_error)?;
    Ok((
        StatusCode::CREATED,
        Json(management_asset_profile_response(profile)),
    ))
}

pub(super) async fn update_management_asset_profile(
    State(state): State<ManagementState>,
    Path(profile_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementAssetProfileResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementAssetProfileRequest = management_request_json(&state, request).await?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let profile = ManagementAssetProfileRepository::update_management_asset_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        profile_id,
        UpdateManagementAssetProfile {
            name: request.name,
            fields: request.fields,
            dashboard_defaults: request.dashboard_defaults,
        },
    )
    .await
    .map_err(management_asset_profile_error)?;
    Ok(Json(management_asset_profile_response(profile)))
}

pub(super) async fn delete_management_asset_profile(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    ManagementAssetProfileRepository::delete_management_asset_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        profile_id,
    )
    .await
    .map_err(management_asset_profile_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn get_management_tenant_profile_configuration(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<TenantProfileConfiguration>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    TenantProfileRepository::export_tenant_profile_configuration(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map(Json)
    .map_err(application_domain_profile_error)
}

pub(super) async fn replace_management_tenant_profile_configuration(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<StatusCode, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let configuration: TenantProfileConfiguration =
        management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    TenantProfileRepository::replace_tenant_profile_configuration(
        state.store.as_ref(),
        tenant.tenant_id,
        configuration,
    )
    .await
    .map_err(tenant_profile_import_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_management_devices(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementDeviceResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
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

pub(super) async fn list_management_device_telemetry(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Query(query): Query<ManagementDeviceTelemetryQuery>,
) -> Result<Json<ManagementDeviceTelemetryPage>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let duration = match query.range.as_deref() {
        None | Some("1h") => chrono::Duration::hours(1),
        Some("1d") => chrono::Duration::days(1),
        Some("7d") => chrono::Duration::days(7),
        Some(_) => return Err(ManagementSessionError::BadRequest),
    };
    let to = chrono::Utc::now();
    let from = to - duration;
    let items = ManagementDeviceTelemetryRepository::list_management_device_telemetry(
        state.store.as_ref(),
        tenant.tenant_id,
        &device_id,
        from,
        to,
        MANAGEMENT_DEVICE_TELEMETRY_LIMIT as u32,
    )
    .await
    .map_err(management_device_error)?
    .into_iter()
    .map(management_device_telemetry_response)
    .collect();

    Ok(Json(ManagementDeviceTelemetryPage { items }))
}

pub(super) async fn update_management_device(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementDeviceResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: UpdateManagementDeviceRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let device = ManagementDeviceRepository::update_management_device(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
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

pub(super) async fn delete_management_device(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    ManagementDeviceRepository::delete_management_device(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        &device_id,
    )
    .await
    .map_err(management_device_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn assign_management_device_owner(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementResourceOwnerRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    TenantAuthorizationRepository::transfer_resource_ownership(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        OwnershipTransferTarget::Device(device_id),
        request.user_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_management_assets(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAssetResponse>>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    ManagementAssetRepository::list_management_assets(state.store.as_ref(), tenant.tenant_id)
        .await
        .map(|assets| Json(assets.into_iter().map(management_asset_response).collect()))
        .map_err(management_asset_error)
}

pub(super) async fn create_management_asset(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementAssetResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ManagementAssetRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let asset = ManagementAssetRepository::create_management_asset(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        CreateManagementAsset {
            name: request.name,
            asset_profile_id: request.asset_profile_id,
            parent_asset_id: request.parent_asset_id,
            metadata: request.metadata,
            attributes: request.attributes,
        },
    )
    .await
    .map_err(management_asset_error)?;
    Ok((StatusCode::CREATED, Json(management_asset_response(asset))))
}

pub(super) async fn update_management_asset(
    State(state): State<ManagementState>,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementAssetResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let request: ManagementAssetRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let asset = ManagementAssetRepository::update_management_asset(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        asset_id,
        UpdateManagementAsset {
            name: request.name,
            asset_profile_id: request.asset_profile_id,
            parent_asset_id: request.parent_asset_id,
            metadata: request.metadata,
            attributes: request.attributes,
        },
    )
    .await
    .map_err(management_asset_error)?;
    Ok(Json(management_asset_response(asset)))
}

pub(super) async fn delete_management_asset(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    ManagementAssetRepository::delete_management_asset(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        asset_id,
    )
    .await
    .map_err(management_asset_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn assign_management_asset_owner(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<StatusCode, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let request: ManagementResourceOwnerRequest = management_request_json(&state, request).await?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    TenantAuthorizationRepository::transfer_resource_ownership(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        OwnershipTransferTarget::Asset(asset_id),
        request.user_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) fn management_user_role(
    value: &str,
) -> Result<ManagementUserRole, ManagementSessionError> {
    match value {
        "admin" => Ok(ManagementUserRole::Admin),
        "viewer" => Ok(ManagementUserRole::Viewer),
        _ => Err(ManagementSessionError::BadRequest),
    }
}

pub(super) fn management_user_capability(
    value: &str,
) -> Result<UserCapability, ManagementSessionError> {
    match value {
        "create_assets" => Ok(UserCapability::CreateAssets),
        "create_devices" => Ok(UserCapability::CreateDevices),
        "claim_devices" => Ok(UserCapability::ClaimDevices),
        "assign_devices_to_assets" => Ok(UserCapability::AssignDevicesToAssets),
        "edit_resources" => Ok(UserCapability::EditResources),
        "control_devices" => Ok(UserCapability::ControlDevices),
        "share_owned_resources" => Ok(UserCapability::ShareOwnedResources),
        "assign_application_profiles" => Ok(UserCapability::AssignApplicationProfiles),
        "manage_device_tokens" => Ok(UserCapability::ManageDeviceTokens),
        _ => Err(ManagementSessionError::BadRequest),
    }
}

pub(super) fn management_user_response(user: ManagementUser) -> ManagementUserResponse {
    ManagementUserResponse {
        id: user.id,
        username: user.username,
        role: user.role.as_str().to_owned(),
        account_class: user.account_class.as_str().to_owned(),
        capabilities: user
            .capabilities
            .into_iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
    }
}

pub(super) fn management_alert_response(alert: StorageManagementAlert) -> ManagementAlertResponse {
    ManagementAlertResponse {
        id: alert.id,
        rule_name: alert.rule_name,
        severity: alert.severity,
        device_id: alert.device_id,
        status: alert.status,
        last_value: alert.last_value,
        updated_at: alert.updated_at,
    }
}

pub(super) fn management_alert_rule_response(
    rule: StorageManagementAlertRule,
) -> ManagementAlertRuleResponse {
    ManagementAlertRuleResponse {
        id: rule.id,
        name: rule.name,
        enabled: rule.enabled,
        device_id: rule.device_id,
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
        archived_at: rule.archived_at,
        updated_at: rule.updated_at,
    }
}

pub(super) fn management_alert_incident_response(
    incident: StorageManagementAlertIncident,
) -> ManagementAlertIncidentResponse {
    ManagementAlertIncidentResponse {
        id: incident.id,
        rule_id: incident.rule_id,
        rule_name: incident.rule_name,
        severity: incident.severity,
        device_id: incident.device_id,
        status: incident.status,
        last_value: incident.last_value,
        condition_started_at: incident.condition_started_at,
        opened_at: incident.opened_at,
        resolved_at: incident.resolved_at,
        acknowledged_at: incident.acknowledged_at,
        acknowledged_by: incident.acknowledged_by,
        updated_at: incident.updated_at,
    }
}

pub(super) fn management_audit_event_response(event: AuditEvent) -> ManagementAuditEventResponse {
    let (actor_kind, actor_id) = audit_actor(event.actor);
    ManagementAuditEventResponse {
        id: event.id,
        occurred_at: event.occurred_at,
        actor_kind,
        actor_id,
        action: audit_action(event.action),
        target_type: audit_target_type(event.target_type),
        target_id: event.target_id,
        changes: event.changes,
    }
}

pub(super) fn tenant_audit_row(
    event: AuditEvent,
) -> Result<crate::TenantAuditRow, ManagementSessionError> {
    let (_, actor_id) = audit_actor(event.actor);
    let target_type = audit_target_type(event.target_type);
    let changes = serde_json::to_string_pretty(&event.changes)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(crate::TenantAuditRow::new(
        event.occurred_at.to_rfc3339(),
        audit_actor_label(event.actor),
        actor_id.to_string(),
        audit_action(event.action),
        format!("{target_type}: {}", event.target_id),
        changes,
    ))
}

pub(super) fn audit_actor(actor: AuditPrincipal) -> (&'static str, Uuid) {
    match actor {
        AuditPrincipal::SystemAccount(id) => ("system_account", id),
        AuditPrincipal::TenantAccount(id) => ("tenant_account", id),
        AuditPrincipal::User(id) => ("user", id),
    }
}

pub(super) fn audit_actor_label(actor: AuditPrincipal) -> &'static str {
    match actor {
        AuditPrincipal::SystemAccount(_) => "System account",
        AuditPrincipal::TenantAccount(_) => "Tenant account",
        AuditPrincipal::User(_) => "User",
    }
}

pub(super) fn audit_action(action: AuditAction) -> &'static str {
    match action {
        AuditAction::PermissionGranted => "permission.granted",
        AuditAction::PermissionRevoked => "permission.revoked",
        AuditAction::GroupMemberAdded => "group.member_added",
        AuditAction::GroupMemberRemoved => "group.member_removed",
        AuditAction::OwnershipTransferred => "ownership.transferred",
        AuditAction::AssetContainmentChanged => "asset.containment_changed",
        AuditAction::GatewayAssigned => "gateway.assigned",
        AuditAction::GatewayDetached => "gateway.detached",
        AuditAction::GatewayReassigned => "gateway.reassigned",
        AuditAction::DeviceRelationCreated => "device_relation.created",
        AuditAction::DeviceRelationDeleted => "device_relation.deleted",
    }
}

pub(super) fn audit_target_type(target_type: AuditTargetType) -> &'static str {
    match target_type {
        AuditTargetType::ResourcePermission => "resource_permission",
        AuditTargetType::UserGroup => "user_group",
        AuditTargetType::Asset => "asset",
        AuditTargetType::Device => "device",
        AuditTargetType::DeviceRelation => "device_relation",
    }
}

pub(super) fn tenant_audit_limit(value: Option<usize>) -> Result<usize, ManagementSessionError> {
    let value = value.unwrap_or(DEFAULT_TENANT_AUDIT_LIMIT);
    if !(1..=MAX_TENANT_AUDIT_LIMIT).contains(&value) {
        return Err(ManagementSessionError::BadRequest);
    }
    Ok(value)
}

pub(super) fn decode_tenant_audit_cursor(
    vault: &TokenVault,
    tenant_id: Uuid,
    value: Option<&str>,
) -> Result<Option<AuditEventCursor>, ManagementSessionError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let plaintext = vault
        .decrypt(value)
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let cursor: TenantAuditCursor =
        serde_json::from_str(&plaintext).map_err(|_| ManagementSessionError::BadRequest)?;
    if cursor.version != 1 || cursor.tenant_id != tenant_id {
        return Err(ManagementSessionError::BadRequest);
    }
    Ok(Some(AuditEventCursor {
        occurred_at: cursor.occurred_at,
        id: cursor.id,
    }))
}

pub(super) fn encode_tenant_audit_cursor(
    vault: &TokenVault,
    tenant_id: Uuid,
    event: &AuditEvent,
) -> Result<String, ManagementSessionError> {
    let plaintext = serde_json::to_string(&TenantAuditCursor {
        version: 1,
        tenant_id,
        occurred_at: event.occurred_at,
        id: event.id,
    })
    .map_err(|_| ManagementSessionError::Unavailable)?;
    vault
        .encrypt(&plaintext)
        .map_err(|_| ManagementSessionError::Unavailable)
}

pub(super) fn audit_event_cursor(event: &AuditEvent) -> AuditEventCursor {
    AuditEventCursor {
        occurred_at: event.occurred_at,
        id: event.id,
    }
}

pub(super) fn management_device_profile_response(
    profile: ManagementDeviceProfile,
) -> ManagementDeviceProfileResponse {
    ManagementDeviceProfileResponse {
        id: profile.id,
        name: profile.name,
        telemetry_schema: profile.telemetry_schema,
        metric_mapping: profile.metric_mapping,
        reporting_settings: profile.reporting_settings,
    }
}

pub(super) fn management_asset_profile_response(
    profile: ManagementAssetProfile,
) -> ManagementAssetProfileResponse {
    ManagementAssetProfileResponse {
        id: profile.id,
        name: profile.name,
        fields: profile.fields,
        dashboard_defaults: profile.dashboard_defaults,
    }
}

pub(super) fn require_system_account(
    session_verifier: &ManagementSessionVerifier,
    headers: &HeaderMap,
) -> Result<(), ManagementSessionError> {
    match session_verifier.system_authorization(headers) {
        ManagementAuthorization::Unauthenticated => Err(ManagementSessionError::Unauthorized),
        ManagementAuthorization::System => Ok(()),
        ManagementAuthorization::Forbidden => Err(ManagementSessionError::Forbidden),
    }
}

pub(super) async fn authorize_tenant_mutation<'a>(
    state: &'a ManagementState,
    headers: &HeaderMap,
) -> Result<ManagementMutationLease<'a>, ManagementSessionError> {
    let lease = state.authorization_gate.acquire_mutation().await;
    require_tenant_account(&state.session_verifier, headers)?;
    Ok(lease)
}

pub(super) fn require_tenant_account(
    session_verifier: &ManagementSessionVerifier,
    headers: &HeaderMap,
) -> Result<TenantSession, ManagementSessionError> {
    if let Some(tenant) = session_verifier.tenant_session(headers) {
        return Ok(tenant);
    }
    match session_verifier.platform_session(headers) {
        Err(ManagementSessionError::Unauthorized) => Err(ManagementSessionError::Unauthorized),
        Ok(_) | Err(ManagementSessionError::Forbidden) => Err(ManagementSessionError::Forbidden),
        Err(error) => Err(error),
    }
}

pub(super) async fn management_request_json<T>(
    state: &ManagementState,
    request: Request,
) -> Result<T, ManagementSessionError>
where
    T: DeserializeOwned,
{
    Json::<T>::from_request(request, state)
        .await
        .map(|Json(value)| value)
        .map_err(|rejection| match rejection.into_response().status() {
            StatusCode::UNSUPPORTED_MEDIA_TYPE => ManagementSessionError::UnsupportedMediaType,
            StatusCode::PAYLOAD_TOO_LARGE => ManagementSessionError::PayloadTooLarge,
            _ => ManagementSessionError::BadRequest,
        })
}

pub(super) async fn management_request_form<T>(
    state: &ManagementState,
    request: Request,
) -> Result<T, ManagementSessionError>
where
    T: DeserializeOwned,
{
    Form::<T>::from_request(request, state)
        .await
        .map(|Form(value)| value)
        .map_err(|rejection| match rejection.into_response().status() {
            StatusCode::UNSUPPORTED_MEDIA_TYPE => ManagementSessionError::UnsupportedMediaType,
            StatusCode::PAYLOAD_TOO_LARGE => ManagementSessionError::PayloadTooLarge,
            _ => ManagementSessionError::BadRequest,
        })
}

pub(super) async fn create_device_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, HeaderValue); 1],
        Json<DeviceTokenResponse>,
    ),
    ManagementSessionError,
> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let token = create_platform_device_token(
        &state.store,
        &state.token_vault,
        tenant.tenant_id,
        &device_id,
    )
    .await
    .map_err(management_device_token_error)?;
    Ok((
        StatusCode::CREATED,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(token),
    ))
}

pub(super) async fn issue_management_device_claim_code(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, HeaderValue); 1],
        Json<ManagementDeviceClaimCodeResponse>,
    ),
    ManagementSessionError,
> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let serial_number =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_error)?
            .into_iter()
            .find(|device| device.device_id == device_id)
            .and_then(|device| device.serial_number)
            .ok_or(ManagementSessionError::Conflict)?;
    let issued = DeviceClaimRepository::issue_device_claim_code_from_console(
        state.store.as_ref(),
        tenant.tenant_id,
        &device_id,
    )
    .await
    .map_err(management_device_claim_error)?;
    let pairing_uri = format!(
        "iotnano://claim?serial_number={serial_number}&code={}",
        issued.code
    );
    let qr_svg = device_claim_qr_svg(&pairing_uri)?;
    Ok((
        StatusCode::CREATED,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(ManagementDeviceClaimCodeResponse {
            device_id,
            serial_number,
            code: issued.code,
            expires_at: issued.expires_at,
            pairing_uri,
            qr_svg,
        }),
    ))
}

fn device_claim_qr_svg(pairing_uri: &str) -> Result<String, ManagementSessionError> {
    let qr = QrCode::encode_text(pairing_uri, QrCodeEcc::Medium)
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let border = 4;
    let size = qr.size() + border * 2;
    let mut modules = String::new();
    for y in 0..qr.size() {
        for x in 0..qr.size() {
            if qr.get_module(x, y) {
                write!(&mut modules, "M{} {}h1v1h-1z", x + border, y + border)
                    .map_err(|_| ManagementSessionError::Unavailable)?;
            }
        }
    }
    Ok(format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {size} {size}\" role=\"img\" aria-label=\"Pairing QR code\"><rect width=\"100%\" height=\"100%\" fill=\"white\"/><path d=\"{modules}\" fill=\"black\"/></svg>"
    ))
}

pub(super) async fn reveal_management_device_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<
    (
        [(axum::http::header::HeaderName, HeaderValue); 1],
        Json<DeviceTokenResponse>,
    ),
    ManagementSessionError,
> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let token = reveal_platform_device_token(
        state.store.as_ref(),
        &state.token_vault,
        tenant.tenant_id,
        &device_id,
    )
    .await
    .map_err(management_device_token_error)?;
    Ok((
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(token),
    ))
}

pub(super) async fn rotate_management_device_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path((device_id, token_id)): Path<(String, String)>,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, HeaderValue); 1],
        Json<DeviceTokenResponse>,
    ),
    ManagementSessionError,
> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let token_id = Uuid::parse_str(&token_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let active = DeviceTokenRepository::active_device_token(
        state.store.as_ref(),
        tenant.tenant_id,
        token_id,
    )
    .await
    .map_err(management_device_token_repository_error)?
    .ok_or(ManagementSessionError::NotFound)?;
    if active.device_id != device_id {
        return Err(ManagementSessionError::NotFound);
    }

    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let token = rotate_platform_device_token(
        state.store.as_ref(),
        &state.token_vault,
        tenant.tenant_id,
        token_id,
    )
    .await
    .map_err(management_device_token_error)?;
    Ok((
        StatusCode::CREATED,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(token),
    ))
}

#[cfg(test)]
pub(super) async fn pause_after_mutation_authorization(state: &ManagementState) {
    let Some(hooks) = &state.authorization_test_hooks else {
        return;
    };
    if !hooks.pause_mutation.swap(false, Ordering::SeqCst) {
        return;
    }
    hooks.mutation_authorized.wait().await;
    hooks.release_mutation.wait().await;
}
