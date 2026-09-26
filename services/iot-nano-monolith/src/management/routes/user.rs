use super::super::*;

pub(in crate::management) async fn platform_app(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let PlatformUiSession::User { user_id, tenant_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    platform_page(
        &state,
        PlatformUiSession::User { user_id, tenant_id },
        user_resource_permission_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_app_invitations(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let session = user_workspace_session(&state, &headers)?;
    let invitations = ResourceInvitationRepository::list_pending_resource_invitations(
        state.store.as_ref(),
        session.tenant_id,
        session.user_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    let asset_names =
        ManagementAssetRepository::list_management_assets(state.store.as_ref(), session.tenant_id)
            .await
            .map_err(management_asset_error)?
            .into_iter()
            .map(|asset| (asset.id, asset.name))
            .collect::<HashMap<_, _>>();
    let device_names = ManagementDeviceRepository::list_management_devices(
        state.store.as_ref(),
        session.tenant_id,
    )
    .await
    .map_err(management_device_error)?
    .into_iter()
    .map(|device| {
        let device_id = device.device_id;
        let display_name = device.display_name.unwrap_or_else(|| device_id.clone());
        (device_id, display_name)
    })
    .collect::<HashMap<_, _>>();
    let usernames =
        ManagementUserRepository::list_management_users(state.store.as_ref(), session.tenant_id)
            .await
            .map_err(management_user_error)?
            .into_iter()
            .map(|user| (user.id, user.username))
            .collect::<HashMap<_, _>>();
    let page = crate::UserInvitationPage::new(
        invitations
            .iter()
            .map(|invitation| {
                let (resource_kind, resource_name) =
                    match (invitation.asset_id, invitation.device_id.as_deref()) {
                        (Some(asset_id), None) => (
                            "Asset",
                            asset_names
                                .get(&asset_id)
                                .cloned()
                                .unwrap_or_else(|| asset_id.to_string()),
                        ),
                        (None, Some(device_id)) => (
                            "Device",
                            device_names
                                .get(device_id)
                                .cloned()
                                .unwrap_or_else(|| device_id.to_owned()),
                        ),
                        _ => ("Resource", "Unavailable".to_owned()),
                    };
                crate::UserInvitationRow::new(
                    invitation.id,
                    resource_kind,
                    resource_name,
                    usernames
                        .get(&invitation.sender_user_id)
                        .cloned()
                        .unwrap_or_else(|| invitation.sender_user_id.to_string()),
                    resource_permission_label(invitation.permission),
                )
            })
            .collect(),
    );
    let identity = crate::PlatformUiIdentity::new(format!("User {}", session.user_id))
        .with_invitation_count(invitations.len());
    let rendered = crate::PlatformUiRenderer::render_user_invitations(&identity, &page)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn platform_app_device_detail(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Response, ManagementSessionError> {
    let headers = request.headers().clone();
    let notice = user_resource_permission_notice(request.uri().query());
    let PlatformUiSession::User { user_id, tenant_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    let subject = user_authorization_subject(&state, user_id, tenant_id).await?;
    let identity = user_platform_identity(&state, user_id, tenant_id).await?;
    let device =
        AuthorizationRepository::authorized_device(state.store.as_ref(), &subject, &device_id)
            .await
            .map_err(|_| ManagementSessionError::Unavailable)?;

    match device {
        Some(device) => {
            let can_manage_access = device.access.source == ResourceAccessSource::Owner;
            let activity = UserDeviceActivityRepository::recent_user_device_activity(
                state.store.as_ref(),
                tenant_id,
                &device.device_id,
                USER_DEVICE_ACTIVITY_LIMIT,
            )
            .await
            .map_err(|_| ManagementSessionError::Unavailable)?;
            let mut page = crate::UserDeviceDetailPage::new(user_device_row(
                device.device_id,
                device.display_name,
                device.last_seen_at,
                device.access,
            ))
            .with_activity(
                activity
                    .telemetry
                    .into_iter()
                    .map(|telemetry| {
                        crate::UserDeviceTelemetryRow::new(
                            user_workspace_timestamp(telemetry.event_at),
                            telemetry.measurements.to_string(),
                        )
                    })
                    .collect(),
                activity
                    .alerts
                    .into_iter()
                    .map(|alert| {
                        crate::UserDeviceAlertRow::new(
                            alert.rule_name,
                            user_alert_label(alert.severity),
                            user_alert_label(alert.status),
                            user_workspace_timestamp(alert.updated_at),
                        )
                    })
                    .collect(),
            );
            if can_manage_access {
                let can_edit = user_capability_enabled(
                    &state,
                    tenant_id,
                    user_id,
                    UserCapability::EditResources,
                )
                .await?;
                let can_share = true;
                let owned_assets = if can_edit {
                    let selected_asset_id = ManagementDeviceRepository::list_management_devices(
                        state.store.as_ref(),
                        tenant_id,
                    )
                    .await
                    .map_err(management_device_error)?
                    .into_iter()
                    .find(|managed| managed.device_id == device_id)
                    .and_then(|managed| managed.asset_id);
                    user_owned_asset_rows(&state, &subject, selected_asset_id).await?
                } else {
                    Vec::new()
                };
                page = page.with_capabilities(owned_assets, can_edit, can_share);
                if can_share {
                    let permissions = user_resource_permission_rows(
                        &state,
                        tenant_id,
                        &UserWorkspacePermissionScope::Device(device_id),
                    )
                    .await?;
                    page = page.with_access_management(permissions, notice);
                }
            }
            let rendered = crate::PlatformUiRenderer::render_user_device(&identity, &page)
                .map_err(|_| ManagementSessionError::Unavailable)?;
            Ok(Html(rendered).into_response())
        }
        None => {
            let rendered = crate::PlatformUiRenderer::render_user_device_unavailable(&identity)
                .map_err(|_| ManagementSessionError::Unavailable)?;
            Ok((StatusCode::NOT_FOUND, Html(rendered)).into_response())
        }
    }
}

pub(in crate::management) async fn create_user_device_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    require_user_capability(&state, &session, UserCapability::CreateDevices).await?;
    let form: UserDeviceForm = management_request_form(&state, request).await?;
    let display_name = form.display_name.trim();
    if display_name.is_empty() || display_name.len() > 128 {
        return Err(ManagementSessionError::BadRequest);
    }
    let device = provision_owned_platform_device_token(
        state.store.as_ref(),
        &state.token_vault,
        session.tenant_id,
        AuditPrincipal::User(session.user_id),
        display_name,
        session.user_id,
        user_optional_uuid(form.asset_id)?,
    )
    .await
    .map_err(management_device_token_error)?;
    Ok(Redirect::to(&format!(
        "/app/devices/{}?notice=device-created",
        device.device_id
    )))
}

pub(in crate::management) async fn claim_user_device_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    require_user_capability(&state, &session, UserCapability::ClaimDevices).await?;
    let form: UserClaimDeviceForm = match management_request_form(&state, request).await {
        Ok(form) => form,
        Err(error) => return user_claim_device_form_error(error),
    };
    let device_id = form.device_id.trim();
    let code = form.code.trim();
    if device_id.is_empty() || device_id.len() > 128 || code.is_empty() || code.len() > 40 {
        return user_claim_device_form_error(ManagementSessionError::BadRequest);
    }
    match DeviceClaimRepository::claim_device_with_code(
        state.store.as_ref(),
        session.tenant_id,
        session.user_id,
        device_id,
        code,
    )
    .await
    {
        Ok(_) => Ok(Redirect::to("/app?notice=device-claimed")),
        Err(error) => user_claim_device_form_error(management_device_claim_error(error)),
    }
}

pub(in crate::management) async fn update_user_device_form(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    require_user_capability(&state, &session, UserCapability::EditResources).await?;
    let form: UserDeviceForm = management_request_form(&state, request).await?;
    let subject = user_authorization_subject(&state, session.user_id, session.tenant_id).await?;
    if !user_can_manage_resource(
        &state,
        &subject,
        &UserWorkspacePermissionScope::Device(device_id.clone()),
    )
    .await?
    {
        return Err(ManagementSessionError::Forbidden);
    }
    let current = ManagementDeviceRepository::list_management_devices(
        state.store.as_ref(),
        session.tenant_id,
    )
    .await
    .map_err(management_device_error)?
    .into_iter()
    .find(|device| device.device_id == device_id)
    .ok_or(ManagementSessionError::NotFound)?;
    ManagementDeviceRepository::update_management_device(
        state.store.as_ref(),
        session.tenant_id,
        AuditPrincipal::User(session.user_id),
        &device_id,
        UpdateManagementDevice {
            display_name: form.display_name,
            asset_id: user_optional_uuid(form.asset_id)?,
            device_profile_id: current.device_profile_id,
            attributes: None,
            topology: None,
        },
    )
    .await
    .map_err(management_device_error)?;
    Ok(Redirect::to(&format!(
        "/app/devices/{device_id}?notice=device-saved"
    )))
}

pub(in crate::management) async fn platform_app_assets(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let PlatformUiSession::User { user_id, tenant_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    let subject = user_authorization_subject(&state, user_id, tenant_id).await?;
    let assets = AuthorizationRepository::list_authorized_assets(
        state.store.as_ref(),
        &subject,
        None,
        USER_ASSET_LIST_LIMIT,
    )
    .await
    .map_err(|_| ManagementSessionError::Unavailable)?;
    let mut page = crate::UserAssetListPage::new(
        assets
            .into_iter()
            .map(|asset| {
                user_asset_row(
                    asset.asset_id,
                    asset.name,
                    asset.parent_asset_id,
                    asset.access,
                )
            })
            .collect(),
    );
    if user_capability_enabled(&state, tenant_id, user_id, UserCapability::CreateAssets).await? {
        page = page.with_management(user_owned_asset_rows(&state, &subject, None).await?);
    }
    let rendered = crate::PlatformUiRenderer::render_user_assets(
        &user_platform_identity(&state, user_id, tenant_id).await?,
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn platform_app_asset_detail(
    State(state): State<ManagementState>,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<Response, ManagementSessionError> {
    let headers = request.headers().clone();
    let notice = user_resource_permission_notice(request.uri().query());
    let PlatformUiSession::User { user_id, tenant_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    let identity = user_platform_identity(&state, user_id, tenant_id).await?;
    let Some(asset_id) = Uuid::parse_str(&asset_id).ok() else {
        return render_user_asset_unavailable(&identity);
    };
    let subject = user_authorization_subject(&state, user_id, tenant_id).await?;
    let asset = AuthorizationRepository::authorized_asset(state.store.as_ref(), &subject, asset_id)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;

    match asset {
        Some(asset) => {
            let can_manage_access = asset.access.source == ResourceAccessSource::Owner;
            let selected_parent_asset_id = asset.parent_asset_id;
            let mut page = crate::UserAssetDetailPage::new(user_asset_row(
                asset.asset_id,
                asset.name,
                asset.parent_asset_id,
                asset.access,
            ));
            if can_manage_access {
                let can_edit = user_capability_enabled(
                    &state,
                    tenant_id,
                    user_id,
                    UserCapability::EditResources,
                )
                .await?;
                let can_share = true;
                let owned_assets = if can_edit {
                    user_owned_asset_rows(&state, &subject, selected_parent_asset_id).await?
                } else {
                    Vec::new()
                };
                page = page.with_capabilities(owned_assets, can_edit, can_share);
                if can_share {
                    let permissions = user_resource_permission_rows(
                        &state,
                        tenant_id,
                        &UserWorkspacePermissionScope::Asset(asset_id),
                    )
                    .await?;
                    page = page.with_access_management(permissions, notice);
                }
            }
            let rendered = crate::PlatformUiRenderer::render_user_asset(&identity, &page)
                .map_err(|_| ManagementSessionError::Unavailable)?;
            Ok(Html(rendered).into_response())
        }
        None => render_user_asset_unavailable(&identity),
    }
}

pub(in crate::management) async fn create_user_asset_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    require_user_capability(&state, &session, UserCapability::CreateAssets).await?;
    let form: UserAssetForm = management_request_form(&state, request).await?;
    let asset = ManagementAssetRepository::create_management_asset(
        state.store.as_ref(),
        session.tenant_id,
        AuditPrincipal::User(session.user_id),
        CreateManagementAsset {
            name: form.name,
            asset_profile_id: None,
            parent_asset_id: user_optional_uuid(form.parent_asset_id)?,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    .map_err(management_asset_error)?;
    Ok(Redirect::to(&format!(
        "/app/assets/{}?notice=asset-created",
        asset.id
    )))
}

pub(in crate::management) async fn update_user_asset_form(
    State(state): State<ManagementState>,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    require_user_capability(&state, &session, UserCapability::EditResources).await?;
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let form: UserAssetForm = management_request_form(&state, request).await?;
    let subject = user_authorization_subject(&state, session.user_id, session.tenant_id).await?;
    if !user_can_manage_resource(
        &state,
        &subject,
        &UserWorkspacePermissionScope::Asset(asset_id),
    )
    .await?
    {
        return Err(ManagementSessionError::Forbidden);
    }
    let current =
        ManagementAssetRepository::list_management_assets(state.store.as_ref(), session.tenant_id)
            .await
            .map_err(management_asset_error)?
            .into_iter()
            .find(|asset| asset.id == asset_id)
            .ok_or(ManagementSessionError::NotFound)?;
    ManagementAssetRepository::update_management_asset(
        state.store.as_ref(),
        session.tenant_id,
        AuditPrincipal::User(session.user_id),
        asset_id,
        UpdateManagementAsset {
            name: form.name,
            asset_profile_id: current.asset_profile_id,
            parent_asset_id: user_optional_uuid(form.parent_asset_id)?,
            metadata: current.metadata,
            attributes: None,
        },
    )
    .await
    .map_err(management_asset_error)?;
    Ok(Redirect::to(&format!(
        "/app/assets/{asset_id}?notice=asset-saved"
    )))
}

pub(in crate::management) fn user_optional_uuid(
    value: Option<String>,
) -> Result<Option<Uuid>, ManagementSessionError> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| ManagementSessionError::BadRequest)
}

pub(in crate::management) fn render_user_asset_unavailable(
    identity: &crate::PlatformUiIdentity,
) -> Result<Response, ManagementSessionError> {
    let rendered = crate::PlatformUiRenderer::render_user_asset_unavailable(identity)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok((StatusCode::NOT_FOUND, Html(rendered)).into_response())
}

#[derive(Clone)]
pub(in crate::management) enum UserWorkspacePermissionScope {
    Asset(Uuid),
    Device(String),
}

impl UserWorkspacePermissionScope {
    fn detail_path(&self) -> String {
        match self {
            Self::Asset(asset_id) => format!("/app/assets/{asset_id}"),
            Self::Device(device_id) => format!("/app/devices/{device_id}"),
        }
    }

    fn matches_permission(&self, permission: &iot_storage::ResourcePermissionRecord) -> bool {
        match self {
            Self::Asset(asset_id) => permission.asset_id == Some(*asset_id),
            Self::Device(device_id) => permission.device_id.as_deref() == Some(device_id),
        }
    }

    fn ownership_target(&self) -> OwnershipTransferTarget {
        match self {
            Self::Asset(asset_id) => OwnershipTransferTarget::Asset(*asset_id),
            Self::Device(device_id) => OwnershipTransferTarget::Device(device_id.clone()),
        }
    }
}

pub(in crate::management) async fn create_user_asset_permission_form(
    State(state): State<ManagementState>,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    create_user_resource_permission_form(
        state,
        UserWorkspacePermissionScope::Asset(asset_id),
        request,
    )
    .await
}

pub(in crate::management) async fn create_user_device_permission_form(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    if device_id.trim().is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    create_user_resource_permission_form(
        state,
        UserWorkspacePermissionScope::Device(device_id),
        request,
    )
    .await
}

pub(in crate::management) async fn create_user_resource_permission_form(
    state: ManagementState,
    scope: UserWorkspacePermissionScope,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    let form: UserResourcePermissionForm = match management_request_form(&state, request).await {
        Ok(form) => form,
        Err(error) => return user_resource_permission_form_error(&scope, error),
    };
    let subject = user_authorization_subject(&state, session.user_id, session.tenant_id).await?;
    if !user_can_manage_resource(&state, &subject, &scope).await? {
        return Err(ManagementSessionError::Forbidden);
    }
    let permission = match user_assignable_permission(&form.permission) {
        Ok(permission) => permission,
        Err(error) => return user_resource_permission_form_error(&scope, error),
    };
    let target_username = form.username.trim();
    if target_username.is_empty() {
        return user_resource_permission_form_error(&scope, ManagementSessionError::BadRequest);
    }
    let target = match ManagementUserRepository::list_management_users(
        state.store.as_ref(),
        session.tenant_id,
    )
    .await
    .map_err(management_user_error)?
    .into_iter()
    .find(|user| user.username == target_username)
    {
        Some(target) => target,
        None => {
            return user_resource_permission_form_error(&scope, ManagementSessionError::NotFound);
        }
    };
    if target.id == session.user_id {
        return user_resource_permission_form_error(&scope, ManagementSessionError::BadRequest);
    }
    match ResourceInvitationRepository::create_owner_resource_invitation(
        state.store.as_ref(),
        session.tenant_id,
        session.user_id,
        target.id,
        scope.ownership_target(),
        permission,
    )
    .await
    {
        Ok(_) => Ok(user_resource_permission_redirect(
            &scope,
            "invitation-created",
        )),
        Err(error) => {
            user_resource_permission_form_error(&scope, tenant_authorization_error(error))
        }
    }
}

pub(in crate::management) async fn accept_user_resource_invitation_form(
    State(state): State<ManagementState>,
    Path(invitation_id): Path<String>,
    headers: HeaderMap,
) -> Result<Redirect, ManagementSessionError> {
    let session = user_workspace_session(&state, &headers)?;
    let invitation_id =
        Uuid::parse_str(&invitation_id).map_err(|_| ManagementSessionError::BadRequest)?;
    ResourceInvitationRepository::accept_resource_invitation(
        state.store.as_ref(),
        session.tenant_id,
        session.user_id,
        invitation_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    Ok(Redirect::to("/app/invitations?notice=accepted"))
}

pub(in crate::management) async fn cancel_user_resource_invitation_form(
    State(state): State<ManagementState>,
    Path(invitation_id): Path<String>,
    headers: HeaderMap,
) -> Result<Redirect, ManagementSessionError> {
    let session = user_workspace_session(&state, &headers)?;
    let invitation_id =
        Uuid::parse_str(&invitation_id).map_err(|_| ManagementSessionError::BadRequest)?;
    ResourceInvitationRepository::cancel_resource_invitation(
        state.store.as_ref(),
        session.tenant_id,
        session.user_id,
        invitation_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    Ok(Redirect::to("/app/invitations?notice=cancelled"))
}

pub(in crate::management) async fn revoke_user_asset_permission_form(
    State(state): State<ManagementState>,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    revoke_user_resource_permission_form(
        state,
        UserWorkspacePermissionScope::Asset(asset_id),
        request,
    )
    .await
}

pub(in crate::management) async fn revoke_user_device_permission_form(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    if device_id.trim().is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    revoke_user_resource_permission_form(
        state,
        UserWorkspacePermissionScope::Device(device_id),
        request,
    )
    .await
}

pub(in crate::management) async fn revoke_user_resource_permission_form(
    state: ManagementState,
    scope: UserWorkspacePermissionScope,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let session = user_workspace_session(&state, &headers)?;
    let form: RevokeUserResourcePermissionForm =
        match management_request_form(&state, request).await {
            Ok(form) => form,
            Err(error) => return user_resource_permission_form_error(&scope, error),
        };
    let subject = user_authorization_subject(&state, session.user_id, session.tenant_id).await?;
    if !user_can_manage_resource(&state, &subject, &scope).await? {
        return Err(ManagementSessionError::Forbidden);
    }
    let permission = TenantAuthorizationRepository::list_active_resource_permissions(
        state.store.as_ref(),
        session.tenant_id,
    )
    .await
    .map_err(tenant_authorization_error)?
    .into_iter()
    .find(|permission| permission.id == form.permission_id && scope.matches_permission(permission))
    .ok_or(ManagementSessionError::NotFound)?;
    match TenantAuthorizationRepository::revoke_owner_resource_permission(
        state.store.as_ref(),
        session.tenant_id,
        session.user_id,
        scope.ownership_target(),
        permission.id,
    )
    .await
    {
        Ok(true) => Ok(user_resource_permission_redirect(
            &scope,
            "permission-revoked",
        )),
        Ok(false) => user_resource_permission_form_error(&scope, ManagementSessionError::NotFound),
        Err(error) => {
            user_resource_permission_form_error(&scope, tenant_authorization_error(error))
        }
    }
}

pub(in crate::management) fn user_workspace_session(
    state: &ManagementState,
    headers: &HeaderMap,
) -> Result<UserSession, ManagementSessionError> {
    let PlatformUiSession::User { user_id, tenant_id } =
        state.session_verifier.platform_session(headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    Ok(UserSession { user_id, tenant_id })
}

pub(in crate::management) async fn require_user_capability(
    state: &ManagementState,
    session: &UserSession,
    capability: UserCapability,
) -> Result<(), ManagementSessionError> {
    let enabled =
        user_capability_enabled(state, session.tenant_id, session.user_id, capability).await?;
    if enabled {
        Ok(())
    } else {
        Err(ManagementSessionError::Forbidden)
    }
}

pub(in crate::management) async fn user_capability_enabled(
    state: &ManagementState,
    tenant_id: Uuid,
    user_id: Uuid,
    capability: UserCapability,
) -> Result<bool, ManagementSessionError> {
    ManagementUserRepository::user_has_management_capability(
        state.store.as_ref(),
        tenant_id,
        user_id,
        capability,
    )
    .await
    .map_err(management_user_error)
}

pub(in crate::management) async fn user_can_manage_resource(
    state: &ManagementState,
    subject: &AuthorizationSubject,
    scope: &UserWorkspacePermissionScope,
) -> Result<bool, ManagementSessionError> {
    let resource = match scope {
        UserWorkspacePermissionScope::Asset(asset_id) => {
            AuthorizationRepository::authorized_asset(state.store.as_ref(), subject, *asset_id)
                .await
                .map(|resource| resource.map(|resource| resource.access.source))
        }
        UserWorkspacePermissionScope::Device(device_id) => {
            AuthorizationRepository::authorized_device(state.store.as_ref(), subject, device_id)
                .await
                .map(|resource| resource.map(|resource| resource.access.source))
        }
    }
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(resource == Some(ResourceAccessSource::Owner))
}

pub(in crate::management) async fn user_resource_permission_rows(
    state: &ManagementState,
    tenant_id: Uuid,
    scope: &UserWorkspacePermissionScope,
) -> Result<Vec<crate::UserResourcePermissionRow>, ManagementSessionError> {
    let users = ManagementUserRepository::list_management_users(state.store.as_ref(), tenant_id)
        .await
        .map_err(management_user_error)?;
    let usernames: HashMap<Uuid, String> = users
        .into_iter()
        .map(|user| (user.id, user.username))
        .collect();
    let permissions = TenantAuthorizationRepository::list_active_resource_permissions(
        state.store.as_ref(),
        tenant_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    Ok(permissions
        .into_iter()
        .filter(|permission| scope.matches_permission(permission))
        .filter_map(|permission| {
            let user_id = permission.subject_user_id?;
            Some(crate::UserResourcePermissionRow::new(
                permission.id.to_string(),
                usernames
                    .get(&user_id)
                    .cloned()
                    .unwrap_or_else(|| user_id.to_string()),
                resource_permission_label(permission.permission),
                if permission.inherit_children {
                    "Child assets and devices"
                } else {
                    "None"
                },
            ))
        })
        .collect())
}

pub(in crate::management) fn user_assignable_permission(
    value: &str,
) -> Result<ResourcePermission, ManagementSessionError> {
    match value {
        "view" => Ok(ResourcePermission::Viewer),
        "control" => Ok(ResourcePermission::Manager),
        _ => Err(ManagementSessionError::BadRequest),
    }
}

pub(in crate::management) fn management_device_claim_error(
    error: DeviceClaimError,
) -> ManagementSessionError {
    match error {
        DeviceClaimError::InvalidPolicy => ManagementSessionError::BadRequest,
        DeviceClaimError::UserUnavailable => ManagementSessionError::Forbidden,
        DeviceClaimError::PolicyDisabled
        | DeviceClaimError::DeviceUnavailable
        | DeviceClaimError::RequestCoolingDown
        | DeviceClaimError::CodeUnavailable => ManagementSessionError::Conflict,
        DeviceClaimError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(in crate::management) fn user_claim_device_form_error(
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => Err(error),
        ManagementSessionError::Unavailable => Ok(Redirect::to("/app?notice=service-unavailable")),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge
        | ManagementSessionError::NotFound
        | ManagementSessionError::Conflict => Ok(Redirect::to("/app?notice=claim-unavailable")),
    }
}

pub(in crate::management) fn user_resource_permission_notice(
    query: Option<&str>,
) -> Option<&'static str> {
    match query {
        Some("notice=invitation-created") => Some("Invitation sent."),
        Some("notice=permission-revoked") => Some("User access revoked."),
        Some("notice=asset-created") => Some("Asset created."),
        Some("notice=asset-saved") => Some("Asset saved."),
        Some("notice=device-created") => Some("Device created."),
        Some("notice=device-claimed") => Some("Device added to your workspace."),
        Some("notice=device-saved") => Some("Device saved."),
        Some("notice=claim-unavailable") => Some("That device or pairing code is unavailable."),
        Some("notice=invalid-request") => Some("Request could not be processed."),
        Some("notice=mutation-unavailable") => Some("The requested access grant is unavailable."),
        Some("notice=service-unavailable") => Some("Access management service is unavailable."),
        _ => None,
    }
}

pub(in crate::management) fn user_resource_permission_redirect(
    scope: &UserWorkspacePermissionScope,
    notice: &'static str,
) -> Redirect {
    Redirect::to(&format!("{}?notice={notice}", scope.detail_path()))
}

pub(in crate::management) fn user_resource_permission_form_error(
    scope: &UserWorkspacePermissionScope,
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    let notice = match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => return Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge => "invalid-request",
        ManagementSessionError::NotFound | ManagementSessionError::Conflict => {
            "mutation-unavailable"
        }
        ManagementSessionError::Unavailable => "service-unavailable",
    };
    Ok(user_resource_permission_redirect(scope, notice))
}

pub(in crate::management) async fn platform_page(
    state: &ManagementState,
    session: PlatformUiSession,
    system_notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let rendered = match session {
        PlatformUiSession::System { system_account_id } => {
            let tenants = TenantIdentityRepository::list_tenant_summaries(state.store.as_ref())
                .await
                .map_err(|_| ManagementSessionError::Unavailable)?;
            let page = crate::SystemPlatformPage::new(
                tenants
                    .into_iter()
                    .map(|tenant| {
                        crate::SystemTenantRow::new(
                            tenant.slug,
                            system_tenant_status_label(tenant.status),
                        )
                    })
                    .collect(),
            )
            .with_operational_health(state.infrastructure_status.operational_health())
            .with_notice(system_notice);
            crate::PlatformUiRenderer::render_system(
                &crate::PlatformUiIdentity::new(format!("System Account {system_account_id}")),
                &page,
            )
            .map_err(|_| ManagementSessionError::Unavailable)
        }
        PlatformUiSession::Tenant { tenant_id } => {
            return tenant_overview_page(state, tenant_id).await;
        }
        PlatformUiSession::User { user_id, tenant_id } => {
            let subject = user_authorization_subject(state, user_id, tenant_id).await?;
            let devices = AuthorizationRepository::list_authorized_devices(
                state.store.as_ref(),
                &subject,
                None,
                USER_DEVICE_LIST_LIMIT,
            )
            .await
            .map_err(|_| ManagementSessionError::Unavailable)?;
            let mut page = crate::UserDeviceListPage::new(
                devices
                    .into_iter()
                    .map(|device| {
                        user_device_row(
                            device.device_id,
                            device.display_name,
                            device.last_seen_at,
                            device.access,
                        )
                    })
                    .collect(),
            )
            .with_notice(system_notice);
            if user_capability_enabled(state, tenant_id, user_id, UserCapability::CreateDevices)
                .await?
            {
                page = page.with_management(user_owned_asset_rows(state, &subject, None).await?);
            }
            if user_capability_enabled(state, tenant_id, user_id, UserCapability::ClaimDevices)
                .await?
            {
                page = page.with_claim_devices();
            }
            let identity = user_platform_identity(state, user_id, tenant_id).await?;
            crate::PlatformUiRenderer::render_user(&identity, &page)
                .map_err(|_| ManagementSessionError::Unavailable)
        }
    }?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn user_authorization_subject(
    state: &ManagementState,
    user_id: Uuid,
    tenant_id: Uuid,
) -> Result<AuthorizationSubject, ManagementSessionError> {
    let subject = AuthorizationRepository::authorization_subject(state.store.as_ref(), user_id)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?
        .ok_or(ManagementSessionError::Unauthorized)?;
    if subject.tenant_id != tenant_id {
        return Err(ManagementSessionError::Forbidden);
    }
    Ok(subject)
}

pub(in crate::management) async fn user_platform_identity(
    state: &ManagementState,
    user_id: Uuid,
    tenant_id: Uuid,
) -> Result<crate::PlatformUiIdentity, ManagementSessionError> {
    let pending = ResourceInvitationRepository::list_pending_resource_invitations(
        state.store.as_ref(),
        tenant_id,
        user_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    Ok(crate::PlatformUiIdentity::new(format!("User {user_id}"))
        .with_invitation_count(pending.len()))
}

pub(in crate::management) async fn user_owned_asset_rows(
    state: &ManagementState,
    subject: &AuthorizationSubject,
    selected_asset_id: Option<Uuid>,
) -> Result<Vec<crate::UserAssetRow>, ManagementSessionError> {
    AuthorizationRepository::list_authorized_assets(
        state.store.as_ref(),
        subject,
        None,
        USER_ASSET_LIST_LIMIT,
    )
    .await
    .map_err(|_| ManagementSessionError::Unavailable)
    .map(|assets| {
        assets
            .into_iter()
            .filter(|asset| asset.access.source == ResourceAccessSource::Owner)
            .map(|asset| {
                let selected = selected_asset_id == Some(asset.asset_id);
                let row = user_asset_row(
                    asset.asset_id,
                    asset.name,
                    asset.parent_asset_id,
                    asset.access,
                );
                if selected { row.with_selected() } else { row }
            })
            .collect()
    })
}

pub(in crate::management) fn user_device_row(
    device_id: String,
    display_name: Option<String>,
    last_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    access: ResourceAccess,
) -> crate::UserDeviceRow {
    let display_name = display_name.unwrap_or_else(|| device_id.clone());
    let activity = match last_seen_at {
        Some(timestamp) => format!(
            "Last seen {}",
            timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
        None => "No activity reported".to_owned(),
    };
    crate::UserDeviceRow::new(
        device_id,
        display_name,
        activity,
        resource_permission_label(access.permission),
        resource_access_source_label(access.source),
    )
}

pub(in crate::management) fn user_workspace_timestamp(
    timestamp: chrono::DateTime<chrono::Utc>,
) -> String {
    timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub(in crate::management) fn user_alert_label(value: String) -> String {
    match value.as_str() {
        "info" => "Info".to_owned(),
        "warning" => "Warning".to_owned(),
        "critical" => "Critical".to_owned(),
        "pending" => "Pending".to_owned(),
        "open" => "Open".to_owned(),
        "resolved" => "Resolved".to_owned(),
        _ => value,
    }
}

pub(in crate::management) fn user_asset_row(
    asset_id: Uuid,
    name: String,
    parent_asset_id: Option<Uuid>,
    access: ResourceAccess,
) -> crate::UserAssetRow {
    crate::UserAssetRow::new(
        asset_id,
        name,
        if parent_asset_id.is_some() {
            "Nested asset"
        } else {
            "Root asset"
        },
        resource_permission_label(access.permission),
        resource_access_source_label(access.source),
    )
}

pub(in crate::management) fn resource_permission_label(
    permission: ResourcePermission,
) -> &'static str {
    match permission {
        ResourcePermission::Viewer => "View",
        ResourcePermission::Controller => "Controller",
        ResourcePermission::Manager => "Control",
        ResourcePermission::Owner => "Owner",
    }
}

pub(in crate::management) fn resource_access_source_label(
    source: ResourceAccessSource,
) -> &'static str {
    match source {
        ResourceAccessSource::TenantAccount => "Tenant account",
        ResourceAccessSource::Owner => "Owner",
        ResourceAccessSource::DirectUser => "Shared by owner",
        ResourceAccessSource::Group => "Group permission",
        ResourceAccessSource::InheritedUser => "Inherited user permission",
        ResourceAccessSource::InheritedGroup => "Inherited group permission",
    }
}
