use super::super::*;

pub(in crate::management) async fn platform_system(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let PlatformUiSession::System { system_account_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    platform_page(
        &state,
        PlatformUiSession::System { system_account_id },
        system_lifecycle_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_system_infrastructure(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let PlatformUiSession::System { system_account_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    let page = state.infrastructure_status.page();
    let rendered = crate::PlatformUiRenderer::render_system_infrastructure(
        &crate::PlatformUiIdentity::new(format!("System Account {system_account_id}")),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn platform_system_infrastructure_status(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let PlatformUiSession::System { .. } = state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    let page = state.infrastructure_status.page();
    let rendered = crate::PlatformUiRenderer::render_system_infrastructure_status(&page)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) fn system_tenant_status_label(status: TenantStatus) -> &'static str {
    match status {
        TenantStatus::Active => "active",
        TenantStatus::Suspended => "suspended",
        TenantStatus::Deleted => "deleted",
    }
}

#[derive(Clone, Copy)]
pub(in crate::management) enum SystemLifecycleNotice {
    TenantCreated,
    TenantSuspended,
    TenantReactivated,
    TenantDeleted,
    TenantAccountReset,
    TenantAccountDisabled,
    InvalidTenantSlug,
    InvalidTenantAccountUsername,
    InvalidTenantAccountPassword,
    InvalidRequest,
    LifecycleUnavailable,
    ServiceUnavailable,
}

impl SystemLifecycleNotice {
    const fn redirect_path(self) -> &'static str {
        match self {
            Self::TenantCreated => "/system?notice=tenant-created",
            Self::TenantSuspended => "/system?notice=tenant-suspended",
            Self::TenantReactivated => "/system?notice=tenant-reactivated",
            Self::TenantDeleted => "/system?notice=tenant-deleted",
            Self::TenantAccountReset => "/system?notice=tenant-account-reset",
            Self::TenantAccountDisabled => "/system?notice=tenant-account-disabled",
            Self::InvalidTenantSlug => "/system?notice=invalid-tenant-slug",
            Self::InvalidTenantAccountUsername => "/system?notice=invalid-tenant-account-username",
            Self::InvalidTenantAccountPassword => "/system?notice=invalid-tenant-account-password",
            Self::InvalidRequest => "/system?notice=invalid-request",
            Self::LifecycleUnavailable => "/system?notice=lifecycle-unavailable",
            Self::ServiceUnavailable => "/system?notice=service-unavailable",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::TenantCreated => "Tenant created.",
            Self::TenantSuspended => "Tenant suspended.",
            Self::TenantReactivated => "Tenant reactivated.",
            Self::TenantDeleted => "Tenant deleted.",
            Self::TenantAccountReset => "Tenant Account password reset.",
            Self::TenantAccountDisabled => "Tenant Account disabled.",
            Self::InvalidTenantSlug => {
                "Tenant slug must use lowercase letters, digits, or hyphens."
            }
            Self::InvalidTenantAccountUsername => {
                "Tenant Account username must be 3 to 64 characters using letters, digits, hyphens, or underscores."
            }
            Self::InvalidTenantAccountPassword => {
                "Tenant Account password must be at least 8 ASCII characters and include uppercase, lowercase, a number, and a symbol."
            }
            Self::InvalidRequest => "Request could not be processed.",
            Self::LifecycleUnavailable => "Tenant lifecycle change was not allowed.",
            Self::ServiceUnavailable => "Tenant lifecycle service is unavailable.",
        }
    }
}

pub(in crate::management) fn system_lifecycle_notice(query: Option<&str>) -> Option<&'static str> {
    let notice = match query {
        Some("notice=tenant-created") => SystemLifecycleNotice::TenantCreated,
        Some("notice=tenant-suspended") => SystemLifecycleNotice::TenantSuspended,
        Some("notice=tenant-reactivated") => SystemLifecycleNotice::TenantReactivated,
        Some("notice=tenant-deleted") => SystemLifecycleNotice::TenantDeleted,
        Some("notice=tenant-account-reset") => SystemLifecycleNotice::TenantAccountReset,
        Some("notice=tenant-account-disabled") => SystemLifecycleNotice::TenantAccountDisabled,
        Some("notice=invalid-tenant-slug") => SystemLifecycleNotice::InvalidTenantSlug,
        Some("notice=invalid-tenant-account-username") => {
            SystemLifecycleNotice::InvalidTenantAccountUsername
        }
        Some("notice=invalid-tenant-account-password") => {
            SystemLifecycleNotice::InvalidTenantAccountPassword
        }
        Some("notice=invalid-request") => SystemLifecycleNotice::InvalidRequest,
        Some("notice=lifecycle-unavailable") => SystemLifecycleNotice::LifecycleUnavailable,
        Some("notice=service-unavailable") => SystemLifecycleNotice::ServiceUnavailable,
        _ => return None,
    };
    Some(notice.message())
}

pub(in crate::management) fn system_lifecycle_redirect(notice: SystemLifecycleNotice) -> Redirect {
    Redirect::to(notice.redirect_path())
}

pub(in crate::management) fn system_lifecycle_form_error(
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge => Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::InvalidRequest,
        )),
        ManagementSessionError::NotFound | ManagementSessionError::Conflict => Ok(
            system_lifecycle_redirect(SystemLifecycleNotice::LifecycleUnavailable),
        ),
        ManagementSessionError::Unavailable => Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::ServiceUnavailable,
        )),
    }
}

pub(in crate::management) async fn platform_stylesheet()
-> ([(axum::http::HeaderName, HeaderValue); 1], &'static str) {
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/css; charset=utf-8"),
        )],
        crate::platform_ui::stylesheet(),
    )
}

pub(in crate::management) async fn platform_htmx()
-> ([(axum::http::HeaderName, HeaderValue); 1], &'static str) {
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/javascript; charset=utf-8"),
        )],
        crate::platform_ui::htmx(),
    )
}

pub(in crate::management) async fn create_system_tenant(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<SystemTenantResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: CreateSystemTenantRequest = management_request_json(&state, request).await?;
    let tenant_account_username = if request.tenant_account_username.is_empty() {
        request.slug.clone()
    } else {
        request.tenant_account_username.clone()
    };
    if !request.metadata.is_object() || !is_platform_username(&tenant_account_username) {
        return Err(ManagementSessionError::BadRequest);
    }
    validate_password(&request.tenant_account_password)
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let password_hash = hash_password(&request.tenant_account_password)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let (tenant, tenant_account) = TenantIdentityRepository::create_tenant_with_named_account(
        state.store.as_ref(),
        NewTenant {
            slug: request.slug,
            metadata: request.metadata,
        },
        tenant_account_username,
        NewTenantAccount { password_hash },
    )
    .await
    .map_err(system_tenant_error)?;
    Ok((
        StatusCode::CREATED,
        Json(SystemTenantResponse {
            id: tenant.id,
            slug: tenant.slug,
            tenant_account_id: tenant_account.id,
        }),
    ))
}

pub(in crate::management) async fn suspend_system_tenant(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(tenant_slug): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_system_account(&state.session_verifier, &headers)?;
    let tenant_id = TenantIdentityRepository::suspend_tenant(state.store.as_ref(), &tenant_slug)
        .await
        .map_err(system_tenant_error)?;
    state.session_verifier.invalidate_tenant(tenant_id);
    Ok(StatusCode::NO_CONTENT)
}

pub(in crate::management) async fn reactivate_system_tenant(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(tenant_slug): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_system_account(&state.session_verifier, &headers)?;
    TenantIdentityRepository::reactivate_tenant(state.store.as_ref(), &tenant_slug)
        .await
        .map_err(system_tenant_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(in crate::management) async fn delete_system_tenant(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(tenant_slug): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_system_account(&state.session_verifier, &headers)?;
    let tenant_id = TenantIdentityRepository::delete_tenant(state.store.as_ref(), &tenant_slug)
        .await
        .map_err(system_tenant_error)?;
    state.session_verifier.invalidate_tenant(tenant_id);
    Ok(StatusCode::NO_CONTENT)
}

pub(in crate::management) async fn reset_system_tenant_account(
    State(state): State<ManagementState>,
    Path(tenant_slug): Path<String>,
    request: Request,
) -> Result<StatusCode, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: ResetTenantAccountRequest = management_request_json(&state, request).await?;
    validate_password(&request.password).map_err(|_| ManagementSessionError::BadRequest)?;
    let password_hash =
        hash_password(&request.password).map_err(|_| ManagementSessionError::Unavailable)?;
    let tenant_account = TenantIdentityRepository::reset_tenant_account_password(
        state.store.as_ref(),
        &tenant_slug,
        password_hash,
    )
    .await
    .map_err(system_tenant_error)?;
    state
        .session_verifier
        .invalidate_tenant(tenant_account.tenant_id);
    Ok(StatusCode::NO_CONTENT)
}

pub(in crate::management) async fn disable_system_tenant_account(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(tenant_slug): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_system_account(&state.session_verifier, &headers)?;
    let tenant_account =
        TenantIdentityRepository::disable_tenant_account(state.store.as_ref(), &tenant_slug)
            .await
            .map_err(system_tenant_error)?;
    state
        .session_verifier
        .invalidate_tenant(tenant_account.tenant_id);
    Ok(StatusCode::NO_CONTENT)
}

pub(in crate::management) async fn create_system_tenant_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: CreateSystemTenantForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return system_lifecycle_form_error(error),
    };
    let tenant_account_username = if request.tenant_account_username.is_empty() {
        request.slug.clone()
    } else {
        request.tenant_account_username.clone()
    };
    if !is_tenant_slug(&request.slug) {
        return Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::InvalidTenantSlug,
        ));
    }
    if !is_platform_username(&tenant_account_username) {
        return Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::InvalidTenantAccountUsername,
        ));
    }
    if validate_password(&request.tenant_account_password).is_err() {
        return Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::InvalidTenantAccountPassword,
        ));
    }
    let password_hash = match hash_password(&request.tenant_account_password) {
        Ok(password_hash) => password_hash,
        Err(_) => return system_lifecycle_form_error(ManagementSessionError::Unavailable),
    };
    match TenantIdentityRepository::create_tenant_with_named_account(
        state.store.as_ref(),
        NewTenant {
            slug: request.slug,
            metadata: json!({}),
        },
        tenant_account_username,
        NewTenantAccount { password_hash },
    )
    .await
    {
        Ok(_) => Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::TenantCreated,
        )),
        Err(error) => system_lifecycle_form_error(system_tenant_error(error)),
    }
}

pub(in crate::management) async fn suspend_system_tenant_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: SystemTenantLifecycleForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return system_lifecycle_form_error(error),
    };
    match TenantIdentityRepository::suspend_tenant(state.store.as_ref(), &request.slug).await {
        Ok(tenant_id) => {
            state.session_verifier.invalidate_tenant(tenant_id);
            Ok(system_lifecycle_redirect(
                SystemLifecycleNotice::TenantSuspended,
            ))
        }
        Err(error) => system_lifecycle_form_error(system_tenant_error(error)),
    }
}

pub(in crate::management) async fn reactivate_system_tenant_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: SystemTenantLifecycleForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return system_lifecycle_form_error(error),
    };
    match TenantIdentityRepository::reactivate_tenant(state.store.as_ref(), &request.slug).await {
        Ok(_) => Ok(system_lifecycle_redirect(
            SystemLifecycleNotice::TenantReactivated,
        )),
        Err(error) => system_lifecycle_form_error(system_tenant_error(error)),
    }
}

pub(in crate::management) async fn delete_system_tenant_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: SystemTenantLifecycleForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return system_lifecycle_form_error(error),
    };
    match TenantIdentityRepository::delete_tenant(state.store.as_ref(), &request.slug).await {
        Ok(tenant_id) => {
            state.session_verifier.invalidate_tenant(tenant_id);
            Ok(system_lifecycle_redirect(
                SystemLifecycleNotice::TenantDeleted,
            ))
        }
        Err(error) => system_lifecycle_form_error(system_tenant_error(error)),
    }
}

pub(in crate::management) async fn reset_system_tenant_account_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: ResetTenantAccountForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return system_lifecycle_form_error(error),
    };
    if validate_password(&request.password).is_err() {
        return system_lifecycle_form_error(ManagementSessionError::BadRequest);
    }
    let password_hash = match hash_password(&request.password) {
        Ok(password_hash) => password_hash,
        Err(_) => return system_lifecycle_form_error(ManagementSessionError::Unavailable),
    };
    match TenantIdentityRepository::reset_tenant_account_password(
        state.store.as_ref(),
        &request.slug,
        password_hash,
    )
    .await
    {
        Ok(tenant_account) => {
            state
                .session_verifier
                .invalidate_tenant(tenant_account.tenant_id);
            Ok(system_lifecycle_redirect(
                SystemLifecycleNotice::TenantAccountReset,
            ))
        }
        Err(error) => system_lifecycle_form_error(system_tenant_error(error)),
    }
}

pub(in crate::management) async fn disable_system_tenant_account_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    require_system_account(&state.session_verifier, &headers)?;
    let request: SystemTenantLifecycleForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return system_lifecycle_form_error(error),
    };
    match TenantIdentityRepository::disable_tenant_account(state.store.as_ref(), &request.slug)
        .await
    {
        Ok(tenant_account) => {
            state
                .session_verifier
                .invalidate_tenant(tenant_account.tenant_id);
            Ok(system_lifecycle_redirect(
                SystemLifecycleNotice::TenantAccountDisabled,
            ))
        }
        Err(error) => system_lifecycle_form_error(system_tenant_error(error)),
    }
}
