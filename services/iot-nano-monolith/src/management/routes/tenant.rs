use super::super::*;

pub(in crate::management) async fn platform_tenant(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let PlatformUiSession::Tenant { tenant_id } =
        state.session_verifier.platform_session(&headers)?
    else {
        return Err(ManagementSessionError::Forbidden);
    };
    tenant_overview_page(&state, tenant_id).await
}

pub(in crate::management) async fn platform_tenant_users(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_users_page(
        &state,
        tenant,
        tenant_management_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_assets(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_assets_page(
        &state,
        tenant,
        tenant_management_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_devices(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_devices_page(&state, tenant, tenant_devices_notice(request.uri().query())).await
}

pub(in crate::management) async fn platform_tenant_ota(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_ota_page(&state, tenant).await
}

async fn tenant_ota_page(
    state: &ManagementState,
    tenant: TenantSession,
) -> Result<Html<String>, ManagementSessionError> {
    let policy = state
        .store
        .ota_policy(tenant.tenant_id)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let artifacts = state
        .store
        .list_ota_artifacts(tenant.tenant_id)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let profiles = ManagementDeviceProfileRepository::list_management_device_profiles(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(management_device_profile_error)?;
    let device_names =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_error)?
            .into_iter()
            .map(|device| {
                (
                    device.device_id.clone(),
                    device.display_name.unwrap_or(device.device_id),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
    let deployments = state
        .store
        .list_ota_deployments(tenant.tenant_id, 100)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let profile_names = profiles
        .iter()
        .map(|profile| (profile.id, profile.name.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    let page = crate::TenantOtaPage::new(
        profiles
            .into_iter()
            .map(|profile| crate::TenantOtaProfileRow::new(profile.id.to_string(), profile.name))
            .collect(),
        artifacts
            .into_iter()
            .map(|artifact| {
                crate::TenantOtaArtifactRow::new(
                    artifact.id.to_string(),
                    profile_names
                        .get(&artifact.device_profile_id)
                        .map(String::as_str)
                        .unwrap_or("Deleted profile"),
                    artifact.version,
                    artifact.filename,
                    artifact.sha256,
                    artifact.size_bytes,
                )
            })
            .collect(),
        deployments
            .into_iter()
            .map(|deployment| {
                crate::TenantOtaDeploymentRow::new(
                    device_names
                        .get(&deployment.device_id)
                        .map(String::as_str)
                        .unwrap_or(&deployment.device_id),
                    deployment
                        .from_version
                        .unwrap_or_else(|| "Unknown".to_owned()),
                    deployment.target_version,
                    deployment.status.as_str(),
                    deployment.started_at,
                    deployment
                        .completed_at
                        .unwrap_or_else(|| "In progress".to_owned()),
                    deployment.error_message.unwrap_or_default(),
                )
            })
            .collect(),
        policy.require_matching_device_profile,
        policy.require_newer_version,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_ota(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn platform_tenant_device_claim_policy(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let policy =
        DeviceClaimRepository::get_device_claim_policy(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_claim_error)?;
    let page = crate::TenantDeviceClaimPolicyPage::new(
        policy.enabled,
        policy.ttl_seconds,
        policy.code_length,
        policy.max_failed_attempts,
        policy.request_cooldown_seconds,
        tenant_device_claim_policy_notice(request.uri().query()),
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_device_claim_policy(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn platform_tenant_alerts(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_alerts_page(&state, tenant).await
}

pub(in crate::management) async fn platform_tenant_audit(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Query(query): Query<TenantAuditQuery>,
) -> Result<Html<String>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_audit_page(&state, tenant, &query).await
}

pub(in crate::management) async fn platform_tenant_device_profiles(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_device_profiles_page(
        &state,
        tenant,
        tenant_profile_notice(TenantProfileKind::Device, request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_asset_profiles(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_asset_profiles_page(
        &state,
        tenant,
        tenant_profile_notice(TenantProfileKind::Asset, request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_profile(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_profile_page(&state, tenant).await
}

pub(in crate::management) async fn platform_tenant_topology(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_topology_page(
        &state,
        tenant,
        tenant_topology_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_relations(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_relations_page(
        &state,
        tenant,
        tenant_relation_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_applications(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_applications_page(
        &state,
        tenant,
        tenant_application_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_personal_access_tokens(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Html<String>, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let token =
        iot_storage::TenantPersonalAccessTokenRepository::active_tenant_personal_access_token(
            state.store.as_ref(),
            tenant.tenant_id,
            tenant.tenant_account_id,
        )
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let page = crate::platform_ui::TenantPersonalAccessTokenPage::new(token);
    let rendered = crate::PlatformUiRenderer::render_tenant_personal_access_tokens(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_assets_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let assets =
        ManagementAssetRepository::list_management_assets(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_asset_error)?;
    let asset_names: HashMap<Uuid, String> = assets
        .iter()
        .map(|asset| (asset.id, asset.name.clone()))
        .collect();
    let parent_assets = assets
        .iter()
        .map(|asset| crate::TenantSelectOption::new(asset.id.to_string(), asset.name.clone()))
        .collect();
    let page = crate::TenantAssetsPage::new(
        assets
            .into_iter()
            .map(|asset| {
                let parent = asset.parent_asset_id.map_or_else(
                    || "Root asset".to_owned(),
                    |parent_id| {
                        let parent_name = asset_names
                            .get(&parent_id)
                            .cloned()
                            .unwrap_or_else(|| parent_id.to_string());
                        format!("{parent_name} ({parent_id})")
                    },
                );
                crate::TenantAssetRow::new(asset.id.to_string(), asset.name, parent, "Configured")
            })
            .collect(),
        parent_assets,
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_assets(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_overview_page(
    state: &ManagementState,
    tenant_id: Uuid,
) -> Result<Html<String>, ManagementSessionError> {
    let users = ManagementUserRepository::list_management_users(state.store.as_ref(), tenant_id)
        .await
        .map_err(management_user_error)?;
    let assets = ManagementAssetRepository::list_management_assets(state.store.as_ref(), tenant_id)
        .await
        .map_err(management_asset_error)?;
    let devices =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant_id)
            .await
            .map_err(management_device_error)?;
    let open_alert_count = ManagementAlertIncidentRepository::open_management_alert_incident_count(
        state.store.as_ref(),
        tenant_id,
    )
    .await
    .map_err(management_alert_incident_error)?;
    let page =
        crate::TenantOverviewPage::new(users.len(), assets.len(), devices.len(), open_alert_count);
    let rendered = crate::PlatformUiRenderer::render_tenant(
        &crate::PlatformUiIdentity::new(format!("Tenant {tenant_id}")),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_devices_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let assets =
        ManagementAssetRepository::list_management_assets(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_asset_error)?;
    let devices =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_error)?;
    let users =
        ManagementUserRepository::list_management_users(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_user_error)?;
    let (serial_number_length, auto_generate_serial_number) =
        tenant_serial_generation_settings(state.store.as_ref(), tenant.tenant_id).await?;
    let asset_names: HashMap<Uuid, String> = assets
        .into_iter()
        .map(|asset| (asset.id, asset.name))
        .collect();
    let user_names: HashMap<Uuid, String> = users
        .into_iter()
        .map(|user| (user.id, user.username))
        .collect();
    let mut rows = Vec::with_capacity(devices.len());
    for device in devices {
        let device_id = device.device_id;
        let serial_number = device
            .serial_number
            .unwrap_or_else(|| "No serial number".to_owned());
        let display_name = device.display_name.unwrap_or_else(|| device_id.clone());
        let asset = device.asset_id.map_or_else(
            || "Unassigned".to_owned(),
            |asset_id| {
                let asset_name = asset_names
                    .get(&asset_id)
                    .cloned()
                    .unwrap_or_else(|| asset_id.to_string());
                format!("{asset_name} ({asset_id})")
            },
        );
        let assigned_user = device.owner_user_id.map_or_else(
            || "Unassigned".to_owned(),
            |user_id| {
                user_names
                    .get(&user_id)
                    .cloned()
                    .unwrap_or_else(|| user_id.to_string())
            },
        );
        let claim_status = DeviceClaimRepository::active_device_claim_code(
            state.store.as_ref(),
            tenant.tenant_id,
            &device_id,
        )
        .await
        .map_err(management_device_claim_error)?;
        let (claim_status, has_active_claim_code) = claim_status.map_or_else(
            || ("No active pairing code".to_owned(), false),
            |status| {
                (
                    format!(
                        "Active until {}",
                        status
                            .expires_at
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                    ),
                    true,
                )
            },
        );
        rows.push(
            crate::TenantDeviceRow::new(
                device_id,
                serial_number,
                display_name,
                if device.health.online {
                    "Online"
                } else {
                    "Offline"
                },
                asset,
                assigned_user,
            )
            .with_claim_status(claim_status, has_active_claim_code),
        );
    }
    let page = crate::TenantDevicesPage::new(rows, notice)
        .with_serial_number_length(serial_number_length)
        .with_auto_generate_serial_number(auto_generate_serial_number);
    let rendered = crate::PlatformUiRenderer::render_tenant_devices(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_alerts_page(
    state: &ManagementState,
    tenant: TenantSession,
) -> Result<Html<String>, ManagementSessionError> {
    let alerts =
        ManagementAlertRepository::list_management_alerts(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_alert_error)?;
    let page = crate::TenantAlertsPage::new(
        alerts
            .into_iter()
            .map(|alert| {
                crate::TenantAlertRow::new(
                    alert.device_id,
                    alert.rule_name,
                    alert.severity,
                    alert.status,
                    alert
                        .last_value
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "Not reported".to_owned()),
                    alert.updated_at.to_rfc3339(),
                )
            })
            .collect(),
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_alerts(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_audit_page(
    state: &ManagementState,
    tenant: TenantSession,
    query: &TenantAuditQuery,
) -> Result<Html<String>, ManagementSessionError> {
    let audit_events = tenant_audit_event_page(state, tenant.tenant_id, query).await?;
    let older_events_href = audit_events
        .next_cursor
        .as_ref()
        .map(|cursor| format!("/tenant/audit?after={cursor}&limit={}", audit_events.limit));
    let page = crate::TenantAuditPage::new(
        audit_events
            .events
            .into_iter()
            .map(tenant_audit_row)
            .collect::<Result<Vec<_>, _>>()?,
        older_events_href,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_audit(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_audit_event_page(
    state: &ManagementState,
    tenant_id: Uuid,
    query: &TenantAuditQuery,
) -> Result<TenantAuditEventPage, ManagementSessionError> {
    let limit = tenant_audit_limit(query.limit)?;
    let after = decode_tenant_audit_cursor(&state.token_vault, tenant_id, query.after.as_deref())?;
    let fetch_limit = if limit == MAX_TENANT_AUDIT_LIMIT {
        limit
    } else {
        limit + 1
    };
    let mut events = AuditEventRepository::list_tenant_audit_events(
        state.store.as_ref(),
        tenant_id,
        after,
        fetch_limit,
    )
    .await
    .map_err(management_audit_error)?;
    let mut has_more = events.len() > limit;
    events.truncate(limit);

    if !has_more && limit == MAX_TENANT_AUDIT_LIMIT && events.len() == limit {
        if let Some(last_event) = events.last() {
            let remaining = AuditEventRepository::list_tenant_audit_events(
                state.store.as_ref(),
                tenant_id,
                Some(audit_event_cursor(last_event)),
                1,
            )
            .await
            .map_err(management_audit_error)?;
            has_more = !remaining.is_empty();
        }
    }

    let next_cursor = if has_more {
        events
            .last()
            .map(|event| encode_tenant_audit_cursor(&state.token_vault, tenant_id, event))
            .transpose()?
    } else {
        None
    };
    Ok(TenantAuditEventPage {
        events,
        next_cursor,
        has_more,
        limit,
    })
}

pub(in crate::management) async fn tenant_device_profiles_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let profiles = ManagementDeviceProfileRepository::list_management_device_profiles(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(management_device_profile_error)?;
    let page = crate::TenantDeviceProfilesPage::new(
        profiles
            .into_iter()
            .map(|profile| crate::TenantProfileRow::new(profile.id, profile.name))
            .collect(),
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_device_profiles(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_asset_profiles_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let profiles = ManagementAssetProfileRepository::list_management_asset_profiles(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(management_asset_profile_error)?;
    let page = crate::TenantAssetProfilesPage::new(
        profiles
            .into_iter()
            .map(|profile| crate::TenantProfileRow::new(profile.id, profile.name))
            .collect(),
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_asset_profiles(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_profile_page(
    state: &ManagementState,
    tenant: TenantSession,
) -> Result<Html<String>, ManagementSessionError> {
    let configuration = TenantProfileRepository::export_tenant_profile_configuration(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(application_domain_profile_error)?;
    let configuration_json = serde_json::to_string_pretty(&configuration)
        .map_err(|_| ManagementSessionError::Unavailable)?;
    let page = crate::TenantProfilePage::new(configuration_json);
    let rendered = crate::PlatformUiRenderer::render_tenant_profile(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_topology_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let devices =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_error)?;
    let gateway_names: HashMap<String, String> = devices
        .iter()
        .filter(|device| device.topology.is_gateway)
        .map(|device| {
            (
                device.device_id.clone(),
                device
                    .display_name
                    .clone()
                    .unwrap_or_else(|| device.device_id.clone()),
            )
        })
        .collect();
    let gateways = devices
        .iter()
        .filter(|device| device.topology.is_gateway)
        .map(|device| {
            crate::TenantSelectOption::new(
                device.device_id.clone(),
                format!(
                    "{} ({})",
                    device
                        .display_name
                        .as_deref()
                        .unwrap_or(device.device_id.as_str()),
                    device.device_id
                ),
            )
        })
        .collect();
    let children = devices
        .iter()
        .filter(|device| !device.topology.is_gateway)
        .map(|device| {
            crate::TenantSelectOption::new(
                device.device_id.clone(),
                format!(
                    "{} ({})",
                    device
                        .display_name
                        .as_deref()
                        .unwrap_or(device.device_id.as_str()),
                    device.device_id
                ),
            )
        })
        .collect();
    let assigned_children = devices
        .iter()
        .filter(|device| !device.topology.is_gateway && device.topology.gateway_device_id.is_some())
        .map(|device| {
            crate::TenantSelectOption::new(
                device.device_id.clone(),
                format!(
                    "{} ({})",
                    device
                        .display_name
                        .as_deref()
                        .unwrap_or(device.device_id.as_str()),
                    device.device_id
                ),
            )
        })
        .collect();
    let page = crate::TenantTopologyPage::new(
        devices
            .into_iter()
            .map(|device| {
                let device_id = device.device_id;
                let gateway = device
                    .topology
                    .gateway_device_id
                    .as_deref()
                    .map(|gateway_id| {
                        let gateway_name = gateway_names
                            .get(gateway_id)
                            .cloned()
                            .unwrap_or_else(|| gateway_id.to_owned());
                        format!("{gateway_name} ({gateway_id})")
                    })
                    .unwrap_or_else(|| "Direct".to_owned());
                crate::TenantTopologyRow::new(
                    device.display_name.unwrap_or_else(|| device_id.clone()),
                    device_id,
                    if device.topology.is_gateway {
                        "Gateway"
                    } else {
                        "Device"
                    },
                    gateway,
                )
            })
            .collect(),
        gateways,
        children,
        assigned_children,
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_topology(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_relations_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let devices =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_error)?;
    let relations =
        DeviceRelationRepository::list_device_relations(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(device_relation_error)?;
    let asset_relations = DeviceRelationRepository::list_device_asset_relations(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(device_relation_error)?;
    let assets =
        ManagementAssetRepository::list_management_assets(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_asset_error)?;
    let device_names: HashMap<String, String> = devices
        .iter()
        .map(|device| {
            (
                device.device_id.clone(),
                device
                    .display_name
                    .clone()
                    .unwrap_or_else(|| device.device_id.clone()),
            )
        })
        .collect();
    let asset_names: HashMap<Uuid, String> = assets
        .iter()
        .map(|asset| (asset.id, asset.name.clone()))
        .collect();
    let options = devices
        .into_iter()
        .map(|device| {
            let label = device
                .display_name
                .unwrap_or_else(|| device.device_id.clone());
            crate::TenantSelectOption::new(device.device_id, label)
        })
        .collect();
    let asset_options = assets
        .into_iter()
        .map(|asset| crate::TenantSelectOption::new(asset.id.to_string(), asset.name))
        .collect();
    let relation_rows = relations
        .into_iter()
        .map(|relation| {
            let from_device = device_names
                .get(&relation.from_device_id)
                .cloned()
                .unwrap_or(relation.from_device_id);
            let target = device_names
                .get(&relation.to_device_id)
                .cloned()
                .unwrap_or(relation.to_device_id);
            crate::TenantRelationRow::new(
                relation.id.to_string(),
                from_device,
                "device",
                relation.relation_type,
                target,
            )
        })
        .chain(asset_relations.into_iter().map(|relation| {
            let from_device = device_names
                .get(&relation.from_device_id)
                .cloned()
                .unwrap_or(relation.from_device_id);
            let target = asset_names
                .get(&relation.to_asset_id)
                .cloned()
                .unwrap_or_else(|| relation.to_asset_id.to_string());
            crate::TenantRelationRow::new(
                relation.id.to_string(),
                from_device,
                "asset",
                relation.relation_type,
                target,
            )
        }))
        .collect();
    let page = crate::TenantRelationsPage::new(options, asset_options, relation_rows, notice);
    let rendered = crate::PlatformUiRenderer::render_tenant_relations(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_applications_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let applications =
        ApplicationRepository::list_applications_for_tenant(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(tenant_application_error)?;
    let page = crate::TenantApplicationsPage::new(
        applications
            .into_iter()
            .map(|application| {
                crate::TenantApplicationRow::new(
                    application.app_id.as_str(),
                    application.client_id.as_str(),
                    application.kind.as_str(),
                    application.launch_url,
                    application
                        .redirect_uris
                        .iter()
                        .map(|uri| uri.as_str())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    application.allowed_scopes.join(" "),
                    if application.enabled {
                        "Enabled"
                    } else {
                        "Disabled"
                    },
                )
            })
            .collect(),
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_applications(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn create_tenant_asset_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateTenantAssetForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return tenant_management_form_error(TenantManagementPage::Assets, error),
    };
    let name = request.name.trim();
    if name.is_empty() || name.len() > 128 {
        return tenant_management_form_error(
            TenantManagementPage::Assets,
            ManagementSessionError::BadRequest,
        );
    }
    let parent_asset_id = match request
        .parent_asset_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => match Uuid::parse_str(value) {
            Ok(id) => Some(id),
            Err(_) => {
                return tenant_management_form_error(
                    TenantManagementPage::Assets,
                    ManagementSessionError::BadRequest,
                );
            }
        },
        None => None,
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match ManagementAssetRepository::create_management_asset(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        CreateManagementAsset {
            name: name.to_owned(),
            asset_profile_id: None,
            parent_asset_id,
            metadata: json!({}),
            attributes: None,
        },
    )
    .await
    {
        Ok(_) => Ok(tenant_management_redirect(
            TenantManagementPage::Assets,
            TenantManagementNotice::AssetCreated,
        )),
        Err(error) => tenant_management_form_error(
            TenantManagementPage::Assets,
            management_asset_error(error),
        ),
    }
}

pub(in crate::management) async fn provision_tenant_device_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Response, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: ProvisionTenantDeviceForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => {
            return tenant_management_form_error(TenantManagementPage::Devices, error)
                .map(|redirect| redirect.into_response());
        }
    };
    let serial_number = request.serial_number.trim();
    let display_name = request.display_name.trim();
    if serial_number.len() > 128 || display_name.is_empty() || display_name.len() > 128 {
        return tenant_management_form_error(
            TenantManagementPage::Devices,
            ManagementSessionError::BadRequest,
        )
        .map(|redirect| redirect.into_response());
    }
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let token = match provision_management_device_token(
        &state.store,
        &state.token_vault,
        tenant.tenant_id,
        serial_number,
        display_name,
        None,
        None,
        json!({}),
    )
    .await
    {
        Ok(token) => token,
        Err(error) => {
            return tenant_management_form_error(
                TenantManagementPage::Devices,
                management_device_token_error(error),
            )
            .map(|redirect| redirect.into_response());
        }
    };
    let credential = token.token.ok_or(ManagementSessionError::Unavailable)?;
    let page = crate::TenantDeviceCredentialPage::new(token.device_id, display_name, credential);
    let rendered = crate::PlatformUiRenderer::render_tenant_device_credential(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok((
        StatusCode::CREATED,
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Html(rendered),
    )
        .into_response())
}

pub(in crate::management) fn tenant_profile_object(
    value: &str,
) -> Result<Value, ManagementSessionError> {
    let value: Value =
        serde_json::from_str(value).map_err(|_| ManagementSessionError::BadRequest)?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(ManagementSessionError::BadRequest)
    }
}

pub(in crate::management) async fn update_tenant_device_claim_policy_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let form: TenantDeviceClaimPolicyForm = match management_request_form(&state, request).await {
        Ok(form) => form,
        Err(error) => return tenant_device_claim_policy_form_error(error),
    };
    let policy = DeviceClaimPolicy {
        enabled: form.enabled.is_some(),
        ttl_seconds: form.ttl_seconds,
        code_length: form.code_length,
        max_failed_attempts: form.max_failed_attempts,
        request_cooldown_seconds: form.request_cooldown_seconds,
    };
    match DeviceClaimRepository::update_device_claim_policy(
        state.store.as_ref(),
        tenant.tenant_id,
        policy,
    )
    .await
    {
        Ok(_) => Ok(Redirect::to(
            "/tenant/devices/claim-policy?notice=claim-policy-saved",
        )),
        Err(error) => tenant_device_claim_policy_form_error(management_device_claim_error(error)),
    }
}

pub(in crate::management) async fn update_tenant_serial_generation_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let form: TenantSerialGenerationForm = management_request_form(&state, request).await?;
    let enabled = form.enabled.is_some();
    let updated = match state.store.as_ref() {
        PlatformStore::Sqlite(store) => {
            sqlx::query("UPDATE tenants SET auto_generate_serial_number = ? WHERE id = ?")
                .bind(i64::from(enabled))
                .bind(tenant.tenant_id.to_string())
                .execute(store.pool())
                .await
                .map(|_| ())
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query("UPDATE tenants SET auto_generate_serial_number = $1 WHERE id = $2")
                .bind(enabled)
                .bind(tenant.tenant_id)
                .execute(pool)
                .await
                .map(|_| ())
        }
    };
    updated.map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Redirect::to("/tenant/devices"))
}

pub(in crate::management) async fn revoke_tenant_device_claim_code_form(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    headers: HeaderMap,
) -> Result<Redirect, ManagementSessionError> {
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    DeviceClaimRepository::revoke_device_claim_code(
        state.store.as_ref(),
        tenant.tenant_id,
        &device_id,
    )
    .await
    .map_err(management_device_claim_error)?;
    Ok(Redirect::to("/tenant/devices?notice=claim-code-revoked"))
}

pub(in crate::management) async fn create_tenant_device_profile_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateTenantDeviceProfileForm =
        match management_request_form(&state, request).await {
            Ok(request) => request,
            Err(error) => return tenant_profile_form_error(TenantProfileKind::Device, error),
        };
    let telemetry_schema = match tenant_profile_object(&request.telemetry_schema) {
        Ok(value) => value,
        Err(error) => return tenant_profile_form_error(TenantProfileKind::Device, error),
    };
    let metric_mapping = match tenant_profile_object(&request.metric_mapping) {
        Ok(value) => value,
        Err(error) => return tenant_profile_form_error(TenantProfileKind::Device, error),
    };
    let reporting_settings = match tenant_profile_object(&request.reporting_settings) {
        Ok(value) => value,
        Err(error) => return tenant_profile_form_error(TenantProfileKind::Device, error),
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match ManagementDeviceProfileRepository::create_management_device_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        CreateManagementDeviceProfile {
            name: request.name,
            telemetry_schema,
            metric_mapping,
            reporting_settings,
        },
    )
    .await
    {
        Ok(_) => Ok(tenant_profile_redirect(
            TenantProfileKind::Device,
            TenantProfileKind::Device.created_notice(),
        )),
        Err(error) => tenant_profile_form_error(
            TenantProfileKind::Device,
            management_device_profile_error(error),
        ),
    }
}

pub(in crate::management) async fn create_tenant_asset_profile_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateTenantAssetProfileForm = match management_request_form(&state, request).await
    {
        Ok(request) => request,
        Err(error) => return tenant_profile_form_error(TenantProfileKind::Asset, error),
    };
    let fields = match tenant_profile_object(&request.fields) {
        Ok(value) => value,
        Err(error) => return tenant_profile_form_error(TenantProfileKind::Asset, error),
    };
    let dashboard_defaults = match tenant_profile_object(&request.dashboard_defaults) {
        Ok(value) => value,
        Err(error) => return tenant_profile_form_error(TenantProfileKind::Asset, error),
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match ManagementAssetProfileRepository::create_management_asset_profile(
        state.store.as_ref(),
        tenant.tenant_id,
        CreateManagementAssetProfile {
            name: request.name,
            fields,
            dashboard_defaults,
        },
    )
    .await
    {
        Ok(_) => Ok(tenant_profile_redirect(
            TenantProfileKind::Asset,
            TenantProfileKind::Asset.created_notice(),
        )),
        Err(error) => tenant_profile_form_error(
            TenantProfileKind::Asset,
            management_asset_profile_error(error),
        ),
    }
}

pub(in crate::management) async fn assign_tenant_gateway_child_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: AssignTenantGatewayChildForm = match management_request_form(&state, request).await
    {
        Ok(request) => request,
        Err(error) => return tenant_topology_form_error(error),
    };
    let child_device_id = match tenant_topology_device_id(&request.child_device_id) {
        Ok(device_id) => device_id,
        Err(error) => return tenant_topology_form_error(error),
    };
    let gateway_device_id = match tenant_topology_device_id(&request.gateway_device_id) {
        Ok(device_id) => device_id,
        Err(error) => return tenant_topology_form_error(error),
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match update_tenant_gateway_child(
        &state,
        tenant,
        child_device_id,
        Some(gateway_device_id.to_owned()),
    )
    .await
    {
        Ok(_) => Ok(Redirect::to("/tenant/topology?notice=gateway-assigned")),
        Err(error) => tenant_topology_form_error(management_device_error(error)),
    }
}

pub(in crate::management) async fn detach_tenant_gateway_child_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: DetachTenantGatewayChildForm = match management_request_form(&state, request).await
    {
        Ok(request) => request,
        Err(error) => return tenant_topology_form_error(error),
    };
    let child_device_id = match tenant_topology_device_id(&request.child_device_id) {
        Ok(device_id) => device_id,
        Err(error) => return tenant_topology_form_error(error),
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match update_tenant_gateway_child(&state, tenant, child_device_id, None).await {
        Ok(_) => Ok(Redirect::to("/tenant/topology?notice=gateway-detached")),
        Err(error) => tenant_topology_form_error(management_device_error(error)),
    }
}

pub(in crate::management) async fn create_tenant_relation_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateTenantDeviceRelationForm =
        match management_request_form(&state, request).await {
            Ok(request) => request,
            Err(error) => return tenant_relation_form_error(error),
        };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let actor = AuditPrincipal::TenantAccount(tenant.tenant_account_id);
    let result = match (
        request.target_kind.as_str(),
        request.to_device_id,
        request.to_asset_id,
    ) {
        ("device", Some(to_device_id), None) => DeviceRelationRepository::create_device_relation(
            state.store.as_ref(),
            tenant.tenant_id,
            actor,
            CreateDeviceRelation {
                from_device_id: request.from_device_id,
                to_device_id,
                relation_type: request.relation_type,
            },
        )
        .await
        .map(|_| ()),
        ("asset", None, Some(to_asset_id)) => {
            DeviceRelationRepository::create_device_asset_relation(
                state.store.as_ref(),
                tenant.tenant_id,
                actor,
                CreateDeviceAssetRelation {
                    from_device_id: request.from_device_id,
                    to_asset_id,
                    relation_type: request.relation_type,
                },
            )
            .await
            .map(|_| ())
        }
        _ => return tenant_relation_form_error(ManagementSessionError::BadRequest),
    };
    match result {
        Ok(_) => Ok(Redirect::to("/tenant/relations?notice=relation-created")),
        Err(error) => tenant_relation_form_error(device_relation_error(error)),
    }
}

pub(in crate::management) async fn delete_tenant_relation_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: DeleteTenantDeviceRelationForm =
        match management_request_form(&state, request).await {
            Ok(request) => request,
            Err(error) => return tenant_relation_form_error(error),
        };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let actor = AuditPrincipal::TenantAccount(tenant.tenant_account_id);
    let result = match request.target_kind.as_str() {
        "device" => {
            DeviceRelationRepository::delete_device_relation(
                state.store.as_ref(),
                tenant.tenant_id,
                actor,
                request.relation_id,
            )
            .await
        }
        "asset" => {
            DeviceRelationRepository::delete_device_asset_relation(
                state.store.as_ref(),
                tenant.tenant_id,
                actor,
                request.relation_id,
            )
            .await
        }
        _ => return tenant_relation_form_error(ManagementSessionError::BadRequest),
    };
    match result {
        Ok(_) => Ok(Redirect::to("/tenant/relations?notice=relation-deleted")),
        Err(error) => tenant_relation_form_error(device_relation_error(error)),
    }
}

pub(in crate::management) async fn save_tenant_application_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: TenantApplicationForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return tenant_application_form_error(error),
    };
    let application = match tenant_application_from_form(request, tenant.tenant_id) {
        Ok(application) => application,
        Err(error) => return tenant_application_form_error(error),
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match ApplicationRepository::upsert_application(state.store.as_ref(), application).await {
        Ok(_) => Ok(Redirect::to(
            "/tenant/applications?notice=application-saved",
        )),
        Err(error) => tenant_application_form_error(tenant_application_error(error)),
    }
}

pub(in crate::management) fn tenant_application_from_form(
    request: TenantApplicationForm,
    tenant_id: Uuid,
) -> Result<NewApplication, ManagementSessionError> {
    let app_id = request
        .app_id
        .trim()
        .parse()
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let kind = ApplicationKind::from_str(request.kind.trim())
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let client_id = request
        .client_id
        .trim()
        .parse()
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let launch_url = request.launch_url.trim();
    if launch_url.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    let redirect_uris = request
        .redirect_uris
        .lines()
        .map(str::trim)
        .filter(|uri| !uri.is_empty())
        .map(|uri| RedirectUri::from_str(uri).map_err(|_| ManagementSessionError::BadRequest))
        .collect::<Result<Vec<_>, _>>()?;
    if redirect_uris.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    let allowed_scopes: Vec<_> = request
        .allowed_scopes
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    if allowed_scopes.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    Ok(NewApplication {
        app_id,
        tenant_id,
        kind,
        launch_url: launch_url.to_owned(),
        client_id,
        redirect_uris,
        allowed_scopes,
        enabled: request.enabled.is_some(),
    })
}

pub(in crate::management) fn tenant_devices_notice(query: Option<&str>) -> Option<&'static str> {
    match query {
        Some("notice=claim-code-revoked") => Some("Pairing code revoked."),
        _ => tenant_management_notice(query),
    }
}

pub(in crate::management) fn tenant_device_claim_policy_notice(
    query: Option<&str>,
) -> Option<&'static str> {
    match query {
        Some("notice=claim-policy-saved") => Some("Pairing policy saved."),
        Some("notice=invalid-request") => Some("Pairing policy values are invalid."),
        Some("notice=service-unavailable") => Some("Pairing policy service is unavailable."),
        _ => None,
    }
}

pub(in crate::management) fn tenant_device_claim_policy_form_error(
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    let path = match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => return Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge
        | ManagementSessionError::NotFound
        | ManagementSessionError::Conflict => "/tenant/devices/claim-policy?notice=invalid-request",
        ManagementSessionError::Unavailable => {
            "/tenant/devices/claim-policy?notice=service-unavailable"
        }
    };
    Ok(Redirect::to(path))
}

pub(in crate::management) fn tenant_application_notice(
    query: Option<&str>,
) -> Option<&'static str> {
    match query {
        Some("notice=application-saved") => Some("Application saved."),
        Some("notice=invalid-request") => Some("Request could not be processed."),
        Some("notice=mutation-unavailable") => {
            Some("The requested OAuth application is unavailable.")
        }
        Some("notice=service-unavailable") => Some("Tenant management service is unavailable."),
        _ => None,
    }
}

pub(in crate::management) fn tenant_application_form_error(
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    let path = match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => return Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge => "/tenant/applications?notice=invalid-request",
        ManagementSessionError::NotFound | ManagementSessionError::Conflict => {
            "/tenant/applications?notice=mutation-unavailable"
        }
        ManagementSessionError::Unavailable => "/tenant/applications?notice=service-unavailable",
    };
    Ok(Redirect::to(path))
}

pub(in crate::management) fn tenant_application_error(
    error: PlatformStoreError,
) -> ManagementSessionError {
    match error {
        PlatformStoreError::InvalidApplicationId(_)
        | PlatformStoreError::InvalidApplicationKind(_)
        | PlatformStoreError::EmptyApplicationLaunchUrl
        | PlatformStoreError::EmptyApplicationClientId
        | PlatformStoreError::EmptyApplicationRedirectUri
        | PlatformStoreError::InvalidApplicationRedirectUri(_)
        | PlatformStoreError::DuplicateApplicationRedirectUri(_)
        | PlatformStoreError::EmptyApplicationScope
        | PlatformStoreError::InvalidApplicationScopes => ManagementSessionError::BadRequest,
        PlatformStoreError::ApplicationClientIdConflict(_)
        | PlatformStoreError::ApplicationTenantConflict(_)
        | PlatformStoreError::TenantApplicationLimit(_) => ManagementSessionError::Conflict,
        _ => ManagementSessionError::Unavailable,
    }
}

pub(in crate::management) fn tenant_topology_device_id(
    value: &str,
) -> Result<&str, ManagementSessionError> {
    let value = value.trim();
    if value.is_empty() {
        Err(ManagementSessionError::BadRequest)
    } else {
        Ok(value)
    }
}

pub(in crate::management) async fn update_tenant_gateway_child(
    state: &ManagementState,
    tenant: TenantSession,
    child_device_id: &str,
    gateway_device_id: Option<String>,
) -> Result<(), ManagementDeviceError> {
    let child =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await?
            .into_iter()
            .find(|device| device.device_id == child_device_id)
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
    ManagementDeviceRepository::update_management_device(
        state.store.as_ref(),
        tenant.tenant_id,
        AuditPrincipal::TenantAccount(tenant.tenant_account_id),
        child_device_id,
        UpdateManagementDevice {
            display_name: child
                .display_name
                .unwrap_or_else(|| child.device_id.clone()),
            asset_id: child.asset_id,
            device_profile_id: child.device_profile_id,
            attributes: None,
            topology: Some(ManagementDeviceTopology {
                is_gateway: child.topology.is_gateway,
                gateway_device_id,
            }),
        },
    )
    .await
    .map(|_| ())
}

pub(in crate::management) fn tenant_topology_notice(query: Option<&str>) -> Option<&'static str> {
    match query {
        Some("notice=gateway-assigned") => Some("Gateway child assigned."),
        Some("notice=gateway-detached") => Some("Gateway child detached."),
        Some("notice=invalid-request") => Some("Request could not be processed."),
        Some("notice=mutation-unavailable") => {
            Some("The requested gateway assignment is unavailable.")
        }
        Some("notice=service-unavailable") => Some("Tenant management service is unavailable."),
        _ => None,
    }
}

pub(in crate::management) fn tenant_topology_form_error(
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    let path = match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => return Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge => "/tenant/topology?notice=invalid-request",
        ManagementSessionError::NotFound | ManagementSessionError::Conflict => {
            "/tenant/topology?notice=mutation-unavailable"
        }
        ManagementSessionError::Unavailable => "/tenant/topology?notice=service-unavailable",
    };
    Ok(Redirect::to(path))
}

pub(in crate::management) fn tenant_relation_notice(query: Option<&str>) -> Option<&'static str> {
    match query {
        Some("notice=relation-created") => Some("Device relation created."),
        Some("notice=relation-deleted") => Some("Device relation deleted."),
        Some("notice=invalid-request") => Some("Request could not be processed."),
        Some("notice=mutation-unavailable") => {
            Some("The requested device relation is unavailable.")
        }
        Some("notice=service-unavailable") => Some("Tenant management service is unavailable."),
        _ => None,
    }
}

pub(in crate::management) fn tenant_relation_form_error(
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    let path = match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => return Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge => "/tenant/relations?notice=invalid-request",
        ManagementSessionError::NotFound | ManagementSessionError::Conflict => {
            "/tenant/relations?notice=mutation-unavailable"
        }
        ManagementSessionError::Unavailable => "/tenant/relations?notice=service-unavailable",
    };
    Ok(Redirect::to(path))
}

pub(in crate::management) async fn platform_tenant_groups(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_groups_page(
        &state,
        tenant,
        tenant_management_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn platform_tenant_permissions(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Html<String>, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    tenant_permissions_page(
        &state,
        tenant,
        tenant_management_notice(request.uri().query()),
    )
    .await
}

pub(in crate::management) async fn tenant_users_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let users =
        ManagementUserRepository::list_management_users(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_user_error)?;
    let page = crate::TenantUsersPage::new(
        users
            .into_iter()
            .map(|user| {
                crate::TenantUserRow::with_capabilities(
                    user.id.to_string(),
                    user.username,
                    "Active",
                    tenant_user_account_class_label(user.account_class.as_str()),
                    user.capabilities
                        .into_iter()
                        .map(|capability| capability.as_str().to_owned())
                        .collect(),
                )
            })
            .collect(),
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_users(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) fn tenant_user_account_class_label(account_class: &str) -> &'static str {
    match account_class {
        "system" => "System",
        "admin" => "Admin",
        "user" => "User",
        _ => "Unknown",
    }
}

pub(in crate::management) async fn tenant_groups_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let users =
        ManagementUserRepository::list_management_users(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_user_error)?;
    let groups = TenantAuthorizationRepository::list_tenant_user_groups(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    let user_names: HashMap<Uuid, String> = users
        .iter()
        .map(|user| (user.id, user.username.clone()))
        .collect();
    let page = crate::TenantGroupsPage::new(
        users
            .into_iter()
            .map(|user| crate::TenantSelectOption::new(user.id.to_string(), user.username))
            .collect(),
        groups
            .into_iter()
            .map(|group| {
                let owner = user_names
                    .get(&group.owner_user_id)
                    .cloned()
                    .unwrap_or_else(|| group.owner_user_id.to_string());
                crate::TenantGroupRow::new(
                    group.id.to_string(),
                    group.name,
                    owner,
                    group
                        .members
                        .into_iter()
                        .map(|member| {
                            crate::TenantGroupMemberRow::new(
                                member.user_id.to_string(),
                                member.username,
                            )
                        })
                        .collect(),
                )
            })
            .collect(),
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_groups(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

pub(in crate::management) async fn tenant_permissions_page(
    state: &ManagementState,
    tenant: TenantSession,
    notice: Option<&'static str>,
) -> Result<Html<String>, ManagementSessionError> {
    let users =
        ManagementUserRepository::list_management_users(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_user_error)?;
    let groups = TenantAuthorizationRepository::list_tenant_user_groups(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(tenant_authorization_error)?;
    let assets =
        ManagementAssetRepository::list_management_assets(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_asset_error)?;
    let devices =
        ManagementDeviceRepository::list_management_devices(state.store.as_ref(), tenant.tenant_id)
            .await
            .map_err(management_device_error)?;
    let permissions = TenantAuthorizationRepository::list_active_resource_permissions(
        state.store.as_ref(),
        tenant.tenant_id,
    )
    .await
    .map_err(tenant_authorization_error)?;

    let user_names: HashMap<Uuid, String> = users
        .iter()
        .map(|user| (user.id, user.username.clone()))
        .collect();
    let group_names: HashMap<Uuid, String> = groups
        .iter()
        .map(|group| (group.id, group.name.clone()))
        .collect();
    let asset_names: HashMap<Uuid, String> = assets
        .iter()
        .map(|asset| (asset.id, asset.name.clone()))
        .collect();
    let device_names: HashMap<String, String> = devices
        .iter()
        .map(|device| {
            (
                device.device_id.clone(),
                device
                    .display_name
                    .clone()
                    .unwrap_or_else(|| device.device_id.clone()),
            )
        })
        .collect();

    let mut subjects: Vec<_> = users
        .iter()
        .map(|user| {
            crate::TenantSelectOption::new(
                format!("user:{}", user.id),
                format!("User: {}", user.username),
            )
        })
        .collect();
    subjects.extend(groups.iter().map(|group| {
        crate::TenantSelectOption::new(
            format!("group:{}", group.id),
            format!("Group: {}", group.name),
        )
    }));
    let page = crate::TenantPermissionsPage::new(
        subjects,
        assets
            .into_iter()
            .map(|asset| {
                crate::TenantSelectOption::new(
                    asset.id.to_string(),
                    format!("Asset: {}", asset.name),
                )
            })
            .collect(),
        devices
            .into_iter()
            .map(|device| {
                let label = device
                    .display_name
                    .unwrap_or_else(|| device.device_id.clone());
                crate::TenantSelectOption::new(device.device_id, format!("Device: {label}"))
            })
            .collect(),
        permissions
            .into_iter()
            .map(|permission| {
                let subject = match (permission.subject_user_id, permission.subject_group_id) {
                    (Some(user_id), None) => format!(
                        "User: {}",
                        user_names
                            .get(&user_id)
                            .cloned()
                            .unwrap_or_else(|| user_id.to_string())
                    ),
                    (None, Some(group_id)) => format!(
                        "Group: {}",
                        group_names
                            .get(&group_id)
                            .cloned()
                            .unwrap_or_else(|| group_id.to_string())
                    ),
                    _ => "Unavailable".to_owned(),
                };
                let resource = match (permission.asset_id, permission.device_id.as_deref()) {
                    (Some(asset_id), None) => format!(
                        "Asset: {}",
                        asset_names
                            .get(&asset_id)
                            .cloned()
                            .unwrap_or_else(|| asset_id.to_string())
                    ),
                    (None, Some(device_id)) => format!(
                        "Device: {}",
                        device_names
                            .get(device_id)
                            .cloned()
                            .unwrap_or_else(|| device_id.to_owned())
                    ),
                    _ => "Unavailable".to_owned(),
                };
                crate::TenantPermissionRow::new(
                    permission.id.to_string(),
                    subject,
                    resource,
                    resource_permission_label(permission.permission),
                    if permission.inherit_children {
                        "Child assets"
                    } else {
                        "None"
                    },
                )
            })
            .collect(),
        notice,
    );
    let rendered = crate::PlatformUiRenderer::render_tenant_permissions(
        &crate::PlatformUiIdentity::new(format!("Tenant {}", tenant.tenant_id)),
        &page,
    )
    .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok(Html(rendered))
}

#[derive(Clone, Copy)]
pub(in crate::management) enum TenantProfileKind {
    Device,
    Asset,
}

impl TenantProfileKind {
    const fn path(self) -> &'static str {
        match self {
            Self::Device => "/tenant/profiles/device",
            Self::Asset => "/tenant/profiles/asset",
        }
    }

    const fn created_notice(self) -> &'static str {
        match self {
            Self::Device => "device-profile-created",
            Self::Asset => "asset-profile-created",
        }
    }
}

pub(in crate::management) fn tenant_profile_notice(
    kind: TenantProfileKind,
    query: Option<&str>,
) -> Option<&'static str> {
    match (kind, query) {
        (TenantProfileKind::Device, Some("notice=device-profile-created"))
        | (TenantProfileKind::Asset, Some("notice=asset-profile-created")) => {
            Some("Profile created.")
        }
        (_, Some("notice=invalid-request")) => Some("Request could not be processed."),
        (_, Some("notice=mutation-unavailable")) => Some("The requested profile is unavailable."),
        (_, Some("notice=service-unavailable")) => {
            Some("Tenant management service is unavailable.")
        }
        _ => None,
    }
}

pub(in crate::management) fn tenant_profile_redirect(
    kind: TenantProfileKind,
    notice: &'static str,
) -> Redirect {
    Redirect::to(&format!("{}?notice={notice}", kind.path()))
}

pub(in crate::management) fn tenant_profile_form_error(
    kind: TenantProfileKind,
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
    Ok(tenant_profile_redirect(kind, notice))
}

#[derive(Clone, Copy)]
pub(in crate::management) enum TenantManagementPage {
    Users,
    Groups,
    Permissions,
    Assets,
    Devices,
}

#[derive(Clone, Copy)]
pub(in crate::management) enum TenantManagementNotice {
    UserCreated,
    GroupCreated,
    MemberAdded,
    MemberRemoved,
    PermissionCreated,
    PermissionRevoked,
    AssetCreated,
    InvalidRequest,
    MutationUnavailable,
    ServiceUnavailable,
}

impl TenantManagementNotice {
    const fn message(self) -> &'static str {
        match self {
            Self::UserCreated => "User created.",
            Self::GroupCreated => "Group created.",
            Self::MemberAdded => "Member added.",
            Self::MemberRemoved => "Member removed.",
            Self::PermissionCreated => "Permission created.",
            Self::PermissionRevoked => "Permission revoked.",
            Self::AssetCreated => "Asset created.",
            Self::InvalidRequest => "Request could not be processed.",
            Self::MutationUnavailable => "The requested tenant resource is unavailable.",
            Self::ServiceUnavailable => "Tenant management service is unavailable.",
        }
    }
}

pub(in crate::management) fn tenant_management_redirect(
    page: TenantManagementPage,
    notice: TenantManagementNotice,
) -> Redirect {
    let path = match page {
        TenantManagementPage::Users => match notice {
            TenantManagementNotice::UserCreated => "/tenant/users?notice=user-created",
            TenantManagementNotice::InvalidRequest => "/tenant/users?notice=invalid-request",
            TenantManagementNotice::MutationUnavailable => {
                "/tenant/users?notice=mutation-unavailable"
            }
            TenantManagementNotice::ServiceUnavailable => {
                "/tenant/users?notice=service-unavailable"
            }
            TenantManagementNotice::GroupCreated
            | TenantManagementNotice::MemberAdded
            | TenantManagementNotice::MemberRemoved
            | TenantManagementNotice::PermissionCreated
            | TenantManagementNotice::PermissionRevoked
            | TenantManagementNotice::AssetCreated => "/tenant/users?notice=invalid-request",
        },
        TenantManagementPage::Groups => match notice {
            TenantManagementNotice::GroupCreated => "/tenant/groups?notice=group-created",
            TenantManagementNotice::MemberAdded => "/tenant/groups?notice=member-added",
            TenantManagementNotice::MemberRemoved => "/tenant/groups?notice=member-removed",
            TenantManagementNotice::InvalidRequest => "/tenant/groups?notice=invalid-request",
            TenantManagementNotice::MutationUnavailable => {
                "/tenant/groups?notice=mutation-unavailable"
            }
            TenantManagementNotice::ServiceUnavailable => {
                "/tenant/groups?notice=service-unavailable"
            }
            TenantManagementNotice::PermissionCreated
            | TenantManagementNotice::PermissionRevoked
            | TenantManagementNotice::UserCreated
            | TenantManagementNotice::AssetCreated => "/tenant/groups?notice=invalid-request",
        },
        TenantManagementPage::Permissions => match notice {
            TenantManagementNotice::PermissionCreated => {
                "/tenant/permissions?notice=permission-created"
            }
            TenantManagementNotice::PermissionRevoked => {
                "/tenant/permissions?notice=permission-revoked"
            }
            TenantManagementNotice::InvalidRequest => "/tenant/permissions?notice=invalid-request",
            TenantManagementNotice::MutationUnavailable => {
                "/tenant/permissions?notice=mutation-unavailable"
            }
            TenantManagementNotice::ServiceUnavailable => {
                "/tenant/permissions?notice=service-unavailable"
            }
            TenantManagementNotice::GroupCreated
            | TenantManagementNotice::MemberAdded
            | TenantManagementNotice::MemberRemoved
            | TenantManagementNotice::UserCreated
            | TenantManagementNotice::AssetCreated => "/tenant/permissions?notice=invalid-request",
        },
        TenantManagementPage::Assets => match notice {
            TenantManagementNotice::AssetCreated => "/tenant/assets?notice=asset-created",
            TenantManagementNotice::InvalidRequest => "/tenant/assets?notice=invalid-request",
            TenantManagementNotice::MutationUnavailable => {
                "/tenant/assets?notice=mutation-unavailable"
            }
            TenantManagementNotice::ServiceUnavailable => {
                "/tenant/assets?notice=service-unavailable"
            }
            TenantManagementNotice::GroupCreated
            | TenantManagementNotice::MemberAdded
            | TenantManagementNotice::MemberRemoved
            | TenantManagementNotice::PermissionCreated
            | TenantManagementNotice::PermissionRevoked
            | TenantManagementNotice::UserCreated => "/tenant/assets?notice=invalid-request",
        },
        TenantManagementPage::Devices => match notice {
            TenantManagementNotice::InvalidRequest => "/tenant/devices?notice=invalid-request",
            TenantManagementNotice::MutationUnavailable => {
                "/tenant/devices?notice=mutation-unavailable"
            }
            TenantManagementNotice::ServiceUnavailable => {
                "/tenant/devices?notice=service-unavailable"
            }
            TenantManagementNotice::GroupCreated
            | TenantManagementNotice::MemberAdded
            | TenantManagementNotice::MemberRemoved
            | TenantManagementNotice::PermissionCreated
            | TenantManagementNotice::PermissionRevoked
            | TenantManagementNotice::AssetCreated
            | TenantManagementNotice::UserCreated => "/tenant/devices?notice=invalid-request",
        },
    };
    Redirect::to(path)
}

pub(in crate::management) fn tenant_management_notice(query: Option<&str>) -> Option<&'static str> {
    let notice = match query {
        Some("notice=user-created") => TenantManagementNotice::UserCreated,
        Some("notice=group-created") => TenantManagementNotice::GroupCreated,
        Some("notice=member-added") => TenantManagementNotice::MemberAdded,
        Some("notice=member-removed") => TenantManagementNotice::MemberRemoved,
        Some("notice=permission-created") => TenantManagementNotice::PermissionCreated,
        Some("notice=permission-revoked") => TenantManagementNotice::PermissionRevoked,
        Some("notice=asset-created") => TenantManagementNotice::AssetCreated,
        Some("notice=invalid-request") => TenantManagementNotice::InvalidRequest,
        Some("notice=mutation-unavailable") => TenantManagementNotice::MutationUnavailable,
        Some("notice=service-unavailable") => TenantManagementNotice::ServiceUnavailable,
        _ => return None,
    };
    Some(notice.message())
}

pub(in crate::management) fn tenant_management_form_error(
    page: TenantManagementPage,
    error: ManagementSessionError,
) -> Result<Redirect, ManagementSessionError> {
    let notice = match error {
        ManagementSessionError::Unauthorized
        | ManagementSessionError::TooManyRequests
        | ManagementSessionError::Forbidden => return Err(error),
        ManagementSessionError::BadRequest
        | ManagementSessionError::UnsupportedMediaType
        | ManagementSessionError::PayloadTooLarge => TenantManagementNotice::InvalidRequest,
        ManagementSessionError::NotFound | ManagementSessionError::Conflict => {
            TenantManagementNotice::MutationUnavailable
        }
        ManagementSessionError::Unavailable => TenantManagementNotice::ServiceUnavailable,
    };
    Ok(tenant_management_redirect(page, notice))
}

pub(in crate::management) async fn create_tenant_user_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateTenantUserForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return tenant_management_form_error(TenantManagementPage::Users, error),
    };
    if validate_password(&request.password).is_err() {
        return tenant_management_form_error(
            TenantManagementPage::Users,
            ManagementSessionError::BadRequest,
        );
    }
    let password_hash = match hash_password(&request.password) {
        Ok(password_hash) => password_hash,
        Err(_) => {
            return tenant_management_form_error(
                TenantManagementPage::Users,
                ManagementSessionError::Unavailable,
            );
        }
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match ManagementUserRepository::create_management_user(
        state.store.as_ref(),
        CreateManagementUser {
            tenant_id: tenant.tenant_id,
            username: request.username,
            password_hash,
        },
    )
    .await
    {
        Ok(_) => Ok(tenant_management_redirect(
            TenantManagementPage::Users,
            TenantManagementNotice::UserCreated,
        )),
        Err(error) => {
            tenant_management_form_error(TenantManagementPage::Users, management_user_error(error))
        }
    }
}

pub(in crate::management) async fn create_tenant_group_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: CreateTenantGroupForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return tenant_management_form_error(TenantManagementPage::Groups, error),
    };
    let name = request.name.trim();
    if name.is_empty() || name.len() > 128 {
        return tenant_management_form_error(
            TenantManagementPage::Groups,
            ManagementSessionError::BadRequest,
        );
    }
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    match TenantAuthorizationRepository::create_user_group(
        state.store.as_ref(),
        NewUserGroup {
            tenant_id: tenant.tenant_id,
            owner_user_id: request.owner_user_id,
            name: name.to_owned(),
            metadata: json!({}),
        },
    )
    .await
    {
        Ok(_) => Ok(tenant_management_redirect(
            TenantManagementPage::Groups,
            TenantManagementNotice::GroupCreated,
        )),
        Err(error) => tenant_management_form_error(
            TenantManagementPage::Groups,
            tenant_authorization_error(error),
        ),
    }
}

pub(in crate::management) async fn add_tenant_group_member_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    tenant_group_member_form(state, request, true).await
}

pub(in crate::management) async fn remove_tenant_group_member_form(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<Redirect, ManagementSessionError> {
    tenant_group_member_form(state, request, false).await
}

pub(in crate::management) async fn tenant_group_member_form(
    state: ManagementState,
    request: Request,
    add_member: bool,
) -> Result<Redirect, ManagementSessionError> {
    let headers = request.headers().clone();
    let tenant = require_tenant_account(&state.session_verifier, &headers)?;
    let request: TenantGroupMemberForm = match management_request_form(&state, request).await {
        Ok(request) => request,
        Err(error) => return tenant_management_form_error(TenantManagementPage::Groups, error),
    };
    let _lease = authorize_tenant_mutation(&state, &headers).await?;
    let result = if add_member {
        TenantAuthorizationRepository::add_user_to_group(
            state.store.as_ref(),
            tenant.tenant_id,
            AuditPrincipal::TenantAccount(tenant.tenant_account_id),
            request.group_id,
            request.user_id,
        )
        .await
    } else {
        TenantAuthorizationRepository::remove_user_from_group(
            state.store.as_ref(),
            tenant.tenant_id,
            AuditPrincipal::TenantAccount(tenant.tenant_account_id),
            request.group_id,
            request.user_id,
        )
        .await
    };
    match result {
        Ok(_) => Ok(tenant_management_redirect(
            TenantManagementPage::Groups,
            if add_member {
                TenantManagementNotice::MemberAdded
            } else {
                TenantManagementNotice::MemberRemoved
            },
        )),
        Err(error) => tenant_management_form_error(
            TenantManagementPage::Groups,
            tenant_authorization_error(error),
        ),
    }
}

pub(in crate::management) async fn create_tenant_permission_form(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Redirect, ManagementSessionError> {
    let _tenant = require_tenant_account(&state.session_verifier, &headers)?;
    Err(ManagementSessionError::Forbidden)
}

pub(in crate::management) async fn revoke_tenant_permission_form(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Redirect, ManagementSessionError> {
    let _tenant = require_tenant_account(&state.session_verifier, &headers)?;
    Err(ManagementSessionError::Forbidden)
}
