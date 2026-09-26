use super::*;

pub(super) fn management_user_error(error: ManagementUserError) -> ManagementSessionError {
    match error {
        ManagementUserError::InvalidUsername(_)
        | ManagementUserError::EmptyPasswordHash
        | ManagementUserError::CapabilitiesRequireUserAccount => ManagementSessionError::BadRequest,
        ManagementUserError::UsernameConflict(_)
        | ManagementUserError::SystemUserImmutable
        | ManagementUserError::LastAdministrator => ManagementSessionError::Conflict,
        ManagementUserError::UserNotFound => ManagementSessionError::NotFound,
        ManagementUserError::InvalidStoredUserId
        | ManagementUserError::InvalidStoredRole(_)
        | ManagementUserError::InvalidStoredAccountClass(_)
        | ManagementUserError::InvalidStoredUserCapability(_)
        | ManagementUserError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_alert_error(error: ManagementAlertError) -> ManagementSessionError {
    match error {
        ManagementAlertError::InvalidStoredAlertId
        | ManagementAlertError::InvalidStoredAlertTimestamp
        | ManagementAlertError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_alert_rule_error(
    error: ManagementAlertRuleError,
) -> ManagementSessionError {
    match error {
        ManagementAlertRuleError::InvalidName
        | ManagementAlertRuleError::InvalidMetricKey
        | ManagementAlertRuleError::InvalidRuleType
        | ManagementAlertRuleError::InvalidComparison
        | ManagementAlertRuleError::InvalidThreshold
        | ManagementAlertRuleError::InvalidWindow
        | ManagementAlertRuleError::InvalidDuration
        | ManagementAlertRuleError::InvalidHysteresis
        | ManagementAlertRuleError::InvalidSeverity => ManagementSessionError::BadRequest,
        ManagementAlertRuleError::DeviceUnavailable(_) => ManagementSessionError::Conflict,
        ManagementAlertRuleError::RuleNotFound => ManagementSessionError::NotFound,
        ManagementAlertRuleError::RuleArchived => ManagementSessionError::Conflict,
        ManagementAlertRuleError::InvalidStoredRule | ManagementAlertRuleError::Storage { .. } => {
            ManagementSessionError::Unavailable
        }
    }
}

pub(super) fn management_alert_incident_error(
    error: ManagementAlertIncidentError,
) -> ManagementSessionError {
    match error {
        ManagementAlertIncidentError::IncidentNotFound => ManagementSessionError::NotFound,
        ManagementAlertIncidentError::InvalidStoredIncident
        | ManagementAlertIncidentError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_audit_error(error: AuditEventError) -> ManagementSessionError {
    match error {
        AuditEventError::InvalidLimit { .. } => ManagementSessionError::BadRequest,
        AuditEventError::InvalidStoredEvent | AuditEventError::Database(_) => {
            ManagementSessionError::Unavailable
        }
    }
}

pub(super) fn tenant_authorization_error(
    error: TenantAuthorizationError,
) -> ManagementSessionError {
    match error {
        TenantAuthorizationError::UserNotFound { .. }
        | TenantAuthorizationError::TenantAccountNotFound { .. }
        | TenantAuthorizationError::GroupNotFound { .. }
        | TenantAuthorizationError::AssetNotFound { .. }
        | TenantAuthorizationError::DeviceNotFound { .. }
        | TenantAuthorizationError::PermissionNotFound { .. }
        | TenantAuthorizationError::InvitationNotFound { .. } => ManagementSessionError::NotFound,
        TenantAuthorizationError::InvalidPermissionSubject
        | TenantAuthorizationError::InvalidPermissionResource
        | TenantAuthorizationError::InvalidPermissionLevel { .. }
        | TenantAuthorizationError::DevicePermissionCannotInherit
        | TenantAuthorizationError::OwnerMustBeRegularUser { .. }
        | TenantAuthorizationError::OwnerCannotShareWithSelf => ManagementSessionError::BadRequest,
        TenantAuthorizationError::InvitationNotPending { .. } => ManagementSessionError::Conflict,
        TenantAuthorizationError::SystemAccountCannotTransferOwnership
        | TenantAuthorizationError::TenantAccountRequiredForOwnership
        | TenantAuthorizationError::ResourceOwnerRequired { .. } => {
            ManagementSessionError::Forbidden
        }
        TenantAuthorizationError::InvalidStoredRecord | TenantAuthorizationError::Database(_) => {
            ManagementSessionError::Unavailable
        }
    }
}

pub(super) fn management_device_profile_error(
    error: ManagementDeviceProfileError,
) -> ManagementSessionError {
    match error {
        ManagementDeviceProfileError::InvalidName
        | ManagementDeviceProfileError::TelemetrySchemaMustBeObject
        | ManagementDeviceProfileError::MetricMappingMustBeObject
        | ManagementDeviceProfileError::ReportingSettingsMustBeObject => {
            ManagementSessionError::BadRequest
        }
        ManagementDeviceProfileError::DeviceProfileNotFound => ManagementSessionError::NotFound,
        ManagementDeviceProfileError::NameConflict(_)
        | ManagementDeviceProfileError::DeviceProfileInUse(_) => ManagementSessionError::Conflict,
        ManagementDeviceProfileError::InvalidStoredProfile
        | ManagementDeviceProfileError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn application_domain_profile_error(
    error: ApplicationDomainProfileError,
) -> ManagementSessionError {
    match error {
        ApplicationDomainProfileError::UnsupportedConfigurationVersion
        | ApplicationDomainProfileError::PermissionDefinitionsMustBeObject
        | ApplicationDomainProfileError::UnknownContainedProfile
        | ApplicationDomainProfileError::InvalidName
        | ApplicationDomainProfileError::DefinitionMustBeObject
        | ApplicationDomainProfileError::LiveViewMustBeObject
        | ApplicationDomainProfileError::AssetProfilesOnly => ManagementSessionError::BadRequest,
        ApplicationDomainProfileError::ApplicationNotFound
        | ApplicationDomainProfileError::ProfileNotFound
        | ApplicationDomainProfileError::RelationNotFound
        | ApplicationDomainProfileError::ResourceNotFound => ManagementSessionError::NotFound,
        ApplicationDomainProfileError::NameConflict(_)
        | ApplicationDomainProfileError::ProfileInUse(_)
        | ApplicationDomainProfileError::ProfileKindMismatch
        | ApplicationDomainProfileError::RelationConflict => ManagementSessionError::Conflict,
        ApplicationDomainProfileError::InvalidStoredProfile
        | ApplicationDomainProfileError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn tenant_profile_import_error(
    error: ApplicationDomainProfileError,
) -> ManagementSessionError {
    match error {
        ApplicationDomainProfileError::Storage { .. } => ManagementSessionError::Unavailable,
        _ => ManagementSessionError::BadRequest,
    }
}

pub(super) fn management_asset_profile_error(
    error: ManagementAssetProfileError,
) -> ManagementSessionError {
    match error {
        ManagementAssetProfileError::InvalidName
        | ManagementAssetProfileError::FieldsMustBeObject
        | ManagementAssetProfileError::DashboardDefaultsMustBeObject => {
            ManagementSessionError::BadRequest
        }
        ManagementAssetProfileError::AssetProfileNotFound => ManagementSessionError::NotFound,
        ManagementAssetProfileError::NameConflict(_)
        | ManagementAssetProfileError::AssetProfileInUse(_) => ManagementSessionError::Conflict,
        ManagementAssetProfileError::InvalidStoredProfile
        | ManagementAssetProfileError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_device_response(
    device: StorageManagementDevice,
) -> ManagementDeviceResponse {
    ManagementDeviceResponse {
        device_id: device.device_id,
        display_name: device.display_name,
        owner_user_id: device.owner_user_id,
        asset_id: device.asset_id,
        device_profile_id: device.device_profile_id,
        attributes: device.attributes,
        online: device.health.online,
        last_seen_at: device.health.last_seen_at,
        is_gateway: device.topology.is_gateway,
        gateway_device_id: device.topology.gateway_device_id,
        gateway_status: device.health.gateway_status.map(|status| match status {
            ManagementGatewayStatus::Online => "online".to_owned(),
            ManagementGatewayStatus::Offline => "offline".to_owned(),
        }),
        child_status: device.health.child_status.map(|status| match status {
            ManagementChildStatus::Fresh => "fresh".to_owned(),
            ManagementChildStatus::Stale => "stale".to_owned(),
            ManagementChildStatus::Unavailable => "unavailable".to_owned(),
        }),
    }
}

pub(super) fn management_device_telemetry_response(
    telemetry: ManagementDeviceTelemetry,
) -> ManagementDeviceTelemetryResponse {
    ManagementDeviceTelemetryResponse {
        event_at: telemetry.event_at,
        received_at: telemetry.received_at,
        device_id: telemetry.device_id,
        boot_id: telemetry.boot_id,
        sequence: telemetry.sequence,
        measurements: telemetry.measurements,
        topic: telemetry.topic,
    }
}

pub(super) fn management_asset_response(asset: StorageManagementAsset) -> ManagementAssetResponse {
    ManagementAssetResponse {
        id: asset.id,
        name: asset.name,
        owner_user_id: asset.owner_user_id,
        asset_profile_id: asset.asset_profile_id,
        parent_asset_id: asset.parent_asset_id,
        metadata: asset.metadata,
        attributes: asset.attributes,
    }
}

pub(super) fn management_device_error(error: ManagementDeviceError) -> ManagementSessionError {
    match error {
        ManagementDeviceError::InvalidDeviceId(_)
        | ManagementDeviceError::InvalidDisplayName
        | ManagementDeviceError::AttributesMustBeObject => ManagementSessionError::BadRequest,
        ManagementDeviceError::DeviceNotFound => ManagementSessionError::NotFound,
        ManagementDeviceError::GatewayCannotHaveParent
        | ManagementDeviceError::DeviceCannotBeOwnGateway
        | ManagementDeviceError::GatewayHasChildren
        | ManagementDeviceError::GatewayUnavailable
        | ManagementDeviceError::GatewayIsNotGateway
        | ManagementDeviceError::AssetUnavailable(_)
        | ManagementDeviceError::DeviceProfileUnavailable(_) => ManagementSessionError::Conflict,
        ManagementDeviceError::InvalidStoredAttributes
        | ManagementDeviceError::InvalidStoredTimestamp
        | ManagementDeviceError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn device_relation_error(error: DeviceRelationError) -> ManagementSessionError {
    match error {
        DeviceRelationError::InvalidDeviceId(_)
        | DeviceRelationError::InvalidRelationType(_)
        | DeviceRelationError::ReservedRelationType
        | DeviceRelationError::SelfRelation => ManagementSessionError::BadRequest,
        DeviceRelationError::DeviceNotFound { .. }
        | DeviceRelationError::AssetNotFound { .. }
        | DeviceRelationError::RelationNotFound => ManagementSessionError::NotFound,
        DeviceRelationError::RelationConflict => ManagementSessionError::Conflict,
        DeviceRelationError::InvalidStoredRelation | DeviceRelationError::Storage { .. } => {
            ManagementSessionError::Unavailable
        }
    }
}

pub(super) fn management_asset_error(error: ManagementAssetError) -> ManagementSessionError {
    match error {
        ManagementAssetError::InvalidName
        | ManagementAssetError::MetadataMustBeObject
        | ManagementAssetError::AttributesMustBeObject => ManagementSessionError::BadRequest,
        ManagementAssetError::AssetNotFound => ManagementSessionError::NotFound,
        ManagementAssetError::AssetProfileUnavailable(_)
        | ManagementAssetError::ParentAssetUnavailable(_)
        | ManagementAssetError::AssetCannotBeOwnParent
        | ManagementAssetError::AssetCannotHaveDescendantParent
        | ManagementAssetError::SiblingNameConflict { .. } => ManagementSessionError::Conflict,
        ManagementAssetError::ParentAssetNotOwned
        | ManagementAssetError::OwnerMustBeRegularUser => ManagementSessionError::Forbidden,
        ManagementAssetError::InvalidStoredAssetId
        | ManagementAssetError::InvalidStoredReferences
        | ManagementAssetError::InvalidStoredMetadata
        | ManagementAssetError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn system_tenant_error(error: TenantIdentityError) -> ManagementSessionError {
    match error {
        TenantIdentityError::InvalidTenantSlug
        | TenantIdentityError::EmptyTenantAccountUsername
        | TenantIdentityError::EmptyPasswordHash => ManagementSessionError::BadRequest,
        TenantIdentityError::TenantLifecycleDenied => ManagementSessionError::Conflict,
        TenantIdentityError::Database(error)
            if error
                .as_database_error()
                .is_some_and(|database| database.is_unique_violation()) =>
        {
            ManagementSessionError::Conflict
        }
        TenantIdentityError::EmptySystemUsername
        | TenantIdentityError::InvalidStoredIdentity
        | TenantIdentityError::Database(_) => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_device_token_error(
    error: DeviceTokenStoreError,
) -> ManagementSessionError {
    match error {
        DeviceTokenStoreError::NotFound => ManagementSessionError::NotFound,
        DeviceTokenStoreError::GatewayChild => ManagementSessionError::Conflict,
        DeviceTokenStoreError::ManagementProvision(error) => {
            management_device_provision_error(error)
        }
        _ => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_device_provision_error(
    error: ProvisionManagementDeviceError,
) -> ManagementSessionError {
    match error {
        ProvisionManagementDeviceError::InvalidDisplayName
        | ProvisionManagementDeviceError::AttributesMustBeObject => {
            ManagementSessionError::BadRequest
        }
        ProvisionManagementDeviceError::AssetUnavailable(_)
        | ProvisionManagementDeviceError::DeviceProfileUnavailable(_) => {
            ManagementSessionError::Conflict
        }
        ProvisionManagementDeviceError::Token(_)
        | ProvisionManagementDeviceError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

pub(super) fn management_device_token_repository_error(
    error: DeviceTokenRepositoryError,
) -> ManagementSessionError {
    match error {
        DeviceTokenRepositoryError::DeviceNotFound | DeviceTokenRepositoryError::TokenNotFound => {
            ManagementSessionError::NotFound
        }
        DeviceTokenRepositoryError::GatewayChild
        | DeviceTokenRepositoryError::TokenPrefixConflict => ManagementSessionError::Conflict,
        DeviceTokenRepositoryError::InvalidStoredTimestamp
        | DeviceTokenRepositoryError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

#[derive(Debug)]
pub(super) enum ManagementSessionError {
    Unauthorized,
    TooManyRequests,
    Forbidden,
    BadRequest,
    UnsupportedMediaType,
    PayloadTooLarge,
    NotFound,
    Conflict,
    Unavailable,
}

impl IntoResponse for ManagementSessionError {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::BadRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::UnsupportedMediaType => {
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type")
            }
            Self::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict => (StatusCode::CONFLICT, "conflict"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        };
        (status, Json(json!({ "error": code }))).into_response()
    }
}

#[cfg(test)]
mod unit_tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::{LoginAttemptKey, LoginRateLimiter, MAX_LOGIN_FAILURES};

    #[test]
    fn login_limiter_reserves_in_flight_attempts_before_authentication() {
        let mut limiter = LoginRateLimiter::default();
        let key = LoginAttemptKey {
            address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            username: "admin".to_owned(),
        };

        for _ in 0..MAX_LOGIN_FAILURES {
            assert!(limiter.reserve(&key));
        }
        assert!(!limiter.reserve(&key));
    }
}
