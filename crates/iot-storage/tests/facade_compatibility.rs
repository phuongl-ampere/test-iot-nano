use iot_storage::{
    AccountClass, AlertComparison, AlertEvaluationEvent, AlertEvaluationRepository,
    AlertEvaluationResult, AlertIncident, AlertIncidentRepository, AlertIncidentStatus, AlertRule,
    AlertRuleKind, AlertSeverity, ApplicationId, ApplicationKind, ApplicationRecord,
    ApplicationRepository, AuthenticatedDeviceToken, AuthorizationRepository, AuthorizationSubject,
    AuthorizedAssetListEntry, AuthorizedAssetSummary, AuthorizedDeviceListEntry,
    AuthorizedDeviceSummary, ClientId, CommandLifecycleRepository, CommandOutboxRecord,
    CommandOutboxState, CommandRepository, DeviceAuthorizationRepository, GatewayIngestEventKind,
    GatewayIngestRepository, GatewayIngestRequest, GatewayIngestResult,
    GatewayIngestValidationError, IdentityRepository, NewAlertIncident, NewApplication,
    NewCommandOutboxEntry, NewNotificationOutboxEntry, NewOAuthAuthorizationCode,
    NewOAuthClientSecret, NewResourcePermission, NewUserGroup, NotificationKind,
    NotificationOutboxRecord, NotificationOutboxState, NotificationRepository,
    OAuthAccessTokenRecord, OAuthAuthorizationCodeExchange, OAuthClientCredentialsToken,
    OAuthRepository, OwnershipTransferTarget, PermissionCreator, PlatformStore, PlatformStoreError,
    RedirectUri, ResourceAccess, ResourceAccessSource, ResourceKind, ResourcePermission,
    ResourcePermissionRecord, RetentionResult, SqliteStore, SqliteStoreError, TelemetryAggregate,
    TelemetryAggregateRepository, TelemetryRepository, TenantAuthorizationError,
    TenantAuthorizationRepository, TenantUserGroup, TenantUserGroupMember, TopologyRepository,
    UserDeviceActivity, UserDeviceActivityRepository, UserDeviceAlert, UserDeviceTelemetry,
    UserGroup,
};

fn storage_contracts<Store>()
where
    Store: ApplicationRepository
        + OAuthRepository
        + TopologyRepository
        + GatewayIngestRepository
        + IdentityRepository
        + DeviceAuthorizationRepository
        + CommandRepository
        + NotificationRepository
        + CommandLifecycleRepository
        + TelemetryRepository
        + TelemetryAggregateRepository
        + AlertEvaluationRepository
        + AlertIncidentRepository
        + AuthorizationRepository
        + UserDeviceActivityRepository
        + TenantAuthorizationRepository,
{
}

#[test]
fn consumer_can_compile_against_the_root_storage_facade() {
    storage_contracts::<PlatformStore>();

    let _ = std::mem::size_of::<AccountClass>();
    let _ = std::mem::size_of::<AlertComparison>();
    let _ = std::mem::size_of::<AlertEvaluationEvent>();
    let _ = std::mem::size_of::<AlertEvaluationResult>();
    let _ = std::mem::size_of::<AlertIncident>();
    let _ = std::mem::size_of::<AlertIncidentStatus>();
    let _ = std::mem::size_of::<ApplicationId>();
    let _ = std::mem::size_of::<ApplicationKind>();
    let _ = std::mem::size_of::<ApplicationRecord>();
    let _ = std::mem::size_of::<ClientId>();
    let _ = std::mem::size_of::<RedirectUri>();
    let _ = std::mem::size_of::<NewApplication>();
    let _ = std::mem::size_of::<NewOAuthClientSecret>();
    let _ = std::mem::size_of::<NewOAuthAuthorizationCode>();
    let _ = std::mem::size_of::<OAuthAuthorizationCodeExchange>();
    let _ = std::mem::size_of::<OAuthClientCredentialsToken>();
    let _ = std::mem::size_of::<OAuthAccessTokenRecord>();
    let _ = std::mem::size_of::<GatewayIngestEventKind>();
    let _ = std::mem::size_of::<GatewayIngestRequest>();
    let _ = std::mem::size_of::<GatewayIngestResult>();
    let _ = std::mem::size_of::<GatewayIngestValidationError>();
    let _ = std::mem::size_of::<AuthenticatedDeviceToken>();
    let _ = std::mem::size_of::<AuthorizedAssetListEntry>();
    let _ = std::mem::size_of::<AuthorizedAssetSummary>();
    let _ = std::mem::size_of::<AuthorizedDeviceListEntry>();
    let _ = std::mem::size_of::<AuthorizedDeviceSummary>();
    let _ = std::mem::size_of::<NewCommandOutboxEntry>();
    let _ = std::mem::size_of::<CommandOutboxRecord>();
    let _ = std::mem::size_of::<CommandOutboxState>();
    let _ = std::mem::size_of::<NotificationKind>();
    let _ = std::mem::size_of::<NotificationOutboxRecord>();
    let _ = std::mem::size_of::<NotificationOutboxState>();
    let _ = std::mem::size_of::<TelemetryAggregate>();
    let _ = std::mem::size_of::<TenantAuthorizationError>();
    let _ = std::mem::size_of::<AlertRule>();
    let _ = std::mem::size_of::<AlertRuleKind>();
    let _ = std::mem::size_of::<AlertSeverity>();
    let _ = std::mem::size_of::<NewAlertIncident>();
    let _ = std::mem::size_of::<NewNotificationOutboxEntry>();
    let _ = std::mem::size_of::<ResourceAccess>();
    let _ = std::mem::size_of::<ResourceAccessSource>();
    let _ = std::mem::size_of::<ResourceKind>();
    let _ = std::mem::size_of::<ResourcePermission>();
    let _ = std::mem::size_of::<ResourcePermissionRecord>();
    let _ = std::mem::size_of::<SqliteStore>();
    let _ = std::mem::size_of::<SqliteStoreError>();
    let _ = std::mem::size_of::<RetentionResult>();
    let _ = std::mem::size_of::<AuthorizationSubject>();
    let _ = std::mem::size_of::<NewUserGroup>();
    let _ = std::mem::size_of::<UserGroup>();
    let _ = std::mem::size_of::<TenantUserGroup>();
    let _ = std::mem::size_of::<TenantUserGroupMember>();
    let _ = std::mem::size_of::<PermissionCreator>();
    let _ = std::mem::size_of::<OwnershipTransferTarget>();
    let _ = std::mem::size_of::<NewResourcePermission>();
    let _ = std::mem::size_of::<PlatformStore>();
    let _ = std::mem::size_of::<PlatformStoreError>();
    let _ = std::mem::size_of::<UserDeviceActivity>();
    let _ = std::mem::size_of::<UserDeviceTelemetry>();
    let _ = std::mem::size_of::<UserDeviceAlert>();
}

#[test]
fn consumer_can_call_application_oauth_and_identity_store_methods() {
    let _ = PlatformStore::upsert_application;
    let _ = PlatformStore::list_applications_for_tenant;
    let _ = PlatformStore::find_application_by_app_id;
    let _ = PlatformStore::find_application_by_client_id;
    let _ = PlatformStore::register_client_secret;
    let _ = PlatformStore::issue_authorization_code;
    let _ = PlatformStore::consume_authorization_code_and_issue_access_token;
    let _ = PlatformStore::issue_client_credentials_access_token;
    let _ = PlatformStore::resolve_access_token;
    let _ = PlatformStore::register_device;
    let _ = PlatformStore::resolve_active_device_token;
}

#[test]
fn consumer_can_call_authorization_and_resource_store_methods() {
    let _ = PlatformStore::authorization_subject;
    let _ = PlatformStore::list_authorized_devices;
    let _ = PlatformStore::list_authorized_assets;
    let _ = PlatformStore::authorized_device;
    let _ = PlatformStore::authorized_asset;
    let _ = PlatformStore::device_permission;
    let _ = PlatformStore::asset_permission;
    let _ = PlatformStore::list_tenant_user_groups;
    let _ = PlatformStore::list_active_resource_permissions;
    let _ = PlatformStore::create_user_group;
    let _ = PlatformStore::add_user_to_group;
    let _ = PlatformStore::remove_user_from_group;
    let _ = PlatformStore::create_resource_permission;
    let _ = PlatformStore::revoke_resource_permission;
    let _ = PlatformStore::transfer_resource_ownership;
}

#[test]
fn consumer_can_call_command_and_notification_store_methods() {
    let _ = PlatformStore::enqueue_command;
    let _ = PlatformStore::enqueue_authorized_command;
    let _ = PlatformStore::ready_command_tenants;
    let _ = PlatformStore::claim_commands;
    let _ = PlatformStore::mark_command_published;
    let _ = PlatformStore::mark_command_failed;
    let _ = PlatformStore::release_command_for_retry;
    let _ = PlatformStore::expire_due_commands;
    let _ = PlatformStore::expire_command_if_elapsed;
    let _ = PlatformStore::mark_command_responded;
    let _ = PlatformStore::claim_notifications;
    let _ = PlatformStore::ready_notification_tenants;
    let _ = PlatformStore::mark_notification_sent;
    let _ = PlatformStore::enqueue_notification;
    let _ = PlatformStore::release_notification_for_retry;
}

#[test]
fn consumer_can_call_telemetry_alert_and_retention_store_methods() {
    let _ = PlatformStore::write_telemetry;
    let _ = PlatformStore::ingest_gateway;
    let _ = PlatformStore::average_metric;
    let _ = PlatformStore::evaluate_alert_events;
    let _ = PlatformStore::evaluate_alert_windows;
    let _ = PlatformStore::create_incident;
    let _ = PlatformStore::update_incident_last_value;
    let _ = PlatformStore::open_incident;
    let _ = PlatformStore::open_incident_with_notification;
    let _ = PlatformStore::recover_incident;
    let _ = PlatformStore::resolve_incident;
    let _ = PlatformStore::resolve_incident_with_notification;
    let _ = PlatformStore::remind_incident;
    let _ = PlatformStore::remind_incident_with_notification;
    let _ = SqliteStore::write_telemetry;
    let _ = SqliteStore::write_telemetry_in_transaction;
    let _ = SqliteStore::enforce_retention;
}
