#![forbid(unsafe_code)]

mod adapters;
mod cache;
mod config;
mod management;
mod ota;
mod platform_ui;
mod readiness;
mod runtime;

pub use adapters::{
    PlatformCommandResponse, PlatformCommandTransport, PlatformCoreFacade,
    PlatformDeviceAuthorization, PlatformDeviceClaimCode,
};
pub use cache::{CacheEntry, CacheError, PersistentCache};
pub use config::{
    ConfigError, MonolithConfig, RETIRED_ENVIRONMENT_NAMES, validate_retired_environment,
};
pub use management::{BootstrapSystemError, ManagementSessionRouter, bootstrap_system};
pub use platform_ui::{
    LoginLayout, PlatformLoginPage, PlatformUiIdentity, PlatformUiRenderer,
    SystemInfrastructureLayout, SystemInfrastructurePage, SystemInfrastructureStatusRow,
    SystemLayout, SystemPlatformPage, SystemTenantRow, TenantAlertRow, TenantAlertsLayout,
    TenantAlertsPage, TenantApplicationRow, TenantApplicationsLayout, TenantApplicationsPage,
    TenantAssetProfilesLayout, TenantAssetProfilesPage, TenantAssetRow, TenantAssetsLayout,
    TenantAssetsPage, TenantAuditLayout, TenantAuditPage, TenantAuditRow,
    TenantDeviceClaimPolicyLayout, TenantDeviceClaimPolicyPage, TenantDeviceCredentialLayout,
    TenantDeviceCredentialPage, TenantDeviceProfilesLayout, TenantDeviceProfilesPage,
    TenantDeviceRow, TenantDevicesLayout, TenantDevicesPage, TenantGroupMemberRow, TenantGroupRow,
    TenantGroupsLayout, TenantGroupsPage, TenantLayout, TenantOtaArtifactRow, TenantOtaLayout,
    TenantOtaPage, TenantOtaProfileRow, TenantOverviewPage, TenantPermissionRow,
    TenantPermissionsLayout, TenantPermissionsPage, TenantProfileLayout, TenantProfilePage,
    TenantProfileRow, TenantRelationRow, TenantRelationsLayout, TenantRelationsPage,
    TenantSelectOption, TenantTopologyLayout, TenantTopologyPage, TenantTopologyRow, TenantUserRow,
    TenantUsersLayout, TenantUsersPage, UserAssetDetailLayout, UserAssetDetailPage,
    UserAssetLayout, UserAssetListPage, UserAssetRow, UserAssetUnavailableLayout,
    UserDeviceAlertRow, UserDeviceDetailPage, UserDeviceLayout, UserDeviceListPage, UserDeviceRow,
    UserDeviceTelemetryRow, UserDeviceUnavailableLayout, UserInvitationPage, UserInvitationRow,
    UserInvitationsLayout, UserLayout, UserResourcePermissionRow,
};
pub use readiness::Readiness;
pub use runtime::{MonolithRuntime, ShutdownError, StartupError};
