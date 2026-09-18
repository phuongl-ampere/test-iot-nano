#![forbid(unsafe_code)]

mod adapters;
mod cache;
mod config;
mod management;
mod platform_ui;
mod readiness;
mod runtime;

pub use adapters::{
    PlatformCommandResponse, PlatformCommandTransport, PlatformCoreFacade,
    PlatformDeviceAuthorization,
};
pub use cache::{CacheEntry, CacheError, PersistentCache};
pub use config::{
    ConfigError, MonolithConfig, RETIRED_ENVIRONMENT_NAMES, validate_retired_environment,
};
pub use management::{BootstrapSystemError, ManagementSessionRouter, bootstrap_system};
pub use platform_ui::{
    LoginLayout, PlatformLoginPage, PlatformUiIdentity, PlatformUiRenderer, SystemLayout,
    SystemPlatformPage, SystemTenantRow, TenantApplicationRow, TenantApplicationsLayout,
    TenantApplicationsPage, TenantAssetProfilesLayout, TenantAssetProfilesPage, TenantAssetRow,
    TenantAssetsLayout, TenantAssetsPage, TenantDeviceCredentialLayout, TenantDeviceCredentialPage,
    TenantDeviceProfilesLayout, TenantDeviceProfilesPage, TenantDeviceRow, TenantDeviceTokenRow,
    TenantDeviceTokensLayout, TenantDeviceTokensPage, TenantDevicesLayout, TenantDevicesPage,
    TenantGroupMemberRow, TenantGroupRow, TenantGroupsLayout, TenantGroupsPage, TenantLayout,
    TenantPermissionRow, TenantPermissionsLayout, TenantPermissionsPage, TenantProfileRow,
    TenantRelationRow, TenantRelationsLayout, TenantRelationsPage, TenantSelectOption,
    TenantTopologyLayout, TenantTopologyPage, TenantTopologyRow, TenantUserRow, TenantUsersLayout,
    TenantUsersPage, UserAssetDetailLayout, UserAssetDetailPage, UserAssetLayout,
    UserAssetListPage, UserAssetRow, UserAssetUnavailableLayout, UserDeviceAlertRow,
    UserDeviceDetailPage, UserDeviceLayout, UserDeviceListPage, UserDeviceRow,
    UserDeviceTelemetryRow, UserDeviceUnavailableLayout, UserLayout,
};
pub use readiness::Readiness;
pub use runtime::{MonolithRuntime, ShutdownError, StartupError};
