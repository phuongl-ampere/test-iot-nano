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
    SystemPlatformPage, SystemTenantRow, TenantAssetRow, TenantAssetsLayout, TenantAssetsPage,
    TenantDeviceCredentialLayout, TenantDeviceCredentialPage, TenantDeviceRow, TenantDevicesLayout,
    TenantDevicesPage, TenantGroupMemberRow, TenantGroupRow, TenantGroupsLayout, TenantGroupsPage,
    TenantLayout, TenantPermissionRow, TenantPermissionsLayout, TenantPermissionsPage,
    TenantSelectOption, TenantTopologyLayout, TenantTopologyPage, TenantTopologyRow, TenantUserRow,
    TenantUsersLayout, TenantUsersPage, UserAssetDetailLayout, UserAssetDetailPage,
    UserAssetLayout, UserAssetListPage, UserAssetRow, UserAssetUnavailableLayout,
    UserDeviceDetailPage, UserDeviceLayout, UserDeviceListPage, UserDeviceRow,
    UserDeviceUnavailableLayout, UserLayout,
};
pub use readiness::Readiness;
pub use runtime::{MonolithRuntime, ShutdownError, StartupError};
