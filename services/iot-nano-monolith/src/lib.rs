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
    PlatformUiIdentity, PlatformUiRenderer, SystemLayout, SystemPlatformPage, SystemTenantRow,
    TenantGroupMemberRow, TenantGroupRow, TenantGroupsLayout, TenantGroupsPage, TenantLayout,
    TenantPermissionRow, TenantPermissionsLayout, TenantPermissionsPage, TenantSelectOption,
    UserAssetDetailLayout, UserAssetDetailPage, UserAssetLayout, UserAssetListPage, UserAssetRow,
    UserAssetUnavailableLayout, UserDeviceDetailPage, UserDeviceLayout, UserDeviceListPage,
    UserDeviceRow, UserDeviceUnavailableLayout, UserLayout,
};
pub use readiness::Readiness;
pub use runtime::{MonolithRuntime, ShutdownError, StartupError};
