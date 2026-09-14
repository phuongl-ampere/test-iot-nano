#![forbid(unsafe_code)]

mod adapters;
mod cache;
mod config;
mod management;
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
pub use management::{BootstrapAdminError, ManagementSessionRouter, bootstrap_admin};
pub use readiness::Readiness;
pub use runtime::{MonolithRuntime, ShutdownError, StartupError};
