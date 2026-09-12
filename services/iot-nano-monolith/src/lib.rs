#![forbid(unsafe_code)]

mod adapters;
mod cache;
mod config;
mod readiness;
mod runtime;

pub use adapters::PlatformDeviceAuthorization;
pub use cache::{CacheEntry, CacheError, PersistentCache};
pub use config::{
    ConfigError, MonolithConfig, RETIRED_ENVIRONMENT_NAMES, validate_retired_environment,
};
pub use readiness::Readiness;
pub use runtime::{MonolithRuntime, ShutdownError, StartupError};
