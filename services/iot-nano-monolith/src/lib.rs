#![forbid(unsafe_code)]

mod config;

pub use config::{
    ConfigError, MonolithConfig, RETIRED_ENVIRONMENT_NAMES, validate_retired_environment,
};
