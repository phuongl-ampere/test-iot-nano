#![forbid(unsafe_code)]

mod auth;
mod core_client;
mod core_facade;
mod device_tokens;
mod power_switcher;
mod powermonitor;
mod resource_authorization;
mod routes;
mod storage;
mod system_config;
mod token_vault;

pub use auth::{
    AccountClass, AuthError, Role, bootstrap_users, bootstrap_users_sqlite, validate_password,
};
pub use core_client::{
    CoreClient, CoreClientError, CoreCommandCreateRequest, CoreCommandRecord,
    CoreCommandResponseRequest, CoreTelemetryBucket, CoreTelemetryPoint,
};
pub use core_facade::{CoreFacade, CoreFacadeError, CoreTelemetryQuery};
pub use power_switcher::{
    POWER_SWITCHER_PROFILE_NAME, bootstrap_power_switcher_profile,
    bootstrap_power_switcher_profile_sqlite,
};
pub use routes::{
    ApiState, MqttdDeviceTransportSessionRevocation, MqttdDeviceTransportSessionRevoker,
    MqttdDeviceTransportSessionRevokerError, SqliteApiState, router, sqlite_router,
};
pub use storage::{ApiSqliteStore, connect_api_database, migrate_api};
pub use system_config::{
    HelperSystemConfigurationService, SystemConfigurationService, SystemConfigurationServiceError,
};
pub use token_vault::TokenVault;
