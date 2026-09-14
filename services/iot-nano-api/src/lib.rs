#![forbid(unsafe_code)]

mod application_registry;
mod auth;
mod core_client;
mod core_facade;
mod device_tokens;
mod oauth;
mod power_switcher;
mod powermonitor;
mod public_v1;
mod resource_authorization;
mod routes;
mod storage;
mod system_config;
mod token_vault;

pub use auth::{
    AccountClass, AuthError, BearerAccessToken, BearerAccessTokenError, Role, bootstrap_users,
    bootstrap_users_sqlite, extract_bearer_access_token, validate_bearer_access_token,
    validate_password,
};
pub use core_client::{
    CoreClient, CoreClientError, CoreCommandCreateRequest, CoreCommandRecord,
    CoreCommandResponseRequest, CoreTelemetryBucket, CoreTelemetryPoint,
};
pub use core_facade::{CoreFacade, CoreFacadeError, CoreTelemetryQuery};
pub use oauth::public_oauth_router;
pub use power_switcher::{
    POWER_SWITCHER_PROFILE_NAME, bootstrap_power_switcher_profile,
    bootstrap_power_switcher_profile_sqlite,
};
pub use public_v1::public_v1_router;
pub use routes::{
    ApiRouters, ApiState, MqttdDeviceTransportSessionRevocation,
    MqttdDeviceTransportSessionRevoker, MqttdDeviceTransportSessionRevokerError, SqliteApiRouters,
    SqliteApiState, router, routers, sqlite_router, sqlite_routers,
};
pub use storage::{ApiSqliteStore, connect_api_database, migrate_api};
pub use system_config::{
    HelperSystemConfigurationService, SystemConfigurationService, SystemConfigurationServiceError,
};
pub use token_vault::TokenVault;
