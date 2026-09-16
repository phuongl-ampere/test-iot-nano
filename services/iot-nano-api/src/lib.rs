#![forbid(unsafe_code)]

mod application_registry;
mod auth;
mod core_facade;
mod device_tokens;
mod oauth;
mod public_v1;
mod token_vault;

pub use auth::{
    AccountClass, AuthError, AuthenticatedUser, BearerAccessToken, BearerAccessTokenError,
    POWER_MONITOR_APP, Role, authenticate_credentials, authenticate_credentials_sqlite,
    bootstrap_users_sqlite, extract_bearer_access_token, generate_session_id, hash_password,
    validate_bearer_access_token, validate_password,
};
pub use core_facade::{
    CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest, CoreFacade,
    CoreFacadeError, CoreTelemetryBucket, CoreTelemetryPoint, CoreTelemetryQuery,
};
pub use device_tokens::{
    DeviceTokenResponse, DeviceTokenStoreError, create_platform_device_token,
    provision_platform_device_token,
};
pub use oauth::{
    OAuthBrowserSessionVerifier, public_oauth_router,
    public_oauth_router_with_browser_session_verifier,
};
pub use public_v1::public_v1_router;
pub use token_vault::TokenVault;
