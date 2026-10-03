mod alerts;
mod assets;
pub(super) mod devices;
mod ota;
mod personal_access_tokens;
mod profiles;
pub(super) mod tokens;
mod users;

pub use alerts::{
    CreateManagementAlertRule, MANAGEMENT_ALERT_INCIDENT_LIST_LIMIT, MANAGEMENT_ALERT_LIST_LIMIT,
    MANAGEMENT_ALERT_RULE_LIST_LIMIT, ManagementAlert, ManagementAlertError,
    ManagementAlertIncident, ManagementAlertIncidentError, ManagementAlertIncidentRepository,
    ManagementAlertRepository, ManagementAlertRule, ManagementAlertRuleError,
    ManagementAlertRuleRepository, UpdateManagementAlertRule,
};

pub use assets::{
    CreateManagementAsset, ManagementAsset, ManagementAssetError, ManagementAssetRepository,
    UpdateManagementAsset,
};

pub use devices::{
    MANAGEMENT_DEVICE_TELEMETRY_LIMIT, ManagementChildStatus, ManagementDevice,
    ManagementDeviceError, ManagementDeviceHealth, ManagementDeviceRepository,
    ManagementDeviceTelemetry, ManagementDeviceTelemetryRepository, ManagementDeviceTopology,
    ManagementGatewayStatus, ProvisionManagementDevice, ProvisionManagementDeviceError,
    UpdateManagementDevice,
};

pub use ota::{OtaArtifact, OtaPolicy};

pub use personal_access_tokens::{
    NewTenantPersonalAccessToken, TenantPersonalAccessTokenRecord,
    TenantPersonalAccessTokenRepository, TenantPersonalAccessTokenRepositoryError,
};

pub use profiles::{
    CreateManagementAssetProfile, CreateManagementDeviceProfile, ManagementAssetProfile,
    ManagementAssetProfileError, ManagementAssetProfileRepository, ManagementDeviceProfile,
    ManagementDeviceProfileError, ManagementDeviceProfileRepository, UpdateManagementAssetProfile,
    UpdateManagementDeviceProfile,
};

pub use tokens::{
    DeviceTokenRecord, DeviceTokenRepository, DeviceTokenRepositoryError, DeviceTokenSecret,
    NewDeviceToken, NewOwnedDeviceToken,
};

pub use users::{
    CreateManagementUser, ManagementUser, ManagementUserError, ManagementUserRepository,
    ManagementUserRole, UpdateManagementUser, UserCapability,
};
