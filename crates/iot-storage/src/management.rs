use std::{collections::BTreeSet, future::Future, pin::Pin};

use chrono::{DateTime, Duration, Utc};
use sqlx::{
    Postgres, Row, Sqlite, Transaction, error::DatabaseError, postgres::PgRow, sqlite::SqliteRow,
    types::Json,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AccountClass, AuditAction, AuditPrincipal, AuditTargetType, PlatformStore, PlatformStoreError,
    audit,
};

pub const BUILT_IN_USER_WORKSPACE: &str = "/app";
pub const MANAGEMENT_ALERT_LIST_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAlert {
    pub id: Uuid,
    pub rule_name: String,
    pub severity: String,
    pub device_id: String,
    pub status: String,
    pub last_value: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ManagementAlertError {
    #[error("stored management alert ID is invalid")]
    InvalidStoredAlertId,
    #[error("stored management alert timestamp is invalid")]
    InvalidStoredAlertTimestamp,
    #[error("management alert storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAlertError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAlertError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAlertRepository: Send + Sync {
    fn list_management_alerts<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAlert>, ManagementAlertError>> + Send + 'a>>;
}

impl ManagementAlertRepository for PlatformStore {
    fn list_management_alerts<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAlert>, ManagementAlertError>> + Send + 'a>>
    {
        Box::pin(async move { list_management_alerts(self, tenant_id).await })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementUserRole {
    Admin,
    Viewer,
}

impl ManagementUserRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Viewer => "viewer",
        }
    }

    fn from_database(value: &str) -> Result<Self, ManagementUserError> {
        match value {
            "admin" => Ok(Self::Admin),
            "viewer" => Ok(Self::Viewer),
            _ => Err(ManagementUserError::InvalidStoredRole(value.to_owned())),
        }
    }
}

fn management_user_account_class_for_role(role: ManagementUserRole) -> AccountClass {
    match role {
        ManagementUserRole::Admin => AccountClass::Admin,
        ManagementUserRole::Viewer => AccountClass::User,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementUser {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub username: String,
    pub role: ManagementUserRole,
    pub account_class: AccountClass,
    pub default_app: String,
    pub granted_apps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateManagementUser {
    pub tenant_id: Uuid,
    pub username: String,
    pub password_hash: String,
    pub default_app: String,
    pub granted_apps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateManagementUser {
    pub default_app: String,
    pub granted_apps: Vec<String>,
    pub role: Option<ManagementUserRole>,
}

#[derive(Debug, Error)]
pub enum ManagementUserError {
    #[error("invalid management username: {0:?}")]
    InvalidUsername(String),
    #[error("management user password hash must not be empty")]
    EmptyPasswordHash,
    #[error("invalid management default app: {0:?}")]
    InvalidDefaultApp(String),
    #[error("invalid management granted apps")]
    InvalidGrantedApps,
    #[error("management username already exists: {0:?}")]
    UsernameConflict(String),
    #[error("management user was not found")]
    UserNotFound,
    #[error("system management users cannot be changed")]
    SystemUserImmutable,
    #[error("at least one administrator must remain")]
    LastAdministrator,
    #[error("stored management user ID is invalid")]
    InvalidStoredUserId,
    #[error("stored management user role is invalid: {0:?}")]
    InvalidStoredRole(String),
    #[error("stored management user account class is invalid: {0:?}")]
    InvalidStoredAccountClass(String),
    #[error("management user storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementUserError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementUserError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementUserRepository: Send + Sync {
    fn list_management_users<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementUser>, ManagementUserError>> + Send + 'a>>;
    fn create_management_user<'a>(
        &'a self,
        user: CreateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>;
    fn update_management_user<'a>(
        &'a self,
        tenant_id: Uuid,
        username: &'a str,
        user: UpdateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>;
}

impl ManagementUserRepository for PlatformStore {
    fn list_management_users<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementUser>, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move { list_management_users(self, tenant_id).await })
    }

    fn create_management_user<'a>(
        &'a self,
        user: CreateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move { create_management_user(self, user).await })
    }

    fn update_management_user<'a>(
        &'a self,
        tenant_id: Uuid,
        username: &'a str,
        user: UpdateManagementUser,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementUser, ManagementUserError>> + Send + 'a>>
    {
        Box::pin(async move { update_management_user(self, tenant_id, username, user).await })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementDeviceProfile {
    pub id: Uuid,
    pub name: String,
    pub telemetry_schema: serde_json::Value,
    pub metric_mapping: serde_json::Value,
    pub reporting_settings: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementDeviceProfile {
    pub name: String,
    pub telemetry_schema: serde_json::Value,
    pub metric_mapping: serde_json::Value,
    pub reporting_settings: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementDeviceProfile {
    pub name: String,
    pub telemetry_schema: serde_json::Value,
    pub metric_mapping: serde_json::Value,
    pub reporting_settings: serde_json::Value,
}

#[derive(Debug, Error)]
pub enum ManagementDeviceProfileError {
    #[error("invalid device profile name")]
    InvalidName,
    #[error("device profile telemetry schema must be an object")]
    TelemetrySchemaMustBeObject,
    #[error("device profile metric mapping must be an object")]
    MetricMappingMustBeObject,
    #[error("device profile reporting settings must be an object")]
    ReportingSettingsMustBeObject,
    #[error("device profile name already exists: {0:?}")]
    NameConflict(String),
    #[error("management device profile was not found")]
    DeviceProfileNotFound,
    #[error("device profile is still referenced: {0}")]
    DeviceProfileInUse(Uuid),
    #[error("stored device profile is invalid")]
    InvalidStoredProfile,
    #[error("management device profile storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementDeviceProfileError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementDeviceProfileError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementDeviceProfileRepository: Send + Sync {
    fn list_management_device_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementDeviceProfile>, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn create_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn update_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn delete_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceProfileError>> + Send + 'a>>;
}

impl ManagementDeviceProfileRepository for PlatformStore {
    fn list_management_device_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementDeviceProfile>, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_management_device_profiles(self, tenant_id).await })
    }

    fn create_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { create_management_device_profile(self, tenant_id, profile).await })
    }

    fn update_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementDeviceProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementDeviceProfile, ManagementDeviceProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            update_management_device_profile(self, tenant_id, profile_id, profile).await
        })
    }

    fn delete_management_device_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceProfileError>> + Send + 'a>> {
        Box::pin(async move { delete_management_device_profile(self, tenant_id, profile_id).await })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAssetProfile {
    pub id: Uuid,
    pub name: String,
    pub fields: serde_json::Value,
    pub dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementAssetProfile {
    pub name: String,
    pub fields: serde_json::Value,
    pub dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementAssetProfile {
    pub name: String,
    pub fields: serde_json::Value,
    pub dashboard_defaults: serde_json::Value,
}

#[derive(Debug, Error)]
pub enum ManagementAssetProfileError {
    #[error("invalid asset profile name")]
    InvalidName,
    #[error("asset profile fields must be an object")]
    FieldsMustBeObject,
    #[error("asset profile dashboard defaults must be an object")]
    DashboardDefaultsMustBeObject,
    #[error("asset profile name already exists: {0:?}")]
    NameConflict(String),
    #[error("management asset profile was not found")]
    AssetProfileNotFound,
    #[error("asset profile is still referenced: {0}")]
    AssetProfileInUse(Uuid),
    #[error("stored asset profile is invalid")]
    InvalidStoredProfile,
    #[error("management asset profile storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAssetProfileError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAssetProfileError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAssetProfileRepository: Send + Sync {
    fn list_management_asset_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAssetProfile>, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn create_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn update_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn delete_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetProfileError>> + Send + 'a>>;
}

impl ManagementAssetProfileRepository for PlatformStore {
    fn list_management_asset_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAssetProfile>, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_management_asset_profiles(self, tenant_id).await })
    }

    fn create_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { create_management_asset_profile(self, tenant_id, profile).await })
    }

    fn update_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
        profile: UpdateManagementAssetProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAssetProfile, ManagementAssetProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            update_management_asset_profile(self, tenant_id, profile_id, profile).await
        })
    }

    fn delete_management_asset_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetProfileError>> + Send + 'a>> {
        Box::pin(async move { delete_management_asset_profile(self, tenant_id, profile_id).await })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementDevice {
    pub device_id: String,
    pub display_name: Option<String>,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub attributes: serde_json::Value,
    pub topology: ManagementDeviceTopology,
    pub health: ManagementDeviceHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementDeviceTopology {
    pub is_gateway: bool,
    pub gateway_device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementDeviceHealth {
    pub online: bool,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub gateway_status: Option<ManagementGatewayStatus>,
    pub child_status: Option<ManagementChildStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementGatewayStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementChildStatus {
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementDevice {
    pub display_name: String,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub attributes: Option<serde_json::Value>,
    pub topology: Option<ManagementDeviceTopology>,
}

#[derive(Debug, Error)]
pub enum ManagementDeviceError {
    #[error("invalid device ID: {0:?}")]
    InvalidDeviceId(String),
    #[error("invalid device display name")]
    InvalidDisplayName,
    #[error("device attributes must be an object")]
    AttributesMustBeObject,
    #[error("management device was not found")]
    DeviceNotFound,
    #[error("a gateway cannot be assigned to another gateway")]
    GatewayCannotHaveParent,
    #[error("a device cannot be its own gateway")]
    DeviceCannotBeOwnGateway,
    #[error("a gateway with assigned children cannot be changed or deleted")]
    GatewayHasChildren,
    #[error("assigned gateway device is unavailable")]
    GatewayUnavailable,
    #[error("assigned gateway device is not a gateway")]
    GatewayIsNotGateway,
    #[error("assigned asset is unavailable: {0}")]
    AssetUnavailable(Uuid),
    #[error("assigned device profile is unavailable: {0}")]
    DeviceProfileUnavailable(Uuid),
    #[error("stored device attributes are invalid")]
    InvalidStoredAttributes,
    #[error("stored device timestamp is invalid")]
    InvalidStoredTimestamp,
    #[error("management device storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementDeviceError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementDeviceError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAsset {
    pub id: Uuid,
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub attributes: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementAsset {
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementAsset {
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Error)]
pub enum ManagementAssetError {
    #[error("invalid asset name")]
    InvalidName,
    #[error("asset metadata must be an object")]
    MetadataMustBeObject,
    #[error("asset attributes must be an object")]
    AttributesMustBeObject,
    #[error("management asset was not found")]
    AssetNotFound,
    #[error("asset profile is unavailable: {0}")]
    AssetProfileUnavailable(Uuid),
    #[error("parent asset is unavailable: {0}")]
    ParentAssetUnavailable(Uuid),
    #[error("an asset cannot be its own parent")]
    AssetCannotBeOwnParent,
    #[error("an asset cannot have a descendant as its parent")]
    AssetCannotHaveDescendantParent,
    #[error("an asset named {name:?} already exists under the same parent")]
    SiblingNameConflict {
        name: String,
        parent_asset_id: Option<Uuid>,
    },
    #[error("stored management asset ID is invalid")]
    InvalidStoredAssetId,
    #[error("stored management asset references are invalid")]
    InvalidStoredReferences,
    #[error("stored management asset metadata is invalid")]
    InvalidStoredMetadata,
    #[error("management asset storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAssetError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAssetError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAssetRepository: Send + Sync {
    fn list_management_assets<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAsset>, ManagementAssetError>> + Send + 'a>>;
    fn create_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset: CreateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>;
    fn update_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
        asset: UpdateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>;
    fn delete_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetError>> + Send + 'a>>;
}

impl ManagementAssetRepository for PlatformStore {
    fn list_management_assets<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAsset>, ManagementAssetError>> + Send + 'a>>
    {
        Box::pin(async move { list_management_assets(self, tenant_id).await })
    }

    fn create_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset: CreateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>
    {
        Box::pin(async move { create_management_asset(self, tenant_id, actor, asset).await })
    }

    fn update_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
        asset: UpdateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>
    {
        Box::pin(
            async move { update_management_asset(self, tenant_id, actor, asset_id, asset).await },
        )
    }

    fn delete_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetError>> + Send + 'a>> {
        Box::pin(async move { delete_management_asset(self, tenant_id, actor, asset_id).await })
    }
}

pub trait ManagementDeviceRepository: Send + Sync {
    fn list_management_devices<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ManagementDevice>, ManagementDeviceError>> + Send + 'a>,
    >;
    fn update_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
        update: UpdateManagementDevice,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementDevice, ManagementDeviceError>> + Send + 'a>>;
    fn delete_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceError>> + Send + 'a>>;
}

impl ManagementDeviceRepository for PlatformStore {
    fn list_management_devices<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<ManagementDevice>, ManagementDeviceError>> + Send + 'a>,
    > {
        Box::pin(async move { list_management_devices(self, tenant_id).await })
    }

    fn update_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
        update: UpdateManagementDevice,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementDevice, ManagementDeviceError>> + Send + 'a>>
    {
        Box::pin(async move {
            update_management_device(self, tenant_id, actor, device_id, update).await
        })
    }

    fn delete_management_device<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementDeviceError>> + Send + 'a>> {
        Box::pin(async move { delete_management_device(self, tenant_id, actor, device_id).await })
    }
}

async fn list_management_users(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementUser>, ManagementUserError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, tenant_id, username, role, account_class, default_app
                 FROM users
                 WHERE tenant_id = ?
                 ORDER BY username, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            let mut users = Vec::with_capacity(rows.len());
            for row in rows {
                users.push(sqlite_management_user_from_row(store.pool(), row).await?);
            }
            Ok(users)
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, tenant_id, username, role, account_class, default_app
                 FROM users
                 WHERE tenant_id = $1
                 ORDER BY username, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            let mut users = Vec::with_capacity(rows.len());
            for row in rows {
                users.push(timescale_management_user_from_row(pool, row).await?);
            }
            Ok(users)
        }
    }
}

async fn list_management_alerts(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAlert>, ManagementAlertError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT incidents.id, rules.name AS rule_name, rules.severity, incidents.device_id,
                        incidents.status, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.tenant_id = ?
                 ORDER BY julianday(incidents.updated_at) DESC, incidents.id DESC
                 LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(MANAGEMENT_ALERT_LIST_LIMIT as i64)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_alert_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT incidents.id, rules.name AS rule_name, rules.severity, incidents.device_id,
                        incidents.status, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.tenant_id = $1
                 ORDER BY incidents.updated_at DESC, incidents.id DESC
                 LIMIT $2",
            )
            .bind(tenant_id)
            .bind(MANAGEMENT_ALERT_LIST_LIMIT as i64)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_alert_from_row)
                .collect()
        }
    }
}

fn sqlite_management_alert_from_row(
    row: SqliteRow,
) -> Result<ManagementAlert, ManagementAlertError> {
    let id: String = row.try_get("id")?;
    let updated_at: String = row.try_get("updated_at")?;
    Ok(ManagementAlert {
        id: Uuid::parse_str(&id).map_err(|_| ManagementAlertError::InvalidStoredAlertId)?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        last_value: row.try_get("last_value")?,
        updated_at: DateTime::parse_from_rfc3339(&updated_at)
            .map(|value| value.with_timezone(&Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(&updated_at, "%Y-%m-%d %H:%M:%S")
                    .map(|value| value.and_utc())
            })
            .map_err(|_| ManagementAlertError::InvalidStoredAlertTimestamp)?,
    })
}

fn timescale_management_alert_from_row(
    row: PgRow,
) -> Result<ManagementAlert, ManagementAlertError> {
    Ok(ManagementAlert {
        id: row.try_get("id")?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        last_value: row.try_get("last_value")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn create_management_user(
    store: &PlatformStore,
    user: CreateManagementUser,
) -> Result<ManagementUser, ManagementUserError> {
    let user = validate_new_management_user(user)?;
    let user_id = Uuid::now_v7();
    let tenant_id = user.tenant_id;
    let username = user.username.clone();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            sqlite_validate_management_user_apps(&mut transaction, tenant_id, &user.granted_apps)
                .await?;
            sqlx::query(
                "INSERT INTO users (
                    id, tenant_id, username, password_hash, role, account_class, default_app, updated_at
                 ) VALUES (?, ?, ?, ?, 'viewer', 'user', ?, ?)",
            )
            .bind(user_id.to_string())
            .bind(tenant_id.to_string())
            .bind(&user.username)
            .bind(user.password_hash)
            .bind(user.default_app)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *transaction)
            .await
            .map_err(|error| map_management_username_conflict(error, &username))?;
            for app_key in user.granted_apps {
                sqlx::query(
                    "INSERT INTO user_app_grants (user_id, tenant_id, app_key) VALUES (?, ?, ?)",
                )
                .bind(user_id.to_string())
                .bind(tenant_id.to_string())
                .bind(app_key)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            timescale_validate_management_user_apps(
                &mut transaction,
                tenant_id,
                &user.granted_apps,
            )
            .await?;
            sqlx::query(
                "INSERT INTO users (
                    id, tenant_id, username, password_hash, role, account_class, default_app
                 ) VALUES ($1, $2, $3, $4, 'viewer', 'user', $5)",
            )
            .bind(user_id)
            .bind(tenant_id)
            .bind(&user.username)
            .bind(user.password_hash)
            .bind(user.default_app)
            .execute(&mut *transaction)
            .await
            .map_err(|error| map_management_username_conflict(error, &username))?;
            for app_key in user.granted_apps {
                sqlx::query(
                    "INSERT INTO user_app_grants (user_id, tenant_id, app_key) VALUES ($1, $2, $3)",
                )
                .bind(user_id)
                .bind(tenant_id)
                .bind(app_key)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
    }
    management_user(store, tenant_id, user_id).await
}

async fn update_management_user(
    store: &PlatformStore,
    tenant_id: Uuid,
    username: &str,
    user: UpdateManagementUser,
) -> Result<ManagementUser, ManagementUserError> {
    if !management_identifier(username) {
        return Err(ManagementUserError::InvalidUsername(username.to_owned()));
    }
    validate_management_user_apps(&user.default_app, &user.granted_apps)?;
    let user_id = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let (user_id, current_role, account_class) =
                sqlite_management_user_mutation_target(&mut transaction, tenant_id, username)
                    .await?;
            protect_management_user_invariants(
                &mut transaction,
                tenant_id,
                username,
                current_role,
                account_class,
                user.role,
            )
            .await?;
            sqlite_validate_management_user_apps(&mut transaction, tenant_id, &user.granted_apps)
                .await?;
            sqlx::query(
                "UPDATE users
                 SET default_app = ?, role = COALESCE(?, role),
                     account_class = COALESCE(?, account_class), updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(&user.default_app)
            .bind(user.role.map(ManagementUserRole::as_str))
            .bind(
                user.role
                    .map(management_user_account_class_for_role)
                    .map(AccountClass::as_str),
            )
            .bind(Utc::now().to_rfc3339())
            .bind(user_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM user_app_grants WHERE user_id = ? AND tenant_id = ?")
                .bind(user_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            for app_key in &user.granted_apps {
                sqlx::query(
                    "INSERT INTO user_app_grants (user_id, tenant_id, app_key)
                     VALUES (?, ?, ?)
                     ON CONFLICT(user_id, app_key) DO NOTHING",
                )
                .bind(user_id.to_string())
                .bind(tenant_id.to_string())
                .bind(app_key)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
            user_id
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "SELECT pg_advisory_xact_lock(hashtext('iot_nano:management-users-admin-role'))",
            )
            .execute(&mut *transaction)
            .await?;
            let (user_id, current_role, account_class) =
                timescale_management_user_mutation_target(&mut transaction, tenant_id, username)
                    .await?;
            protect_timescale_management_user_invariants(
                &mut transaction,
                tenant_id,
                username,
                current_role,
                account_class,
                user.role,
            )
            .await?;
            timescale_validate_management_user_apps(
                &mut transaction,
                tenant_id,
                &user.granted_apps,
            )
            .await?;
            sqlx::query(
                "UPDATE users
                 SET default_app = $2, role = COALESCE($3, role),
                     account_class = COALESCE($4, account_class), updated_at = now()
                 WHERE id = $1 AND tenant_id = $5",
            )
            .bind(user_id)
            .bind(&user.default_app)
            .bind(user.role.map(ManagementUserRole::as_str))
            .bind(
                user.role
                    .map(management_user_account_class_for_role)
                    .map(AccountClass::as_str),
            )
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM user_app_grants WHERE user_id = $1 AND tenant_id = $2")
                .bind(user_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            for app_key in &user.granted_apps {
                sqlx::query(
                    "INSERT INTO user_app_grants (user_id, tenant_id, app_key)
                     VALUES ($1, $2, $3)
                     ON CONFLICT(user_id, app_key) DO NOTHING",
                )
                .bind(user_id)
                .bind(tenant_id)
                .bind(app_key)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
            user_id
        }
    };
    management_user(store, tenant_id, user_id).await
}

async fn sqlite_validate_management_user_apps(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    granted_apps: &[String],
) -> Result<(), ManagementUserError> {
    for app_key in granted_apps {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM applications WHERE app_id = ? AND tenant_id = ?",
        )
        .bind(app_key)
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !exists {
            return Err(ManagementUserError::InvalidGrantedApps);
        }
    }
    Ok(())
}

async fn timescale_validate_management_user_apps(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    granted_apps: &[String],
) -> Result<(), ManagementUserError> {
    for app_key in granted_apps {
        let exists = sqlx::query_scalar::<_, String>(
            "SELECT app_id
             FROM applications
             WHERE app_id = $1 AND tenant_id = $2
             FOR KEY SHARE",
        )
        .bind(app_key)
        .bind(tenant_id)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !exists {
            return Err(ManagementUserError::InvalidGrantedApps);
        }
    }
    Ok(())
}

async fn sqlite_management_user_mutation_target(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    username: &str,
) -> Result<(Uuid, ManagementUserRole, AccountClass), ManagementUserError> {
    let row = sqlx::query(
        "SELECT id, role, account_class
         FROM users
         WHERE username = ? AND tenant_id = ?",
    )
    .bind(username)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementUserError::UserNotFound)?;
    let user_id = Uuid::parse_str(&row.try_get::<String, _>("id")?)
        .map_err(|_| ManagementUserError::InvalidStoredUserId)?;
    Ok((
        user_id,
        ManagementUserRole::from_database(&row.try_get::<String, _>("role")?)?,
        management_user_account_class(row.try_get::<String, _>("account_class")?)?,
    ))
}

async fn timescale_management_user_mutation_target(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    username: &str,
) -> Result<(Uuid, ManagementUserRole, AccountClass), ManagementUserError> {
    let row = sqlx::query(
        "SELECT id, role, account_class
         FROM users
         WHERE username = $1 AND tenant_id = $2",
    )
    .bind(username)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementUserError::UserNotFound)?;
    Ok((
        row.try_get("id")?,
        ManagementUserRole::from_database(&row.try_get::<String, _>("role")?)?,
        management_user_account_class(row.try_get::<String, _>("account_class")?)?,
    ))
}

async fn protect_management_user_invariants(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    username: &str,
    current_role: ManagementUserRole,
    account_class: AccountClass,
    next_role: Option<ManagementUserRole>,
) -> Result<(), ManagementUserError> {
    if account_class == AccountClass::System {
        return Err(ManagementUserError::SystemUserImmutable);
    }
    if current_role == ManagementUserRole::Admin && next_role == Some(ManagementUserRole::Viewer) {
        let remaining_admins: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users
                 WHERE tenant_id = ? AND role = 'admin' AND username <> ?",
        )
        .bind(tenant_id.to_string())
        .bind(username)
        .fetch_one(&mut **transaction)
        .await?;
        if remaining_admins == 0 {
            return Err(ManagementUserError::LastAdministrator);
        }
    }
    Ok(())
}

async fn protect_timescale_management_user_invariants(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    username: &str,
    current_role: ManagementUserRole,
    account_class: AccountClass,
    next_role: Option<ManagementUserRole>,
) -> Result<(), ManagementUserError> {
    if account_class == AccountClass::System {
        return Err(ManagementUserError::SystemUserImmutable);
    }
    if current_role == ManagementUserRole::Admin && next_role == Some(ManagementUserRole::Viewer) {
        let remaining_admins: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM users
             WHERE tenant_id = $1 AND role = 'admin' AND username <> $2",
        )
        .bind(tenant_id)
        .bind(username)
        .fetch_one(&mut **transaction)
        .await?;
        if remaining_admins == 0 {
            return Err(ManagementUserError::LastAdministrator);
        }
    }
    Ok(())
}

fn validate_new_management_user(
    user: CreateManagementUser,
) -> Result<CreateManagementUser, ManagementUserError> {
    if !management_identifier(&user.username) {
        return Err(ManagementUserError::InvalidUsername(user.username));
    }
    if user.password_hash.trim().is_empty() {
        return Err(ManagementUserError::EmptyPasswordHash);
    }
    validate_management_user_apps(&user.default_app, &user.granted_apps)?;
    Ok(user)
}

fn validate_management_user_apps(
    default_app: &str,
    granted_apps: &[String],
) -> Result<(), ManagementUserError> {
    if default_app == BUILT_IN_USER_WORKSPACE {
        return if granted_apps.is_empty() {
            Ok(())
        } else {
            Err(ManagementUserError::InvalidGrantedApps)
        };
    }
    let Some(default_app_key) = default_app.strip_prefix("/apps/") else {
        return Err(ManagementUserError::InvalidDefaultApp(
            default_app.to_owned(),
        ));
    };
    if !management_identifier(default_app_key) {
        return Err(ManagementUserError::InvalidDefaultApp(
            default_app.to_owned(),
        ));
    }
    if granted_apps.is_empty()
        || granted_apps
            .iter()
            .any(|app_key| !management_identifier(app_key))
        || !granted_apps
            .iter()
            .any(|app_key| app_key == default_app_key)
    {
        return Err(ManagementUserError::InvalidGrantedApps);
    }
    let distinct_apps = granted_apps.iter().collect::<BTreeSet<_>>();
    if distinct_apps.len() != granted_apps.len() {
        return Err(ManagementUserError::InvalidGrantedApps);
    }
    Ok(())
}

fn management_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|character| {
            character.is_ascii_alphanumeric() || character == b'_' || character == b'-'
        })
}

fn map_management_username_conflict(error: sqlx::Error, username: &str) -> ManagementUserError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ManagementUserError::UsernameConflict(username.to_owned())
    } else {
        ManagementUserError::from(error)
    }
}

async fn management_user(
    store: &PlatformStore,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<ManagementUser, ManagementUserError> {
    let users = list_management_users(store, tenant_id).await?;
    users
        .into_iter()
        .find(|user| user.id == user_id)
        .ok_or(ManagementUserError::InvalidStoredUserId)
}

async fn sqlite_management_user_from_row(
    pool: &sqlx::SqlitePool,
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementUser, ManagementUserError> {
    let id = Uuid::parse_str(&row.try_get::<String, _>("id")?)
        .map_err(|_| ManagementUserError::InvalidStoredUserId)?;
    let tenant_id = Uuid::parse_str(&row.try_get::<String, _>("tenant_id")?)
        .map_err(|_| ManagementUserError::InvalidStoredUserId)?;
    let granted_apps = sqlite_management_user_grants(pool, id, tenant_id).await?;
    management_user_from_parts(
        id,
        tenant_id,
        row.try_get("username")?,
        row.try_get("role")?,
        row.try_get("account_class")?,
        row.try_get("default_app")?,
        granted_apps,
    )
}

async fn sqlite_management_user_grants(
    pool: &sqlx::SqlitePool,
    user_id: Uuid,
    tenant_id: Uuid,
) -> Result<Vec<String>, ManagementUserError> {
    sqlx::query_scalar(
        "SELECT app_key
         FROM user_app_grants
         WHERE user_id = ? AND tenant_id = ?
         ORDER BY app_key",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(ManagementUserError::from)
}

async fn timescale_management_user_from_row(
    pool: &sqlx::PgPool,
    row: sqlx::postgres::PgRow,
) -> Result<ManagementUser, ManagementUserError> {
    let id = row.try_get("id")?;
    let tenant_id = row.try_get("tenant_id")?;
    let granted_apps = timescale_management_user_grants(pool, id, tenant_id).await?;
    management_user_from_parts(
        id,
        tenant_id,
        row.try_get("username")?,
        row.try_get("role")?,
        row.try_get("account_class")?,
        row.try_get("default_app")?,
        granted_apps,
    )
}

async fn timescale_management_user_grants(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    tenant_id: Uuid,
) -> Result<Vec<String>, ManagementUserError> {
    sqlx::query_scalar(
        "SELECT app_key
         FROM user_app_grants
         WHERE user_id = $1 AND tenant_id = $2
         ORDER BY app_key",
    )
    .bind(user_id)
    .bind(tenant_id)
    .fetch_all(pool)
    .await
    .map_err(ManagementUserError::from)
}

fn management_user_from_parts(
    id: Uuid,
    tenant_id: Uuid,
    username: String,
    role: String,
    account_class: String,
    default_app: String,
    granted_apps: Vec<String>,
) -> Result<ManagementUser, ManagementUserError> {
    let role = ManagementUserRole::from_database(&role)?;
    let account_class = management_user_account_class(account_class)?;
    Ok(ManagementUser {
        id,
        tenant_id,
        username,
        role,
        account_class,
        default_app,
        granted_apps,
    })
}

fn management_user_account_class(
    account_class: String,
) -> Result<AccountClass, ManagementUserError> {
    let account_class = match account_class.as_str() {
        "system" => AccountClass::System,
        "admin" => AccountClass::Admin,
        "user" => AccountClass::User,
        _ => {
            return Err(ManagementUserError::InvalidStoredAccountClass(
                account_class,
            ));
        }
    };
    Ok(account_class)
}

fn management_profile_name(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value.len() <= 128).then(|| value.to_owned())
}

fn validate_management_device_profile(
    id: Uuid,
    name: String,
    telemetry_schema: serde_json::Value,
    metric_mapping: serde_json::Value,
    reporting_settings: serde_json::Value,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let name = management_profile_name(name).ok_or(ManagementDeviceProfileError::InvalidName)?;
    if !telemetry_schema.is_object() {
        return Err(ManagementDeviceProfileError::TelemetrySchemaMustBeObject);
    }
    if !metric_mapping.is_object() {
        return Err(ManagementDeviceProfileError::MetricMappingMustBeObject);
    }
    if !reporting_settings.is_object() {
        return Err(ManagementDeviceProfileError::ReportingSettingsMustBeObject);
    }
    Ok(ManagementDeviceProfile {
        id,
        name,
        telemetry_schema,
        metric_mapping,
        reporting_settings,
    })
}

fn validate_management_asset_profile(
    id: Uuid,
    name: String,
    fields: serde_json::Value,
    dashboard_defaults: serde_json::Value,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let name = management_profile_name(name).ok_or(ManagementAssetProfileError::InvalidName)?;
    if !fields.is_object() {
        return Err(ManagementAssetProfileError::FieldsMustBeObject);
    }
    if !dashboard_defaults.is_object() {
        return Err(ManagementAssetProfileError::DashboardDefaultsMustBeObject);
    }
    Ok(ManagementAssetProfile {
        id,
        name,
        fields,
        dashboard_defaults,
    })
}

fn map_management_device_profile_conflict(
    error: sqlx::Error,
    name: &str,
) -> ManagementDeviceProfileError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ManagementDeviceProfileError::NameConflict(name.to_owned())
    } else {
        ManagementDeviceProfileError::from(error)
    }
}

fn map_management_asset_profile_conflict(
    error: sqlx::Error,
    name: &str,
) -> ManagementAssetProfileError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        ManagementAssetProfileError::NameConflict(name.to_owned())
    } else {
        ManagementAssetProfileError::from(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDeviceToken {
    pub id: Uuid,
    pub token_prefix: String,
    pub token_hash: String,
    pub token_ciphertext: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOwnedDeviceToken {
    pub display_name: String,
    pub owner_user_id: Uuid,
    pub asset_id: Option<Uuid>,
    pub token: NewDeviceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTokenRecord {
    pub id: Uuid,
    pub device_id: String,
    pub token_prefix: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
pub enum DeviceTokenRepositoryError {
    #[error("device token device was not found")]
    DeviceNotFound,
    #[error("device token was not found")]
    TokenNotFound,
    #[error("gateway child devices cannot have MQTT tokens")]
    GatewayChild,
    #[error("device token prefix already exists")]
    TokenPrefixConflict,
    #[error("stored device token timestamp is invalid")]
    InvalidStoredTimestamp,
    #[error("device token storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for DeviceTokenRepositoryError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for DeviceTokenRepositoryError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait DeviceTokenRepository: Send + Sync {
    fn provision_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        display_name: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn provision_owned_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device: NewOwnedDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn create_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn list_device_tokens<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    >;
    fn active_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    >;
    fn rotate_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    >;
    fn revoke_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), DeviceTokenRepositoryError>> + Send + 'a>>;
}

impl DeviceTokenRepository for PlatformStore {
    fn provision_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        display_name: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { provision_device_token(self, tenant_id, display_name, token).await })
    }

    fn provision_owned_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        device: NewOwnedDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { provision_owned_device_token(self, tenant_id, actor, device).await })
    }

    fn create_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { create_device_token(self, tenant_id, device_id, token).await })
    }

    fn list_device_tokens<'a>(
        &'a self,
        tenant_id: Uuid,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_device_tokens(self, tenant_id, device_id).await })
    }

    fn active_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { active_device_token(self, tenant_id, token_id).await })
    }

    fn rotate_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
        token: NewDeviceToken,
    ) -> Pin<
        Box<dyn Future<Output = Result<DeviceTokenRecord, DeviceTokenRepositoryError>> + Send + 'a>,
    > {
        Box::pin(async move { rotate_device_token(self, tenant_id, token_id, token).await })
    }

    fn revoke_device_token<'a>(
        &'a self,
        tenant_id: Uuid,
        token_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), DeviceTokenRepositoryError>> + Send + 'a>> {
        Box::pin(async move { revoke_device_token(self, tenant_id, token_id).await })
    }
}

#[derive(Debug)]
struct ValidatedManagementAsset {
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: serde_json::Value,
}

async fn list_management_assets(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAsset>, ManagementAssetError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE tenant_id = ?
                 ORDER BY name, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_asset_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE tenant_id = $1
                 ORDER BY name, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_asset_from_row)
                .collect()
        }
    }
}

async fn create_management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    asset: CreateManagementAsset,
) -> Result<ManagementAsset, ManagementAssetError> {
    let asset = validate_management_asset(
        asset.name,
        asset.asset_profile_id,
        asset.parent_asset_id,
        asset.metadata,
        asset.attributes,
    )?;
    let asset_id = Uuid::now_v7();
    let sibling_name = asset.name.clone();
    let sibling_parent_asset_id = asset.parent_asset_id;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            validate_sqlite_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            sqlx::query(
                "INSERT INTO assets (id, tenant_id, name, asset_profile_id, parent_asset_id, metadata)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .bind(asset.name)
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(asset.parent_asset_id.map(|id| id.to_string()))
            .bind(asset.metadata.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": null,
                            "after": parent_asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            validate_timescale_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            sqlx::query(
                "INSERT INTO assets (id, tenant_id, name, asset_profile_id, parent_asset_id, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .bind(asset.name)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(Json(asset.metadata))
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": null,
                            "after": parent_asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
    }
    management_asset(store, tenant_id, asset_id).await
}

async fn update_management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    asset_id: Uuid,
    asset: UpdateManagementAsset,
) -> Result<ManagementAsset, ManagementAssetError> {
    let asset = validate_management_asset(
        asset.name,
        asset.asset_profile_id,
        asset.parent_asset_id,
        asset.metadata,
        asset.attributes,
    )?;
    if asset.parent_asset_id == Some(asset_id) {
        return Err(ManagementAssetError::AssetCannotBeOwnParent);
    }
    let sibling_name = asset.name.clone();
    let sibling_parent_asset_id = asset.parent_asset_id;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            sqlite_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            let previous_parent_id: Option<String> = sqlx::query_scalar(
                "SELECT parent_asset_id FROM assets WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            let next_parent_id = asset.parent_asset_id.map(|id| id.to_string());
            validate_sqlite_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if sqlite_asset_is_descendant(
                    &mut transaction,
                    tenant_id,
                    asset_id,
                    parent_asset_id,
                )
                .await?
                {
                    return Err(ManagementAssetError::AssetCannotHaveDescendantParent);
                }
            }
            sqlx::query(
                "UPDATE assets
                 SET name = ?, asset_profile_id = ?, parent_asset_id = ?, metadata = ?,
                     updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset.name)
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(next_parent_id.as_deref())
            .bind(asset.metadata.to_string())
            .bind(Utc::now().to_rfc3339())
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if previous_parent_id != next_parent_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": previous_parent_id,
                            "after": next_parent_id,
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            // Match profile deletion before taking hierarchy row locks.
            sqlx::query("LOCK TABLE assets IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            lock_timescale_management_asset_update_scope(
                &mut transaction,
                tenant_id,
                asset_id,
                asset.parent_asset_id,
            )
            .await?;
            timescale_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            let previous_parent_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT parent_asset_id FROM assets WHERE id = $1 AND tenant_id = $2",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            let next_parent_id = asset.parent_asset_id;
            validate_timescale_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if timescale_asset_is_descendant(
                    &mut transaction,
                    tenant_id,
                    asset_id,
                    parent_asset_id,
                )
                .await?
                {
                    return Err(ManagementAssetError::AssetCannotHaveDescendantParent);
                }
            }
            sqlx::query(
                "UPDATE assets
                 SET name = $2, asset_profile_id = $3, parent_asset_id = $4, metadata = $5,
                     updated_at = now()
                 WHERE id = $1 AND tenant_id = $6",
            )
            .bind(asset_id)
            .bind(asset.name)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(Json(asset.metadata))
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if previous_parent_id != next_parent_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": previous_parent_id.map(|id| id.to_string()),
                            "after": next_parent_id.map(|id| id.to_string()),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
    }
    management_asset(store, tenant_id, asset_id).await
}

async fn delete_management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    asset_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let deleted = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            sqlite_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            if let Some(name) =
                sqlite_promoted_asset_root_name_conflict(&mut transaction, tenant_id, asset_id)
                    .await?
            {
                return Err(ManagementAssetError::SiblingNameConflict {
                    name,
                    parent_asset_id: None,
                });
            }
            let detached_asset_ids = sqlx::query_scalar::<_, String>(
                "UPDATE assets
                 SET parent_asset_id = NULL
                 WHERE parent_asset_id = ? AND tenant_id = ?
                 RETURNING id",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_all(&mut *transaction)
            .await?;
            let detached_device_ids = sqlx::query_scalar::<_, String>(
                "UPDATE devices
                 SET asset_id = NULL
                 WHERE asset_id = ? AND tenant_id = ?
                 RETURNING device_id",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_all(&mut *transaction)
            .await?;
            if !detached_asset_ids.is_empty() || !detached_device_ids.is_empty() {
                for detached_asset_id in detached_asset_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Asset,
                        detached_asset_id,
                        serde_json::json!({
                            "parent_asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
                for detached_device_id in detached_device_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Device,
                        detached_device_id,
                        serde_json::json!({
                            "asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
            }
            let deleted = sqlx::query("DELETE FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(asset_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            deleted
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            timescale_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            if let Some(name) =
                timescale_promoted_asset_root_name_conflict(&mut transaction, tenant_id, asset_id)
                    .await?
            {
                return Err(ManagementAssetError::SiblingNameConflict {
                    name,
                    parent_asset_id: None,
                });
            }
            let detached_asset_ids = sqlx::query_scalar::<_, Uuid>(
                "UPDATE assets
                 SET parent_asset_id = NULL
                 WHERE parent_asset_id = $1 AND tenant_id = $2
                 RETURNING id",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_all(&mut *transaction)
            .await?;
            let detached_device_ids = sqlx::query_scalar::<_, String>(
                "UPDATE devices
                 SET asset_id = NULL
                 WHERE asset_id = $1 AND tenant_id = $2
                 RETURNING device_id",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_all(&mut *transaction)
            .await?;
            if !detached_asset_ids.is_empty() || !detached_device_ids.is_empty() {
                for detached_asset_id in detached_asset_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Asset,
                        detached_asset_id.to_string(),
                        serde_json::json!({
                            "parent_asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
                for detached_device_id in detached_device_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Device,
                        detached_device_id,
                        serde_json::json!({
                            "asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
            }
            let deleted = sqlx::query("DELETE FROM assets WHERE id = $1 AND tenant_id = $2")
                .bind(asset_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            deleted
        }
    };
    if deleted == 0 {
        Err(ManagementAssetError::AssetNotFound)
    } else {
        Ok(())
    }
}

async fn sqlite_promoted_asset_root_name_conflict(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<Option<String>, ManagementAssetError> {
    sqlx::query_scalar(
        "SELECT child.name
         FROM assets AS child
         JOIN assets AS root
           ON root.name = child.name
          AND root.tenant_id = child.tenant_id
          AND root.parent_asset_id IS NULL
          AND root.id <> ?
         WHERE child.parent_asset_id = ? AND child.tenant_id = ?
         ORDER BY child.name, child.id
         LIMIT 1",
    )
    .bind(asset_id.to_string())
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(ManagementAssetError::from)
}

async fn timescale_promoted_asset_root_name_conflict(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<Option<String>, ManagementAssetError> {
    sqlx::query_scalar(
        "SELECT child.name
         FROM assets AS child
         JOIN assets AS root
           ON root.name = child.name
          AND root.tenant_id = child.tenant_id
          AND root.parent_asset_id IS NULL
          AND root.id <> $1
         WHERE child.parent_asset_id = $1 AND child.tenant_id = $2
         ORDER BY child.name, child.id
         LIMIT 1",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(ManagementAssetError::from)
}

async fn management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<ManagementAsset, ManagementAssetError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT id, name, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementAssetError::AssetNotFound)?;
            sqlite_management_asset_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT id, name, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE id = $1 AND tenant_id = $2",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementAssetError::AssetNotFound)?;
            timescale_management_asset_from_row(row)
        }
    }
}

fn sqlite_management_asset_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementAsset, ManagementAssetError> {
    let metadata: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("metadata")?)
        .map_err(|_| ManagementAssetError::InvalidStoredMetadata)?;
    if !metadata.is_object() {
        return Err(ManagementAssetError::InvalidStoredMetadata);
    }
    Ok(ManagementAsset {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| ManagementAssetError::InvalidStoredAssetId)?,
        name: row.try_get("name")?,
        asset_profile_id: row
            .try_get::<Option<String>, _>("asset_profile_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementAssetError::InvalidStoredReferences)?,
        parent_asset_id: row
            .try_get::<Option<String>, _>("parent_asset_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementAssetError::InvalidStoredReferences)?,
        attributes: metadata.clone(),
        metadata,
    })
}

fn timescale_management_asset_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementAsset, ManagementAssetError> {
    let metadata = row.try_get::<Json<serde_json::Value>, _>("metadata")?.0;
    if !metadata.is_object() {
        return Err(ManagementAssetError::InvalidStoredMetadata);
    }
    Ok(ManagementAsset {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        asset_profile_id: row.try_get("asset_profile_id")?,
        parent_asset_id: row.try_get("parent_asset_id")?,
        attributes: metadata.clone(),
        metadata,
    })
}

fn validate_management_asset(
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: serde_json::Value,
    attributes: Option<serde_json::Value>,
) -> Result<ValidatedManagementAsset, ManagementAssetError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err(ManagementAssetError::InvalidName);
    }
    let metadata = match attributes {
        Some(attributes) => validate_asset_attributes(attributes)?,
        None => validate_asset_metadata(metadata)?,
    };
    Ok(ValidatedManagementAsset {
        name: name.to_owned(),
        asset_profile_id,
        parent_asset_id,
        metadata,
    })
}

fn map_management_asset_sibling_name_conflict(
    error: sqlx::Error,
    name: &str,
    parent_asset_id: Option<Uuid>,
) -> ManagementAssetError {
    if error
        .as_database_error()
        .is_some_and(is_management_asset_sibling_name_unique_violation)
    {
        ManagementAssetError::SiblingNameConflict {
            name: name.to_owned(),
            parent_asset_id,
        }
    } else {
        ManagementAssetError::from(error)
    }
}

fn is_management_asset_sibling_name_unique_violation(
    database_error: &(dyn DatabaseError + 'static),
) -> bool {
    match database_error.code().as_deref() {
        Some("23505") => {
            database_error.constraint() == Some("assets_tenant_id_parent_asset_id_name_key")
                || database_error.constraint() == Some("assets_tenant_root_name_unique_index")
                || database_error
                    .message()
                    .contains("assets_tenant_id_parent_asset_id_name_key")
                || database_error
                    .message()
                    .contains("assets_tenant_root_name_unique_index")
        }
        Some("19") | Some("2067") => {
            let message = database_error.message();
            message.contains(
                "UNIQUE constraint failed: assets.tenant_id, assets.parent_asset_id, assets.name",
            ) || message.contains("UNIQUE constraint failed: assets.tenant_id, assets.name")
        }
        _ => false,
    }
}

fn validate_asset_metadata(
    value: serde_json::Value,
) -> Result<serde_json::Value, ManagementAssetError> {
    if value.is_null() {
        Ok(serde_json::json!({}))
    } else if value.is_object() {
        Ok(value)
    } else {
        Err(ManagementAssetError::MetadataMustBeObject)
    }
}

fn validate_asset_attributes(
    value: serde_json::Value,
) -> Result<serde_json::Value, ManagementAssetError> {
    if value.is_null() {
        Ok(serde_json::json!({}))
    } else if value.is_object() {
        Ok(value)
    } else {
        Err(ManagementAssetError::AttributesMustBeObject)
    }
}

async fn sqlite_require_management_asset(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let exists =
        sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?")
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .is_some();
    if exists {
        Ok(())
    } else {
        Err(ManagementAssetError::AssetNotFound)
    }
}

async fn timescale_require_management_asset(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let exists = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM assets WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(ManagementAssetError::AssetNotFound)
    }
}

async fn lock_timescale_management_asset_update_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
    proposed_parent_asset_id: Option<Uuid>,
) -> Result<(), ManagementAssetError> {
    // Lock every row that can affect this hierarchy transition in UUID order. This
    // serializes reciprocal reparenting before validation sees a stale hierarchy.
    sqlx::query_scalar::<_, Uuid>(
        "WITH RECURSIVE roots(id) AS (
             SELECT id FROM assets WHERE id = $1 AND tenant_id = $3
             UNION
             SELECT id FROM assets WHERE id = $2 AND tenant_id = $3
         ),
         ancestors(id) AS (
             SELECT id FROM roots
             UNION
             SELECT asset.parent_asset_id
             FROM assets AS asset
             JOIN ancestors ON asset.id = ancestors.id
             WHERE asset.parent_asset_id IS NOT NULL AND asset.tenant_id = $3
         ),
         descendants(id) AS (
             SELECT id FROM roots
             UNION
             SELECT child.id
             FROM assets AS child
             JOIN descendants ON child.parent_asset_id = descendants.id
             WHERE child.tenant_id = $3
         )
         SELECT asset.id
         FROM assets AS asset
         WHERE asset.tenant_id = $3 AND asset.id IN (
             SELECT id FROM ancestors
             UNION
             SELECT id FROM descendants
         )
         ORDER BY asset.id
         FOR UPDATE",
    )
    .bind(asset_id)
    .bind(proposed_parent_asset_id)
    .bind(tenant_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(())
}

async fn validate_sqlite_asset_references(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
) -> Result<(), ManagementAssetError> {
    if let Some(asset_profile_id) = asset_profile_id {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM asset_profiles WHERE id = ? AND tenant_id = ?",
        )
        .bind(asset_profile_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !exists {
            return Err(ManagementAssetError::AssetProfileUnavailable(
                asset_profile_id,
            ));
        }
    }
    if let Some(parent_asset_id) = parent_asset_id {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(parent_asset_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if !exists {
            return Err(ManagementAssetError::ParentAssetUnavailable(
                parent_asset_id,
            ));
        }
    }
    Ok(())
}

async fn validate_timescale_asset_references(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
) -> Result<(), ManagementAssetError> {
    if let Some(asset_profile_id) = asset_profile_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM asset_profiles WHERE id = $1 AND tenant_id = $2
             )",
        )
        .bind(asset_profile_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementAssetError::AssetProfileUnavailable(
                asset_profile_id,
            ));
        }
    }
    if let Some(parent_asset_id) = parent_asset_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)",
        )
        .bind(parent_asset_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementAssetError::ParentAssetUnavailable(
                parent_asset_id,
            ));
        }
    }
    Ok(())
}

async fn sqlite_asset_is_descendant(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
    candidate_parent_id: Uuid,
) -> Result<bool, ManagementAssetError> {
    let descendant = sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE descendants(id) AS (
             SELECT id FROM assets WHERE parent_asset_id = ? AND tenant_id = ?
             UNION
             SELECT child.id
             FROM assets AS child
             JOIN descendants ON child.parent_asset_id = descendants.id
             WHERE child.tenant_id = ?
         )
         SELECT 1 FROM descendants WHERE id = ? LIMIT 1",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(candidate_parent_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    Ok(descendant)
}

async fn timescale_asset_is_descendant(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
    candidate_parent_id: Uuid,
) -> Result<bool, ManagementAssetError> {
    let descendant = sqlx::query_scalar::<_, bool>(
        "WITH RECURSIVE descendants(id) AS (
             SELECT id FROM assets WHERE parent_asset_id = $1 AND tenant_id = $3
             UNION
             SELECT child.id
             FROM assets AS child
             JOIN descendants ON child.parent_asset_id = descendants.id
             WHERE child.tenant_id = $3
         )
         SELECT EXISTS(SELECT 1 FROM descendants WHERE id = $2)",
    )
    .bind(asset_id)
    .bind(candidate_parent_id)
    .bind(tenant_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(descendant)
}

async fn list_management_devices(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementDevice>, ManagementDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                        is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
                 FROM devices
                 WHERE tenant_id = ? AND deleted_at IS NULL
                 ORDER BY device_id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_device_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT d.device_id, d.display_name, d.asset_id, d.device_profile_id, d.metadata,
                        runtime.last_seen_at, d.is_gateway, d.gateway_device_id,
                        runtime.gateway_last_read_at, runtime.gateway_read_quality
                 FROM devices AS d
                 LEFT JOIN device_runtime_state AS runtime
                   ON runtime.device_id = d.device_id
                 WHERE d.tenant_id = $1 AND d.deleted_at IS NULL
                 ORDER BY d.device_id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_device_from_row)
                .collect()
        }
    }
}

async fn management_device(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<ManagementDevice, ManagementDeviceError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT device_id, display_name, asset_id, device_profile_id, metadata, last_seen_at,
                        is_gateway, gateway_device_id, gateway_last_read_at, gateway_read_quality
                 FROM devices
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
            sqlite_management_device_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT d.device_id, d.display_name, d.asset_id, d.device_profile_id, d.metadata,
                        runtime.last_seen_at, d.is_gateway, d.gateway_device_id,
                        runtime.gateway_last_read_at, runtime.gateway_read_quality
                 FROM devices AS d
                 LEFT JOIN device_runtime_state AS runtime
                   ON runtime.device_id = d.device_id
                 WHERE d.device_id = $1 AND d.tenant_id = $2 AND d.deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementDeviceError::DeviceNotFound)?;
            timescale_management_device_from_row(row)
        }
    }
}

fn sqlite_management_device_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementDevice, ManagementDeviceError> {
    let last_seen_at = sqlite_timestamp(row.try_get("last_seen_at")?)?;
    let gateway_last_read_at = sqlite_timestamp(row.try_get("gateway_last_read_at")?)?;
    let topology = ManagementDeviceTopology {
        is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
        gateway_device_id: row.try_get("gateway_device_id")?,
    };
    let health = management_health(
        &topology,
        last_seen_at,
        gateway_last_read_at,
        row.try_get::<Option<String>, _>("gateway_read_quality")?
            .as_deref(),
    );
    Ok(ManagementDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        asset_id: row
            .try_get::<Option<String>, _>("asset_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
        device_profile_id: row
            .try_get::<Option<String>, _>("device_profile_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
        attributes: serde_json::from_str(&row.try_get::<String, _>("metadata")?)
            .map_err(|_| ManagementDeviceError::InvalidStoredAttributes)?,
        topology,
        health,
    })
}

fn timescale_management_device_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementDevice, ManagementDeviceError> {
    let topology = ManagementDeviceTopology {
        is_gateway: row.try_get("is_gateway")?,
        gateway_device_id: row.try_get("gateway_device_id")?,
    };
    let health = management_health(
        &topology,
        row.try_get("last_seen_at")?,
        row.try_get("gateway_last_read_at")?,
        row.try_get::<Option<String>, _>("gateway_read_quality")?
            .as_deref(),
    );
    Ok(ManagementDevice {
        device_id: row.try_get("device_id")?,
        display_name: row.try_get("display_name")?,
        asset_id: row.try_get("asset_id")?,
        device_profile_id: row.try_get("device_profile_id")?,
        attributes: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
        topology,
        health,
    })
}

fn management_health(
    topology: &ManagementDeviceTopology,
    last_seen_at: Option<DateTime<Utc>>,
    gateway_last_read_at: Option<DateTime<Utc>>,
    gateway_read_quality: Option<&str>,
) -> ManagementDeviceHealth {
    let fresh_after = Utc::now() - Duration::minutes(5);
    if topology.is_gateway {
        let online = last_seen_at.is_some_and(|seen| seen >= fresh_after);
        return ManagementDeviceHealth {
            online,
            last_seen_at,
            gateway_status: Some(if online {
                ManagementGatewayStatus::Online
            } else {
                ManagementGatewayStatus::Offline
            }),
            child_status: None,
        };
    }
    if topology.gateway_device_id.is_some() {
        let unavailable_after = fresh_after - Duration::minutes(10);
        let status = if gateway_read_quality == Some("unavailable") {
            ManagementChildStatus::Unavailable
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= fresh_after) {
            ManagementChildStatus::Fresh
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= unavailable_after) {
            ManagementChildStatus::Stale
        } else {
            ManagementChildStatus::Unavailable
        };
        return ManagementDeviceHealth {
            online: status == ManagementChildStatus::Fresh,
            last_seen_at: gateway_last_read_at,
            gateway_status: None,
            child_status: Some(status),
        };
    }
    ManagementDeviceHealth {
        online: last_seen_at.is_some_and(|seen| seen >= fresh_after),
        last_seen_at,
        gateway_status: None,
        child_status: None,
    }
}

fn sqlite_timestamp(value: Option<String>) -> Result<Option<DateTime<Utc>>, ManagementDeviceError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|value| value.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                        .map(|value| value.and_utc())
                })
                .map_err(|_| ManagementDeviceError::InvalidStoredTimestamp)
        })
        .transpose()
}

async fn update_management_device(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    device_id: &str,
    update: UpdateManagementDevice,
) -> Result<ManagementDevice, ManagementDeviceError> {
    validate_device_id(device_id)?;
    let display_name = validate_display_name(&update.display_name)?.to_owned();
    let attributes = validate_attributes(update.attributes)?;

    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            let current = sqlite_topology(&mut transaction, tenant_id, device_id).await?;
            let previous_asset_id: Option<String> = sqlx::query_scalar(
                "SELECT asset_id FROM devices
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            let next_asset_id = update.asset_id.map(|id| id.to_string());
            let topology = update.topology.unwrap_or(current.clone());
            validate_sqlite_topology(tenant_id, device_id, &current, &topology, &mut transaction)
                .await?;
            let topology_changed = current.gateway_device_id != topology.gateway_device_id;
            validate_sqlite_references(
                &mut transaction,
                tenant_id,
                update.asset_id,
                update.device_profile_id,
            )
            .await?;
            sqlx::query(
                "UPDATE devices
                 SET display_name = ?, asset_id = ?, device_profile_id = ?,
                     metadata = COALESCE(?, metadata), is_gateway = ?, gateway_device_id = ?,
                     gateway_topology_version = gateway_topology_version + ?
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(display_name)
            .bind(next_asset_id.as_deref())
            .bind(update.device_profile_id.map(|id| id.to_string()))
            .bind(attributes.map(|value| value.to_string()))
            .bind(i64::from(topology.is_gateway))
            .bind(&topology.gateway_device_id)
            .bind(i32::from(topology_changed))
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if previous_asset_id != next_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "asset_id": {
                            "before": previous_asset_id,
                            "after": next_asset_id,
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            if topology_changed {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    gateway_audit_action(
                        current.gateway_device_id.as_deref(),
                        topology.gateway_device_id.as_deref(),
                    ),
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "gateway_device_id": {
                            "before": current.gateway_device_id,
                            "after": topology.gateway_device_id,
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            if topology.gateway_device_id.is_some() {
                sqlx::query(
                    "UPDATE device_tokens
                     SET revoked_at = ?
                     WHERE device_id = ? AND revoked_at IS NULL
                       AND EXISTS (
                           SELECT 1 FROM devices
                           WHERE devices.device_id = device_tokens.device_id
                             AND devices.tenant_id = ?
                             AND devices.deleted_at IS NULL
                       )",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(device_id)
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            // Match profile deletion before taking topology row locks.
            sqlx::query("LOCK TABLE devices IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            let current = timescale_topology(&mut transaction, tenant_id, device_id).await?;
            let previous_asset_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT asset_id FROM devices
                 WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                 FOR UPDATE",
            )
            .bind(device_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            let next_asset_id = update.asset_id;
            let topology = update.topology.unwrap_or(current.clone());
            validate_timescale_topology(
                tenant_id,
                device_id,
                &current,
                &topology,
                &mut transaction,
            )
            .await?;
            let topology_changed = current.gateway_device_id != topology.gateway_device_id;
            validate_timescale_references(
                &mut transaction,
                tenant_id,
                update.asset_id,
                update.device_profile_id,
            )
            .await?;
            sqlx::query(
                "UPDATE devices
                 SET display_name = $2, asset_id = $3, device_profile_id = $4,
                     metadata = COALESCE($5, metadata), is_gateway = $6, gateway_device_id = $7,
                     gateway_topology_version = gateway_topology_version + $8
                 WHERE device_id = $1 AND tenant_id = $9 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(display_name)
            .bind(update.asset_id)
            .bind(update.device_profile_id)
            .bind(attributes.map(Json))
            .bind(topology.is_gateway)
            .bind(&topology.gateway_device_id)
            .bind(i32::from(topology_changed))
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            if previous_asset_id != next_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "asset_id": {
                            "before": previous_asset_id.map(|id| id.to_string()),
                            "after": next_asset_id.map(|id| id.to_string()),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            if topology_changed {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    gateway_audit_action(
                        current.gateway_device_id.as_deref(),
                        topology.gateway_device_id.as_deref(),
                    ),
                    AuditTargetType::Device,
                    device_id.to_owned(),
                    serde_json::json!({
                        "gateway_device_id": {
                            "before": current.gateway_device_id,
                            "after": topology.gateway_device_id,
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            if topology.gateway_device_id.is_some() {
                sqlx::query(
                    "UPDATE device_tokens
                     SET revoked_at = now()
                     WHERE device_id = $1 AND revoked_at IS NULL
                       AND EXISTS (
                           SELECT 1 FROM devices
                           WHERE devices.device_id = device_tokens.device_id
                             AND devices.tenant_id = $2
                             AND devices.deleted_at IS NULL
                       )",
                )
                .bind(device_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
    }
    management_device(store, tenant_id, device_id).await
}

fn gateway_audit_action(
    previous_gateway_device_id: Option<&str>,
    next_gateway_device_id: Option<&str>,
) -> AuditAction {
    match (previous_gateway_device_id, next_gateway_device_id) {
        (None, Some(_)) => AuditAction::GatewayAssigned,
        (Some(_), None) => AuditAction::GatewayDetached,
        (Some(_), Some(_)) => AuditAction::GatewayReassigned,
        (None, None) => unreachable!("gateway audit action requires a topology change"),
    }
}

async fn delete_management_device(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    device_id: &str,
) -> Result<(), ManagementDeviceError> {
    validate_device_id(device_id)?;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            if sqlite_has_children(&mut transaction, tenant_id, device_id).await? {
                return Err(ManagementDeviceError::GatewayHasChildren);
            }
            let deleted = sqlx::query(
                "UPDATE devices SET deleted_at = ?
                 WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted == 0 {
                return Err(ManagementDeviceError::DeviceNotFound);
            }
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = ?
                 WHERE device_id = ? AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = ?
                   )",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            if timescale_has_children(&mut transaction, tenant_id, device_id).await? {
                return Err(ManagementDeviceError::GatewayHasChildren);
            }
            let deleted = sqlx::query(
                "UPDATE devices SET deleted_at = now()
                 WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if deleted == 0 {
                return Err(ManagementDeviceError::DeviceNotFound);
            }
            sqlx::query(
                "UPDATE device_tokens SET revoked_at = now()
                 WHERE device_id = $1 AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = $2
                   )",
            )
            .bind(device_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

fn validate_device_id(device_id: &str) -> Result<(), ManagementDeviceError> {
    if !device_id.is_empty()
        && device_id.len() <= 64
        && device_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || value == b'-' || value == b'_')
    {
        Ok(())
    } else {
        Err(ManagementDeviceError::InvalidDeviceId(device_id.to_owned()))
    }
}

fn validate_display_name(value: &str) -> Result<&str, ManagementDeviceError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 {
        Err(ManagementDeviceError::InvalidDisplayName)
    } else {
        Ok(value)
    }
}

fn validate_attributes(
    attributes: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, ManagementDeviceError> {
    attributes
        .map(|value| {
            if value.is_null() {
                Ok(serde_json::json!({}))
            } else if value.is_object() {
                Ok(value)
            } else {
                Err(ManagementDeviceError::AttributesMustBeObject)
            }
        })
        .transpose()
}

async fn sqlite_topology(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<ManagementDeviceTopology, ManagementDeviceError> {
    let row = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementDeviceError::DeviceNotFound)?;
    Ok(ManagementDeviceTopology {
        is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
        gateway_device_id: row.try_get("gateway_device_id")?,
    })
}

async fn timescale_topology(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<ManagementDeviceTopology, ManagementDeviceError> {
    let row = sqlx::query(
        "SELECT is_gateway, gateway_device_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ManagementDeviceError::DeviceNotFound)?;
    Ok(ManagementDeviceTopology {
        is_gateway: row.try_get("is_gateway")?,
        gateway_device_id: row.try_get("gateway_device_id")?,
    })
}

async fn sqlite_has_children(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<bool, ManagementDeviceError> {
    Ok(sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = ? AND tenant_id = ? AND deleted_at IS NULL
         LIMIT 1",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn timescale_has_children(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<bool, ManagementDeviceError> {
    Ok(sqlx::query(
        "SELECT 1 FROM devices
         WHERE gateway_device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some())
}

async fn validate_sqlite_topology(
    tenant_id: Uuid,
    device_id: &str,
    current: &ManagementDeviceTopology,
    topology: &ManagementDeviceTopology,
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), ManagementDeviceError> {
    if topology.is_gateway && topology.gateway_device_id.is_some() {
        return Err(ManagementDeviceError::GatewayCannotHaveParent);
    }
    if topology.gateway_device_id.as_deref() == Some(device_id) {
        return Err(ManagementDeviceError::DeviceCannotBeOwnGateway);
    }
    if current.is_gateway
        && !topology.is_gateway
        && sqlite_has_children(transaction, tenant_id, device_id).await?
    {
        return Err(ManagementDeviceError::GatewayHasChildren);
    }
    if let Some(gateway_device_id) = topology.gateway_device_id.as_deref() {
        let is_gateway = sqlx::query_scalar::<_, i64>(
            "SELECT is_gateway FROM devices
             WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
        )
        .bind(gateway_device_id)
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ManagementDeviceError::GatewayUnavailable)?;
        if is_gateway == 0 {
            return Err(ManagementDeviceError::GatewayIsNotGateway);
        }
    }
    Ok(())
}

async fn validate_timescale_topology(
    tenant_id: Uuid,
    device_id: &str,
    current: &ManagementDeviceTopology,
    topology: &ManagementDeviceTopology,
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), ManagementDeviceError> {
    if topology.is_gateway && topology.gateway_device_id.is_some() {
        return Err(ManagementDeviceError::GatewayCannotHaveParent);
    }
    if topology.gateway_device_id.as_deref() == Some(device_id) {
        return Err(ManagementDeviceError::DeviceCannotBeOwnGateway);
    }
    if current.is_gateway
        && !topology.is_gateway
        && timescale_has_children(transaction, tenant_id, device_id).await?
    {
        return Err(ManagementDeviceError::GatewayHasChildren);
    }
    if let Some(gateway_device_id) = topology.gateway_device_id.as_deref() {
        let is_gateway = sqlx::query_scalar::<_, bool>(
            "SELECT is_gateway FROM devices
             WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(gateway_device_id)
        .bind(tenant_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(ManagementDeviceError::GatewayUnavailable)?;
        if !is_gateway {
            return Err(ManagementDeviceError::GatewayIsNotGateway);
        }
    }
    Ok(())
}

async fn validate_sqlite_references(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> Result<(), ManagementDeviceError> {
    if let Some(asset_id) = asset_id {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(asset_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if !exists {
            return Err(ManagementDeviceError::AssetUnavailable(asset_id));
        }
    }
    if let Some(device_profile_id) = device_profile_id {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM device_profiles WHERE id = ? AND tenant_id = ?",
        )
        .bind(device_profile_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !exists {
            return Err(ManagementDeviceError::DeviceProfileUnavailable(
                device_profile_id,
            ));
        }
    }
    Ok(())
}

async fn validate_timescale_references(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
) -> Result<(), ManagementDeviceError> {
    if let Some(asset_id) = asset_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)",
        )
        .bind(asset_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementDeviceError::AssetUnavailable(asset_id));
        }
    }
    if let Some(device_profile_id) = device_profile_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM device_profiles WHERE id = $1 AND tenant_id = $2
             )",
        )
        .bind(device_profile_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementDeviceError::DeviceProfileUnavailable(
                device_profile_id,
            ));
        }
    }
    Ok(())
}

async fn provision_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    display_name: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let device_id = Uuid::now_v7().to_string();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            sqlx::query(
                "INSERT INTO devices (device_id, tenant_id, display_name)
                 VALUES (?, ?, ?)",
            )
            .bind(&device_id)
            .bind(tenant_id.to_string())
            .bind(display_name)
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "INSERT INTO devices (device_id, tenant_id, display_name)
                 VALUES ($1, $2, $3)",
            )
            .bind(&device_id)
            .bind(tenant_id)
            .bind(display_name)
            .execute(&mut *transaction)
            .await?;
            let record = insert_timescale_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn provision_owned_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    device: NewOwnedDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let device_id = Uuid::now_v7().to_string();
    let owner_user_id = device.owner_user_id;
    let asset_id = device.asset_id;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            let owner_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM users WHERE id = ? AND tenant_id = ?)",
            )
            .bind(owner_user_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            if !owner_exists {
                return Err(DeviceTokenRepositoryError::DeviceNotFound);
            }
            if let Some(asset_id) = asset_id {
                let asset_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?)",
                )
                .bind(asset_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
                if !asset_exists {
                    return Err(DeviceTokenRepositoryError::DeviceNotFound);
                }
            }
            sqlx::query(
                "INSERT INTO devices (
                     device_id, tenant_id, display_name, owner_user_id, asset_id, claimed_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&device_id)
            .bind(tenant_id.to_string())
            .bind(&device.display_name)
            .bind(owner_user_id.to_string())
            .bind(asset_id.map(|id| id.to_string()))
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *transaction)
            .await?;
            let record =
                insert_sqlite_device_token(&mut transaction, &device_id, device.token).await?;
            let ownership_event = audit::NewAuditEvent::new(
                tenant_id,
                actor,
                AuditAction::OwnershipTransferred,
                AuditTargetType::Device,
                device_id.clone(),
                serde_json::json!({
                    "owner_user_id": {
                        "before": null,
                        "after": owner_user_id.to_string(),
                    }
                }),
            );
            audit::insert_sqlite_audit_event(&mut transaction, &ownership_event).await?;
            if let Some(asset_id) = asset_id {
                let containment_event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.clone(),
                    serde_json::json!({
                        "asset_id": {
                            "before": null,
                            "after": asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &containment_event).await?;
            }
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            let owner_exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2)",
            )
            .bind(owner_user_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            if !owner_exists {
                return Err(DeviceTokenRepositoryError::DeviceNotFound);
            }
            if let Some(asset_id) = asset_id {
                let asset_exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)",
                )
                .bind(asset_id)
                .bind(tenant_id)
                .fetch_one(&mut *transaction)
                .await?;
                if !asset_exists {
                    return Err(DeviceTokenRepositoryError::DeviceNotFound);
                }
            }
            sqlx::query(
                "INSERT INTO devices (
                     device_id, tenant_id, display_name, owner_user_id, asset_id, claimed_at
                 ) VALUES ($1, $2, $3, $4, $5, now())",
            )
            .bind(&device_id)
            .bind(tenant_id)
            .bind(&device.display_name)
            .bind(owner_user_id)
            .bind(asset_id)
            .execute(&mut *transaction)
            .await?;
            let record =
                insert_timescale_device_token(&mut transaction, &device_id, device.token).await?;
            let ownership_event = audit::NewAuditEvent::new(
                tenant_id,
                actor,
                AuditAction::OwnershipTransferred,
                AuditTargetType::Device,
                device_id.clone(),
                serde_json::json!({
                    "owner_user_id": {
                        "before": null,
                        "after": owner_user_id.to_string(),
                    }
                }),
            );
            audit::insert_timescale_audit_event(&mut transaction, &ownership_event).await?;
            if let Some(asset_id) = asset_id {
                let containment_event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Device,
                    device_id.clone(),
                    serde_json::json!({
                        "asset_id": {
                            "before": null,
                            "after": asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &containment_event).await?;
            }
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn create_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            ensure_sqlite_token_eligible(&mut transaction, tenant_id, device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = ?
                 WHERE device_id = ? AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = ?
                         AND devices.deleted_at IS NULL
                   )",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(device_id)
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            ensure_timescale_token_eligible(&mut transaction, tenant_id, device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = now()
                 WHERE device_id = $1 AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = $2
                         AND devices.deleted_at IS NULL
                   )",
            )
            .bind(device_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            let record = insert_timescale_device_token(&mut transaction, device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn list_device_tokens(
    store: &PlatformStore,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<Vec<DeviceTokenRecord>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
                 FROM device_tokens dt
                 INNER JOIN devices d ON d.device_id = dt.device_id
                 WHERE dt.device_id = ? AND d.tenant_id = ? AND d.deleted_at IS NULL
                 ORDER BY dt.created_at DESC, dt.id DESC",
            )
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_device_token_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
                 FROM device_tokens dt
                 INNER JOIN devices d ON d.device_id = dt.device_id
                 WHERE dt.device_id = $1 AND d.tenant_id = $2 AND d.deleted_at IS NULL
                 ORDER BY dt.created_at DESC, dt.id DESC",
            )
            .bind(device_id)
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_device_token_record)
                .collect()
        }
    }
}

async fn active_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<Option<DeviceTokenRecord>, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
             FROM device_tokens dt
             INNER JOIN devices d ON d.device_id = dt.device_id
             WHERE dt.id = ? AND dt.revoked_at IS NULL
               AND d.tenant_id = ? AND d.deleted_at IS NULL",
        )
        .bind(token_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_device_token_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT dt.id, dt.device_id, dt.token_prefix, dt.created_at, dt.last_used_at, dt.revoked_at
             FROM device_tokens dt
             INNER JOIN devices d ON d.device_id = dt.device_id
             WHERE dt.id = $1 AND dt.revoked_at IS NULL
               AND d.tenant_id = $2 AND d.deleted_at IS NULL",
        )
        .bind(token_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_device_token_record)
        .transpose(),
    }
}

async fn rotate_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    token_id: Uuid,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            let device_id =
                sqlite_active_token_device_id(&mut transaction, tenant_id, token_id).await?;
            ensure_sqlite_token_eligible(&mut transaction, tenant_id, &device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = ?
                 WHERE id = ? AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = ?
                         AND devices.deleted_at IS NULL
                   )",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(token_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let record = insert_sqlite_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            let device_id =
                timescale_active_token_device_id(&mut transaction, tenant_id, token_id).await?;
            ensure_timescale_token_eligible(&mut transaction, tenant_id, &device_id).await?;
            sqlx::query(
                "UPDATE device_tokens
                 SET revoked_at = now()
                 WHERE id = $1 AND revoked_at IS NULL
                   AND EXISTS (
                       SELECT 1 FROM devices
                       WHERE devices.device_id = device_tokens.device_id
                         AND devices.tenant_id = $2
                         AND devices.deleted_at IS NULL
                   )",
            )
            .bind(token_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            let record = insert_timescale_device_token(&mut transaction, &device_id, token).await?;
            transaction.commit().await?;
            Ok(record)
        }
    }
}

async fn revoke_device_token(
    store: &PlatformStore,
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<(), DeviceTokenRepositoryError> {
    let revoked = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE device_tokens
             SET revoked_at = ?
             WHERE id = ? AND revoked_at IS NULL
               AND EXISTS (
                   SELECT 1 FROM devices
                   WHERE devices.device_id = device_tokens.device_id
                     AND devices.tenant_id = ?
                     AND devices.deleted_at IS NULL
               )",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(token_id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE device_tokens
             SET revoked_at = now()
             WHERE id = $1 AND revoked_at IS NULL
               AND EXISTS (
                   SELECT 1 FROM devices
                   WHERE devices.device_id = device_tokens.device_id
                     AND devices.tenant_id = $2
                     AND devices.deleted_at IS NULL
               )",
        )
        .bind(token_id)
        .bind(tenant_id)
        .execute(pool)
        .await?
        .rows_affected(),
    };
    if revoked == 0 {
        Err(DeviceTokenRepositoryError::TokenNotFound)
    } else {
        Ok(())
    }
}

async fn sqlite_active_token_device_id(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<String, DeviceTokenRepositoryError> {
    sqlx::query_scalar(
        "SELECT dt.device_id FROM device_tokens dt
         INNER JOIN devices d ON d.device_id = dt.device_id
         WHERE dt.id = ? AND dt.revoked_at IS NULL
           AND d.tenant_id = ? AND d.deleted_at IS NULL",
    )
    .bind(token_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenNotFound)
}

async fn timescale_active_token_device_id(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    token_id: Uuid,
) -> Result<String, DeviceTokenRepositoryError> {
    sqlx::query_scalar(
        "SELECT dt.device_id FROM device_tokens dt
         INNER JOIN devices d ON d.device_id = dt.device_id
         WHERE dt.id = $1 AND dt.revoked_at IS NULL
           AND d.tenant_id = $2 AND d.deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(token_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenNotFound)
}

async fn ensure_sqlite_token_eligible(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceTokenRepositoryError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::DeviceNotFound)?;
    if gateway_device_id.is_some() {
        Err(DeviceTokenRepositoryError::GatewayChild)
    } else {
        Ok(())
    }
}

async fn ensure_timescale_token_eligible(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceTokenRepositoryError> {
    let gateway_device_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT gateway_device_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::DeviceNotFound)?;
    if gateway_device_id.is_some() {
        Err(DeviceTokenRepositoryError::GatewayChild)
    } else {
        Ok(())
    }
}

async fn insert_sqlite_device_token(
    transaction: &mut Transaction<'_, Sqlite>,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let created_at = Utc::now();
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO device_tokens (
             id, device_id, token_prefix, token_hash, token_ciphertext, created_at
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(token.id.to_string())
    .bind(device_id)
    .bind(&token.token_prefix)
    .bind(&token.token_hash)
    .bind(&token.token_ciphertext)
    .bind(created_at.to_rfc3339())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Err(DeviceTokenRepositoryError::TokenPrefixConflict);
    }
    Ok(DeviceTokenRecord {
        id: token.id,
        device_id: device_id.to_owned(),
        token_prefix: token.token_prefix,
        created_at,
        last_used_at: None,
        revoked_at: None,
    })
}

async fn insert_timescale_device_token(
    transaction: &mut Transaction<'_, Postgres>,
    device_id: &str,
    token: NewDeviceToken,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    let row = sqlx::query(
        "INSERT INTO device_tokens (
             id, device_id, token_prefix, token_hash, token_ciphertext
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (token_prefix) DO NOTHING
         RETURNING id, device_id, token_prefix, created_at, last_used_at, revoked_at",
    )
    .bind(token.id)
    .bind(device_id)
    .bind(&token.token_prefix)
    .bind(&token.token_hash)
    .bind(&token.token_ciphertext)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DeviceTokenRepositoryError::TokenPrefixConflict)?;
    Ok(DeviceTokenRecord {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn sqlite_device_token_record(
    row: sqlx::sqlite::SqliteRow,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    Ok(DeviceTokenRecord {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| DeviceTokenRepositoryError::TokenNotFound)?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: sqlite_device_token_timestamp(row.try_get("created_at")?)?,
        last_used_at: row
            .try_get::<Option<String>, _>("last_used_at")?
            .map(sqlite_device_token_timestamp)
            .transpose()?,
        revoked_at: row
            .try_get::<Option<String>, _>("revoked_at")?
            .map(sqlite_device_token_timestamp)
            .transpose()?,
    })
}

fn timescale_device_token_record(
    row: sqlx::postgres::PgRow,
) -> Result<DeviceTokenRecord, DeviceTokenRepositoryError> {
    Ok(DeviceTokenRecord {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        token_prefix: row.try_get("token_prefix")?,
        created_at: row.try_get("created_at")?,
        last_used_at: row.try_get("last_used_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn sqlite_device_token_timestamp(
    value: String,
) -> Result<DateTime<Utc>, DeviceTokenRepositoryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc())
        })
        .map_err(|_| DeviceTokenRepositoryError::InvalidStoredTimestamp)
}

async fn list_management_device_profiles(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementDeviceProfile>, ManagementDeviceProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, telemetry_schema, metric_mapping, reporting_settings
                 FROM device_profiles
                 WHERE tenant_id = ?
                 ORDER BY name, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_device_profile_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, telemetry_schema, metric_mapping, reporting_settings
                 FROM device_profiles
                 WHERE tenant_id = $1
                 ORDER BY name, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_device_profile_from_row)
                .collect()
        }
    }
}

async fn create_management_device_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile: CreateManagementDeviceProfile,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let profile = validate_management_device_profile(
        Uuid::now_v7(),
        profile.name,
        profile.telemetry_schema,
        profile.metric_mapping,
        profile.reporting_settings,
    )?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO device_profiles (
                    id, tenant_id, name, telemetry_schema, metric_mapping, reporting_settings, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(profile.id.to_string())
            .bind(tenant_id.to_string())
            .bind(&profile.name)
            .bind(profile.telemetry_schema.to_string())
            .bind(profile.metric_mapping.to_string())
            .bind(profile.reporting_settings.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO device_profiles (
                    id, tenant_id, name, telemetry_schema, metric_mapping, reporting_settings
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(profile.id)
            .bind(tenant_id)
            .bind(&profile.name)
            .bind(Json(profile.telemetry_schema.clone()))
            .bind(Json(profile.metric_mapping.clone()))
            .bind(Json(profile.reporting_settings.clone()))
            .execute(pool)
            .await
            .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?;
        }
    }
    Ok(profile)
}

async fn update_management_device_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
    profile: UpdateManagementDeviceProfile,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let profile = validate_management_device_profile(
        profile_id,
        profile.name,
        profile.telemetry_schema,
        profile.metric_mapping,
        profile.reporting_settings,
    )?;
    let updated = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE device_profiles
                 SET name = ?, telemetry_schema = ?, metric_mapping = ?,
                     reporting_settings = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
        )
        .bind(&profile.name)
        .bind(profile.telemetry_schema.to_string())
        .bind(profile.metric_mapping.to_string())
        .bind(profile.reporting_settings.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(profile.id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await
        .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE device_profiles
                 SET name = $2, telemetry_schema = $3, metric_mapping = $4,
                     reporting_settings = $5, updated_at = now()
                 WHERE id = $1 AND tenant_id = $6",
        )
        .bind(profile.id)
        .bind(&profile.name)
        .bind(Json(profile.telemetry_schema.clone()))
        .bind(Json(profile.metric_mapping.clone()))
        .bind(Json(profile.reporting_settings.clone()))
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|error| map_management_device_profile_conflict(error, &profile.name))?
        .rows_affected(),
    };
    if updated == 0 {
        return Err(ManagementDeviceProfileError::DeviceProfileNotFound);
    }
    Ok(profile)
}

async fn delete_management_device_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
) -> Result<(), ManagementDeviceProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM device_profiles WHERE id = ? AND tenant_id = ?",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementDeviceProfileError::DeviceProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM devices
                    WHERE device_profile_id = ? AND tenant_id = ? AND deleted_at IS NULL
                 )",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?
                != 0;
            if referenced {
                return Err(ManagementDeviceProfileError::DeviceProfileInUse(profile_id));
            }
            sqlx::query(
                "UPDATE devices
                 SET device_profile_id = NULL
                 WHERE device_profile_id = ? AND tenant_id = ? AND deleted_at IS NOT NULL",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM device_profiles WHERE id = ? AND tenant_id = ?")
                .bind(profile_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("LOCK TABLE devices IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM device_profiles WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementDeviceProfileError::DeviceProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM devices
                    WHERE device_profile_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                 )",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            if referenced {
                return Err(ManagementDeviceProfileError::DeviceProfileInUse(profile_id));
            }
            sqlx::query(
                "UPDATE devices
                 SET device_profile_id = NULL
                 WHERE device_profile_id = $1 AND tenant_id = $2 AND deleted_at IS NOT NULL",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query("DELETE FROM device_profiles WHERE id = $1 AND tenant_id = $2")
                .bind(profile_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

fn sqlite_management_device_profile_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse()
        .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?;
    validate_management_device_profile(
        id,
        row.try_get("name")?,
        serde_json::from_str(&row.try_get::<String, _>("telemetry_schema")?)
            .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?,
        serde_json::from_str(&row.try_get::<String, _>("metric_mapping")?)
            .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?,
        serde_json::from_str(&row.try_get::<String, _>("reporting_settings")?)
            .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)?,
    )
    .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)
}

fn timescale_management_device_profile_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementDeviceProfile, ManagementDeviceProfileError> {
    validate_management_device_profile(
        row.try_get("id")?,
        row.try_get("name")?,
        row.try_get::<Json<serde_json::Value>, _>("telemetry_schema")?
            .0,
        row.try_get::<Json<serde_json::Value>, _>("metric_mapping")?
            .0,
        row.try_get::<Json<serde_json::Value>, _>("reporting_settings")?
            .0,
    )
    .map_err(|_| ManagementDeviceProfileError::InvalidStoredProfile)
}

async fn list_management_asset_profiles(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAssetProfile>, ManagementAssetProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, fields, dashboard_defaults
                 FROM asset_profiles
                 WHERE tenant_id = ?
                 ORDER BY name, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_asset_profile_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, fields, dashboard_defaults
                 FROM asset_profiles
                 WHERE tenant_id = $1
                 ORDER BY name, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_asset_profile_from_row)
                .collect()
        }
    }
}

async fn create_management_asset_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile: CreateManagementAssetProfile,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let profile = validate_management_asset_profile(
        Uuid::now_v7(),
        profile.name,
        profile.fields,
        profile.dashboard_defaults,
    )?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO asset_profiles (
                    id, tenant_id, name, fields, dashboard_defaults, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(profile.id.to_string())
            .bind(tenant_id.to_string())
            .bind(&profile.name)
            .bind(profile.fields.to_string())
            .bind(profile.dashboard_defaults.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO asset_profiles (id, tenant_id, name, fields, dashboard_defaults)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(profile.id)
            .bind(tenant_id)
            .bind(&profile.name)
            .bind(Json(profile.fields.clone()))
            .bind(Json(profile.dashboard_defaults.clone()))
            .execute(pool)
            .await
            .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?;
        }
    }
    Ok(profile)
}

async fn update_management_asset_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
    profile: UpdateManagementAssetProfile,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let profile = validate_management_asset_profile(
        profile_id,
        profile.name,
        profile.fields,
        profile.dashboard_defaults,
    )?;
    let updated = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE asset_profiles
                 SET name = ?, fields = ?, dashboard_defaults = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
        )
        .bind(&profile.name)
        .bind(profile.fields.to_string())
        .bind(profile.dashboard_defaults.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(profile.id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await
        .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE asset_profiles
                 SET name = $2, fields = $3, dashboard_defaults = $4, updated_at = now()
                 WHERE id = $1 AND tenant_id = $5",
        )
        .bind(profile.id)
        .bind(&profile.name)
        .bind(Json(profile.fields.clone()))
        .bind(Json(profile.dashboard_defaults.clone()))
        .bind(tenant_id)
        .execute(pool)
        .await
        .map_err(|error| map_management_asset_profile_conflict(error, &profile.name))?
        .rows_affected(),
    };
    if updated == 0 {
        return Err(ManagementAssetProfileError::AssetProfileNotFound);
    }
    Ok(profile)
}

async fn delete_management_asset_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
) -> Result<(), ManagementAssetProfileError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM asset_profiles WHERE id = ? AND tenant_id = ?",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementAssetProfileError::AssetProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM assets
                    WHERE asset_profile_id = ? AND tenant_id = ?
                 )",
            )
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?
                != 0;
            if referenced {
                return Err(ManagementAssetProfileError::AssetProfileInUse(profile_id));
            }
            sqlx::query("DELETE FROM asset_profiles WHERE id = ? AND tenant_id = ?")
                .bind(profile_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("LOCK TABLE assets IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            let exists = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM asset_profiles WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !exists {
                return Err(ManagementAssetProfileError::AssetProfileNotFound);
            }
            let referenced = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
                    SELECT 1
                    FROM assets
                    WHERE asset_profile_id = $1 AND tenant_id = $2
                 )",
            )
            .bind(profile_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            if referenced {
                return Err(ManagementAssetProfileError::AssetProfileInUse(profile_id));
            }
            sqlx::query("DELETE FROM asset_profiles WHERE id = $1 AND tenant_id = $2")
                .bind(profile_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

fn sqlite_management_asset_profile_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse()
        .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)?;
    validate_management_asset_profile(
        id,
        row.try_get("name")?,
        serde_json::from_str(&row.try_get::<String, _>("fields")?)
            .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)?,
        serde_json::from_str(&row.try_get::<String, _>("dashboard_defaults")?)
            .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)?,
    )
    .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)
}

fn timescale_management_asset_profile_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementAssetProfile, ManagementAssetProfileError> {
    validate_management_asset_profile(
        row.try_get("id")?,
        row.try_get("name")?,
        row.try_get::<Json<serde_json::Value>, _>("fields")?.0,
        row.try_get::<Json<serde_json::Value>, _>("dashboard_defaults")?
            .0,
    )
    .map_err(|_| ManagementAssetProfileError::InvalidStoredProfile)
}
