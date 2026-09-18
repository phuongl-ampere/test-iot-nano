use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{AuditPrincipal, PlatformStoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountClass {
    System,
    Admin,
    User,
}

impl AccountClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Admin => "admin",
            Self::User => "user",
        }
    }
}

pub(crate) fn authorization_account_class(value: &str) -> Result<AccountClass, PlatformStoreError> {
    match value {
        "system" => Ok(AccountClass::System),
        "admin" => Ok(AccountClass::Admin),
        "user" => Ok(AccountClass::User),
        _ => Err(PlatformStoreError::InvalidAuthorizationAccountClass(
            value.to_owned(),
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourcePermission {
    Viewer,
    Controller,
    Manager,
    Owner,
}

impl ResourcePermission {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Controller => "controller",
            Self::Manager => "manager",
            Self::Owner => "owner",
        }
    }

    pub const fn allows(self, required: Self) -> bool {
        self as u8 >= required as u8
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "viewer" => Some(Self::Viewer),
            "controller" => Some(Self::Controller),
            "manager" => Some(Self::Manager),
            "owner" => Some(Self::Owner),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceAccessSource {
    TenantAccount,
    Owner,
    DirectUser,
    Group,
    InheritedUser,
    InheritedGroup,
}

impl ResourceAccessSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TenantAccount => "tenant_account",
            Self::Owner => "owner",
            Self::DirectUser => "direct_user",
            Self::Group => "group",
            Self::InheritedUser => "inherited_user",
            Self::InheritedGroup => "inherited_group",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "tenant_account" => Some(Self::TenantAccount),
            "owner" => Some(Self::Owner),
            "direct_user" => Some(Self::DirectUser),
            "group" => Some(Self::Group),
            "inherited_user" => Some(Self::InheritedUser),
            "inherited_group" => Some(Self::InheritedGroup),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceAccess {
    pub permission: ResourcePermission,
    pub source: ResourceAccessSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Asset,
    Device,
}

impl ResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asset => "asset",
            Self::Device => "device",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationSubject {
    pub user_id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub account_class: AccountClass,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizedDeviceSummary {
    pub device_id: String,
    pub display_name: Option<String>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub access: ResourceAccess,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizedDeviceListEntry {
    pub device_id: String,
    pub display_name: Option<String>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub access: ResourceAccess,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizedAssetSummary {
    pub asset_id: uuid::Uuid,
    pub name: String,
    pub parent_asset_id: Option<uuid::Uuid>,
    pub access: ResourceAccess,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizedAssetListEntry {
    pub asset_id: uuid::Uuid,
    pub name: String,
    pub parent_asset_id: Option<uuid::Uuid>,
    pub access: ResourceAccess,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewUserGroup {
    pub tenant_id: uuid::Uuid,
    pub owner_user_id: uuid::Uuid,
    pub name: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UserGroup {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub owner_user_id: uuid::Uuid,
    pub name: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantUserGroupMember {
    pub user_id: uuid::Uuid,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantUserGroup {
    pub id: uuid::Uuid,
    pub owner_user_id: uuid::Uuid,
    pub name: String,
    pub members: Vec<TenantUserGroupMember>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionCreator {
    User(uuid::Uuid),
    TenantAccount(uuid::Uuid),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipTransferTarget {
    Asset(uuid::Uuid),
    Device(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewResourcePermission {
    pub tenant_id: uuid::Uuid,
    pub subject_user_id: Option<uuid::Uuid>,
    pub subject_group_id: Option<uuid::Uuid>,
    pub asset_id: Option<uuid::Uuid>,
    pub device_id: Option<String>,
    pub permission: ResourcePermission,
    pub inherit_children: bool,
    pub created_by: PermissionCreator,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResourcePermissionRecord {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub subject_user_id: Option<uuid::Uuid>,
    pub subject_group_id: Option<uuid::Uuid>,
    pub asset_id: Option<uuid::Uuid>,
    pub device_id: Option<String>,
    pub permission: ResourcePermission,
    pub inherit_children: bool,
    pub created_by: PermissionCreator,
}

#[derive(Debug, Error)]
pub enum TenantAuthorizationError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("user {user_id} does not belong to tenant {tenant_id}")]
    UserNotFound {
        tenant_id: uuid::Uuid,
        user_id: uuid::Uuid,
    },
    #[error("tenant account {tenant_account_id} does not belong to tenant {tenant_id}")]
    TenantAccountNotFound {
        tenant_id: uuid::Uuid,
        tenant_account_id: uuid::Uuid,
    },
    #[error("group {group_id} does not belong to tenant {tenant_id}")]
    GroupNotFound {
        tenant_id: uuid::Uuid,
        group_id: uuid::Uuid,
    },
    #[error("asset {asset_id} does not belong to tenant {tenant_id}")]
    AssetNotFound {
        tenant_id: uuid::Uuid,
        asset_id: uuid::Uuid,
    },
    #[error("device {device_id:?} does not belong to tenant {tenant_id}")]
    DeviceNotFound {
        tenant_id: uuid::Uuid,
        device_id: String,
    },
    #[error("permission {permission_id} does not belong to tenant {tenant_id}")]
    PermissionNotFound {
        tenant_id: uuid::Uuid,
        permission_id: uuid::Uuid,
    },
    #[error("resource permission must select exactly one user or group subject")]
    InvalidPermissionSubject,
    #[error("resource permission must select exactly one asset or device scope")]
    InvalidPermissionResource,
    #[error("resource permission level must be viewer or manager, got {permission:?}")]
    InvalidPermissionLevel { permission: ResourcePermission },
    #[error("device resource permissions cannot inherit children")]
    DevicePermissionCannotInherit,
    #[error("stored tenant authorization data is invalid")]
    InvalidStoredRecord,
    #[error("system accounts cannot transfer tenant resource ownership")]
    SystemAccountCannotTransferOwnership,
}

pub trait AuthorizationRepository: Send + Sync {
    fn authorization_subject<'a>(
        &'a self,
        user_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizationSubject>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn list_authorized_devices<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<AuthorizedDeviceListEntry>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn list_authorized_assets<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        after: Option<uuid::Uuid>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<AuthorizedAssetListEntry>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn authorized_device<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizedDeviceSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn authorized_asset<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizedAssetSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn device_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
    fn asset_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
}

/// Recent read-only activity for one already-authorized device.
#[derive(Debug, Clone, PartialEq)]
pub struct UserDeviceActivity {
    pub telemetry: Vec<UserDeviceTelemetry>,
    pub alerts: Vec<UserDeviceAlert>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UserDeviceTelemetry {
    pub event_at: DateTime<Utc>,
    pub measurements: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UserDeviceAlert {
    pub rule_name: String,
    pub severity: String,
    pub status: String,
    pub updated_at: DateTime<Utc>,
}

pub trait UserDeviceActivityRepository: Send + Sync {
    fn recent_user_device_activity<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        device_id: &'a str,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<UserDeviceActivity, PlatformStoreError>> + Send + 'a>>;
}

pub trait TenantAuthorizationRepository: Send + Sync {
    fn list_tenant_user_groups<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<TenantUserGroup>, TenantAuthorizationError>> + Send + 'a,
        >,
    >;
    fn list_active_resource_permissions<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ResourcePermissionRecord>, TenantAuthorizationError>>
                + Send
                + 'a,
        >,
    >;
    fn create_user_group<'a>(
        &'a self,
        group: NewUserGroup,
    ) -> Pin<Box<dyn Future<Output = Result<UserGroup, TenantAuthorizationError>> + Send + 'a>>;
    fn add_user_to_group<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        group_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>>;
    fn remove_user_from_group<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        group_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>>;
    fn create_resource_permission<'a>(
        &'a self,
        permission: NewResourcePermission,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ResourcePermissionRecord, TenantAuthorizationError>>
                + Send
                + 'a,
        >,
    >;
    fn revoke_resource_permission<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        permission_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>>;
    fn transfer_resource_ownership<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        target: OwnershipTransferTarget,
        new_owner_user_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>>;
}
