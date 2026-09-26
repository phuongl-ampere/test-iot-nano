use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{ApplicationId, PlatformStoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplicationDomainResourceKind {
    Asset,
    Device,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantProfileDefinition {
    pub id: Uuid,
    pub resource_kind: ApplicationDomainResourceKind,
    pub name: String,
    pub definition: serde_json::Value,
    pub live_view: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantProfileContainmentRule {
    pub parent_profile_id: Uuid,
    pub child_profile_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantProfileConfiguration {
    pub version: u16,
    pub profiles: Vec<TenantProfileDefinition>,
    pub containment_rules: Vec<TenantProfileContainmentRule>,
    pub permission_definitions: serde_json::Value,
}

impl TenantProfileConfiguration {
    pub fn empty() -> Self {
        Self {
            version: 1,
            profiles: Vec::new(),
            containment_rules: Vec::new(),
            permission_definitions: serde_json::json!({}),
        }
    }
}

impl ApplicationDomainResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asset => "asset",
            Self::Device => "device",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "asset" => Some(Self::Asset),
            "device" => Some(Self::Device),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApplicationDomainProfile {
    pub id: Uuid,
    pub app_id: ApplicationId,
    pub resource_kind: ApplicationDomainResourceKind,
    pub name: String,
    pub definition: serde_json::Value,
    pub live_view: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateApplicationDomainProfile {
    pub app_id: ApplicationId,
    pub resource_kind: ApplicationDomainResourceKind,
    pub name: String,
    pub definition: serde_json::Value,
    pub live_view: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateApplicationDomainProfile {
    pub name: String,
    pub definition: serde_json::Value,
    pub live_view: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationAssetProfileRelation {
    pub id: Uuid,
    pub app_id: ApplicationId,
    pub parent_profile_id: Uuid,
    pub child_profile_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateApplicationAssetProfileRelation {
    pub app_id: ApplicationId,
    pub parent_profile_id: Uuid,
    pub child_profile_id: Uuid,
}

#[derive(Debug, Error)]
pub enum ApplicationDomainProfileError {
    #[error("tenant profile configuration version is not supported")]
    UnsupportedConfigurationVersion,
    #[error("tenant profile permission definitions must be an object")]
    PermissionDefinitionsMustBeObject,
    #[error("tenant profile containment rule references an unknown profile")]
    UnknownContainedProfile,
    #[error("application domain application was not found")]
    ApplicationNotFound,
    #[error("application domain profile was not found")]
    ProfileNotFound,
    #[error("application domain profile name is invalid")]
    InvalidName,
    #[error("application domain profile definition must be an object")]
    DefinitionMustBeObject,
    #[error("application domain live view must be an object")]
    LiveViewMustBeObject,
    #[error("application domain profile name already exists: {0:?}")]
    NameConflict(String),
    #[error("application domain profile is still in use: {0}")]
    ProfileInUse(Uuid),
    #[error("application domain profile does not match the requested resource kind")]
    ProfileKindMismatch,
    #[error("stored application domain profile is invalid")]
    InvalidStoredProfile,
    #[error("application resource was not found")]
    ResourceNotFound,
    #[error("asset profile relations require two distinct asset profiles")]
    AssetProfilesOnly,
    #[error("application asset profile relation already exists")]
    RelationConflict,
    #[error("application asset profile relation was not found")]
    RelationNotFound,
    #[error("application domain storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

pub trait TenantProfileRepository: Send + Sync {
    fn export_tenant_profile_configuration<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<TenantProfileConfiguration, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn replace_tenant_profile_configuration<'a>(
        &'a self,
        tenant_id: Uuid,
        configuration: TenantProfileConfiguration,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>>;
}

impl From<PlatformStoreError> for ApplicationDomainProfileError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ApplicationDomainProfileError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ApplicationDomainProfileRepository: Send + Sync {
    fn list_application_domain_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        resource_kind: Option<ApplicationDomainResourceKind>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Vec<ApplicationDomainProfile>, ApplicationDomainProfileError>,
                > + Send
                + 'a,
        >,
    >;
    fn create_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateApplicationDomainProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ApplicationDomainProfile, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn update_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        profile_id: Uuid,
        profile: UpdateApplicationDomainProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ApplicationDomainProfile, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    >;
    fn delete_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>>;
    fn list_application_asset_profile_relations<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<ApplicationAssetProfileRelation>,
                        ApplicationDomainProfileError,
                    >,
                > + Send
                + 'a,
        >,
    >;
    fn create_application_asset_profile_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        relation: CreateApplicationAssetProfileRelation,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<ApplicationAssetProfileRelation, ApplicationDomainProfileError>,
                > + Send
                + 'a,
        >,
    >;
    fn delete_application_asset_profile_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        relation_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>>;
    fn assign_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        resource_kind: ApplicationDomainResourceKind,
        resource_id: &'a str,
        profile_id: Option<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>>;
    fn application_domain_profile_assignment<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        resource_kind: ApplicationDomainResourceKind,
        resource_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<ApplicationDomainProfile>,
                        ApplicationDomainProfileError,
                    >,
                > + Send
                + 'a,
        >,
    >;
}
