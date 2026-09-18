use askama::Template;

const PLATFORM_UI_STYLESHEET: &str = include_str!("../assets/platform-ui.css");

pub(crate) fn stylesheet() -> &'static str {
    PLATFORM_UI_STYLESHEET
}

/// A display-only identity value for a server-rendered platform page.
///
/// Askama HTML-escapes this value in every layout. Authorization and session
/// resolution intentionally remain outside this Phase 0 shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformUiIdentity {
    label: String,
}

impl PlatformUiIdentity {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemTenantRow {
    slug: String,
    status: String,
}

impl SystemTenantRow {
    pub fn new(slug: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            slug: slug.into(),
            status: status.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemPlatformPage {
    tenants: Vec<SystemTenantRow>,
    tenant_account_status: &'static str,
    operational_health: &'static str,
    notice: &'static str,
    has_notice: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantSelectOption {
    value: String,
    label: String,
}

impl TenantSelectOption {
    pub(crate) fn new(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            label: label.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantGroupMemberRow {
    user_id: String,
    username: String,
}

impl TenantGroupMemberRow {
    pub(crate) fn new(user_id: impl Into<String>, username: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            username: username.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantGroupRow {
    id: String,
    name: String,
    owner: String,
    members: Vec<TenantGroupMemberRow>,
}

impl TenantGroupRow {
    pub(crate) fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        owner: impl Into<String>,
        members: Vec<TenantGroupMemberRow>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            owner: owner.into(),
            members,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantGroupsPage {
    users: Vec<TenantSelectOption>,
    groups: Vec<TenantGroupRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantGroupsPage {
    pub(crate) fn new(
        users: Vec<TenantSelectOption>,
        groups: Vec<TenantGroupRow>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            users,
            groups,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantUserRow {
    username: String,
    status: String,
    account_class: String,
}

impl TenantUserRow {
    pub fn new(
        username: impl Into<String>,
        status: impl Into<String>,
        account_class: impl Into<String>,
    ) -> Self {
        Self {
            username: username.into(),
            status: status.into(),
            account_class: account_class.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantUsersPage {
    users: Vec<TenantUserRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantUsersPage {
    pub fn new(users: Vec<TenantUserRow>, notice: Option<&'static str>) -> Self {
        Self {
            users,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantPermissionRow {
    id: String,
    subject: String,
    resource: String,
    permission: String,
    inheritance: String,
}

impl TenantPermissionRow {
    pub(crate) fn new(
        id: impl Into<String>,
        subject: impl Into<String>,
        resource: impl Into<String>,
        permission: impl Into<String>,
        inheritance: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            subject: subject.into(),
            resource: resource.into(),
            permission: permission.into(),
            inheritance: inheritance.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantPermissionsPage {
    subjects: Vec<TenantSelectOption>,
    assets: Vec<TenantSelectOption>,
    devices: Vec<TenantSelectOption>,
    permissions: Vec<TenantPermissionRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantPermissionsPage {
    pub(crate) fn new(
        subjects: Vec<TenantSelectOption>,
        assets: Vec<TenantSelectOption>,
        devices: Vec<TenantSelectOption>,
        permissions: Vec<TenantPermissionRow>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            subjects,
            assets,
            devices,
            permissions,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDeviceRow {
    device_id: String,
    display_name: String,
    activity: String,
    permission: String,
    access_source: String,
}

impl UserDeviceRow {
    pub fn new(
        device_id: impl Into<String>,
        display_name: impl Into<String>,
        activity: impl Into<String>,
        permission: impl Into<String>,
        access_source: impl Into<String>,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            display_name: display_name.into(),
            activity: activity.into(),
            permission: permission.into(),
            access_source: access_source.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDeviceListPage {
    devices: Vec<UserDeviceRow>,
}

impl UserDeviceListPage {
    pub fn new(devices: Vec<UserDeviceRow>) -> Self {
        Self { devices }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDeviceDetailPage {
    device: UserDeviceRow,
}

impl UserDeviceDetailPage {
    pub fn new(device: UserDeviceRow) -> Self {
        Self { device }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAssetRow {
    asset_id: String,
    name: String,
    containment: String,
    permission: String,
    access_source: String,
}

impl UserAssetRow {
    pub fn new(
        asset_id: impl Into<String>,
        name: impl Into<String>,
        containment: impl Into<String>,
        permission: impl Into<String>,
        access_source: impl Into<String>,
    ) -> Self {
        Self {
            asset_id: asset_id.into(),
            name: name.into(),
            containment: containment.into(),
            permission: permission.into(),
            access_source: access_source.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAssetListPage {
    assets: Vec<UserAssetRow>,
}

impl UserAssetListPage {
    pub fn new(assets: Vec<UserAssetRow>) -> Self {
        Self { assets }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAssetDetailPage {
    asset: UserAssetRow,
}

impl UserAssetDetailPage {
    pub fn new(asset: UserAssetRow) -> Self {
        Self { asset }
    }
}

impl SystemPlatformPage {
    pub fn new(tenants: Vec<SystemTenantRow>) -> Self {
        Self {
            tenants,
            tenant_account_status: "Not reported",
            operational_health: "Not reported",
            notice: "",
            has_notice: false,
        }
    }

    pub(crate) fn with_notice(mut self, notice: Option<&'static str>) -> Self {
        if let Some(notice) = notice {
            self.notice = notice;
            self.has_notice = true;
        }
        self
    }
}

/// Renders server-side platform layouts.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlatformUiRenderer;

impl PlatformUiRenderer {
    pub fn render_system(
        identity: &PlatformUiIdentity,
        page: &SystemPlatformPage,
    ) -> Result<String, askama::Error> {
        SystemLayout::new(identity, page).render()
    }

    pub fn render_tenant(identity: &PlatformUiIdentity) -> Result<String, askama::Error> {
        TenantLayout::new(identity).render()
    }

    pub fn render_tenant_users(
        identity: &PlatformUiIdentity,
        page: &TenantUsersPage,
    ) -> Result<String, askama::Error> {
        TenantUsersLayout::new(identity, page).render()
    }

    pub fn render_tenant_groups(
        identity: &PlatformUiIdentity,
        page: &TenantGroupsPage,
    ) -> Result<String, askama::Error> {
        TenantGroupsLayout::new(identity, page).render()
    }

    pub fn render_tenant_permissions(
        identity: &PlatformUiIdentity,
        page: &TenantPermissionsPage,
    ) -> Result<String, askama::Error> {
        TenantPermissionsLayout::new(identity, page).render()
    }

    pub fn render_user(
        identity: &PlatformUiIdentity,
        page: &UserDeviceListPage,
    ) -> Result<String, askama::Error> {
        UserLayout::new(identity, page).render()
    }

    pub fn render_user_device(
        identity: &PlatformUiIdentity,
        page: &UserDeviceDetailPage,
    ) -> Result<String, askama::Error> {
        UserDeviceLayout::new(identity, page).render()
    }

    pub fn render_user_device_unavailable(
        identity: &PlatformUiIdentity,
    ) -> Result<String, askama::Error> {
        UserDeviceUnavailableLayout::new(identity).render()
    }

    pub fn render_user_assets(
        identity: &PlatformUiIdentity,
        page: &UserAssetListPage,
    ) -> Result<String, askama::Error> {
        UserAssetLayout::new(identity, page).render()
    }

    pub fn render_user_asset(
        identity: &PlatformUiIdentity,
        page: &UserAssetDetailPage,
    ) -> Result<String, askama::Error> {
        UserAssetDetailLayout::new(identity, page).render()
    }

    pub fn render_user_asset_unavailable(
        identity: &PlatformUiIdentity,
    ) -> Result<String, askama::Error> {
        UserAssetUnavailableLayout::new(identity).render()
    }
}

#[derive(Template)]
#[template(path = "platform_ui/system.html")]
pub struct SystemLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a SystemPlatformPage,
}

impl<'a> SystemLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a SystemPlatformPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant.html")]
pub struct TenantLayout<'a> {
    identity: &'a PlatformUiIdentity,
}

impl<'a> TenantLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self { identity }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_users.html")]
pub struct TenantUsersLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantUsersPage,
}

impl<'a> TenantUsersLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantUsersPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_groups.html")]
pub struct TenantGroupsLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantGroupsPage,
}

impl<'a> TenantGroupsLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantGroupsPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_permissions.html")]
pub struct TenantPermissionsLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantPermissionsPage,
}

impl<'a> TenantPermissionsLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantPermissionsPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user.html")]
pub struct UserLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a UserDeviceListPage,
}

impl<'a> UserLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a UserDeviceListPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user_device.html")]
pub struct UserDeviceLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a UserDeviceDetailPage,
}

impl<'a> UserDeviceLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a UserDeviceDetailPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user_device_unavailable.html")]
pub struct UserDeviceUnavailableLayout<'a> {
    identity: &'a PlatformUiIdentity,
}

impl<'a> UserDeviceUnavailableLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self { identity }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user_assets.html")]
pub struct UserAssetLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a UserAssetListPage,
}

impl<'a> UserAssetLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a UserAssetListPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user_asset.html")]
pub struct UserAssetDetailLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a UserAssetDetailPage,
}

impl<'a> UserAssetDetailLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a UserAssetDetailPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user_asset_unavailable.html")]
pub struct UserAssetUnavailableLayout<'a> {
    identity: &'a PlatformUiIdentity,
}

impl<'a> UserAssetUnavailableLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self { identity }
    }
}
