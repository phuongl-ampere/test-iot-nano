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
