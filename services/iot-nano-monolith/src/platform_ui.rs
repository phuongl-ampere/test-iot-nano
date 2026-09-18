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
}

impl SystemPlatformPage {
    pub fn new(tenants: Vec<SystemTenantRow>) -> Self {
        Self {
            tenants,
            tenant_account_status: "Not reported",
            operational_health: "Not reported",
        }
    }
}

/// Renders the static platform layouts that future server handlers will use.
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

    pub fn render_user(identity: &PlatformUiIdentity) -> Result<String, askama::Error> {
        UserLayout::new(identity).render()
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
}

impl<'a> UserLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self { identity }
    }
}
