use askama::Template;

const PLATFORM_UI_STYLESHEET: &str = include_str!("../assets/platform-ui.css");

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

/// Renders the static platform layouts that future server handlers will use.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlatformUiRenderer;

impl PlatformUiRenderer {
    pub fn render_system(identity: &PlatformUiIdentity) -> Result<String, askama::Error> {
        SystemLayout::new(identity).render()
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
    stylesheet: &'static str,
}

impl<'a> SystemLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self {
            identity,
            stylesheet: PLATFORM_UI_STYLESHEET,
        }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant.html")]
pub struct TenantLayout<'a> {
    identity: &'a PlatformUiIdentity,
    stylesheet: &'static str,
}

impl<'a> TenantLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self {
            identity,
            stylesheet: PLATFORM_UI_STYLESHEET,
        }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/user.html")]
pub struct UserLayout<'a> {
    identity: &'a PlatformUiIdentity,
    stylesheet: &'static str,
}

impl<'a> UserLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity) -> Self {
        Self {
            identity,
            stylesheet: PLATFORM_UI_STYLESHEET,
        }
    }
}
