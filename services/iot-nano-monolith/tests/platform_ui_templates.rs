use iot_nano_monolith::{PlatformUiIdentity, PlatformUiRenderer};

#[test]
fn system_layout_renders_only_system_navigation_and_escapes_identity() {
    let identity = PlatformUiIdentity::new("System <operator>");

    let rendered = PlatformUiRenderer::render_system(&identity).unwrap();

    assert!(rendered.contains("System Console"));
    assert!(rendered.contains("href=\"/system/tenants\""));
    assert!(rendered.contains("href=\"/system/infrastructure\""));
    assert!(!rendered.contains("href=\"/tenant\""));
    assert!(!rendered.contains("href=\"/app\""));
    assert!(rendered.contains("System &#60;operator&#62;"));
    assert!(!rendered.contains("System <operator>"));
}

#[test]
fn tenant_layout_renders_only_tenant_navigation() {
    let identity = PlatformUiIdentity::new("Tenant Account");

    let rendered = PlatformUiRenderer::render_tenant(&identity).unwrap();

    assert!(rendered.contains("Tenant Console"));
    assert!(rendered.contains("href=\"/tenant/users\""));
    assert!(rendered.contains("href=\"/tenant/assets\""));
    assert!(rendered.contains("href=\"/tenant/devices\""));
    assert!(!rendered.contains("href=\"/system\""));
    assert!(!rendered.contains("href=\"/app\""));
}

#[test]
fn user_layout_renders_only_workspace_navigation() {
    let identity = PlatformUiIdentity::new("Nguyen");

    let rendered = PlatformUiRenderer::render_user(&identity).unwrap();

    assert!(rendered.contains("My Workspace"));
    assert!(rendered.contains("href=\"/app/devices\""));
    assert!(rendered.contains("href=\"/app/assets\""));
    assert!(!rendered.contains("href=\"/system\""));
    assert!(!rendered.contains("href=\"/tenant\""));
}
