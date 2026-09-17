use iot_nano_monolith::{PlatformUiIdentity, PlatformUiRenderer};

fn navigation_hrefs(rendered: &str) -> Vec<&str> {
    rendered
        .split(r#"href=""#)
        .skip(1)
        .map(|href_and_rest| {
            href_and_rest
                .split_once('"')
                .expect("rendered navigation href should have a closing quote")
                .0
        })
        .collect()
}

fn route_in_namespace(href: &str, namespace: &str) -> bool {
    href.strip_prefix(namespace).is_some_and(|suffix| {
        suffix.is_empty()
            || suffix.starts_with('/')
            || suffix.starts_with('?')
            || suffix.starts_with('#')
    })
}

fn assert_excludes_navigation_namespaces(rendered: &str, forbidden_namespaces: &[&str]) {
    let hrefs = navigation_hrefs(rendered);

    for namespace in forbidden_namespaces {
        assert!(
            hrefs
                .iter()
                .all(|href| !route_in_namespace(href, namespace)),
            "unexpected {namespace} navigation link: {hrefs:?}",
        );
    }
}

#[test]
fn system_layout_renders_only_system_navigation_and_escapes_identity() {
    let identity = PlatformUiIdentity::new("System <operator>");

    let rendered = PlatformUiRenderer::render_system(&identity).unwrap();

    assert!(rendered.contains("System Console"));
    assert!(rendered.contains("href=\"/system/tenants\""));
    assert!(rendered.contains("href=\"/system/infrastructure\""));
    assert!(rendered.contains("href=\"/system\" aria-current=\"page\""));
    assert_excludes_navigation_namespaces(&rendered, &["/tenant", "/app"]);
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
    assert!(rendered.contains("href=\"/tenant\" aria-current=\"page\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/app"]);
}

#[test]
fn user_layout_renders_only_workspace_navigation() {
    let identity = PlatformUiIdentity::new("Nguyen");

    let rendered = PlatformUiRenderer::render_user(&identity).unwrap();

    assert!(rendered.contains("My Workspace"));
    assert!(rendered.contains("href=\"/app/devices\""));
    assert!(rendered.contains("href=\"/app/assets\""));
    assert!(rendered.contains("href=\"/app\" aria-current=\"page\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/tenant"]);
}
