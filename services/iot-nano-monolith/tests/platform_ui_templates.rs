use iot_nano_monolith::{
    PlatformUiIdentity, PlatformUiRenderer, SystemPlatformPage, SystemTenantRow,
};

fn navigation_hrefs(rendered: &str) -> Vec<&str> {
    let bytes = rendered.as_bytes();
    let mut hrefs = Vec::new();
    let mut cursor = 0;

    while let Some(tag_offset) = rendered[cursor..].find('<') {
        let tag_start = cursor + tag_offset;
        let tag_name_start = tag_start + 1;

        if is_anchor_tag(bytes, tag_name_start) {
            let (href, next_cursor) = anchor_href(rendered, tag_name_start + 1);
            if let Some(href) = href {
                hrefs.push(href);
            }
            cursor = next_cursor;
        } else {
            cursor = skip_html_tag(rendered, tag_name_start);
        }
    }

    hrefs
}

fn is_anchor_tag(bytes: &[u8], tag_name_start: usize) -> bool {
    bytes
        .get(tag_name_start)
        .is_some_and(|byte| byte.eq_ignore_ascii_case(&b'a'))
        && bytes
            .get(tag_name_start + 1)
            .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'>' | b'/'))
}

fn skip_html_tag(rendered: &str, mut cursor: usize) -> usize {
    let bytes = rendered.as_bytes();
    let mut active_quote = None;

    while let Some(byte) = bytes.get(cursor) {
        match active_quote {
            Some(quote) if *byte == quote => active_quote = None,
            Some(_) => {}
            None if matches!(byte, b'\'' | b'"') => active_quote = Some(*byte),
            None if *byte == b'>' => return cursor + 1,
            None => {}
        }
        cursor += 1;
    }

    cursor
}

fn anchor_href(rendered: &str, mut cursor: usize) -> (Option<&str>, usize) {
    let bytes = rendered.as_bytes();
    let mut href = None;

    while cursor < bytes.len() {
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }

        match bytes.get(cursor) {
            None => break,
            Some(b'>') => return (href, cursor + 1),
            Some(b'/') if bytes.get(cursor + 1) == Some(&b'>') => return (href, cursor + 2),
            _ => {}
        }

        let attribute_name_start = cursor;
        while bytes
            .get(cursor)
            .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(byte, b'=' | b'>' | b'/'))
        {
            cursor += 1;
        }

        if attribute_name_start == cursor {
            cursor += 1;
            continue;
        }

        let is_href = rendered[attribute_name_start..cursor].eq_ignore_ascii_case("href");
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }

        if bytes.get(cursor) != Some(&b'=') {
            continue;
        }

        cursor += 1;
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }

        let Some(value_start) = bytes.get(cursor) else {
            break;
        };

        let value_end = if matches!(value_start, b'\'' | b'"') {
            let quote = *value_start;
            cursor += 1;
            let value_start = cursor;
            while bytes.get(cursor) != Some(&quote) {
                if cursor == bytes.len() {
                    return (href, cursor);
                }
                cursor += 1;
            }
            let value_end = cursor;
            cursor += 1;
            (value_start, value_end)
        } else {
            let value_start = cursor;
            while bytes
                .get(cursor)
                .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(byte, b'>'))
            {
                cursor += 1;
            }
            (value_start, cursor)
        };

        if is_href && href.is_none() {
            href = Some(&rendered[value_end.0..value_end.1]);
        }
    }

    (href, cursor)
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
fn navigation_hrefs_extracts_only_anchor_href_attributes() {
    let rendered = r#"
        <a href="/tenant/users">Nested tenant resource</a>
        <a HREF = '/tenant?tab=x'>Tenant query</a>
        <A hReF = "/app#settings">Application fragment</A>
        <a data-href="/tenant/ignored">Attribute is not navigation</a>
        <button href="/app/ignored">Not an anchor</button>
        <div data-href="/tenant/ignored">Not navigation</div>
        <span DATA-HREF = "/app#ignored">Not navigation</span>
        <div data-template='<a HREF = "/tenant/ignored">Not navigation</a>'></div>
    "#;

    assert_eq!(
        navigation_hrefs(rendered),
        vec!["/tenant/users", "/tenant?tab=x", "/app#settings"],
    );
}

#[test]
fn route_namespace_matching_catches_nested_query_and_fragment_navigation() {
    for (href, namespace) in [
        ("/tenant/users", "/tenant"),
        ("/tenant?tab=x", "/tenant"),
        ("/app#settings", "/app"),
    ] {
        assert!(
            route_in_namespace(href, namespace),
            "{href} should belong to the {namespace} namespace",
        );
    }
}

#[test]
fn system_layout_renders_only_system_navigation_and_escapes_identity() {
    let identity = PlatformUiIdentity::new("System <operator>");

    let rendered =
        PlatformUiRenderer::render_system(&identity, &SystemPlatformPage::new(Vec::new())).unwrap();

    assert!(rendered.contains("System Console"));
    assert!(rendered.contains("href=\"/system/tenants\""));
    assert!(rendered.contains("href=\"/system/infrastructure\""));
    assert!(rendered.contains("href=\"/system\" aria-current=\"page\""));
    assert_excludes_navigation_namespaces(&rendered, &["/tenant", "/app"]);
    assert!(rendered.contains("System &#60;operator&#62;"));
    assert!(!rendered.contains("System <operator>"));
}

#[test]
fn system_layout_renders_escaped_tenant_rows_and_neutral_runtime_fields() {
    let identity = PlatformUiIdentity::new("System Account");
    let page = SystemPlatformPage::new(vec![SystemTenantRow::new("tenant-<unsafe>", "suspended")]);

    let rendered = PlatformUiRenderer::render_system(&identity, &page).unwrap();

    assert!(rendered.contains("<th scope=\"col\">Tenant</th>"));
    assert!(rendered.contains("<th scope=\"col\">Status</th>"));
    assert!(rendered.contains("<th scope=\"col\">Tenant account</th>"));
    assert!(rendered.contains("tenant-&#60;unsafe&#62;"));
    assert!(!rendered.contains("tenant-<unsafe>"));
    assert!(rendered.contains("suspended"));
    assert_eq!(rendered.matches("Not reported").count(), 2);
}

#[test]
fn system_layout_renders_lifecycle_forms_without_tenant_secrets() {
    let identity = PlatformUiIdentity::new("System Account");
    let page = SystemPlatformPage::new(vec![
        SystemTenantRow::new("tenant-<unsafe>", "active"),
        SystemTenantRow::new("suspended-tenant", "suspended"),
    ]);

    let rendered = PlatformUiRenderer::render_system(&identity, &page).unwrap();

    for action in [
        "/system/tenants",
        "/system/tenants/suspend",
        "/system/tenants/reactivate",
        "/system/tenants/delete",
        "/system/tenants/tenant-account/reset",
    ] {
        assert!(
            rendered.contains(&format!("action=\"{action}\"")),
            "missing lifecycle form for {action}"
        );
    }
    assert!(rendered.contains("method=\"post\""));
    assert!(rendered.contains("name=\"tenant_account_password\""));
    assert!(rendered.contains("name=\"password\""));
    assert!(rendered.contains("type=\"password\""));
    assert!(rendered.contains("value=\"tenant-&#60;unsafe&#62;\""));
    assert!(!rendered.contains("value=\"TenantPassword@2026\""));
    assert!(!rendered.contains("TenantPassword@2026"));
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
