use iot_nano_monolith::{
    PlatformLoginPage, PlatformUiIdentity, PlatformUiRenderer, SystemInfrastructurePage,
    SystemInfrastructureStatusRow, SystemPlatformPage, SystemTenantRow, TenantAlertRow,
    TenantAlertsPage, TenantAuditPage, TenantAuditRow, TenantUserRow, TenantUsersPage,
    UserDeviceDetailPage, UserDeviceListPage, UserDeviceRow,
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

fn assert_read_only_page_allows_only_logout_form(rendered: &str) {
    assert_eq!(rendered.matches("<form").count(), 1);
    assert!(rendered.contains("action=\"/logout\""));
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
    assert!(rendered.contains("href=\"/system\">Tenants</a>"));
    assert!(rendered.contains("href=\"/system/infrastructure\""));
    assert!(rendered.contains("href=\"/system\" aria-current=\"page\""));
    assert_excludes_navigation_namespaces(&rendered, &["/tenant", "/app"]);
    assert!(rendered.contains("System &#60;operator&#62;"));
    assert!(!rendered.contains("System <operator>"));
}

#[test]
fn authenticated_layouts_render_logout_but_login_layout_does_not() {
    let identity = PlatformUiIdentity::new("System Account");
    let system =
        PlatformUiRenderer::render_system(&identity, &SystemPlatformPage::new(Vec::new())).unwrap();
    let login = PlatformUiRenderer::render_login(&PlatformLoginPage::new(false)).unwrap();

    assert!(system.contains("action=\"/logout\""));
    assert!(system.contains("Sign out"));
    assert!(!login.contains("action=\"/logout\""));
}

#[test]
fn system_layout_renders_escaped_tenant_rows_without_runtime_placeholders() {
    let identity = PlatformUiIdentity::new("System Account");
    let page = SystemPlatformPage::new(vec![SystemTenantRow::new("tenant-<unsafe>", "suspended")]);

    let rendered = PlatformUiRenderer::render_system(&identity, &page).unwrap();

    assert!(rendered.contains("<th scope=\"col\">Tenant</th>"));
    assert!(rendered.contains("<th scope=\"col\">Status</th>"));
    assert!(rendered.contains("tenant-&#60;unsafe&#62;"));
    assert!(!rendered.contains("tenant-<unsafe>"));
    assert!(rendered.contains("suspended"));
    assert!(rendered.contains("Not ready"));
    assert!(!rendered.contains("Tenant account"));
    assert!(!rendered.contains("Not reported"));
}

#[test]
fn infrastructure_layout_renders_escaped_status_and_system_navigation_only() {
    let identity = PlatformUiIdentity::new("System <operator>");
    let page = SystemInfrastructurePage::new(
        "Ready",
        vec![
            SystemInfrastructureStatusRow::new(
                "Public HTTP listener",
                "Listening on 127.0.0.1:8080",
            ),
            SystemInfrastructureStatusRow::new(
                "MQTT TLS listener",
                "Listening (TLS endpoint bound)",
            ),
        ],
        vec![
            SystemInfrastructureStatusRow::new("Migrations", "Completed at startup"),
            SystemInfrastructureStatusRow::new("Storage", "SQLite connected"),
            SystemInfrastructureStatusRow::new("TLS", "Loaded for MQTT TLS"),
            SystemInfrastructureStatusRow::new("Unsafe", "<secret-value>"),
        ],
    );

    let rendered = PlatformUiRenderer::render_system_infrastructure(&identity, &page).unwrap();

    assert!(rendered.contains("System infrastructure"));
    assert!(rendered.contains("href=\"/system/infrastructure\" aria-current=\"page\""));
    assert!(rendered.contains("Runtime health"));
    assert!(rendered.contains("Ready"));
    assert!(rendered.contains("Public HTTP listener"));
    assert!(rendered.contains("Listening on 127.0.0.1:8080"));
    assert!(rendered.contains("Migrations"));
    assert!(rendered.contains("SQLite connected"));
    assert!(rendered.contains("&#60;secret-value&#62;"));
    assert!(!rendered.contains("<secret-value>"));
    assert_excludes_navigation_namespaces(&rendered, &["/tenant", "/app"]);
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
        "/system/tenants/tenant-account/disable",
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
    assert!(rendered.contains("href=\"/tenant\" aria-current=\"page\""));
    assert!(rendered.contains("href=\"/tenant/users\""));
    assert!(rendered.contains("href=\"/tenant/groups\""));
    assert!(rendered.contains("href=\"/tenant/permissions\""));
    assert!(rendered.contains("href=\"/tenant/alerts\""));
    assert!(rendered.contains("href=\"/tenant/audit\""));
    assert!(rendered.contains("href=\"/tenant/profiles/device\""));
    assert!(rendered.contains("href=\"/tenant/profiles/asset\""));
    assert!(rendered.contains("href=\"/tenant/applications\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/app"]);
}

#[test]
fn tenant_users_layout_escapes_rows_and_contains_only_tenant_management_fields() {
    let identity = PlatformUiIdentity::new("Tenant <account>");
    let page = TenantUsersPage::new(
        vec![TenantUserRow::new("user-<unsafe>", "Active", "User")],
        Some("User created."),
    );

    let rendered = PlatformUiRenderer::render_tenant_users(&identity, &page).unwrap();

    assert!(rendered.contains("href=\"/tenant/users\" aria-current=\"page\""));
    assert!(rendered.contains("action=\"/tenant/users\""));
    assert!(rendered.contains("name=\"username\""));
    assert!(rendered.contains("name=\"password\""));
    assert!(rendered.contains("type=\"password\""));
    assert!(rendered.contains("User created."));
    assert!(rendered.contains("user-&#60;unsafe&#62;"));
    assert!(!rendered.contains("user-<unsafe>"));
    assert!(rendered.contains("Username"));
    assert!(rendered.contains("Status"));
    assert!(rendered.contains("Account class"));
    assert!(!rendered.contains("Default app"));
    assert!(!rendered.contains("Granted apps"));
    assert!(!rendered.contains("Role"));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/app"]);
}

#[test]
fn tenant_alerts_layout_is_read_only_and_escapes_server_rows() {
    let identity = PlatformUiIdentity::new("Tenant <account>");
    let page = TenantAlertsPage::new(vec![TenantAlertRow::new(
        "device-<unsafe>",
        "Rule <unsafe>",
        "Critical",
        "Open",
        "42.5",
        "2026-09-18T10:20:30Z",
    )]);

    let rendered = PlatformUiRenderer::render_tenant_alerts(&identity, &page).unwrap();

    assert!(rendered.contains("href=\"/tenant/alerts\" aria-current=\"page\""));
    assert!(rendered.contains("Alerts"));
    assert!(rendered.contains("Rule &#60;unsafe&#62;"));
    assert!(!rendered.contains("Rule <unsafe>"));
    assert!(rendered.contains("Device"));
    assert!(rendered.contains("Severity"));
    assert!(rendered.contains("Status"));
    assert!(rendered.contains("Last value"));
    assert!(rendered.contains("Updated"));
    assert_read_only_page_allows_only_logout_form(&rendered);
    assert!(!rendered.contains("name=\"tenant_id\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/app"]);
}

#[test]
fn tenant_audit_layout_escapes_structured_changes_and_uses_tenant_navigation() {
    let identity = PlatformUiIdentity::new("Tenant <account>");
    let page = TenantAuditPage::new(
        vec![TenantAuditRow::new(
            "2026-09-18T10:20:30Z",
            "Tenant account",
            "actor-<unsafe>",
            "gateway.assigned",
            "device: device-<unsafe>",
            r#"{"change":"<unsafe>"}"#,
        )],
        Some("/tenant/audit?after=opaque-cursor"),
    );

    let rendered = PlatformUiRenderer::render_tenant_audit(&identity, &page).unwrap();

    assert!(rendered.contains("href=\"/tenant/audit\" aria-current=\"page\""));
    assert!(rendered.contains("Audit log"));
    assert!(rendered.contains("Timestamp"));
    assert!(rendered.contains("Actor"));
    assert!(rendered.contains("Action"));
    assert!(rendered.contains("Target"));
    assert!(rendered.contains("Changes"));
    assert!(rendered.contains("Older events"));
    assert!(rendered.contains("actor-&#60;unsafe&#62;"));
    assert!(rendered.contains("device-&#60;unsafe&#62;"));
    assert!(rendered.contains("&#60;unsafe&#62;"));
    assert!(!rendered.contains("actor-<unsafe>"));
    assert!(!rendered.contains("device-<unsafe>"));
    assert_read_only_page_allows_only_logout_form(&rendered);
    assert!(!rendered.contains("name=\"tenant_id\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/app"]);
}

#[test]
fn user_layout_renders_only_workspace_navigation() {
    let identity = PlatformUiIdentity::new("Nguyen");
    let page = UserDeviceListPage::new(vec![UserDeviceRow::new(
        "device-<unsafe>",
        "Device <unsafe>",
        "Last seen 2026-09-18T10:20:30Z",
        "Viewer",
        "Direct user permission",
    )]);

    let rendered = PlatformUiRenderer::render_user(&identity, &page).unwrap();

    assert!(rendered.contains("My Devices"));
    assert!(rendered.contains("href=\"/app/devices/device-%3Cunsafe%3E\""));
    assert!(rendered.contains("href=\"/app\" aria-current=\"page\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/tenant"]);
    assert!(rendered.contains("Device &#60;unsafe&#62;"));
    assert!(!rendered.contains("Device <unsafe>"));
}

#[test]
fn user_device_detail_renders_only_server_supplied_device_context() {
    let identity = PlatformUiIdentity::new("Nguyen");
    let page = UserDeviceDetailPage::new(UserDeviceRow::new(
        "device-1",
        "Device 1",
        "No activity reported",
        "Viewer",
        "Group permission",
    ));

    let rendered = PlatformUiRenderer::render_user_device(&identity, &page).unwrap();

    assert!(rendered.contains("Device 1"));
    assert!(rendered.contains("No activity reported"));
    assert!(rendered.contains("Viewer"));
    assert!(rendered.contains("Group permission"));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/tenant"]);
    assert_read_only_page_allows_only_logout_form(&rendered);
    assert!(!rendered.contains("/commands"));
}
