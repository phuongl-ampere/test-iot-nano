use iot_nano_monolith::{
    PlatformLoginPage, PlatformUiIdentity, PlatformUiRenderer, SystemInfrastructurePage,
    SystemInfrastructureStatusRow, SystemPlatformPage, SystemTenantRow, TenantAlertRow,
    TenantAlertsPage, TenantAuditPage, TenantAuditRow, TenantUserRow, TenantUsersPage,
    UserDeviceDetailPage, UserDeviceListPage, UserDeviceRow,
};
use std::fs;
use std::path::Path;

const TENANT_NAVIGATION: [(&str, &str, &str); 13] = [
    ("overview", "/tenant", "Overview"),
    ("devices", "/tenant/devices", "Devices"),
    ("assets", "/tenant/assets", "Assets"),
    (
        "device-profiles",
        "/tenant/profiles/device",
        "Device profiles",
    ),
    ("asset-profiles", "/tenant/profiles/asset", "Asset profiles"),
    ("alerts", "/tenant/alerts", "Alerts"),
    ("audit", "/tenant/audit", "Audit"),
    ("topology", "/tenant/topology", "Topology"),
    ("relations", "/tenant/relations", "Relations"),
    ("users", "/tenant/users", "Users"),
    ("groups", "/tenant/groups", "Groups"),
    ("permissions", "/tenant/permissions", "Permissions"),
    ("applications", "/tenant/applications", "Applications"),
];

const TENANT_TEMPLATES: [(&str, &str); 15] = [
    ("tenant.html", "overview"),
    ("tenant_devices.html", "devices"),
    ("tenant_assets.html", "assets"),
    ("tenant_device_profiles.html", "device-profiles"),
    ("tenant_asset_profiles.html", "asset-profiles"),
    ("tenant_alerts.html", "alerts"),
    ("tenant_audit.html", "audit"),
    ("tenant_topology.html", "topology"),
    ("tenant_relations.html", "relations"),
    ("tenant_users.html", "users"),
    ("tenant_groups.html", "groups"),
    ("tenant_permissions.html", "permissions"),
    ("tenant_applications.html", "applications"),
    ("tenant_device_credential.html", "devices"),
    ("tenant_device_tokens.html", "devices"),
];

fn platform_template_source(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("templates/platform_ui")
            .join(name),
    )
    .unwrap_or_else(|error| panic!("could not read platform template {name}: {error}"))
}

fn active_navigation_key(template: &str) -> &str {
    const DECLARATION: &str = r##"{% let active_nav = ""##;

    assert_eq!(
        template.matches(DECLARATION).count(),
        1,
        "tenant templates declare one active navigation key"
    );

    let (_, declaration) = template
        .split_once(DECLARATION)
        .expect("tenant template declares its active navigation key");
    declaration
        .split_once('"')
        .map(|(key, _)| key)
        .expect("tenant template closes its active navigation key")
}

fn navigation_anchor_source<'a>(navigation: &'a str, href: &str) -> &'a str {
    let href = format!("href=\"{href}\"");
    let href_offset = navigation
        .find(&href)
        .unwrap_or_else(|| panic!("shared navigation contains {href}"));
    let anchor_start = navigation[..href_offset]
        .rfind("<a")
        .expect("navigation href belongs to an anchor");
    let anchor_end = navigation[href_offset..]
        .find("</a>")
        .map(|offset| href_offset + offset + "</a>".len())
        .expect("navigation anchor is closed");

    &navigation[anchor_start..anchor_end]
}

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
fn every_tenant_page_uses_the_fixed_navigation_and_exactly_one_active_item() {
    let navigation = platform_template_source("tenant_navigation.html");
    let expected_hrefs: Vec<_> = TENANT_NAVIGATION.iter().map(|(_, href, _)| *href).collect();

    assert_eq!(navigation_hrefs(&navigation), expected_hrefs);

    for (key, href, label) in TENANT_NAVIGATION {
        let anchor = navigation_anchor_source(&navigation, href);
        let active_condition = format!(r#"active_nav == "{key}""#);

        assert!(anchor.contains(label), "{href} keeps its approved label");
        assert_eq!(
            anchor.matches(&active_condition).count(),
            2,
            "{href} has one class and one aria-current condition"
        );
        assert!(anchor.contains(" active"), "{href} marks the active class");
        assert!(
            anchor.contains("aria-current=\"page\""),
            "{href} marks the current page"
        );
    }

    for (template_name, active_key) in TENANT_TEMPLATES {
        let template = platform_template_source(template_name);

        assert_eq!(
            template
                .matches(r#"{% include "platform_ui/tenant_navigation.html" %}"#)
                .count(),
            1,
            "{template_name} includes the shared tenant navigation once"
        );
        assert_eq!(
            active_navigation_key(&template),
            active_key,
            "{template_name} selects the expected active navigation item"
        );
        assert_eq!(
            TENANT_NAVIGATION
                .iter()
                .filter(|(key, _, _)| *key == active_key)
                .count(),
            1,
            "{template_name} resolves to exactly one active navigation item"
        );
    }
}

#[test]
fn base_layout_provides_the_local_htmx_visibility_pause_primitive() {
    let base = platform_template_source("base.html");

    for marker in [
        "htmx:beforeRequest",
        "document.hidden",
        "data-pause-when-hidden",
        "visibilitychange",
        "visibilityrefresh",
        "window.htmx",
    ] {
        assert!(base.contains(marker), "base layout contains {marker}");
    }
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

#[test]
fn tenant_device_and_asset_templates_keep_progressive_existing_actions() {
    let devices = platform_template_source("tenant_devices.html");
    let credential = platform_template_source("tenant_device_credential.html");
    let tokens = platform_template_source("tenant_device_tokens.html");
    let assets = platform_template_source("tenant_assets.html");

    for marker in [
        "id=\"devices-panel\"",
        "hx-get=\"/tenant/devices\"",
        "hx-trigger=\"every 10s, visibilityrefresh\"",
        "hx-select=\"#devices-panel\"",
        "hx-target=\"#devices-panel\"",
        "hx-swap=\"outerHTML\"",
        "data-pause-when-hidden",
        "href=\"/tenant/devices\"",
        "id=\"device-{{ device.device_id|urlencode_strict }}\"",
        "href=\"/tenant/devices/{{ device.device_id|urlencode_strict }}/tokens\"",
        "action=\"/tenant/devices\"",
    ] {
        assert!(
            devices.contains(marker),
            "devices template contains {marker}"
        );
    }

    let (_, refresh_panel) = devices
        .split_once("id=\"devices-panel\"")
        .expect("devices template contains the refresh panel");
    let (refresh_panel, _) = refresh_panel
        .split_once("</section>")
        .expect("devices refresh panel is closed");
    assert!(
        !refresh_panel.contains("aria-live"),
        "the HTMX-swapped table container must not be a live region"
    );
    assert!(devices.contains(
        "<p id=\"devices-refresh-status\" class=\"visually-hidden\" role=\"status\" aria-live=\"polite\">"
    ));
    assert_eq!(
        devices.matches("aria-live=\"polite\"").count(),
        1,
        "only the concise refresh status owns live semantics"
    );
    let status_offset = devices
        .find("id=\"devices-refresh-status\"")
        .expect("devices template contains a concise refresh status");
    let panel_offset = devices
        .find("id=\"devices-panel\"")
        .expect("devices template contains the refresh panel");
    assert!(
        status_offset < panel_offset,
        "refresh status remains outside the HTMX-swapped table container"
    );

    for template in [&credential, &tokens] {
        assert!(template.contains("data-copy-target"));
        assert!(template.contains("navigator.clipboard.writeText"));
        assert!(template.contains("Copy"));
        assert!(!template.contains("href=\"?credential="));
    }

    for marker in [
        "id=\"asset-{{ asset.id|urlencode_strict }}\"",
        "href=\"#asset-{{ asset.id|urlencode_strict }}\"",
        "action=\"/tenant/assets\"",
    ] {
        assert!(assets.contains(marker), "assets template contains {marker}");
    }

    assert!(tokens.contains("<details class=\"confirmation\""));
    assert!(tokens.contains(
        "action=\"/tenant/devices/{{ page.device_id|urlencode_strict }}/tokens/revoke\""
    ));
    // UI-only pages advertise only actions backed by the existing HTML forms.
    for unsupported in ["/edit", "/delete", "/rotate"] {
        assert!(!devices.contains(unsupported));
        assert!(!assets.contains(unsupported));
        assert!(!tokens.contains(unsupported));
    }
}
