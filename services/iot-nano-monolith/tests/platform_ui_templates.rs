use iot_nano_monolith::{
    PlatformLoginPage, PlatformUiIdentity, PlatformUiRenderer, SystemInfrastructurePage,
    SystemInfrastructureStatusRow, SystemPlatformPage, SystemTenantRow, TenantAlertRow,
    TenantAlertsPage, TenantAuditPage, TenantAuditRow, TenantOverviewPage, TenantUserRow,
    TenantUsersPage, UserAssetDetailPage, UserAssetListPage, UserAssetRow, UserDeviceDetailPage,
    UserDeviceListPage, UserDeviceRow, UserInvitationPage, UserInvitationRow,
};
use std::fs;
use std::path::Path;

const TENANT_NAVIGATION: [(&str, &str, &str); 15] = [
    ("overview", "/tenant", "Overview"),
    ("devices", "/tenant/devices", "Devices"),
    ("assets", "/tenant/assets", "Assets"),
    ("ota", "/tenant/ota", "OTA"),
    ("alerts", "/tenant/alerts", "Alerts"),
    ("audit", "/tenant/audit", "Audit"),
    ("topology", "/tenant/topology", "Topology"),
    ("relations", "/tenant/relations", "Relations"),
    (
        "profile-configuration",
        "/tenant/profile",
        "Profile Configuration",
    ),
    ("asset-profiles", "/tenant/profiles/asset", "Asset Profiles"),
    (
        "device-profiles",
        "/tenant/profiles/device",
        "Device Profiles",
    ),
    ("users", "/tenant/users", "Users"),
    ("groups", "/tenant/groups", "Groups"),
    ("permissions", "/tenant/permissions", "Permissions"),
    ("applications", "/tenant/applications", "Applications"),
];

const TENANT_TEMPLATES: [(&str, &str); 17] = [
    ("tenant.html", "overview"),
    ("tenant_devices.html", "devices"),
    ("tenant_assets.html", "assets"),
    ("tenant_ota.html", "ota"),
    ("tenant_alerts.html", "alerts"),
    ("tenant_audit.html", "audit"),
    ("tenant_topology.html", "topology"),
    ("tenant_relations.html", "relations"),
    ("tenant_users.html", "users"),
    ("tenant_groups.html", "groups"),
    ("tenant_permissions.html", "permissions"),
    ("tenant_applications.html", "applications"),
    ("tenant_profile.html", "profile-configuration"),
    ("tenant_asset_profiles.html", "asset-profiles"),
    ("tenant_device_profiles.html", "device-profiles"),
    ("tenant_device_credential.html", "devices"),
    ("tenant_device_claim_policy.html", "devices"),
];

#[test]
fn tenant_ota_template_offers_profile_scoped_upload_and_policy_controls() {
    let template = platform_template_source("tenant_ota.html");

    for marker in [
        "data-ota-upload-form",
        "data-ota-policy-form",
        "data-ota-artifact-items",
        "Device profile",
        "Semantic version",
        "/api/management/ota/artifacts",
        "/api/management/ota/policy",
        "x-ota-device-profile-id",
        "x-ota-version",
        "x-ota-filename",
        "require_matching_device_profile",
        "require_newer_version",
    ] {
        assert!(template.contains(marker), "OTA template contains {marker}");
    }
}

fn platform_template_source(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("templates/platform_ui")
            .join(name),
    )
    .unwrap_or_else(|error| panic!("could not read platform template {name}: {error}"))
}

fn platform_stylesheet_source() -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/platform-ui.css"))
        .unwrap_or_else(|error| panic!("could not read platform stylesheet: {error}"))
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
    assert!(rendered.contains("href=\"/system#tenants\">Tenants</a>"));
    assert!(rendered.contains("id=\"tenants\""));
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
    assert!(rendered.contains("href=\"/system#tenants\">Tenants</a>"));
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
    assert!(rendered.contains("name=\"tenant_account_username\""));
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
    let page = TenantOverviewPage::new(2, 3, 4, 1);

    let rendered = PlatformUiRenderer::render_tenant(&identity, &page).unwrap();

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
    assert!(rendered.contains("href=\"/tenant/profile\""));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/app"]);
}

#[test]
fn tenant_profile_template_keeps_json_import_export_separate_from_profile_tabs() {
    let template = platform_template_source("tenant_profile.html");
    let asset_profiles = platform_template_source("tenant_asset_profiles.html");
    let device_profiles = platform_template_source("tenant_device_profiles.html");
    let tabs = platform_template_source("tenant_profile_tabs.html");

    assert!(template.contains("data-tenant-profile-json"));
    assert!(template.contains("/api/management/profile/export"));
    assert!(template.contains("/api/management/profile/import"));
    assert!(template.contains("type=\"file\""));
    assert!(tabs.contains("href=\"/tenant/profile\""));
    assert!(tabs.contains("href=\"/tenant/profiles/asset\""));
    assert!(tabs.contains("href=\"/tenant/profiles/device\""));
    assert_eq!(tabs.matches("aria-current=\"page\"").count(), 3);
    assert!(template.contains("{% include \"platform_ui/tenant_profile_tabs.html\" %}"));
    assert!(asset_profiles.contains("{% include \"platform_ui/tenant_profile_tabs.html\" %}"));
    assert!(device_profiles.contains("{% include \"platform_ui/tenant_profile_tabs.html\" %}"));
    assert!(template.contains("{% let active_profile_tab = \"configuration\" %}"));
    assert!(asset_profiles.contains("{% let active_profile_tab = \"asset\" %}"));
    assert!(device_profiles.contains("{% let active_profile_tab = \"device\" %}"));
    assert!(asset_profiles.contains("action=\"/tenant/profiles/asset\""));
    assert!(device_profiles.contains("action=\"/tenant/profiles/device\""));
}

#[test]
fn tenant_applications_template_does_not_link_to_an_application_profile_page() {
    let template = platform_template_source("tenant_applications.html");

    assert!(!template.contains("Domain profile"));
    assert!(!template.contains("/tenant/applications/{{ application.app_id }}"));
}

#[test]
fn tenant_devices_table_includes_the_assigned_user() {
    let template = platform_template_source("tenant_devices.html");

    assert!(template.contains("<th scope=\"col\">Assigned user</th>"));
    assert!(template.contains("{{ device.assigned_user }}"));
}

#[test]
fn tenant_resource_editors_load_and_save_json_attributes() {
    let assets = platform_template_source("tenant_assets.html");
    let devices = platform_template_source("tenant_devices.html");

    for template in [&assets, &devices] {
        assert!(template.contains("<span>Attributes (JSON)</span>"));
        assert!(template.contains("<textarea name=\"attributes\""));
        assert!(template.contains("attributes: ui.parseObject(fields.attributes.value)"));
    }

    assert!(assets.contains(
        "fields.attributes.value = JSON.stringify(selectedAsset.attributes || {}, null, 2);"
    ));
    assert!(devices.contains(
        "fields.attributes.value = JSON.stringify(selectedDevice.attributes || {}, null, 2);"
    ));
}

#[test]
fn owner_scoped_resource_sharing_uses_assignment_for_tenants_and_sharing_for_owners() {
    let tenant_device = platform_template_source("tenant_devices.html");
    let tenant_asset = platform_template_source("tenant_assets.html");
    let user_device = platform_template_source("user_device.html");
    let user_asset = platform_template_source("user_asset.html");

    for template in [&tenant_device, &tenant_asset] {
        assert!(template.contains("Assigned user"));
        assert!(!template.contains("User access"));
        assert!(!template.contains("permission: fields.permission.value"));
    }
    for template in [&user_device, &user_asset] {
        assert!(template.contains("Invite user"));
        assert!(template.contains("value=\"view\""));
        assert!(template.contains("value=\"control\""));
    }
    assert!(!user_asset.contains("inherit_children"));
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
    let devices = platform_template_source("tenant_devices.html");

    assert!(
        devices.contains(r#"{% extends "platform_ui/base.html" %}"#),
        "the Device page inherits the shared base layout"
    );
    assert!(
        devices.contains("hx-get=\"/tenant/devices\""),
        "the Device page uses HTMX refresh"
    );
    assert_eq!(
        base.matches(r#"<script src="/assets/htmx.min.js"></script>"#)
            .count(),
        1,
        "the shared base layout loads the local HTMX asset once"
    );

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
fn system_infrastructure_and_device_pages_load_local_htmx_exactly_once() {
    let identity = PlatformUiIdentity::new("Platform operator");
    let infrastructure = PlatformUiRenderer::render_system_infrastructure(
        &identity,
        &SystemInfrastructurePage::new("Ready", Vec::new(), Vec::new()),
    )
    .unwrap();
    let base = platform_template_source("base.html");
    let devices = platform_template_source("tenant_devices.html");
    let htmx_src = r#"src="/assets/htmx.min.js""#;

    assert_eq!(
        infrastructure.matches(htmx_src).count(),
        1,
        "rendered System Infrastructure loads local HTMX once"
    );
    assert_eq!(
        base.matches(htmx_src).count(),
        1,
        "the inherited base layout provides Device with local HTMX once"
    );
    assert!(
        devices.contains(r#"{% extends "platform_ui/base.html" %}"#),
        "Device inherits the shared base layout"
    );
    assert_eq!(
        devices.matches(htmx_src).count(),
        0,
        "Device does not add a second local HTMX script"
    );
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
fn tenant_users_layout_renders_a_capability_editor_only_for_user_accounts() {
    let identity = PlatformUiIdentity::new("Tenant account");
    let page = TenantUsersPage::new(
        vec![
            TenantUserRow::with_capabilities(
                "user-id",
                "operator",
                "Active",
                "User",
                vec![
                    "create_devices".to_owned(),
                    "claim_devices".to_owned(),
                    "control_devices".to_owned(),
                ],
            ),
            TenantUserRow::with_capabilities(
                "admin-id",
                "tenant-admin",
                "Active",
                "Admin",
                Vec::new(),
            ),
        ],
        None,
    );

    let rendered = PlatformUiRenderer::render_tenant_users(&identity, &page).unwrap();

    assert!(rendered.contains("data-user-capability-edit"));
    assert!(rendered.contains("data-user-capability-form"));
    assert!(rendered.contains("/api/management/users/"));
    assert!(rendered.contains("/capabilities"));
    assert!(rendered.contains("name=\"create_devices\""));
    assert!(rendered.contains("name=\"claim_devices\""));
    assert!(rendered.contains("name=\"control_devices\""));
    assert!(
        rendered.contains("data-capabilities=\"create_devices claim_devices control_devices\"")
    );
    assert!(rendered.contains("\"create_devices\", \"claim_devices\", \"edit_resources\""));
    assert_eq!(rendered.matches("data-user-id=").count(), 1);
}

#[test]
fn tenant_application_scopes_use_a_described_checklist_and_preserve_the_form_contract() {
    let applications = platform_template_source("tenant_applications.html");
    let stylesheet = platform_stylesheet_source();

    assert!(applications.contains("class=\"scope-picker\""));
    assert!(applications.contains("data-scope-checkbox"));
    assert!(applications.contains("data-allowed-scopes-output"));
    assert!(applications.contains("name=\"allowed_scopes\""));
    assert!(applications.contains("devices:read"));
    assert!(applications.contains("commands:write"));
    assert!(applications.contains("authorization:write"));
    assert!(applications.contains("Read device inventory and status."));
    assert!(applications.contains("syncAllowedScopes"));
    assert!(stylesheet.contains(".scope-picker"));
}

#[test]
fn tenant_users_table_uses_the_shared_console_table_treatment() {
    let users = platform_template_source("tenant_users.html");
    let stylesheet = platform_stylesheet_source();

    assert!(users.contains("class=\"table-scroll\""));
    assert!(users.contains("class=\"data-table user-table\""));
    assert!(users.contains("class=\"user-table__identity\""));
    assert_eq!(
        users
            .matches("class=\"status-chip status-chip--neutral\"")
            .count(),
        2
    );
    assert!(stylesheet.contains(".user-table"));
}

#[test]
fn tenant_alerts_layout_loads_dynamic_rows_without_rendering_server_data() {
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
    assert!(!rendered.contains("Rule &#60;unsafe&#62;"));
    assert!(!rendered.contains("Rule <unsafe>"));
    assert!(rendered.contains("Device"));
    assert!(rendered.contains("Severity"));
    assert!(rendered.contains("Status"));
    assert!(rendered.contains("Last value"));
    assert!(rendered.contains("Updated"));
    assert!(rendered.contains("data-alert-rule-items"));
    assert!(rendered.contains("data-alert-incident-items"));
    assert!(rendered.contains("/api/management/alert-rules"));
    assert!(rendered.contains("cell.textContent = value"));
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
        "View",
        "Shared by owner",
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
fn user_workspace_renders_recipient_specific_invitation_badge() {
    let identity = PlatformUiIdentity::new("Nguyen").with_invitation_count(2);
    let page = UserDeviceListPage::new(Vec::new());

    let rendered = PlatformUiRenderer::render_user(&identity, &page).unwrap();

    assert!(rendered.contains("href=\"/app/invitations\""));
    assert!(rendered.contains("Invitations (2)"));
}

#[test]
fn user_workspace_renders_a_secret_free_device_claim_form_only_with_capability() {
    let identity = PlatformUiIdentity::new("Nguyen");
    let without_claim_capability = UserDeviceListPage::new(Vec::new());
    let with_claim_capability = UserDeviceListPage::new(Vec::new()).with_claim_devices();

    let without_claim = PlatformUiRenderer::render_user(&identity, &without_claim_capability)
        .expect("render workspace without device-claim capability");
    let rendered = PlatformUiRenderer::render_user(&identity, &with_claim_capability)
        .expect("render workspace with device-claim capability");

    assert!(!without_claim.contains("action=\"/app/devices/claim\""));
    assert!(rendered.contains("action=\"/app/devices/claim\" method=\"post\""));

    let (_, claim_form) = rendered
        .split_once("action=\"/app/devices/claim\"")
        .expect("claim-enabled workspace contains the claim form");
    let (claim_form, _) = claim_form
        .split_once("</form>")
        .expect("claim form is closed");

    assert!(claim_form.contains("name=\"serial_number\""));
    assert!(!claim_form.contains("name=\"device_id\""));
    assert!(claim_form.contains(
        "name=\"code\" type=\"text\" autocomplete=\"one-time-code\" maxlength=\"40\" required"
    ));
    assert!(claim_form.contains("Add device"));
    assert!(
        !claim_form.contains("value="),
        "the pairing code must be entered by the User and never rendered back into the form"
    );
    assert!(
        !claim_form.contains("type=\"hidden\""),
        "the claim form must not carry a raw pairing code in hidden markup"
    );
}

#[test]
fn shared_resource_template_has_no_owner_mutation_controls() {
    let identity = PlatformUiIdentity::new("Nguyen");
    let page = UserAssetDetailPage::new(UserAssetRow::new(
        "asset-1",
        "Shared asset",
        "Root asset",
        "View",
        "Shared by owner",
    ));

    let rendered = PlatformUiRenderer::render_user_asset(&identity, &page).unwrap();

    assert!(!rendered.contains("Invite user"));
    assert!(!rendered.contains("Save asset"));
    assert!(!rendered.contains("name=\"parent_asset_id\""));
}

#[test]
fn owner_workspace_forms_render_only_with_owned_asset_options() {
    let identity = PlatformUiIdentity::new("Owner");
    let owned_asset = UserAssetRow::new(
        "asset-1",
        "Owned pump room",
        "Root asset",
        "Control",
        "Owner",
    );
    let asset_page = UserAssetListPage::new(Vec::new()).with_management(vec![owned_asset.clone()]);
    let device_page = UserDeviceDetailPage::new(UserDeviceRow::new(
        "device-1",
        "Pump 1",
        "No activity reported",
        "Control",
        "Owner",
    ))
    .with_management(vec![owned_asset]);

    let rendered_assets = PlatformUiRenderer::render_user_assets(&identity, &asset_page).unwrap();
    let rendered_device = PlatformUiRenderer::render_user_device(&identity, &device_page).unwrap();

    assert!(rendered_assets.contains("Create asset"));
    assert!(rendered_assets.contains("action=\"/app/assets\""));
    assert!(rendered_assets.contains("name=\"parent_asset_id\""));
    assert!(rendered_device.contains("Save device"));
    assert!(rendered_device.contains("name=\"asset_id\""));
    assert!(rendered_device.contains("Owned pump room"));
    assert!(rendered_device.contains("Invite user"));
}

#[test]
fn owner_device_editor_preserves_the_current_asset_selection() {
    let identity = PlatformUiIdentity::new("Owner");
    let selected_asset = UserAssetRow::new(
        "asset-1",
        "Owned pump room",
        "Root asset",
        "Control",
        "Owner",
    )
    .with_selected();
    let page = UserDeviceDetailPage::new(UserDeviceRow::new(
        "device-1",
        "Pump 1",
        "No activity reported",
        "Control",
        "Owner",
    ))
    .with_management(vec![selected_asset]);

    let rendered = PlatformUiRenderer::render_user_device(&identity, &page).unwrap();

    assert!(rendered.contains("<option value=\"asset-1\" selected>Owned pump room</option>"));
}

#[test]
fn invitation_page_renders_incoming_pending_invitation_controls() {
    let identity = PlatformUiIdentity::new("Nguyen").with_invitation_count(1);
    let page = UserInvitationPage::new(vec![UserInvitationRow::new(
        "invitation-1",
        "Device",
        "Pump <unsafe>",
        "owner-a",
        "Control",
    )]);

    let rendered = PlatformUiRenderer::render_user_invitations(&identity, &page).unwrap();

    assert!(rendered.contains("Incoming invitations"));
    assert!(rendered.contains("Pump &#60;unsafe&#62;"));
    assert!(rendered.contains("action=\"/app/invitations/invitation-1/accept\""));
    assert!(rendered.contains("action=\"/app/invitations/invitation-1/cancel\""));
    assert!(rendered.contains("Accept"));
    assert!(rendered.contains("Cancel"));
}

#[test]
fn user_device_detail_renders_only_server_supplied_device_context() {
    let identity = PlatformUiIdentity::new("Nguyen");
    let page = UserDeviceDetailPage::new(UserDeviceRow::new(
        "device-1",
        "Device 1",
        "No activity reported",
        "View",
        "Group permission",
    ));

    let rendered = PlatformUiRenderer::render_user_device(&identity, &page).unwrap();

    assert!(rendered.contains("Device 1"));
    assert!(rendered.contains("No activity reported"));
    assert!(rendered.contains("View"));
    assert!(rendered.contains("Group permission"));
    assert_excludes_navigation_namespaces(&rendered, &["/system", "/tenant"]);
    assert_read_only_page_allows_only_logout_form(&rendered);
    assert!(!rendered.contains("/commands"));
}

#[test]
fn system_and_user_templates_expose_only_supported_console_operations() {
    let system = platform_template_source("system.html");
    let infrastructure = platform_template_source("system_infrastructure.html");
    let infrastructure_status = platform_template_source("system_infrastructure_status.html");
    let user_devices = platform_template_source("user.html");
    let user_assets = platform_template_source("user_assets.html");
    let user_device = platform_template_source("user_device.html");
    let user_asset = platform_template_source("user_asset.html");

    assert!(system.contains("System settings"));
    assert!(system.contains(
        "SMTP, MQTT, retention, and worker tuning require backend configuration routes."
    ));
    assert!(!system.contains("action=\"/system/settings\""));
    assert!(!system.contains("name=\"smtp_"));

    assert!(infrastructure.contains("id=\"infrastructure-refresh\""));
    assert!(infrastructure.contains("href=\"/system/infrastructure\""));
    assert!(infrastructure_status.contains("data-pause-when-hidden"));
    assert!(infrastructure_status.contains("hx-trigger=\"every 5s, visibilityrefresh\""));

    assert!(user_devices.contains("View device"));
    assert!(user_assets.contains("View asset"));
    assert!(user_device.contains("Back to devices"));
    assert!(user_device.contains("href=\"/app/assets\""));
    assert!(user_asset.contains("Back to assets"));

    for template in [&user_devices, &user_assets, &user_device, &user_asset] {
        assert!(!template.contains("/tenant"));
        assert!(!template.contains("/system"));
        assert!(!template.contains("/commands"));
    }
    for template in [&user_devices, &user_assets] {
        assert!(template.contains("{% if page.can_manage %}"));
    }
    assert!(user_device.contains(
        "action=\"/app/devices/{{ page.device.device_id|urlencode_strict }}/permissions\""
    ));
    assert!(user_device.contains("Invite user"));
    assert!(
        user_asset.contains(
            "action=\"/app/assets/{{ page.asset.asset_id|urlencode_strict }}/permissions\""
        )
    );
    assert!(user_asset.contains("Invite user"));
    assert!(!user_asset.contains("inherit_children"));
}

#[test]
fn tenant_device_and_asset_templates_keep_progressive_existing_actions() {
    let devices = platform_template_source("tenant_devices.html");
    let credential = platform_template_source("tenant_device_credential.html");
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

    for template in [&credential] {
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
}

#[test]
fn tenant_devices_editor_reveals_and_replaces_the_active_token_without_a_separate_page() {
    let devices = platform_template_source("tenant_devices.html");
    let token_page = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("templates/platform_ui/tenant_device_tokens.html");

    assert!(devices.contains("data-device-token-issue"));
    assert!(devices.contains(
        "/api/management/devices/${encodeURIComponent(selectedDevice.device_id)}/tokens"
    ));
    assert!(
        devices.contains(
            "/api/management/devices/${encodeURIComponent(selectedDevice.device_id)}/token"
        )
    );
    assert!(devices.contains("Device token"));
    assert!(devices.contains("data-device-token-copy"));
    assert!(devices.contains("Only one token is active."));
    assert!(
        devices.contains("if (!editor.open || selectedDevice?.device_id !== deviceId) return;")
    );
    assert!(
        !token_page.exists(),
        "device token management belongs in the device editor, not a separate page"
    );
}

#[test]
fn tenant_pairing_policy_and_device_editor_keep_pairing_codes_out_of_server_rendered_markup() {
    let policy = platform_template_source("tenant_device_claim_policy.html");
    let devices = platform_template_source("tenant_devices.html");

    assert!(policy.contains("<h1>Device pairing</h1>"));
    assert!(policy.contains("href=\"/tenant/devices\">Back to devices</a>"));
    assert!(policy.contains("action=\"/tenant/devices/claim-policy\" method=\"post\""));
    for marker in [
        "name=\"enabled\" type=\"checkbox\"",
        "name=\"ttl_seconds\" type=\"number\" min=\"60\" max=\"86400\"",
        "name=\"code_length\" type=\"hidden\" value=\"6\"",
        "Pairing codes are always six digits.",
        "name=\"max_failed_attempts\" type=\"number\" min=\"1\" max=\"20\"",
        "name=\"request_cooldown_seconds\" type=\"number\" min=\"10\" max=\"3600\"",
        "Save policy",
    ] {
        assert!(policy.contains(marker), "pairing policy contains {marker}");
    }

    for secret_marker in [
        "name=\"device_id\"",
        "name=\"code\"",
        "{{ page.code }}",
        "{{ page.claim_code",
        "{{ page.pairing_code",
    ] {
        assert!(
            !policy.contains(secret_marker),
            "tenant policy never renders a raw pairing code: {secret_marker}"
        );
    }
    assert!(devices.contains("{{ device.claim_status }}"));
    assert!(devices.contains("/claim-code/revoke"));
    assert!(devices.contains("data-device-claim-code-issue"));
    assert!(devices.contains("data-device-claim-code-copy"));
    assert!(devices.contains(
        "/api/management/devices/${encodeURIComponent(selectedDevice.device_id)}/claim-code"
    ));
    assert!(devices.contains("clearIssuedClaimCode();"));
    assert!(!devices.contains("{{ device.claim_code"));
    assert!(!devices.contains("{{ device.pairing_code"));
}

#[test]
fn tenant_resource_editors_assign_one_owner_and_never_create_direct_grants() {
    let devices = platform_template_source("tenant_devices.html");
    let assets = platform_template_source("tenant_assets.html");

    for (template, form, resource_id, owner_path) in [
        (
            &devices,
            "data-device-owner-form",
            "selectedDevice.device_id",
            "/api/management/devices/${encodeURIComponent(selectedDevice.device_id)}/owner",
        ),
        (
            &assets,
            "data-asset-owner-form",
            "selectedAsset.id",
            "/api/management/assets/${encodeURIComponent(selectedAsset.id)}/owner",
        ),
    ] {
        assert!(template.contains(form), "resource editor contains {form}");
        assert!(template.contains("Assigned user"));
        assert!(template.contains("Transfer or unassigning clears existing shares."));
        assert!(template.contains("user.account_class === \"user\""));
        assert!(template.contains(resource_id));
        assert!(template.contains(owner_path));
        assert!(!template.contains("User access"));
        assert!(!template.contains("/api/management/resource-access?scope="));
        assert!(!template.contains("/tenant/permissions/revoke"));
    }

    for template in [&devices, &assets] {
        assert!(!template.contains("inherit_children"));
    }
}

#[test]
fn tenant_devices_editor_separates_device_and_asset_relations() {
    let devices = platform_template_source("tenant_devices.html");
    let relations = platform_template_source("tenant_relations.html");

    for (template, markers) in [
        (
            &devices,
            &[
                "data-device-device-relation-form",
                "data-device-asset-relation-form",
                "name=\"target_kind\" value=\"device\"",
                "name=\"target_kind\" value=\"asset\"",
                "Related device",
                "Related asset",
                "value=\"depends_on\"",
                "value=\"installed_in\"",
            ][..],
        ),
        (
            &relations,
            &[
                "name=\"target_kind\" value=\"device\"",
                "name=\"target_kind\" value=\"asset\"",
                "name=\"to_device_id\"",
                "name=\"to_asset_id\"",
                "Create device relation",
                "Create asset relation",
            ][..],
        ),
    ] {
        for marker in markers {
            assert!(template.contains(marker), "template contains {marker}");
        }
    }
}

#[test]
fn tenant_devices_editor_lists_recent_raw_telemetry_for_the_selected_range() {
    let devices = platform_template_source("tenant_devices.html");

    for marker in [
        "data-device-telemetry-range",
        "data-device-telemetry-items",
        "data-device-telemetry-status",
        "data-device-telemetry-scroll",
        "<option value=\"1h\">Last hour</option>",
        "<option value=\"1d\">Last day</option>",
        "<option value=\"7d\">Last 7 days</option>",
        "/api/management/devices/${encodeURIComponent(deviceId)}/telemetry?range=${encodeURIComponent(range)}",
        "<th scope=\"col\">Value</th>",
        "formatTelemetryValue(event.measurements)",
        "const TELEMETRY_REFRESH_INTERVAL_MS = 2_000;",
        "window.setInterval",
        "document.hidden",
        "startTelemetryRefresh();",
    ] {
        assert!(devices.contains(marker), "devices editor contains {marker}");
    }

    let stylesheet = platform_stylesheet_source();
    for marker in [
        ".device-telemetry-scroll",
        "max-height: min(252px, 32vh)",
        "overflow-y: auto",
        ".device-telemetry-value",
        "overflow-wrap: anywhere",
    ] {
        assert!(
            stylesheet.contains(marker),
            "telemetry stylesheet contains {marker}"
        );
    }
}

#[test]
fn tenant_operations_templates_make_supported_work_clear_without_inventing_backend_actions() {
    let base = platform_template_source("base.html");
    let device_profiles = platform_template_source("tenant_device_profiles.html");
    let asset_profiles = platform_template_source("tenant_asset_profiles.html");
    let groups = platform_template_source("tenant_groups.html");
    let permissions = platform_template_source("tenant_permissions.html");
    let topology = platform_template_source("tenant_topology.html");
    let relations = platform_template_source("tenant_relations.html");
    let applications = platform_template_source("tenant_applications.html");
    let alerts = platform_template_source("tenant_alerts.html");
    let audit = platform_template_source("tenant_audit.html");
    let stylesheet = platform_stylesheet_source();

    for template in [&device_profiles, &asset_profiles] {
        assert!(template.contains("data-json-input"));
        assert!(template.contains("data-json-feedback"));
        assert!(template.contains("JSON syntax is checked in this browser before submit."));
        assert!(template.contains("ui.parseObject"));
        assert!(template.contains("aria-invalid"));
    }
    assert!(base.contains("const parseObject = (value)"));
    assert!(base.contains("JSON.parse(value)"));

    for marker in [
        "Remove member",
        "Add member",
        "Assign child to gateway",
        "Detach child from gateway",
        "Delete relation",
        "Save application",
    ] {
        assert!(
            [
                groups.as_str(),
                permissions.as_str(),
                topology.as_str(),
                relations.as_str(),
                applications.as_str(),
            ]
            .iter()
            .any(|template| template.contains(marker)),
            "supported action is explicitly labelled: {marker}"
        );
    }

    for template in [&groups, &topology, &relations] {
        assert!(template.contains("<details class=\"confirmation\">"));
    }
    assert!(!permissions.contains("<form"));

    for template in [&applications, &alerts, &audit] {
        assert!(template.contains("class=\"data-table\""));
    }

    for marker in [
        "data-alert-rule-create",
        "data-alert-rule-form",
        "data-alert-rule-items",
        "data-alert-incident-items",
        "/api/management/alert-rules",
        "/api/management/alert-incidents",
        "/archive",
        "Acknowledge",
    ] {
        assert!(alerts.contains(marker), "alerts template contains {marker}");
    }

    for template in [&audit] {
        assert!(template.contains("Read-only"));
        assert!(!template.contains("<form"));
        assert!(!template.contains("Acknowledge"));
        assert!(!template.contains("Archive"));
    }

    for marker in [
        ".field textarea",
        ".json-feedback",
        ".operation-note",
        ".status-chip--neutral",
        ".status-chip--critical",
        ".confirmation",
    ] {
        assert!(stylesheet.contains(marker), "stylesheet contains {marker}");
    }
}
