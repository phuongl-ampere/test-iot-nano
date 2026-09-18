use askama::Template;

const PLATFORM_UI_STYLESHEET: &str = include_str!("../assets/platform-ui.css");
const HTMX: &str = include_str!("../assets/htmx.min.js");

pub(crate) fn stylesheet() -> &'static str {
    PLATFORM_UI_STYLESHEET
}

pub(crate) fn htmx() -> &'static str {
    HTMX
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlatformLoginPage {
    has_error: bool,
}

impl PlatformLoginPage {
    pub fn new(has_error: bool) -> Self {
        Self { has_error }
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
    operational_health: String,
    notice: &'static str,
    has_notice: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemInfrastructureStatusRow {
    label: String,
    value: String,
}

impl SystemInfrastructureStatusRow {
    pub fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemInfrastructurePage {
    health: String,
    listeners: Vec<SystemInfrastructureStatusRow>,
    configuration: Vec<SystemInfrastructureStatusRow>,
}

impl SystemInfrastructurePage {
    pub fn new(
        health: impl Into<String>,
        listeners: Vec<SystemInfrastructureStatusRow>,
        configuration: Vec<SystemInfrastructureStatusRow>,
    ) -> Self {
        Self {
            health: health.into(),
            listeners,
            configuration,
        }
    }
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
pub struct TenantAssetRow {
    id: String,
    name: String,
    parent: String,
    status: String,
}

impl TenantAssetRow {
    pub(crate) fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        parent: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            parent: parent.into(),
            status: status.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAssetsPage {
    assets: Vec<TenantAssetRow>,
    parent_assets: Vec<TenantSelectOption>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantAssetsPage {
    pub(crate) fn new(
        assets: Vec<TenantAssetRow>,
        parent_assets: Vec<TenantSelectOption>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            assets,
            parent_assets,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeviceRow {
    device_id: String,
    display_name: String,
    status: String,
    asset: String,
}

impl TenantDeviceRow {
    pub(crate) fn new(
        device_id: impl Into<String>,
        display_name: impl Into<String>,
        status: impl Into<String>,
        asset: impl Into<String>,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            display_name: display_name.into(),
            status: status.into(),
            asset: asset.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDevicesPage {
    devices: Vec<TenantDeviceRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantDevicesPage {
    pub(crate) fn new(devices: Vec<TenantDeviceRow>, notice: Option<&'static str>) -> Self {
        Self {
            devices,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAlertRow {
    device_id: String,
    rule_name: String,
    severity: String,
    status: String,
    last_value: String,
    updated_at: String,
}

impl TenantAlertRow {
    pub fn new(
        device_id: impl Into<String>,
        rule_name: impl Into<String>,
        severity: impl Into<String>,
        status: impl Into<String>,
        last_value: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            rule_name: rule_name.into(),
            severity: severity.into(),
            status: status.into(),
            last_value: last_value.into(),
            updated_at: updated_at.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAlertsPage {
    alerts: Vec<TenantAlertRow>,
}

impl TenantAlertsPage {
    pub fn new(alerts: Vec<TenantAlertRow>) -> Self {
        Self { alerts }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantTopologyRow {
    display_name: String,
    device_id: String,
    role: String,
    gateway: String,
}

impl TenantTopologyRow {
    pub(crate) fn new(
        display_name: impl Into<String>,
        device_id: impl Into<String>,
        role: impl Into<String>,
        gateway: impl Into<String>,
    ) -> Self {
        Self {
            display_name: display_name.into(),
            device_id: device_id.into(),
            role: role.into(),
            gateway: gateway.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantTopologyPage {
    devices: Vec<TenantTopologyRow>,
    gateways: Vec<TenantSelectOption>,
    children: Vec<TenantSelectOption>,
    assigned_children: Vec<TenantSelectOption>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantTopologyPage {
    pub(crate) fn new(
        devices: Vec<TenantTopologyRow>,
        gateways: Vec<TenantSelectOption>,
        children: Vec<TenantSelectOption>,
        assigned_children: Vec<TenantSelectOption>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            devices,
            gateways,
            children,
            assigned_children,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRelationRow {
    id: String,
    from_device: String,
    relation_type: String,
    to_device: String,
}

impl TenantRelationRow {
    pub(crate) fn new(
        id: impl Into<String>,
        from_device: impl Into<String>,
        relation_type: impl Into<String>,
        to_device: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            from_device: from_device.into(),
            relation_type: relation_type.into(),
            to_device: to_device.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRelationsPage {
    devices: Vec<TenantSelectOption>,
    relations: Vec<TenantRelationRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantRelationsPage {
    pub(crate) fn new(
        devices: Vec<TenantSelectOption>,
        relations: Vec<TenantRelationRow>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            devices,
            relations,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantApplicationRow {
    app_id: String,
    client_id: String,
    kind: String,
    launch_url: String,
    redirect_uris: String,
    scopes: String,
    status: String,
}

impl TenantApplicationRow {
    pub(crate) fn new(
        app_id: impl Into<String>,
        client_id: impl Into<String>,
        kind: impl Into<String>,
        launch_url: impl Into<String>,
        redirect_uris: impl Into<String>,
        scopes: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            client_id: client_id.into(),
            kind: kind.into(),
            launch_url: launch_url.into(),
            redirect_uris: redirect_uris.into(),
            scopes: scopes.into(),
            status: status.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantApplicationsPage {
    applications: Vec<TenantApplicationRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantApplicationsPage {
    pub(crate) fn new(
        applications: Vec<TenantApplicationRow>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            applications,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeviceCredentialPage {
    device_id: String,
    display_name: String,
    credential: String,
}

impl TenantDeviceCredentialPage {
    pub(crate) fn new(
        device_id: impl Into<String>,
        display_name: impl Into<String>,
        credential: impl Into<String>,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            display_name: display_name.into(),
            credential: credential.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantProfileRow {
    id: String,
    name: String,
}

impl TenantProfileRow {
    pub(crate) fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeviceProfilesPage {
    profiles: Vec<TenantProfileRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantDeviceProfilesPage {
    pub(crate) fn new(profiles: Vec<TenantProfileRow>, notice: Option<&'static str>) -> Self {
        Self {
            profiles,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantAssetProfilesPage {
    profiles: Vec<TenantProfileRow>,
    notice: &'static str,
    has_notice: bool,
}

impl TenantAssetProfilesPage {
    pub(crate) fn new(profiles: Vec<TenantProfileRow>, notice: Option<&'static str>) -> Self {
        Self {
            profiles,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeviceTokenRow {
    id: String,
    prefix: String,
    created_at: String,
    last_used_at: String,
    status: String,
    revocable: bool,
}

impl TenantDeviceTokenRow {
    pub(crate) fn new(
        id: impl Into<String>,
        prefix: impl Into<String>,
        created_at: impl Into<String>,
        last_used_at: impl Into<String>,
        status: impl Into<String>,
        revocable: bool,
    ) -> Self {
        Self {
            id: id.into(),
            prefix: prefix.into(),
            created_at: created_at.into(),
            last_used_at: last_used_at.into(),
            status: status.into(),
            revocable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantDeviceTokensPage {
    device_id: String,
    display_name: String,
    tokens: Vec<TenantDeviceTokenRow>,
    credential: String,
    has_credential: bool,
    notice: &'static str,
    has_notice: bool,
}

impl TenantDeviceTokensPage {
    pub(crate) fn new(
        device_id: impl Into<String>,
        display_name: impl Into<String>,
        tokens: Vec<TenantDeviceTokenRow>,
        notice: Option<&'static str>,
    ) -> Self {
        Self {
            device_id: device_id.into(),
            display_name: display_name.into(),
            tokens,
            credential: String::new(),
            has_credential: false,
            notice: notice.unwrap_or_default(),
            has_notice: notice.is_some(),
        }
    }

    pub(crate) fn with_credential(mut self, credential: impl Into<String>) -> Self {
        self.credential = credential.into();
        self.has_credential = true;
        self
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
    telemetry: Vec<UserDeviceTelemetryRow>,
    alerts: Vec<UserDeviceAlertRow>,
    has_telemetry: bool,
    has_alerts: bool,
}

impl UserDeviceDetailPage {
    pub fn new(device: UserDeviceRow) -> Self {
        Self {
            device,
            telemetry: Vec::new(),
            alerts: Vec::new(),
            has_telemetry: false,
            has_alerts: false,
        }
    }

    pub fn with_activity(
        mut self,
        telemetry: Vec<UserDeviceTelemetryRow>,
        alerts: Vec<UserDeviceAlertRow>,
    ) -> Self {
        self.has_telemetry = !telemetry.is_empty();
        self.has_alerts = !alerts.is_empty();
        self.telemetry = telemetry;
        self.alerts = alerts;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDeviceTelemetryRow {
    observed_at: String,
    measurements: String,
}

impl UserDeviceTelemetryRow {
    pub fn new(observed_at: impl Into<String>, measurements: impl Into<String>) -> Self {
        Self {
            observed_at: observed_at.into(),
            measurements: measurements.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDeviceAlertRow {
    rule_name: String,
    severity: String,
    status: String,
    updated_at: String,
}

impl UserDeviceAlertRow {
    pub fn new(
        rule_name: impl Into<String>,
        severity: impl Into<String>,
        status: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Self {
        Self {
            rule_name: rule_name.into(),
            severity: severity.into(),
            status: status.into(),
            updated_at: updated_at.into(),
        }
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
            operational_health: "Not ready".to_owned(),
            notice: "",
            has_notice: false,
        }
    }

    pub(crate) fn with_operational_health(mut self, operational_health: impl Into<String>) -> Self {
        self.operational_health = operational_health.into();
        self
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
    pub fn render_login(page: &PlatformLoginPage) -> Result<String, askama::Error> {
        let identity = PlatformUiIdentity::new("Sign in");
        LoginLayout::new(&identity, page).render()
    }

    pub fn render_system(
        identity: &PlatformUiIdentity,
        page: &SystemPlatformPage,
    ) -> Result<String, askama::Error> {
        SystemLayout::new(identity, page).render()
    }

    pub fn render_system_infrastructure(
        identity: &PlatformUiIdentity,
        page: &SystemInfrastructurePage,
    ) -> Result<String, askama::Error> {
        SystemInfrastructureLayout::new(identity, page).render()
    }

    pub(crate) fn render_system_infrastructure_status(
        page: &SystemInfrastructurePage,
    ) -> Result<String, askama::Error> {
        SystemInfrastructureStatusLayout::new(page).render()
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

    pub fn render_tenant_assets(
        identity: &PlatformUiIdentity,
        page: &TenantAssetsPage,
    ) -> Result<String, askama::Error> {
        TenantAssetsLayout::new(identity, page).render()
    }

    pub fn render_tenant_devices(
        identity: &PlatformUiIdentity,
        page: &TenantDevicesPage,
    ) -> Result<String, askama::Error> {
        TenantDevicesLayout::new(identity, page).render()
    }

    pub fn render_tenant_alerts(
        identity: &PlatformUiIdentity,
        page: &TenantAlertsPage,
    ) -> Result<String, askama::Error> {
        TenantAlertsLayout::new(identity, page).render()
    }

    pub fn render_tenant_topology(
        identity: &PlatformUiIdentity,
        page: &TenantTopologyPage,
    ) -> Result<String, askama::Error> {
        TenantTopologyLayout::new(identity, page).render()
    }

    pub fn render_tenant_relations(
        identity: &PlatformUiIdentity,
        page: &TenantRelationsPage,
    ) -> Result<String, askama::Error> {
        TenantRelationsLayout::new(identity, page).render()
    }

    pub fn render_tenant_applications(
        identity: &PlatformUiIdentity,
        page: &TenantApplicationsPage,
    ) -> Result<String, askama::Error> {
        TenantApplicationsLayout::new(identity, page).render()
    }

    pub fn render_tenant_device_credential(
        identity: &PlatformUiIdentity,
        page: &TenantDeviceCredentialPage,
    ) -> Result<String, askama::Error> {
        TenantDeviceCredentialLayout::new(identity, page).render()
    }

    pub fn render_tenant_device_profiles(
        identity: &PlatformUiIdentity,
        page: &TenantDeviceProfilesPage,
    ) -> Result<String, askama::Error> {
        TenantDeviceProfilesLayout::new(identity, page).render()
    }

    pub fn render_tenant_asset_profiles(
        identity: &PlatformUiIdentity,
        page: &TenantAssetProfilesPage,
    ) -> Result<String, askama::Error> {
        TenantAssetProfilesLayout::new(identity, page).render()
    }

    pub fn render_tenant_device_tokens(
        identity: &PlatformUiIdentity,
        page: &TenantDeviceTokensPage,
    ) -> Result<String, askama::Error> {
        TenantDeviceTokensLayout::new(identity, page).render()
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
#[template(path = "platform_ui/login.html")]
pub struct LoginLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a PlatformLoginPage,
}

impl<'a> LoginLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a PlatformLoginPage) -> Self {
        Self { identity, page }
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
#[template(path = "platform_ui/system_infrastructure.html")]
pub struct SystemInfrastructureLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a SystemInfrastructurePage,
}

impl<'a> SystemInfrastructureLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a SystemInfrastructurePage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/system_infrastructure_status.html")]
pub(crate) struct SystemInfrastructureStatusLayout<'a> {
    page: &'a SystemInfrastructurePage,
}

impl<'a> SystemInfrastructureStatusLayout<'a> {
    fn new(page: &'a SystemInfrastructurePage) -> Self {
        Self { page }
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
#[template(path = "platform_ui/tenant_assets.html")]
pub struct TenantAssetsLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantAssetsPage,
}

impl<'a> TenantAssetsLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantAssetsPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_devices.html")]
pub struct TenantDevicesLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantDevicesPage,
}

impl<'a> TenantDevicesLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantDevicesPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_alerts.html")]
pub struct TenantAlertsLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantAlertsPage,
}

impl<'a> TenantAlertsLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantAlertsPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_topology.html")]
pub struct TenantTopologyLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantTopologyPage,
}

impl<'a> TenantTopologyLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantTopologyPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_relations.html")]
pub struct TenantRelationsLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantRelationsPage,
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_applications.html")]
pub struct TenantApplicationsLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantApplicationsPage,
}

impl<'a> TenantApplicationsLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantApplicationsPage) -> Self {
        Self { identity, page }
    }
}

impl<'a> TenantRelationsLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantRelationsPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_device_credential.html")]
pub struct TenantDeviceCredentialLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantDeviceCredentialPage,
}

impl<'a> TenantDeviceCredentialLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantDeviceCredentialPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_device_profiles.html")]
pub struct TenantDeviceProfilesLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantDeviceProfilesPage,
}

impl<'a> TenantDeviceProfilesLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantDeviceProfilesPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_asset_profiles.html")]
pub struct TenantAssetProfilesLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantAssetProfilesPage,
}

impl<'a> TenantAssetProfilesLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantAssetProfilesPage) -> Self {
        Self { identity, page }
    }
}

#[derive(Template)]
#[template(path = "platform_ui/tenant_device_tokens.html")]
pub struct TenantDeviceTokensLayout<'a> {
    identity: &'a PlatformUiIdentity,
    page: &'a TenantDeviceTokensPage,
}

impl<'a> TenantDeviceTokensLayout<'a> {
    pub fn new(identity: &'a PlatformUiIdentity, page: &'a TenantDeviceTokensPage) -> Self {
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
