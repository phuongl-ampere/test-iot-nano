pub(crate) const CANONICAL_TABLES: &[&str] = &[
    "platform_schema",
    "devices",
    "telemetry",
    "gateway_event_receipts",
    "telemetry_rollups_5m",
    "telemetry_rollups_1h",
    "system_accounts",
    "tenants",
    "tenant_accounts",
    "login_usernames",
    "users",
    "applications",
    "user_capabilities",
    "application_redirect_uris",
    "oauth_client_secrets",
    "oauth_authorization_codes",
    "oauth_access_tokens",
    "asset_profiles",
    "device_profiles",
    "application_domain_profiles",
    "application_asset_profile_relations",
    "resource_application_profile_assignments",
    "tenant_profile_configurations",
    "resource_tenant_profile_assignments",
    "assets",
    "device_relations",
    "device_asset_relations",
    "user_groups",
    "user_group_members",
    "resource_permissions",
    "resource_invitations",
    "audit_events",
    "device_tokens",
    "tenant_device_claim_policies",
    "device_claim_codes",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
];

pub(crate) const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS platform_schema (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 1)
);

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    display_name TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT,
    asset_id TEXT,
    device_profile_id TEXT,
    deleted_at TEXT,
    is_gateway INTEGER NOT NULL DEFAULT 0,
    gateway_device_id TEXT,
    gateway_topology_version INTEGER NOT NULL DEFAULT 0,
    gateway_last_read_at TEXT,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    owner_user_id TEXT,
    claimed_at TEXT,
    UNIQUE (device_id, tenant_id),
    FOREIGN KEY (asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_profile_id, tenant_id)
        REFERENCES device_profiles(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    CHECK (
        (is_gateway = 1 AND gateway_device_id IS NULL)
        OR (is_gateway = 0 AND gateway_device_id IS NOT device_id)
    )
);

CREATE TABLE IF NOT EXISTS telemetry (
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    measurements TEXT NOT NULL,
    topic TEXT NOT NULL,
    gateway_device_id TEXT,
    UNIQUE (tenant_id, event_at, device_id, boot_id, sequence),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS telemetry_tenant_device_event_at_index
    ON telemetry (tenant_id, device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_tenant_gateway_device_event_at_index
    ON telemetry (tenant_id, gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, gateway_device_id, idempotency_key),
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS telemetry_rollups_5m (
    bucket_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    avg_temperature_c REAL,
    temperature_count INTEGER NOT NULL DEFAULT 0,
    avg_humidity_pct REAL,
    humidity_count INTEGER NOT NULL DEFAULT 0,
    avg_voltage_v REAL,
    voltage_count INTEGER NOT NULL DEFAULT 0,
    avg_current_a REAL,
    current_count INTEGER NOT NULL DEFAULT 0,
    avg_power_w REAL,
    power_count INTEGER NOT NULL DEFAULT 0,
    avg_energy_kwh REAL,
    energy_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, bucket_at, device_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS telemetry_rollups_1h (
    bucket_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    avg_temperature_c REAL,
    temperature_count INTEGER NOT NULL DEFAULT 0,
    avg_humidity_pct REAL,
    humidity_count INTEGER NOT NULL DEFAULT 0,
    avg_voltage_v REAL,
    voltage_count INTEGER NOT NULL DEFAULT 0,
    avg_current_a REAL,
    current_count INTEGER NOT NULL DEFAULT 0,
    avg_power_w REAL,
    power_count INTEGER NOT NULL DEFAULT 0,
    avg_energy_kwh REAL,
    energy_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, bucket_at, device_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS system_accounts (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS system_accounts_one_active_index
    ON system_accounts (status)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('active', 'suspended', 'deleted')),
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tenant_accounts (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    credential_version INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id)
);

CREATE TABLE IF NOT EXISTS login_usernames (
    username TEXT PRIMARY KEY,
    principal_kind TEXT NOT NULL CHECK (principal_kind IN ('system_account', 'tenant_account', 'user')),
    principal_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user'
        CHECK (account_class IN ('system', 'admin', 'user')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS users_id_tenant_id_index
    ON users (id, tenant_id);
CREATE INDEX IF NOT EXISTS users_tenant_username_id_index
    ON users (tenant_id, username, id);
CREATE TRIGGER IF NOT EXISTS users_tenant_id_immutable
BEFORE UPDATE OF tenant_id ON users
FOR EACH ROW WHEN NEW.tenant_id IS NOT OLD.tenant_id
BEGIN
    SELECT RAISE(ABORT, 'users.tenant_id is immutable');
END;
CREATE TRIGGER IF NOT EXISTS system_accounts_login_username_insert
AFTER INSERT ON system_accounts
BEGIN
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, 'system_account', NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS system_accounts_login_username_update
AFTER UPDATE OF username, id ON system_accounts
BEGIN
    DELETE FROM login_usernames
    WHERE principal_kind = 'system_account' AND principal_id = OLD.id;
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, 'system_account', NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS system_accounts_login_username_delete
AFTER DELETE ON system_accounts
BEGIN
    DELETE FROM login_usernames
    WHERE principal_kind = 'system_account' AND principal_id = OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS tenant_accounts_login_username_insert
AFTER INSERT ON tenant_accounts
BEGIN
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, 'tenant_account', NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS tenant_accounts_login_username_update
AFTER UPDATE OF username, id ON tenant_accounts
BEGIN
    DELETE FROM login_usernames
    WHERE principal_kind = 'tenant_account' AND principal_id = OLD.id;
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, 'tenant_account', NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS tenant_accounts_login_username_delete
AFTER DELETE ON tenant_accounts
BEGIN
    DELETE FROM login_usernames
    WHERE principal_kind = 'tenant_account' AND principal_id = OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS users_login_username_insert
AFTER INSERT ON users
BEGIN
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, 'user', NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS users_login_username_update
AFTER UPDATE OF username, id ON users
BEGIN
    DELETE FROM login_usernames
    WHERE principal_kind = 'user' AND principal_id = OLD.id;
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, 'user', NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS users_login_username_delete
AFTER DELETE ON users
BEGIN
    DELETE FROM login_usernames
    WHERE principal_kind = 'user' AND principal_id = OLD.id;
END;

CREATE TABLE IF NOT EXISTS applications (
    app_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('frontend', 'full_stack')),
    launch_url TEXT NOT NULL,
    client_id TEXT NOT NULL UNIQUE,
    allowed_scopes_json TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    UNIQUE (app_id, tenant_id)
);
CREATE TABLE IF NOT EXISTS user_capabilities (
    user_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    capability TEXT NOT NULL CHECK (capability IN (
        'create_assets', 'create_devices', 'claim_devices', 'edit_resources', 'control_devices',
        'share_owned_resources', 'assign_application_profiles', 'manage_device_tokens'
    )),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, capability),
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS application_redirect_uris (
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    PRIMARY KEY (app_id, tenant_id, redirect_uri),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS application_redirect_uris_lookup_index
    ON application_redirect_uris (app_id, tenant_id, redirect_uri);

CREATE TABLE IF NOT EXISTS oauth_client_secrets (
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, secret_hash),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    scopes_json TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    CHECK (expires_at > issued_at),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS oauth_authorization_codes_active_index
    ON oauth_authorization_codes (tenant_id, app_id, expires_at)
    WHERE consumed_at IS NULL;

CREATE TABLE IF NOT EXISTS oauth_access_tokens (
    token_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    scopes_json TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    CHECK (expires_at > issued_at),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS oauth_access_tokens_expiry_index
    ON oauth_access_tokens (expires_at);

CREATE TABLE IF NOT EXISTS asset_profiles (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    fields TEXT NOT NULL DEFAULT '{}',
    dashboard_defaults TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, name)
);
CREATE TABLE IF NOT EXISTS device_profiles (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    telemetry_schema TEXT NOT NULL DEFAULT '{}',
    metric_mapping TEXT NOT NULL DEFAULT '{}',
    reporting_settings TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, name)
);
CREATE TABLE IF NOT EXISTS application_domain_profiles (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('asset', 'device')),
    name TEXT NOT NULL,
    definition TEXT NOT NULL DEFAULT '{}',
    live_view TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (app_id, tenant_id, resource_kind, name),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS application_domain_profiles_tenant_app_kind_index
    ON application_domain_profiles (tenant_id, app_id, resource_kind, name);
CREATE TABLE IF NOT EXISTS application_asset_profile_relations (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    parent_profile_id TEXT NOT NULL,
    child_profile_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (app_id, tenant_id, parent_profile_id, child_profile_id),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_profile_id, tenant_id)
        REFERENCES application_domain_profiles(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (child_profile_id, tenant_id)
        REFERENCES application_domain_profiles(id, tenant_id) ON DELETE RESTRICT,
    CHECK (parent_profile_id <> child_profile_id)
);
CREATE INDEX IF NOT EXISTS application_asset_profile_relations_tenant_app_index
    ON application_asset_profile_relations (tenant_id, app_id, parent_profile_id, child_profile_id);
CREATE TABLE IF NOT EXISTS resource_application_profile_assignments (
    app_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, tenant_id, resource_kind, resource_id),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (profile_id, tenant_id)
        REFERENCES application_domain_profiles(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS resource_application_profile_assignments_profile_index
    ON resource_application_profile_assignments (tenant_id, profile_id);
CREATE TABLE IF NOT EXISTS tenant_profile_configurations (
    tenant_id TEXT PRIMARY KEY REFERENCES tenants(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL,
    configuration TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS resource_tenant_profile_assignments (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, resource_kind, resource_id)
);
CREATE INDEX IF NOT EXISTS resource_tenant_profile_assignments_profile_index
    ON resource_tenant_profile_assignments (tenant_id, profile_id);
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_profile_id TEXT,
    parent_asset_id TEXT,
    owner_user_id TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, parent_asset_id, name),
    FOREIGN KEY (asset_profile_id, tenant_id)
        REFERENCES asset_profiles(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (parent_asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS assets_tenant_parent_index
    ON assets (tenant_id, parent_asset_id);
CREATE UNIQUE INDEX IF NOT EXISTS assets_tenant_root_name_unique_index
    ON assets (tenant_id, name)
    WHERE parent_asset_id IS NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_asset_index
    ON devices (tenant_id, asset_id);
CREATE INDEX IF NOT EXISTS devices_tenant_device_profile_index
    ON devices (tenant_id, device_profile_id)
    WHERE device_profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_active_index
    ON devices (tenant_id, device_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_gateway_active_index
    ON devices (tenant_id, gateway_device_id)
    WHERE deleted_at IS NULL AND gateway_device_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
    ON devices (owner_user_id)
    WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
    ON assets (owner_user_id)
    WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS device_relations (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    from_device_id TEXT NOT NULL,
    to_device_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tenant_id, from_device_id, to_device_id, relation_type),
    FOREIGN KEY (from_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (to_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    CHECK (from_device_id <> to_device_id),
    CHECK (
        length(relation_type) BETWEEN 1 AND 64
        AND relation_type NOT GLOB '*[^A-Za-z0-9_-]*'
        AND relation_type <> 'gateway_child'
    )
);
CREATE INDEX IF NOT EXISTS device_relations_tenant_list_index
    ON device_relations (tenant_id, relation_type, from_device_id, to_device_id, id);

CREATE TABLE IF NOT EXISTS device_asset_relations (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    from_device_id TEXT NOT NULL,
    to_asset_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tenant_id, from_device_id, to_asset_id, relation_type),
    FOREIGN KEY (from_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (to_asset_id, tenant_id)
        REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    CHECK (
        length(relation_type) BETWEEN 1 AND 64
        AND relation_type NOT GLOB '*[^A-Za-z0-9_-]*'
        AND relation_type <> 'gateway_child'
    )
);
CREATE INDEX IF NOT EXISTS device_asset_relations_tenant_list_index
    ON device_asset_relations (tenant_id, relation_type, from_device_id, to_asset_id, id);

CREATE TABLE IF NOT EXISTS user_groups (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    owner_user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    FOREIGN KEY (owner_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS user_group_members (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    group_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id, tenant_id)
        REFERENCES user_groups(id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS user_group_members_tenant_user_group_index
    ON user_group_members (tenant_id, user_id, group_id);
CREATE TABLE IF NOT EXISTS resource_permissions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    subject_user_id TEXT,
    subject_group_id TEXT,
    asset_id TEXT,
    device_id TEXT,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
    inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
    created_by_user_id TEXT,
    created_by_tenant_account_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at TEXT,
    CHECK (
        (subject_user_id IS NOT NULL AND subject_group_id IS NULL)
        OR (subject_user_id IS NULL AND subject_group_id IS NOT NULL)
    ),
    CHECK (
        (asset_id IS NOT NULL AND device_id IS NULL)
        OR (asset_id IS NULL AND device_id IS NOT NULL)
    ),
    CHECK (
        (created_by_user_id IS NOT NULL AND created_by_tenant_account_id IS NULL)
        OR (created_by_user_id IS NULL AND created_by_tenant_account_id IS NOT NULL)
    ),
    CHECK (device_id IS NULL OR inherit_children = 0),
    FOREIGN KEY (subject_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (subject_group_id, tenant_id)
        REFERENCES user_groups(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (asset_id, tenant_id)
        REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by_tenant_account_id, tenant_id)
        REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS resource_permissions_active_device_user_index
    ON resource_permissions (tenant_id, device_id, subject_user_id)
    WHERE revoked_at IS NULL AND device_id IS NOT NULL AND subject_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_permissions_active_device_group_index
    ON resource_permissions (tenant_id, device_id, subject_group_id)
    WHERE revoked_at IS NULL AND device_id IS NOT NULL AND subject_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_permissions_active_asset_user_index
    ON resource_permissions (tenant_id, asset_id, subject_user_id)
    WHERE revoked_at IS NULL AND asset_id IS NOT NULL AND subject_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_permissions_active_asset_group_index
    ON resource_permissions (tenant_id, asset_id, subject_group_id)
    WHERE revoked_at IS NULL AND asset_id IS NOT NULL AND subject_group_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS resource_invitations (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    sender_user_id TEXT NOT NULL,
    recipient_user_id TEXT NOT NULL,
    asset_id TEXT,
    device_id TEXT,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'accepted', 'cancelled', 'withdrawn', 'invalidated')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    accepted_at TEXT,
    closed_at TEXT,
    CHECK (
        (asset_id IS NOT NULL AND device_id IS NULL)
        OR (asset_id IS NULL AND device_id IS NOT NULL)
    ),
    FOREIGN KEY (sender_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (recipient_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (asset_id, tenant_id)
        REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS resource_invitations_pending_asset_recipient_index
    ON resource_invitations (tenant_id, asset_id, recipient_user_id)
    WHERE state = 'pending' AND asset_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS resource_invitations_pending_device_recipient_index
    ON resource_invitations (tenant_id, device_id, recipient_user_id)
    WHERE state = 'pending' AND device_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS resource_invitations_pending_recipient_index
    ON resource_invitations (tenant_id, recipient_user_id, created_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    occurred_at TEXT NOT NULL,
    actor_principal_kind TEXT NOT NULL
        CHECK (actor_principal_kind IN ('system_account', 'tenant_account', 'user')),
    actor_principal_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    changes TEXT NOT NULL CHECK (json_type(changes) = 'object')
);
CREATE INDEX IF NOT EXISTS audit_events_tenant_occurred_at_id_index
    ON audit_events (tenant_id, occurred_at DESC, id DESC);
CREATE TRIGGER IF NOT EXISTS audit_events_immutable_insert
BEFORE INSERT ON audit_events
FOR EACH ROW
WHEN EXISTS (SELECT 1 FROM audit_events WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT, 'audit_events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS audit_events_immutable_update
BEFORE UPDATE ON audit_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'audit_events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS audit_events_immutable_delete
BEFORE DELETE ON audit_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'audit_events are immutable');
END;

CREATE TABLE IF NOT EXISTS device_tokens (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    token_prefix TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL,
    token_ciphertext TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS device_tokens_one_active_per_device
    ON device_tokens (device_id)
    WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS device_tokens_active_prefix_index
    ON device_tokens (token_prefix)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS tenant_device_claim_policies (
    tenant_id TEXT NOT NULL PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    ttl_seconds INTEGER NOT NULL DEFAULT 900 CHECK (ttl_seconds BETWEEN 60 AND 86400),
    code_length INTEGER NOT NULL DEFAULT 12 CHECK (code_length BETWEEN 8 AND 32),
    max_failed_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_failed_attempts BETWEEN 1 AND 20),
    request_cooldown_seconds INTEGER NOT NULL DEFAULT 30
        CHECK (request_cooldown_seconds BETWEEN 10 AND 3600),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS device_claim_codes (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    failed_attempts INTEGER NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
    consumed_at TEXT,
    revoked_at TEXT,
    CHECK (expires_at > issued_at),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS device_claim_codes_one_active_per_device
    ON device_claim_codes (tenant_id, device_id)
    WHERE consumed_at IS NULL AND revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS device_claim_codes_active_expiry_index
    ON device_claim_codes (tenant_id, device_id, expires_at)
    WHERE consumed_at IS NULL AND revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    device_id TEXT,
    metric_key TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    comparison TEXT NOT NULL,
    threshold REAL NOT NULL,
    window_seconds INTEGER,
    for_seconds INTEGER NOT NULL DEFAULT 300,
    resolve_after_seconds INTEGER NOT NULL DEFAULT 300,
    reopen_grace_seconds INTEGER NOT NULL DEFAULT 3600,
    hysteresis REAL,
    severity TEXT NOT NULL DEFAULT 'warning',
    reminder_interval_seconds INTEGER NOT NULL DEFAULT 86400,
    archived_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (tenant_id, rule_type, device_id)
    WHERE enabled = 1;
CREATE INDEX IF NOT EXISTS alert_rules_active_index
    ON alert_rules (tenant_id, created_at DESC, id)
    WHERE archived_at IS NULL;

CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    rule_id TEXT NOT NULL,
    event_at TEXT NOT NULL,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence TEXT NOT NULL,
    PRIMARY KEY (tenant_id, rule_id, event_at, device_id, boot_id, sequence),
    FOREIGN KEY (rule_id, tenant_id)
        REFERENCES alert_rules(id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    rule_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL,
    condition_started_at TEXT NOT NULL,
    recovery_started_at TEXT,
    opened_at TEXT,
    resolved_at TEXT,
    acknowledged_at TEXT,
    acknowledged_by TEXT,
    last_value REAL,
    last_notified_at TEXT,
    last_reminder_at TEXT,
    state_version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    FOREIGN KEY (rule_id, tenant_id)
        REFERENCES alert_rules(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (tenant_id, rule_id, device_id)
    WHERE status IN ('pending', 'open');
CREATE INDEX IF NOT EXISTS alert_incidents_status_updated_index
    ON alert_incidents (tenant_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    incident_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, dedupe_key),
    FOREIGN KEY (incident_id, tenant_id)
        REFERENCES alert_incidents(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (tenant_id, state, next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS command_outbox (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    method TEXT NOT NULL CHECK (trim(method) <> ''),
    params TEXT NOT NULL DEFAULT '{}',
    mode TEXT NOT NULL DEFAULT 'one_way'
        CHECK (mode IN ('one_way', 'two_way')),
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
    expires_at TEXT NOT NULL,
    next_attempt_at TEXT NOT NULL,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    published_at TEXT,
    response TEXT,
    responded_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (tenant_id, state, next_attempt_at)
    WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS command_outbox_tenant_device_index
    ON command_outbox (tenant_id, device_id);
CREATE INDEX IF NOT EXISTS command_outbox_expiring_index
    ON command_outbox (tenant_id, expires_at)
    WHERE state IN ('queued', 'leased')
       OR (state = 'published_to_broker' AND mode = 'two_way');
"#;
