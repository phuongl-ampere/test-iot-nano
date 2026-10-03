CREATE TABLE IF NOT EXISTS platform_schema (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 5)
);

CREATE TABLE IF NOT EXISTS system_accounts (
    id UUID PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    serial_number_length INTEGER NOT NULL DEFAULT 9 CHECK (serial_number_length BETWEEN 6 AND 32),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS system_accounts_one_active_index
    ON system_accounts (status)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('active', 'suspended', 'deleted')),
    auto_generate_serial_number BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenant_accounts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL UNIQUE REFERENCES tenants(id) ON DELETE RESTRICT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    credential_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id)
);

CREATE TABLE IF NOT EXISTS login_usernames (
    username TEXT PRIMARY KEY,
    principal_kind TEXT NOT NULL CHECK (principal_kind IN ('system_account', 'tenant_account', 'user')),
    principal_id UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user' CHECK (account_class IN ('system', 'admin', 'user')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS users_id_tenant_id_index
    ON users (id, tenant_id);
CREATE INDEX IF NOT EXISTS users_tenant_username_id_index
    ON users (tenant_id, username, id);
CREATE FUNCTION prevent_users_tenant_id_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
        RAISE EXCEPTION 'users.tenant_id is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER users_tenant_id_immutable
    BEFORE UPDATE OF tenant_id ON users
    FOR EACH ROW EXECUTE FUNCTION prevent_users_tenant_id_update();

CREATE FUNCTION sync_login_username()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM login_usernames
        WHERE principal_kind = TG_ARGV[0] AND principal_id = OLD.id;
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.username IS NOT DISTINCT FROM OLD.username
           AND NEW.id IS NOT DISTINCT FROM OLD.id THEN
            RETURN NEW;
        END IF;
        DELETE FROM login_usernames
        WHERE principal_kind = TG_ARGV[0] AND principal_id = OLD.id;
    END IF;
    INSERT INTO login_usernames (username, principal_kind, principal_id)
    VALUES (NEW.username, TG_ARGV[0], NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER system_accounts_login_username_sync
    BEFORE INSERT OR UPDATE OR DELETE ON system_accounts
    FOR EACH ROW EXECUTE FUNCTION sync_login_username('system_account');
CREATE TRIGGER tenant_accounts_login_username_sync
    BEFORE INSERT OR UPDATE OR DELETE ON tenant_accounts
    FOR EACH ROW EXECUTE FUNCTION sync_login_username('tenant_account');
CREATE TRIGGER users_login_username_sync
    BEFORE INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW EXECUTE FUNCTION sync_login_username('user');

CREATE TABLE IF NOT EXISTS applications (
    app_id TEXT PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('frontend', 'full_stack')),
    launch_url TEXT NOT NULL,
    client_id TEXT NOT NULL UNIQUE,
    allowed_scopes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (app_id, tenant_id)
);
CREATE TABLE IF NOT EXISTS user_capabilities (
    user_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    capability TEXT NOT NULL CHECK (capability IN (
        'create_assets', 'create_devices', 'claim_devices', 'assign_devices_to_assets', 'edit_resources', 'control_devices',
        'share_owned_resources', 'assign_application_profiles', 'manage_device_tokens'
    )),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, capability),
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS application_redirect_uris (
    app_id TEXT NOT NULL,
    tenant_id UUID NOT NULL,
    redirect_uri TEXT NOT NULL,
    PRIMARY KEY (app_id, tenant_id, redirect_uri),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS application_redirect_uris_lookup_index
    ON application_redirect_uris (app_id, tenant_id, redirect_uri);
CREATE TABLE IF NOT EXISTS oauth_client_secrets (
    app_id TEXT NOT NULL,
    tenant_id UUID NOT NULL,
    secret_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, secret_hash),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id UUID NOT NULL,
    user_id UUID NOT NULL,
    redirect_uri TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    scopes_json JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
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
    tenant_id UUID NOT NULL,
    user_id UUID,
    scopes_json JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > issued_at),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS oauth_access_tokens_expiry_index
    ON oauth_access_tokens (expires_at);
CREATE TABLE IF NOT EXISTS asset_profiles (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    dashboard_defaults JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, name)
);
CREATE TABLE IF NOT EXISTS device_profiles (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    telemetry_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_mapping JSONB NOT NULL DEFAULT '{}'::jsonb,
    reporting_settings JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, name)
);
CREATE TABLE IF NOT EXISTS application_domain_profiles (
    id UUID PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('asset', 'device')),
    name TEXT NOT NULL,
    definition JSONB NOT NULL DEFAULT '{}'::jsonb,
    live_view JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    UNIQUE (app_id, tenant_id, resource_kind, name),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS application_domain_profiles_tenant_app_kind_index
    ON application_domain_profiles (tenant_id, app_id, resource_kind, name);
CREATE TABLE IF NOT EXISTS application_asset_profile_relations (
    id UUID PRIMARY KEY,
    app_id TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    parent_profile_id UUID NOT NULL,
    child_profile_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    profile_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, tenant_id, resource_kind, resource_id),
    FOREIGN KEY (app_id, tenant_id)
        REFERENCES applications(app_id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (profile_id, tenant_id)
        REFERENCES application_domain_profiles(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS resource_application_profile_assignments_profile_index
    ON resource_application_profile_assignments (tenant_id, profile_id);
CREATE TABLE IF NOT EXISTS tenant_profile_configurations (
    tenant_id UUID PRIMARY KEY REFERENCES tenants(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL,
    configuration JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS resource_tenant_profile_assignments (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    profile_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, resource_kind, resource_id)
);
CREATE INDEX IF NOT EXISTS resource_tenant_profile_assignments_profile_index
    ON resource_tenant_profile_assignments (tenant_id, profile_id);
CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_profile_id UUID,
    parent_asset_id UUID,
    owner_user_id UUID,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, parent_asset_id, name),
    CONSTRAINT assets_asset_profile_id_fkey
        FOREIGN KEY (asset_profile_id, tenant_id)
        REFERENCES asset_profiles(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (parent_asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS assets_tenant_root_name_unique_index
    ON assets (tenant_id, name)
    WHERE parent_asset_id IS NULL;
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    serial_number TEXT,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    display_name TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    asset_id UUID,
    device_profile_id UUID,
    deleted_at TIMESTAMPTZ, is_gateway BOOLEAN NOT NULL DEFAULT FALSE,
    gateway_device_id TEXT,
    gateway_topology_version INTEGER NOT NULL DEFAULT 0,
    owner_user_id UUID, claimed_at TIMESTAMPTZ,
    UNIQUE (device_id, tenant_id),
    FOREIGN KEY (asset_id, tenant_id) REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    CONSTRAINT devices_device_profile_id_fkey
        FOREIGN KEY (device_profile_id, tenant_id)
        REFERENCES device_profiles(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (owner_user_id, tenant_id) REFERENCES users(id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    CHECK ((is_gateway = TRUE AND gateway_device_id IS NULL)
        OR (is_gateway = FALSE AND gateway_device_id IS DISTINCT FROM device_id))
);
CREATE UNIQUE INDEX IF NOT EXISTS devices_serial_number_unique_index
    ON devices (tenant_id, lower(serial_number))
    WHERE serial_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_asset_id_index ON devices (asset_id) WHERE asset_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_asset_index ON devices (tenant_id, asset_id) WHERE asset_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_device_profile_id_index ON devices (device_profile_id) WHERE device_profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_device_profile_index
    ON devices (tenant_id, device_profile_id) WHERE device_profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_active_index
    ON devices (tenant_id, device_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS devices_tenant_gateway_device_id_index
    ON devices (tenant_id, gateway_device_id)
    WHERE deleted_at IS NULL AND gateway_device_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_owner_user_id_index ON devices (owner_user_id) WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS assets_owner_user_id_index ON assets (owner_user_id) WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS tenant_device_claim_policies (
    tenant_id UUID PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ttl_seconds INTEGER NOT NULL DEFAULT 900 CHECK (ttl_seconds BETWEEN 60 AND 86400),
    code_length INTEGER NOT NULL DEFAULT 6 CHECK (code_length = 6),
    max_failed_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_failed_attempts BETWEEN 1 AND 20),
    request_cooldown_seconds INTEGER NOT NULL DEFAULT 30
        CHECK (request_cooldown_seconds BETWEEN 10 AND 3600),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenant_ota_policies (
    tenant_id UUID NOT NULL PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    require_matching_device_profile BOOLEAN NOT NULL DEFAULT TRUE,
    require_newer_version BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ota_artifacts (
    id UUID NOT NULL PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    device_profile_id UUID NOT NULL,
    version TEXT NOT NULL,
    filename TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, device_profile_id, version),
    FOREIGN KEY (device_profile_id, tenant_id)
        REFERENCES device_profiles(id, tenant_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS ota_artifacts_tenant_profile_version_index
    ON ota_artifacts (tenant_id, device_profile_id, version);

CREATE TABLE IF NOT EXISTS device_claim_codes (
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    code_hash TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    failed_attempts INTEGER NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
    consumed_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
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

CREATE TABLE IF NOT EXISTS device_relations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    from_device_id TEXT NOT NULL,
    to_device_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, from_device_id, to_device_id, relation_type),
    FOREIGN KEY (from_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (to_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    CHECK (from_device_id <> to_device_id),
    CHECK (
        relation_type ~ '^[A-Za-z0-9_-]{1,64}$'
        AND relation_type <> 'gateway_child'
    )
);
CREATE INDEX IF NOT EXISTS device_relations_tenant_list_index
    ON device_relations (tenant_id, relation_type, from_device_id, to_device_id, id);

CREATE TABLE IF NOT EXISTS device_asset_relations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    from_device_id TEXT NOT NULL,
    to_asset_id UUID NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, from_device_id, to_asset_id, relation_type),
    FOREIGN KEY (from_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (to_asset_id, tenant_id)
        REFERENCES assets(id, tenant_id) ON DELETE RESTRICT,
    CHECK (
        relation_type ~ '^[A-Za-z0-9_-]{1,64}$'
        AND relation_type <> 'gateway_child'
    )
);
CREATE INDEX IF NOT EXISTS device_asset_relations_tenant_list_index
    ON device_asset_relations (tenant_id, relation_type, from_device_id, to_asset_id, id);

CREATE TABLE IF NOT EXISTS device_tokens (
    id UUID PRIMARY KEY, device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    token_prefix TEXT NOT NULL UNIQUE, token_hash TEXT NOT NULL, token_ciphertext TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_used_at TIMESTAMPTZ, revoked_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS device_tokens_one_active_per_device ON device_tokens (device_id) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS device_tokens_active_prefix_index ON device_tokens (token_prefix) WHERE revoked_at IS NULL;
CREATE TABLE IF NOT EXISTS tenant_personal_access_tokens (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    tenant_account_user_id UUID NOT NULL,
    name TEXT NOT NULL,
    token_prefix TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_account_user_id, tenant_id)
        REFERENCES tenant_accounts(id, tenant_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS tenant_personal_access_tokens_one_active_per_account
    ON tenant_personal_access_tokens (tenant_account_user_id)
    WHERE revoked_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS tenant_personal_access_tokens_active_hash_index
    ON tenant_personal_access_tokens (token_hash)
    WHERE revoked_at IS NULL;
CREATE TABLE IF NOT EXISTS user_groups (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    owner_user_id UUID NOT NULL,
    name TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    FOREIGN KEY (owner_user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS user_group_members (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id),
    FOREIGN KEY (group_id, tenant_id)
        REFERENCES user_groups(id, tenant_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id, tenant_id)
        REFERENCES users(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS user_group_members_tenant_user_group_index
    ON user_group_members (tenant_id, user_id, group_id);
CREATE TABLE IF NOT EXISTS resource_permissions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    subject_user_id UUID,
    subject_group_id UUID,
    asset_id UUID,
    device_id TEXT,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
    inherit_children BOOLEAN NOT NULL DEFAULT FALSE,
    created_by_user_id UUID,
    created_by_tenant_account_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
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
    CHECK (device_id IS NULL OR inherit_children = FALSE),
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
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    sender_user_id UUID NOT NULL,
    recipient_user_id UUID NOT NULL,
    asset_id UUID,
    device_id TEXT,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'accepted', 'cancelled', 'withdrawn', 'invalidated')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
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
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    occurred_at TIMESTAMPTZ NOT NULL,
    actor_principal_kind TEXT NOT NULL
        CHECK (actor_principal_kind IN ('system_account', 'tenant_account', 'user')),
    actor_principal_id UUID NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    changes JSONB NOT NULL CHECK (jsonb_typeof(changes) = 'object')
);
CREATE INDEX IF NOT EXISTS audit_events_tenant_occurred_at_id_index
    ON audit_events (tenant_id, occurred_at DESC, id DESC);
CREATE FUNCTION prevent_audit_events_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_events are immutable';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER audit_events_immutable
    BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION prevent_audit_events_mutation();
CREATE TRIGGER audit_events_immutable_truncate
    BEFORE TRUNCATE ON audit_events
    FOR EACH STATEMENT EXECUTE FUNCTION prevent_audit_events_mutation();

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS device_runtime_state (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    last_seen_at TIMESTAMPTZ, gateway_last_read_at TIMESTAMPTZ,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    PRIMARY KEY (tenant_id, device_id),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS telemetry (
    event_at TIMESTAMPTZ NOT NULL, received_at TIMESTAMPTZ NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL, boot_id UUID NOT NULL,
    sequence BIGINT NOT NULL, measurements JSONB NOT NULL, topic TEXT NOT NULL, gateway_device_id TEXT,
    CONSTRAINT telemetry_event_identity
        UNIQUE (tenant_id, event_at, device_id, boot_id, sequence),
    FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT,
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
SELECT public.create_hypertable('telemetry', 'event_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS telemetry_tenant_device_event_at_index
    ON telemetry (tenant_id, device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_tenant_gateway_device_event_at_index
    ON telemetry (tenant_id, gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;
ALTER TABLE telemetry SET (timescaledb.compress, timescaledb.compress_segmentby = 'tenant_id,device_id');
SELECT public.add_compression_policy('telemetry', INTERVAL '7 days', if_not_exists => TRUE);
SELECT public.add_retention_policy('telemetry', INTERVAL '30 days', if_not_exists => TRUE);
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_5m WITH (timescaledb.continuous) AS
SELECT public.time_bucket(INTERVAL '5 minutes', event_at) AS bucket, tenant_id, device_id,
    count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry GROUP BY bucket, tenant_id, device_id WITH NO DATA;
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h WITH (timescaledb.continuous) AS
SELECT public.time_bucket(INTERVAL '1 hour', event_at) AS bucket, tenant_id, device_id,
    count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry GROUP BY bucket, tenant_id, device_id WITH NO DATA;
SELECT public.add_continuous_aggregate_policy('telemetry_5m', start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '5 minutes', schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
SELECT public.add_continuous_aggregate_policy('telemetry_1h', start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY, tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name TEXT NOT NULL CHECK (btrim(name) <> ''), enabled BOOLEAN NOT NULL DEFAULT TRUE,
    device_id TEXT, metric_key TEXT NOT NULL CHECK (metric_key ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'),
    rule_type TEXT NOT NULL CHECK (rule_type IN ('event_threshold', 'window_average')),
    comparison TEXT NOT NULL CHECK (comparison IN ('gt', 'gte', 'lt', 'lte')),
    threshold DOUBLE PRECISION NOT NULL,
    window_seconds INTEGER CHECK (window_seconds IS NULL OR window_seconds >= 60),
    for_seconds INTEGER NOT NULL DEFAULT 300 CHECK (for_seconds >= 0),
    resolve_after_seconds INTEGER NOT NULL DEFAULT 300 CHECK (resolve_after_seconds >= 0),
    reopen_grace_seconds INTEGER NOT NULL DEFAULT 3600 CHECK (reopen_grace_seconds >= 0),
    hysteresis DOUBLE PRECISION CHECK (hysteresis IS NULL OR hysteresis >= 0),
    severity TEXT NOT NULL DEFAULT 'warning' CHECK (severity IN ('info', 'warning', 'critical')),
    reminder_interval_seconds INTEGER NOT NULL DEFAULT 86400 CHECK (reminder_interval_seconds > 0),
    archived_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((rule_type = 'event_threshold' AND window_seconds IS NULL)
        OR (rule_type = 'window_average' AND window_seconds IS NOT NULL)),
    UNIQUE (id, tenant_id),
    CONSTRAINT alert_rules_tenant_device_fkey
        FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (tenant_id, rule_type, device_id) WHERE enabled;
CREATE INDEX IF NOT EXISTS alert_rules_active_index
    ON alert_rules (tenant_id, created_at DESC, id) WHERE archived_at IS NULL;
CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    rule_id UUID NOT NULL,
    event_at TIMESTAMPTZ NOT NULL, device_id TEXT NOT NULL, boot_id UUID NOT NULL, sequence BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, rule_id, event_at, device_id, boot_id, sequence),
    CONSTRAINT alert_rule_event_evaluations_tenant_rule_fkey
        FOREIGN KEY (rule_id, tenant_id)
        REFERENCES alert_rules(id, tenant_id) ON DELETE CASCADE,
    CONSTRAINT alert_rule_event_evaluations_tenant_device_fkey
        FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS alert_incidents (
    id UUID PRIMARY KEY, tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    rule_id UUID NOT NULL,
    device_id TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending', 'open', 'resolved')),
    condition_started_at TIMESTAMPTZ NOT NULL, recovery_started_at TIMESTAMPTZ, opened_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ, acknowledged_at TIMESTAMPTZ, acknowledged_by TEXT,
    last_value DOUBLE PRECISION, last_notified_at TIMESTAMPTZ, last_reminder_at TIMESTAMPTZ,
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    CONSTRAINT alert_incidents_tenant_rule_fkey
        FOREIGN KEY (rule_id, tenant_id)
        REFERENCES alert_rules(id, tenant_id) ON DELETE RESTRICT,
    CONSTRAINT alert_incidents_tenant_device_fkey
        FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (tenant_id, rule_id, device_id) WHERE status IN ('pending', 'open');
CREATE INDEX IF NOT EXISTS alert_incidents_status_updated_index
    ON alert_incidents (tenant_id, status, updated_at DESC);
CREATE TABLE IF NOT EXISTS notification_outbox (
    id UUID PRIMARY KEY, tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    incident_id UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('opened', 'resolved', 'reminder')), dedupe_key TEXT NOT NULL,
    subject TEXT NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased', 'sent')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0), last_error TEXT, sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id, tenant_id),
    UNIQUE (tenant_id, dedupe_key),
    CONSTRAINT notification_outbox_tenant_incident_fkey
        FOREIGN KEY (incident_id, tenant_id)
        REFERENCES alert_incidents(id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (tenant_id, state, next_attempt_at) WHERE state = 'pending';
CREATE TABLE IF NOT EXISTS command_outbox (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    method TEXT NOT NULL CHECK (btrim(method) <> ''), params JSONB NOT NULL DEFAULT '{}'::jsonb,
    mode TEXT NOT NULL DEFAULT 'one_way' CHECK (mode IN ('one_way', 'two_way')),
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
    expires_at TIMESTAMPTZ NOT NULL, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0), last_error TEXT, published_at TIMESTAMPTZ,
    response JSONB, responded_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT command_outbox_tenant_device_fkey
        FOREIGN KEY (device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (tenant_id, state, next_attempt_at) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS command_outbox_expiring_index
    ON command_outbox (tenant_id, expires_at) WHERE state IN ('queued', 'leased');
CREATE INDEX IF NOT EXISTS command_outbox_two_way_expiring_index ON command_outbox (tenant_id, expires_at)
    WHERE state = 'published_to_broker' AND mode = 'two_way';
CREATE INDEX IF NOT EXISTS command_outbox_tenant_device_index
    ON command_outbox (tenant_id, device_id);

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, gateway_device_id, idempotency_key),
    FOREIGN KEY (gateway_device_id, tenant_id)
        REFERENCES devices(device_id, tenant_id) ON DELETE RESTRICT
);
