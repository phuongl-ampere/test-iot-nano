CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user'
        CHECK (account_class IN ('system', 'admin', 'user')),
    default_app TEXT NOT NULL DEFAULT '/apps/powermonitor',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    app_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, app_key)
);

CREATE TABLE IF NOT EXISTS asset_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    fields TEXT NOT NULL DEFAULT '{}',
    dashboard_defaults TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS device_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    telemetry_schema TEXT NOT NULL DEFAULT '{}',
    metric_mapping TEXT NOT NULL DEFAULT '{}',
    reporting_settings TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    asset_profile_id TEXT REFERENCES asset_profiles(id) ON DELETE SET NULL,
    parent_asset_id TEXT REFERENCES assets(id) ON DELETE SET NULL,
    owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (parent_asset_id, name)
);

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    display_name TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    asset_id TEXT REFERENCES assets(id) ON DELETE SET NULL,
    device_profile_id TEXT REFERENCES device_profiles(id) ON DELETE SET NULL,
    deleted_at TEXT,
    is_gateway INTEGER NOT NULL DEFAULT 0,
    gateway_device_id TEXT REFERENCES devices(device_id) ON DELETE RESTRICT,
    owner_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    claimed_at TEXT,
    CHECK (
        (is_gateway = 1 AND gateway_device_id IS NULL)
        OR (is_gateway = 0 AND gateway_device_id IS NOT device_id)
    )
);
CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
    ON devices (owner_user_id)
    WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_asset_id_index
    ON devices (asset_id)
    WHERE asset_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_device_profile_id_index
    ON devices (device_profile_id)
    WHERE device_profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_gateway_device_id_index
    ON devices (gateway_device_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS devices_active_index
    ON devices (device_id)
    WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
    ON assets (owner_user_id)
    WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS resource_shares (
    id TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
    inherit_children INTEGER NOT NULL DEFAULT 0 CHECK (inherit_children IN (0, 1)),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'active', 'declined', 'cancelled', 'expired')
    ),
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    responded_at TEXT,
    expires_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
    ON resource_shares (resource_type, resource_id, target_user_id)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS resource_shares_target_state_index
    ON resource_shares (target_user_id, state, created_at DESC);
CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index
    ON resource_shares (resource_type, resource_id, state);

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    actor_account_class TEXT NOT NULL
        CHECK (actor_account_class IN ('system', 'admin', 'user')),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_value TEXT,
    after_value TEXT,
    request_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS audit_events_resource_index
    ON audit_events (resource_type, resource_id, created_at DESC);
CREATE INDEX IF NOT EXISTS audit_events_actor_index
    ON audit_events (actor_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS device_claim_codes (
    device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    issued_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    issued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_at TEXT
);
CREATE INDEX IF NOT EXISTS device_claim_codes_expiry_index
    ON device_claim_codes (expires_at)
    WHERE used_at IS NULL;

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
