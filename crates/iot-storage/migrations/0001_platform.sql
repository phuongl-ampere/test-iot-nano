CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS api_access_tokens_username_index ON api_access_tokens (username);

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    account_class TEXT NOT NULL DEFAULT 'user' CHECK (account_class IN ('system', 'admin', 'user')),
    default_app TEXT NOT NULL DEFAULT '/apps/powermonitor',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE users ALTER COLUMN id SET DEFAULT public.uuid_generate_v4();

CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    app_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, app_key)
);
CREATE TABLE IF NOT EXISTS applications (
    app_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('frontend', 'full_stack')),
    launch_url TEXT NOT NULL,
    client_id TEXT NOT NULL UNIQUE,
    allowed_scopes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS application_redirect_uris (
    app_id TEXT NOT NULL REFERENCES applications(app_id) ON DELETE CASCADE,
    redirect_uri TEXT NOT NULL,
    PRIMARY KEY (app_id, redirect_uri)
);
CREATE INDEX IF NOT EXISTS application_redirect_uris_lookup_index
    ON application_redirect_uris (app_id, redirect_uri);
CREATE TABLE IF NOT EXISTS oauth_client_secrets (
    app_id TEXT NOT NULL REFERENCES applications(app_id) ON DELETE CASCADE,
    secret_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (app_id, secret_hash)
);
CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL REFERENCES applications(app_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    redirect_uri TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    scopes_json JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    CHECK (expires_at > issued_at)
);
CREATE INDEX IF NOT EXISTS oauth_authorization_codes_active_index
    ON oauth_authorization_codes (app_id, expires_at)
    WHERE consumed_at IS NULL;
CREATE TABLE IF NOT EXISTS oauth_access_tokens (
    token_hash TEXT PRIMARY KEY,
    app_id TEXT NOT NULL REFERENCES applications(app_id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    scopes_json JSONB NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > issued_at)
);
CREATE INDEX IF NOT EXISTS oauth_access_tokens_expiry_index
    ON oauth_access_tokens (expires_at);
CREATE TABLE IF NOT EXISTS asset_profiles (
    id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    dashboard_defaults JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS device_profiles (
    id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    telemetry_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_mapping JSONB NOT NULL DEFAULT '{}'::jsonb,
    reporting_settings JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY, name TEXT NOT NULL,
    asset_profile_id UUID REFERENCES asset_profiles(id) ON DELETE SET NULL,
    parent_asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (parent_asset_id, name)
);
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY, display_name TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    device_profile_id UUID REFERENCES device_profiles(id) ON DELETE SET NULL,
    deleted_at TIMESTAMPTZ, is_gateway BOOLEAN NOT NULL DEFAULT FALSE,
    gateway_device_id TEXT REFERENCES devices(device_id) ON DELETE RESTRICT,
    owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL, claimed_at TIMESTAMPTZ,
    CHECK ((is_gateway = TRUE AND gateway_device_id IS NULL)
        OR (is_gateway = FALSE AND gateway_device_id IS DISTINCT FROM device_id))
);
CREATE INDEX IF NOT EXISTS devices_asset_id_index ON devices (asset_id) WHERE asset_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_device_profile_id_index ON devices (device_profile_id) WHERE device_profile_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS devices_active_index ON devices (device_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS devices_gateway_device_id_index ON devices (gateway_device_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS devices_owner_user_id_index ON devices (owner_user_id) WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS assets_owner_user_id_index ON assets (owner_user_id) WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS device_tokens (
    id UUID PRIMARY KEY, device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    token_prefix TEXT NOT NULL UNIQUE, token_hash TEXT NOT NULL, token_ciphertext TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_used_at TIMESTAMPTZ, revoked_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS device_tokens_one_active_per_device ON device_tokens (device_id) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS device_tokens_active_prefix_index ON device_tokens (token_prefix) WHERE revoked_at IS NULL;
CREATE TABLE IF NOT EXISTS resource_shares (
    id UUID PRIMARY KEY, resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
    resource_id TEXT NOT NULL, target_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
    inherit_children BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'active', 'declined', 'cancelled', 'expired')),
    created_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), responded_at TIMESTAMPTZ, expires_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
    ON resource_shares (resource_type, resource_id, target_user_id) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS resource_shares_target_state_index ON resource_shares (target_user_id, state, created_at DESC);
CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index ON resource_shares (resource_type, resource_id, state);
CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY, actor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    actor_account_class TEXT NOT NULL CHECK (actor_account_class IN ('system', 'admin', 'user')),
    resource_type TEXT NOT NULL, resource_id TEXT NOT NULL, action TEXT NOT NULL,
    before_value JSONB, after_value JSONB, request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_events_resource_index ON audit_events (resource_type, resource_id, created_at DESC);
CREATE INDEX IF NOT EXISTS audit_events_actor_index ON audit_events (actor_user_id, created_at DESC);
CREATE TABLE IF NOT EXISTS device_claim_codes (
    device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
    issued_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(), used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS device_claim_codes_expiry_index ON device_claim_codes (expires_at) WHERE used_at IS NULL;

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS device_runtime_state (
    device_id TEXT PRIMARY KEY REFERENCES devices(device_id) ON DELETE CASCADE,
    last_seen_at TIMESTAMPTZ, gateway_last_read_at TIMESTAMPTZ,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable'))
);
CREATE TABLE IF NOT EXISTS telemetry (
    event_at TIMESTAMPTZ NOT NULL, received_at TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id), boot_id UUID NOT NULL,
    sequence BIGINT NOT NULL, measurements JSONB NOT NULL, topic TEXT NOT NULL, gateway_device_id TEXT,
    CONSTRAINT telemetry_event_identity UNIQUE (event_at, device_id, boot_id, sequence)
);
SELECT public.create_hypertable('telemetry', 'event_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS telemetry_device_event_at_index ON telemetry (device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_gateway_device_event_at_index ON telemetry (gateway_device_id, event_at DESC) WHERE gateway_device_id IS NOT NULL;
ALTER TABLE telemetry SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id');
SELECT public.add_compression_policy('telemetry', INTERVAL '7 days', if_not_exists => TRUE);
SELECT public.add_retention_policy('telemetry', INTERVAL '30 days', if_not_exists => TRUE);
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_5m WITH (timescaledb.continuous) AS
SELECT public.time_bucket(INTERVAL '5 minutes', event_at) AS bucket, device_id, count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry GROUP BY bucket, device_id WITH NO DATA;
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h WITH (timescaledb.continuous) AS
SELECT public.time_bucket(INTERVAL '1 hour', event_at) AS bucket, device_id, count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry GROUP BY bucket, device_id WITH NO DATA;
SELECT public.add_continuous_aggregate_policy('telemetry_5m', start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '5 minutes', schedule_interval => INTERVAL '5 minutes', if_not_exists => TRUE);
SELECT public.add_continuous_aggregate_policy('telemetry_1h', start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY, name TEXT NOT NULL CHECK (btrim(name) <> ''), enabled BOOLEAN NOT NULL DEFAULT TRUE,
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
        OR (rule_type = 'window_average' AND window_seconds IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index ON alert_rules (rule_type, device_id) WHERE enabled;
CREATE INDEX IF NOT EXISTS alert_rules_active_index ON alert_rules (created_at DESC, id) WHERE archived_at IS NULL;
CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    event_at TIMESTAMPTZ NOT NULL, device_id TEXT NOT NULL, boot_id UUID NOT NULL, sequence BIGINT NOT NULL,
    PRIMARY KEY (rule_id, event_at, device_id, boot_id, sequence)
);
CREATE TABLE IF NOT EXISTS alert_incidents (
    id UUID PRIMARY KEY, rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending', 'open', 'resolved')),
    condition_started_at TIMESTAMPTZ NOT NULL, recovery_started_at TIMESTAMPTZ, opened_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ, acknowledged_at TIMESTAMPTZ, acknowledged_by TEXT,
    last_value DOUBLE PRECISION, last_notified_at TIMESTAMPTZ, last_reminder_at TIMESTAMPTZ,
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index ON alert_incidents (rule_id, device_id) WHERE status IN ('pending', 'open');
CREATE INDEX IF NOT EXISTS alert_incidents_status_updated_index ON alert_incidents (status, updated_at DESC);
CREATE TABLE IF NOT EXISTS notification_outbox (
    id UUID PRIMARY KEY, incident_id UUID NOT NULL REFERENCES alert_incidents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('opened', 'resolved', 'reminder')), dedupe_key TEXT NOT NULL UNIQUE,
    subject TEXT NOT NULL, body TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased', 'sent')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0), last_error TEXT, sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index ON notification_outbox (state, next_attempt_at) WHERE state = 'pending';
CREATE TABLE IF NOT EXISTS command_outbox (
    id UUID PRIMARY KEY, device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    method TEXT NOT NULL CHECK (btrim(method) <> ''), params JSONB NOT NULL DEFAULT '{}'::jsonb,
    mode TEXT NOT NULL DEFAULT 'one_way' CHECK (mode IN ('one_way', 'two_way')),
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
    expires_at TIMESTAMPTZ NOT NULL, next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0), last_error TEXT, published_at TIMESTAMPTZ,
    response JSONB, responded_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index ON command_outbox (state, next_attempt_at) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS command_outbox_expiring_index ON command_outbox (expires_at) WHERE state IN ('queued', 'leased');
CREATE INDEX IF NOT EXISTS command_outbox_two_way_expiring_index ON command_outbox (expires_at)
    WHERE state = 'published_to_broker' AND mode = 'two_way';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'command_outbox_device_id_fkey'
          AND conrelid = 'command_outbox'::regclass
    ) THEN
        ALTER TABLE command_outbox
            ADD CONSTRAINT command_outbox_device_id_fkey
            FOREIGN KEY (device_id)
            REFERENCES devices(device_id)
            ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'device_runtime_state_device_id_fkey'
          AND conrelid = 'device_runtime_state'::regclass
    ) THEN
        ALTER TABLE device_runtime_state
            ADD CONSTRAINT device_runtime_state_device_id_fkey
            FOREIGN KEY (device_id)
            REFERENCES devices(device_id)
            ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'telemetry_device_id_fkey'
          AND conrelid = 'telemetry'::regclass
    ) THEN
        ALTER TABLE telemetry
            ADD CONSTRAINT telemetry_device_id_fkey
            FOREIGN KEY (device_id)
            REFERENCES devices(device_id);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    gateway_device_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (gateway_device_id, idempotency_key)
);
