CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS device_runtime_state (
    tenant_id UUID NOT NULL,
    device_id TEXT NOT NULL,
    last_seen_at TIMESTAMPTZ,
    gateway_last_read_at TIMESTAMPTZ,
    gateway_read_quality TEXT
        CHECK (gateway_read_quality IN ('good', 'unavailable')),
    PRIMARY KEY (tenant_id, device_id)
);

CREATE TABLE IF NOT EXISTS telemetry (
    event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    tenant_id UUID NOT NULL,
    device_id TEXT NOT NULL,
    boot_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    measurements JSONB NOT NULL,
    topic TEXT NOT NULL,
    gateway_device_id TEXT,
    CONSTRAINT telemetry_event_identity
        UNIQUE (tenant_id, event_at, device_id, boot_id, sequence)
);

SELECT public.create_hypertable('telemetry', 'event_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS telemetry_tenant_device_event_at_index
    ON telemetry (tenant_id, device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_tenant_gateway_device_event_at_index
    ON telemetry (tenant_id, gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;

ALTER TABLE telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'tenant_id,device_id'
);

SELECT public.add_compression_policy(
    'telemetry',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT public.add_retention_policy(
    'telemetry',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_5m
WITH (timescaledb.continuous) AS
SELECT
    public.time_bucket(INTERVAL '5 minutes', event_at) AS bucket,
    tenant_id,
    device_id,
    count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry
GROUP BY bucket, tenant_id, device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h
WITH (timescaledb.continuous) AS
SELECT
    public.time_bucket(INTERVAL '1 hour', event_at) AS bucket,
    tenant_id,
    device_id,
    count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry
GROUP BY bucket, tenant_id, device_id
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
    'telemetry_5m',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

SELECT public.add_continuous_aggregate_policy(
    'telemetry_1h',
    start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    device_id TEXT,
    metric_key TEXT NOT NULL CHECK (metric_key ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'),
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
    archived_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (rule_type = 'event_threshold' AND window_seconds IS NULL)
        OR (rule_type = 'window_average' AND window_seconds IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (rule_type, device_id)
    WHERE enabled;
CREATE INDEX IF NOT EXISTS alert_rules_active_index
    ON alert_rules (created_at DESC, id)
    WHERE archived_at IS NULL;

CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    event_at TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    boot_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    PRIMARY KEY (rule_id, event_at, device_id, boot_id, sequence)
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id UUID PRIMARY KEY,
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'open', 'resolved')),
    condition_started_at TIMESTAMPTZ NOT NULL,
    recovery_started_at TIMESTAMPTZ,
    opened_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    last_value DOUBLE PRECISION,
    last_notified_at TIMESTAMPTZ,
    last_reminder_at TIMESTAMPTZ,
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (rule_id, device_id)
    WHERE status IN ('pending', 'open');
CREATE INDEX IF NOT EXISTS alert_incidents_status_updated_index
    ON alert_incidents (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES alert_incidents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('opened', 'resolved', 'reminder')),
    dedupe_key TEXT NOT NULL UNIQUE,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased', 'sent')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (state, next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS command_outbox (
    id UUID PRIMARY KEY,
    device_id TEXT NOT NULL,
    method TEXT NOT NULL CHECK (btrim(method) <> ''),
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    mode TEXT NOT NULL DEFAULT 'one_way'
        CHECK (mode IN ('one_way', 'two_way')),
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'leased', 'published_to_broker', 'responded', 'expired', 'failed')),
    expires_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    published_at TIMESTAMPTZ,
    response JSONB,
    responded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (state, next_attempt_at)
    WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS command_outbox_expiring_index
    ON command_outbox (expires_at)
    WHERE state IN ('queued', 'leased');
CREATE INDEX IF NOT EXISTS command_outbox_two_way_expiring_index
    ON command_outbox (expires_at)
    WHERE state = 'published_to_broker' AND mode = 'two_way';

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    tenant_id UUID NOT NULL,
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, gateway_device_id, idempotency_key)
);
