CREATE TABLE IF NOT EXISTS telemetry (
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    measurements TEXT NOT NULL,
    topic TEXT NOT NULL,
    gateway_device_id TEXT,
    UNIQUE (tenant_id, event_at, device_id, boot_id, sequence)
);
CREATE TABLE IF NOT EXISTS device_runtime_state (
    tenant_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    last_seen_at TEXT,
    gateway_last_read_at TEXT,
    gateway_read_quality TEXT CHECK (gateway_read_quality IN ('good', 'unavailable')),
    PRIMARY KEY (tenant_id, device_id)
);
CREATE INDEX IF NOT EXISTS telemetry_tenant_device_event_at_index
    ON telemetry (tenant_id, device_id, event_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_tenant_gateway_device_event_at_index
    ON telemetry (tenant_id, gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    tenant_id TEXT NOT NULL,
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, gateway_device_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS telemetry_rollups_5m (
    bucket_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
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
    PRIMARY KEY (tenant_id, bucket_at, device_id)
);
CREATE TABLE IF NOT EXISTS telemetry_rollups_1h (
    bucket_at TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
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
    PRIMARY KEY (tenant_id, bucket_at, device_id)
);

CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
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
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS alert_rules_enabled_kind_device_index
    ON alert_rules (rule_type, device_id)
    WHERE enabled = 1;

CREATE TABLE IF NOT EXISTS alert_rule_event_evaluations (
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    event_at TEXT NOT NULL,
    device_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    sequence TEXT NOT NULL,
    PRIMARY KEY (rule_id, event_at, device_id, boot_id, sequence)
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
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
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS alert_incidents_active_rule_device_index
    ON alert_incidents (rule_id, device_id)
    WHERE status IN ('pending', 'open');

CREATE TABLE IF NOT EXISTS notification_outbox (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES alert_incidents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_until TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS notification_outbox_due_index
    ON notification_outbox (state, next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS command_outbox (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
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
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (tenant_id, state, next_attempt_at)
    WHERE state = 'queued';
