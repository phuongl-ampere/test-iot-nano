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
