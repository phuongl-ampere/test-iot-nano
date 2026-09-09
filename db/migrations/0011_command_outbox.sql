CREATE TABLE IF NOT EXISTS command_outbox (
    id UUID PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    method TEXT NOT NULL CHECK (btrim(method) <> ''),
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'leased', 'published_to_broker', 'expired', 'failed')),
    expires_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS command_outbox_due_index
    ON command_outbox (state, next_attempt_at)
    WHERE state = 'queued';

CREATE INDEX IF NOT EXISTS command_outbox_expiring_index
    ON command_outbox (expires_at)
    WHERE state IN ('queued', 'leased');
