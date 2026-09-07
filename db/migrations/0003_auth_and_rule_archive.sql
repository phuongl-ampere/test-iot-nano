CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY CHECK (role IN ('admin', 'viewer')),
    token_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE alert_rules
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS alert_rules_active_index
    ON alert_rules (created_at DESC, id)
    WHERE archived_at IS NULL;
