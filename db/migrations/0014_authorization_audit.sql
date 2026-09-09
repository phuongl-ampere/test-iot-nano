CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY,
    actor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    actor_account_class TEXT NOT NULL
        CHECK (actor_account_class IN ('system', 'admin', 'user')),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_value JSONB,
    after_value JSONB,
    request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS audit_events_resource_index
    ON audit_events (resource_type, resource_id, created_at DESC);

CREATE INDEX IF NOT EXISTS audit_events_actor_index
    ON audit_events (actor_user_id, created_at DESC);
