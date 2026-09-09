ALTER TABLE users
    ADD COLUMN IF NOT EXISTS account_class TEXT NOT NULL DEFAULT 'user'
        CHECK (account_class IN ('system', 'admin', 'user'));

UPDATE users
SET account_class = CASE role
    WHEN 'admin' THEN 'admin'
    ELSE 'user'
END
WHERE account_class IS NULL
   OR account_class NOT IN ('system', 'admin', 'user')
   OR (account_class = 'user' AND role = 'admin');

ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

ALTER TABLE assets
    ADD COLUMN IF NOT EXISTS owner_user_id UUID REFERENCES users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS devices_owner_user_id_index
    ON devices (owner_user_id)
    WHERE owner_user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS assets_owner_user_id_index
    ON assets (owner_user_id)
    WHERE owner_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS resource_shares (
    id UUID PRIMARY KEY,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('asset', 'device')),
    resource_id TEXT NOT NULL,
    target_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'controller', 'manager')),
    inherit_children BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'active', 'declined', 'cancelled', 'expired')
    ),
    created_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    responded_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS resource_shares_one_pending_index
    ON resource_shares (resource_type, resource_id, target_user_id)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS resource_shares_target_state_index
    ON resource_shares (target_user_id, state, created_at DESC);

CREATE INDEX IF NOT EXISTS resource_shares_resource_state_index
    ON resource_shares (resource_type, resource_id, state);
