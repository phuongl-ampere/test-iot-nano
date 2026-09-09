ALTER TABLE command_outbox
    ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'one_way'
        CHECK (mode IN ('one_way', 'two_way'));

ALTER TABLE command_outbox
    ADD COLUMN IF NOT EXISTS response JSONB;

ALTER TABLE command_outbox
    ADD COLUMN IF NOT EXISTS responded_at TIMESTAMPTZ;

DO $$
BEGIN
    PERFORM pg_advisory_xact_lock(72431001);
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'command_outbox'::regclass
          AND conname = 'command_outbox_state_check'
          AND pg_get_constraintdef(oid) LIKE '%responded%'
    ) THEN
        EXECUTE 'ALTER TABLE command_outbox DROP CONSTRAINT IF EXISTS command_outbox_state_check';
        EXECUTE 'ALTER TABLE command_outbox
                 ADD CONSTRAINT command_outbox_state_check
                 CHECK (state IN (''queued'', ''leased'', ''published_to_broker'', ''responded'', ''expired'', ''failed''))';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS command_outbox_two_way_expiring_index
    ON command_outbox (expires_at)
    WHERE state = 'published_to_broker' AND mode = 'two_way';
