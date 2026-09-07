ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS devices_active_index
    ON devices (device_id)
    WHERE deleted_at IS NULL;
