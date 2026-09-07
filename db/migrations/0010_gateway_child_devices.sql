ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS is_gateway BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS gateway_device_id TEXT REFERENCES devices(device_id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS gateway_last_read_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS gateway_read_quality TEXT
        CHECK (gateway_read_quality IN ('good', 'unavailable'));

DO $$
BEGIN
    ALTER TABLE devices
        ADD CONSTRAINT devices_gateway_parent_check
        CHECK (
            (is_gateway = TRUE AND gateway_device_id IS NULL)
            OR (is_gateway = FALSE AND gateway_device_id IS DISTINCT FROM device_id)
        );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS devices_gateway_device_id_index
    ON devices (gateway_device_id)
    WHERE deleted_at IS NULL;

ALTER TABLE telemetry
    ADD COLUMN IF NOT EXISTS gateway_device_id TEXT;

CREATE INDEX IF NOT EXISTS telemetry_gateway_device_event_at_index
    ON telemetry (gateway_device_id, event_at DESC)
    WHERE gateway_device_id IS NOT NULL;
