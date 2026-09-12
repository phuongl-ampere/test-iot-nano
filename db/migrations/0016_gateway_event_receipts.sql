CREATE TABLE IF NOT EXISTS gateway_event_receipts (
    gateway_device_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (gateway_device_id, idempotency_key)
);
