CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    display_name TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    configuration_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS telemetry (
    event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    boot_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    measurements JSONB NOT NULL,
    topic TEXT NOT NULL,
    CONSTRAINT telemetry_event_identity UNIQUE (event_at, device_id, boot_id, sequence)
);

SELECT create_hypertable('telemetry', 'event_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS telemetry_device_event_at_index
    ON telemetry (device_id, event_at DESC);

ALTER TABLE telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id'
);

SELECT add_compression_policy(
    'telemetry',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'telemetry',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '5 minutes', event_at) AS bucket,
    device_id,
    count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry
GROUP BY bucket, device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 hour', event_at) AS bucket,
    device_id,
    count(*) AS event_count,
    avg((measurements ->> 'temperature_c')::double precision) AS avg_temperature_c,
    avg((measurements ->> 'humidity_pct')::double precision) AS avg_humidity_pct
FROM telemetry
GROUP BY bucket, device_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'telemetry_5m',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy(
    'telemetry_1h',
    start_offset => INTERVAL '1 year',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);
