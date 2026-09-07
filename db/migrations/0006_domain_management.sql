CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL UNIQUE CHECK (role IN ('admin', 'viewer')),
    default_app TEXT NOT NULL DEFAULT '/apps/powermonitor',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO users (username, password_hash, role, default_app)
SELECT username, password_hash, role, '/apps/powermonitor'
FROM api_access_tokens
ON CONFLICT (username) DO NOTHING;

CREATE TABLE IF NOT EXISTS user_app_grants (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    app_key TEXT NOT NULL CHECK (app_key IN ('powermonitor')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, app_key)
);

INSERT INTO user_app_grants (user_id, app_key)
SELECT id, 'powermonitor'
FROM users
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS asset_profiles (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    dashboard_defaults JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS device_profiles (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    telemetry_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_mapping JSONB NOT NULL DEFAULT '{}'::jsonb,
    reporting_settings JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    asset_profile_id UUID REFERENCES asset_profiles(id) ON DELETE SET NULL,
    parent_asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (parent_asset_id, name)
);

ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS device_profile_id UUID REFERENCES device_profiles(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS assets_parent_asset_id_index
    ON assets (parent_asset_id, name);
CREATE INDEX IF NOT EXISTS devices_asset_id_index
    ON devices (asset_id);
CREATE INDEX IF NOT EXISTS devices_device_profile_id_index
    ON devices (device_profile_id);
