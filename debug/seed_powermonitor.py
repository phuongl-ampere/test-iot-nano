#!/usr/bin/env python3
"""Reset local Power Monitor domain data and insert a deterministic demo topology."""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone


DEMO_SITE_ID = "019f6da9-1234-7abc-8def-012345678901"
DEMO_PANEL_ID = "019f6da9-1234-7abc-8def-012345678902"
DEMO_PUMP_ID = "019f6da9-1234-7abc-8def-012345678903"
DEMO_LIGHTING_ID = "019f6da9-1234-7abc-8def-012345678904"
POWER_SWITCHER_PROFILE_ID = "019f6da9-1234-7abc-8def-012345678905"
POWER_METER_PROFILE_ID = "019f6da9-1234-7abc-8def-012345678906"
LIGHT_SWITCH_PROFILE_ID = "019f6da9-1234-7abc-8def-01234567890c"
SITE_PROFILE_ID = "019f6da9-1234-7abc-8def-012345678907"
PANEL_PROFILE_ID = "019f6da9-1234-7abc-8def-012345678908"
LOAD_PROFILE_ID = "019f6da9-1234-7abc-8def-012345678909"
DEMO_SWITCHER_BOOT_ID = "019f6da9-1234-7abc-8def-012345678903"
DEMO_METER_BOOT_ID = "019f6da9-1234-7abc-8def-012345678904"
DEMO_PUMP_METER_BOOT_ID = "019f6da9-1234-7abc-8def-01234567890a"
DEMO_LIGHT_SWITCHER_BOOT_ID = "019f6da9-1234-7abc-8def-01234567890b"
DEMO_SWITCHER_ID = "demo-switcher-01"
DEMO_METER_ID = "demo-meter-01"
DEMO_PUMP_METER_ID = "demo-pump-meter-01"
DEMO_LIGHT_SWITCHER_ID = "demo-light-switcher-01"
DEFAULT_LIGHTING_ENV_FILE = "debug/.demo_lighting_switcher.env"


def rounded_now():
    now = datetime.now(timezone.utc)
    return now.replace(second=0, microsecond=0, minute=now.minute - now.minute % 5)


def iso(value):
    return value.isoformat().replace("+00:00", "Z")


def device_profiles():
    return [
        {
            "id": POWER_SWITCHER_PROFILE_ID,
            "name": "PowerSwitcher",
            "telemetry_schema": {
                "switch_state": {"type": "boolean"},
                "relay_state": {"type": "boolean"},
                "brightness_pct": {"type": "number", "unit": "%"},
                "voltage_v": {"type": "number", "unit": "V"},
                "current_a": {"type": "number", "unit": "A"},
                "power_w": {"type": "number", "unit": "W"},
                "energy_kwh": {"type": "number", "unit": "kWh"},
            },
            "metric_mapping": {
                "state": "switch_state",
                "power": "power_w",
                "energy": "energy_kwh",
            },
            "reporting_settings": {
                "control": {"kind": "power_switcher"},
                "rpc": {
                    "methods": [
                        "switch_on",
                        "switch_off",
                        "set_power",
                        "set_brightness",
                    ],
                    "two_way_supported": True,
                },
            },
        },
        {
            "id": POWER_METER_PROFILE_ID,
            "name": "PowerMeter",
            "telemetry_schema": {
                "voltage_v": {"type": "number", "unit": "V"},
                "current_a": {"type": "number", "unit": "A"},
                "power_w": {"type": "number", "unit": "W"},
                "energy_kwh": {"type": "number", "unit": "kWh"},
            },
            "metric_mapping": {"power": "power_w", "energy": "energy_kwh"},
            "reporting_settings": {"control": {"kind": "read_only"}},
        },
        {
            "id": LIGHT_SWITCH_PROFILE_ID,
            "name": "LightSwitch",
            "telemetry_schema": {
                "switch_state": {"type": "boolean"},
                "relay_state": {"type": "boolean"},
                "brightness_pct": {"type": "number", "unit": "%"},
                "power_w": {"type": "number", "unit": "W"},
                "energy_kwh": {"type": "number", "unit": "kWh"},
            },
            "metric_mapping": {
                "state": "switch_state",
                "brightness": "brightness_pct",
                "power": "power_w",
                "energy": "energy_kwh",
            },
            "reporting_settings": {
                "control": {"kind": "light_switch", "max_power_w": 250},
                "rpc": {
                    "methods": [
                        "switch_on",
                        "switch_off",
                        "set_power",
                        "set_brightness",
                    ],
                    "two_way_supported": True,
                },
            },
        },
    ]


def asset_profiles():
    return [
        {
            "id": SITE_PROFILE_ID,
            "name": "Site",
            "fields": {"address": "string", "timezone": "string"},
            "dashboard_defaults": {"layout": "site"},
        },
        {
            "id": PANEL_PROFILE_ID,
            "name": "ElectricalPanel",
            "fields": {"voltage_level": "string", "breaker_rating_a": "number"},
            "dashboard_defaults": {"layout": "panel"},
        },
        {
            "id": LOAD_PROFILE_ID,
            "name": "LoadEquipment",
            "fields": {"category": "string", "nominal_power_w": "number"},
            "dashboard_defaults": {"layout": "equipment"},
        },
    ]


def demo_assets():
    return [
        {
            "id": DEMO_SITE_ID,
            "name": "Demo Site",
            "parent_asset_id": None,
            "asset_profile_id": SITE_PROFILE_ID,
            "metadata": {"address": "Demo campus", "timezone": "Asia/Ho_Chi_Minh"},
        },
        {
            "id": DEMO_PANEL_ID,
            "name": "Main Distribution Panel",
            "parent_asset_id": DEMO_SITE_ID,
            "asset_profile_id": PANEL_PROFILE_ID,
            "metadata": {"voltage_level": "230V", "breaker_rating_a": 63},
        },
        {
            "id": DEMO_PUMP_ID,
            "name": "Pump Station",
            "parent_asset_id": DEMO_PANEL_ID,
            "asset_profile_id": LOAD_PROFILE_ID,
            "metadata": {"category": "pump", "nominal_power_w": 552},
        },
        {
            "id": DEMO_LIGHTING_ID,
            "name": "Lighting Circuit",
            "parent_asset_id": DEMO_PANEL_ID,
            "asset_profile_id": LOAD_PROFILE_ID,
            "metadata": {"category": "lighting", "nominal_power_w": 250},
        },
    ]


def demo_devices():
    return [
        {
            "device_id": DEMO_SWITCHER_ID,
            "display_name": "Demo Pump Switcher",
            "asset_id": DEMO_PUMP_ID,
            "device_profile_id": POWER_SWITCHER_PROFILE_ID,
        },
        {
            "device_id": DEMO_PUMP_METER_ID,
            "display_name": "Demo Pump Meter",
            "asset_id": DEMO_PUMP_ID,
            "device_profile_id": POWER_METER_PROFILE_ID,
        },
        {
            "device_id": DEMO_LIGHT_SWITCHER_ID,
            "display_name": "Demo Lighting Switcher",
            "asset_id": DEMO_LIGHTING_ID,
            "device_profile_id": LIGHT_SWITCH_PROFILE_ID,
            "metadata": {
                "seed": "powermonitor",
                "simulator": "debug/demo_lighting_switcher.py",
                "nominal_power_w": 250,
            },
        },
        {
            "device_id": DEMO_METER_ID,
            "display_name": "Demo Main Meter",
            "asset_id": DEMO_PANEL_ID,
            "device_profile_id": POWER_METER_PROFILE_ID,
        },
    ]


def demo_events(last_event_at):
    events = []
    for offset in range(24):
        event_at = last_event_at - timedelta(minutes=23 - offset)
        pump_on = offset >= 12
        lights_on = offset % 6 >= 3
        events.extend(
            [
                {
                    "event_at": iso(event_at),
                    "device_id": DEMO_SWITCHER_ID,
                    "boot_id": DEMO_SWITCHER_BOOT_ID,
                    "sequence": offset + 1,
                    "measurements": {
                        "switch_state": pump_on,
                        "relay_state": pump_on,
                        "voltage_v": 229.8,
                        "current_a": 2.4 if pump_on else 0.0,
                        "power_w": 552.0 if pump_on else 0.0,
                        "energy_kwh": round(18.4 + offset * 0.0025, 5),
                        "frequency_hz": 50.0,
                        "power_factor": 0.96 if pump_on else 0.0,
                    },
                },
                {
                    "event_at": iso(event_at),
                    "device_id": DEMO_PUMP_METER_ID,
                    "boot_id": DEMO_PUMP_METER_BOOT_ID,
                    "sequence": offset + 1,
                    "measurements": {
                        "voltage_v": 229.7,
                        "current_a": 2.4 if pump_on else 0.0,
                        "power_w": 552.0 if pump_on else 0.0,
                        "energy_kwh": round(17.1 + offset * 0.0025, 5),
                        "frequency_hz": 50.0,
                        "power_factor": 0.96 if pump_on else 0.0,
                    },
                },
                {
                    "event_at": iso(event_at),
                    "device_id": DEMO_LIGHT_SWITCHER_ID,
                    "boot_id": DEMO_LIGHT_SWITCHER_BOOT_ID,
                    "sequence": offset + 1,
                    "measurements": {
                        "switch_state": lights_on,
                        "relay_state": lights_on,
                        "brightness_pct": 100.0 if lights_on else 0.0,
                        "voltage_v": 230.4,
                        "current_a": 1.1 if lights_on else 0.0,
                        "power_w": 250.0 if lights_on else 0.0,
                        "energy_kwh": round(9.6 + offset * 0.0012, 5),
                        "frequency_hz": 50.0,
                        "power_factor": 0.94 if lights_on else 0.0,
                    },
                },
                {
                    "event_at": iso(event_at),
                    "device_id": DEMO_METER_ID,
                    "boot_id": DEMO_METER_BOOT_ID,
                    "sequence": offset + 1,
                    "measurements": {
                        "voltage_v": 231.2,
                        "current_a": round(1.8 + offset * 0.01, 2),
                        "power_w": round(414.0 + offset * 2.3, 1),
                        "energy_kwh": round(42.8 + offset * 0.0018, 5),
                        "frequency_hz": 50.0,
                        "power_factor": 0.98,
                    },
                },
            ]
        )
    return events


def timescale_reset_sql():
    return """
TRUNCATE
  audit_events,
  resource_shares,
  device_claim_codes,
  command_outbox,
  device_tokens,
  notification_outbox,
  alert_incidents,
  alert_rules,
  telemetry,
  assets,
  asset_profiles,
  device_profiles,
  devices
RESTART IDENTITY CASCADE;
"""


def timescale_seed_owner_sql():
    return (
        "(SELECT id FROM users "
        "WHERE username = 'viewer' AND account_class = 'user' LIMIT 1)"
    )


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def api_json(url, method, payload=None, session_id=None):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if session_id:
        headers["Authorization"] = f"Session {session_id}"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            content = response.read()
    except urllib.error.HTTPError as error:
        content = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"API {method} {url} failed with HTTP {error.code}: {content}"
        ) from None
    return {} if not content else json.loads(content)


def issue_lighting_switcher_token(api_url, username, password):
    base_url = api_url.rstrip("/")
    login = api_json(
        f"{base_url}/api/auth/login",
        "POST",
        {"username": username, "password": password},
    )
    session_id = login["session_id"]
    token = api_json(
        f"{base_url}/api/devices/{DEMO_LIGHT_SWITCHER_ID}/tokens",
        "POST",
        session_id=session_id,
    )
    if not token.get("token"):
        raise RuntimeError("API did not return the Lighting Switcher MQTT token")
    return token["token"]


def write_lighting_environment(path, token, host, port):
    environment = (
        f"DEVICE_TOKEN={token}\n"
        f"MQTT_HOST={host}\n"
        f"MQTT_PORT={port}\n"
        "PUBLISH_INTERVAL_SECONDS=10\n"
    )
    with open(path, "w", encoding="ascii") as output:
        output.write(environment)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def seed_timescale(compose_file, service):
    sql = [timescale_reset_sql()]
    owner = timescale_seed_owner_sql()
    for profile in device_profiles():
        sql.append(
            """
INSERT INTO device_profiles (id, name, telemetry_schema, metric_mapping, reporting_settings)
VALUES ({id}, {name}, {schema}::jsonb, {mapping}::jsonb, {settings}::jsonb);
""".format(
                id=sql_literal(profile["id"]),
                name=sql_literal(profile["name"]),
                schema=sql_literal(json.dumps(profile["telemetry_schema"])),
                mapping=sql_literal(json.dumps(profile["metric_mapping"])),
                settings=sql_literal(json.dumps(profile["reporting_settings"])),
            )
        )
    for profile in asset_profiles():
        sql.append(
            """
INSERT INTO asset_profiles (id, name, fields, dashboard_defaults)
VALUES ({id}, {name}, {fields}::jsonb, {defaults}::jsonb);
""".format(
                id=sql_literal(profile["id"]),
                name=sql_literal(profile["name"]),
                fields=sql_literal(json.dumps(profile["fields"])),
                defaults=sql_literal(json.dumps(profile["dashboard_defaults"])),
            )
        )
    for asset in demo_assets():
        parent_id = "NULL" if asset["parent_asset_id"] is None else sql_literal(asset["parent_asset_id"])
        sql.append(
            """
INSERT INTO assets (id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata)
VALUES ({id}, {name}, {profile}, {parent}, {owner}, {metadata}::jsonb);
""".format(
                id=sql_literal(asset["id"]),
                name=sql_literal(asset["name"]),
                profile=sql_literal(asset["asset_profile_id"]),
                parent=parent_id,
                owner=owner,
                metadata=sql_literal(json.dumps(asset["metadata"], separators=(",", ":"))),
            )
        )
    for device in demo_devices():
        sql.append(
            """
INSERT INTO devices (
  device_id, display_name, asset_id, device_profile_id, owner_user_id, claimed_at, metadata
)
VALUES (
  {id}, {name}, {asset}, {profile}, {owner}, now(), {metadata}::jsonb
);
""".format(
                id=sql_literal(device["device_id"]),
                name=sql_literal(device["display_name"]),
                asset=sql_literal(device["asset_id"]),
                profile=sql_literal(device["device_profile_id"]),
                owner=owner,
                metadata=sql_literal(
                    json.dumps(
                        device.get("metadata", {"seed": "powermonitor"}),
                        separators=(",", ":"),
                    )
                ),
            )
        )
    values = []
    for event in demo_events(rounded_now()):
        measurements = sql_literal(json.dumps(event["measurements"], separators=(",", ":")))
        values.append(
            "({event_at}, {event_at}, {device_id}, {boot_id}, {sequence}, {measurements}::jsonb, 'v1/devices/me/telemetry')".format(
                event_at=sql_literal(event["event_at"]),
                device_id=sql_literal(event["device_id"]),
                boot_id=sql_literal(event["boot_id"]),
                sequence=event["sequence"],
                measurements=measurements,
            )
        )
    sql.append(
        """
INSERT INTO telemetry (
  event_at, received_at, device_id, boot_id, sequence, measurements, topic
) VALUES
{values};
""".format(values=",\n".join(values))
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "--file",
            compose_file,
            "exec",
            "--no-TTY",
            service,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "iot",
            "-d",
            "iot",
            "-c",
            "\n".join(sql),
        ],
        check=True,
    )


def seed_sqlite(path):
    with sqlite3.connect(path) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
DELETE FROM audit_events;
DELETE FROM resource_shares;
DELETE FROM device_claim_codes;
DELETE FROM command_outbox;
DELETE FROM device_tokens;
DELETE FROM notification_outbox;
DELETE FROM alert_rule_event_evaluations;
DELETE FROM alert_incidents;
DELETE FROM alert_rules;
DELETE FROM telemetry_rollups_5m;
DELETE FROM telemetry_rollups_1h;
DELETE FROM telemetry;
DELETE FROM assets;
DELETE FROM asset_profiles;
DELETE FROM device_profiles;
DELETE FROM devices;
"""
        )
        owner_row = cursor.execute(
            """
SELECT id
FROM users
WHERE username = 'viewer' AND account_class = 'user'
LIMIT 1
"""
        ).fetchone()
        owner_user_id = owner_row[0] if owner_row is not None else None
        claimed_at = iso(rounded_now())
        cursor.executemany(
            """
INSERT INTO device_profiles (id, name, telemetry_schema, metric_mapping, reporting_settings)
VALUES (?, ?, ?, ?, ?)
""",
            [
                (
                    profile["id"],
                    profile["name"],
                    json.dumps(profile["telemetry_schema"]),
                    json.dumps(profile["metric_mapping"]),
                    json.dumps(profile["reporting_settings"]),
                )
                for profile in device_profiles()
            ],
        )
        cursor.executemany(
            """
INSERT INTO asset_profiles (id, name, fields, dashboard_defaults)
VALUES (?, ?, ?, ?)
""",
            [
                (
                    profile["id"],
                    profile["name"],
                    json.dumps(profile["fields"]),
                    json.dumps(profile["dashboard_defaults"]),
                )
                for profile in asset_profiles()
            ],
        )
        cursor.executemany(
            """
INSERT INTO assets (id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata)
VALUES (?, ?, ?, ?, ?, ?)
""",
            [
                (
                    asset["id"],
                    asset["name"],
                    asset["asset_profile_id"],
                    asset["parent_asset_id"],
                    owner_user_id,
                    json.dumps(asset["metadata"]),
                )
                for asset in demo_assets()
            ],
        )
        cursor.executemany(
            """
INSERT INTO devices (
  device_id, display_name, asset_id, device_profile_id, owner_user_id, claimed_at, metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?)
""",
            [
                (
                    device["device_id"],
                    device["display_name"],
                    device["asset_id"],
                    device["device_profile_id"],
                    owner_user_id,
                    claimed_at,
                    json.dumps(device.get("metadata", {"seed": "powermonitor"})),
                )
                for device in demo_devices()
            ],
        )
        cursor.executemany(
            """
INSERT INTO telemetry (
  event_at, received_at, device_id, boot_id, sequence, measurements, topic
) VALUES (?, ?, ?, ?, ?, ?, 'v1/devices/me/telemetry')
""",
            [
                (
                    event["event_at"],
                    event["event_at"],
                    event["device_id"],
                    event["boot_id"],
                    event["sequence"],
                    json.dumps(event["measurements"]),
                )
                for event in demo_events(rounded_now())
            ],
        )


def main():
    parser = argparse.ArgumentParser(
        description="Delete local Power Monitor domain data and seed the demo topology."
    )
    parser.add_argument(
        "--storage",
        choices=("timescale", "sqlite"),
        default=os.environ.get("IOT_DATABASE_STORAGE", "timescale"),
    )
    parser.add_argument(
        "--sqlite-path",
        default=os.environ.get("IOT_SQLITE_PATH"),
        help="SQLite database path when --storage sqlite",
    )
    parser.add_argument(
        "--compose-file",
        default="infra/compose.yaml",
        help="Compose file used for the local TimescaleDB container",
    )
    parser.add_argument("--service", default="timescaledb")
    parser.add_argument(
        "--issue-lighting-token",
        action="store_true",
        help="Use the local iot-api admin session to issue a token for demo-light-switcher-01.",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("IOT_API_URL", "http://127.0.0.1:8080"),
    )
    parser.add_argument(
        "--admin-username",
        default=os.environ.get("IOT_ADMIN_USERNAME", "admin"),
    )
    parser.add_argument(
        "--admin-password",
        default=os.environ.get("IOT_ADMIN_PASSWORD", "NanoAdmin@1234"),
    )
    parser.add_argument(
        "--lighting-env-file",
        default=DEFAULT_LIGHTING_ENV_FILE,
        help="Output file for DEVICE_TOKEN and MQTT settings when issuing a lighting token.",
    )
    parser.add_argument(
        "--mqtt-host",
        default=os.environ.get("MQTT_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--mqtt-port",
        type=int,
        default=int(os.environ.get("MQTT_PORT", "1883")),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required acknowledgement: deletes Power Monitor domain data before seeding.",
    )
    arguments = parser.parse_args()
    if not arguments.yes:
        parser.error("--yes is required because this resets Power Monitor domain data")

    if arguments.storage == "sqlite":
        if not arguments.sqlite_path:
            parser.error("--sqlite-path or IOT_SQLITE_PATH is required for SQLite")
        seed_sqlite(arguments.sqlite_path)
    else:
        seed_timescale(arguments.compose_file, arguments.service)
    if arguments.issue_lighting_token:
        token = issue_lighting_switcher_token(
            arguments.api_url,
            arguments.admin_username,
            arguments.admin_password,
        )
        write_lighting_environment(
            arguments.lighting_env_file,
            token,
            arguments.mqtt_host,
            arguments.mqtt_port,
        )
        print(f"Lighting Switcher environment written to {arguments.lighting_env_file}.")
    print("Power Monitor seed ready: profiles, demo site, panels, switchers, and meters.")


if __name__ == "__main__":
    main()
