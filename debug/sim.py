import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = "localhost"
MQTT_PORT = 1883
DEVICE_TOKEN = os.environ.get("DEVICE_TOKEN", "iotd_08fbddcb5c89efbf28bad2ab4c69bc37b9df249a3d89acd5e882462b5faef1d0")
PUBLISH_INTERVAL_SECONDS = 10

if not DEVICE_TOKEN:
    raise SystemExit("DEVICE_TOKEN is required for the token-authenticated local broker.")


def next_measurements(
    energy_kwh: float,
    interval_seconds: float,
) -> tuple[dict[str, float], float]:
    voltage_v = round(random.uniform(220.0, 240.0), 1)
    current_a = round(random.uniform(0.1, 10.0), 2)
    frequency_hz = round(random.uniform(49.8, 50.2), 2)
    power_factor = round(random.uniform(0.8, 1.0), 2)
    power_w = round(voltage_v * current_a * power_factor, 1)
    energy_kwh += power_w * interval_seconds / 3_600_000

    return {
        "voltage_v": voltage_v,
        "current_a": current_a,
        "power_w": power_w,
        "energy_kwh": energy_kwh,
        "frequency_hz": frequency_hz,
        "power_factor": power_factor,
    }, energy_kwh


def main():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"python-powermonitor-{uuid.uuid4()}",
    )
    client.username_pw_set(DEVICE_TOKEN, password="")
    connected = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            return
        connected.set()

    client.on_connect = on_connect
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    if not connected.wait(timeout=5):
        client.loop_stop()
        client.disconnect()
        raise SystemExit("MQTT token authentication failed.")

    boot_id = str(uuid.uuid4())
    sequence = 0
    energy_kwh = 31.25

    try:
        while True:
            sequence += 1
            measurements, energy_kwh = next_measurements(
                energy_kwh,
                PUBLISH_INTERVAL_SECONDS,
            )
            payload = {
                "schema_version": 1,
                "boot_id": boot_id,
                "sequence": sequence,
                "event_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "measurements": measurements,
            }
            result = client.publish(
                "v1/devices/me/telemetry",
                json.dumps(payload),
                qos=1,
            )
            result.wait_for_publish()
            print("Published", payload)
            time.sleep(PUBLISH_INTERVAL_SECONDS)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
