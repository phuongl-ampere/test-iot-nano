#!/usr/bin/env python3
"""Deterministic lighting switcher state and telemetry helpers."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt


RPC_REQUEST_PREFIX = "v1/devices/me/rpc/request/"
RPC_REQUEST_FILTER = RPC_REQUEST_PREFIX + "+"
RPC_RESPONSE_PREFIX = "v1/devices/me/rpc/response/"
TELEMETRY_TOPIC = "v1/devices/me/telemetry"


@dataclass(frozen=True)
class CommandOutcome:
    applied: bool
    response: Optional[dict]


@dataclass(frozen=True)
class MqttConfiguration:
    token: str
    host: str
    port: int
    ca_file: Optional[str]
    publish_interval_seconds: float


def configuration_from_environment() -> MqttConfiguration:
    token = os.environ.get("DEVICE_TOKEN")
    if not token:
        raise ValueError("DEVICE_TOKEN is required")
    return MqttConfiguration(
        token=token,
        host=os.environ.get("MQTT_HOST", "127.0.0.1"),
        port=int(os.environ.get("MQTT_PORT", "1883")),
        ca_file=os.environ.get("MQTT_CA_FILE") or None,
        publish_interval_seconds=float(
            os.environ.get(
                "TELEMETRY_INTERVAL_SECONDS",
                os.environ.get("PUBLISH_INTERVAL_SECONDS", "10"),
            )
        ),
    )


class TelemetryPublisher:
    def __init__(
        self,
        client: mqtt.Client,
        state: LightingSwitcherState,
        interval_seconds: float,
    ):
        self.client = client
        self.state = state
        self.interval_seconds = interval_seconds
        self.boot_id = str(uuid.uuid4())
        self.sequence = 0

    def publish(self, wait_for_ack: bool = True) -> None:
        telemetry = build_telemetry(
            self.state,
            self.boot_id,
            self.sequence,
            self.interval_seconds,
        )
        self.sequence += 1
        info = self.client.publish(
            TELEMETRY_TOPIC,
            json.dumps(telemetry),
            qos=1,
        )
        if wait_for_ack:
            info.wait_for_publish()


def decode_rpc_request(topic: str, payload: bytes) -> Optional[dict]:
    if not is_rpc_request_topic(topic):
        return None
    try:
        request = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(request, dict) or not isinstance(request.get("method"), str):
        return None
    request_id = topic[len(RPC_REQUEST_PREFIX) :]
    request.setdefault("id", request_id)
    return request


class LightingSwitcherState:
    MAX_POWER_W = 10.0

    def __init__(self):
        self.switch_state = False
        self.brightness_pct = 0
        self.energy_kwh = 0.0
        self._lock = threading.Lock()

    def handle_rpc(self, request: dict) -> CommandOutcome:
        if not isinstance(request, dict):
            return self._outcome(request, False, "invalid_request")

        params = request.get("params", {})
        if not isinstance(params, dict):
            return self._outcome(request, False, "invalid_params")

        with self._lock:
            method = request.get("method")
            if method == "switch_on":
                self.switch_state = True
                self.brightness_pct = self.brightness_pct or 100.0
            elif method == "switch_off":
                self.switch_state = False
            elif method == "set_power" and isinstance(params.get("on"), bool):
                self.switch_state = params["on"]
                if self.switch_state:
                    self.brightness_pct = self.brightness_pct or 100.0
            elif method == "set_brightness" and self._valid_brightness(
                params.get("brightness_pct")
            ):
                self.brightness_pct = float(params["brightness_pct"])
                self.switch_state = self.brightness_pct > 0
            else:
                return self._outcome(request, False, "unsupported_method")
            return self._outcome(request, True, self._snapshot())

    def measurements(self, interval_seconds: float) -> dict:
        with self._lock:
            power_w = (
                self.MAX_POWER_W * self.brightness_pct / 100
                if self.switch_state
                else 0.0
            )
            if self.switch_state:
                self.energy_kwh += power_w * interval_seconds / 3_600_000
            return {
                "switch_state": self.switch_state,
                "brightness_pct": self.brightness_pct,
                "power_w": round(power_w, 3),
                "energy_kwh": round(self.energy_kwh, 9),
            }

    @staticmethod
    def _valid_brightness(value):
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and 0 <= value <= 100
        )

    def _snapshot(self):
        return {
            "switch_state": self.switch_state,
            "brightness_pct": self.brightness_pct,
        }

    @staticmethod
    def _outcome(request, applied, value):
        if not isinstance(request, dict) or request.get("mode") != "two_way":
            return CommandOutcome(applied=applied, response=None)
        if applied:
            return CommandOutcome(applied=True, response={"ok": True, "result": value})
        return CommandOutcome(applied=False, response={"ok": False, "error": value})


def build_telemetry(
    state: LightingSwitcherState,
    boot_id: str,
    sequence: int,
    interval_seconds: float,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "boot_id": boot_id,
        "sequence": sequence,
        "event_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "measurements": state.measurements(interval_seconds),
    }


def is_rpc_request_topic(topic: str) -> bool:
    if not isinstance(topic, str) or not topic.startswith(RPC_REQUEST_PREFIX):
        return False
    request_id = topic[len(RPC_REQUEST_PREFIX) :]
    return (
        bool(request_id)
        and "/" not in request_id
        and "+" not in request_id
        and "#" not in request_id
    )


def rpc_response_topic(request_id: str) -> str:
    if (
        not isinstance(request_id, str)
        or not request_id
        or "/" in request_id
        or "+" in request_id
        or "#" in request_id
    ):
        raise ValueError("request_id must be one non-empty topic segment")
    return RPC_RESPONSE_PREFIX + request_id


def main() -> None:
    configuration = configuration_from_environment()
    state = LightingSwitcherState()
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"python-lighting-switcher-{uuid.uuid4()}",
    )
    client.username_pw_set(configuration.token, password="")
    if configuration.ca_file:
        client.tls_set(ca_certs=configuration.ca_file)

    connected = threading.Event()
    subscribed = threading.Event()
    publisher = TelemetryPublisher(
        client, state, configuration.publish_interval_seconds
    )

    def on_connect(
        mqtt_client,
        _userdata,
        _flags,
        reason_code,
        _properties,
    ):
        if reason_code.is_failure:
            return
        mqtt_client.subscribe(RPC_REQUEST_FILTER, qos=1)
        connected.set()

    def on_subscribe(
        _mqtt_client,
        _userdata,
        _mid,
        granted_qos,
        _properties,
    ):
        if granted_qos and all(qos != 128 for qos in granted_qos):
            subscribed.set()

    def on_message(mqtt_client, _userdata, message):
        request = decode_rpc_request(message.topic, message.payload)
        if request is None:
            return
        outcome = state.handle_rpc(request)
        if outcome.applied:
            publisher.publish(wait_for_ack=False)
        if outcome.response is not None:
            mqtt_client.publish(
                rpc_response_topic(request["id"]),
                json.dumps(outcome.response),
                qos=1,
            )

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    try:
        client.connect(configuration.host, configuration.port, keepalive=60)
        client.loop_start()
        if not connected.wait(10) or not subscribed.wait(10):
            raise SystemExit("MQTT token authentication or RPC subscription failed.")
        while True:
            publisher.publish()
            time.sleep(configuration.publish_interval_seconds)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
