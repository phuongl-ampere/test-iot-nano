#!/usr/bin/env python3
"""Deterministic lighting switcher state and telemetry helpers."""

from __future__ import annotations

import json
import logging
import math
import os
from queue import Empty, Full, Queue
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
PUBLISH_ACK_TIMEOUT_SECONDS = 5.0
REQUEST_QUEUE_MAXSIZE = 100
LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
    )


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
    publish_interval_seconds = float(
        os.environ.get("PUBLISH_INTERVAL_SECONDS", "10")
    )
    if not math.isfinite(publish_interval_seconds) or publish_interval_seconds <= 0:
        raise ValueError("PUBLISH_INTERVAL_SECONDS must be finite and positive")
    return MqttConfiguration(
        token=token,
        host=os.environ.get("MQTT_HOST", "127.0.0.1"),
        port=int(os.environ.get("MQTT_PORT", "1883")),
        ca_file=os.environ.get("MQTT_CA_FILE") or None,
        publish_interval_seconds=publish_interval_seconds,
    )


def publish_qos1(
    client: mqtt.Client,
    topic: str,
    payload: str,
    *,
    description: str,
) -> None:
    try:
        info = client.publish(topic, payload, qos=1)
        if info.rc != 0:
            raise RuntimeError(f"{description} publish failed immediately (rc={info.rc})")
        info.wait_for_publish(timeout=PUBLISH_ACK_TIMEOUT_SECONDS)
        if not info.is_published():
            raise RuntimeError(f"{description} publish acknowledgment timed out")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"{description} publish failed: {exc}") from exc


class TelemetryPublisher:
    def __init__(
        self,
        client: mqtt.Client,
        state: LightingSwitcherState,
        boot_id: str,
    ):
        self.client = client
        self.state = state
        self.boot_id = boot_id

    def publish(self, sequence: int, interval_seconds: float = 0) -> None:
        telemetry = build_telemetry(
            self.state,
            self.boot_id,
            sequence,
            interval_seconds,
        )
        publish_qos1(
            self.client,
            TELEMETRY_TOPIC,
            json.dumps(telemetry),
            description="telemetry",
        )
        LOGGER.info("telemetry sequence=%s", telemetry["sequence"])


def decode_rpc_request(topic: str, payload: bytes) -> Optional[dict]:
    if not is_rpc_request_topic(topic):
        return None
    try:
        request = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(request, dict) or not isinstance(request.get("method"), str):
        return None
    if "mode" in request and request["mode"] not in {"one_way", "two_way"}:
        return None
    request_id = topic[len(RPC_REQUEST_PREFIX) :]
    request["id"] = request_id
    return request


def enqueue_rpc_request(request_queue: Queue, topic: str, payload: bytes) -> bool:
    request = decode_rpc_request(topic, payload)
    if request is None:
        return False
    try:
        request_queue.put_nowait(request)
    except Full:
        LOGGER.warning("request queue full; dropping inbound request")
        return False
    return True


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
        if "mode" in request and request["mode"] not in {"one_way", "two_way"}:
            return self._outcome(request, False, "invalid_mode")

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

    def advance_energy(self, elapsed_seconds: float) -> None:
        if elapsed_seconds < 0 or not math.isfinite(elapsed_seconds):
            raise ValueError("elapsed_seconds must be finite and non-negative")
        with self._lock:
            power_w = (
                self.MAX_POWER_W * self.brightness_pct / 100
                if self.switch_state
                else 0.0
            )
            if self.switch_state:
                self.energy_kwh += power_w * elapsed_seconds / 3_600_000

    def measurements(self, interval_seconds: float) -> dict:
        self.advance_energy(interval_seconds)
        with self._lock:
            power_w = (
                self.MAX_POWER_W * self.brightness_pct / 100
                if self.switch_state
                else 0.0
            )
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


def subscription_granted(granted_qos) -> bool:
    return isinstance(granted_qos, (list, tuple)) and granted_qos == [1]


def main() -> None:
    try:
        configuration = configuration_from_environment()
    except (TypeError, ValueError, OverflowError) as exc:
        LOGGER.error("MQTT configuration failed: %s", exc)
        raise SystemExit("MQTT configuration failed.") from None

    state = LightingSwitcherState()
    client = None
    try:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"python-lighting-switcher-{uuid.uuid4()}",
        )
        client.username_pw_set(configuration.token, password="")
        if configuration.ca_file:
            client.tls_set(ca_certs=configuration.ca_file)
    except Exception as exc:
        LOGGER.error("MQTT initialization failed: %s", exc)
        if client is not None:
            client.loop_stop()
            client.disconnect()
        raise SystemExit("MQTT initialization failed.") from None

    connected = threading.Event()
    subscribed = threading.Event()
    startup_failed = threading.Event()
    readiness = threading.Event()
    stopping = threading.Event()
    request_queue = Queue(maxsize=REQUEST_QUEUE_MAXSIZE)
    publisher = TelemetryPublisher(
        client, state, str(uuid.uuid4())
    )

    def fail_startup(message: str) -> None:
        LOGGER.error("MQTT startup failed: %s", message)
        startup_failed.set()
        readiness.set()

    def on_connect(
        mqtt_client,
        _userdata,
        _flags,
        reason_code,
        _properties,
    ):
        if reason_code.is_failure:
            fail_startup("broker connection rejected")
            return
        try:
            subscribe_result = mqtt_client.subscribe(RPC_REQUEST_FILTER, qos=1)
        except Exception as exc:
            fail_startup(f"RPC subscription failed: {exc}")
            return
        subscribe_rc = (
            subscribe_result[0]
            if isinstance(subscribe_result, (tuple, list))
            else subscribe_result
        )
        if subscribe_rc != 0:
            fail_startup(f"RPC subscription failed immediately (rc={subscribe_rc})")
            return
        if startup_failed.is_set():
            return
        connected.set()

    def on_subscribe(
        _mqtt_client,
        _userdata,
        _mid,
        granted_qos,
        _properties,
    ):
        if subscription_granted(granted_qos):
            subscribed.set()
            readiness.set()
        else:
            fail_startup("RPC subscription did not grant exact QoS 1")

    def on_message(_mqtt_client, _userdata, message):
        enqueue_rpc_request(request_queue, message.topic, message.payload)

    def on_connect_fail(_mqtt_client, _userdata):
        if not stopping.is_set():
            fail_startup("broker reconnect failed")

    def on_disconnect(
        _mqtt_client,
        _userdata,
        _disconnect_flags,
        _reason_code,
        _properties,
    ):
        if not stopping.is_set():
            fail_startup("unexpected broker disconnect")

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_connect_fail = on_connect_fail
    client.on_disconnect = on_disconnect
    try:
        client.connect(configuration.host, configuration.port, keepalive=60)
        client.loop_start()
        if startup_failed.is_set():
            raise SystemExit("MQTT token authentication or RPC subscription failed.")
        if (
            not readiness.wait(10)
            or startup_failed.is_set()
            or not connected.is_set()
            or not subscribed.is_set()
        ):
            raise SystemExit("MQTT token authentication or RPC subscription failed.")
        LOGGER.info(
            "connected host=%s port=%s subscription=%s",
            configuration.host,
            configuration.port,
            RPC_REQUEST_FILTER,
        )
        sequence = 0
        last_energy_at = time.monotonic()
        next_periodic_at = last_energy_at + configuration.publish_interval_seconds
        publisher.publish(sequence)
        sequence += 1
        while True:
            if startup_failed.is_set():
                raise SystemExit("MQTT connection or RPC subscription failed.")
            now = time.monotonic()
            if now >= next_periodic_at:
                state.advance_energy(max(0.0, now - last_energy_at))
                last_energy_at = now
                publisher.publish(sequence)
                sequence += 1
                next_periodic_at = now + configuration.publish_interval_seconds
                continue

            try:
                request = request_queue.get_nowait()
            except Empty:
                time.sleep(min(0.1, max(0.0, next_periodic_at - now)))
                continue

            state.advance_energy(max(0.0, now - last_energy_at))
            last_energy_at = now
            outcome = state.handle_rpc(request)
            if outcome.applied:
                publisher.publish(sequence)
                sequence += 1
            if outcome.response is not None:
                publish_qos1(
                    client,
                    rpc_response_topic(request["id"]),
                    json.dumps(outcome.response),
                    description="RPC response",
                )
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested")
    except Exception as exc:
        LOGGER.error("MQTT worker failed: %s", exc)
        raise SystemExit("MQTT publication failed.") from exc
    finally:
        stopping.set()
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    configure_logging()
    main()
