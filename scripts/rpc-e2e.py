#!/usr/bin/env python3
"""Exercise the public TLS MQTT RPC transport against a locally started stack."""

import argparse
import json
import queue
import ssl
import threading
import time
import uuid
from dataclasses import dataclass, field
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import paho.mqtt.client as mqtt


def api_request(base_url, method, path, body=None, session_id=None):
    payload = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if session_id:
        headers["Authorization"] = f"Session {session_id}"
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        data=payload,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=5) as response:
            response_body = response.read()
            return response.status, json.loads(response_body or b"{}")
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned HTTP {error.code}: {body}") from error


def wait_for_command_state(base_url, session_id, command_id, expected, timeout_seconds=10):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        _, command = api_request(
            base_url,
            "GET",
            f"/api/device-commands/{command_id}",
            session_id=session_id,
        )
        if command["state"] == expected:
            return command
        time.sleep(0.2)
    raise RuntimeError(f"command {command_id} did not reach {expected}")


@dataclass
class RpcDevice:
    host: str
    port: int
    ca_path: str
    token: str
    client_id: str
    messages: queue.Queue = field(default_factory=queue.Queue)
    subscribed: threading.Event = field(default_factory=threading.Event)

    def __post_init__(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=mqtt.MQTTv311,
        )
        self.client.username_pw_set(self.token, password="")
        self.client.tls_set(ca_certs=self.ca_path, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties):
        if reason_code.is_failure:
            return
        client.subscribe("v1/devices/me/rpc/request/+", qos=1)

    def _on_subscribe(self, _client, _userdata, _mid, granted_qos, _properties):
        if granted_qos and granted_qos[0] <= 1:
            self.subscribed.set()

    def _on_message(self, _client, _userdata, message):
        self.messages.put((message.topic, json.loads(message.payload.decode("utf-8"))))

    def start(self):
        self.client.connect(self.host, self.port, keepalive=30)
        self.client.loop_start()
        if not self.subscribed.wait(timeout=10):
            self.stop()
            raise RuntimeError(f"{self.client_id} did not subscribe through the transport")

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def receive(self, timeout_seconds=5):
        return self.messages.get(timeout=timeout_seconds)

    def assert_no_message(self, timeout_seconds=1):
        try:
            message = self.messages.get(timeout=timeout_seconds)
        except queue.Empty:
            return
        raise RuntimeError(f"{self.client_id} unexpectedly received {message}")


def provision_device(base_url, session_id, name):
    _, device = api_request(
        base_url,
        "POST",
        "/api/management/devices",
        {"display_name": name},
        session_id,
    )
    if not device.get("device_id") or not device.get("token"):
        raise RuntimeError(f"device provisioning did not return ID and token: {device}")
    return device


def send_command(base_url, session_id, device_id):
    _, command = api_request(
        base_url,
        "POST",
        f"/api/devices/{device_id}/commands",
        {"method": "sample_now", "params": {}, "mode": "one_way"},
        session_id,
    )
    return command


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-base-url", required=True)
    parser.add_argument("--mqtt-host", default="127.0.0.1")
    parser.add_argument("--mqtt-port", type=int, required=True)
    parser.add_argument("--ca-path", required=True)
    args = parser.parse_args()

    _, login = api_request(
        args.api_base_url,
        "POST",
        "/api/auth/login",
        {"username": "admin", "password": "NanoAdmin@1234"},
    )
    session_id = login["session_id"]
    suffix = uuid.uuid4().hex[:8]
    device_a = provision_device(args.api_base_url, session_id, f"rpc-e2e-a-{suffix}")
    device_b = provision_device(args.api_base_url, session_id, f"rpc-e2e-b-{suffix}")
    device_a_client = RpcDevice(
        args.mqtt_host,
        args.mqtt_port,
        args.ca_path,
        device_a["token"],
        f"rpc-e2e-a-{suffix}",
    )
    device_b_client = RpcDevice(
        args.mqtt_host,
        args.mqtt_port,
        args.ca_path,
        device_b["token"],
        f"rpc-e2e-b-{suffix}",
    )

    try:
        device_a_client.start()
        device_b_client.start()

        command_a = send_command(args.api_base_url, session_id, device_a["device_id"])
        topic, payload = device_a_client.receive()
        if topic != f"v1/devices/me/rpc/request/{command_a['id']}":
            raise RuntimeError(f"device A received the wrong RPC topic: {topic}")
        if payload["id"] != command_a["id"] or payload["mode"] != "one_way":
            raise RuntimeError(f"device A received the wrong RPC payload: {payload}")
        device_b_client.assert_no_message()
        wait_for_command_state(args.api_base_url, session_id, command_a["id"], "published_to_broker")

        api_request(
            args.api_base_url,
            "POST",
            f"/api/device-tokens/{device_a['id']}/revoke",
            session_id=session_id,
        )
        revoked_command = send_command(args.api_base_url, session_id, device_a["device_id"])
        device_a_client.assert_no_message()
        time.sleep(1)
        wait_for_command_state(args.api_base_url, session_id, revoked_command["id"], "queued")

        rotated = api_request(
            args.api_base_url,
            "POST",
            f"/api/device-tokens/{device_b['id']}/rotate",
            session_id=session_id,
        )[1]
        device_b_client.assert_no_message()
        replacement = RpcDevice(
            args.mqtt_host,
            args.mqtt_port,
            args.ca_path,
            rotated["token"],
            f"rpc-e2e-b-replacement-{suffix}",
        )
        replacement.start()
        try:
            command_b = send_command(args.api_base_url, session_id, device_b["device_id"])
            topic, payload = replacement.receive()
            if topic != f"v1/devices/me/rpc/request/{command_b['id']}":
                raise RuntimeError(f"rotated device B received the wrong RPC topic: {topic}")
            if payload["id"] != command_b["id"]:
                raise RuntimeError(f"rotated device B received the wrong RPC payload: {payload}")
            wait_for_command_state(
                args.api_base_url, session_id, command_b["id"], "published_to_broker"
            )
        finally:
            replacement.stop()

        offline_command = send_command(args.api_base_url, session_id, device_b["device_id"])
        wait_for_command_state(args.api_base_url, session_id, offline_command["id"], "expired", 45)
    finally:
        device_a_client.stop()
        device_b_client.stop()

    print("RPC transport E2E passed: isolation, publication, revoke, rotate, and offline expiry")


if __name__ == "__main__":
    main()
