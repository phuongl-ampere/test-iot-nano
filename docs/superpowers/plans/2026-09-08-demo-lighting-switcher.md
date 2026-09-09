# Demo Lighting Switcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a Python MQTT lighting-switcher simulator using the
local NanoMQ listener at port 1883.

**Architecture:** `debug/demo_lighting_switcher.py` separates deterministic
lighting state and telemetry generation from MQTT callback wiring. The MQTT
client authenticates with a token supplied only through `DEVICE_TOKEN`,
subscribes to the direct RPC request filter, and publishes telemetry or
two-way command results at QoS 1.

**Tech Stack:** Python 3 standard library and installed `paho-mqtt`.

## Global Constraints

- Use `MQTT_HOST=127.0.0.1` and `MQTT_PORT=1883` as defaults.
- Require `DEVICE_TOKEN`; do not hard-code, persist, or log it.
- Use QoS 1 for RPC subscription, telemetry, and two-way response publishes.
- Support `switch_on`, `switch_off`, `set_power`, and `set_brightness`.
- Emit `switch_state`, `brightness_pct`, `power_w`, and `energy_kwh`.
- Keep implementation and tests scoped to `debug/`.

---

### Task 1: Lighting State And Telemetry

**Files:**
- Create: `debug/test_demo_lighting_switcher.py`
- Create: `debug/demo_lighting_switcher.py`

**Interfaces:**
- Produces `LightingSwitcherState`, with
  `handle_rpc(request: dict) -> CommandOutcome` and
  `measurements(interval_seconds: float) -> dict`.
- Produces `build_telemetry(state, boot_id, sequence, interval_seconds) -> dict`.
- Produces `is_rpc_request_topic(topic: str) -> bool` and
  `rpc_response_topic(request_id: str) -> str`.

- [ ] **Step 1: Write the failing state and telemetry tests**

```python
class LightingSwitcherTests(unittest.TestCase):
    def test_set_brightness_updates_state_and_two_way_result(self):
        state = module.LightingSwitcherState()
        outcome = state.handle_rpc({
            "id": "command-1",
            "method": "set_brightness",
            "params": {"brightness_pct": 75},
            "mode": "two_way",
        })
        self.assertTrue(outcome.applied)
        self.assertEqual(
            outcome.response,
            {"ok": True, "result": {"switch_state": True, "brightness_pct": 75}},
        )

    def test_off_measurement_has_zero_power_and_unchanged_energy(self):
        state = module.LightingSwitcherState()
        before = state.measurements(10)["energy_kwh"]
        after = state.measurements(10)
        self.assertEqual(after["power_w"], 0.0)
        self.assertEqual(after["energy_kwh"], before)
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
python3 -m unittest debug/test_demo_lighting_switcher.py -v
```

Expected: FAIL because `debug/demo_lighting_switcher.py` does not exist.

- [ ] **Step 3: Implement the minimal state and telemetry API**

```python
@dataclass(frozen=True)
class CommandOutcome:
    applied: bool
    response: dict | None


class LightingSwitcherState:
    MAX_POWER_W = 10.0

    def handle_rpc(self, request: dict) -> CommandOutcome:
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
                self.brightness_pct = self.brightness_pct or 100.0 if self.switch_state else self.brightness_pct
            elif method == "set_brightness" and self._valid_brightness(params.get("brightness_pct")):
                self.brightness_pct = float(params["brightness_pct"])
                self.switch_state = self.brightness_pct > 0
            else:
                return self._outcome(request, False, "unsupported_method")
            return self._outcome(request, True, self._snapshot())

    def measurements(self, interval_seconds: float) -> dict:
        with self._lock:
            power_w = self.MAX_POWER_W * self.brightness_pct / 100 if self.switch_state else 0.0
            self.energy_kwh += power_w * interval_seconds / 3_600_000
            return {
                "switch_state": self.switch_state,
                "brightness_pct": self.brightness_pct,
                "power_w": round(power_w, 3),
                "energy_kwh": round(self.energy_kwh, 9),
            }
```

Implement one-way requests as `CommandOutcome(applied=True, response=None)`;
two-way requests return `{"ok": true, "result": snapshot}`. Invalid
requests return `applied=False` and a two-way
`{"ok": false, "error": "<reason>"}` response. `build_telemetry` must add
the envelope fields from the design document.

- [ ] **Step 4: Run the focused test to verify it passes**

Run:

```bash
python3 -m unittest debug/test_demo_lighting_switcher.py -v
```

Expected: PASS with all lighting state and telemetry tests green.

- [ ] **Step 5: Commit the tested state layer**

```bash
git add debug/demo_lighting_switcher.py debug/test_demo_lighting_switcher.py
git commit -m "feat: add lighting switcher state simulator"
```

### Task 2: MQTT Runtime

**Files:**
- Modify: `debug/demo_lighting_switcher.py`
- Modify: `debug/test_demo_lighting_switcher.py`

**Interfaces:**
- Consumes `LightingSwitcherState`, `build_telemetry`,
  `is_rpc_request_topic`, and `rpc_response_topic` from Task 1.
- Produces `main() -> None`, the executable MQTT simulator entry point.

- [ ] **Step 1: Write failing configuration and topic tests**

```python
def test_configuration_defaults_to_the_local_plain_mqtt_listener(self):
    with mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True):
        configuration = module.configuration_from_environment()
    self.assertEqual(configuration.host, "127.0.0.1")
    self.assertEqual(configuration.port, 1883)
    self.assertEqual(configuration.ca_file, None)

def test_rpc_topics_require_one_request_identifier(self):
    self.assertTrue(module.is_rpc_request_topic("v1/devices/me/rpc/request/cmd-1"))
    self.assertFalse(module.is_rpc_request_topic("v1/devices/me/rpc/request/+"))
    self.assertEqual(
        module.rpc_response_topic("cmd-1"),
        "v1/devices/me/rpc/response/cmd-1",
    )
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
python3 -m unittest debug/test_demo_lighting_switcher.py -v
```

Expected: FAIL because `configuration_from_environment` is absent.

- [ ] **Step 3: Implement MQTT configuration and callbacks**

```python
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
    publisher = TelemetryPublisher(client, state, configuration.publish_interval_seconds)

    def on_connect(mqtt_client, _userdata, _flags, reason_code, _properties):
        if reason_code.is_failure:
            return
        mqtt_client.subscribe(RPC_REQUEST_FILTER, qos=1)
        connected.set()

    def on_subscribe(_mqtt_client, _userdata, _mid, _granted_qos, _properties):
        subscribed.set()

    def on_message(mqtt_client, _userdata, message):
        request = decode_rpc_request(message.topic, message.payload)
        if request is None:
            return
        outcome = state.handle_rpc(request)
        if outcome.applied:
            publisher.publish(wait_for_ack=False)
        if outcome.response is not None:
            mqtt_client.publish(rpc_response_topic(request["id"]), json.dumps(outcome.response), qos=1)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect(configuration.host, configuration.port, keepalive=60)
    client.loop_start()
    if not connected.wait(10) or not subscribed.wait(10):
        raise SystemExit("MQTT token authentication or RPC subscription failed.")
    try:
        while True:
            publisher.publish()
            time.sleep(configuration.publish_interval_seconds)
    finally:
        client.loop_stop()
        client.disconnect()
```

On successful connection, subscribe to `v1/devices/me/rpc/request/+` at QoS
1. On a valid RPC request, call `state.handle_rpc`, publish immediate
telemetry when `outcome.applied` is true, and publish `outcome.response` when
it is non-null. Publish periodic telemetry at the configured interval.
Enable `tls_set` only when `MQTT_CA_FILE` is set. Do not print the token.

- [ ] **Step 4: Run all simulator tests to verify they pass**

Run:

```bash
python3 -m unittest debug/test_demo_lighting_switcher.py -v
```

Expected: PASS with state, telemetry, configuration, and topic tests green.

- [ ] **Step 5: Commit the MQTT runtime**

```bash
git add debug/demo_lighting_switcher.py debug/test_demo_lighting_switcher.py
git commit -m "feat: run lighting switcher over mqtt"
```

### Task 3: Live MQTT Smoke Check

**Files:**
- Verify: `debug/demo_lighting_switcher.py`

**Interfaces:**
- Consumes the port-1883 configuration and a supplied process-local
  `DEVICE_TOKEN`.
- Produces a connected simulator that publishes telemetry and awaits RPC.

- [ ] **Step 1: Run the focused unit suite**

Run:

```bash
python3 -m unittest debug/test_demo_lighting_switcher.py -v
```

Expected: PASS.

- [ ] **Step 2: Start the simulator with the token in the process environment**

Run:

```bash
DEVICE_TOKEN="$DEVICE_TOKEN" \
MQTT_HOST=127.0.0.1 \
MQTT_PORT=1883 \
PUBLISH_INTERVAL_SECONDS=2 \
python3 debug/demo_lighting_switcher.py
```

Expected: logs a successful connection, subscription, and QoS 1 telemetry
publication without printing the token.

- [ ] **Step 3: Stop the smoke-check process cleanly**

Send `SIGINT` after observing at least one telemetry publication. Expected:
the MQTT loop stops and the client disconnects without a traceback.
