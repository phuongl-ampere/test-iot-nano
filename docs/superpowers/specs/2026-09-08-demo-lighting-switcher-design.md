# Demo Lighting Switcher Design

## Goal

Provide a Python MQTT device simulator at `debug/demo_lighting_switcher.py`
that behaves as a controllable lighting switcher on the platform's virtual
device RPC interface.

## Scope

The simulator connects directly to the configured NanoMQ listener, consumes
direct device RPC requests, publishes state telemetry, and supports both
one-way and two-way commands. It is a standalone debug tool and does not
modify the Rust services, API, frontend, database schema, or device profiles.

## Configuration

The script requires `DEVICE_TOKEN`; it must never contain a default token or
write the token to output. It accepts these optional environment variables:

- `MQTT_HOST`, defaulting to `127.0.0.1`
- `MQTT_PORT`, defaulting to `1883`
- `MQTT_CA_FILE`, which enables TLS validation when set
- `PUBLISH_INTERVAL_SECONDS`, defaulting to `10`

It will use the device token as the MQTT username and an empty password. TLS
is optional because the requested development listener at port 1883 is plain
TCP by default.

## MQTT Protocol

After connecting, the simulator subscribes at QoS 1 to:

```text
v1/devices/me/rpc/request/+
```

It publishes telemetry at QoS 1 to:

```text
v1/devices/me/telemetry
```

For a two-way request, it publishes the result at QoS 1 to:

```text
v1/devices/me/rpc/response/{request_id}
```

## State And Commands

`LightingSwitcherState` owns mutable state behind a lock:

- `switch_state`, initially `false`
- `brightness_pct`, initially `0`
- `energy_kwh`, initially a deterministic non-negative value

Supported commands:

- `switch_on`: turns the light on, restoring a usable brightness when needed
- `switch_off`: turns the light off
- `set_power`: requires boolean `params.on`
- `set_brightness`: requires numeric `params.brightness_pct` in the inclusive
  range 0 through 100; zero turns the light off and a positive value turns it
  on

Unsupported methods and invalid parameter shapes are rejected. One-way
requests apply valid changes and return no MQTT response. Two-way requests
return either a structured state result or a structured error.

## Telemetry

Every periodic sample and accepted state change emits a telemetry envelope:

```json
{
  "schema_version": 1,
  "boot_id": "UUID",
  "sequence": 1,
  "event_at": "RFC 3339 UTC timestamp",
  "measurements": {
    "switch_state": true,
    "brightness_pct": 75,
    "power_w": 7.5,
    "energy_kwh": 0.001
  }
}
```

`power_w` is zero while off and increases linearly with brightness while on.
`energy_kwh` advances only while the light is on. The implementation logs
connection, command, and telemetry metadata without logging the token.

## Reliability

Malformed MQTT payloads, non-object request JSON, missing request IDs, and
unrelated topics are ignored. Connection or subscription failure terminates
with a descriptive error. The client stops its MQTT loop and disconnects on
interrupt or shutdown.

## Testing And Verification

`debug/test_demo_lighting_switcher.py` will be written before the
implementation and will cover:

- state transitions and brightness boundaries
- valid one-way and two-way RPC behavior
- error responses for invalid requests
- deterministic telemetry behavior while on and off
- request and response topic validation

Verification will run the focused Python test suite, then start the simulator
with its token supplied only in the process environment. The live smoke check
will confirm a successful MQTT connection, subscription, and telemetry
publication without exposing the token.
