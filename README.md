# Rush IoT Nano

Rush IoT Nano is a Rust and Next.js IoT platform for token-authenticated
devices.

## Runtime

The production target is one Rust binary, `iot-nano-monolith`, with API, Core,
Stream, MQTTD, cache, and command delivery composed in one Tokio runtime:

```text
device -> iot-nano-monolith -> stream.sqlite -> platform storage
browser -> iot-nano-monolith -> platform storage
```

- The public listener serves HTTP, MQTT TCP, and MQTT TLS.
- Management and administrative routes bind to
  `IOT_NANO_MANAGEMENT_ADDRESS`, which defaults to loopback
  (`127.0.0.1:8081`).
- API, Core, Stream, and MQTTD are library packages in monolith mode, not
  child processes or deployment services.

The monolith deployment is for a fresh environment. It must not be pointed at,
or used to directly convert or reuse, state owned by the former API, Core,
Stream, or MQTTD services. See
[docs/operations-monolith.md](docs/operations-monolith.md) for storage,
secrets, startup, and rollback procedures.

## Device MQTT

Devices connect with the device token as username and an empty password.
MQTTD supports MQTT 3.1.1 and MQTT 5 over TCP `1883` and TLS `8883`.

```text
subscribe: v1/devices/me/rpc/request/+
request:   v1/devices/me/rpc/request/{command_id}
response:  v1/devices/me/rpc/response/{command_id}
```

RPC uses QoS 1. MQTTD publishes a request only to the authenticated active
device session and waits for device PUBACK. A two-way response is accepted
only from that session, at the matching response topic and command ID.

## Development

Development state is disposable. Architecture or schema changes do not migrate
old data: stop the monolith, reset its platform and internal state, then
bootstrap the current schema.

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test -p iot-nano-mqttd -- --test-threads=1
cargo test -p iot-nano-stream -- --test-threads=1
cargo test -p iot-nano-core -- --test-threads=1
cargo test -p iot-nano-api -- --test-threads=1
```

### Cargo lanes

Use the lane wrapper for focused Rust commands. It assigns a persistent target
directory unique to the current worktree and lane, so concurrent worktrees do
not share Cargo fingerprints or locks.

```bash
./scripts/dev/test-cargo-lane.sh
./scripts/dev/cargo-lane.sh fast-storage -- test -p iot-storage --test identity
./scripts/dev/cargo-lane.sh sqlite-contract -- test -p iot-storage
RUSTC_WRAPPER="$(command -v sccache)" ./scripts/dev/cargo-lane.sh fast-monolith -- check -p iot-nano-monolith
```

For a focused test, do not run `cargo check` first unless a type-only result is
all that is needed. The test command already compiles the selected crate and
test harness; using the same lane reuses cached dependencies. For example:

```bash
./scripts/dev/cargo-lane.sh ui-console -- \
  test -p iot-nano-monolith --test platform_ui_templates
```

The wrapper appends local elapsed-time entries to `.cargo-lane/timing.log`.
Set `IOT_NANO_LANE_TARGET_ROOT` or `IOT_NANO_LANE_LOG` to override the local
cache or timing-log locations. It never configures Rust compiler wrappers;
callers opt into `sccache` and retain all Rust/Cargo wrapper settings.

### Fresh local PowerMonitor seed

Use the local seed configuration at
`infra/monolith/local-platform-seed.env` and reset the complete disposable
local platform with:

```bash
IOT_NANO_ALLOW_LOCAL_SEED=1 ./scripts/dev/seed-local-platform.sh --reset
```

The command stops only a verified local monolith, clears its platform SQLite
database plus stream, MQTTD, and cache state, bootstraps the system account,
starts a new monolith, and seeds the PowerMonitor fixture. It retains local
TLS material and the device-token vault key. Existing PowerMonitor browser
sessions are invalid after the reset and require a new sign-in.

Run broad checks only in the release lane:

```bash
IOT_NANO_TIMESCALE_TEST_URL=postgres://... ./scripts/dev/release-verify.sh
```

The release command runs the workspace compile gate, SQLite process check,
optional Timescale check, and external PowerMonitor contract. Without
`IOT_NANO_TIMESCALE_TEST_URL`, it reports that the Timescale check was skipped.

## Repository Layout

```text
services/   Rust libraries plus the deployable monolith binary
crates/     storage and core platform crates
contracts/  versioned public and device data contracts
infra/      Compose, systemd, and monolith environment templates
docs/       architecture, plans, and operations guidance
firmware/   ESP32 firmware
```

The production design and delivery plan are in
`docs/superpowers/specs/` and `docs/superpowers/plans/`. Operational guidance
is in [docs/operations-monolith.md](docs/operations-monolith.md).
