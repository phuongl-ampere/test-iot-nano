# MQTTD Standalone Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` or `executing-plans` task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `iot-nano-mqttd` as an independently configured MQTT broker with authenticated ACLs and safe MQTT 5 Topic Alias authorization.

**Architecture:** Resolve inbound MQTT 5 Topic Alias state inside the per-connection remote link before an external authorization handler runs. The broker router still owns protocol validation and delivery. A standalone TOML configuration disables platform device transport and configures only broker state, listener TLS, management credentials, static auth, and ACL rules.

**Tech Stack:** Rust 2024, vendored `rumqttd`, Tokio, MQTT 3.1.1/MQTT 5, SQLite, TOML, and Rust integration tests.

## Global Constraints

- Keep API, Stream, and Core integrations optional; standalone config requires none of their URLs or secrets.
- Authorization remains fail-closed for missing, invalid, unknown, and unauthorized aliases.
- Alias mappings are connection-local and never persistent.
- Static ACL and HTTP authorization remain mutually exclusive.
- Every behavioral change starts with a focused failing test.

---

### Task 1: Resolve MQTT 5 Topic Aliases Before Policy Evaluation

**Files:**
- Modify: `services/iot-nano-mqttd/vendor/rumqttd/src/{lib.rs,link/remote.rs,router/routing.rs}`
- Modify: `services/iot-nano-mqttd/tests/authorization.rs`

**Interfaces:**
- Consumes: `Packet::Publish(Publish, Option<PublishProperties>)` and `PublishProperties.topic_alias`.
- Produces: `AuthorizationRequest.topic` containing the canonical topic for every authorized MQTT 5 alias-only PUBLISH.

- [x] **Step 1: Write the failing test**

Replace the current alias-denial test with an authorization handler that accepts only `sensors/unit-1/telemetry`, publishes once with that topic and alias `1`, then publishes with an empty topic and alias `1`.

```rust
assert_eq!(received.recv().await.unwrap(), b"first");
assert_eq!(received.recv().await.unwrap(), b"second");
assert_eq!(
    captured.lock().unwrap().as_slice(),
    ["sensors/unit-1/telemetry", "sensors/unit-1/telemetry"]
);
```

- [x] **Step 2: Verify RED**

Run: `cargo test -p iot-nano-mqttd --test authorization mqtt5_alias_only -- --test-threads=1`

Expected: failure because `authorize_packet` rejects an empty topic before router alias resolution.

- [x] **Step 3: Write the minimum production code**

Export a shared `MAX_TOPIC_ALIAS` from vendored `rumqttd`. Add `topic_aliases: HashMap<u16, String>` to `RemoteLink`. Before `authorize_batch`, only when `authorization_handler` exists, update non-empty mappings and replace alias-only topics from that map. Reject alias `0`, aliases above the maximum, aliases with non-UTF-8 topics, and unknown alias-only publishes with `Error::AuthorizationDenied`.

```rust
if publish.topic.is_empty() {
    publish.topic = aliases.get(&alias).ok_or(Error::AuthorizationDenied)?.clone().into();
} else {
    aliases.insert(alias, std::str::from_utf8(&publish.topic)
        .map_err(|_| Error::AuthorizationDenied)?.to_owned());
}
```

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p iot-nano-mqttd --test authorization mqtt5_alias_only -- --test-threads=1`

Expected: PASS; both delivery and policy use the canonical topic.

### Task 2: Prove Standalone Authenticated Broker Startup

**Files:**
- Create: `services/iot-nano-mqttd/config/standalone.toml`
- Create: `services/iot-nano-mqttd/tests/standalone.rs`

**Interfaces:**
- Consumes: `iot-nano-mqttd --config <toml>`.
- Produces: a running broker with SQLite persistence, authenticated management status, static MQTT authentication/ACL, and no platform device transport.

- [x] **Step 1: Write the failing test**

Create `tests/standalone.rs`:

```rust
const EXAMPLE: &str = include_str!("../config/standalone.toml");

#[test]
fn standalone_example_disables_platform_transport() {
    let config = BrokerFileConfig::from_toml(EXAMPLE).unwrap();
    assert!(!config.device_transport.enabled);
    assert!(matches!(config.storage, StorageConfig::Sqlite { .. }));
    assert!(config.static_acl.as_ref().is_some_and(|acl| acl.enabled));
}
```

- [x] **Step 2: Verify RED**

Run: `cargo test -p iot-nano-mqttd --test standalone -- --test-threads=1`

Expected: compilation fails because the package configuration artifact does not exist.

- [x] **Step 3: Add config and process acceptance coverage**

Create a version `1` config with explicit TCP/TLS listener addresses, SQLite persistence, management credentials, `device_transport.enabled = false`, one example static user, and publish/subscribe rules limited to `sensors/+/telemetry`. Extend the test to start `CARGO_BIN_EXE_iot-nano-mqttd` with a temporary replacement config, without platform variables; poll `/healthz`, read authenticated status, and connect an MQTT client using the configured static credentials.

- [x] **Step 4: Verify GREEN**

Run: `cargo test -p iot-nano-mqttd --test standalone -- --test-threads=1`

Expected: PASS; standalone startup does not require platform configuration and reports SQLite/static ACL capability.

### Task 3: Package the Standalone Service

**Files:**
- Create: `services/iot-nano-mqttd/README.md`
- Create: `infra/systemd/iot-nano-mqttd-standalone.service`
- Create: `scripts/install-mqttd-standalone.sh`

**Interfaces:**
- Consumes: an installed binary, `/etc/rush-iot-nano/iot-nano-mqttd.toml`, SQLite state directory, TLS certificate, and TLS key.
- Produces: a systemd service that starts after only network readiness.

- [x] **Step 1: Add the package instructions**

Document the standalone config copy location, config file permission `0600`, TLS paths, data directory ownership, `systemctl enable --now iot-nano-mqttd`, the two policy modes, and MQTT 5 Topic Alias behavior.

- [x] **Step 2: Add an isolated systemd unit**

Create `iot-nano-mqttd-standalone.service` with
`After=network-online.target`, `Wants=network-online.target`, and
`ExecStart=/opt/rush-iot-nano/iot-nano-mqttd --config /etc/rush-iot-nano/iot-nano-mqttd.toml`.
Grant only `CAP_NET_BIND_SERVICE`, provision the SQLite parent directory, and
install the sample configuration only when an operator configuration does not
already exist.

- [x] **Step 3: Verify final package**

Run:

```bash
cargo fmt --all -- --check
cargo test -p iot-nano-mqttd -- --test-threads=1
cargo build --release -p iot-nano-mqttd
```

Expected: formatter succeeds, all package tests pass, and the release binary builds.
