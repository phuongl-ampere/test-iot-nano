# Upstream

This crate vendors the embedded `rumqttd` library from the upstream project
`bytebeamio/rumqtt`, version `0.20.0`.

Source: `/Users/phuongl/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/rumqttd-0.20.0`

The upstream source was copied mechanically for this local `iot-mqtt-core`
fork. Local changes will be documented in this file.

## Local Differences

- The Cargo package is renamed from `rumqttd` to `iot-mqtt-core`, while the
  exported Rust library name remains `rumqttd` for existing consumers.
- The local package sets `publish = false`.
- The upstream `src/main.rs` and all upstream examples are omitted; this fork
  provides the embedded library only.
- `src/lib.rs` adds
  `IOT_MQTT_CORE_UPSTREAM = "bytebeamio/rumqtt@0.20.0"`.

The following local source changes have been made after the initial fork:

- `src/lib.rs:55-72` adds the public `AuthorizationHandler`,
  `AuthorizationAction`, and `AuthorizationRequest` async policy API.
- `src/lib.rs:152-159` and `src/lib.rs:202-212` add
  `set_authorization_handler` injection methods to `ServerSettings` and
  `ConnectionSettings`.
- `src/lib.rs:181-184` adds the non-serialized authorization handler field to
  `ConnectionSettings`; `src/lib.rs:215-229` reports only whether the handler
  is installed in debug output.
- `src/link/remote.rs:53-67` adds authorization-denial state to the remote
  link.
- `src/link/remote.rs:70-136` carries the policy handler and authenticated
  username into an established connection.
- `src/link/remote.rs:146-170` preserves the initial packet plus `readv` batch
  in a temporary queue, authorizes the complete queue before extending the
  router buffer, and sends one notification only after success.
- `src/link/remote.rs:197-258` authorizes every PUBLISH and each SUBSCRIBE
  filter in order without forwarding denied packets.
- `src/link/remote.rs:286-292` and `src/link/remote.rs:319-350` authorize
  CONNECT after credential authentication and before router registration.
- `src/server/broker.rs:509` retains the connection settings after CONNECT
  parsing, and `src/server/broker.rs:554-562` passes the policy handler into
  the remote link.
- `src/router/connection.rs:39-58` centralizes effective router identity
  construction and tests tenant prefixing.
- `src/server/broker.rs:517-546` computes the assigned and tenant-prefixed
  identity before CONNECT authorization, then uses that identity for will
  tracking and router-link authorization.
- `src/link/remote.rs:66-80` and `src/link/remote.rs:154-164` carry the
  effective identity into all established-connection authorization requests.
- `src/link/remote.rs:210-214` fails closed for empty-topic MQTT5 publishes so
  topic aliases cannot be resolved by the router after authorization.
- `src/link/remote.rs:243-258` carries the authenticated username as both
  username and principal for CONNECT, PUBLISH, and SUBSCRIBE requests.
- `src/link/remote.rs:157-166` and `src/link/remote.rs:265-283` add the
  deterministic authorization batch seam used by the core `Network::readv`
  regression test.
- `src/link/remote.rs:319-355` derives an authenticated principal only when
  static or external credential authentication is configured and has already
  succeeded.
- `src/server/broker.rs:536-576` computes that configured-auth principal once
  and passes it to CONNECT and established-connection authorization.
