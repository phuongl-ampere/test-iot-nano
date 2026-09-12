# MQTTD Standalone Policy Design

## Goal

Package `iot-nano-mqttd` as a standalone MQTT broker that can run with a
single TOML file, SQLite broker persistence, static credential and ACL
enforcement, optional HTTP authorization, and MQTT 5 Topic Alias support.

## Scope

- Standalone mode is selected by a TOML configuration whose
  `device_transport.enabled` value is `false`. It must not require API, Stream,
  Core, or any platform secret.
- Static ACL and HTTP authorization remain mutually exclusive. Both
  authenticate before accepting a connection and authorize each publish or
  subscription using a canonical MQTT topic.
- A MQTT 5 PUBLISH that supplies a topic alias maps the alias to its canonical
  topic for that connection. A later PUBLISH with an empty topic and the same
  alias is authorized against that canonical topic.
- An invalid, unknown, or unauthorized alias remains fail-closed.

## Design

The vendored MQTT core currently calls its external authorization handler
before resolving a MQTT 5 Topic Alias. This makes an alias-only PUBLISH look
like an empty topic and the handler denies it, even though the router could
later resolve the alias.

`RemoteLink` will maintain a per-connection alias map only when an external
authorization handler is configured. Before authorizing a received packet
batch, it will validate the alias range and:

1. record a non-empty topic against its supplied alias;
2. replace an alias-only PUBLISH topic with its previously recorded canonical
   topic; and
3. reject an unknown alias before it reaches authorization or routing.

The router continues to resolve and validate the same protocol field, so the
authorization map does not become business state or bypass router validation.
No aliases are persisted across reconnects.

`services/iot-nano-mqttd/config/standalone.toml` will provide a production
starting point with SQLite, TCP/TLS listeners, management authentication,
static credentials, static ACL rules, and disabled platform device transport.
The systemd unit starts this config directly, has no API or Stream ordering
dependency, and grants only `CAP_NET_BIND_SERVICE` to the unprivileged broker
process. Platform integration remains available only when explicitly enabled
in a different config.

## Testing

- A failing integration test changes the existing MQTT 5 alias denial
  expectation to require two authorized publishes, with the second using only
  a Topic Alias.
- The test captures authorization requests and requires both to contain the
  canonical topic. A static ACL regression verifies the same behavior through
  credential authentication and ACL rules.
- A standalone process test compiles in the example configuration, starts the
  broker without platform environment variables, checks management status, and
  authenticates a configured MQTT client.
- The package suite, formatter, and standalone binary build verify the final
  result.
