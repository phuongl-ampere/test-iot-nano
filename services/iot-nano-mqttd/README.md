# iot-nano-mqttd

`iot-nano-mqttd` runs as a standalone MQTT 3.1.1 and MQTT 5 broker. The
platform device transport is optional and disabled in the standalone
configuration.

## Standalone Installation

Install the release binary, systemd unit, data directory, and initial
configuration:

```bash
scripts/install-mqttd-standalone.sh
```

Replace both placeholder passwords in the installed TOML file and set the
certificate and key paths before starting the service:

```bash
sudo systemctl enable --now iot-nano-mqttd-standalone.service
```

The broker persists retained messages, sessions, and QoS state in the SQLite
file configured by `[storage]`. The management API binds to loopback and
requires the configured management credentials.

The sample keeps plaintext MQTT on loopback only. Use the TLS listener for
remote clients; exposing username/password authentication on plaintext MQTT
would transmit those credentials without transport protection.

## Authorization

`[static_acl]` authenticates MQTT users from `[[static_acl.users]]` and
authorizes each publish or subscription using `[[static_acl.rules]]`. Limit
each rule to the topic filter required by that user.

`[http_authorization]` is an alternative policy mode for an external
authorization endpoint. It is intentionally mutually exclusive with
`[static_acl]`; an invalid configuration fails before the broker binds its
listeners.

## MQTT 5 Topic Alias

Topic aliases are connection-local. When static ACL or HTTP authorization is
enabled, the broker resolves an alias to its canonical topic before
authorization. An unknown, invalid, or unauthorized alias is rejected.
