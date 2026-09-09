# NanoMQ Virtual RPC Spike

**Date:** 2026-09-08

## Question

Can NanoMQ 0.25.6 route the literal ThingsBoard device subscription
`v1/devices/me/rpc/request/+` to only the MQTT connection authenticated for a
target device token?

## Probe

An isolated `emqx/nanomq:0.25.6` container accepted two clients:

```text
client A: SUBSCRIBE v1/devices/me/rpc/request/+
client B: SUBSCRIBE v1/devices/me/rpc/request/+
```

The platform published:

```text
v1/devices/me/rpc/request/01JRPC
{"method":"sample_now","params":{}}
```

Both clients received the publication.

## Result

NanoMQ 0.25.6 applies standard MQTT topic-filter routing. HTTP auth/ACL can
allow or deny a subscribe based on `%u`, `%A`, and `%t`, but it does not
rewrite a `me` topic to a per-session subscription and has no documented
per-session downlink publish API.

## Decision

`iot-mqtt-transport` owns public TLS MQTT connections on port 8883. It maps:

```text
device token -> token_id -> device_id -> active connection
```

It accepts the virtual ThingsBoard-style subscription and directly emits an
MQTT QoS 1 PUBLISH only to the selected connection. NanoMQ remains an
internal broker for existing private and development flows.
