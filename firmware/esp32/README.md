# ESP32 Firmware

The firmware starts `Rush-IoT-Setup` when NVS configuration is missing, Wi-Fi
cannot connect within 30 seconds, or GPIO0 is held low during boot. Open the AP
address shown on serial output to configure Wi-Fi, DHCP/static IPv4, MQTT TLS
host, device token, CA PEM, and telemetry interval.

The AP and web configuration endpoint require username `admin` and the
`PROVISIONING_PASSWORD` build flag. Replace
`replace-before-flashing` in `platformio.ini` with a per-deployment password
before flashing.

MQTT is TLS-only on port `8883`. The device token is sent as the MQTT username
with an empty password. The CA PEM is used to validate the broker certificate;
the token and CA are intentionally not rendered back into the provisioning
form. Telemetry is published to `v1/devices/me/telemetry` without a
`device_id` JSON field. Each boot assigns a canonical RFC 4122 UUID v4
`boot_id`.

The default sensor implementation publishes `adc_raw` from GPIO34 and
`uptime_ms`. Replace those measurements in `src/main.cpp` with the assigned
sensor driver while preserving the telemetry envelope.

Run native configuration tests:

```bash
/Users/phuongl/Library/Python/3.9/bin/pio test -d firmware/esp32 -e native
```

Build the ESP32 firmware:

```bash
/Users/phuongl/Library/Python/3.9/bin/pio run -d firmware/esp32 -e esp32dev
```

The firmware uses ESP-IDF `esp-mqtt` with broker CA validation and publishes
telemetry at MQTT QoS 1. It waits for NTP time synchronization before connecting
so broker certificate validation can succeed.

## RPC

After MQTT connects, the generic firmware requests both QoS 1 filters:

```text
v1/devices/me/rpc/request/+
v1/gateways/me/rpc/request/+
```

The transport grants only the filter valid for the token session: direct
devices receive the first route and gateway devices receive the second. This
keeps provisioning token-only without storing an internal device ID or role in
firmware.
Each request must be a JSON object with a UUIDv7 `id`, an ASCII `method`, an
object `params`, and UTC RFC3339 `issued_at` and `expires_at` fields:

```json
{
  "id": "018f6da9-1234-7abc-8def-0123456789ab",
  "method": "sample_now",
  "params": {},
  "issued_at": "2026-09-07T00:00:00Z",
  "expires_at": "2026-09-07T00:01:00Z",
  "mode": "two_way"
}
```

`mode` is optional and defaults to `one_way`. One-way commands finish at MQTT
publication acknowledgement, not device execution. For a two-way command, the
firmware publishes a QoS 1 JSON response after handling it:

```text
v1/devices/me/rpc/response/{id}
v1/gateways/me/rpc/response/{id}
```

`sample_now` responds with `{"ok":true,"result":{"sampled":true}}`. A two-way
`reboot` is deliberately rejected with
`{"ok":false,"error":"two_way_reboot_unsupported"}` and does not restart the
device. Use one-way `reboot` when restart is intended.

Expired, malformed, unsupported, and duplicate requests are ignored.
`sample_now` immediately publishes one telemetry sample. `reboot` records the
last command ID in NVS, then restarts through a dedicated reboot function.

Gateway child commands use the gateway route with `method` set to
`gateway_child_rpc` and this envelope in `params`:

```json
{
  "child_device_id": "UUIDv7",
  "method": "sample_now",
  "params": {}
}
```

The generic ESP sample intentionally ignores `gateway_child_rpc`; a deployed
Modbus, BLE, RS485, or GPIO gateway must consume that envelope and dispatch it
to the assigned child protocol. Such an adapter is also responsible for
publishing any two-way child result on the gateway response topic.
