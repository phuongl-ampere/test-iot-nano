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
