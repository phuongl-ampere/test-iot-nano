# IoT Nano Monolith Operations

This guide covers a **fresh monolith deployment only**. It does not convert a
four-service installation and does not authorize reuse of state from
`iot-nano-api`, `iot-nano-core`, `iot-nano-stream`, or `iot-nano-mqttd`.

Unsupported:

```text
Stop the four services and point iot-nano-monolith at their existing database
or state directories.
```

The monolith has different ownership, schemas, and internal state boundaries.
An existing four-service installation requires a separately approved,
offline export/import design. Do not run both topologies against shared state
or shared listener ports.

## Topology And Bindings

Deploy one `iot-nano-monolith` binary or application container. It owns the
public HTTP API, MQTT TCP, MQTT TLS, stream processing, Core workers, cache,
and command delivery in one process.

The default listeners are:

| Purpose | Setting | Default |
| --- | --- | --- |
| Public HTTP and health | `IOT_NANO_PUBLIC_HTTP_ADDRESS` | `0.0.0.0:8080` |
| Management and administration | `IOT_NANO_MANAGEMENT_ADDRESS` | `127.0.0.1:8081` |
| MQTT TCP | `IOT_NANO_MQTT_TCP_ADDRESS` | `0.0.0.0:1883` |
| MQTT TLS | `IOT_NANO_MQTT_TLS_ADDRESS` | `0.0.0.0:8883` |

Only the HTTP and MQTT listeners are public. Keep management on loopback or a
separately protected operator network. Do not publish or firewall-open port
8081 to untrusted networks. Use an authenticated SSH tunnel or an equivalent
protected access path for remote administration.

`/healthz` and `/readyz` are served by the public HTTP listener. Readiness is
reported only after storage migration, internal SQLite state, workers, and
listeners are ready.

## Storage, Volumes, And Secrets

Create empty, monolith-owned locations. Never mount a former service's
database or internal state into these paths.

SQLite platform mode requires:

```text
IOT_NANO_STORAGE=sqlite
IOT_NANO_SQLITE_PATH=/var/lib/iot-nano/platform/platform.sqlite
IOT_NANO_INTERNAL_DIR=/var/lib/iot-nano/internal
```

The platform volume contains platform data. The internal directory is separate
and contains exactly these monolith-owned files:

```text
stream.sqlite
mqttd.sqlite
cache.sqlite
instance.lock
```

Timescale mode requires `IOT_NANO_STORAGE=timescale` and `DATABASE_URL`. It
uses only the internal-state volume locally; do not set
`IOT_NANO_SQLITE_PATH` in this mode. SQLite mode must not receive
`DATABASE_URL`. The process rejects contradictory or retired service
environment variables before binding listeners.

Protect the environment file and token vault key:

```bash
install -d -o root -g root -m 0755 /etc/iot-nano
install -d -o iotnano -g iotnano -m 0700 /var/lib/iot-nano/platform
install -d -o iotnano -g iotnano -m 0700 /var/lib/iot-nano/internal
install -o iotnano -g iotnano -m 0600 /dev/null /etc/iot-nano/monolith.env
```

Set `IOT_DEVICE_TOKEN_VAULT_KEY` to a randomly generated ASCII value of at
least 32 non-whitespace characters. Keep the TLS certificate and private key
readable by `iotnano` and inaccessible to other users. Do not put
`DATABASE_URL`, vault keys, bootstrap passwords, or private keys in command
arguments, source control, or operational logs.

## Fresh Deployment

Install the pinned release binary at
`/opt/rush-iot-nano/iot-nano-monolith`, install the unit at
`/etc/systemd/system/iot-nano-monolith.service`, and create the environment
file from
[`infra/monolith/monolith.env.example`](../infra/monolith/monolith.env.example).
Provision the TLS files before starting. Run the following as the service
user with the environment file loaded:

```bash
set -a
. /etc/iot-nano/monolith.env
set +a
/opt/rush-iot-nano/iot-nano-monolith --config-check
/opt/rush-iot-nano/iot-nano-monolith --migrate-only
```

`--config-check` validates configuration without opening a socket or database.
`--migrate-only` runs the selected platform migrations and exits without
starting a listener. Do not continue if either command fails.

Start only after both checks succeed:

```bash
systemctl daemon-reload
systemctl enable --now iot-nano-monolith.service
curl --fail http://127.0.0.1:8080/readyz
systemctl status iot-nano-monolith.service
journalctl -u iot-nano-monolith.service --since "5 minutes ago"
```

The first administrative user is a one-time operation. Load the bootstrap
credentials only for that invocation, run `--bootstrap-admin`, remove those
variables, and restart the normal service. Do not leave bootstrap credentials
in the persistent environment file.

## Monolith-Only Upgrades And Rollback

This section applies only after a monolith has already been deployed. It is
not a path for importing four-service state.

Before an upgrade, stop the single monolith instance and create verified,
timestamped backups of the complete platform volume and the complete internal
state directory. For SQLite, stop the process before copying files so WAL
state is included. For TimescaleDB, record a tested restore point or backup.
Keep the previous binary and environment file available.

The safe upgrade order is:

```text
stop -> backup platform and internal state -> install new binary
-> --config-check -> --migrate-only -> start -> /readyz verification
```

If the new monolith fails before readiness:

```bash
systemctl stop iot-nano-monolith.service
# Restore the matching pre-migration platform and internal-state backups.
# Install the previous monolith binary and its matching environment file.
systemctl start iot-nano-monolith.service
curl --fail http://127.0.0.1:8080/readyz
```

Do not resume traffic against a partial migration. A rollback requires a
matching pre-migration backup or a verified Timescale restore point; it is not
the deletion of selected SQLite files. Restore the platform and internal
state together so stream offsets, MQTT sessions, cache checkpoints, and
platform records remain consistent.

## Service Hardening

The supplied unit runs as `iotnano:iotnano`, uses `UMask=0077`, creates an
owner-only `StateDirectory`, and restricts filesystem, device, namespace,
kernel, syscall, and network-family access. It grants no
`CAP_NET_BIND_SERVICE` capability for the default unprivileged ports. If a
deployment selects a public HTTP or MQTT port below 1024, enable only the
documented capability lines in the unit and keep management on a protected
address.

Review service logs after startup, but do not expect secrets there. A failed
configuration or migration must prevent public listener binding and must be
fixed before retrying.
