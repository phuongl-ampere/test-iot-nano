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
and contains exactly these monolith-owned state entries:

```text
stream.sqlite
mqttd.sqlite
cache.sqlite
instance.lock
```

The process requires owner-only regular files and rejects undeclared entries,
unsafe permissions, and symbolic links before it migrates platform storage or
binds a listener. SQLite may create transient `-wal` and `-shm` journal
sidecars while a database is open; they are part of the corresponding SQLite
file, not additional monolith state entries. A valid legacy
`.iot-nano-monolith-state` marker from an earlier build is removed during the
first safe restart. Do not create or restore that marker.

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

Use `infra/monolith/migrate.sh` for Compose-managed upgrades. It stops the
monolith before migration, then creates and verifies an owner-only,
timestamped paired backup directory. In SQLite mode, each directory contains
`platform-state.tar.gz`, `internal-state.tar.gz`, and a matching manifest.
The archives cover the complete platform volume and the complete internal
state directory, including the ownership marker, three SQLite databases, and
instance lock. Keep the previous binary and environment file available.

Set an owner-only destination outside the source checkout in production:

```bash
IOT_NANO_MONOLITH_BACKUP_DIR=/var/backups/iot-nano/monolith \
  infra/monolith/migrate.sh
```

For TimescaleDB, provide an executable `TIMESCALE_BACKUP_COMMAND`. The script
passes it one destination path inside the paired backup directory; it must
create a nonempty restore-point file or directory there. The script archives
and verifies internal state only after the restore point exists. The callback
output is suppressed so it cannot disclose connection data.

The safe upgrade order is:

```text
stop -> backup platform and internal state -> install new binary
-> --config-check -> --migrate-only -> start -> /readyz verification
```

If the new monolith fails before readiness:

```bash
ROLLBACK_BACKUP_DIR=/var/backups/iot-nano/monolith/<timestamp-and-pid> \
  infra/monolith/rollback.sh
```

`rollback.sh` refuses individual SQLite files and requires the matched
platform plus internal-state archives from one manifest before it stops the
service. It verifies archive safety, snapshots the complete current platform
and internal state, then stages both target archives before moving either live
state. A target-side I/O failure triggers restoration of both current states
and leaves the monolith stopped. If that compensation fails, the command
reports the exact generated `rollback-current-platform-state-*.tar.gz` and
`rollback-current-internal-state-*.tar.gz` archives for manual recovery and
does not restart.

For a Timescale rollback, the operator must provide both sides of a
compensation contract. `TIMESCALE_RESTORE_COMMAND` restores the target
restore point from the paired backup. Before running rollback, create a
separate, nonempty `TIMESCALE_CURRENT_RESTORE_POINT` that represents the
currently deployed database after writes have stopped. Set
`TIMESCALE_RESTORE_ROOT` to its existing, owner-only, non-symlinked parent
directory; the current restore point must be inside that root. Both restore
points are validated without symlinked path components or directory contents.
They must have different device-and-inode identities, so a hardlink to the
target restore point is rejected. `TIMESCALE_COMPENSATE_COMMAND` must accept
that current restore-point path as its sole argument and restore it exactly.

```bash
IOT_NANO_TIMESCALE_COMPOSE=1 \
  ROLLBACK_BACKUP_DIR=/var/backups/iot-nano/monolith/<timestamp-and-pid> \
  TIMESCALE_RESTORE_COMMAND=/usr/local/libexec/iot-nano/restore-timescale \
  TIMESCALE_RESTORE_ROOT=/var/backups/iot-nano/timescale-restore-points \
  TIMESCALE_CURRENT_RESTORE_POINT=/var/backups/iot-nano/timescale-restore-points/current \
  TIMESCALE_COMPENSATE_COMMAND=/usr/local/libexec/iot-nano/restore-timescale \
  infra/monolith/rollback.sh
```

The script stages and validates the target internal state and creates a
recoverable snapshot of the current internal state before either live state is
replaced. If target restore or the internal swap fails, it invokes the
compensation command with the current restore point and restores the internal
snapshot. It never restarts after any failed restore. If compensation fails,
the command reports the exact database restore point and generated
`rollback-current-internal-state-*.tar.gz` archive required for manual
recovery; keep the service stopped until both are restored.

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
