# PowerMonitor Fresh Local Seed Design

## Goal

Make `seed-local-platform.sh --reset` recreate the complete local PowerMonitor
environment from empty SQLite state and leave a fresh monolith running with a
deterministic fixture.

## Scope

`--reset` owns the local monolith bound to the configured local listeners, the
local platform database, and the monolith internal state directory. It finds a
running instance through its PID file or its configured HTTP listener, then
stops it only after proving that its executable and open SQLite paths belong
to this workspace's local runtime. It never finds processes by a broad name
match or touches a different workspace's runtime.

The reset removes the local `platform.sqlite` database and the internal
`stream.sqlite`, `mqttd.sqlite`, `cache.sqlite`, and lock state. It retains
the local TLS certificate, TLS private key, device-token vault key,
PowerMonitor `.env.local`, source tree, Cargo lane cache, and all unrelated
services and databases.

The local seed configuration remains untracked at
`infra/monolith/local-platform-seed.env`. It supplies system-bootstrap,
demo-tenant, and four demo-user credentials. The PowerMonitor server remains
running; its existing browser sessions become invalid because the platform
database is recreated.

## Reset Lifecycle

1. The command requires `--reset`, `IOT_NANO_ALLOW_LOCAL_SEED=1`, the local
   seed file, and loopback addresses.
2. It resolves the current workspace's local runtime directory, binary, PID
   file, log file, and listeners. It obtains a running PID from the PID file
   or configured HTTP listener; before sending a signal, it verifies that the
   PID belongs to the expected monolith binary and has the workspace runtime's
   SQLite paths open. No running instance is also valid.
3. It asks the verified monolith to stop and waits for the process and its
   HTTP/MQTT listeners to disappear. A timeout is a failure; the script does
   not delete database files while the process holds them.
4. It clears only the platform database and internal-state paths listed in
   the scope. TLS and vault material remain in place.
5. It performs the one-shot system bootstrap using the configured system
   credentials while no monolith owns the SQLite database.
6. It starts the current workspace's monolith binary with the local runtime
   configuration, records its PID, and waits for `/healthz` and `/readyz`.
7. It creates the tenant, PowerMonitor application, resource hierarchy,
   devices, ownership, and permissions through existing management APIs.
8. It verifies the fixture and prints the four user cases. The monolith
   remains running when the command exits.

No application API is introduced. The previous tenant-deletion endpoint is
not required because resetting the local database produces the clean
environment directly.

## Demo Fixture

The recreated tenant contains the `powermonitor` application, two farms, two
zones per farm, and two devices per zone. The seed deliberately does not
write the retired application-scoped profile endpoints; tenant profile
configuration is managed independently.

| User case | Expected access |
| --- | --- |
| Owner | Owns every seeded asset and device; sees and manages all demo resources. |
| Controller | Receives `control` access to a selected asset and selected device only. |
| Viewer | Receives `view` access to a different selected asset and device only. |
| Unassigned | Can authenticate but sees no seeded asset or device. |

The controller and viewer assignments use disjoint resources. This makes the
PowerMonitor access boundary visible without ambiguity. The tenant account is
only the seed operator and is not presented as a PowerMonitor demo user.

## Failure Handling

The script fails before mutation when the acknowledgement, seed file, local
runtime identity, or loopback validation is missing. It fails rather than
deleting data if the monolith does not stop within its deadline. A failure
after clearing state leaves the local runtime intentionally empty; rerunning
`--reset` recreates it from the beginning. A failure after startup leaves the
new monolith running for diagnosis and never removes TLS or vault material.

## Verification

Automated coverage will prove that a second `--reset` removes a sentinel
resource and stale user from the first run, then recreates exactly one demo
tenant and the four user cases. It will verify the owner, controller, viewer,
and unassigned visibility/permission matrix, preserve TLS/vault files, and
reject deletion while an unverified process occupies a configured listener.
The integration smoke check will confirm that the restarted monolith is ready
and PowerMonitor remains reachable after the reset.
