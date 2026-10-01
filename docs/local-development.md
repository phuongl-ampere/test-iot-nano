# Local Development: Monolith, PowerMonitor, and Demo Data

This guide starts a disposable local IoT Nano platform, creates the
PowerMonitor demo fixture, and runs the PowerMonitor Next.js application. It
uses the current single-process `iot-nano-monolith` topology; it does not run
API, Core, Stream, or MQTTD as separate services.

## What This Starts

`seed-local-platform.sh --reset` bootstraps and starts a local monolith, then
loads the PowerMonitor application and demo resources. It does **not** start
the PowerMonitor development server; start that in a second terminal.

Unless overridden, the local runtime uses these loopback listeners:

| Purpose | Address |
| --- | --- |
| Public HTTP (`/healthz`, `/readyz`) | `http://127.0.0.1:18080` |
| Management API | `http://127.0.0.1:18081` |
| MQTT TCP | `127.0.0.1:18883` |
| MQTT TLS | `127.0.0.1:18884` |
| PowerMonitor | `http://localhost:3002` |

## Prerequisites

Install a Rust toolchain with Cargo, Node.js with npm, and these command-line
tools: `curl`, `jq`, `lsof`, and OpenSSL. The local reset helper invokes all
of them. Run all commands below from the repository root unless a step says
otherwise.

## 1. Create Local Runtime Material

The reset command retains three sensitive files in the local runtime root:
the device-token vault key, MQTT certificate, and MQTT private key. Create
them once for a new workspace. The default root is
`$XDG_CACHE_HOME/rush-iot-nano/local-platform`, or
`$HOME/.cache/rush-iot-nano/local-platform` when `XDG_CACHE_HOME` is unset.

The following block refuses to overwrite existing material. If it stops
because files already exist, retain them; do not regenerate them during a
normal reset.

```bash
local_platform_root="${XDG_CACHE_HOME:-$HOME/.cache}/rush-iot-nano/local-platform"
install -d -m 700 "$local_platform_root"

for local_platform_file in vault.key mqtt-cert.pem mqtt-key.pem; do
  if [ -e "$local_platform_root/$local_platform_file" ]; then
    printf 'Local runtime material already exists: %s\n' \
      "$local_platform_root/$local_platform_file" >&2
    exit 1
  fi
done

umask 077
openssl rand -base64 48 | tr -d '\n' >"$local_platform_root/vault.key"
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj '/CN=localhost' \
  -keyout "$local_platform_root/mqtt-key.pem" \
  -out "$local_platform_root/mqtt-cert.pem"
chmod 600 "$local_platform_root/vault.key" "$local_platform_root/mqtt-key.pem"
chmod 644 "$local_platform_root/mqtt-cert.pem"
```

These files are local secrets. Never commit or paste their contents into
configuration, issues, or logs.

## 2. Configure Demo Seed Credentials

Create the ignored local seed file without overwriting an existing one:

```bash
if [ -e infra/monolith/local-platform-seed.env ]; then
  printf '%s\n' 'Seed file already exists; edit it in place.'
else
  cp infra/monolith/local-platform-seed.env.example \
    infra/monolith/local-platform-seed.env
fi
```

Edit `infra/monolith/local-platform-seed.env` and replace every placeholder.
It contains the required system-bootstrap, tenant-account, owner, controller,
viewer, and unassigned-user credentials. The system and tenant credentials
operate the seed; sign in to PowerMonitor with one of the four demo-user
accounts.

## 3. Configure and Install PowerMonitor

Create `apps/powermonitor/.env.local` from the tracked template without
overwriting an existing file:

```bash
if [ -e apps/powermonitor/.env.local ]; then
  printf '%s\n' 'PowerMonitor environment already exists; edit it in place.'
else
  cp apps/powermonitor/.env.example apps/powermonitor/.env.local
fi
```

Set these values for the local monolith:

```dotenv
PLATFORM_BASE_URL=http://127.0.0.1:18080
PLATFORM_AUTH_BASE_URL=http://127.0.0.1:18080
IOT_NANO_HTTPS_ENABLED=false
OAUTH_CLIENT_ID=powermonitor-client
OAUTH_REDIRECT_URI=http://localhost:3002/api/auth/callback
OAUTH_SCOPE=devices:read devices:write assets:read assets:write telemetry:read alerts:read alerts:write commands:read commands:write authorization:read authorization:write
SESSION_SECRET=replace-with-at-least-32-random-bytes
```

`IOT_NANO_HTTPS_ENABLED` defaults to `false`: both the monolith console and
PowerMonitor therefore issue HTTP-compatible `HttpOnly` cookies for a LAN.
For HTTPS behind a reverse proxy, set it to `true` in both the monolith
environment and PowerMonitor environment, and change all three URLs above to
their `https://` origins. The variable controls cookie security; the monolith
does not itself terminate web TLS.

Generate a unique `SESSION_SECRET`, for example with `openssl rand -base64
48`. Delete the `OAUTH_CLIENT_SECRET` line entirely: the local seed registers
`powermonitor-client` as a public client. An empty value is still treated as a
configured client secret by the application.
The current local fixture does not create credentials for
`OAUTH_SERVICE_CLIENT_ID=powermonitor-service`; do not use the template's
placeholder service-client values as live credentials.

Install the application's locked dependencies:

```bash
cd apps/powermonitor
npm ci
cd ../..
```

## 4. Reset and Seed the Local Monolith

Run the guarded reset command:

```bash
IOT_NANO_ALLOW_LOCAL_SEED=1 ./scripts/dev/seed-local-platform.sh --reset
```

It stops only a verified monolith for this workspace, removes its disposable
platform SQLite database, uploaded OTA firmware files, and internal stream,
MQTTD, and cache state,
bootstraps the system account, starts a fresh monolith, and creates the
PowerMonitor fixture. It records the PowerMonitor launch URL and callback as
`http://localhost:3002`; keep that port when starting the UI below.

## 5. Start PowerMonitor

In another terminal, run:

```bash
cd apps/powermonitor
npm run dev -- --port 3002
```

The server remains in the foreground. Visit `http://localhost:3002` after it
reports that it is ready.

## 6. Verify and Sign In

From the repository root, verify the monolith and PowerMonitor:

```bash
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
curl --fail http://localhost:3002/
```

Sign in at `http://localhost:3002` with a demo user from your local seed file:

| User | Seed variable | Expected access |
| --- | --- | --- |
| Owner | `IOT_NANO_SEED_OWNER_USERNAME` | Owns and manages every seeded resource. |
| Controller | `IOT_NANO_SEED_CONTROLLER_USERNAME` | `control` access to one zone and one device. |
| Viewer | `IOT_NANO_SEED_VIEWER_USERNAME` | `view` access to a different zone and device. |
| Unassigned | `IOT_NANO_SEED_UNASSIGNED_USERNAME` | Can authenticate but sees no seeded resource. |

The seed creates two farms, two zones per farm, and two devices per zone.

## Reset Scope and Troubleshooting

Each reset invalidates PowerMonitor browser sessions because it recreates the
platform database. It preserves `vault.key`, `mqtt-cert.pem`, and
`mqtt-key.pem` in the local runtime root, as well as the source tree,
PowerMonitor environment file, and Cargo lane cache.

If reset reports missing local runtime material, complete step 1. If it
refuses to stop a listener, do not force-kill the process: the guard is
protecting a process or runtime that cannot be proven to belong to this
workspace. Resolve the listener conflict first. By default, the monolith log
is at `$XDG_CACHE_HOME/rush-iot-nano/local-platform/monolith.log`, or
`$HOME/.cache/rush-iot-nano/local-platform/monolith.log` when
`XDG_CACHE_HOME` is unset.

## Production Boundary

This guide is only for disposable development state. For production setup,
secrets, backups, upgrades, and rollback, use
[Monolith Operations](operations-monolith.md).
