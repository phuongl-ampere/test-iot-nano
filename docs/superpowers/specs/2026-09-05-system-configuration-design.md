# System Configuration Design

**Date:** 2026-09-05

## Goal

Provide an admin-only System Configuration view for SMTP credentials and safe
`iot-ingest` operational tuning. A saved change applies only after an explicit
`iot-ingest` restart.

## Architecture

The configuration remains in `/etc/rush-iot-nano/ingest.env`, not TimescaleDB.
This keeps SMTP credentials out of the database and retains the current
deployment model. A root-owned `iot-admin-helper` binary is the only process
allowed to read or atomically update that file.

The API runs as `iot` and invokes the helper through a narrow sudoers rule. It
has only fixed `read` and `apply` commands. The helper never
returns `SMTP_PASSWORD`; reads expose only `password_configured`. It has a
fixed config path.

`apply` saves configuration without restarting. The operator restarts
`iot-ingest.service` manually. API writes cannot supply a shell command, file
path, or unit name.

## Editable Settings

- SMTP enablement, host, port, username, password, sender, recipient, and
  send timeout. Password is write-only; blank preserves the existing password;
  disabling SMTP removes all SMTP values.
- Stream retention bytes/seconds, segment bytes, and max record bytes.
- Writer, alert evaluator, and notification batch sizes.
- Writer flush, alert event/window, notification dispatch, and retention
  intervals.
- Notification lease duration and retry base/max backoff.

`iot-ingest` gains environment inputs for these fields and reads them on
startup.

## Locked Settings

The System Configuration response and update path omit:

- `DATABASE_URL`
- MQTT host, port, and credentials
- stream directory and partition count
- writer/alert group identity and member identity
- health bind address

They are deployment topology or connection values, not dashboard settings.

## API And UI

All routes are admin-only:

```text
GET  /api/system-configuration
PUT  /api/system-configuration
```

The dashboard exposes a dedicated System Configuration view only to admins.
It has SMTP, stream, and worker-tuning sections, a save action which indicates
a restart is required. `401` returns to login.

## Installation And Verification

The Raspberry Pi installer creates a root-owned helper at
`/usr/local/sbin/iot-admin-helper` and a sudoers rule allowing user `iot` to
run only its `read` and `apply` commands without a password. Config file writes are atomic and
secret values have restrictive file permissions.

Tests cover redaction, SMTP password retention/clearing, config validation,
allowlist preservation, API roles, UI roles, and the restart-required state.
Full workspace, web, failure, e2e, and stress verification remain required.
