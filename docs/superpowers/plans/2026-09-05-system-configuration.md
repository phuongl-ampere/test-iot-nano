# System Configuration Implementation Plan

**Goal:** Add an admin-only System Configuration view for SMTP credentials and
safe `iot-ingest` operational tuning, saved through a root-owned helper and
applied by a manual service restart.

## Tasks

1. Add shared, serializable SMTP and ingest tuning models in `iot-core`.
   They validate numeric limits, redact SMTP password on reads, preserve a
   configured password when the update omits it, and expose no connection
   string or MQTT fields.
2. Add `iot-admin-helper`, a root-owned binary with fixed `read` and `apply`
   commands. It uses only `/etc/rush-iot-nano/ingest.env`, writes config
   atomically, and permits only allowed SMTP/tuning keys.
3. Extend `iot-ingest` startup configuration for batch sizes, intervals,
   lease duration, and retry limits so a restart applies the saved values.
4. Add admin-only API endpoints for read/save. The API invokes the fixed
   helper through `sudo -n`, never returns SMTP password, and does not
   allow arbitrary paths, units, database settings, or MQTT settings.
5. Add a dedicated admin System Configuration view in the dashboard, with
   SMTP, stream, and worker tuning sections plus a Save action and
   restart-required state. Viewers do not see it.
6. Install the helper and narrow sudoers rule from the Raspberry Pi installer,
   document the operation, then run Rust/web/e2e/stress verification.

## Test Order

- Write failing shared-config tests, then models.
- Write failing helper and API tests, then helper/client/routes.
- Write failing dashboard tests, then UI.
- Run formatter, workspace/failure suites, web test/build, local e2e, and
  stress test after the implementation is complete.
