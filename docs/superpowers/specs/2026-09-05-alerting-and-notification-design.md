# Alerting And Notification Design

**Date:** 2026-09-05

## Goal

Add simple, durable alerting without delaying MQTT ingestion. Operators create
and enable rules through the existing API and dashboard. Rules create
incidents visible in the dashboard and email notifications through SMTP.

## Scope

- Event threshold rules, such as `temperature_c > 40`.
- Window average rules, such as `avg(temperature_c, 5m) > 40`.
- Rule scope per device or all devices.
- Incidents: `pending`, `open`, and `resolved`.
- API/dashboard: list/create rules, toggle a rule, list incidents, and
  acknowledge an incident.
- SMTP email on open, resolved, and a configured reminder while open.
- Stress coverage for stream fan-out, evaluation, DB writes, and outbox.

V1 excludes no-data/offline rules, arbitrary expressions, rule edit/delete,
webhooks, Slack, and user authentication.

## Runtime Architecture

`iot-stream` stays an in-process Rust library. `iot-ingest` starts separate
Tokio tasks:

```text
MQTT ingress
  -> telemetry stream append + fsync
  -> MQTT ACK

timescaledb-writer consumer group
  -> telemetry and device tables

alert-evaluator consumer group
  -> alert incidents + notification outbox

window-rule scheduler
  -> TimescaleDB telemetry query
  -> alert incidents + notification outbox

notification dispatcher
  -> SMTP email

retention and metrics tasks
```

`timescaledb-writer` and `alert-evaluator` have independent offsets. A slow
or retrying alert evaluator creates only alert-group lag; it does not delay
telemetry persistence or MQTT acknowledgement.

The MQTT task uses `spawn_blocking` for stream append. It sends the QoS 1 ACK
only after append and `sync_data` complete. File retention runs in a separate
blocking task. SMTP, DB evaluation, and email retries never run in ingress.

## Database Model

Migration `0002_alerting.sql` creates:

### `alert_rules`

```text
id UUID primary key
name text
enabled boolean
device_id nullable text
metric_key text
rule_type event_threshold | window_average
comparison gt | gte | lt | lte
threshold double precision
window_seconds nullable integer
for_seconds integer default 300
resolve_after_seconds integer default 300
reopen_grace_seconds integer default 3600
hysteresis nullable double precision
severity info | warning | critical
reminder_interval_seconds integer default 86400
created_at, updated_at
```

`device_id = NULL` applies to every device. Event rules have no window.
Window rules require `window_seconds >= 60`.

### `alert_incidents`

```text
id UUID primary key
rule_id UUID
device_id text
status pending | open | resolved
condition_started_at
recovery_started_at nullable
opened_at nullable
resolved_at nullable
acknowledged_at nullable
acknowledged_by nullable text
last_value nullable double precision
last_notified_at nullable
last_reminder_at nullable
state_version integer
created_at, updated_at
```

At most one `pending` or `open` incident exists for `(rule_id, device_id)`.
Resolved incidents remain history. A breach during `reopen_grace_seconds`
reuses the latest resolved incident; later breaches create a new one.

### `notification_outbox`

```text
id UUID primary key
incident_id UUID
kind opened | resolved | reminder
dedupe_key text unique
subject text
body text
state pending | leased | sent
next_attempt_at
lease_until nullable
attempt_count
last_error nullable
sent_at nullable
created_at
```

The outbox is the durable queue for SMTP side effects. The dispatcher claims
due rows using `FOR UPDATE SKIP LOCKED`, leases for 30 seconds, and retries
with exponential backoff capped at one hour.

## Rule Evaluation

The `alert-evaluator` group polls records in batches. It loads enabled event
rules once per batch, evaluates only matching device scopes, and ignores a
rule when the metric is absent or nonnumeric. It commits the stream offset
only after its transaction commits incident and outbox updates.

The window scheduler runs every 60 seconds. It loads enabled window rules and
queries `AVG((measurements ->> metric_key)::double precision)` for each
matching device over `window_seconds`. No returned telemetry rows means no
evaluation; no-data detection is out of scope.

## Incident State Machine

```text
inactive -> pending -> open -> resolved
                    ^              |
                    +--- reopen ---+
```

- A breach starts or maintains `pending`.
- After continuous breach for `for_seconds`, the incident opens.
- Critical rules can set `for_seconds = 0`.
- A normal observation begins recovery. After `resolve_after_seconds`, an
  open incident resolves.
- Hysteresis opens at the configured threshold and requires the opposite
  threshold plus/minus `hysteresis` to recover.
- Acknowledging an open incident suppresses reminder email until its next
  state transition. It does not resolve the incident.

Each `open`, `resolved`, or reminder transition inserts an idempotent outbox
row in the same transaction. Reminder dedupe is derived from incident ID,
state version, and the rule's reminder time bucket.

## SMTP

SMTP is optional. If no SMTP variables are set, `iot-ingest` starts without a
dispatcher and outbox rows remain pending for later delivery. A partial SMTP
configuration fails startup.

```text
SMTP_HOST
SMTP_PORT=465
SMTP_USERNAME
SMTP_PASSWORD
ALERT_EMAIL_FROM
ALERT_EMAIL_TO
SMTP_TIMEOUT_SECONDS=15
```

The implementation uses `lettre` async SMTP over implicit TLS. Credentials
are never stored in the database, source code, dashboard, or logs.

## API And Dashboard

```text
GET  /api/alert-rules
POST /api/alert-rules
POST /api/alert-rules/{id}/toggle
GET  /api/alert-incidents
POST /api/alert-incidents/{id}/acknowledge
```

Creation validates identifiers, metric key, comparison, thresholds, duration
defaults, window requirements, and severity. The dashboard adds an alert
section with a compact creation form, rule table/toggle, and incident table
with acknowledge.

## Verification And Stress

- Unit tests cover comparison, hysteresis, pending/open/resolved/reopen,
  outbox dedupe, lease/retry, and SMTP configuration.
- Integration tests cover event and window evaluators, independent stream
  offsets, API routes, and dashboard interactions.
- Stress test starts the local stack and sends 10,000 messages across 100
  devices. It requires the telemetry writer and alert evaluator to drain all
  records, produce expected incidents, preserve no duplicate notifications,
  and print elapsed time and messages per second.
- The stress test has a 120-second watchdog. It measures throughput but does
  not impose a hardware-specific minimum rate.
