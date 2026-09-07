# Alerting And Notification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add durable alert rules, dashboard incidents, and SMTP email without
allowing alert work to delay MQTT ingestion.

**Architecture:** `iot-ingest` runs independent Tokio tasks. The existing
`timescaledb-writer` group persists telemetry. A second `alert-evaluator`
group evaluates event rules and commits its own offset after a transaction
writes incidents/outbox rows. A scheduler evaluates window rules from
TimescaleDB, while the notification dispatcher leases SMTP outbox rows.

**Tech Stack:** Rust 1.96, Tokio, SQLx/PostgreSQL/TimescaleDB, lettre, Axum,
Next.js 16, React, Vitest.

## Global Constraints

- Keep `iot-stream` in-process and do not add a broker or dynamic topic model.
- MQTT ACK remains after durable stream append only.
- Event rule stream offset commits only after incident/outbox transaction.
- SMTP runs only from a leased PostgreSQL outbox row and never in ingress.
- Defaults: `for_seconds=300`, `resolve_after_seconds=300`,
  `reopen_grace_seconds=3600`, `reminder_interval_seconds=86400`.
- SMTP uses env variables only, never stored credentials.
- Stress test sends 10,000 events across 100 devices with a 120-second
  watchdog and reports measured throughput without hardware-specific SLA.

---

## File Structure

```text
db/migrations/0002_alerting.sql
crates/iot-ingest/src/alert.rs
crates/iot-ingest/src/notification.rs
crates/iot-ingest/src/main.rs
crates/iot-ingest/src/mqtt.rs
crates/iot-ingest/src/metrics.rs
crates/iot-ingest/src/writer.rs
crates/iot-ingest/tests/alert.rs
crates/iot-ingest/tests/notification.rs
crates/iot-ingest/tests/stress.rs
crates/iot-api/src/routes.rs
crates/iot-api/tests/api.rs
web/lib/api.ts
web/components/alert-panel.tsx
web/components/dashboard.tsx
web/app/globals.css
web/tests/alert-panel.test.tsx
scripts/stress-local.sh
```

### Task 1: Persist Rules, Incidents, and Email Outbox

**Files:**
- Create: `db/migrations/0002_alerting.sql`
- Modify: `crates/iot-ingest/src/writer.rs`
- Modify: `crates/iot-ingest/tests/writer.rs`

**Interfaces:**
- `migrate(&PgPool)` executes both `0001_telemetry.sql` and
  `0002_alerting.sql`.
- Tables: `alert_rules`, `alert_incidents`, `notification_outbox`.

- [x] **Step 1: Write a failing schema test**

```rust
#[tokio::test]
async fn migration_installs_alert_rules_incidents_and_outbox() {
    let pool = prepared_pool().await;
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name IN
         ('alert_rules', 'alert_incidents', 'notification_outbox')
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(tables.len(), 3);
}
```

- [x] **Step 2: Run the test and verify RED**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test writer migration_installs_alert_rules_incidents_and_outbox -- --test-threads=1`

Expected: fail because the three tables do not exist.

- [x] **Step 3: Add `0002_alerting.sql`**

Create rule fields:

```text
id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
hysteresis, severity, reminder_interval_seconds, created_at, updated_at
```

Create incident fields:

```text
id, rule_id, device_id, status, condition_started_at, recovery_started_at,
opened_at, resolved_at, acknowledged_at, acknowledged_by, last_value,
last_notified_at, last_reminder_at, state_version, created_at, updated_at
```

Create outbox fields:

```text
id, incident_id, kind, dedupe_key, subject, body, state, next_attempt_at,
lease_until, attempt_count, last_error, sent_at, created_at
```

Add checks for rule/status/kind/state values, a partial unique index for active
`(rule_id, device_id)` incidents, outbox `dedupe_key` uniqueness, and due-row
index `(state, next_attempt_at)`.

- [x] **Step 4: Apply ordered migrations**

```rust
const MIGRATIONS: &[&str] = &[
    include_str!("../../../db/migrations/0001_telemetry.sql"),
    include_str!("../../../db/migrations/0002_alerting.sql"),
];

pub async fn migrate(pool: &PgPool) -> Result<(), WriterError> {
    for migration in MIGRATIONS {
        sqlx::raw_sql(migration).execute(pool).await?;
    }
    Ok(())
}
```

- [x] **Step 5: Verify GREEN**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test writer -- --test-threads=1`

Expected: alert schema and current migration tests pass.

### Task 2: Implement Event Rules and Incidents

**Files:**
- Create: `crates/iot-ingest/src/alert.rs`
- Modify: `crates/iot-ingest/src/lib.rs`
- Create: `crates/iot-ingest/tests/alert.rs`

**Interfaces:**
- `AlertEvaluator::new(PgPool, usize)`.
- `AlertEvaluator::flush_event_rules(&mut StreamConsumer, DateTime<Utc>)`.
- `AlertEvaluator::flush_window_rules(DateTime<Utc>)`.
- `AlertRule`, `RuleKind`, `Comparison`, `Severity`, `IncidentStatus`,
  `AlertError`.

- [x] **Step 1: Write failing state-machine tests**

```rust
#[tokio::test]
async fn breach_opens_after_for_duration_and_enqueues_one_email() {
    let (pool, stream, mut consumer) = prepared_alert_runtime().await;
    insert_event_rule(&pool, "temperature_c", 40.0, 60).await;
    append_temperature(&stream, "esp-000123", 41.0, at(0));
    append_temperature(&stream, "esp-000123", 41.0, at(61));

    AlertEvaluator::new(pool.clone(), 100)
        .flush_event_rules(&mut consumer, at(61))
        .await
        .unwrap();

    assert_eq!(incident_status(&pool).await, "open");
    assert_eq!(outbox_count(&pool, "opened").await, 1);
}
```

- [x] **Step 2: Run and verify RED**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test alert breach_opens_after_for_duration_and_enqueues_one_email -- --test-threads=1`

Expected: fail because `AlertEvaluator` does not exist.

- [x] **Step 3: Implement comparison and lifecycle**

Implement `gt`, `gte`, `lt`, and `lte`; use hysteresis only for recovery.
Inside one DB transaction:

```text
new breach -> pending, or open when for_seconds=0
pending continuous breach -> open after for_seconds
open breach -> cancel recovery; enqueue due reminder unless acknowledged
open normal -> start recovery; resolve after resolve_after_seconds
pending normal -> delete pending state
recent resolved breach -> reuse inside reopen grace; otherwise create incident
```

On transition use idempotent outbox keys:

```text
incident:{id}:opened:{state_version}
incident:{id}:resolved:{state_version}
incident:{id}:reminder:{state_version}:{unix_reminder_bucket}
```

- [x] **Step 4: Commit only after transaction**

```rust
let batch = consumer.poll(self.batch_size, now)?;
let mut transaction = self.pool.begin().await?;
evaluate_event_batch(&mut transaction, &batch.records, now).await?;
transaction.commit().await?;
consumer.commit(batch, now)?;
```

- [x] **Step 5: Add and verify edge tests**

Cover fleet/device rule scope, disabled rules, missing metrics, hysteresis,
resolve, reopen grace, acknowledge suppressing reminders, and replay dedupe.

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test alert -- --test-threads=1`

Expected: all lifecycle assertions pass.

### Task 3: Add Window Rules and SMTP Outbox

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/iot-ingest/Cargo.toml`
- Modify: `crates/iot-ingest/src/alert.rs`
- Create: `crates/iot-ingest/src/notification.rs`
- Create: `crates/iot-ingest/tests/notification.rs`

**Interfaces:**
- `AlertEvaluator::flush_window_rules(DateTime<Utc>)`.
- `SmtpConfig::from_env() -> Result<Option<SmtpConfig>, NotificationError>`.
- `NotificationDispatcher::dispatch_once()`.
- `EmailSender` trait lets tests use a recording sender.

- [x] **Step 1: Write failing window and retry tests**

```rust
#[tokio::test]
async fn five_minute_average_opens_a_window_rule() {
    insert_window_rule(&pool, "temperature_c", 40.0, 300).await;
    insert_telemetry(&pool, "esp-000123", 41.0, now - Duration::minutes(1)).await;

    evaluator.flush_window_rules(now).await.unwrap();

    assert_eq!(incident_status(&pool).await, "open");
}

#[tokio::test]
async fn failed_email_returns_leased_outbox_row_to_pending_with_backoff() {
    let dispatcher = NotificationDispatcher::new(pool.clone(), RecordingSender::failing(), 10);
    seed_outbox(&pool).await;

    dispatcher.dispatch_once().await.unwrap();

    assert_eq!(outbox_state(&pool).await, "pending");
    assert!(outbox_next_attempt_at(&pool).await > Utc::now());
}
```

- [x] **Step 2: Run and verify RED**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test notification -- --test-threads=1`

Expected: fail because dispatcher and window evaluation do not exist.

- [x] **Step 3: Implement window average and SMTP config**

Use the parameterized query:

```sql
SELECT device_id,
       AVG((measurements ->> $1)::double precision) AS observed_value
FROM telemetry
WHERE event_at >= $2
  AND measurements ? $1
  AND ($3::TEXT IS NULL OR device_id = $3)
GROUP BY device_id
```

Add `lettre` async SMTP/TLS. Require all SMTP values when any are provided:
`SMTP_HOST`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_EMAIL_FROM`,
`ALERT_EMAIL_TO`; default `SMTP_PORT=465`,
`SMTP_TIMEOUT_SECONDS=15`.

- [x] **Step 4: Implement lease/retry**

Claim due rows with `FOR UPDATE SKIP LOCKED`, set a 30-second lease, and send
outside the claim transaction. On failure restore `pending` with
`min(2^attempt_count seconds, 3600 seconds)`; on success mark `sent`.

- [x] **Step 5: Verify GREEN**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test alert --test notification -- --test-threads=1`

Expected: window, outbox, retry, config, and no-real-SMTP tests pass.

### Task 4: Isolate `iot-ingest` Workloads

**Files:**
- Modify: `crates/iot-ingest/src/mqtt.rs`
- Modify: `crates/iot-ingest/src/main.rs`
- Modify: `crates/iot-ingest/src/metrics.rs`
- Modify: `crates/iot-ingest/tests/metrics.rs`

**Interfaces:**
- Critical tasks: MQTT ingress, telemetry writer, and alert evaluator.
- Supporting tasks: window scheduler, SMTP dispatcher, retention, metrics.
- Metrics preserve more than one `GroupStats`, keyed by group name.

- [x] **Step 1: Write a failing multi-group metrics test**

```rust
#[test]
fn metrics_render_writer_and_alert_group_lag() {
    let metrics = IngestMetrics::default();
    metrics.update_group(writer_group_stats());
    metrics.update_group(alert_group_stats());

    let rendered = metrics.render_prometheus();

    assert!(rendered.contains("group=\"timescaledb-writer\""));
    assert!(rendered.contains("group=\"alert-evaluator\""));
}
```

- [x] **Step 2: Run and verify RED**

Run: `cargo test -p iot-ingest --test metrics metrics_render_writer_and_alert_group_lag`

Expected: fail because metrics store only one group snapshot.

- [x] **Step 3: Refactor ingress and task loops**

In `MqttRuntime::poll_once`, move `MqttStreamProducer::ingest` into
`tokio::task::spawn_blocking`, await it, then call `client.ack`. Add a join
error variant to `MqttRuntimeError`.

In `main`, create `JoinSet<Result<(), IngestRuntimeError>>` tasks:

```text
run_mqtt_ingress
run_telemetry_writer
run_alert_evaluator
run_window_scheduler
run_notification_dispatcher (only when SMTP configured)
run_retention
run_metrics_refresh
```

`run_retention` calls `spawn_blocking` for `stream.enforce_retention`.
Tasks with stream groups heartbeat every 10 seconds. Main awaits a task
completion and exits nonzero if a critical task returns an error.

- [x] **Step 4: Add runtime metrics**

Render:

```text
iot_ingest_stream_group_lag{group="timescaledb-writer",partition="N"}
iot_ingest_stream_group_lag{group="alert-evaluator",partition="N"}
iot_ingest_alert_open_incidents
iot_ingest_notification_outbox_pending
iot_ingest_notification_failures_total
```

- [x] **Step 5: Verify isolation**

Run: `cargo test -p iot-ingest --test mqtt_consumer --test metrics`

Expected: manual ACK remains after append; both group metrics pass.

### Task 5: Add Alert API and Dashboard

**Files:**
- Modify: `crates/iot-api/Cargo.toml`
- Modify: `crates/iot-api/src/routes.rs`
- Modify: `crates/iot-api/tests/api.rs`
- Modify: `web/lib/api.ts`
- Create: `web/components/alert-panel.tsx`
- Modify: `web/components/dashboard.tsx`
- Modify: `web/app/globals.css`
- Create: `web/tests/alert-panel.test.tsx`

**Interfaces:**
- `GET /api/alert-rules`
- `POST /api/alert-rules`
- `POST /api/alert-rules/{id}/toggle`
- `GET /api/alert-incidents`
- `POST /api/alert-incidents/{id}/acknowledge`

- [x] **Step 1: Write failing API tests**

```rust
#[tokio::test]
async fn creates_and_lists_a_window_alert_rule() {
    let app = router(ApiState::new(prepared_pool().await));
    let response = app
        .oneshot(post_json(
            "/api/alert-rules",
            json!({
                "name": "Hot average",
                "metric_key": "temperature_c",
                "rule_type": "window_average",
                "comparison": "gt",
                "threshold": 40.0,
                "window_seconds": 300
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
}
```

- [x] **Step 2: Run and verify RED**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-api --test api creates_and_lists_a_window_alert_rule`

Expected: fail because alert API routes do not exist.

- [x] **Step 3: Implement route validation**

Validate identifiers, UUID paths, `metric_key`, comparison, severity,
nonnegative duration fields, and window-rule minimum 60 seconds. Generate rule
UUIDs in Rust. Toggle receives `{ "enabled": boolean }`. Acknowledge sets
`acknowledged_at=now()` and `acknowledged_by='dashboard'`.

- [x] **Step 4: Write failing dashboard tests**

```tsx
it("creates a threshold rule and acknowledges an open incident", async () => {
  render(<AlertPanel apiBaseUrl="http://api" />);
  await userEvent.type(screen.getByLabelText("Rule name"), "High temperature");
  await userEvent.click(screen.getByRole("button", { name: "Create rule" }));
  expect(fetch).toHaveBeenCalledWith(
    "http://api/api/alert-rules",
    expect.objectContaining({ method: "POST" }),
  );
});
```

- [x] **Step 5: Implement compact operational UI**

Create `AlertPanel` with rule table, simple create form, enabled toggle,
incident table, and acknowledge icon button. Load data with dashboard refresh.
Use concise headings, native inputs/selects, and no nested cards.

- [x] **Step 6: Verify API and web**

Run:

```bash
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-api --test api
npm --prefix web test
npm --prefix web run build
```

Expected: API rule/incident tests and dashboard interaction/build pass.

### Task 6: Stress And Operational Verification

**Files:**
- Create: `crates/iot-ingest/tests/stress.rs`
- Create: `scripts/stress-local.sh`
- Modify: `scripts/verify-failures.sh`
- Modify: `docs/operations.md`
- Modify: `docs/rush-iot-nano-runtime.architecture.json`

**Interfaces:**
- Ignored Rust stress test appends 10,000 messages from 100 devices and drains
  telemetry writer plus alert evaluator groups.
- `scripts/stress-local.sh` provides `STRESS_EVENTS` and `STRESS_DEVICES`
  overrides and prints elapsed time and messages per second.

- [x] **Step 1: Write the stress test**

```rust
#[tokio::test]
#[ignore = "run with scripts/stress-local.sh"]
async fn ten_thousand_events_drain_to_telemetry_and_alert_groups() {
    let started = Instant::now();
    let (pool, stream, writer, evaluator) = prepared_stress_runtime().await;
    append_events(&stream, 100, 10_000).await;
    drain_groups(&writer, &evaluator).await;

    assert_eq!(telemetry_count(&pool).await, 10_000);
    assert_eq!(group_lag(&writer).await, 0);
    assert_eq!(group_lag(&evaluator).await, 0);
    assert!(started.elapsed() < Duration::from_secs(120));
    eprintln!("stress throughput={:.0} msg/s", 10_000.0 / started.elapsed().as_secs_f64());
}
```

- [x] **Step 2: Run stress RED before helper implementation**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test stress -- --ignored --test-threads=1 --nocapture`

Expected: fail because stress runtime helpers do not exist.

- [x] **Step 3: Implement test helpers and script**

`stress-local.sh` starts Compose, waits for TimescaleDB, exports
`DATABASE_URL`, then runs the ignored test with `--test-threads=1`. It prints
the measured events/second supplied by the test and preserves no state outside
the test database.

- [x] **Step 4: Update operations**

Document SMTP environment variables, alert group lag and outbox alerts, plus:

```bash
STRESS_EVENTS=10000 STRESS_DEVICES=100 scripts/stress-local.sh
```

Update runtime architecture JSON with `alert-evaluator`, outbox, and SMTP
dispatcher.

- [x] **Step 5: Run final verification**

Run:

```bash
cargo fmt --check
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test --workspace
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot scripts/verify-failures.sh
scripts/e2e-local.sh
STRESS_EVENTS=10000 STRESS_DEVICES=100 scripts/stress-local.sh
npm --prefix web test
npm --prefix web run build
```

Expected: every test passes, local smoke returns the expected telemetry count,
stress drains both groups inside 120 seconds, and the dashboard builds.
