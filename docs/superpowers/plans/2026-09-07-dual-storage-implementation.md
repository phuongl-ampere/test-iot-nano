# Dual Storage Implementation Plan

**Goal:** Run Rush IoT Nano with either TimescaleDB or embedded SQLite WAL,
selected at startup without changing REST, MQTT, alert, or management behavior.

## Fixed Decisions

- `IOT_DATABASE_STORAGE=timescale|sqlite` is canonical; `USE_DATABASE_STORAGE`
  is an accepted compatibility alias.
- Timescale requires `DATABASE_URL`; SQLite requires absolute
  `IOT_SQLITE_PATH`.
- One deployment uses exactly one backend. There is no runtime switch or
  dual-write.
- SQLite uses WAL, foreign keys, a five-second busy timeout, one telemetry
  write actor, and batch retention maintenance.
- Raw telemetry retention and rollup retention are database settings distinct
  from `iot-stream` segment retention.

## Tasks

- [x] Storage selection parser, SQLite WAL opening, full SQLite schema, raw
  telemetry idempotency, 5m/1h rollups, and batch retention maintenance.
- [ ] SQLite auth, sessions, users, tokens, gateway ownership, and management
  entity repository.
- [ ] SQLite telemetry/power query repository and API read models.
- [ ] SQLite alert evaluator, incidents, reminder outbox, and notification
  repository.
- [ ] Runtime selection in `iot-api` and `iot-ingest`; Timescale compatibility
  regression tests and SQLite end-to-end tests.
- [ ] SQLite backup/export-import migration CLI and operations documentation.
