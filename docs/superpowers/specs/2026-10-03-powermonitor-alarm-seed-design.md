# Power Monitor alarm seed design

## Goal

Make the local Power Monitor demo show editable alarm configuration without
creating telemetry samples or alert incidents.

## Scope

The local seed creates one enabled `High active power` alarm rule for each of
the eight seeded `Power Meter` devices. Every rule uses the existing
`event_threshold` evaluator with `power_w > 500`, `warning` severity, no
evaluation delay, and the UI's standard resolution, reopening, and reminder
durations.

The seed does not publish telemetry, create alert incidents, or insert records
outside the platform APIs. The current platform has no liveness/offline alert
rule kind, so no simulated disconnect alarm is created.

## Data flow

`seed-local-platform.sh` provisions the existing devices, then uses the Tenant
Account session to create or validate each device's alert rule through the
management API. Re-running the reset-only local seed produces exactly eight
enabled overload rules and zero telemetry or incidents.

## Error handling and verification

The seed fails if a device has an ambiguous or conflicting rule. Its regression
test checks that all eight device IDs are supplied to the rule-seeding helper.
Verification checks the database/API for eight enabled rules and zero telemetry
and incidents.
