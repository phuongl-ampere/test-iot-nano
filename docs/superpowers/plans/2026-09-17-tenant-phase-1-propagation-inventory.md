# Tenant Phase 1 Propagation Inventory

**Audit date:** 2026-09-17
**Basis:** `docs/superpowers/specs/2026-09-17-tenant-scoped-authorization-design.md`,
current SQLite/Timescale schema, repositories, API/runtime call paths, and
existing tests.

## Cutover Rules

- Tenant is the absolute boundary. Every tenant-scoped row has `tenant_id
  NOT NULL`; every lookup, write, join, and worker claim carries the tenant
  predicate or a same-tenant composite foreign key.
- No runtime default tenant, fallback lookup, legacy dual path, or role-only
  authorization. Migration input may name one explicit tenant, but runtime
  registration and authorization may not infer one.
- `system_accounts`, `tenants`, and `tenant_accounts` are intentionally global
  identity roots. They are the control plane and are not tenant resources.
  `api_access_tokens` is a legacy global credential table, not an approved
  tenant resource; it must be removed or replaced during cutover, never used as
  a fallback.

## Gap Inventory

| Scope and current gap | Owning repository/API/runtime modules | Existing tests to extend |
|---|---|---|
| `user_app_grants` has no `tenant_id`; `app_key` is not constrained to a same-tenant application. | `crates/iot-storage/src/management.rs` user CRUD; `services/iot-nano-monolith/src/management.rs` management routes. | `crates/iot-storage/tests/management_users.rs`; `services/iot-nano-monolith/tests/management_users_profiles.rs`. |
| `asset_profiles`, `device_profiles` have no tenant ID and globally unique names; profile references are not same-tenant constrained. | `crates/iot-storage/src/management.rs` profile repositories; monolith management profile handlers; `crates/iot-storage/src/public_api.rs` profile existence checks. | `crates/iot-storage/tests/management_profiles.rs`; `crates/iot-storage/tests/public_api.rs`; monolith profile route tests. |
| `device_tokens` and `device_claim_codes` have no tenant ID; token/claim reads and writes identify only `device_id`. | `crates/iot-storage/src/management.rs` `DeviceTokenRepository`; `services/iot-nano-api/src/device_tokens.rs`; `crates/iot-storage/src/lib.rs` device authorization; `iot-nano-foundation` token validation. | `crates/iot-storage/tests/device_tokens.rs`; `crates/iot-storage/tests/device_authorization.rs`; `crates/iot-nano-foundation/tests/device_token.rs`; monolith management-session token tests. |
| `applications`, redirect URIs, client secrets, OAuth codes/tokens, and `user_app_grants` have no tenant ID. OAuth joins by app/user do not prove the same tenant; client IDs are globally unique. | `crates/iot-storage/src/lib.rs` `ApplicationRepository`/`OAuthRepository`; `services/iot-nano-api/src/application_registry.rs`, `oauth.rs`; monolith application management; PowerMonitor clients are consumers only. | `crates/iot-storage/tests/application_registry.rs`; `crates/iot-storage/tests/oauth_persistence.rs`; `services/iot-nano-api/tests/oauth.rs`; monolith external-app contract tests. |
| `resource_shares` and `resource_grants` have no tenant ID; polymorphic `resource_id`/`grantee_id` are not database-bound to same-tenant assets/devices/users/applications. Existing permission names also include legacy `controller`. | `crates/iot-storage/src/public_api.rs` grant repository and authorization; `services/iot-nano-api/src/public_v1.rs`; `crates/iot-storage/src/lib.rs` authorization contracts. | `crates/iot-storage/tests/resource_authorization.rs`; `crates/iot-storage/tests/public_api.rs`; `services/iot-nano-api/tests/public_v1.rs`. |
| `telemetry`, `telemetry_rollups_5m/1h`, and Timescale views have no tenant ID. Device-only grouping/indexes permit accidental cross-tenant aggregation if a device predicate is omitted. | `crates/iot-storage/src/lib.rs` `TelemetryRepository`, `TelemetryAggregateRepository`, rollup writers; `services/iot-nano-core/src/writer.rs`; `services/iot-nano-api/src/public_v1.rs` telemetry routes. | `crates/iot-storage/tests/telemetry_aggregate.rs`; `crates/iot-storage/tests/gateway_ingest.rs`; `services/iot-nano-core/tests/writer.rs`; `services/iot-nano-core/tests/platform_writer.rs`; API public-v1 tests. |
| `gateway_event_receipts` has no tenant ID and keys only on gateway/idempotency key. `device_runtime_state` has no tenant ID and is keyed only by device. | `crates/iot-storage/src/lib.rs` gateway ingest and runtime-state writes; `services/iot-nano-core/src/writer.rs`; `services/iot-nano-mqttd` gateway session/authorization paths. | `crates/iot-storage/tests/gateway_ingest.rs`; `services/iot-nano-core/tests/writer.rs`; `services/iot-nano-mqttd/tests/authorization.rs`; local-port and transport tests. |
| `alert_rules`, rule evaluations, `alert_incidents`, and `notification_outbox` have no tenant ID. Rule/device, incident/rule/device, and outbox/incident relations do not enforce same-tenant scope. Worker claims are global. | `crates/iot-storage/src/lib.rs` alert evaluation, incident, notification repositories; `services/iot-nano-core/src/alert.rs`, alert consumer/notification dispatcher; `services/iot-nano-api/src/public_v1.rs` alert routes. | `crates/iot-storage/tests/alert_rule.rs`, `alert_evaluation.rs`, `alert_incident.rs`, `notification_outbox.rs`; `services/iot-nano-core/tests/alert.rs`, `platform_alert.rs`; API alert tests. |
| `command_outbox` has no tenant ID; command enqueue, claim, response, and expiration use device/command IDs without an explicit tenant predicate. | `crates/iot-storage/src/lib.rs` `CommandRepository`/`CommandLifecycleRepository`; `services/iot-nano-api/src/public_v1.rs` and `core_facade.rs`; `services/iot-nano-core` command dispatch; `iot-nano-mqttd` transport. | `crates/iot-storage/tests/command_outbox.rs`; `services/iot-nano-monolith/tests/command_transport.rs`; `services/iot-nano-api/tests/public_v1.rs`; MQTT transport tests. |
| `audit_events` has no tenant ID; actor identity is legacy `actor_account_class`, and resource IDs are polymorphic without same-tenant references. System/Tenant Account actions need an explicit principal identity without granting resource access. | `crates/iot-storage/src/lib.rs` audit call sites; management/public API handlers; system lifecycle handlers in `services/iot-nano-monolith/src/management.rs`. | `crates/iot-storage/tests/migration_safety.rs`; tenant lifecycle tests; management/public authorization tests. |
| `devices.gateway_device_id` is tenant-scoped only indirectly: its FK is `device_id`-only, so a child can reference a gateway from another tenant. The same issue applies to gateway ingest pair checks unless both device tenant IDs are selected/locked. | `crates/iot-storage/src/management.rs` topology validation; `crates/iot-storage/src/lib.rs` gateway authorization/ingest; `services/iot-nano-core/src/writer.rs`; `services/iot-nano-mqttd`. | `crates/iot-storage/tests/device_authorization.rs`, `gateway_ingest.rs`; management device tests; core writer and MQTT gateway tests. |
| `assets` and `devices` already have required tenant IDs and same-tenant owner/asset FKs, but repository queries are mixed: several management/profile/token paths still select by resource ID alone. | `crates/iot-storage/src/management.rs`, `public_api.rs`; `services/iot-nano-monolith/src/management.rs`; `services/iot-nano-api/src/public_v1.rs`. | `crates/iot-storage/tests/management_assets.rs`, `management_devices.rs`, `public_api.rs`, `resource_authorization.rs`; cross-tenant HTTP tests. |

## Required New Spec Tables

The existing `resource_shares` and `resource_grants` model is not the target
authorization model. Phase 5 must replace it, rather than merely add tenant
columns to it, with these tenant-scoped records and constraints:

- `user_groups` and `user_group_members`, with same-tenant owner/member
  composite foreign keys and a `(tenant_id, user_id, group_id)` membership
  index.
- `resource_permissions`, with exactly one User or Group subject, exactly one
  Asset or Device scope, same-tenant composite foreign keys for every subject,
  resource, and creator, and active-permission indexes from the approved
  design.
- `device_relations`, with tenant-scoped endpoint foreign keys, unique
  `(tenant_id, from_device_id, relation_type, to_device_id)`, no self-relation,
  and rejection of the reserved `gateway_child` relation type.

Those replacements also remove the legacy `controller` permission. They are
scheduled after the base resource/OAuth scope work, because only then can
effective `owner > manager > viewer > deny` evaluation be safely introduced.

## Ordered Implementation Slices

1. **Canonical schema and migration gate.** Update both SQLite bootstrap and
   `crates/iot-storage/migrations/0001_platform.sql`: add non-null tenant IDs,
   `(id, tenant_id)` keys, same-tenant composite FKs, tenant-prefixed indexes,
   and explicit migration-tenant configuration/backfill failure behavior.
   Remove the runtime use of `api_access_tokens` and legacy account-class
   fallback. Extend `crates/iot-storage/tests/tenant_identity.rs`,
   `migration_safety.rs`, and backend contract tests. Dependencies: existing
   System/Tenant identity code; blocks every later slice.
2. **Profiles, user app grants, tokens, and claims.** Scope profile ownership,
   `user_app_grants`, `device_tokens`, and `device_claim_codes`; make all
   repository signatures accept tenant scope and enforce device/profile/app
   same-tenant joins. Extend management profile/user/token tests and core token
   tests. Depends on slice 1.
3. **Applications and OAuth.** Add tenant scope to applications and all OAuth
   child tables; resolve clients/codes/tokens by `(tenant_id, ...)`, bind
   authenticated users and app grants to one tenant, and pass tenant scope
   through `application_registry.rs` and `oauth.rs`. Extend registry,
   persistence, API OAuth, and external-app tests. Depends on slices 1-2.
4. **Resource sharing and authorization.** Add tenant scope and composite
   constraints to shares/grants, normalize permission levels to the approved
   owner/manager/viewer model, and require tenant-first resource lookup in
   `public_api.rs`, `public_v1.rs`, and authorization repositories. Extend
   cross-tenant writes, direct/group/inherited access, list, and enumeration
   tests. Depends on slices 1-3.
5. **Telemetry and gateway operational state.** Add tenant IDs to telemetry,
   rollups/views, receipts, and runtime state; make ingest authenticate the
   device credential plus tenant and validate the exact active gateway-child
   pair. Partition/index by tenant and propagate scope through
   `iot-nano-core/src/writer.rs` and MQTT authorization. Extend ingest,
   aggregate, writer, gateway authorization, and former-gateway rejection
   tests. Depends on slices 1-2; independent of user sharing.
6. **Alerts, incidents, notifications, and commands.** Add tenant IDs and
   same-tenant foreign keys to alert/evaluation/incident/outbox and command
   outbox tables. Make worker claims, transitions, command responses, and
   expiration tenant-aware; pass scope from API/core dispatch. Extend all
   alert, notification, command lifecycle, and transport tests. Depends on
   slices 1, 4, and 5.
7. **Audit and end-to-end cutover.** Add tenant-aware principal fields to
   audit events, emit tenant-scoped events for ownership/sharing/containment/
   gateway changes, and verify System Account is limited to lifecycle APIs while
   Tenant Account/User access is same-tenant only. Extend tenant lifecycle,
   management, public API, migration, and service integration tests. Depends
   on all prior slices.

## High-Risk Gaps

- **Critical:** Telemetry, alert, notification, command, receipt, and runtime
  tables are not tenant-keyed; global worker claims and rollups can mix or
  expose tenant data.
- **Critical:** OAuth applications and resource grants use polymorphic IDs
  without tenant constraints; a valid UUID/client identifier can cross the
  tenant boundary unless every query is corrected together.
- **High:** Device gateway topology is not same-tenant at the database FK
  level, and device token/claim records lack explicit tenant identity.
- **High:** Profiles and `user_app_grants` are global despite being referenced
  by tenant-owned assets/users.
- **High:** Audit records cannot currently represent System/Tenant Account
  principals with tenant scope, weakening evidence for lifecycle and
  authorization enforcement.
