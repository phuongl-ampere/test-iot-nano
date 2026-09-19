# Platform Management Console UI-Only Design

## Scope

Improve the built-in monolith Platform UI at system, tenant, and app routes.
Do not use the legacy web directory and do not change PowerMonitor.

## Binding Constraints

- Change templates, platform UI presentation types, local CSS, and local HTMX
  enhancement only.
- Do not add or modify management/API routes, storage repositories, schemas,
  migrations, authorization rules, runtime configuration handling, or
  PowerMonitor.
- Existing server-rendered forms must remain usable without JavaScript.
- Tenant identity always comes from the authenticated session; no tenant picker
  is rendered.
- User workspace never renders Tenant or System management navigation.

## Tenant Navigation

The shared order is fixed:

  Overview, Devices, Assets, Device profiles, Asset profiles, Alerts, Audit,
  Topology, Relations, Users, Groups, Permissions, Applications.

Users is included because Tenant Accounts manage tenant users. The active item
alone varies by page.

## UI Direction

This is a dense operational console, not a marketing page. Tables, status
chips, compact field groups, visible notices, and direct actions optimize
repeated setup and incident workflows. The one distinct visual device is the
fixed tenant navigation rail with grouped operational sections and a small
incident count slot that remains empty until backend data exists.

## UI-Only Deliverables

- Shared tenant navigation and compact responsive console shell.
- Existing device, asset, profile, group, permission, topology, relation,
  application, audit, alert, system infrastructure, and user workspace pages
  receive consistent tables, status chips, action affordances, keyboard focus,
  notices, and confirmation presentation.
- Devices use HTMX polling every ten seconds with a visibility pause guard and
  a manual refresh control, reusing the existing GET page response.
- Existing one-time credential pages get copy controls without adding secrets
  to URLs or persistence.
- Existing JSON text fields receive client-side syntax feedback only; their
  server validation contract is unchanged.

## Backend Gap Ledger

The UI must not fake actions that lack a server-rendered contract. These remain
explicit backend follow-ups:

- Tenant HTML update/delete forms and detail data for devices, assets, and
  profiles.
- Atomic token rotation.
- Alert rule CRUD, incident acknowledge/archive, and open count.
- System settings routes for SMTP, MQTT, retention, and worker tuning.
- Live-versus-restart apply state.
- Structured metadata key editor persistence and server-side JSON field errors.
