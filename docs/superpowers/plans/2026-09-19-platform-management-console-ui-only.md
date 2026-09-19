# Platform Management Console UI-Only Plan

Goal: make the built-in monolith console consistent, dense, accessible, and
safe using only existing backend routes and data.

Constraints:

- No backend, storage, API, schema, runtime, or PowerMonitor changes.
- Preserve non-JavaScript form submissions.
- Keep Tenant scope server-derived from the session.
- Do not render unsupported destructive or mutation actions as working UI.

## Task 1: Shared Shell And Tenant Navigation

Files: base template, all tenant templates, tenant navigation partial,
platform CSS, platform template tests.

Add one shared tenant navigation partial in the approved order. Add compact
section grouping, consistent active state, accessible icon buttons and a local
HTMX/visibility enhancement in the base layout. Test every tenant template for
the exact order and exactly one active route.

## Task 2: Devices And Assets Presentation

Files: tenant device, credential, token, asset templates, CSS, template tests.

Improve existing provision/create forms, tables, status labels, manual refresh,
credential copy, deep-link-compatible row anchors, notices, and non-JS
confirmation presentation. Use only existing fields and routes. Document
unsupported edit/delete/drawer operations in the backend gap ledger.

## Task 3: Tenant Operations Presentation

Files: profile, group, permission, topology, relation, application, alert, and
audit templates; CSS; template tests.

Apply the same compact operational table/form pattern. Surface current
read-only alert/audit limitations honestly and avoid controls without routes.

## Task 4: System And User Presentation

Files: system, infrastructure, user workspace templates, base template, CSS,
template tests.

Improve system lifecycle/infrastructure readability and make configuration
settings absence explicit without inventing forms. Improve user resource
workspace detail and control affordances only where existing routes support
them.

## Task 5: UI Verification And Gap Ledger

Files: UI-only design doc, plan, template tests.

Run template tests, focused management session tests, desktop/mobile browser
screenshots, and ensure no backend source files changed. Record exact backend
gaps without implementing them.
