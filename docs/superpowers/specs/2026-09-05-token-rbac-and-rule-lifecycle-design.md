# Token RBAC And Rule Lifecycle Design

**Date:** 2026-09-05

## Goal

Protect the dashboard and HTTP API with a simple two-role token login:

- One `admin` role with read and write access.
- One `viewer` role with read-only access.
- No username.
- Tokens are at least eight ASCII characters and contain at least one uppercase
  letter, lowercase letter, digit, and special character.

Add rule editing and deletion while preserving alert and notification history.

## Scope

- Bootstrap hard-coded admin and viewer tokens on a new database.
- Allow a signed-in user to replace the token permitted by their role.
- Apply role authorization to all existing API operations.
- Add API and dashboard support for editing and archiving alert rules.
- Keep existing incidents and notification outbox records when a rule is
  deleted from the dashboard.

This version does not add user accounts, usernames, token recovery, multi-node
session state, audit log views, or third-party identity providers.

## Authentication Model

The dashboard keeps the submitted token in browser `sessionStorage`, so it
survives a page refresh but not a new browser session. Every authenticated API
request carries:

```text
Authorization: Bearer <token>
```

This is preferable to an HttpOnly cookie for the current development topology:
the Next.js dashboard uses `localhost:3000` while the Rust API uses
`127.0.0.1:8080`. Bearer authentication works with the existing cross-origin
setup without a cookie proxy or credentialed CORS configuration.

The token is used only for login and authenticated HTTP requests. It is never
returned by the API, displayed by the profile, persisted as plaintext, or
logged.

### Bootstrap And Storage

When the token table is empty, the API initializes `admin` with
`NanoAdmin@1234` and `viewer` with `NanoView@1234`. The API stores Argon2
hashes, one per fixed role, in `api_access_tokens`.

After bootstrap, token rows are the source of truth. Restarting the API does
not overwrite tokens that were changed through the profile. Resetting the
database recreates the hard-coded initial values.

The two initial tokens differ. Token validation accepts only ASCII
characters, rejects whitespace, and requires at least eight characters.

### Roles

| Role | Allowed |
| --- | --- |
| `viewer` | Read devices, telemetry, rules, and incidents; view own profile; replace the viewer token. |
| `admin` | All viewer access plus commands, rule create/edit/archive/toggle, incident acknowledge, and replacement of either token. |

`/healthz` and `POST /api/auth/login` remain public. Every other `/api` route
requires a valid bearer token. Invalid or missing tokens return `401
Unauthorized`. A valid token without the necessary role returns `403
Forbidden`.

The API uses a request authentication middleware that derives an `AuthContext`
role from the bearer token. Admin-only handlers use a small authorization
extractor or guard rather than duplicating role checks in each handler.

Because the token has intentionally limited entropy, login failures receive an
in-memory limit of five failed attempts per IP address per minute. Successful
login clears that IP's failure state. This is only single-node protection and
does not attempt durable or distributed rate limiting.

## Authentication API

```text
POST /api/auth/login
GET  /api/auth/me
POST /api/auth/logout
PUT  /api/auth/tokens/{role}
```

`POST /api/auth/login` accepts only:

```json
{ "token": "Aa1!bcDe" }
```

It validates the token against the persisted hashes and responds only with the
role. `GET /api/auth/me` returns the effective role so the dashboard can
restore its state after refresh. `POST /api/auth/logout` requires a valid
bearer token but has no server-side state; the dashboard removes its session
token.

`PUT /api/auth/tokens/{role}` accepts a new token. A viewer may target only
`viewer`; an admin may target either `admin` or `viewer`. The API validates and
hashes the new value before it atomically replaces the stored hash. Old tokens
are rejected on their next request. When a user replaces their own token, the
dashboard replaces its `sessionStorage` value with the new token before making
another request.

## Rule Editing And Archive Delete

### API

```text
PUT    /api/alert-rules/{id}
DELETE /api/alert-rules/{id}
```

Both are admin-only. `PUT` is a full replacement using the same schema and
validation as `POST /api/alert-rules`. It preserves the existing `enabled`
state; enablement remains the responsibility of the toggle endpoint.

`DELETE` has archive semantics rather than hard-delete semantics:

1. The rule is atomically disabled and receives `archived_at`.
2. It disappears from `GET /api/alert-rules` and is no longer evaluated.
3. Any `pending` or `open` incidents for that rule move to `resolved`.
4. Each incident resolution queues the normal idempotent `resolved`
   notification in the existing outbox transaction.
5. The rule, incidents, and notifications remain in the database for incident
   history and email auditability.

A migration adds nullable `archived_at` to `alert_rules` and an index suitable
for filtering active rules. Existing rule loaders and list queries filter
`archived_at IS NULL`. Incident history retains the rule name through the
existing foreign key.

An update to an active rule takes effect for subsequent evaluations. Existing
incidents retain their state and may resolve or continue according to the new
configuration; V1 does not introduce rule-versioned incident state.

### Dashboard

The dashboard starts in an unauthenticated login view containing one token
field. It shows the dashboard only after login succeeds. The top-level profile
shows the current role and supports logout.

The profile form never reveals the current token. A viewer sees a form to
replace the viewer token. An admin can select `admin` or `viewer` as the token
target. When an admin changes the admin token, their own browser session is
updated with the new token.

The alert form has two modes:

- Create: the existing compact form and `Create rule` action.
- Edit: an edit icon populates the form, changes the primary action to `Save
  changes`, and exposes a `Cancel` action.

Each rule row has an edit icon and a delete icon for admins. Delete has an
explicit confirmation because it resolves active incidents. The table hides
all mutable controls for viewers. The incident acknowledge control is also
admin-only.

## Error Handling

- Bad login token: `401` with a generic authentication failure response.
- Login rate limit reached: `429 Too Many Requests`.
- Invalid replacement token or invalid rule edit payload: `400 Bad Request`.
- Archived or unknown rule: `404 Not Found`.
- Attempting a mutation as viewer: `403 Forbidden`.
- An archive transaction rolls back entirely if any incident transition or
  outbox insert fails.

The dashboard maps `401` to its login screen and removes the stale local token.
Other failures stay in the existing error surface without exposing sensitive
token detail.

## Verification

Rust API coverage will prove:

- Initial bootstrap uses the two designated hard-coded role tokens.
- Plaintext tokens are absent from database rows and API responses.
- Login identifies both roles, rejects wrong tokens, and rate limits repeated
  failures.
- Viewer reads are authorized while every mutation is forbidden.
- Admin commands, rule mutations, and incident acknowledgement remain
  authorized.
- Token replacement immediately rejects the old token; self-replacement
  supports subsequent authenticated requests with the new token.
- Rule update preserves validation rules and enablement.
- Rule archive removes it from active listings and evaluation, resolves active
  incidents, preserves history, and queues one idempotent resolved
  notification per transitioned incident.

Dashboard tests will prove:

- Login blocks dashboard data requests until a valid token has been entered.
- API requests include the bearer header.
- Viewer mode hides all mutation controls.
- Admin mode supports edit, archive confirmation, token replacement, and
  logout.

The full Rust workspace tests, failure checks, dashboard tests/build, local
end-to-end test, and stream stress test remain the completion gate.
