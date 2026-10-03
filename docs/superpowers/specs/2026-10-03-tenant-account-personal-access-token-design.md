# Tenant Account personal access token

## Goal

Add a Personal Access Token (PAT) screen to the tenant console. Only a Tenant
Account can manage its own PAT. The account may have exactly one active token,
which grants the full public API authority of that Tenant Account within its
tenant.

## Scope and authorization

- Tenant Account is the sole PAT principal and the sole actor allowed to list,
  create, rotate, and revoke its PAT.
- System Account, Platform Account, and Tenant User cannot access PAT UI or
  PAT management APIs; they receive the existing forbidden response.
- A PAT authenticates only public `/api/v1/*` resource requests. It cannot
  authenticate management console JSON routes, HTML pages, OAuth endpoints, or
  MQTT traffic.
- PAT authorization is tenant-scoped and uses the full public API authority of
  its owning Tenant Account. It has no scope picker and no expiry setting.
- Validation checks that the token is active and that its owning Tenant Account
  and tenant remain enabled. Disabling either prevents use immediately.

## Token lifecycle and storage

The token format uses a recognizable prefix followed by cryptographically
random secret material. The plaintext secret is returned exactly once by a
successful create or rotation operation and is never persisted, rendered in a
list, logged, or returned again.

Persist a token record with the Tenant Account identifier, tenant identifier,
display name, non-secret prefix, hash/digest, created timestamp, last-used
timestamp, and nullable revoked timestamp. A database uniqueness constraint
allows at most one record with a null revoked timestamp for each Tenant
Account.

Creating when no active PAT exists inserts one. Rotating an existing PAT
revokes it and inserts the replacement in one transaction. Revoking sets the
active record's revoked timestamp; it does not delete the audit record. The
previous secret becomes invalid immediately after rotation or revocation.

## Console and API

Add a Tenant Account navigation item and page at
`/tenant/personal-access-tokens`. The page displays the current active token's
name, prefix, creation time, and last-used time; no secret value appears in the
normal page response.

The page provides:

- **Create token** when no PAT is active, with a required display name.
- **Rotate token** when a PAT is active, with a required display name for the
  replacement and a clear notice that the prior token stops working.
- **Revoke token** for the active PAT.
- A one-time success panel with the secret and a Copy action after create or
  rotate. Reloading or navigating away never reveals it again.

Management API routes follow the current versioned namespace:

- `GET /api/v1/management/personal-access-token`
- `POST /api/v1/management/personal-access-token`
- `POST /api/v1/management/personal-access-token/revoke`

The GET response contains only safe metadata. The POST response includes the
one-time secret plus safe metadata. The revoke response contains no secret.

## Public API authentication

Bearer authentication recognizes a PAT by its prefix, hashes the presented
secret, resolves the active record, and derives the Tenant Account's tenant
and public authority. A successful request updates `last_used_at` without
altering the token secret. OAuth bearer access tokens continue to work exactly
as before.

## Validation

Tests must first fail for the intended feature gap, then verify:

- token plaintext is returned only once and storage contains a digest, never
  the plaintext;
- the one-active-PAT constraint, atomic rotation, revocation, and tenant
  isolation;
- PAT bearer access to public v1 resources, denial after revocation, and
  denial for disabled owners;
- PAT management authorization is Tenant Account only;
- page/API metadata does not disclose a secret and the UI shows the one-time
  secret only immediately after create/rotate.

Run relevant Rust tests, the full Power Monitor suite and production build,
and the tenant console tests affected by navigation and management APIs.
