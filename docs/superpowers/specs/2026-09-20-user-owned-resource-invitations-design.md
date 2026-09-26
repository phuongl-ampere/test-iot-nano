# User-Owned Resource CRUD and Invitations

## Scope

Regular users may manage Device and Asset resources they own inside their active
tenant. Resource ownership remains distinct from access sharing:

- Only a Tenant Account may assign, clear, or transfer `owner_user_id`.
- A regular user never transfers ownership.
- A recipient of a shared resource cannot edit, delete, assign, share, revoke,
  or transfer it.
- A user owner may share access only through an in-app invitation accepted by
  the recipient. There is no email delivery or external invitation link.

This replaces immediate owner-to-user permission creation in the user workspace.

## Owner Workspace CRUD

The `/app` workspace adds owner-only mutations.

- An owner may create a root Asset such as a farm, or a child Asset whose parent
  is also owned by that user. New Assets receive the current user as owner.
- An owner may rename, edit metadata, reparent, and delete owned Assets. A
  parent change must stay in the tenant, remain within the user's owned assets,
  and reject cycles.
- An owner may provision a Device. The device receives the current user as
  owner and may be attached only to an Asset owned by that user.
- An owner may edit an owned Device, assign or unassign its Asset only among
  Assets they own, manage metadata, and delete it.
- Device and Asset profiles remain tenant-defined. A user may select an
  existing tenant profile permitted by the management API, but cannot create,
  update, or delete profiles.
- Device credentials are issued to the device owner only. A newly provisioned
  or rotated credential is displayed once and is never placed in a URL, audit
  payload, or invitation.

Tenant Account CRUD remains available for the entire tenant. It can assign a
resource to a regular-user owner, but cannot create direct user grants through
the former generic tenant permission form.

## Invitation Data Model

Add a tenant-scoped resource invitation persistence model with exactly one
target resource:

- `id`, `tenant_id`
- `sender_user_id`, `recipient_user_id`
- `asset_id` xor `device_id`
- proposed permission: `viewer` or `manager` (`View` or `Control` in UI)
- state: `pending`, `accepted`, `cancelled`, `withdrawn`, or `invalidated`
- `created_at`, `updated_at`, `accepted_at`, `closed_at`
- optional `resource_permission_id` after acceptance

At most one pending invitation is allowed for a resource-recipient pair. An
owner inviting the same recipient again updates the requested permission on
the pending invitation rather than adding another row. Closed invitations are
retained as audit history; they do not grant access.

## Invitation Lifecycle

1. An owner opens one owned Device or Asset and enters an existing username in
   the same tenant, then selects `View` or `Control`.
2. The backend validates that the sender still owns the resource, recipient is
   a different regular user in the same tenant, and writes or updates a
   `pending` invitation. No `resource_permissions` row exists yet.
3. Every `/app` page displays an `Invitations (n)` header badge for the active
   user's pending received invitations. The badge links to `/app/invitations`.
4. The recipient sees the resource name, sender, proposed access, and `Accept`
   and `Cancel` actions.
5. `Accept` atomically revalidates the sender's ownership and resource scope,
   writes a direct resource permission, and changes the invitation to
   `accepted`. A duplicate active permission is not created.
6. `Cancel` closes the invitation as `cancelled` without creating a permission.
   The sender may close a pending invitation as `withdrawn`.
7. When a resource is deleted, its owner changes, or its tenant becomes
   unavailable, pending invitations for it are marked `invalidated`. Existing
   accepted permissions follow the existing delete/ownership revocation rules.

Only the resource owner can create or withdraw invitations. Only the named
recipient can accept or cancel. A recipient with an accepted invitation still
cannot invite others or transfer ownership.

## Backend Boundaries

`iot-storage` exposes typed contracts and repositories for:

- owner-scoped Asset and Device CRUD;
- create/update, list, accept, cancel, withdraw, and invalidate invitations;
- a transaction that accepts an invitation and creates its direct permission;
- count and list pending invitations for one recipient.

The monolith management layer exposes user-session routes for owner CRUD and
invitation lifecycle. Tenant API routes continue to require a Tenant Account
for tenant-wide management and ownership transfer. All resource, user, parent,
and profile references are tenant-scoped in storage, not trusted from UI input.

## User Interface

The common `/app` base template receives a stable `Invitations (n)` navigation
item. The count is derived only from invitations addressed to the signed-in
user, so no sender or tenant data leaks through the badge.

Owner Device and Asset detail pages provide:

- create and edit actions for owned resources;
- Asset assignment constrained to owned Assets;
- an invitation form and pending/accepted access list;
- a withdraw action for pending invitations and revoke action for accepted
  direct grants.

Shared-resource pages remain read-only. They do not render management or
invitation controls. The invitation page is available to all regular users but
shows only their incoming invitations.

## Failure Handling

- Unknown, self, non-regular-user, or cross-tenant usernames fail without
  creating an invitation.
- If sender ownership is lost before acceptance, acceptance fails and the
  invitation is invalidated.
- A Device cannot be assigned to an Asset the owner does not own.
- Asset reparenting rejects cycles and non-owned parents.
- Delete confirmation presents the number of child resources, active grants,
  and pending invitations affected.
- Session expiry redirects to login. Generated credentials and invitation
  actions never use query-string secrets.

## Verification

Focused tests must cover:

- user owner creates and edits only owned Assets and Devices;
- assignment and reparenting reject non-owned or cross-tenant references;
- a pending invitation gives recipient no resource access;
- recipient acceptance creates exactly one direct permission;
- cancellation, withdrawal, ownership change, and delete do not grant access;
- shared recipients cannot mutate, invite, revoke, or transfer;
- `/app` badge and invitation page render only recipient-specific pending rows.
