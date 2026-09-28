# Owner-Scoped Resource Sharing Design

> Current implemented behavior is maintained in
> [Platform current behavior](../../platform-current-behavior.md).

## Goal

Make Device and Asset access explicit: a Tenant Account assigns one regular-user owner, and only that owner can share `view` or `control` access with other regular users.

## Ownership

- Every Device and Asset remains tenant-scoped through `tenant_id`.
- `owner_user_id` is the sole primary user relationship. It is nullable only while a Tenant Account has not assigned the resource.
- A Tenant Account can assign, transfer, or clear an owner only through a dedicated ownership operation. The target must be a regular user in the same tenant.
- Device and Asset ownership is independent. Owning or sharing an Asset never grants access to a child Asset or Device.
- A transfer or clear operation atomically revokes every active direct resource grant for that Device or Asset and emits an ownership audit event with the previous owner, new owner, and revoked-grant count.

## Sharing

- The owner has implicit `owner` access and may create or revoke direct grants to another regular user.
- Device grants expose `view` and `control`; Asset grants expose `view` and `manager`. A shared recipient never becomes the owner and cannot delete an Asset.
- A recipient can view or control the granted resource but cannot share, revoke grants, transfer ownership, modify the resource, or manage access.
- Tenant Accounts no longer create resource grants through Device, Asset, or generic tenant permission forms. They assign ownership instead.
- The resource record does not duplicate recipient lists. The user workspace derives “Shared by <owner>” from the owner/grant data; the owner detail derives “Shared with” rows from the same grants.

## New user defaults

When a Tenant Account creates a regular user, the user receives these capabilities:

- Create assets
- Claim devices
- Control devices
- Share owned resources

The user starts without owning an Asset or Device, and has no `view`, `manager`, or `control` permission on a specific resource. They therefore cannot see or operate resources owned by another user.

After the user claims a Device or creates an Asset, they become its owner. An owner can share that resource, can control their own Device, and is the only actor allowed to delete their own Asset. A new user does not receive `Create devices`, `Edit resources`, `Manage device tokens`, `Assign profiles`, or tenant-wide resource access by default.

## UX

- Tenant Device and Asset editors replace “User access” with one `Assigned user` selector and an explicit unassign action. Saving warns that transfer clears existing shares.
- Owner Device and Asset detail pages show `Share with user`, an access selector (`View`, `Control`), and existing direct shares. The asset inheritance control is removed.
- Recipient pages show the resource, effective access, and owner/share source, but no share or revoke controls.
- Cross-tenant accounts, Tenant Accounts, System Accounts, and self-grants are rejected server-side.

## Non-goals

- No resource-to-user denormalized lists.
- No asset-to-child resource permission inheritance.
- No change to tenant isolation, telemetry, gateway topology, or generic group membership.
