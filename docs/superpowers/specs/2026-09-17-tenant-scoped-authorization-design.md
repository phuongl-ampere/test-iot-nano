# Tenant-Scoped Authorization Design

**Date:** 2026-09-17

## Status

Approved design for the first tenant-aware identity and authorization release.

## Goal

Introduce strict tenant isolation while supporting direct user sharing, group
sharing, and inherited access over an asset containment tree. The core must
model arbitrary real-world domains without hard-coded objects such as homes,
companies, rooms, customers, departments, or workspaces.

## Non-Goals

- Multi-tenant membership for a User.
- Nested User Groups.
- Custom permission definitions beyond owner, manager, and viewer.
- Customer, workspace, organization, billing, or subscription entities.
- Arbitrary graph relations that affect authorization.
- Cross-tenant resource sharing, relations, group membership, or ownership.

## Core Principles

1. Tenant is the absolute data and authorization boundary.
2. Globally unique IDs and usernames identify records; they never grant access.
3. Every tenant-scoped record stores a non-null tenant ID.
4. A User belongs to exactly one Tenant permanently.
5. A relation describes domain structure. A permission explicitly grants access.
6. Only containment participates in inherited authorization.
7. Storage and API queries enforce authorization; UI visibility is never a
   security control.

## Account Model

~~~text
System Account
  -> creates and manages Tenant
      -> has exactly one Tenant Account
      -> contains Users, User Groups, Assets, Devices, and Permissions
~~~

### System Account

One active System Account exists per platform deployment. It is the only
principal permitted to create, suspend, reactivate, or delete a Tenant, and to
create, disable, or reset a Tenant Account credential.

System Account access is limited to infrastructure and tenant-lifecycle APIs.
It does not implicitly read tenant assets, devices, telemetry, or user data.
Any future support access must be a separate audited capability.

~~~text
system_accounts
  id UUID primary key
  username text unique
  password_hash text
  status active | disabled
  created_at
  updated_at
~~~

### Tenant

A Tenant is a durable data boundary, not a login identity. It owns all
tenant-scoped data and remains present when its Tenant Account credential is
reset or replaced.

~~~text
tenants
  id UUID primary key
  slug text unique
  status active | suspended | deleted
  metadata JSON
  created_at
  updated_at
~~~

Slug is the public tenant identifier used during User registration and User
login. It is immutable in the first release.

### Tenant Account

Tenant Account is a dedicated administrator credential created by System
Account and bound one-to-one to a Tenant. It is not a User Account and cannot
be a resource owner or User Group member.

~~~text
tenant_accounts
  id UUID primary key
  tenant_id UUID unique references tenants(id)
  password_hash text
  status active | disabled
  credential_version integer
  created_at
  updated_at
~~~

Tenant Account login uses tenant slug plus its password. It has full
management permission inside its own tenant: user, group, resource, profile,
token, and permission administration. It cannot access another Tenant or
System Account APIs.

### User Account

User Accounts are global identities with a globally unique UUID and username.
Each User belongs to exactly one Tenant permanently.

~~~text
users
  id UUID primary key
  tenant_id UUID not null references tenants(id)
  username text unique
  password_hash text
  status active | disabled
  profile JSON
  created_at
  updated_at

unique (id, tenant_id)
index (tenant_id, username, id)
~~~

Self-registration accepts tenant slug, username, password, and profile. The
server resolves an active Tenant from the slug, writes the Tenant ID once, and
does not expose an API to change a User Tenant ID. Tenant Account may create
Users only in its own Tenant.

## User Groups

User Group is a tenant-scoped collection of Users. Its name and metadata
describe whatever real-world construct a Tenant needs; the platform assigns no
business meaning to it.

~~~text
user_groups
  id UUID primary key
  tenant_id UUID not null references tenants(id)
  owner_user_id UUID not null
  name text
  metadata JSON
  created_at
  updated_at

user_group_members
  tenant_id UUID not null
  group_id UUID not null
  user_id UUID not null
  created_at

primary key (group_id, user_id)
index (tenant_id, user_id, group_id)
~~~

The group owner and Tenant Account can add or remove members in the first
release. Group members must belong to the same Tenant as the group. A future
release may add group managers without changing the core ownership model.

## Assets, Devices, and Containment

Asset is the generic organizational resource. Profile and metadata may express
any tenant-defined real-world meaning. The platform does not define object
types such as home, company, room, site, or department.

~~~text
assets
  id UUID primary key
  tenant_id UUID not null
  owner_user_id UUID not null
  parent_asset_id UUID nullable
  profile_id UUID nullable
  metadata JSON
  created_at
  updated_at

unique (id, tenant_id)
index (tenant_id, parent_asset_id)

devices
  id UUID primary key
  tenant_id UUID not null
  owner_user_id UUID not null
  asset_id UUID nullable
  status active | disabled | deleted
  is_gateway boolean not null default false
  gateway_device_id UUID nullable
  gateway_topology_version integer not null default 0
  device-specific fields
  created_at
  updated_at

unique (id, tenant_id)
index (tenant_id, asset_id)
index (tenant_id, gateway_device_id)
~~~

Containment is the only relation that participates in inherited authorization:

~~~text
Asset child.parent_asset_id -> Asset parent.id
Device asset_id            -> Asset id
~~~

Composite foreign keys ensure that parent Asset, assigned Asset, owner User,
and child record have the same Tenant ID. Storage rejects containment cycles
and limits containment depth to 64. Each Asset has at most one parent Asset;
each Device has at most one assigned Asset.

### Gateway-Child Device Topology

Gateway is a Device capability, not a separate business entity. A gateway may
carry telemetry or commands for many child Devices. The child points at its
single active gateway:

~~~text
Device G
  is_gateway = true
  gateway_device_id = null

Device D
  is_gateway = false
  gateway_device_id = G.id
~~~

The gateway and every child must have the same tenant ID. A child cannot point
to itself, and a gateway cannot itself be a child. Gateway-child topology is
operational: it authorizes gateway telemetry ingestion and command routing
using the exact gateway-child pair. It does not imply resource ownership,
sharing, containment, or inherited user permission.

Sharing a gateway does not automatically share its children. Sharing a child
does not automatically share its gateway. User access remains defined only by
ownership and resource permissions.

### Gateway Conflict and Lifecycle Rules

Gateway-child topology has exactly one source of truth:

~~~text
devices.gateway_device_id
~~~

The generic device-relations table must reject the reserved relation type
gateway_child. A second topology representation would permit a gateway
assignment and a descriptive relation to contradict one another.

Gateway topology rules:

1. A child Device has zero or one active gateway; one gateway may have many
   child Devices.
2. Gateway and child must be active Devices in the same Tenant.
3. A User must have manager or owner permission on both gateway and child to
   create, change, or detach gateway topology. Tenant Account may do this
   anywhere in its Tenant.
4. Gateway ownership and child ownership may differ. The topology operation
   does not transfer ownership or create resource permission for either owner.
5. Gateway telemetry and gateway-routed commands are accepted only when the
   submitted gateway-child pair matches the current active topology.
6. Reassignment from gateway G1 to gateway G2 locks the child topology row,
   updates the assignment, increments its topology revision, writes an audit
   event, and commits atomically. Messages from G1 received after commit are
   rejected.
7. A gateway with active children cannot be deleted. A caller must detach or
   reassign every child in the same transaction before deletion.
8. Detaching a child makes gateway-routed telemetry and commands unavailable
   for that child until it is assigned to an active gateway again.

Gateway topology does not affect Asset containment. If a gateway and child
share an Asset scope, an inherited Asset permission may make both visible to a
User; that result is caused by the Asset permission, never by the gateway
edge.

### Descriptive Device-to-Device Relations

The first release also supports generic, tenant-scoped device-to-device
relations for real-world cases that are neither containment nor gateway
topology.

~~~text
device_relations
  id UUID primary key
  tenant_id UUID not null
  from_device_id UUID not null
  relation_type text not null
  to_device_id UUID not null
  metadata JSON
  created_by_user_id UUID not null
  created_at
  updated_at

unique (tenant_id, from_device_id, relation_type, to_device_id)
index (tenant_id, from_device_id, relation_type)
index (tenant_id, to_device_id, relation_type)
~~~

Relation type is tenant-defined metadata such as paired_with, controls,
depends_on, or located_near. The storage layer validates an identifier-shaped
relation type, rejects reserved operational relation types, and rejects
self-relations. Generic device relations may form a graph and do not require
an acyclic constraint because authorization never traverses them.

Creating, changing, or removing a device relation requires manager or owner
permission on both endpoint Devices, unless the caller is Tenant Account.
Descriptive device relations never participate in authorization, command
routing, telemetry routing, or permission inheritance.

An optional future resource-relations table may extend the same descriptive
model to Asset-to-Device and Asset-to-Asset many-to-many relations. It follows
the same rule: descriptive relations do not affect authorization.

## Resource Permissions

The storage schema calls an Access Grant a resource permission. It is the only
record that grants a non-owner User or Group access to a resource.

~~~text
resource_permissions
  id UUID primary key
  tenant_id UUID not null

  subject_user_id UUID nullable
  subject_group_id UUID nullable

  asset_id UUID nullable
  device_id UUID nullable

  permission viewer | manager
  inherit_children boolean not null default false
  created_by_user_id UUID not null
  created_at
  revoked_at nullable
~~~

Database constraints enforce:

~~~text
Exactly one of subject_user_id and subject_group_id is non-null.
Exactly one of asset_id and device_id is non-null.
inherit_children is false for a Device scope.
All subject, scope, and creator records have the same tenant_id.
~~~

The repository performs the same same-tenant checks transactionally before
every permission write. Active-permission indexes are:

~~~text
(tenant_id, device_id, subject_user_id)
(tenant_id, device_id, subject_group_id)
(tenant_id, asset_id, subject_user_id)
(tenant_id, asset_id, subject_group_id)
~~~

### Permission Semantics

~~~text
owner
  Intrinsic resource ownership from owner_user_id.
  All actions, including ownership transfer.

manager
  View, control, configure, manage tokens, create and revoke viewer or
  manager permissions, and manage resources in scope.
  Cannot transfer ownership or create an owner permission.

viewer
  Read the granted resource, state, telemetry, alerts, and permitted
  dashboards.
  Cannot mutate, control, share, revoke, or transfer.
~~~

Owner is not a resource-permission row. Tenant Account receives full
tenant-level access without becoming the owner of every resource.

## Direct, Group, and Inherited Sharing

Direct User share:

~~~text
User B -> Device D -> viewer
~~~

Group share:

~~~text
Group G -> Device D -> manager
~~~

Asset scope with containment inheritance:

~~~text
User B or Group G -> Asset A -> viewer or manager -> inherit_children=true
~~~

An inherited permission applies to all descendant Assets and Devices reached
through containment only. A Device added under an inherited Asset scope gains
the scope permission automatically. A Device moved away from that scope loses
only inherited access; direct Device permissions remain active.

The final effective permission is:

~~~text
owner > manager > viewer > deny
~~~

## Authorization Flow

~~~text
1. Authenticate System Account, Tenant Account, or User.
2. For System APIs, require System Account.
3. For Tenant APIs, resolve the authenticated tenant and require an active
   Tenant.
4. Tenant Account is allowed within its own Tenant only.
5. For User resource APIs, resolve the requested Asset or Device by both
   resource ID and authenticated tenant ID.
6. Allow intrinsic owner access.
7. Resolve active direct User permissions.
8. Resolve active Group permissions for Groups containing the User.
9. Resolve active inheritable Asset permissions along the containment chain.
10. Select the strongest permission or deny the request.
~~~

Requests for another Tenant resource never bypass the tenant predicate. APIs
may return not found rather than forbidden for a resource outside the caller
Tenant to avoid resource enumeration.

## User-Facing Behavior

User resource lists are authorization-aware. They return the union of:

~~~text
resources owned by the authenticated User
resources directly permitted to the authenticated User
resources permitted to a Group containing the authenticated User
resources reached through an inherited Asset permission
~~~

The response includes effective permission and access source so applications
may display owned and shared resources differently without duplicating
authorization policy in the UI.

## Query Performance

Normal single-resource authorization uses indexed lookup of:

~~~text
resource by (tenant_id, id)
direct User permission
active Group memberships and Group permissions
ancestor containment chain
~~~

Resource lists use a single set-based query or CTE with keyset pagination.
They never call an authorization resolver once per returned row. Telemetry
ingest authenticates a Device credential against its Device and Tenant and
does not join User Groups or resource permissions.

The first release uses recursive containment traversal with a depth cap of 64.
If measurements show a bottleneck for deep trees or very large list queries,
add an entity-closure table as an optimization without changing permission
semantics.

Gateway-child checks use the indexed pair of tenant ID and gateway Device ID;
they do not traverse descriptive device relations. Descriptive
device-relation reads use either indexed endpoint and never run during normal
permission checks.

~~~text
entity_closure
  tenant_id
  ancestor_asset_id
  descendant_asset_or_device_id
  depth
~~~

Permission-result caching is deferred. If introduced, its cache key includes
tenant ID, User ID, resource ID, and an authorization revision. Permission,
group-membership, ownership, and containment changes increment the relevant
revision before commit.

## Data Isolation Invariants

1. Every tenant-scoped table has a non-null tenant ID.
2. A User, Group, Asset, Device, and permission subject or scope must share
   the same tenant ID.
3. Tenant Account can only administer its own tenant ID.
4. User Tenant ID cannot change after creation.
5. A containment edge cannot cross tenants or create a cycle.
6. A gateway-child edge cannot cross tenants, point to itself, or make a
   gateway a child.
7. Gateway-child topology has no representation in descriptive device
   relations.
8. A descriptive device relation cannot cross tenants or point to itself.
9. A User Group cannot contain a User from another tenant.
10. A resource permission cannot target a User, Group, Asset, or Device from
   another tenant.
11. Device credentials resolve to exactly one Device and its tenant.

## Migration and Rollout

### Phase 1: Tenant and Identity Boundary

1. Add System Account bootstrap configuration.
2. Add Tenant and one-to-one Tenant Account tables.
3. Add tenant ID to Users, Assets, Devices, tokens, telemetry, alerts,
   dashboards, OAuth clients, and audit records.
4. Create a named initial migration tenant from an explicit deployment
   configuration value. This is migration input only; there is no runtime
   default-tenant registration path.
5. Backfill all existing tenant-scoped rows into that named migration tenant.
6. Reject startup when existing data requires migration but the named migration
   tenant configuration is absent.

### Phase 2: Sharing and Group Authorization

1. Add User Groups and same-tenant membership validation.
2. Add resource permissions and direct User sharing.
3. Add Group sharing.
4. Add Asset containment inheritance.
5. Preserve and tenant-scope existing gateway-child topology.
6. Add descriptive device-to-device relations.
7. Add permission, membership, ownership, containment, gateway, and relation
   audit events.

### Phase 3: Measured Extensions

1. Add group managers when delegation is required.
2. Add entity closure only after query measurements justify it.
3. Add descriptive Asset-to-Device and Asset-to-Asset many-to-many relations
   for real-world cases not represented by containment.
4. Add custom roles or per-capability permissions only when the three-level
   permission model no longer meets a real product requirement.

## Verification Requirements

Tests must prove:

1. System Account can manage tenants and Tenant Accounts but cannot use tenant
   resource APIs.
2. Tenant Account has full access only inside its own tenant.
3. User registration binds a User permanently to the supplied active tenant.
4. Tenant Account cannot create a User in another tenant.
5. User, Group, Asset, Device, containment, and permission cross-tenant writes
   fail.
6. Direct User permission gives access without changing resource ownership.
7. Group membership grants and revokes access immediately.
8. Asset permission with inheritance reaches all containment descendants.
9. Moving a Device changes inherited access but preserves direct permissions.
10. Manager cannot transfer ownership or create owner permissions.
11. Viewer cannot mutate, control, share, revoke, or transfer.
12. List endpoints return only resources authorized for the authenticated User
    and use keyset pagination.
13. Gateway telemetry and commands require an exact active gateway-child pair
    in the same tenant.
14. Gateway-child topology does not grant user access to either endpoint.
15. Device-to-device relations cannot cross tenants and do not grant user
    access or affect permission inheritance.
16. Reassigning a gateway rejects telemetry and commands from the former
    gateway after the topology transaction commits.
17. Deleting a gateway with active children fails until every child is
    detached or reassigned.
18. The generic device-relations API rejects the reserved gateway_child type.
