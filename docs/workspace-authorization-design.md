# Simple User-Owned Resource Sharing Design

## Status

Implemented foundation:

```text
- account_class = system | admin | user
- user-owned assets and devices
- direct and inherited asset/device sharing
- manager/controller/viewer permission checks
- internal username invitation lifecycle
- audited share, claim, owned provisioning, and assignment actions
- safe one-time device claims
```

Ownership transfer, group sharing, and dedicated sharing screens remain future
product work. The backend data model and resource permission resolver support
those additions without introducing workspace or customer entities.

## Goal

Provide a simple ownership and sharing model for the current platform without
introducing workspace, customer, organization, group, or custom-role entities.

The primary user flow is:

```text
User A claims Device D
  -> A owns Device D

User A creates Asset X
  -> A owns Asset X

User A puts Device D into Asset X
  -> D remains owned by A and belongs to Asset X

User A shares Asset X or Device D with User B
  -> B accepts an internal username invitation
```

## Core Model

```text
User A
  owns Device D
  owns Asset X
  puts Device D into Asset X
  shares Asset X or Device D with User B
```

Users are global login identities. Assets and devices are user-owned resources.
There is no customer or workspace boundary in the first implementation.

The core does not define semantic asset kinds such as Home, Room, Floor, Site,
Panel, or Equipment. An application may label an asset as "Home" through an
asset profile or metadata, but authorization treats every asset identically.

## Platform Account Classes

Platform account class is separate from resource permission. The account class
answers which platform API surface a principal may enter. Resource permission
answers which user-owned asset or device a normal user may access.

```text
system
  -> infrastructure and system configuration only

admin
  -> all data and resource APIs
  -> never system-only infrastructure APIs

user
  -> only owned and shared assets/devices
```

```text
users
  id
  username
  account_class        system | admin | user
```

### System Account

The `system` account is for platform operations only:

```text
SMTP configuration
MQTT broker configuration
stream partitions, retention, and segment tuning
database/storage settings
infrastructure health and maintenance operations
platform-wide operational audit
```

It must not be used as a normal device owner or resource-share recipient.
System-only endpoints require `account_class = system`. A normal `admin`
receives `403` from these endpoints.

### Admin Account

The `admin` account is a platform data administrator:

```text
read and manage all users
read and manage all devices and assets
read and manage profiles, alerts, tokens, and RPC
resolve ownership and sharing support cases
```

Admin bypasses normal owner/share checks for resource APIs, but cannot enter
system-only SMTP, MQTT, stream, data storage, or infrastructure configuration
APIs.

### User Account

The `user` account follows the owner/share model in this document:

```text
owned device/asset
direct device share
inherited asset share
resolved viewer/controller/manager/owner permission
```

Users cannot access administration or system configuration APIs.

## Data Model

```text
devices
  device_id
  owner_user_id nullable references users(id)
  asset_id nullable references assets(id)
  claimed_at nullable

assets
  id
  owner_user_id nullable references users(id)
  parent_asset_id nullable references assets(id)
  asset_profile_id nullable
  metadata

resource_shares
  id
  resource_type         asset | device
  resource_id
  target_user_id references users(id)
  permission            viewer | controller | manager
  inherit_children      boolean
  state                 pending | active | declined | cancelled | expired
  created_by_user_id references users(id)
  created_at
  responded_at nullable
  expires_at nullable

unique_pending_resource_share
  (resource_type, resource_id, target_user_id) WHERE state = pending

device_claim_codes
  device_id primary key
  code_hash
  expires_at
  issued_by_user_id
  issued_at
  used_at nullable

audit_events
  actor_user_id
  actor_account_class
  resource_type
  resource_id
  action
  before_value
  after_value
  request_id
  created_at
```

`resource_shares` is both the internal invitation record and, after acceptance,
the active permission grant. A separate invitation table is intentionally not
needed in this version.

## Ownership

### Claim Device

When A claims an unclaimed device:

```text
Device D.owner_user_id = A
Device D.asset_id = null
Device D.claimed_at = now
```

Only the owner can initially see or manage the device.

### Safe Claim Code

An administrator may prepare an unowned device for a user without assigning
ownership manually:

```text
POST /api/management/devices/{device_id}/claim-code
  -> returns a one-time claim code, default expiry 24 hours

POST /api/device-claims
  body: { device_id, claim_code }
  -> assigns owner_user_id to the logged-in normal user
```

The database stores only `code_hash`. Claim is transactional: the device must
still be unowned, the code must be unexpired and unused, then `used_at` is
written together with `owner_user_id` and `claimed_at`.

### Create Asset

When A creates Asset X:

```text
Asset X.owner_user_id = A
```

When A places Device D in Asset X:

```text
Device D.asset_id = Asset X
```

The device owner does not change when it is moved into an asset. Ownership
transfer is an explicit privileged operation.

## Permissions

| Permission level | Read | Relay RPC | Device config | Token rotate | Share | Delete / transfer |
|---|---:|---:|---:|---:|---:|---:|
| `viewer` | Yes | No | No | No | No | No |
| `controller` | Yes | Yes | No | No | No | No |
| `manager` | Yes | Yes | Yes | Yes | Yes | No |
| `owner` | Yes | Yes | Yes | Yes | Yes | Yes |

The resource owner automatically has `owner` permission. A share may grant
only `viewer`, `controller`, or `manager`.

## Share Asset and Descendants

A enters an existing platform username B and chooses `viewer`:

```text
resource_share
  resource_type = asset
  resource_id = Asset X
  target_user_id = B
  permission = viewer
  inherit_children = true
  state = pending
```

B opens the internal `Shared with me` screen:

```text
Accept  -> resource_share.state = active
Decline -> resource_share.state = declined
```

When active, B can see:

```text
Asset X
  -> all child assets
  -> all devices below Asset X
  -> telemetry, charts, and alerts
```

B cannot see relay controls, edit configuration, rotate tokens, or change
sharing because the permission is `viewer`.

## Share One Device

A may share Device D without sharing its parent asset:

```text
resource_share
  resource_type = device
  resource_id = D
  target_user_id = B
  permission = controller
  inherit_children = false
  state = pending
```

After acceptance, B sees Device D in a `Shared with me` list. B does not need
to see the parent asset or other devices below it.

`controller` allows B to send relay on/off and two-way RPC commands, but not
to rotate the MQTT token or reconfigure Device D.

## Authorization Evaluation

When user B requests an action against Device D, the backend evaluates:

```text
1. Is the API system-only?
   -> require system account

2. Is the API resource/data API and principal an admin?
   -> allow platform admin access

3. Is B the owner of Device D?
   -> allow owner permissions

4. Does B have an active direct device share for Device D?
   -> use the direct share permission

5. Does D belong to an asset for which B has an active asset share?
   -> if inherit_children = true, use the asset share permission

6. Does an ancestor asset of D have an active inherited share for B?
   -> use the nearest or strongest applicable share permission

7. Otherwise
   -> deny with HTTP 403
```

Permission ranking is:

```text
owner > manager > controller > viewer > deny
```

The server must enforce this check for every read and mutation. Hiding a UI
button is not authorization.

## Asset Inheritance

Assets may form a normal hierarchy:

```text
Asset A
  -> Asset B
      -> Asset C
          -> Device D
```

An inherited share on an asset applies to all child assets and devices. The first
implementation may resolve ancestry recursively from `parent_asset_id`. Add an
asset closure table only if hierarchy queries become a measured performance
bottleneck.

## Internal Username Invitation

Invitation is internal only:

```text
POST /api/assets/{asset_id}/shares
POST /api/devices/{device_id}/shares
  body: { username, permission, inherit_children }

GET /api/me/resource-shares?state=pending

POST /api/resource-shares/{id}/accept

DELETE /api/resource-shares/{id}
```

Owned provisioning APIs:

```text
POST /api/my/assets
POST /api/my/devices
PUT  /api/my/devices/{device_id}/asset
```

Rules:

```text
- Username must resolve to an existing active user.
- The owner or manager creates the pending share.
- The target user can list, accept, or decline only their own shares.
- The resource owner or manager can cancel a pending or active share.
- A user cannot be invited if they already own the resource.
- Only one pending share exists for each target user and resource.
- Accept must atomically validate state=pending and set state=active.
```

Email notification is optional product messaging. It is not part of the
authorization flow.

## UI Model

Owner A sees:

```text
My devices
My assets
Manage sharing
```

User B sees:

```text
Shared with me
  -> Assets shared to B
  -> Direct devices shared to B
```

The app determines available controls from the resolved permission:

```text
viewer      -> telemetry and alert views only
controller  -> relay controls and two-way RPC
manager     -> controller controls plus config/token/share
owner       -> all controls
```

Platform account navigation is separate:

```text
system -> System / Infrastructure configuration only
admin  -> Full management and all resource data, no System configuration
user   -> My devices, My assets, Shared with me
```

## Device Tokens

Device MQTT tokens remain technical credentials:

```text
device token -> device
```

They are never shared with User B. User authorization is always evaluated from
the logged-in user and active ownership/share records.

## Audit

Write audit events for:

```text
device claim
ownership transfer
asset/device share create
share accept, decline, cancel
permission change
token rotate or revoke
device configuration change
relay RPC request and result
```

Each event includes:

```text
actor_user_id
actor_account_class
resource_type
resource_id
action
before_value
after_value
request_id
created_at
```

## Rollout

### Phase 1

```text
owner_user_id on devices and assets
device claim
direct device sharing
viewer / controller / manager / owner permission checks
internal username invitations
```

### Phase 2

```text
asset sharing
asset inheritance to child assets and devices
Shared with me UI
ownership transfer
audit log
```

### Future Expansion

When real requirements require teams, enterprise customers, or billing,
introduce groups and workspaces around the same primitives:

```text
direct user share -> group share
owner user -> workspace owner
asset/device share -> scoped workspace role binding
```

The existing `resource_shares` records can be migrated into generalized role
bindings instead of discarded.
