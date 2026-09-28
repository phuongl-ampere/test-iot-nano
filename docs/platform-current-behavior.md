# IoT Nano current platform behavior

This is the current behavioral reference for the monolith, Tenant Console, User
Workspace, and PowerMonitor. It describes implemented behavior; historical
documents under `docs/superpowers/specs/` are design records and may describe
an earlier stage.

## Identity scopes

| Account | Scope | Primary responsibility |
| --- | --- | --- |
| System Account | Platform | Creates and manages tenant lifecycle; configures generated serial length. |
| Tenant Account | One tenant | Creates users and tenant resources; assigns a resource owner; manages tenant pairing policy and profiles. |
| User | One tenant | Uses resources they own or that an owner has shared with them. |

System Account access does not grant access to a tenant's assets, devices, or
telemetry. Tenant Account access does not make the Tenant Account a resource
owner.

## Tenant creation defaults

Creating a tenant persists an active tenant record and its first Tenant Account.
It does not create users, assets, devices, profiles, applications, direct
resource permissions, or demo fixtures.

The effective device-claim policy defaults to enabled, a 900-second lifetime,
six numeric digits, five failed attempts, and a 30-second request cooldown.
The policy may be changed by the Tenant Account.

## New regular users

A User created by the Tenant Account is a `viewer` / `user` account and receives
these capabilities:

- `create_assets`
- `claim_devices`
- `control_devices`
- `share_owned_resources`

The user starts with no owned Asset or Device and no direct permission on any
specific resource. They cannot see or operate resources owned by another user
until they own or receive access to one.

The following capabilities are not granted by default:

- `create_devices`
- `edit_resources`
- `manage_device_tokens`
- `assign_application_profiles`

Tenant Account can replace a User's capability set from the Users page.

## Capabilities versus resource access

Authorization has two gates:

1. A capability answers **what kind of action** a User may attempt.
2. Ownership or a share answers **which exact resource** the User may use.

For example, `control_devices` alone does not expose every device in the
tenant. A user needs both `control_devices` and ownership or `control` access
to the specific device before a command can be sent.

## Ownership and sharing

Tenant Account assigns one regular User as the owner of an Asset or Device.
Changing or clearing that owner revokes existing shares for that resource.

An owner may invite another User to a resource. The recipient must accept the
invitation before the share becomes active.

| Resource | Share levels | Meaning |
| --- | --- | --- |
| Device | `view`, `control` | `control` allows commands when the recipient also has `control_devices`. It does not allow configuration, token management, ownership transfer, or re-sharing. |
| Asset | `view`, `manager` | `manager` allows asset management operations when the recipient also has `edit_resources`. It does not transfer ownership or allow deletion. |

Only the owner may delete an Asset. `share_owned_resources` permits sharing a
resource the User owns; it does not let a shared recipient re-share it.

## Serial numbers and device creation

`device_id` is the internal logical identifier used by telemetry, relations,
and management routes. `serial_number` identifies the physical device.

- A serial number is unique case-insensitively within its tenant.
- Existing or legacy records may have no serial number in persistence.
- Tenant Console requires a serial number when provisioning a Device.
- Tenant Console provides **Generate serial** beside the field so the generated
  value is visible and can be reviewed before creation.
- System Account configures the generated serial length from 6 to 32
  characters; the default is 9.
- Pairing by serial works only for a Device that has a serial number.

The management and public device APIs return `serial_number` when present.

## Device claiming and pairing

Manual pairing identifies a Device by `serial_number`, not `device_id`.

1. Tenant Account enables pairing and generates a short-lived code for an
   unowned direct Device.
2. The console returns the six-digit code and a QR payload of the form
   `iotnano://claim?serial_number=...&code=...`.
3. A User enters the serial and code, or PowerMonitor scans a QR image to fill
   both values.
4. A successful claim makes that User the Device owner and consumes the code.

Generating a new code revokes the prior active code. Codes are stored hashed
and are not returned by ordinary list or resource APIs.

## Console layout behavior

The console content area grows to the available desktop width. Data tables fill
their panel width on large screens. On narrow screens, tables stay inside a
horizontal `table-scroll` region instead of expanding the page beyond the
viewport.
