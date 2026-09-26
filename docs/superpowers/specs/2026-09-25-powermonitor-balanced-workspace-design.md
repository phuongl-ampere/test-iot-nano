# PowerMonitor Balanced Workspace Design

## Goal

Make the PowerMonitor workspace quicker to scan and operate without removing resource, telemetry, control, invitation, or edit capabilities.

## Scope

- Change only the PowerMonitor frontend.
- Keep the existing API contracts and polling behavior.
- Keep resource configuration in the existing Edit drawer.
- Keep live control on the selected device view.

## Information Architecture

The left rail is the resource navigator. It contains a compact product mark, one `Assets` heading, concise fleet counts, and an asset tree. It is not a second dashboard.

The main workspace is the selected resource view. Its header has the resource breadcrumb, name, current device status when applicable, and the actions needed at the current level: invitations, edit, refresh, and sign out. Resource IDs remain secondary metadata rather than headline content.

Telemetry is the primary workspace content. A resource with a live-view profile renders only the profile-defined charts. A resource without one renders the raw telemetry table. Device controls and command delivery remain below the telemetry view; the command response appears only after a command completes.

## Resource Tree

- Assets are expandable/collapsible nodes with a dedicated asset icon and child count.
- Nested asset and device rows use a restrained connector line to communicate parentage.
- Devices display a connection dot beside their name; no status prose is repeated in the row.
- The selected node has one accent rail and a quiet surface fill.
- `Unassigned` is a distinct, collapsed group at the end of the tree.
- The tree remains keyboard operable and uses semantic buttons with labels.

## Visual Direction

The fixed dark PowerMonitor theme remains. The UI uses a compact operational layout with thin dividers, disciplined spacing, and 4-6px radii. Teal signals active selection and action, green signals connected devices, amber remains the concise warning color, and red is reserved for destructive/error states.

Text is functional only. Remove duplicate product labels, generic section copy, and repeated state labels. Empty states state the next action plainly. Actions use concise labels; only clear commands retain text labels.

## Responsive Behavior

Desktop keeps the persistent resource rail. At narrow widths, the resource rail becomes a compact top navigator; the selected resource remains visible before telemetry. Charts and tables preserve their minimum readable widths without horizontal page overflow.

## Verification

- Tree rendering tests cover hierarchy, collapsed state, selected resource, and unassigned devices.
- Dashboard tests cover selected-resource rendering and existing polling/command behavior.
- Run focused PowerMonitor tests and inspect desktop plus narrow mobile rendering before completion.

## Non-Goals

- No API, backend, data-model, or permission changes.
- No new dashboard widgets or profile formats.
- No removal of current Edit drawer capabilities.
