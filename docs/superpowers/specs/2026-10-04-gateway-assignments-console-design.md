# Gateway Assignments Console Design

## Goal

Reduce tenant-console noise by presenting gateway communication assignments
instead of every device's default direct-connectivity state.

## Navigation And Empty State

Keep the existing `/tenant/topology` route and its sidebar entry, but rename
the entry, page title, and visible terminology to **Gateway assignments**.

When the tenant has no gateway devices, the page remains reachable and shows
an empty state: `No gateway assignments`. It explains that ordinary devices
connect directly by default and provides the existing way to designate a
device as a gateway.

## Assigned Gateway View

When gateways exist, the page lists gateway devices only. Each gateway shows
its assigned child devices and keeps the existing assignment and detachment
actions available.

Devices that are not gateways and have no `gateway_device_id` are omitted;
they remain ordinary devices managed from the Devices page. A gateway with no
children remains visible so a tenant can assign children to it.

`Direct` is removed from the displayed topology table. No persistence,
authorization, MQTT routing, gateway topology validation, or endpoint route
changes are part of this work.

## Scope And Verification

This change is limited to the tenant topology page's presentation model,
template copy, and UI tests. Existing gateway-child assignment and detachment
forms retain their current behavior.

Tests verify the renamed navigation and heading, the no-gateway empty state,
the omission of direct devices, and visibility of gateways with and without
children.
