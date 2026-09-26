# Tenant Profile Tabs Design

## Goal

Expose the existing tenant-scoped Asset Profiles and Device Profiles pages as tabs beside the tenant Profile configuration page.

## Scope

The tenant Profile area has three routes:

- `/tenant/profile` — JSON import, export, and edit for the tenant Profile configuration.
- `/tenant/profiles/asset` — tenant Asset Profile catalog.
- `/tenant/profiles/device` — tenant Device Profile catalog.

Each route renders the same tab strip directly below its page heading. The selected tab has an explicit visual active state and `aria-current="page"`. Asset Profile and Device Profile pages also mark the existing **Profile** sidebar item active, because they are part of that tenant Profile area. A small local CSS component supplies the tab border, horizontal wrapping, keyboard focus treatment, and selected-tab contrast. The shared strip is a template partial so URLs and labels do not drift between the three pages.

## Non-goals

- No change to profile JSON schema, import/export behavior, storage, API routes, permissions, or tenant data.
- No restoration of Application-scoped Profile pages.
- No migration or seed-data changes.

## Verification

Template regression tests must assert that the shared tab partial exposes all three tenant routes and that all three pages include it. The existing monolith template test suite must pass. After rebuilding the local monolith, authenticated tenant requests to all three routes must render the tabs.
