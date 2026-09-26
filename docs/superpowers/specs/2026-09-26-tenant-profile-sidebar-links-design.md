# Tenant Profile Sidebar Links Design

## Goal

Restore direct tenant-console sidebar access to Asset Profiles and Device Profiles
without removing the existing Profile Configuration page or its in-page tabs.

## Navigation

Add a new `Profiles` section between `Topology` and `Access` in the tenant
sidebar:

- `Profile Configuration` links to `/tenant/profile`.
- `Asset Profiles` links to `/tenant/profiles/asset`.
- `Device Profiles` links to `/tenant/profiles/device`.

The existing Access section remains unchanged.

## Active State

Each profile page passes a distinct navigation key to the shared tenant
navigation template:

- `profile-configuration`
- `asset-profiles`
- `device-profiles`

Only the current direct sidebar link is active. The existing profile-tab
partial remains on all three pages as a compact local switcher.

## Scope And Verification

This change only modifies tenant-console navigation templates and the
corresponding template assertions. It does not change profile APIs,
authorization, profile data, PowerMonitor, or seed behavior.

Template tests verify the Profiles section, all three routes, and their
page-specific active navigation keys.

