# Tenant Profile Configuration Design

## Goal

Move domain-profile ownership from applications to tenants. Each tenant owns
one replaceable profile configuration that can be edited, exported, and
imported as JSON. A tenant represents one application for this domain.

## Scope

The tenant Profile page is the only UI for the configuration package. It
provides JSON editing, download/export, and upload/import. It is not rendered
inside an Application page.

The configuration package contains:

- a required format version;
- asset and device domain-profile definitions;
- levels, hierarchy, and inheritance information represented by those
  definitions;
- asset containment rules; and
- permission definitions.

The package never contains tenant resources or operating data: devices,
assets, users, credentials, assignments, telemetry, alerts, or audit history.

Asset Profile and Device Profile remain separate tenant-level tabs and keep
their existing CRUD behavior. They are not moved under Application and are not
removed by this change.

## Ownership and Runtime Rules

The domain-profile catalog, containment rules, profile assignments, management
APIs, and public runtime lookups are tenant-scoped. No route, repository
method, schema key, or runtime lookup may require an `app_id` for this
configuration.

The legacy Application Domain page and its application-scoped management
routes are removed. The tenant Profile page uses the authenticated tenant ID
as its sole scope.

## Import and Export Contract

Export returns a self-contained, versioned JSON document for the current
tenant's profile configuration.

Import accepts exactly one JSON document, validates its shape and all
cross-references before persisting anything, then atomically replaces the
tenant's profile configuration. A valid import removes the previous profile
catalog and containment rules for that tenant; preservation or merge behavior
is intentionally out of scope. If validation or persistence fails, the tenant
configuration remains unchanged.

Profile references in ordinary tenant data are outside the package. The import
operation must reject a replacement that would leave a persisted resource with
an invalid profile reference, or explicitly clear such references within the
same transaction if that is the established storage contract. The chosen
behavior must be consistent across SQLite and Timescale/PostgreSQL.

## Error Handling and Security

Only an authenticated tenant account with the existing tenant-mutation
authorization can edit or import a profile. Export is limited to the current
tenant. Invalid JSON, an unsupported format version, duplicate names or IDs,
invalid resource kinds, malformed definitions, invalid inheritance or
containment references, and forbidden permission definitions return a clear
validation error without partial writes.

## Verification

Tests cover tenant isolation, export document shape, valid replacement import,
rejection with rollback on invalid input, authorization, removal of
application-scoped paths, preservation of the Asset Profile and Device Profile
tabs, and runtime tenant-scoped profile lookup.
