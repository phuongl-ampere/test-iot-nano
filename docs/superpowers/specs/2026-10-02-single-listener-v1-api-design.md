# IoT Nano single-listener API migration

## Goal

Make `:18081` the only IoT Nano HTTP listener. It will serve OAuth and every
versioned Platform API. Remove `:18080` and every legacy unversioned API path
without compatibility aliases because this is an early-development breaking
change.

## Public routing contract

The single Platform HTTP origin is `http://127.0.0.1:18081` in local
development. Production uses the same one-origin topology with its configured
host and HTTPS setting.

| Contract | Path at the single origin | Notes |
| --- | --- | --- |
| OAuth authorization and token exchange | `/oauth/*` | Kept outside `/api/v1` because it is a standard OAuth protocol endpoint and registered clients use these paths. |
| Public resource API | `/api/v1/*` | Existing bearer-token asset, device, telemetry, command, profile, alert, and invitation API. |
| Management session and administration API | `/api/v1/auth/*`, `/api/v1/system/*`, `/api/v1/tenant/*`, `/api/v1/user/*`, `/api/v1/management/*` | Renamed from the current `/api/*` paths. |
| Management HTML, static assets, OpenAPI/Swagger, and health | Existing non-API web/operational paths | These are not versioned JSON APIs and remain on the same `:18081` origin. |

`http://127.0.0.1:18080` is not bound. It does not redirect or proxy requests;
clients receive a connection failure. The former `/api/*` management paths are
removed rather than redirected and return 404 from the new listener.

## Platform runtime and configuration

`MonolithConfig` will replace the two HTTP fields (`public_http` and
`management_http`) with one `http` address. The runtime will replace the old
`IOT_NANO_PUBLIC_HTTP_ADDRESS` and `IOT_NANO_MANAGEMENT_ADDRESS` settings with
one `IOT_NANO_HTTP_ADDRESS`, defaulting to the current management port `18081`.
The retired settings are invalid configuration, not aliases.

At startup, the runtime builds one router by merging the health router, OTA
router, public `/api/v1` router, public OAuth router, and management-session
router. It binds that router once at `config.http`. Runtime health/status data
reports one HTTP listener and removes the obsolete public-vs-management
distinction. MQTT listeners are unchanged.

The management router retains its HTML form/page routes but renames all of its
JSON routes from `/api/...` to the paths in the routing table. Its authentication
and authorization mechanisms remain unchanged: management routes keep their
session checks, public resources keep bearer-token scope checks, and OAuth
continues to use its protocol-specific validation.

## Power Monitor

Power Monitor's development configuration sets both `PLATFORM_BASE_URL` and
`PLATFORM_AUTH_BASE_URL` to `http://127.0.0.1:18081`.

Its existing data BFF remains `:3002/api/v1/[...path]` and therefore forwards
only to `:18081/api/v1/*`. Its app-owned auth helpers move from
`:3002/api/auth/*` to `:3002/api/v1/auth/*` so all Power Monitor HTTP API
routes are versioned. Static auth handlers take precedence over the existing
catch-all BFF route. OAuth client redirect URIs, forms, callback tests, and
session/logout links update to the new `/api/v1/auth/*` paths.

## Failure behaviour and scope

- No HTTP fallback, proxy, redirect, compatibility environment variable, or
  legacy route is retained for `:18080` or non-versioned API paths.
- Browser traffic still reaches Power Monitor at `:3002`; its server-side BFF
  reaches the Platform only at `:18081`.
- This migration does not change MQTT ports, OAuth grants/scopes, storage,
  resource authorization, management HTML navigation, or telemetry logic.

## Verification

Tests will first demonstrate the desired single-listener contract and fail
against the two-listener implementation. They will then verify that `:18081`
serves OAuth, public `/api/v1` routes, and management routes at their new v1
paths; `:18080` is not bound; legacy management API paths are absent; and the
Power Monitor BFF completes login, logout, callback, and resource requests
using only `:18081`. The relevant Rust tests, Power Monitor Vitest suite, and
Power Monitor production build must pass.
