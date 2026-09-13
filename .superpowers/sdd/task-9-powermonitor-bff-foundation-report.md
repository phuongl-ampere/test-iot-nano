# Task 9a PowerMonitor External BFF Foundation Report

## Status

Implemented and committed as a standalone `apps/powermonitor` Next.js
application foundation.

The scope is intentionally limited to the Task 9a BFF foundation. The
existing `web` PowerMonitor UI and its platform-specific routes were not
moved or modified because the brief explicitly prohibits touching `web/` and
does not authorize the UI extraction work from the broader Task 9 plan.

## Read First

- `.superpowers/sdd/task-9-powermonitor-bff-foundation-brief.md`
- `web/AGENTS.md`
- `web/node_modules/next/dist/docs/01-app/01-getting-started/15-route-handlers.md`
- `web/node_modules/next/dist/docs/01-app/02-guides/backend-for-frontend.md`
- `web/node_modules/next/dist/docs/01-app/02-guides/server-and-client-boundary.md`
- `web/node_modules/next/dist/docs/01-app/02-guides/environment-variables.md`
- `web/node_modules/next/dist/docs/01-app/02-guides/authentication.md`

## Implementation

Added:

- Independent Next.js 16.3.4 app package and App Router shell.
- `GET /api/auth/login` route that creates a random OAuth state and PKCE
  verifier, computes an S256 challenge, stores encrypted state in an
  HttpOnly/Secure/SameSite=Lax cookie, and redirects to `/oauth/authorize`.
- `GET /api/auth/callback` route that validates the state cookie, exchanges
  the authorization code server-side at `/oauth/token`, encrypts the opaque
  token payload into an application session cookie, clears the one-time state
  cookie, and redirects to `/`.
- AES-256-GCM state/session sealing keyed by the server-only `SESSION_SECRET`.
- Server-only generic platform client that constructs requests exclusively as
  `PLATFORM_BASE_URL + /api/v1 + relative path`, sends the session bearer
  token, disables caching, and converts denied responses into a bounded
  `PlatformApiError`.
- `.env.example` covering platform URL, OAuth configuration, and session
  secret.
- Static isolation test rejecting direct storage/internal-platform coupling
  patterns and workspace imports in application source.

No browser-facing module imports OAuth secrets or platform access tokens. No
platform workspace package, database, storage path, Docker, Compose, or
existing web application file was changed.

## TDD Evidence

1. Wrote callback, platform-client, and isolation tests before production
   modules.
2. Ran `npm --prefix apps/powermonitor test`; it failed because the new
   package did not yet exist.
3. Added the minimal app and server modules.
4. Fixed the test harness and ran the suite through focused failures:
   platform URL object assertion and a runner dependency mismatch were
   corrected.
5. Final test suite passed: 3 files and 5 tests.

## Verification

- `npm --prefix apps/powermonitor test`
  - Passed: 3 test files, 5 tests.
- `npm --prefix apps/powermonitor run build`
  - Passed: Next.js production build.
  - Routes generated: `/`, `/api/auth/login`, `/api/auth/callback`.
- `git diff --check`
  - Passed with no whitespace errors.
- Static source scan for `DATABASE_URL`, `postgres://`, `sqlite`,
  `IOT_NANO_INTERNAL_DIR`, and `/internal/`
  - No matches in `apps/powermonitor/app` or `apps/powermonitor/lib`.

## Concerns / Follow-up

- This is the BFF foundation only. Generic browser data/command proxy routes
  and extraction of the existing PowerMonitor UI remain for a later task.
- The encrypted session cookie is a foundation-level session container; a
  production deployment should add rotation/revocation and refresh-token
  lifecycle handling when those requirements are finalized.
- Cookies are always marked `Secure` as required. Local HTTP development needs
  HTTPS or an explicit local proxy.

## Commit

Commit: `feat: add powermonitor external bff foundation`

## Review Fixes

### Path Traversal Regression

RED:

- Added a `platformRequest("/../internal", ...)` regression with a mock
  fetcher.
- `npm --prefix apps/powermonitor test -- tests/platform-client.test.ts`
  failed because the path reached the fetcher and then failed while reading
  the undefined mock response.

GREEN:

- `platformRequest` now checks the normalized URL pathname remains
  `/api/v1` or under `/api/v1/` before it invokes fetch.
- The targeted test passed with 3 tests, including the assertion that fetch
  was never called.

### Proxied OAuth Callback Regression

RED:

- Added a callback request with `proxy.example.test` and a configured
  `powermonitor.example.test` callback URI.
- `npm --prefix apps/powermonitor test -- tests/oauth-callback.test.ts`
  failed because the token exchange received the proxy origin.

GREEN:

- `createCallbackHandler` now accepts the configured redirect URI, uses it
  for token exchange, and constructs the post-login redirect from it.
- The Next.js callback route passes `config.redirectUri` from
  `OAUTH_REDIRECT_URI`.
- The targeted test passed with 3 tests, including the proxy-origin
  regression.
