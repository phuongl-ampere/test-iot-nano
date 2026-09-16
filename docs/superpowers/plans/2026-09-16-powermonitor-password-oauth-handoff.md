# PowerMonitor Password OAuth Handoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a PowerMonitor user submit platform credentials once and reach the dashboard through the existing PKCE OAuth callback.

**Architecture:** A same-origin PowerMonitor POST validates its configured app Origin, sends credentials to the server-only platform login endpoint, then uses the returned platform session only in server-to-server OAuth authorization and token exchange requests. The browser receives only the final PowerMonitor session cookie.

**Tech Stack:** Next.js 16 App Router, React 19, TypeScript, Vitest, existing iot-api OAuth endpoints.

## Global Constraints

- Do not add a Platform Login Page or browser call to the management listener.
- `PLATFORM_AUTH_BASE_URL` is server-only; `PLATFORM_BASE_URL` remains public OAuth/API only.
- Do not expose password, platform session ID, OAuth code, access token, or client secret to browser code, URLs, logs, or UI errors.
- Keep GET `/api/auth/login` behavior unchanged for existing callers.
- POST `/api/auth/login` must reject cross-origin requests, exchange its OAuth code server-side, and only issue the final PowerMonitor session cookie after a successful token exchange.
- iot-api login rate limits must distinguish usernames behind a shared BFF source IP.
- Keep the current compact PowerMonitor visual language.

---

### Task 1: Password-to-OAuth BFF Handoff

**Files:**
- Modify: `apps/powermonitor/lib/oauth.ts`
- Modify: `apps/powermonitor/app/api/auth/login/route.ts`
- Modify: `apps/powermonitor/components/powermonitor-login-gate.tsx`
- Modify: `apps/powermonitor/app/globals.css`
- Modify: `apps/powermonitor/.env.example`
- Create: `apps/powermonitor/tests/password-oauth-handoff.test.ts`
- Create: `apps/powermonitor/tests/password-login-route.test.ts`
- Modify: `apps/powermonitor/tests/login-gate.test.tsx`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs`

**Interfaces:**
- Consumes: platform `POST /api/auth/login` and public `/oauth/authorize`.
- Produces: `createPasswordLoginHandler(...)` in `lib/oauth.ts`.
- Produces: same-origin POST `/api/auth/login` that returns a 303 dashboard redirect on success.

- [x] **Step 1: Write failing behavior tests**

Add tests for:

```ts
it("logs in with iot-api then exchanges the matching PKCE code server-side", async () => {
  // Browser receives only a final powermonitor_session and redirect to /.
});

it("returns a generic credential failure without setting OAuth state", async () => {
  // iot-api 401 must not call /oauth/authorize.
});

it("rejects an authorization redirect with a mismatched state", async () => {
  // callback location state must equal the generated state.
});
```

Update the login gate test to require username/password controls, a POST form
action targeting `/api/auth/login`, and no direct `/api/auth/login` link.

- [x] **Step 2: Run focused tests to verify RED**

Run:

```bash
npm --prefix apps/powermonitor test -- password-oauth-handoff.test.ts login-gate.test.tsx
```

Expected: FAIL because password handoff and form controls do not exist.

- [x] **Step 3: Implement server-only handoff**

Add `PLATFORM_AUTH_BASE_URL` configuration. Implement
`createPasswordLoginHandler` to:

```ts
// 1. POST JSON credentials to new URL("/api/auth/login", authBaseUrl).
// 2. Read only iot_nano_session from the server response Set-Cookie header.
// 3. Generate PKCE state/verifier and call /oauth/authorize with that session
//    in a server-side Cookie header and redirect: "manual".
// 4. Verify callback origin/path and matching state, then exchange its code
//    server-side and set sealed powermonitor_session before a 303 redirect to /.
```

Map bad credentials to a generic local login error; map platform/OAuth failures
to a generic unavailable local login error.

- [x] **Step 4: Wire the compact app login form**

Replace the login link with a same-origin form:

```tsx
<form action="/api/auth/login" method="post">
  <label><span>Username</span><input autoComplete="username" name="username" required /></label>
  <label><span>Password</span><input autoComplete="current-password" name="password" required type="password" /></label>
  <button type="submit">Sign in</button>
</form>
```

Update POST route handling to validate `Origin` against the configured OAuth
redirect origin, read form data, call the handoff helper, and return generic
error redirects. Keep GET unchanged. Update iot-api's login limiter key to
include the submitted username so one BFF source IP cannot throttle all users.

- [x] **Step 5: Run focused tests to verify GREEN**

Run:

```bash
npm --prefix apps/powermonitor test -- password-oauth-handoff.test.ts login-gate.test.tsx oauth-callback.test.ts
```

Expected: all focused tests pass.

- [x] **Step 6: Run package test and production build**

Run:

```bash
npm --prefix apps/powermonitor test
npm --prefix apps/powermonitor run build
```

Expected: all tests and production build pass.

- [x] **Step 7: Verify the live flow**

Configure the local PowerMonitor environment with
`PLATFORM_AUTH_BASE_URL=http://127.0.0.1:18081`. Submit the form in an
isolated browser profile with demo credentials, then verify the browser ends
at `/` with `powermonitor_session`, without an `iot_nano_session` browser
cookie.

- [x] **Step 8: Commit**

```bash
git add apps/powermonitor docs/superpowers
git commit -m "feat(powermonitor): add password oauth handoff"
```
