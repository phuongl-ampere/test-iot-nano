# PowerMonitor OAuth Login Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Render a simple PowerMonitor OAuth sign-in gate before any dashboard
or detail page is exposed to an unauthenticated browser.

**Architecture:** Reuse the existing sealed PowerMonitor OAuth session cookie.
A server-only helper reads the cookie for each browser entry page. When no
valid session exists, those pages render one small login component whose only
action starts the existing OAuth BFF route. Authenticated pages keep rendering
the existing dashboard unchanged.

**Tech Stack:** Next.js 16 App Router, React 19, TypeScript, Vitest, React
Testing Library, existing OAuth BFF helpers.

## Global Constraints

- Do not add a username/password form, new OAuth grant, or platform API route.
- Do not expose a client secret, token, database URL, or internal path to
  browser code.
- Keep GET /api/auth/login and GET /api/auth/callback behavior unchanged.
- Gate /, /devices/[deviceId], and /assets/[assetId] on the same sealed
  PowerMonitor session.
- Reuse the current PowerMonitor palette and keep the sign-in surface compact.

---

### Task 1: Gate Browser Pages With Existing OAuth Session

**Files:**
- Create: apps/powermonitor/lib/page-session.ts
- Create: apps/powermonitor/components/powermonitor-login-gate.tsx
- Create: apps/powermonitor/tests/login-gate.test.tsx
- Modify: apps/powermonitor/app/page.tsx
- Modify: apps/powermonitor/app/devices/[deviceId]/page.tsx
- Modify: apps/powermonitor/app/assets/[assetId]/page.tsx
- Modify: apps/powermonitor/app/globals.css

**Interfaces:**
- Consumes: sessionCookieName and readSession(value) from lib/oauth.ts.
- Produces: hasPowerMonitorSession(): Promise<boolean>.
- Produces: PowerMonitorLoginGate, whose primary action targets
  /api/auth/login.

- [x] **Step 1: Write the failing component test**

Create apps/powermonitor/tests/login-gate.test.tsx:

~~~tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { PowerMonitorLoginGate } from "../components/powermonitor-login-gate";

describe("PowerMonitorLoginGate", () => {
  it("starts the existing OAuth BFF route without rendering a password form", () => {
    render(<PowerMonitorLoginGate />);

    expect(screen.getByRole("link", { name: "Continue to sign in" }).getAttribute("href"))
      .toBe("/api/auth/login");
    expect(screen.queryByLabelText(/password/i)).toBeNull();
  });
});
~~~

- [x] **Step 2: Run test to verify RED**

Run:

~~~bash
npm --prefix apps/powermonitor test -- login-gate.test.tsx
~~~

Expected: FAIL because PowerMonitorLoginGate does not exist.

- [x] **Step 3: Add the server session helper and login gate**

Create lib/page-session.ts:

~~~ts
import { cookies } from "next/headers";

import { readSession, sessionCookieName } from "./oauth";

export async function hasPowerMonitorSession(): Promise<boolean> {
  const cookieStore = await cookies();
  return readSession(cookieStore.get(sessionCookieName)?.value) !== null;
}
~~~

Create components/powermonitor-login-gate.tsx:

~~~tsx
export function PowerMonitorLoginGate() {
  return (
    <main className="powermonitor-login">
      <section aria-labelledby="powermonitor-login-title" className="powermonitor-login-panel">
        <span aria-hidden="true" className="brand-mark">P</span>
        <p className="eyebrow">Power Monitor</p>
        <h1 id="powermonitor-login-title">Sign in to view your energy operations</h1>
        <p>Use your authorized platform session to continue.</p>
        <a className="powermonitor-login-action" href="/api/auth/login">Continue to sign in</a>
      </section>
    </main>
  );
}
~~~

- [x] **Step 4: Gate all browser entry pages**

Replace each page body with the same server-side branch before rendering the
existing dashboard:

~~~tsx
if (!(await hasPowerMonitorSession())) {
  return <PowerMonitorLoginGate />;
}
~~~

Preserve initialDeviceId and initialAssetId for authenticated detail pages.

- [x] **Step 5: Add compact login styles**

Append focused CSS:

~~~css
.powermonitor-login {
  display: grid;
  min-height: 100vh;
  place-items: center;
  padding: 24px;
}

.powermonitor-login-panel {
  width: min(100%, 420px);
  padding: 32px;
  border: 1px solid #c7d8da;
  border-radius: 6px;
  background: #f7fbfb;
}

.powermonitor-login-action {
  display: inline-flex;
  min-height: 40px;
  align-items: center;
  padding: 10px 14px;
  border-radius: 5px;
  background: #167b83;
  color: #fff;
  font-weight: 700;
  text-decoration: none;
}
~~~

- [x] **Step 6: Run focused tests and verify GREEN**

Run:

~~~bash
npm --prefix apps/powermonitor test -- login-gate.test.tsx
npm --prefix apps/powermonitor test -- oauth-callback.test.ts
~~~

Expected: both test files pass.

- [x] **Step 7: Run package test and production build**

Run:

~~~bash
npm --prefix apps/powermonitor test
npm --prefix apps/powermonitor run build
~~~

Expected: all tests and the production build pass.

- [x] **Step 8: Commit**

~~~bash
git add apps/powermonitor docs/superpowers/plans/2026-09-16-powermonitor-oauth-login-gate.md
git commit -m "feat(powermonitor): add oauth login gate"
~~~
