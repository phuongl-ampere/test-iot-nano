# Local Development Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document a safe, complete local workflow that starts the monolith, seeds PowerMonitor demo data, starts the PowerMonitor UI, and verifies the result.

**Architecture:** Add `docs/local-development.md` as the single operational guide for a developer's disposable local environment. Keep `README.md` as a short entry point, retain the production monolith runbook as the production authority, and explicitly retire the old four-service runbook as operational guidance.

**Tech Stack:** Markdown, Bash, Cargo, Node.js/npm, OpenSSL, existing local runtime and seed scripts.

## Global Constraints

- Document the current single-process `iot-nano-monolith` topology only; do not provide four-service startup instructions.
- Do not modify runtime, seed, Compose, application, or test behavior.
- Never put real credentials, vault keys, private keys, or certificates in tracked documentation.
- Use the default local endpoints from `scripts/dev/local-platform-runtime.sh`: public HTTP `127.0.0.1:18080`, management `127.0.0.1:18081`, MQTT `127.0.0.1:18883`, MQTT TLS `127.0.0.1:18884`, and PowerMonitor `http://localhost:3002`.
- State that `seed-local-platform.sh --reset` requires `IOT_NANO_ALLOW_LOCAL_SEED=1`, resets only disposable state, preserves local TLS/vault material, and invalidates browser sessions.

---

### Task 1: Add the canonical local-development guide

**Files:**
- Create: `docs/local-development.md`
- Read: `README.md:86-106`
- Read: `apps/powermonitor/.env.example:1-10`
- Read: `infra/monolith/local-platform-seed.env.example:1-15`
- Read: `scripts/dev/local-platform-runtime.sh:20-34,254-359`
- Read: `scripts/dev/seed-local-platform.sh:1-85`

**Interfaces:**
- Consumes: `infra/monolith/local-platform-seed.env.example`, `apps/powermonitor/.env.example`, `scripts/dev/seed-local-platform.sh --reset`, and `npm run dev -- --port 3002`.
- Produces: one ordered, safe setup guide that a developer can follow without reading scripts.

- [ ] **Step 1: Record exact local inputs and commands**

Read the listed templates and scripts. Capture only documented placeholders and defaults:

```bash
IOT_NANO_ALLOW_LOCAL_SEED=1 ./scripts/dev/seed-local-platform.sh --reset
cd apps/powermonitor && npm ci && npm run dev -- --port 3002
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
curl --fail http://localhost:3002/
```

Confirm that the reset command starts the local monolith and does not start the PowerMonitor development server.

- [ ] **Step 2: Write the guide**

Create `docs/local-development.md` with these sections in order:

```markdown
# Local Development: Monolith, PowerMonitor, and Demo Data

## What This Starts
## Prerequisites
## 1. Create Local Runtime Material
## 2. Configure Demo Seed Credentials
## 3. Configure and Install PowerMonitor
## 4. Reset and Seed the Local Monolith
## 5. Start PowerMonitor
## 6. Verify and Sign In
## Reset Scope and Troubleshooting
## Production Boundary
```

Use generated placeholder-only vault and self-signed TLS commands. Explain that the developer must replace the seed credentials and local platform URLs in the two ignored environment files, delete `OAUTH_CLIENT_SECRET` because the seeded client is public, and generate `SESSION_SECRET`. Include the four seeded access cases, health commands, UI URL, listener addresses, preserved files, and session invalidation behavior.

- [ ] **Step 3: Check Markdown links and command references**

Run:

```bash
rg -n 'scripts/dev/seed-local-platform\.sh|local-platform-seed\.env\.example|apps/powermonitor/\.env\.example|operations-monolith\.md' docs/local-development.md
test -f scripts/dev/seed-local-platform.sh
test -f infra/monolith/local-platform-seed.env.example
test -f apps/powermonitor/.env.example
test -f docs/operations-monolith.md
```

Expected: every referenced repository path exists and all expected guide references are found.

- [ ] **Step 4: Commit the guide**

```bash
git add docs/local-development.md
git commit -m "docs: add local development guide"
```

### Task 2: Make the README the discoverable entry point

**Files:**
- Modify: `README.md:44-116`
- Read: `docs/local-development.md`

**Interfaces:**
- Consumes: the guide created in Task 1.
- Produces: concise links from the repository homepage to local and production procedures.

- [ ] **Step 1: Replace the detailed local-seed subsection with an entry point**

Keep the `### Fresh local PowerMonitor seed` heading, but replace its body with a short summary and a relative link:

```markdown
For a complete local workflow—creating local TLS and vault material,
configuring PowerMonitor, resetting the monolith, loading demo data, and
verifying the UI—follow [docs/local-development.md](docs/local-development.md).

The guarded reset command is:

```bash
IOT_NANO_ALLOW_LOCAL_SEED=1 ./scripts/dev/seed-local-platform.sh --reset
```
```

Keep the existing explanation that the reset is disposable and requires a new browser sign-in. Do not duplicate setup details now owned by the local guide.

- [ ] **Step 2: Make the production link explicit**

In the Runtime section, retain the link to `docs/operations-monolith.md` and name it the production deployment and rollback runbook.

- [ ] **Step 3: Verify local and production entry points**

Run:

```bash
rg -n 'local-development\.md|operations-monolith\.md|seed-local-platform\.sh --reset' README.md
```

Expected: the README links to both the local guide and production monolith runbook, and shows the guarded reset command once.

- [ ] **Step 4: Commit the entry-point update**

```bash
git add README.md
git commit -m "docs: link local development workflow"
```

### Task 3: Retire the stale four-service operations procedure

**Files:**
- Modify: `docs/operations.md:1-18`
- Read: `docs/operations-monolith.md:1-20`
- Read: `README.md:6-27`

**Interfaces:**
- Consumes: the current monolith topology stated by README and the production runbook.
- Produces: a historical architecture reference that cannot be mistaken for a current startup guide.

- [ ] **Step 1: Add a historical-status notice at the top**

Put this notice immediately below the title:

```markdown
> **Historical architecture reference — not an operational runbook.**
> The four-service topology documented below has been superseded by the single
> `iot-nano-monolith` runtime. Do not start `iot-nano-stream`,
> `iot-nano-core`, `iot-nano-api`, or `iot-nano-mqttd` as separate deployment
> services. For local development, use [Local Development](local-development.md).
> For production deployment, storage, and rollback, use
> [Monolith Operations](operations-monolith.md).
```

Retain the former content only as historical context. Do not remove it or change architectural history.

- [ ] **Step 2: Verify that the redirect is unambiguous**

Run:

```bash
sed -n '1,20p' docs/operations.md
rg -n 'Historical architecture reference|local-development\.md|operations-monolith\.md' docs/operations.md
```

Expected: the notice precedes all four-service startup text and names both current replacement documents.

- [ ] **Step 3: Commit the historical notice**

```bash
git add docs/operations.md
git commit -m "docs: retire four-service operations guide"
```

### Task 4: Validate the completed documentation set

**Files:**
- Read: `README.md`
- Read: `docs/local-development.md`
- Read: `docs/operations.md`
- Read: `docs/operations-monolith.md`

**Interfaces:**
- Consumes: all documentation changes from Tasks 1–3.
- Produces: evidence that a reader finds one current local path and one current production path.

- [ ] **Step 1: Verify repository paths and documentation hygiene**

Run:

```bash
git diff --check HEAD~3..HEAD
rg -n -i 'tbd|todo|replace-with-system-password|replace-with-owner-password' README.md docs/local-development.md docs/operations.md
```

Expected: no whitespace errors, no unfinished placeholders, and template-only placeholder values appear only where the guide explicitly tells the reader to replace them.

- [ ] **Step 2: Syntax-check the documented runtime and seed scripts**

Run:

```bash
bash -n scripts/dev/local-platform-runtime.sh scripts/dev/seed-local-platform.sh
```

Expected: exit status `0`.

- [ ] **Step 3: Check the guidance path manually**

Confirm the following reading order is true:

```text
README.md
  -> docs/local-development.md (local monolith + PowerMonitor + demo seed)
  -> docs/operations-monolith.md (production deployment and rollback)
docs/operations.md
  -> historical notice + both current documents
```

- [ ] **Step 4: Commit any validation-driven wording fixes**

```bash
git add README.md docs/local-development.md docs/operations.md
git commit -m "docs: verify local development workflow"
```
