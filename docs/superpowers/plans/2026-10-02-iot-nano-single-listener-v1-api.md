# IoT Nano Single-Listener V1 API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Platform listeners `:18080` and `:18081` with one `:18081` HTTP origin for OAuth, all versioned APIs, and management web routes; update Power Monitor to use it.

**Architecture:** `MonolithConfig` owns one `http` address. The monolith binds it once to a composite Axum router: health, OTA, public resource API, OAuth, and management sessions. Management JSON routes move from `/api/...` to `/api/v1/...`; Power Monitor's own auth helpers move to `/api/v1/auth/*`.

**Tech Stack:** Rust, Axum, Tokio, Next.js 16, React 19, Vitest 5, Cargo integration tests.

## Global Constraints

- `:18081` is the only Platform HTTP listener. `:18080` must not bind, redirect, or proxy.
- OAuth stays at `/oauth/*`; public resources stay at `/api/v1/*`.
- Management JSON paths move from `/api/...` to `/api/v1/...`; management HTML, static assets, health, and docs remain unchanged on the same origin.
- Replace `IOT_NANO_PUBLIC_HTTP_ADDRESS` and `IOT_NANO_MANAGEMENT_ADDRESS` with `IOT_NANO_HTTP_ADDRESS`. Retired names are invalid, not aliases.
- Both Power Monitor Platform bases use `http://127.0.0.1:18081`; app auth helpers use `/api/v1/auth/*`.
- Preserve MQTT, OAuth grants/scopes, storage, authorization, and telemetry behavior. Do not add legacy compatibility paths.

## File Structure

- `services/iot-nano-monolith/src/config.rs`: one HTTP configuration field and environment parser.
- `services/iot-nano-monolith/src/runtime.rs`: one composite HTTP router and server task.
- `services/iot-nano-monolith/src/management/{mod,openapi}.rs`: v1 management JSON routes and status text.
- `services/iot-nano-monolith/tests/{config,startup,migration,durable_recovery,shutdown,alpha_runtime,e2e_sqlite,e2e_timescale,external_app_contract,management_sessions}.rs`: Platform contracts and fixtures.
- `apps/powermonitor/app/api/v1/auth/{login,callback,logout}/route.ts`: static v1 auth helpers, moved from `app/api/auth`.
- `apps/powermonitor/{lib/oauth.ts,components/powermonitor-dashboard.tsx,components/powermonitor-login-gate.tsx,.env.example,.env.local}`: paths and Platform bases.
- `apps/powermonitor/tests/*`: app auth route and BFF contracts.

---

### Task 1: Replace two Platform HTTP configuration values with one

**Files:**
- Modify: `services/iot-nano-monolith/src/config.rs`
- Modify: `services/iot-nano-monolith/tests/{config,startup,migration,durable_recovery,shutdown,alpha_runtime,e2e_sqlite,e2e_timescale,external_app_contract}.rs`

**Interfaces:**
- Consumes: `MonolithConfig::from_values(BTreeMap<String, String>)` and fixtures constructing `MonolithConfig`.
- Produces: `MonolithConfig { http: SocketAddr, .. }`, read from `IOT_NANO_HTTP_ADDRESS`.

- [ ] **Step 1: Write the failing configuration contract**

Add this test to `services/iot-nano-monolith/tests/config.rs` using its `sqlite_values` helper:

```rust
#[test]
fn config_uses_one_http_address_and_rejects_retired_listener_names() {
    let mut values = sqlite_values();
    values.insert("IOT_NANO_HTTP_ADDRESS".to_owned(), "127.0.0.1:18081".to_owned());
    let config = MonolithConfig::from_values(values).unwrap();
    assert_eq!(config.http, "127.0.0.1:18081".parse().unwrap());
    for retired in ["IOT_NANO_PUBLIC_HTTP_ADDRESS", "IOT_NANO_MANAGEMENT_ADDRESS"] {
        let mut retired_values = sqlite_values();
        retired_values.insert(retired.to_owned(), "127.0.0.1:18080".to_owned());
        assert!(MonolithConfig::from_values(retired_values).unwrap_err().to_string().contains(retired));
    }
}
```

- [ ] **Step 2: Verify the test fails correctly**

Run: `cargo test -p iot-nano-monolith config_uses_one_http_address_and_rejects_retired_listener_names -- --exact`

Expected: failure because the current config has no `http` field and does not parse `IOT_NANO_HTTP_ADDRESS`.

- [ ] **Step 3: Implement the one-address configuration**

Replace `public_http` and `management_http` in `MonolithConfig` with `pub http: SocketAddr`. In `from_values`, use:

```rust
let http = socket_address(&values, "IOT_NANO_HTTP_ADDRESS", DEFAULT_HTTP_ADDRESS)?;
validate_unique_addresses([http, mqtt_tcp, mqtt_tls])?;
```

Add both removed names to `validate_retired_environment`. In every listed fixture, replace two HTTP fields with one reserved `http` address and replace both old environment assignments with `.env("IOT_NANO_HTTP_ADDRESS", self.http.to_string())`.

- [ ] **Step 4: Verify the task is green**

Run: `cargo test -p iot-nano-monolith config_uses_one_http_address_and_rejects_retired_listener_names -- --exact`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/iot-nano-monolith/src/config.rs services/iot-nano-monolith/tests
git commit -m "refactor: configure one IoT Nano HTTP listener"
```

### Task 2: Bind the composite Platform router once

**Files:**
- Modify: `services/iot-nano-monolith/src/runtime.rs`
- Modify: `services/iot-nano-monolith/src/management/mod.rs`
- Modify: `services/iot-nano-monolith/tests/{alpha_runtime,startup,shutdown}.rs`

**Interfaces:**
- Consumes: `MonolithConfig.http`, `ManagementSessionRouter.router`, `iot_api::public_v1_router`, and `iot_api::public_oauth_router_with_browser_session_verifier`.
- Produces: one HTTP server task at `config.http` serving health, OTA, OAuth, public v1 resources, management pages, and management APIs.

- [ ] **Step 1: Write the failing one-listener runtime contract**

Replace separate-listener assertions in `tests/alpha_runtime.rs` with:

```rust
#[tokio::test]
async fn alpha_runtime_serves_oauth_public_api_and_management_api_on_one_listener() {
    let fixture = Fixture::new().await;
    let mut runtime = MonolithRuntime::start(fixture.config.clone()).await.unwrap();
    assert_http_status(fixture.config.http, "/api/v1/devices", 401).await;
    assert_http_status(fixture.config.http, "/api/auth/me", 401).await;
    assert_form_post_status(fixture.config.http, "/oauth/token", "", 400).await;
    runtime.shutdown(Instant::now() + Duration::from_secs(2)).await.unwrap();
}
```

- [ ] **Step 2: Verify the runtime contract is red**

Run: `cargo test -p iot-nano-monolith alpha_runtime_serves_oauth_public_api_and_management_api_on_one_listener -- --exact`

Expected: failure because the runtime still binds public and management routers separately.

- [ ] **Step 3: Build and bind one router**

In `MonolithRuntime::start`, replace both HTTP listener blocks with:

```rust
let web_router = health_router(readiness.clone())
    .merge(crate::ota::router(Arc::clone(&platform)))
    .merge(iot_api::public_v1_router(
        Arc::clone(&platform), token_vault,
        Arc::new(PlatformCoreFacade::new(Arc::clone(&platform))),
    ))
    .merge(iot_api::public_oauth_router_with_browser_session_verifier(
        Arc::clone(&platform), browser_session_verifier,
    ))
    .merge(management_sessions.router);
let http_listener = TcpListener::bind(config.http).await.map_err(StartupError::HttpBind)?;
let http_tasks = vec![spawn_http_server(http_listener, web_router, http_cancellation.clone())];
```

Rename public/management bind errors and infrastructure status to one HTTP-listener value. Update lifecycle tests to assert this listener plus both MQTT listeners only.

- [ ] **Step 4: Verify the runtime contract is green**

Run: `cargo test -p iot-nano-monolith alpha_runtime_serves_oauth_public_api_and_management_api_on_one_listener -- --exact`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/iot-nano-monolith/src/runtime.rs services/iot-nano-monolith/src/management/mod.rs services/iot-nano-monolith/tests
git commit -m "feat: serve Platform routes from one HTTP listener"
```

### Task 3: Version the management JSON namespace

**Files:**
- Modify: `services/iot-nano-monolith/src/management/{mod,openapi}.rs`
- Modify: `services/iot-nano-monolith/tests/{management_sessions,alpha_runtime}.rs`

**Interfaces:**
- Consumes: existing management handlers and session checks.
- Produces: `/api/v1/auth/*`, `/api/v1/system/*`, `/api/v1/tenant/*`, `/api/v1/user/*`, and `/api/v1/management/*`; old `/api/*` paths return 404.

- [ ] **Step 1: Write red route-migration assertions**

In `tests/management_sessions.rs`, change the login/session requests to `/api/v1/auth/login` and `/api/v1/auth/me`, then add:

```rust
assert_json_post_status(address, "/api/v1/auth/login", body, 200).await;
assert_http_status(address, "/api/auth/login", 404).await;
```

Change the management-login assertion in `tests/alpha_runtime.rs` to `/api/v1/auth/login` and retain a 404 assertion for `/api/auth/login`.

- [ ] **Step 2: Verify the management API test is red**

Run: `cargo test -p iot-nano-monolith management_login -- --nocapture`

Expected: failures show the old `/api/auth/*` registrations are still active.

- [ ] **Step 3: Move every management JSON path**

Apply this exact mechanical transformation, then inspect its diff:

```bash
perl -0pi -e 's#"/api/#"/api/v1/#g' \
  services/iot-nano-monolith/src/management/mod.rs \
  services/iot-nano-monolith/src/management/openapi.rs \
  services/iot-nano-monolith/tests/management_sessions.rs \
  services/iot-nano-monolith/tests/alpha_runtime.rs
```

Only quoted JSON API paths change. Keep `/oauth/*`, management HTML/form routes, static assets, health, and docs unchanged.

- [ ] **Step 4: Verify the management routes are green**

Run: `cargo test -p iot-nano-monolith management_sessions -- --nocapture && cargo test -p iot-nano-monolith alpha_runtime -- --nocapture`

Expected: v1 management routes pass and old management API paths do not match.

- [ ] **Step 5: Commit**

```bash
git add services/iot-nano-monolith/src/management services/iot-nano-monolith/tests
git commit -m "feat: version management APIs under v1"
```

### Task 4: Move Power Monitor auth helpers and Platform bases

**Files:**
- Move: `apps/powermonitor/app/api/auth/{login,callback,logout}/route.ts` to `apps/powermonitor/app/api/v1/auth/{login,callback,logout}/route.ts`
- Modify: `apps/powermonitor/{lib/oauth.ts,components/powermonitor-dashboard.tsx,components/powermonitor-login-gate.tsx,.env.example,.env.local}`
- Modify: `apps/powermonitor/tests/{oauth-callback,password-login-route,password-oauth-handoff,logout-route,login-gate,page-session,dashboard-contract,platform-client}.test.ts*`
- Modify: `services/iot-nano-monolith/tests/external_app_contract.rs`

**Interfaces:**
- Consumes: static Next.js route precedence over `app/api/v1/[...path]/route.ts` and OAuth redirect registration.
- Produces: Power Monitor helpers at `/api/v1/auth/*` and BFF resource requests to `http://127.0.0.1:18081/api/v1/*`.

- [ ] **Step 1: Write failing Power Monitor route and base expectations**

Replace expected `/api/auth/` strings with `/api/v1/auth/` in the listed Power Monitor tests. In `tests/platform-client.test.ts`, add this expected URL when the test uses `PLATFORM_BASE_URL=http://127.0.0.1:18081`:

```ts
expect(capturedUrl).toBe("http://127.0.0.1:18081/api/v1/devices");
```

Change external-app registration, login, and callback expectations to `/api/v1/auth/*`.

- [ ] **Step 2: Verify the Power Monitor contract is red**

Run: `(cd apps/powermonitor && npm test -- oauth-callback.test.ts password-login-route.test.ts logout-route.test.ts platform-client.test.ts)`

Expected: failures because route modules and OAuth helpers still resolve `/api/auth/*`.

- [ ] **Step 3: Move routes and update URLs/configuration**

Move the static route directories with:

```bash
git mv apps/powermonitor/app/api/auth/login apps/powermonitor/app/api/v1/auth/login
git mv apps/powermonitor/app/api/auth/callback apps/powermonitor/app/api/v1/auth/callback
git mv apps/powermonitor/app/api/auth/logout apps/powermonitor/app/api/v1/auth/logout
```

Replace Power Monitor literals `/api/auth/` with `/api/v1/auth/`. Set `.env.example` to:

```dotenv
PLATFORM_BASE_URL=http://192.168.1.10:18081
PLATFORM_AUTH_BASE_URL=http://192.168.1.10:18081
OAUTH_REDIRECT_URI=http://192.168.1.10:3002/api/v1/auth/callback
```

In ignored `.env.local`, alter only `PLATFORM_BASE_URL`, `PLATFORM_AUTH_BASE_URL`, and `OAUTH_REDIRECT_URI` to local `127.0.0.1:18081` and `localhost:3002/api/v1/auth/callback`; never stage that file. Update the external app contract so both Platform bases use its one HTTP fixture address.

- [ ] **Step 4: Verify the Power Monitor task is green**

Run: `(cd apps/powermonitor && npm test -- oauth-callback.test.ts password-login-route.test.ts logout-route.test.ts platform-client.test.ts && npm test)`

Expected: selected and full Vitest suites pass; static auth routes win over the BFF catch-all.

- [ ] **Step 5: Commit**

```bash
git add apps/powermonitor/app/api apps/powermonitor/lib/oauth.ts apps/powermonitor/components apps/powermonitor/.env.example apps/powermonitor/tests services/iot-nano-monolith/tests/external_app_contract.rs
git commit -m "feat: version Power Monitor auth routes"
```

### Task 5: Verify and run the single-listener development stack

**Files:**
- Modify: `docs/superpowers/plans/2026-10-02-iot-nano-single-listener-v1-api.md`

**Interfaces:**
- Consumes: completed Tasks 1-4.
- Produces: verified source, a local Platform listener only at `:18081`, and Power Monitor configured to use it.

- [ ] **Step 1: Run complete source verification**

```bash
cargo test -p iot-nano-monolith
(cd apps/powermonitor && npm test && npm run build)
```

Expected: both suites and the production build exit 0.

- [ ] **Step 2: Restart only the identified development monolith**

First identify the exact parent and child processes whose command includes `scripts/dev/cargo-lane.sh local-platform -- run -p iot-nano-monolith`. Stop only those recorded PIDs, then start it in the background and retain its log:

```bash
task_log=/tmp/iot-nano-single-listener.log
scripts/dev/cargo-lane.sh local-platform -- run -p iot-nano-monolith > "$task_log" 2>&1 &
task_pid=$!
```

Do not stop unrelated Node, SSH, Docker, MQTT, or user processes. Wait for `127.0.0.1:18081` to bind.

- [ ] **Step 3: Verify live route behaviour**

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18081/api/v1/devices
curl -sS -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18081/oauth/token
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18081/api/v1/auth/me
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18081/api/auth/me
curl --connect-timeout 2 http://127.0.0.1:18080
```

Expected: `401`, `400`, `401`, `404`, then connection failure. Confirm `lsof -nP -iTCP:18081 -sTCP:LISTEN` lists the monolith and `lsof -nP -iTCP:18080 -sTCP:LISTEN` is empty.

- [ ] **Step 4: Record verification and commit the plan state**

Mark every checkbox in this plan complete after observing its stated evidence, then run:

```bash
git add docs/superpowers/plans/2026-10-02-iot-nano-single-listener-v1-api.md
git commit -m "docs: record single-listener migration verification"
```
