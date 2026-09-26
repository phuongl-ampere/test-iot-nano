# PowerMonitor Fresh Local Seed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `seed-local-platform.sh --reset` safely recreate the local SQLite environment, restart the monolith, bootstrap its system account, and seed deterministic PowerMonitor user cases.

**Architecture:** A focused runtime helper owns local paths, process identity, database reset, bootstrap, and monolith lifecycle. The existing seed script uses that helper before issuing its existing management-API fixture writes. Shell tests run the helper against isolated temporary paths and fake system commands; an optional live smoke check verifies the complete flow.

**Tech Stack:** Bash, Cargo lane wrapper, SQLite monolith binary, curl, jq, macOS/Linux process utilities.

## Global Constraints

- Require both `--reset` and `IOT_NANO_ALLOW_LOCAL_SEED=1` before stopping a process or removing any local state.
- Only reset `IOT_NANO_LOCAL_PLATFORM_ROOT` (default `$XDG_CACHE_HOME/rush-iot-nano/local-platform`) and retain `vault.key`, `mqtt-cert.pem`, and `mqtt-key.pem`.
- A listener PID may be stopped only after its executable is `iot-nano-monolith` and its open files include the configured `platform.sqlite` path.
- Bootstrap before starting the normal runtime, matching the monolith E2E lifecycle.
- Never source PowerMonitor `.env.local`; Next.js loads it itself.

---

### Task 1: Add a testable local-runtime lifecycle helper

**Files:**

- Create: `scripts/dev/local-platform-runtime.sh`
- Create: `scripts/dev/test-local-platform-runtime.sh`

**Interfaces:**

- Produces `local_platform_require_reset`, `local_platform_stop`, `local_platform_clear_state`, `local_platform_bootstrap`, and `local_platform_start` for `seed-local-platform.sh`.
- Reads `IOT_NANO_LOCAL_PLATFORM_ROOT`, `IOT_NANO_MANAGEMENT_URL`, `IOT_NANO_PUBLIC_HTTP_ADDRESS`, `IOT_NANO_MANAGEMENT_ADDRESS`, `IOT_NANO_MQTT_TCP_ADDRESS`, and `IOT_NANO_MQTT_TLS_ADDRESS`.

- [ ] **Step 1: Write a failing lifecycle-helper test**

Create a temporary local-platform root containing sentinel database files plus TLS/vault files. Supply fake `lsof`, `kill`, and monolith commands through `PATH`, then assert the helper refuses an unverified listener and, for a verified listener, removes only the following paths:

```bash
assert_missing "$platform_root/platform.sqlite"
assert_missing "$platform_root/internal"
assert_present "$platform_root/vault.key"
assert_present "$platform_root/mqtt-cert.pem"
assert_present "$platform_root/mqtt-key.pem"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash scripts/dev/test-local-platform-runtime.sh`

Expected: FAIL because `scripts/dev/local-platform-runtime.sh` does not exist.

- [ ] **Step 3: Implement the minimal runtime helper**

Implement explicit path construction and process validation. The deletion function must enumerate the known database files rather than remove the runtime root:

```bash
rm -f "$platform_path" "$platform_path-wal" "$platform_path-shm"
rm -rf "$internal_dir"
```

Use `lsof` to resolve a configured public-listener PID, inspect its executable and open-file list, send `TERM`, and poll until the listener disappears. Run bootstrap synchronously through:

```bash
./scripts/dev/cargo-lane.sh local-platform -- \
  run -p iot-nano-monolith -- --bootstrap-system
```

with the complete local monolith environment and `IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME/PASSWORD`. Start the normal process in a new process group, record the verified listener PID, and poll `/healthz` and `/readyz`.

- [ ] **Step 4: Run the helper test to verify it passes**

Run: `bash scripts/dev/test-local-platform-runtime.sh`

Expected: `test-local-platform-runtime: ok`.

- [ ] **Step 5: Commit the helper**

```bash
git add scripts/dev/local-platform-runtime.sh scripts/dev/test-local-platform-runtime.sh
git commit -m "feat(dev): add local platform reset runtime helper"
```

### Task 2: Make the seed command own reset, bootstrap, and startup

**Files:**

- Modify: `scripts/dev/seed-local-platform.sh`
- Modify: `README.md`

**Interfaces:**

- Consumes the Task 1 helper.
- Accepts exactly `--reset`; exits with status `2` for any other argument.
- Preserves the existing `IOT_NANO_ALLOW_LOCAL_SEED=1` acknowledgement.

- [ ] **Step 1: Write a failing command-contract test**

Extend `scripts/dev/test-local-platform-runtime.sh` with command-contract cases that invoke the seed script using fake helpers and assert:

```bash
run_seed_without --reset
assert_exit 2
assert_not_called kill

run_seed_with --reset
assert_called bootstrap-system
assert_called readyz
```

- [ ] **Step 2: Run the contract test to verify it fails**

Run: `bash scripts/dev/test-local-platform-runtime.sh`

Expected: FAIL because the seed script still accepts no lifecycle argument.

- [ ] **Step 3: Implement the reset entry point**

At the top of `seed-local-platform.sh`, validate the argument, source the runtime helper after sourcing the seed config, and execute this order before creating HTTP cookies:

```bash
local_platform_require_reset "$@"
local_platform_stop
local_platform_clear_state
local_platform_bootstrap "$IOT_NANO_SEED_SYSTEM_USERNAME" "$IOT_NANO_SEED_SYSTEM_PASSWORD"
local_platform_start
```

Keep the subsequent system login and fixture requests. Update the README with the one supported invocation and the scope of data that is reset.

- [ ] **Step 4: Run the command-contract test to verify it passes**

Run: `bash scripts/dev/test-local-platform-runtime.sh && bash -n scripts/dev/seed-local-platform.sh`

Expected: helper test prints `ok`; shell syntax check exits `0`.

- [ ] **Step 5: Commit lifecycle integration**

```bash
git add scripts/dev/seed-local-platform.sh README.md scripts/dev/test-local-platform-runtime.sh
git commit -m "feat(dev): reset local platform before seeding"
```

### Task 3: Seed the four PowerMonitor user cases

**Files:**

- Modify: `scripts/dev/seed-local-platform.sh`
- Create: `infra/monolith/local-platform-seed.env.example`
- Modify: `README.md`
- Modify: `scripts/dev/test-local-platform-runtime.sh`

**Interfaces:**

- Requires `IOT_NANO_SEED_OWNER_*`, `IOT_NANO_SEED_CONTROLLER_*`, `IOT_NANO_SEED_VIEWER_*`, and `IOT_NANO_SEED_UNASSIGNED_*` credentials.
- Replaces `ensure_direct_view_share` with `ensure_direct_share <username> <permission> <asset|device> <id>`.

- [ ] **Step 1: Write failing fixture assertions**

Add fake-management response cases to prove the script requests four users and emits the two distinct permission writes:

```bash
assert_request_contains 'username=seed-controller'
assert_request_contains 'permission=control'
assert_request_contains 'username=seed-viewer'
assert_request_contains 'permission=view'
assert_request_contains 'username=seed-unassigned'
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash scripts/dev/test-local-platform-runtime.sh`

Expected: FAIL because only owner and recipient are currently seeded.

- [ ] **Step 3: Implement the fixture matrix**

Create owner, controller, viewer, and unassigned users. Assign every seeded asset/device to the owner; share one zone and one device with the controller using `control`; share disjoint resources with the viewer using `view`; give the unassigned user no ownership or direct permissions. Add a tracked example file documenting every required local seed variable without real credentials.

- [ ] **Step 4: Run the fixture test to verify it passes**

Run: `bash scripts/dev/test-local-platform-runtime.sh && bash -n scripts/dev/seed-local-platform.sh`

Expected: all assertions pass and the script remains syntactically valid.

- [ ] **Step 5: Commit the fixture matrix**

```bash
git add scripts/dev/seed-local-platform.sh scripts/dev/test-local-platform-runtime.sh \
  infra/monolith/local-platform-seed.env.example README.md
git commit -m "feat(dev): seed powermonitor access scenarios"
```

### Task 4: Run the local reset smoke test and record the outcome

**Files:**

- Modify: `README.md` only if the verified command differs from Task 2.

**Interfaces:**

- Uses the local seed file, local TLS/vault material, and the command from Task 2.

- [ ] **Step 1: Create a disposable sentinel before the second reset**

Use the seeded management session to create a uniquely named asset, then record that its identifier is returned by the management API.

- [ ] **Step 2: Run the reset twice**

Run:

```bash
IOT_NANO_ALLOW_LOCAL_SEED=1 ./scripts/dev/seed-local-platform.sh --reset
IOT_NANO_ALLOW_LOCAL_SEED=1 ./scripts/dev/seed-local-platform.sh --reset
```

Expected: both commands complete, the second run has no sentinel, and it prints all four user cases.

- [ ] **Step 3: Verify runtime and UI availability**

Run:

```bash
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
curl --fail http://127.0.0.1:3002/
```

Expected: each command exits `0`.

- [ ] **Step 4: Run the complete automated checks**

Run:

```bash
bash scripts/dev/test-local-platform-runtime.sh
bash scripts/dev/test-cargo-lane.sh
bash -n scripts/dev/seed-local-platform.sh scripts/dev/local-platform-runtime.sh
```

Expected: all commands exit `0`.

- [ ] **Step 5: Commit verification/documentation adjustments**

```bash
git add README.md scripts/dev
git commit -m "test(dev): verify fresh local seed lifecycle"
```

