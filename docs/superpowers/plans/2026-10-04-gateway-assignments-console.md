# Gateway Assignments Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the tenant gateway-topology UI into a Gateway assignments view that omits ordinary direct devices.

**Architecture:** Preserve the `/tenant/topology` route, all assignment/detachment handlers, persistence, and authorization. Change only the page view-model and Askama templates: construct one row per gateway, nest its assigned children beneath it, and render an empty state only when no gateway exists.

**Tech Stack:** Rust, Axum, Askama templates, Tokio integration tests, Cargo.

## Global Constraints

- Keep `/tenant/topology`, `/tenant/topology/assign`, and `/tenant/topology/detach` unchanged.
- Do not alter storage schema, MQTT routing, authorization, or gateway validation.
- Keep gateways with zero assigned children visible.
- Do not render ordinary direct devices in the assignments summary.
- Preserve the current tenant-session authorization and form behavior.

---

### Task 1: Specify the simplified gateway-assignment page behavior in tests

**Files:**
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs:tenant_topology_forms_scope_gateway_children_and_reject_invalid_assignments`
- Modify: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

**Interfaces:**
- Consumes: the existing tenant `/tenant/topology` route and topology update/assignment form helpers.
- Produces: regression coverage for the renamed page, no-gateway empty state, gateway-only summary, and retained assign/detach controls.

- [ ] **Step 1: Extend the functional topology-page test before changing production code**

  In `tenant_topology_forms_scope_gateway_children_and_reject_invalid_assignments`, fetch `/tenant/topology` before provisioning a gateway and assert a successful page with:

  ```rust
  assert!(body.contains("Gateway assignments"));
  assert!(body.contains("No gateway assignments"));
  ```

  After creating one gateway, one assigned child, and one unrelated direct device, fetch the page again. Assert that it contains the gateway and assigned child labels, and does not contain the direct device label:

  ```rust
  assert!(body.contains("Gateway"));
  assert!(body.contains("Child"));
  assert!(!body.contains("<td><strong>Direct device</strong></td>"));
  ```

  Keep the existing POST assignment and detach assertions unchanged so the UI simplification cannot alter topology mutations.

- [ ] **Step 2: Add focused template-source assertions**

  Add a `platform_ui_templates` test that reads `tenant_topology.html` and `tenant_navigation.html`. Assert the new visible labels and the removal of the obsolete summary labels:

  ```rust
  assert!(topology.contains("Gateway assignments"));
  assert!(topology.contains("No gateway assignments"));
  assert!(!topology.contains("Topology devices"));
  assert!(!navigation.contains(">Topology</span>"));
  assert!(navigation.contains(">Gateway assignments</span>"));
  ```

- [ ] **Step 3: Run the focused tests and confirm the expected failure**

  Run:

  ```bash
  cargo test -p iot-nano-monolith tenant_topology_forms_scope_gateway_children_and_reject_invalid_assignments -- --exact
  cargo test -p iot-nano-monolith gateway_assignments -- --nocapture
  ```

  Expected: the new assertions fail because the page currently calls itself `Gateway Topology`, renders `Topology devices`, and includes direct devices.

### Task 2: Build a gateway-only view model and render the simplified page

**Files:**
- Modify: `services/iot-nano-monolith/src/management/routes/tenant.rs:tenant_topology_page`
- Modify: `services/iot-nano-monolith/src/lib.rs`
- Modify: `services/iot-nano-monolith/src/platform_ui.rs:TenantTopologyRow`, `TenantTopologyPage`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_topology.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_navigation.html`
- Test: `services/iot-nano-monolith/tests/management_sessions.rs`
- Test: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

**Interfaces:**
- Consumes: `ManagementDeviceRepository::list_management_devices`, where each record exposes `topology.is_gateway` and `topology.gateway_device_id`.
- Produces: `TenantTopologyPage` rows containing a gateway plus only the child devices assigned to that gateway; existing `gateways`, `children`, and `assigned_children` option lists remain available to the forms.

- [ ] **Step 1: Replace the flat topology summary row with a gateway assignment row**

  In `platform_ui.rs`, change `TenantTopologyRow` so it represents one gateway and its child display rows instead of `role` and `gateway` strings. Keep all fields private and construct them through `new`, following the existing page-model pattern:

  ```rust
  pub struct TenantTopologyChildRow {
      device_id: String,
      display_name: String,
  }

  pub struct TenantTopologyRow {
      device_id: String,
      display_name: String,
      children: Vec<TenantTopologyChildRow>,
  }
  ```

  Rename `TenantTopologyPage.devices` to `assignments`. Add an `is_empty` helper only if Askama needs it to distinguish a no-gateway empty state from an empty child list.

  Re-export `TenantTopologyChildRow` from `src/lib.rs` beside `TenantTopologyRow`, so the tenant route can follow the crate-root view-model import pattern.

- [ ] **Step 2: Construct one row per gateway in `tenant_topology_page`**

  Keep the current `gateways`, `children`, and `assigned_children` select-option construction intact. Replace the current `devices.into_iter().map(...)` summary construction with an iteration over gateway devices only. For every gateway, collect non-gateway devices whose `gateway_device_id` equals that gateway's `device_id`:

  ```rust
  let assignments = devices
      .iter()
      .filter(|device| device.topology.is_gateway)
      .map(|gateway| {
          let children = devices
              .iter()
              .filter(|device| {
                  !device.topology.is_gateway
                      && device.topology.gateway_device_id.as_deref()
                          == Some(gateway.device_id.as_str())
              })
              .map(|child| TenantTopologyChildRow::new(
                  child.device_id.clone(),
                  child.display_name.clone().unwrap_or_else(|| child.device_id.clone()),
              ))
              .collect();
          TenantTopologyRow::new(
              gateway.device_id.clone(),
              gateway.display_name.clone().unwrap_or_else(|| gateway.device_id.clone()),
              children,
          )
      })
      .collect();
  ```

  Pass `assignments` into `TenantTopologyPage::new`. Do not change form handlers, topology persistence, or their redirects.

- [ ] **Step 3: Rename and simplify the Askama templates**

  In `tenant_navigation.html`, retain the `topology` active-navigation key and route but change only its visible label to `Gateway assignments`.

  In `tenant_topology.html`:

  ```html
  {% block title %}Gateway assignments | IoT Nano{% endblock %}
  <h1>Gateway assignments</h1>
  <p>Manage which devices communicate through each gateway.</p>
  ```

  Replace the `Topology devices` table with a `Gateway assignments` table showing a gateway's name and ID plus its assigned children. For a gateway with no children, render `No assigned devices`. If `page.assignments.is_empty()`, render only:

  ```html
  <p class="empty-state">No gateway assignments. Ordinary devices connect directly by default.</p>
  ```

  Do not render the word `Direct`, a generic device role column, or unrelated direct devices in this summary. Keep the existing assignment and detachment forms and their current empty states.

- [ ] **Step 4: Format and run focused tests**

  Run:

  ```bash
  cargo fmt --check
  cargo test -p iot-nano-monolith tenant_topology_forms_scope_gateway_children_and_reject_invalid_assignments -- --exact
  cargo test -p iot-nano-monolith --test platform_ui_templates gateway_assignments -- --nocapture
  ```

  Expected: formatting succeeds and all added/updated tests pass.

- [ ] **Step 5: Run the relevant regression suite**

  Run:

  ```bash
  cargo test -p iot-nano-monolith --test management_sessions tenant_topology -- --nocapture
  cargo test -p iot-nano-monolith --test platform_ui_templates
  ```

  Expected: tenant topology authorization, assignment/detachment behavior, and platform template assertions all pass.

- [ ] **Step 6: Review the diff and commit only this feature's files**

  Run:

  ```bash
  git diff --check
  git diff -- services/iot-nano-monolith/src/management/routes/tenant.rs \
    services/iot-nano-monolith/src/lib.rs \
    services/iot-nano-monolith/src/platform_ui.rs \
    services/iot-nano-monolith/templates/platform_ui/tenant_topology.html \
    services/iot-nano-monolith/templates/platform_ui/tenant_navigation.html \
    services/iot-nano-monolith/tests/management_sessions.rs \
    services/iot-nano-monolith/tests/platform_ui_templates.rs
  git add services/iot-nano-monolith/src/management/routes/tenant.rs \
    services/iot-nano-monolith/src/lib.rs \
    services/iot-nano-monolith/src/platform_ui.rs \
    services/iot-nano-monolith/templates/platform_ui/tenant_topology.html \
    services/iot-nano-monolith/templates/platform_ui/tenant_navigation.html \
    services/iot-nano-monolith/tests/management_sessions.rs \
    services/iot-nano-monolith/tests/platform_ui_templates.rs
  git commit -m "feat: simplify gateway assignments console"
  ```

  Expected: no whitespace errors; only the seven feature files are staged and committed.
