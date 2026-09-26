# Tenant Profile Tabs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing tenant Asset Profiles and Device Profiles pages visibly reachable as tabs in the tenant Profile area.

**Architecture:** Introduce one Askama template partial that owns the three stable tenant Profile links and determines the active tab from an `active_profile_tab` value supplied by each existing page. Include that partial in the JSON Profile, Asset Profiles, and Device Profiles templates. The feature is presentation-only; existing management routes and API behavior remain unchanged.

**Tech Stack:** Rust, Axum, Askama templates, Cargo integration tests.

## Global Constraints

- Work only in `/Users/phuongl/myai/projects/fuvi/rush-iot-nano`; do not create a worktree.
- Keep Profile tenant-scoped; never reintroduce an `app_id` to Profile UI or runtime.
- Preserve `/tenant/profile`, `/tenant/profiles/asset`, and `/tenant/profiles/device` route behavior.
- Change no stored configuration or operational data.

---

### Task 1: Add a shared tenant Profile tab partial and include it on every Profile-area page

**Files:**
- Create: `services/iot-nano-monolith/templates/platform_ui/tenant_profile_tabs.html`
- Modify: `services/iot-nano-monolith/assets/platform-ui.css`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_profile.html:9-14`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_asset_profiles.html:7-16`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_device_profiles.html:7-16`
- Test: `services/iot-nano-monolith/tests/platform_ui_templates.rs:429-440`

**Interfaces:**
- Consumes: Askama local `active_profile_tab` with exactly one of `configuration`, `asset`, or `device`.
- Produces: a rendered `nav` element with links to `/tenant/profile`, `/tenant/profiles/asset`, and `/tenant/profiles/device`.

- [x] **Step 1: Write the failing regression test**

Extend `tenant_profile_template_keeps_json_import_export_separate_from_profile_tabs` to load `tenant_profile_tabs.html` and assert all three `href` values. Assert each of the three Profile-area templates contains `{% include "platform_ui/tenant_profile_tabs.html" %}`.

```rust
let tabs = platform_template_source("tenant_profile_tabs.html");
assert!(tabs.contains("href=\"/tenant/profile\""));
assert!(tabs.contains("href=\"/tenant/profiles/asset\""));
assert!(tabs.contains("href=\"/tenant/profiles/device\""));
assert!(template.contains("{% include \"platform_ui/tenant_profile_tabs.html\" %}"));
assert!(asset_profiles.contains("{% include \"platform_ui/tenant_profile_tabs.html\" %}"));
assert!(device_profiles.contains("{% include \"platform_ui/tenant_profile_tabs.html\" %}"));
```

- [x] **Step 2: Run the focused test and verify it fails**

Run:

```bash
./scripts/dev/cargo-lane.sh tenant-profile-tabs-red -- test -p iot-nano-monolith --test platform_ui_templates tenant_profile_template_keeps_json_import_export_separate_from_profile_tabs -- --exact --test-threads=1
```

Expected: FAIL because `tenant_profile_tabs.html` does not exist and no Profile-area template includes it.

- [x] **Step 3: Write the minimal template and style implementation**

Create `tenant_profile_tabs.html`:

```html
<nav class="profile-tabs" aria-label="Profile catalog">
  <a class="profile-tab{% if active_profile_tab == \"configuration\" %} active{% endif %}" href="/tenant/profile"{% if active_profile_tab == \"configuration\" %} aria-current="page"{% endif %}>Profile configuration</a>
  <a class="profile-tab{% if active_profile_tab == \"asset\" %} active{% endif %}" href="/tenant/profiles/asset"{% if active_profile_tab == \"asset\" %} aria-current="page"{% endif %}>Asset Profiles</a>
  <a class="profile-tab{% if active_profile_tab == \"device\" %} active{% endif %}" href="/tenant/profiles/device"{% if active_profile_tab == \"device\" %} aria-current="page"{% endif %}>Device Profiles</a>
</nav>
```

Set the page-local value and include the partial after each Profile-area page heading. Both catalog templates must also set `active_nav` to `profile`, keeping the existing sidebar's **Profile** item selected:

```html
{% let active_profile_tab = "configuration" %}
{% include "platform_ui/tenant_profile_tabs.html" %}
```

Use `asset` in `tenant_asset_profiles.html` and `device` in `tenant_device_profiles.html`.

Append this focused component style to `platform-ui.css`:

```css
.profile-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.25rem;
  border-bottom: 1px solid #d3dce8;
}

.profile-tab {
  color: #536277;
  padding: 0.625rem 0.875rem;
  text-decoration: none;
}

.profile-tab:hover,
.profile-tab:focus-visible,
.profile-tab.active {
  color: #182235;
}

.profile-tab:focus-visible {
  outline: 2px solid #0d5cb5;
  outline-offset: -2px;
}

.profile-tab.active {
  border-bottom: 2px solid #139f9a;
  font-weight: 700;
}
```

- [x] **Step 4: Run the focused test and verify it passes**

Run the command from Step 2.

Expected: PASS with one test passed.

- [x] **Step 5: Run formatting and affected suite**

Run:

```bash
cargo fmt --all -- --check
./scripts/dev/cargo-lane.sh tenant-profile-tabs-green -- test -p iot-nano-monolith --test platform_ui_templates -- --test-threads=1
git diff --check
```

Expected: all commands exit 0.

- [x] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-09-26-tenant-profile-tabs-design.md docs/superpowers/plans/2026-09-26-tenant-profile-tabs.md services/iot-nano-monolith/assets/platform-ui.css services/iot-nano-monolith/templates/platform_ui/tenant_profile_tabs.html services/iot-nano-monolith/templates/platform_ui/tenant_profile.html services/iot-nano-monolith/templates/platform_ui/tenant_asset_profiles.html services/iot-nano-monolith/templates/platform_ui/tenant_device_profiles.html services/iot-nano-monolith/tests/platform_ui_templates.rs
git commit -m "fix(profile): expose tenant profile tabs"
```
