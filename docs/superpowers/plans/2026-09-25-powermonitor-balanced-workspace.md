# PowerMonitor Balanced Workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the PowerMonitor navigation and selected-resource workspace balanced, concise, and easier to operate without changing backend contracts.

**Architecture:** Keep `PowerMonitorDashboard` as the data/polling owner. Make `PowerMonitorTree` own only expandable resource navigation, while CSS provides the hierarchy and responsive layout. Existing drawer, controls, command lifecycle, and BFF calls remain unchanged.

**Tech Stack:** Next.js, React 19, TypeScript, Vitest, Testing Library, CSS.

## Global Constraints

- Change only `apps/powermonitor` frontend files and documentation.
- Keep current BFF API routes, polling intervals, and permission behavior unchanged.
- Use the fixed dark theme and 4-6px UI radii.
- Keep configuration inside `ResourceEditDrawer`; keep live device control in the workspace.
- Avoid explanatory or duplicate UI text.

---

### Task 1: Expandable Resource Tree

**Files:**
- Modify: `apps/powermonitor/components/powermonitor-tree.tsx`
- Modify: `apps/powermonitor/tests/powermonitor-tree.test.tsx`

**Interfaces:**
- Consumes: `Asset`, `Device`, selection IDs, and selection callbacks.
- Produces: keyboard-operable asset navigation with `aria-expanded`, hierarchy rows, selected state, and an unassigned-device group.

- [ ] **Step 1: Write the failing tests**

```tsx
it("collapses nested resources until the asset is expanded", () => {
  render(<PowerMonitorTree {...treeProps} />);
  expect(screen.queryByText("Main meter")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Expand Site A" }));
  expect(screen.getByText("Main meter")).toBeTruthy();
});

it("marks the selected device in the resource navigator", () => {
  render(<PowerMonitorTree {...treeProps} selectedDeviceId="meter-1" />);
  fireEvent.click(screen.getByRole("button", { name: "Expand Site A" }));
  expect(screen.getByRole("button", { name: "Select Main meter" })).toHaveAttribute("aria-current", "true");
});
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `npm test -- --run tests/powermonitor-tree.test.tsx`

Expected: FAIL because expand buttons and collapsed descendants do not exist.

- [ ] **Step 3: Implement the tree state and semantic rows**

```tsx
const [expandedAssetIds, setExpandedAssetIds] = useState(() => new Set<string>());

<button aria-expanded={expanded} aria-label={(expanded ? "Collapse " : "Expand ") + asset.name} ... />
{expanded && <ul className="tree-children">...</ul>}
```

- [ ] **Step 4: Run the focused test and verify it passes**

Run: `npm test -- --run tests/powermonitor-tree.test.tsx`

Expected: PASS.

### Task 2: Simplify the Selected-Resource Workspace

**Files:**
- Modify: `apps/powermonitor/components/powermonitor-dashboard.tsx`
- Modify: `apps/powermonitor/tests/dashboard-contract.test.tsx`

**Interfaces:**
- Consumes: existing selected device/asset, alert, invitation, and telemetry state.
- Produces: concise breadcrumb header, selected resource status, compact fleet counts, and unchanged edit/control/command surfaces.

- [ ] **Step 1: Write the failing rendering test**

```tsx
it("renders concise selected-resource navigation without the legacy explorer heading", () => {
  const markup = renderToStaticMarkup(<PowerMonitorDashboard initialDeviceId="meter-1" />);
  expect(markup).toContain('aria-label="Resource path"');
  expect(markup).not.toContain("Operational energy view");
  expect(markup).not.toContain("Asset explorer");
});
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `npm test -- --run tests/dashboard-contract.test.tsx`

Expected: FAIL because the dashboard renders the legacy explorer copy and has no resource-path navigation.

- [ ] **Step 3: Render the compact header and preserve behaviors**

```tsx
<nav aria-label="Resource path" className="resource-path">
  <span>Assets</span><span aria-hidden="true">/</span><strong>{title}</strong>
</nav>
```

Keep invitation opening, range selection, edit drawer opening, refresh, logout, live charts/raw table, controls, and command submission exactly on their existing handlers.

- [ ] **Step 4: Run the focused test and verify it passes**

Run: `npm test -- --run tests/dashboard-contract.test.tsx`

Expected: PASS.

### Task 3: Apply the Balanced Dark Layout

**Files:**
- Modify: `apps/powermonitor/app/globals.css`
- Modify: `apps/powermonitor/tests/dark-mode-styles.test.ts`

**Interfaces:**
- Consumes: existing and new tree/dashboard class names.
- Produces: compact desktop rail, readable narrow layout, visible selected rows, and bounded tables/charts.

- [ ] **Step 1: Write the failing style assertions**

```ts
it("styles the resource hierarchy and selected navigator row", () => {
  expect(styles).toContain(".tree-children");
  expect(styles).toContain(".tree-row[aria-current=\"true\"]");
  expect(styles).toContain(".resource-path");
});
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `npm test -- --run tests/dark-mode-styles.test.ts`

Expected: FAIL because hierarchy and breadcrumb selectors do not exist.

- [ ] **Step 3: Implement balanced layout rules**

```css
.tree-children { margin-left: 13px; border-left: 1px solid var(--pm-border-subtle); }
.tree-row[aria-current="true"] { box-shadow: inset 3px 0 var(--pm-accent); }
.resource-path { display: flex; gap: 7px; color: var(--pm-text-dim); font-size: 12px; }
```

Use one-line dividers and responsive grid changes; do not introduce floating section cards or page-level decorative effects.

- [ ] **Step 4: Run focused UI tests**

Run: `npm test -- --run tests/powermonitor-tree.test.tsx tests/dashboard-contract.test.tsx tests/dark-mode-styles.test.ts`

Expected: PASS.

- [ ] **Step 5: Run the PowerMonitor test suite and build**

Run: `npm test && npm run build`

Expected: all tests and production build pass.
