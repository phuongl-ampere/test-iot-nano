# Power Monitor YMS Style Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the approved light YMS design to the Power Monitor without changing its behaviour.

**Architecture:** Preserve the existing single global stylesheet and component markup. Add the five YMS source tokens and remap the established `--pm-*` semantic tokens to them, then add focused component overrides for cards, typography, control alignment, responsiveness, and keyboard focus.

**Tech Stack:** Next.js 16, React 19, CSS custom properties, Vitest 5.

## Global Constraints

- Use these exact source tokens: `--yms-canvas: #eef1f2`, `--yms-panel: #ffffff`, `--yms-primary: #253957`, `--yms-accent: #ff775c`, and `--yms-radius: 9px`.
- Keep the explorer rail navy and all app behaviour, routes, labels, and component markup unchanged.
- Use `Iowan Old Style` then Georgia serif fallbacks only for display headings; use a system sans-serif stack for body and controls.
- Keep online, warning, and error colour roles distinct; use coral for visible focus and restrained active/selected cues.
- Maintain the existing 760px layout transition and prevent action/control overflow at narrow widths.

---

### Task 1: Establish and verify the YMS visual contract

**Files:**
- Modify: `apps/powermonitor/tests/dark-mode-styles.test.ts`
- Modify: `apps/powermonitor/app/globals.css:1-1296`

**Interfaces:**
- Consumes: the stylesheet loaded by `apps/powermonitor/app/layout.tsx` and the Vitest command `npm test` from `apps/powermonitor`.
- Produces: YMS design tokens and style rules consumed by the current Power Monitor component class names; no TypeScript or API interface changes.

- [x] **Step 1: Write the failing stylesheet contract**

Replace the dark-surface assertions in `apps/powermonitor/tests/dark-mode-styles.test.ts` with this test body. Retain the final hierarchy/path test because the UI structure must not regress.

```ts
describe("PowerMonitor YMS style", () => {
  it("declares the approved light workspace tokens", () => {
    expect(styles).toContain("--yms-canvas: #eef1f2;");
    expect(styles).toContain("--yms-panel: #ffffff;");
    expect(styles).toContain("--yms-primary: #253957;");
    expect(styles).toContain("--yms-accent: #ff775c;");
    expect(styles).toContain("--yms-radius: 9px;");
    expect(styles).toContain("--pm-sidebar: var(--yms-primary);");
    expect(styles).toContain("color-scheme: light;");
  });

  it("uses serif display headings and coral keyboard focus", () => {
    expect(styles).toContain('font-family: "Iowan Old Style", Iowan Old Style, Georgia, serif;');
    expect(styles).toContain("outline: 3px solid var(--yms-accent);");
  });

  it("gives panels and responsive action groups the approved treatment", () => {
    expect(styles).toContain("border-radius: var(--yms-radius);");
    expect(styles).toContain("box-shadow: var(--yms-panel-shadow);");
    expect(styles).toContain(".workspace-actions {");
    expect(styles).toContain("flex-wrap: wrap;");
  });
});
```

- [x] **Step 2: Run the contract to verify it fails for the intended reason**

Run: `npm test -- dark-mode-styles.test.ts`

Expected: FAIL because `--yms-canvas: #eef1f2;` is absent from the current dark stylesheet.

- [x] **Step 3: Implement the stylesheet-only restyle**

At the top of `apps/powermonitor/app/globals.css`, replace the dark token values with the following complete source and semantic token block:

```css
:root {
  --yms-canvas: #eef1f2;
  --yms-panel: #ffffff;
  --yms-primary: #253957;
  --yms-accent: #ff775c;
  --yms-radius: 9px;
  --yms-panel-shadow: 0 2px 10px rgb(37 57 87 / 7%);
  --yms-font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  --yms-font-serif: "Iowan Old Style", Iowan Old Style, Georgia, serif;
  --pm-canvas: var(--yms-canvas);
  --pm-sidebar: var(--yms-primary);
  --pm-surface: var(--yms-panel);
  --pm-surface-hover: #314866;
  --pm-input: #f8fafb;
  --pm-border: #d9e0e4;
  --pm-border-subtle: #e8edef;
  --pm-border-strong: #bdc8d0;
  --pm-text: #253957;
  --pm-text-muted: #53647a;
  --pm-text-dim: #738197;
  --pm-accent: var(--yms-primary);
  --pm-accent-hover: #1c2d48;
  --pm-accent-soft: #fff0ec;
  --pm-accent-bright: var(--yms-accent);
  --pm-live: #218765;
  --pm-offline: #7a8796;
  --pm-warning: #b87318;
  --pm-on-warning: #253957;
  --pm-danger: #bd4a42;
  --pm-danger-soft: #fff0ee;
  --pm-danger-text: #923a33;
  --pm-on-accent: #ffffff;
  color-scheme: light;
  color: var(--pm-text);
  background: var(--pm-canvas);
  font-family: var(--yms-font-sans);
}
```

Change the existing focus rule to `outline: 3px solid var(--yms-accent);`. Add `font-family: var(--yms-font-serif);` to the brand lockup, workspace h1, section h2, command h2, device-control h2, resource-edit h2, drawer h3, and profile-line-chart h3 heading selectors.

Append a final override block so existing markup receives the new cards and ordered controls without duplication:

```css
.unassigned-device-panel,
.invitation-panel,
.claim-device-panel,
.telemetry-section,
.asset-details,
.resource-profile,
.resource-share,
.alert-panel,
.profile-line-chart,
.powermonitor-login-panel,
.resource-edit-drawer {
  border: 1px solid var(--pm-border);
  border-radius: var(--yms-radius);
  background: var(--yms-panel);
  box-shadow: var(--yms-panel-shadow);
}

.workspace-actions,
.invitation-actions,
.share-form,
.profile-form,
.command-controls,
.drawer-form-actions {
  flex-wrap: wrap;
}

.workspace-actions > button,
.workspace-actions > form > button,
.invitation-actions button,
.share-form button,
.profile-form button,
.command-controls > button,
.brightness-control button,
.drawer-form button,
.token-controls button {
  min-height: 38px;
  border-radius: var(--yms-radius);
}

@media (max-width: 760px) {
  .workspace-actions,
  .invitation-actions,
  .share-form,
  .profile-form,
  .command-controls,
  .drawer-form-actions {
    align-items: stretch;
  }
}
```

Keep the existing selector-specific layout and media rules, adjusting their old 4px–7px radii to `var(--yms-radius)` when they are panel, form, or control surfaces. Preserve square data-table structure where rounding would not communicate a surface.

- [x] **Step 4: Run the focused contract and full frontend suite**

Run: `npm test -- dark-mode-styles.test.ts && npm test`

Expected: both commands exit 0, with the first confirming the styling contract and the second confirming no Power Monitor interaction contract regressed.

- [x] **Step 5: Build the frontend and inspect the change**

Run: `npm run build`

Expected: Next.js build exits 0. Review the working tree diff to confirm the only production code change is `app/globals.css` and that the test reflects the new light style.

- [x] **Step 6: Commit the completed UI restyle**

```bash
git add apps/powermonitor/app/globals.css apps/powermonitor/tests/dark-mode-styles.test.ts
git commit -m "style: apply YMS theme to Power Monitor"
```
