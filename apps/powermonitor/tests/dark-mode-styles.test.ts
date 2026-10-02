import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

const styles = readFileSync(resolve(process.cwd(), "app/globals.css"), "utf8");

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
    expect(styles).toContain(".range-control,\n.command-mode,\n.relay-actions {");
  });

  it("keeps the navy rail readable and device assignment controls within a narrow screen", () => {
    expect(styles).toMatch(/\.explorer\s*\{[\s\S]*?--pm-live: #7ee2b8;/);
    expect(styles).toMatch(/\.unassigned-device-actions\s*\{[\s\S]*?flex-wrap: wrap;/);
    expect(styles).toMatch(/\.unassigned-device-actions select\s*\{[\s\S]*?width: 100%;/);
  });

  it("styles the resource hierarchy and workspace path", () => {
    expect(styles).toContain(".tree-children {");
    expect(styles).toContain(".tree-disclosure {");
    expect(styles).toContain("box-shadow: inset 3px 0 0 var(--pm-accent);");
    expect(styles).toContain(".resource-path {");
  });
});
