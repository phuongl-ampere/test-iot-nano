import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

const styles = readFileSync(resolve(process.cwd(), "app/globals.css"), "utf8");

describe("PowerMonitor dark mode", () => {
  it("uses dark application, workspace, and form surfaces", () => {
    expect(styles).toContain("--pm-canvas: #11181f;");
    expect(styles).toContain("--pm-surface: #1c2732;");
    expect(styles).toContain("--pm-input: #16212b;");
    expect(styles).toContain("color-scheme: dark;");
  });

  it("preserves distinct semantic colors for live and error states", () => {
    expect(styles).toContain("--pm-live: #48d3ad;");
    expect(styles).toContain("--pm-danger: #ff8275;");
    expect(styles).toContain("--pm-warning: #f3bd5b;");
  });

  it("styles the resource hierarchy and workspace path", () => {
    expect(styles).toContain(".tree-children {");
    expect(styles).toContain(".tree-disclosure {");
    expect(styles).toContain("box-shadow: inset 3px 0 0 var(--pm-accent);");
    expect(styles).toContain(".resource-path {");
  });
});
