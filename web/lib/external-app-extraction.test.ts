import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

const webRoot = join(import.meta.dirname, "..");

describe("external application extraction", () => {
  it("keeps the platform console free of embedded PowerMonitor code", () => {
    expect(existsSync(join(webRoot, "app", "apps", "powermonitor"))).toBe(false);

    const runtimeFiles = ["app", "components", "lib"]
      .flatMap((directory) => listRuntimeFiles(join(webRoot, directory)));
    for (const file of runtimeFiles) {
      expect(readFileSync(file, "utf8"), file + " retains an embedded PowerMonitor surface").not.toMatch(
        /\/apps\/powermonitor|\/api\/apps\/powermonitor|PowerMonitor|powermonitor-/,
      );
    }

    expect(readFileSync(join(webRoot, "app", "globals.css"), "utf8")).not.toMatch(
      /powermonitor|power-switcher|light-switch/i,
    );
  });
});

function listRuntimeFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      return listRuntimeFiles(path);
    }
    return /\.(ts|tsx)$/.test(entry.name) && !/\.test\.(ts|tsx)$/.test(entry.name) ? [path] : [];
  });
}
