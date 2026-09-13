import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

const sourceRoot = join(import.meta.dirname, "..");
const forbidden = [
  /DATABASE_URL/,
  /postgres:\/\//,
  /sqlite/i,
  /IOT_NANO_INTERNAL_DIR/,
  /\/internal\//,
  /from ["'](?:@[^"']+\/)?(?:platform|web|iot-nano)/,
];

describe("application isolation", () => {
  it("does not couple application source to platform storage or workspace packages", () => {
    const sourceFiles = ["app", "components", "lib"]
      .flatMap((directory) => listTypeScriptFiles(join(sourceRoot, directory)));

    for (const file of sourceFiles) {
      const source = readFileSync(file, "utf8");
      for (const pattern of forbidden) {
        expect(source, `${file} matches ${pattern}`).not.toMatch(pattern);
      }
    }
  });
});

function listTypeScriptFiles(directory: string): string[] {
  try {
    return readdirSync(directory, { withFileTypes: true })
      .flatMap((entry: { isDirectory(): boolean; name: string }) => {
        const path = join(directory, entry.name);
        return entry.isDirectory()
          ? listTypeScriptFiles(path)
          : /\.(ts|tsx)$/.test(entry.name)
            ? [path]
            : [];
      });
  } catch {
    return [];
  }
}
