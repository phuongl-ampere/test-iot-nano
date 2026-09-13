import { existsSync, readdirSync, readFileSync } from "node:fs";
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
  /from ["']\.\.\/(?:\.\.\/)*(?:web|platform)/,
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

  it("keeps OAuth and service credentials out of client modules", () => {
    const clientSource = ["app", "components", "lib"]
      .flatMap((directory) => listTypeScriptFiles(join(sourceRoot, directory)))
      .filter((file) => file.endsWith("browser-api.ts") || readFileSync(file, "utf8").includes('"use client"'));

    expect(clientSource).not.toHaveLength(0);
    for (const file of clientSource) {
      expect(readFileSync(file, "utf8"), file + " references a server secret").not.toMatch(
        /OAUTH_(?:CLIENT|SERVICE_CLIENT)_SECRET|SESSION_SECRET|PLATFORM_BASE_URL/,
      );
    }
  });

  it("ships an independent Docker image without platform volume mounts", () => {
    const dockerfile = join(sourceRoot, "Dockerfile");

    expect(existsSync(dockerfile), "PowerMonitor requires its own Dockerfile").toBe(true);
    expect(readFileSync(dockerfile, "utf8")).not.toMatch(
      /(?:VOLUME|--mount=type=bind|volumes:).*(?:platform|iot-nano|web)/i,
    );
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
