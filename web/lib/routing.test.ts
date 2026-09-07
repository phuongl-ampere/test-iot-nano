import { describe, expect, it } from "vitest";

import { canAccessPath, defaultAppPath, rootPath } from "./routing";

describe("portal route policy", () => {
  it("always sends an administrator from root to management", () => {
    expect(rootPath({
      defaultApp: "/apps/powermonitor",
      grantedApps: ["powermonitor"],
      role: "admin",
      username: "admin",
    })).toBe("/management");
  });

  it("sends a viewer from root to their configured domain app", () => {
    expect(rootPath({
      defaultApp: "/apps/powermonitor",
      grantedApps: ["powermonitor"],
      role: "viewer",
      username: "viewer",
    })).toBe("/apps/powermonitor");
  });

  it("uses Power Monitor when a viewer has no valid default app", () => {
    expect(defaultAppPath({
      defaultApp: "/management",
      grantedApps: ["powermonitor"],
      role: "viewer",
      username: "viewer",
    })).toBe("/apps/powermonitor");
  });

  it("does not authorize a viewer to open management", () => {
    const viewer = {
      defaultApp: "/apps/powermonitor",
      grantedApps: ["powermonitor"],
      role: "viewer" as const,
      username: "viewer",
    };

    expect(canAccessPath(viewer, "/management/entities/devices")).toBe(false);
    expect(canAccessPath(viewer, "/apps/powermonitor")).toBe(true);
  });
});
