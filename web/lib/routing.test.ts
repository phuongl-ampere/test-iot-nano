import { describe, expect, it } from "vitest";

import type { UserSession } from "./api";
import { canAccessPath, rootPath } from "./routing";

describe("account-class routing", () => {
  it("sends the system account only to system configuration", () => {
    const system = {
      role: "admin",
      accountClass: "system",
      username: "system",
      defaultApp: "/apps/powermonitor",
      grantedApps: [],
    } as UserSession;

    expect(rootPath(system)).toBe("/management/settings");
    expect(canAccessPath(system, "/management/settings")).toBe(true);
    expect(canAccessPath(system, "/management")).toBe(false);
    expect(canAccessPath(system, "/apps/powermonitor")).toBe(false);
  });

  it("keeps admin out of system configuration", () => {
    const admin = {
      role: "admin",
      accountClass: "admin",
      username: "admin",
      defaultApp: "/apps/powermonitor",
      grantedApps: ["powermonitor"],
    } as UserSession;

    expect(rootPath(admin)).toBe("/management");
    expect(canAccessPath(admin, "/management/entities/devices")).toBe(true);
    expect(canAccessPath(admin, "/management/settings")).toBe(false);
  });
});
