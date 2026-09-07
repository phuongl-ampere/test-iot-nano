import { describe, expect, it } from "vitest";

import { managementNavigation } from "./management-app";

describe("management navigation", () => {
  it("includes an Apps tab for installed domain apps", () => {
    expect(managementNavigation).toContainEqual(
      expect.objectContaining({ href: "/management/apps", label: "Apps", section: "apps" }),
    );
  });

  it("includes alerts and notifications management", () => {
    expect(managementNavigation).toContainEqual(
      expect.objectContaining({
        href: "/management/alerts",
        label: "Alerts & Notifications",
        section: "alerts",
      }),
    );
  });
});
