// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ResourceEditDrawer } from "../components/resource-edit-drawer";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ResourceEditDrawer", () => {
  it("keeps device configuration in the edit drawer", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices/meter-1/live-view") {
        return json({ charts: [], profile: { id: "power-meter", name: "Power meter" } });
      }
      if (path === "/api/v1/devices/meter-1/alert-rules") {
        return json({ items: [] });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(
      <ResourceEditDrawer
        assets={[{ id: "farm-1", name: "Main farm" }]}
        onClose={vi.fn()}
        onSaved={vi.fn()}
        resource={{
          asset_id: "farm-1",
          id: "meter-1",
          kind: "device",
          name: "Main meter",
          permission: "owner",
          device_profile_id: "power-meter",
          profile_name: "Power meter",
        }}
      />,
    );

    expect(screen.getByRole("heading", { name: "Edit Main meter" })).toBeTruthy();
    expect((screen.getByLabelText("Device name") as HTMLInputElement).value).toBe("Main meter");
    expect((screen.getByLabelText("Assigned asset") as HTMLSelectElement).value).toBe("farm-1");
    expect(screen.queryByLabelText("Device profile")).toBeNull();
    expect(screen.getByText("Power meter")).toBeTruthy();
    expect(screen.queryByLabelText("Active device token")).toBeNull();
    expect(screen.queryByRole("button", { name: "Regenerate token" })).toBeNull();
    expect(screen.getByLabelText("Recipient username")).toBeTruthy();
    expect((await screen.findByLabelText("Alert metric key") as HTMLInputElement).value).toBe("power_w");
  });

  it("uses an asset drawer without token or device alert controls", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/assets/farm-1/live-view") {
        return json({ charts: [], profile: { id: "farm-profile", name: "Power farm" } });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(
      <ResourceEditDrawer
        assets={[
          { id: "root-1", name: "Portfolio" },
          { id: "farm-1", name: "Main farm" },
        ]}
        onClose={vi.fn()}
        onSaved={vi.fn()}
        resource={{
          id: "farm-1",
          kind: "asset",
          name: "Main farm",
          parent_id: "root-1",
          permission: "owner",
          asset_profile_id: "farm-profile",
          profile_name: "Power farm",
        }}
      />,
    );

    expect(screen.getByRole("heading", { name: "Edit Main farm" })).toBeTruthy();
    expect((screen.getByLabelText("Asset name") as HTMLInputElement).value).toBe("Main farm");
    expect((screen.getByLabelText("Parent asset") as HTMLSelectElement).value).toBe("root-1");
    expect(screen.queryByLabelText("Active device token")).toBeNull();
    expect(screen.queryByLabelText("Asset profile")).toBeNull();
    expect(screen.getByText("Power farm")).toBeTruthy();
    expect(screen.queryByLabelText("Alert metric key")).toBeNull();
  });
});

function json(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200 });
}
