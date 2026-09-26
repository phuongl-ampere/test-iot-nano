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
      if (path === "/api/v1/application-domain/profiles?kind=device") {
        return json([{ id: "power-meter", name: "Power meter" }]);
      }
      if (path === "/api/v1/devices/meter-1/live-view") {
        return json({ charts: [], profile: { id: "power-meter", name: "Power meter" } });
      }
      if (path === "/api/v1/devices/meter-1/token") {
        return json({ token: "iotn_test_token" });
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
        }}
      />,
    );

    expect(screen.getByRole("heading", { name: "Edit Main meter" })).toBeTruthy();
    expect((screen.getByLabelText("Device name") as HTMLInputElement).value).toBe("Main meter");
    expect((screen.getByLabelText("Assigned asset") as HTMLSelectElement).value).toBe("farm-1");
    expect(await screen.findByLabelText("Device profile")).toBeTruthy();
    expect((await screen.findByLabelText("Active device token") as HTMLInputElement).value).toBe("iotn_test_token");
    expect(screen.getByRole("button", { name: "Regenerate token" })).toBeTruthy();
    expect(screen.getByLabelText("Recipient username")).toBeTruthy();
    expect((screen.getByLabelText("Alert metric key") as HTMLInputElement).value).toBe("power_w");
  });

  it("uses an asset drawer without token or device alert controls", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/application-domain/profiles?kind=asset") {
        return json([{ id: "farm-profile", name: "Power farm" }]);
      }
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
        }}
      />,
    );

    expect(screen.getByRole("heading", { name: "Edit Main farm" })).toBeTruthy();
    expect((screen.getByLabelText("Asset name") as HTMLInputElement).value).toBe("Main farm");
    expect((screen.getByLabelText("Parent asset") as HTMLSelectElement).value).toBe("root-1");
    expect(screen.queryByLabelText("Active device token")).toBeNull();
    expect(screen.queryByLabelText("Alert metric key")).toBeNull();
  });
});

function json(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200 });
}
