// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("PowerMonitor dashboard", () => {
  it("shows Add asset only when the current user can create assets", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices" || path === "/api/v1/assets" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path === "/api/v1/user-capabilities") {
        return json({ capabilities: ["create_assets"] });
      }
      if (path === "/api/v1/resource-invitations") {
        return json({ items: [] });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard />);

    expect(await screen.findByRole("button", { name: "Add asset" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Add device" })).toBeNull();
  });

  it("shows Add device only when the current user can claim devices", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices" || path === "/api/v1/assets" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path === "/api/v1/user-capabilities") {
        return json({ capabilities: ["claim_devices"] });
      }
      if (path === "/api/v1/resource-invitations") {
        return json({ items: [] });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard />);

    expect(await screen.findByRole("button", { name: "Add device" })).toBeTruthy();
  });

  it("renders a concise workspace header, telemetry, alerts, and command surfaces", () => {
    const markup = renderToStaticMarkup(<PowerMonitorDashboard initialDeviceId="meter-1" />);

    expect(markup).toContain("Power Monitor");
    expect(markup).toContain("Assets");
    expect(markup).toContain('aria-label="Resource path"');
    expect(markup).not.toContain("Operational energy view");
    expect(markup).not.toContain("Asset explorer");
    expect(markup).toContain("Telemetry");
    expect(markup).toContain("Alerts");
    expect(markup).toContain("Send command");
    expect(markup).toContain('action="/api/auth/logout"');
    expect(markup).toContain("Sign out");
  });

  it("shows pending invitations from the workspace header", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices" || path === "/api/v1/assets" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path === "/api/v1/resource-invitations") {
        return json({ items: [{
          id: "invite-1",
          permission: "viewer",
          resource_id: "meter-1",
          resource_kind: "device",
          resource_name: "Main meter",
          sender_username: "owner-a",
        }] });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard />);

    const button = await screen.findByRole("button", { name: "Invitations (1)" });
    fireEvent.click(button);

    expect(screen.getByText("Main meter")).toBeTruthy();
    expect(screen.getByText(/owner-a shared viewer access to this device/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Accept invitation for Main meter" })).toBeTruthy();
  });

  it("keeps authorized resources usable when an existing session lacks invitation scope", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices") {
        return json({ items: [{ id: "meter-1", name: "Main meter" }] });
      }
      if (path === "/api/v1/assets" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path === "/api/v1/resource-invitations") {
        return new Response(JSON.stringify({ message: "The current scope cannot access this resource." }), {
          status: 403,
        });
      }
      if (path.startsWith("/api/v1/telemetry/meter-1?")) {
        return json({ items: [] });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialDeviceId="meter-1" />);

    expect(await screen.findByRole("heading", { level: 1, name: "Main meter" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Invitations" })).toBeTruthy();
  });

  it("shows a manager device profile as read-only and never patches it", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices") {
        return json({ items: [{
          device_id: "meter-1",
          device_profile_id: "meter-v1",
          display_name: "Main meter",
          effective_permission: "manager",
        }] });
      }
      if (path === "/api/v1/assets" || path === "/api/v1/alerts" || path === "/api/v1/asset-profiles") {
        return json({ items: [] });
      }
      if (path === "/api/v1/resource-invitations") {
        return json({ items: [] });
      }
      if (path === "/api/v1/device-profiles") {
        return json([
          { id: "meter-v1", name: "Power Meter v1" },
          { id: "inverter-v1", name: "Solar Inverter v1" },
        ]);
      }
      if (path === "/api/v1/devices/meter-1/live-view") {
        return json({
          profile: { id: "meter-v1", name: "Power Meter v1" },
          charts: [{
            aggregation: "last",
            label: "Configured device power",
            metric: "power_w",
            unit: "W",
          }],
        });
      }
      if (path.startsWith("/api/v1/telemetry/meter-1?")) {
        return json({ items: [] });
      }
      if (path === "/api/v1/devices/meter-1/alert-rules") {
        return json({ items: [] });
      }
      if (path === "/api/v1/devices/meter-1" && init?.method === "PATCH") {
        return json({ device_id: "meter-1", device_profile_id: "meter-v1", display_name: "Main meter" });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialDeviceId="meter-1" />);

    expect(await screen.findByText("Configured device power")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Edit device" }));
    expect(screen.queryByLabelText("Device profile")).toBeNull();
    expect((await screen.findAllByText("Power Meter v1")).length).toBeGreaterThan(1);
    fireEvent.click(screen.getByRole("button", { name: "Save configuration" }));

    await waitFor(() => expect(fetcher).toHaveBeenCalledWith(
      "/api/v1/devices/meter-1",
      expect.objectContaining({
        body: JSON.stringify({ asset_id: null, display_name: "Main meter" }),
        method: "PATCH",
      }),
    ));
  });

  it("shows a manager asset profile as read-only and never patches it", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path === "/api/v1/assets") {
        return json({ items: [{
          asset_profile_id: "farm-v1",
          effective_permission: "manager",
          id: "farm-1",
          name: "Main farm",
        }] });
      }
      if (path === "/api/v1/resource-invitations") {
        return json({ items: [] });
      }
      if (path === "/api/v1/asset-profiles") {
        return json([{ id: "farm-v1", name: "Power Farm v1" }]);
      }
      if (path === "/api/v1/device-profiles") {
        return json([]);
      }
      if (path === "/api/v1/assets/farm-1/live-view") {
        return json({
          profile: { id: "farm-v1", name: "Power Farm v1" },
          charts: [{ aggregation: "sum", label: "Farm demand", metric: "power_w", unit: "W" }],
        });
      }
      if (path.startsWith("/api/v1/telemetry?asset_id=farm-1&")) {
        return json({ items: [] });
      }
      if (path === "/api/v1/assets/farm-1" && init?.method === "PATCH") {
        return json({ asset_profile_id: "farm-v1", id: "farm-1", name: "Main farm" });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialAssetId="farm-1" />);

    fireEvent.click(await screen.findByRole("button", { name: "Edit asset" }));
    expect(screen.queryByLabelText("Asset profile")).toBeNull();
    expect((await screen.findAllByText("Power Farm v1")).length).toBeGreaterThan(1);
    fireEvent.click(screen.getByRole("button", { name: "Save configuration" }));

    await waitFor(() => expect(fetcher).toHaveBeenCalledWith(
      "/api/v1/assets/farm-1",
      expect.objectContaining({
        body: JSON.stringify({ name: "Main farm", parent_asset_id: null }),
        method: "PATCH",
      }),
    ));
  });

  it("keeps profile assignment unavailable to a viewer", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices") {
        return json({ items: [{
          device_id: "meter-1",
          display_name: "Main meter",
          effective_permission: "viewer",
        }] });
      }
      if (path === "/api/v1/assets" || path === "/api/v1/alerts" || path === "/api/v1/resource-invitations") {
        return json({ items: [] });
      }
      if (path === "/api/v1/devices/meter-1/live-view") {
        return json({ profile: { id: "meter-v1", name: "Power Meter v1" }, charts: [] });
      }
      if (path.startsWith("/api/v1/telemetry/meter-1?")) {
        return json({ items: [] });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialDeviceId="meter-1" />);

    await screen.findByRole("heading", { level: 1, name: "Main meter" });
    expect(screen.queryByLabelText("Device profile")).toBeNull();
    expect(screen.queryByRole("button", { name: "Edit device" })).toBeNull();
    expect(fetcher).not.toHaveBeenCalledWith(
      "/api/v1/tenant-profile/profiles?kind=device",
      expect.anything(),
    );
    expect(fetcher).toHaveBeenCalledWith(
      "/api/v1/devices/meter-1/live-view",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("refreshes invitations after a decline to invalidate an in-flight workspace read", async () => {
    let invitationReads = 0;
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices" || path === "/api/v1/assets" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path === "/api/v1/resource-invitations") {
        invitationReads += 1;
        return json({ items: invitationReads === 1 ? [{
          id: "invite-1",
          permission: "viewer",
          resource_id: "meter-1",
          resource_kind: "device",
          resource_name: "Main meter",
          sender_username: "owner-a",
        }] : [] });
      }
      if (path === "/api/v1/resource-invitations/invite-1/cancel") {
        return new Response(null, { status: 204 });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard />);

    fireEvent.click(await screen.findByRole("button", { name: "Invitations (1)" }));
    fireEvent.click(screen.getByRole("button", { name: "Decline invitation for Main meter" }));

    await waitFor(() => expect(invitationReads).toBe(2));
    expect(screen.queryByText("Main meter")).toBeNull();
  });

});

function json(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200 });
}
