// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("PowerMonitor asset selection", () => {
  it("keeps an asset page target selected and loads aggregate telemetry through the BFF", async () => {
    const fetcher = installDashboardFetch();
    render(<PowerMonitorDashboard initialAssetId="asset-1" />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { level: 1, name: "Site A" })).not.toBeNull();
      expect(fetcher).toHaveBeenCalledWith(
        expect.stringMatching(/^\/api\/v1\/telemetry\?asset_id=asset-1&aggregate=asset&/),
        expect.any(Object),
      );
    });

    expect(fetcher.mock.calls.map(([path]) => path)).not.toContain(
      expect.stringMatching("/api/v1/telemetry/device-1"),
    );
  });

  it("loads aggregate telemetry after a manual asset selection", async () => {
    const fetcher = installDashboardFetch();
    render(<PowerMonitorDashboard />);

    await screen.findByRole("button", { name: "Select Site A" });
    fireEvent.click(screen.getByRole("button", { name: "Select Site A" }));

    await waitFor(() => {
      expect(fetcher).toHaveBeenCalledWith(
        expect.stringMatching(/^\/api\/v1\/telemetry\?asset_id=asset-1&aggregate=asset&/),
        expect.any(Object),
      );
    });
  });
});

function installDashboardFetch() {
  const fetcher = vi.fn(async (input: RequestInfo | URL) => {
    const path = typeof input === "string" ? input : input.toString();
    if (path === "/api/v1/devices") {
      return json({ items: [{ asset_id: "asset-1", id: "device-1", name: "Main meter", online: true }] });
    }
    if (path === "/api/v1/assets") {
      return json({ items: [{ id: "asset-1", name: "Site A", parent_id: null }] });
    }
    if (path === "/api/v1/alerts") {
      return json({ items: [] });
    }
    if (path.startsWith("/api/v1/telemetry?asset_id=asset-1&aggregate=asset&")) {
      return json({ items: [{ at: "2026-09-13T10:00:00Z", power_w: 42 }] });
    }
    if (path.startsWith("/api/v1/telemetry/device-1?")) {
      return json({ items: [{ at: "2026-09-13T10:00:00Z", power_w: 9 }] });
    }
    throw new Error("Unexpected BFF request: " + path);
  });
  vi.stubGlobal("fetch", fetcher);
  return fetcher;
}

function json(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200 });
}
