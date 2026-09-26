// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("PowerMonitor terminal commands", () => {
  it("refreshes device controls and telemetry after a confirmed command", async () => {
    let deviceRead = 0;
    let telemetryRead = 0;
    let resolveInitialTelemetry!: (response: Response) => void;
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices") {
        deviceRead += 1;
        return json({
          items: [{
            brightness_pct: deviceRead === 1 ? 20 : 75,
            capabilities: ["switch", "brightness"],
            id: "device-1",
            name: "Main meter",
            online: true,
            device_profile_id: "power-meter-v1",
            switch_state: deviceRead > 1,
          }],
        });
      }
      if (
        path === "/api/v1/assets"
        || path === "/api/v1/alerts"
        || path === "/api/v1/resource-invitations"
      ) {
        return json({ items: [] });
      }
      if (path.startsWith("/api/v1/telemetry/device-1?")) {
        telemetryRead += 1;
        if (telemetryRead === 1) {
          return new Promise<Response>((resolve) => {
            resolveInitialTelemetry = resolve;
          });
        }
        return json({ items: [{ at: "2026-09-13T10:00:00Z", power_w: 75 }] });
      }
      if (path === "/api/v1/devices/device-1/live-view") {
        return json({
          profile: { id: "power-meter-v1", name: "Power Meter v1" },
          charts: [{
            aggregation: "last",
            color: "#167b83",
            label: "Active power",
            metric: "power_w",
            unit: "W",
          }],
        });
      }
      if (path === "/api/v1/devices/device-1/commands") {
        return json({ id: "command-1", state: "responded" });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialDeviceId="device-1" />);

    fireEvent.click(await screen.findByRole("button", { name: "Relay on" }));

    await waitFor(() => {
      expect(deviceRead).toBe(2);
      expect((screen.getByRole("button", { name: "Relay on" }) as HTMLButtonElement).disabled).toBe(true);
      expect((screen.getByLabelText("Brightness value") as HTMLInputElement).value).toBe("75");
      expect(fetcher.mock.calls.filter(([path]) => String(path).startsWith("/api/v1/telemetry/device-1?"))).toHaveLength(2);
      expect(screen.getAllByText("75.0 W")).toHaveLength(2);
    });

    await act(async () => {
      resolveInitialTelemetry(json({ items: [{ at: "2026-09-13T10:00:00Z", power_w: 20 }] }));
      await Promise.resolve();
    });

    expect(screen.getAllByText("75.0 W")).toHaveLength(2);
  });
});

function json(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200 });
}
