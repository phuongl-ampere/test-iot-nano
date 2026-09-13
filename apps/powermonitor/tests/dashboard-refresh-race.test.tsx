// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("PowerMonitor refresh ownership", () => {
  it("does not let an older manual refresh overwrite a terminal-command refresh", async () => {
    const staleDevices = deferred<Response>();
    const staleAssets = deferred<Response>();
    const staleAlerts = deferred<Response>();
    let deviceRead = 0;
    let assetRead = 0;
    let alertRead = 0;
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices") {
        deviceRead += 1;
        if (deviceRead === 2) return staleDevices.promise;
        return json({ items: [device(deviceRead > 1)] });
      }
      if (path === "/api/v1/assets") {
        assetRead += 1;
        return assetRead === 2 ? staleAssets.promise : json({ items: [] });
      }
      if (path === "/api/v1/alerts") {
        alertRead += 1;
        return alertRead === 2 ? staleAlerts.promise : json({ items: [] });
      }
      if (path.startsWith("/api/v1/telemetry/device-1?")) {
        return json({ items: [{ at: "2026-09-13T10:00:00Z", power_w: deviceRead > 1 ? 75 : 20 }] });
      }
      if (path === "/api/v1/devices/device-1/commands") {
        return json({ id: "command-1", state: "responded" });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialDeviceId="device-1" />);
    await screen.findByRole("button", { name: "Relay on" });

    fireEvent.click(screen.getByRole("button", { name: "Refresh Power Monitor" }));
    fireEvent.click(screen.getByRole("button", { name: "Relay on" }));

    await waitFor(() => {
      expect(deviceRead).toBe(3);
      expect((screen.getByLabelText("Brightness value") as HTMLInputElement).value).toBe("75");
    });

    await act(async () => {
      staleDevices.resolve(json({ items: [device(false)] }));
      staleAssets.resolve(json({ items: [] }));
      staleAlerts.resolve(json({ items: [] }));
      await Promise.resolve();
    });

    expect((screen.getByRole("button", { name: "Relay on" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByLabelText("Brightness value") as HTMLInputElement).value).toBe("75");
  });

  it("uses the current range when a terminal command refreshes telemetry", async () => {
    let deviceRead = 0;
    const telemetryPaths: string[] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = typeof input === "string" ? input : input.toString();
      if (path === "/api/v1/devices") {
        deviceRead += 1;
        return json({ items: [device(deviceRead > 1)] });
      }
      if (path === "/api/v1/assets" || path === "/api/v1/alerts") {
        return json({ items: [] });
      }
      if (path.startsWith("/api/v1/telemetry/device-1?")) {
        telemetryPaths.push(path);
        return json({ items: [{ at: "2026-09-13T10:00:00Z", power_w: 42 }] });
      }
      if (path === "/api/v1/devices/device-1/commands") {
        return json({ id: "command-1", state: "queued" });
      }
      if (path === "/api/v1/commands/command-1") {
        return json({ id: "command-1", state: "responded" });
      }
      throw new Error("Unexpected BFF request: " + path);
    });
    vi.stubGlobal("fetch", fetcher);

    render(<PowerMonitorDashboard initialDeviceId="device-1" />);
    await screen.findByRole("button", { name: "Relay on" });

    vi.useFakeTimers();
    fireEvent.click(screen.getByRole("button", { name: "Relay on" }));
    fireEvent.click(screen.getByRole("button", { name: "24h" }));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1_000);
      await Promise.resolve();
    });
    vi.useRealTimers();

    await waitFor(() => {
      expect(deviceRead).toBe(2);
      const lastPath = telemetryPaths.at(-1);
      expect(lastPath).toBeDefined();
      expect(telemetryDuration(lastPath as string)).toBe(24 * 60 * 60 * 1_000);
    });
  });
});

function device(on: boolean) {
  return {
    brightness_pct: on ? 75 : 20,
    capabilities: ["switch", "brightness"],
    id: "device-1",
    name: "Main meter",
    online: true,
    switch_state: on,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function json(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200 });
}

function telemetryDuration(path: string): number {
  const url = new URL(path, "https://powermonitor.example.test");
  return new Date(url.searchParams.get("to") as string).getTime()
    - new Date(url.searchParams.get("from") as string).getTime();
}
