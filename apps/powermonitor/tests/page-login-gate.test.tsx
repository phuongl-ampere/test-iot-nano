// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

const { hasPowerMonitorSession } = vi.hoisted(() => ({
  hasPowerMonitorSession: vi.fn(),
}));

vi.mock("../lib/page-session", () => ({ hasPowerMonitorSession }));
vi.mock("../components/powermonitor-dashboard", () => ({
  PowerMonitorDashboard: ({
    initialAssetId,
    initialDeviceId,
  }: {
    initialAssetId?: string;
    initialDeviceId?: string;
  }) => (
    <div data-asset-id={initialAssetId} data-device-id={initialDeviceId} data-testid="dashboard" />
  ),
}));
vi.mock("../components/powermonitor-login-gate", () => ({
  PowerMonitorLoginGate: () => <div data-testid="login-gate" />,
}));

import AssetDetailPage from "../app/assets/[assetId]/page";
import DeviceDetailPage from "../app/devices/[deviceId]/page";
import PowerMonitorPage from "../app/page";

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("PowerMonitor browser entry pages", () => {
  it("renders the login gate for every unauthenticated entry page", async () => {
    hasPowerMonitorSession.mockResolvedValue(false);

    render(await PowerMonitorPage());
    expect(screen.getByTestId("login-gate")).toBeTruthy();
    expect(screen.queryByTestId("dashboard")).toBeNull();
    cleanup();

    render(await DeviceDetailPage({ params: Promise.resolve({ deviceId: "device-1" }) }));
    expect(screen.getByTestId("login-gate")).toBeTruthy();
    expect(screen.queryByTestId("dashboard")).toBeNull();
    cleanup();

    render(await AssetDetailPage({ params: Promise.resolve({ assetId: "asset-1" }) }));
    expect(screen.getByTestId("login-gate")).toBeTruthy();
    expect(screen.queryByTestId("dashboard")).toBeNull();
    expect(hasPowerMonitorSession).toHaveBeenCalledTimes(3);
  });

  it("renders the dashboard for an authenticated root page", async () => {
    hasPowerMonitorSession.mockResolvedValue(true);

    render(await PowerMonitorPage());

    expect(screen.getByTestId("dashboard")).toBeTruthy();
    expect(screen.queryByTestId("login-gate")).toBeNull();
  });

  it("preserves a device ID for an authenticated detail page", async () => {
    hasPowerMonitorSession.mockResolvedValue(true);

    render(await DeviceDetailPage({ params: Promise.resolve({ deviceId: "device-1" }) }));

    expect(screen.getByTestId("dashboard").getAttribute("data-device-id")).toBe("device-1");
    expect(screen.queryByTestId("login-gate")).toBeNull();
  });

  it("preserves an asset ID for an authenticated detail page", async () => {
    hasPowerMonitorSession.mockResolvedValue(true);

    render(await AssetDetailPage({ params: Promise.resolve({ assetId: "asset-1" }) }));

    expect(screen.getByTestId("dashboard").getAttribute("data-asset-id")).toBe("asset-1");
    expect(screen.queryByTestId("login-gate")).toBeNull();
  });
});
