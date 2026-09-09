// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorTree } from "./powermonitor-tree";

describe("PowerMonitorTree", () => {
  afterEach(cleanup);

  it("nests assigned devices under assets and keeps unassigned devices visible", () => {
    render(
      <PowerMonitorTree
        assets={[
          {
            asset_profile_id: null,
            device_count: 1,
            id: "site-a",
            metadata: {},
            name: "Site A",
            permission: "viewer",
            parent_asset_id: null,
            total_energy_kwh: 0,
            total_power_w: 0,
          },
          {
            asset_profile_id: "panel-profile",
            device_count: 1,
            id: "panel-main",
            metadata: {},
            name: "Panel main",
            permission: "viewer",
            parent_asset_id: "site-a",
            total_energy_kwh: 0,
            total_power_w: 0,
          },
        ]}
        devices={[
          {
            asset_id: "panel-main",
            current_a: null,
            device_id: "meter-1",
            display_name: "Meter tang 1",
            permission: "viewer",
            energy_kwh: null,
            frequency_hz: null,
            gateway_device_id: "gateway-1",
            gateway_status: null,
            is_gateway: false,
            last_seen_at: null,
            child_status: "fresh",
            online: true,
            power_factor: null,
            power_w: null,
            voltage_v: null,
          },
          {
            asset_id: null,
            current_a: null,
            device_id: "orphan-meter",
            display_name: "Unassigned meter",
            permission: "viewer",
            energy_kwh: null,
            frequency_hz: null,
            last_seen_at: null,
            online: false,
            power_factor: null,
            power_w: null,
            voltage_v: null,
          },
        ]}
        onSelectAsset={vi.fn()}
        onSelectDevice={vi.fn()}
        search=""
        selectedAssetId={null}
        selectedDeviceId={null}
      />,
    );

    expect(screen.getByText("Site A")).not.toBeNull();
    expect(screen.getByText("Panel main")).not.toBeNull();
    expect(screen.getByText("Meter tang 1")).not.toBeNull();
    expect(screen.getByText("Unassigned devices")).not.toBeNull();
    expect(screen.getByText("Unassigned meter")).not.toBeNull();
    expect(screen.getByText("No asset profile")).not.toBeNull();
    expect(screen.getByText("Child · fresh")).not.toBeNull();
  });

  it("renders per-row edit actions for a location and meter", () => {
    const editAsset = vi.fn();
    const editDevice = vi.fn();

    render(
      <PowerMonitorTree
        assets={[
          {
            asset_profile_id: "site-profile",
            device_count: 1,
            id: "site-a",
            metadata: {},
            name: "Site A",
            permission: "viewer",
            parent_asset_id: null,
            total_energy_kwh: 0,
            total_power_w: 0,
          },
        ]}
        devices={[
          {
            asset_id: "site-a",
            current_a: null,
            device_id: "meter-1",
            display_name: "Main meter",
            permission: "viewer",
            energy_kwh: null,
            frequency_hz: null,
            last_seen_at: null,
            online: true,
            power_factor: null,
            power_w: null,
            voltage_v: null,
          },
        ]}
        onEditAsset={editAsset}
        onEditDevice={editDevice}
        onSelectAsset={vi.fn()}
        onSelectDevice={vi.fn()}
        search=""
        selectedAssetId={null}
        selectedDeviceId={null}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Edit location Site A" }));
    fireEvent.click(screen.getByRole("button", { name: "Edit meter Main meter" }));

    expect(editAsset).toHaveBeenCalledWith("site-a");
    expect(editDevice).toHaveBeenCalledWith("meter-1");
  });
});
