// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorTree } from "../components/powermonitor-tree";

describe("PowerMonitorTree", () => {
  afterEach(cleanup);

  it("nests generic devices under assets and leaves unassigned devices visible", () => {
    const onSelectDevice = vi.fn();
    render(
      <PowerMonitorTree
        assets={[
          { id: "site-a", name: "Site A", parent_id: null },
          { id: "panel-a", name: "Main panel", parent_id: "site-a" },
        ]}
        devices={[
          { asset_id: "panel-a", id: "meter-1", name: "Main meter", online: true },
          { asset_id: null, id: "meter-2", name: "Unassigned meter", online: false },
        ]}
        onSelectAsset={vi.fn()}
        onSelectDevice={onSelectDevice}
        selectedAssetId={null}
        selectedDeviceId={null}
      />,
    );

    expect(screen.getByText("Site A")).not.toBeNull();
    expect(screen.getByText("Main panel")).not.toBeNull();
    expect(screen.getByText("Main meter")).not.toBeNull();
    expect(screen.getByText("Unassigned devices")).not.toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Select Main meter" }));
    expect(onSelectDevice).toHaveBeenCalledWith("meter-1");
  });
});
