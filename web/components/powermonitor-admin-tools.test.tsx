// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerMonitorAdminTools } from "./powermonitor-admin-tools";

describe("PowerMonitorAdminTools", () => {
  afterEach(cleanup);

  it("shows domain actions to an administrator", () => {
    render(
      <PowerMonitorAdminTools
        onAddAsset={vi.fn()}
        onAddDevice={vi.fn()}
        onAssignDevice={vi.fn()}
        role="admin"
        selectedDevice={true}
      />,
    );

    expect(screen.getByRole("button", { name: "Add location" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "Add meter" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "Assign meter" })).not.toBeNull();
  });

  it("hides domain actions from a viewer", () => {
    render(
      <PowerMonitorAdminTools
        onAddAsset={vi.fn()}
        onAddDevice={vi.fn()}
        onAssignDevice={vi.fn()}
        role="viewer"
        selectedDevice={false}
      />,
    );

    expect(screen.queryByRole("button", { name: "Add location" })).toBeNull();
  });

  it("uses readable Power Monitor commands and guides assignment selection", () => {
    render(
      <PowerMonitorAdminTools
        onAddAsset={vi.fn()}
        onAddDevice={vi.fn()}
        onAssignDevice={vi.fn()}
        role="admin"
        selectedDevice={false}
      />,
    );

    expect(screen.getByText("Add location")).not.toBeNull();
    expect(screen.getByText("Add meter")).not.toBeNull();
    const assignMeter = screen.getByRole("button", { name: "Assign meter" });
    expect(assignMeter.getAttribute("disabled")).toBe("");
    expect(assignMeter.getAttribute("title")).toBe("Select a meter to assign");
  });
});
