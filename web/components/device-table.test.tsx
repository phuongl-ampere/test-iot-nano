// @vitest-environment jsdom

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { DeviceTable } from "./device-table";

describe("DeviceTable", () => {
  it("shows device state and selects the clicked device", () => {
    const onSelect = vi.fn();

    render(
      <DeviceTable
        devices={[
          {
            device_id: "esp-000123",
            display_name: "Greenhouse sensor",
            online: true,
            last_seen_at: "2026-09-04T10:12:01Z",
          },
        ]}
        selectedDeviceId={null}
        onSelect={onSelect}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /Greenhouse sensor/i }));

    expect(screen.getByText("Online")).toBeTruthy();
    expect(onSelect).toHaveBeenCalledWith("esp-000123");
  });
});
