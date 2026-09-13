// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DeviceControlPanel } from "../components/device-control-panel";

describe("DeviceControlPanel", () => {
  afterEach(cleanup);

  it("restores two-way relay and brightness controls for capable devices", () => {
    const onCommand = vi.fn();
    render(
      <DeviceControlPanel
        brightnessPct={40}
        busy={false}
        capabilities={["switch", "brightness"]}
        commandState={null}
        onCommand={onCommand}
        switchState={false}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Relay on" }));
    fireEvent.change(screen.getByLabelText("Brightness"), { target: { value: "75" } });
    fireEvent.change(screen.getByLabelText("Brightness value"), { target: { value: "75" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply brightness" }));

    expect(onCommand).toHaveBeenNthCalledWith(1, "switch_on", {}, "two_way");
    expect(onCommand).toHaveBeenNthCalledWith(
      2,
      "set_brightness",
      { brightness_pct: 75 },
      "two_way",
    );
  });
});
