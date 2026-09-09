// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { LightSwitchControl } from "./light-switch-control";

describe("LightSwitchControl", () => {
  afterEach(cleanup);

  it("sends a two-way brightness command for a controller", () => {
    const onCommand = vi.fn();
    render(
      <LightSwitchControl
        brightnessPct={40}
        busy={false}
        commandState={null}
        onCommand={onCommand}
        permission="controller"
        switchState
      />,
    );

    fireEvent.change(screen.getByLabelText("Brightness"), { target: { value: "75" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply brightness" }));

    expect(onCommand).toHaveBeenCalledWith("set_brightness", { brightness_pct: 75 }, "two_way");
  });

  it("does not expose light controls to a viewer", () => {
    render(
      <LightSwitchControl
        brightnessPct={0}
        busy={false}
        commandState={null}
        onCommand={vi.fn()}
        permission="viewer"
        switchState={false}
      />,
    );

    expect(screen.getByText("Off")).not.toBeNull();
    expect(screen.queryByLabelText("Brightness")).toBeNull();
  });
});
