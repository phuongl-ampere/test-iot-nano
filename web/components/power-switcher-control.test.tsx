// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PowerSwitcherControl } from "./power-switcher-control";

describe("PowerSwitcherControl", () => {
  afterEach(cleanup);

  it("sends a two-way switch-on command for a controller", () => {
    const onCommand = vi.fn();
    render(
      <PowerSwitcherControl
        busy={false}
        commandState={null}
        onCommand={onCommand}
        permission="controller"
        switchState={false}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Relay on" }));

    expect(onCommand).toHaveBeenCalledWith("switch_on", "two_way");
    expect(screen.getByText("Off")).not.toBeNull();
    expect(screen.queryByRole("button", { name: "One-way" })).toBeNull();
  });

  it("shows state without device controls to a viewer", () => {
    render(
      <PowerSwitcherControl
        busy={false}
        commandState="responded"
        onCommand={vi.fn()}
        permission="viewer"
        switchState={true}
      />,
    );

    expect(screen.getByText("On")).not.toBeNull();
    expect(screen.queryByRole("button", { name: "Relay off" })).toBeNull();
  });
});
