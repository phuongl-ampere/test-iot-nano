// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CommandPanel } from "../components/command-panel";

describe("CommandPanel", () => {
  afterEach(cleanup);

  it("submits a selected command through the generic device command flow", () => {
    const onSubmit = vi.fn();
    render(<CommandPanel busy={false} onSubmit={onSubmit} />);

    fireEvent.change(screen.getByLabelText("Command"), { target: { value: "switch_on" } });
    fireEvent.click(screen.getByRole("button", { name: "Send command" }));

    expect(onSubmit).toHaveBeenCalledWith("switch_on", {}, "one_way");
  });

  it("submits a two-way command and shows its device response", () => {
    const onSubmit = vi.fn();
    render(
      <CommandPanel
        busy={false}
        onSubmit={onSubmit}
        response={{ ok: true, result: { reboot_count: 1 } }}
      />,
    );

    fireEvent.change(screen.getByLabelText("Command"), { target: { value: "reboot" } });
    fireEvent.click(screen.getByRole("button", { name: "Two-way" }));
    fireEvent.click(screen.getByRole("button", { name: "Send command" }));

    expect(onSubmit).toHaveBeenCalledWith("reboot", {}, "two_way");
    expect(screen.getByLabelText("Command response").textContent).toContain('"reboot_count": 1');
  });
});
