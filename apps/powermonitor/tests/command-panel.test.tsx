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

    expect(onSubmit).toHaveBeenCalledWith("switch_on", {});
  });
});
