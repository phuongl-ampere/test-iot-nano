// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AlertPanel } from "../components/alert-panel";

describe("AlertPanel", () => {
  afterEach(cleanup);

  it("acknowledges an open generic alert without exposing rule administration", () => {
    const onAcknowledge = vi.fn();
    render(
      <AlertPanel
        alerts={[
          {
            id: "alert-1",
            message: "Power threshold exceeded",
            severity: "warning",
            status: "open",
          },
        ]}
        onAcknowledge={onAcknowledge}
        workingId={null}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Acknowledge Power threshold exceeded" }));

    expect(onAcknowledge).toHaveBeenCalledWith("alert-1");
    expect(screen.queryByRole("button", { name: /Edit/i })).toBeNull();
  });
});
