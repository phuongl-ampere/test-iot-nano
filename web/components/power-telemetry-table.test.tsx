// @vitest-environment jsdom

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { PowerTelemetryTable } from "./power-telemetry-table";

describe("PowerTelemetryTable", () => {
  it("renders dynamic measurement keys for an unprofiled device", () => {
    render(
      <PowerTelemetryTable
        records={[
          {
            at: "2026-09-07T01:00:00Z",
            measurements: { pressure_kpa: 101.2, valve_open: true },
          },
        ]}
      />,
    );

    expect(screen.getByText("pressure_kpa")).not.toBeNull();
    expect(screen.getByText("valve_open")).not.toBeNull();
    expect(screen.getByText("101.2")).not.toBeNull();
    expect(screen.getByText("true")).not.toBeNull();
  });

  it("shows an empty telemetry state without changing table shape", () => {
    render(<PowerTelemetryTable records={[]} />);

    expect(screen.getByText("No telemetry records in this range.")).not.toBeNull();
  });
});
