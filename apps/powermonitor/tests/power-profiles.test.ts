import { describe, expect, it } from "vitest";

import { powerProfilePresentation } from "../lib/power-profiles";

describe("PowerMonitor profile presentation", () => {
  it("renders a Power Meter as an instantaneous active-power chart", () => {
    expect(powerProfilePresentation("device", "Power Meter")).toMatchObject({
      label: "Power Meter",
      charts: [{ aggregation: "last", metric: "power_w", unit: "W" }],
    });
  });

  it("renders Power Farm demand as an aggregated chart", () => {
    expect(powerProfilePresentation("asset", "Power Farm")).toMatchObject({
      label: "Power Farm",
      charts: [{ aggregation: "sum", metric: "power_w", unit: "W" }],
    });
  });

  it("does not impose PowerMonitor UI on an unknown profile", () => {
    expect(powerProfilePresentation("asset", "Other")).toBeNull();
  });
});

