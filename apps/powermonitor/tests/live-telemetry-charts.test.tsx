import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { LiveTelemetryCharts } from "../components/live-telemetry-charts";

describe("LiveTelemetryCharts", () => {
  it("renders a configured device metric as a labelled line chart", () => {
    const markup = renderToStaticMarkup(
      <LiveTelemetryCharts
        charts={[{
          aggregation: "last",
          color: "#167b83",
          label: "Active power",
          metric: "power_w",
          unit: "W",
        }]}
        isAsset={false}
        points={[
          { at: "2026-09-20T10:00:00Z", measurements: { power_w: 42 } },
          { at: "2026-09-20T10:01:00Z", measurements: { power_w: 64 } },
        ]}
        range="1h"
      />,
    );

    expect(markup).toContain("Active power");
    expect(markup).toContain("42.0 W");
    expect(markup).toContain("64.0 W");
    expect(markup).toContain('aria-label="Active power line chart"');
  });

  it("sums metric values from accessible devices into an asset time bucket", () => {
    const markup = renderToStaticMarkup(
      <LiveTelemetryCharts
        charts={[{
          aggregation: "sum",
          label: "Farm demand",
          metric: "power_w",
          unit: "W",
        }]}
        isAsset
        points={[
          { at: "2026-09-20T10:00:10Z", device_id: "meter-1", measurements: { power_w: 42 } },
          { at: "2026-09-20T10:00:30Z", device_id: "meter-2", measurements: { power_w: 64 } },
        ]}
        range="1h"
      />,
    );

    expect(markup).toContain("Farm demand");
    expect(markup).toContain("106.0 W");
  });
});
