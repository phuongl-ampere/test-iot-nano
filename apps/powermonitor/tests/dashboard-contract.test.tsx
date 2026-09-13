import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";

describe("PowerMonitor dashboard", () => {
  it("renders the external explorer, telemetry, alerts, and command surfaces", () => {
    const markup = renderToStaticMarkup(<PowerMonitorDashboard initialDeviceId="meter-1" />);

    expect(markup).toContain("Power Monitor");
    expect(markup).toContain("Asset explorer");
    expect(markup).toContain("Telemetry");
    expect(markup).toContain("Alerts");
    expect(markup).toContain("Send command");
  });
});
