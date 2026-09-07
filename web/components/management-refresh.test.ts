import { describe, expect, it } from "vitest";

import { managementDeviceRefreshMilliseconds } from "./management-panels";

describe("management device refresh", () => {
  it("refreshes device telemetry state every five seconds", () => {
    expect(managementDeviceRefreshMilliseconds).toBe(5_000);
  });
});
