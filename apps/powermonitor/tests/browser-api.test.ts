import { afterEach, describe, expect, it, vi } from "vitest";

import { listDevices, submitDeviceCommand } from "../lib/browser-api";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("browser PowerMonitor API", () => {
  it("uses same-origin generic BFF resources without a bearer token", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ items: [{ id: "meter-1", name: "Main meter" }] }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "command-1", state: "queued" }), { status: 202 }),
      );
    vi.stubGlobal("fetch", fetcher);

    await expect(listDevices()).resolves.toEqual([{ id: "meter-1", name: "Main meter" }]);
    await expect(submitDeviceCommand("meter-1", "switch_on", {})).resolves.toMatchObject({
      id: "command-1",
      state: "queued",
    });

    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "/api/v1/devices",
      expect.objectContaining({
        cache: "no-store",
        credentials: "same-origin",
      }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "/api/v1/devices/meter-1/commands",
      expect.objectContaining({
        body: JSON.stringify({ method: "switch_on", params: {} }),
        headers: expect.objectContaining({ "idempotency-key": expect.any(String) }),
        method: "POST",
      }),
    );
    expect(new Headers(fetcher.mock.calls[1][1].headers).get("authorization")).toBeNull();
  });
});
