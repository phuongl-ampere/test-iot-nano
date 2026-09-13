import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getAssetTelemetry,
  getDeviceTelemetry,
  listAlerts,
  listAssets,
  listDevices,
  sendDeviceCommandAndWait,
  submitDeviceCommand,
} from "../lib/browser-api";

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
        body: JSON.stringify({ method: "switch_on", mode: "one_way", params: {} }),
        headers: expect.objectContaining({ "idempotency-key": expect.any(String) }),
        method: "POST",
      }),
    );
    expect(new Headers(fetcher.mock.calls[1][1].headers).get("authorization")).toBeNull();
  });

  it("requests selected asset aggregate telemetry through the generic telemetry collection", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ items: [{ at: "2026-09-13T10:00:00Z", power_w: 42 }] }), {
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetcher);

    await expect(
      getAssetTelemetry("asset-1", "1h", new Date("2026-09-13T10:00:00Z")),
    ).resolves.toEqual([{ at: "2026-09-13T10:00:00Z", power_w: 42 }]);

    expect(fetcher).toHaveBeenCalledWith(
      "/api/v1/telemetry?asset_id=asset-1&aggregate=asset&from=2026-09-13T09%3A00%3A00.000Z&to=2026-09-13T10%3A00%3A00.000Z",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("follows cursor pages for all dashboard resource lists", async () => {
    const fetcher = vi.fn(async (path: string) => {
      const pages: Record<string, unknown> = {
        "/api/v1/devices": { has_more: true, items: [{ id: "device-1" }], next_cursor: "devices-2" },
        "/api/v1/devices?after=devices-2": { has_more: false, items: [{ id: "device-2" }], next_cursor: null },
        "/api/v1/assets": { has_more: true, items: [{ id: "asset-1", name: "Site" }], next_cursor: "assets-2" },
        "/api/v1/assets?after=assets-2": { has_more: false, items: [{ id: "asset-2", name: "Panel" }], next_cursor: null },
        "/api/v1/alerts": { has_more: true, items: [{ id: "alert-1", message: "Open" }], next_cursor: "alerts-2" },
        "/api/v1/alerts?after=alerts-2": { has_more: false, items: [{ id: "alert-2", message: "Closed" }], next_cursor: null },
        "/api/v1/telemetry/device-1?from=2026-09-13T09%3A00%3A00.000Z&to=2026-09-13T10%3A00%3A00.000Z": {
          has_more: true,
          items: [{ at: "2026-09-13T09:30:00Z" }],
          next_cursor: "telemetry-2",
        },
        "/api/v1/telemetry/device-1?from=2026-09-13T09%3A00%3A00.000Z&to=2026-09-13T10%3A00%3A00.000Z&after=telemetry-2": {
          has_more: false,
          items: [{ at: "2026-09-13T09:45:00Z" }],
          next_cursor: null,
        },
        "/api/v1/telemetry?asset_id=asset-1&aggregate=asset&from=2026-09-13T09%3A00%3A00.000Z&to=2026-09-13T10%3A00%3A00.000Z": {
          has_more: true,
          items: [{ at: "2026-09-13T09:30:00Z" }],
          next_cursor: "asset-telemetry-2",
        },
        "/api/v1/telemetry?asset_id=asset-1&aggregate=asset&from=2026-09-13T09%3A00%3A00.000Z&to=2026-09-13T10%3A00%3A00.000Z&after=asset-telemetry-2": {
          has_more: false,
          items: [{ at: "2026-09-13T09:45:00Z" }],
          next_cursor: null,
        },
      };
      return new Response(JSON.stringify(pages[path]), { status: 200 });
    });
    vi.stubGlobal("fetch", fetcher);
    const now = new Date("2026-09-13T10:00:00Z");

    await expect(listDevices()).resolves.toHaveLength(2);
    await expect(listAssets()).resolves.toHaveLength(2);
    await expect(listAlerts()).resolves.toHaveLength(2);
    await expect(getDeviceTelemetry("device-1", "1h", now)).resolves.toHaveLength(2);
    await expect(getAssetTelemetry("asset-1", "1h", now)).resolves.toHaveLength(2);
  });

  it("rejects an endlessly unique cursor stream at the pagination page budget", async () => {
    let page = 0;
    const fetcher = vi.fn(() => {
      page += 1;
      if (page > 25) {
        throw new Error("test exhausted unique cursors");
      }
      return Promise.resolve(new Response(JSON.stringify({
        has_more: true,
        items: [{ id: "device-" + page }],
        next_cursor: "cursor-" + page,
      }), { status: 200 }));
    });
    vi.stubGlobal("fetch", fetcher);

    await expect(listDevices()).rejects.toThrow("Platform pagination page budget exceeded.");
    expect(fetcher).toHaveBeenCalledTimes(25);
  });

  it("retains the repeated-cursor guard", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ has_more: true, items: [{ id: "device-1" }], next_cursor: "again" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ has_more: true, items: [{ id: "device-2" }], next_cursor: "again" }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetcher);

    await expect(listDevices()).rejects.toThrow("Platform pagination cursor repeated.");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("sends a two-way brightness command and polls its lifecycle through getCommand", async () => {
    const wait = vi.fn().mockResolvedValue(undefined);
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "command-1", state: "queued" }), { status: 202 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "command-1", state: "published_to_broker" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ id: "command-1", response: { brightness_pct: 75 }, state: "responded" }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetcher);

    await expect(
      sendDeviceCommandAndWait(
        "meter-1",
        "set_brightness",
        { brightness_pct: 75 },
        "two_way",
        { wait },
      ),
    ).resolves.toMatchObject({ response: { brightness_pct: 75 }, state: "responded" });

    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "/api/v1/devices/meter-1/commands",
      expect.objectContaining({
        body: JSON.stringify({
          method: "set_brightness",
          mode: "two_way",
          params: { brightness_pct: 75 },
        }),
        method: "POST",
      }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "/api/v1/commands/command-1",
      expect.objectContaining({ credentials: "same-origin" }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      3,
      "/api/v1/commands/command-1",
      expect.objectContaining({ credentials: "same-origin" }),
    );
    expect(wait).toHaveBeenCalledTimes(2);
  });
});
