import { afterEach, describe, expect, it, vi } from "vitest";

import {
  acceptResourceInvitation,
  archiveDeviceAlertRule,
  cancelResourceInvitation,
  claimDevice,
  createAsset,
  createDeviceAlertRule,
  createDeviceResourceInvitation,
  getAssetLiveView,
  getAssetTelemetry,
  getDeviceLiveView,
  getDeviceTelemetry,
  listDeviceAlertRules,
  listResourceProfiles,
  listTenantProfiles,
  listAlerts,
  listAssets,
  listDevices,
  listResourceInvitations,
  sendDeviceCommandAndWait,
  submitDeviceCommand,
  updateAsset,
  updateDevice,
  updateDeviceAlertRule,
} from "../lib/browser-api";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("browser PowerMonitor API", () => {
  it("creates an asset with an optional parent and object metadata", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({
        id: "field-1",
        metadata: {},
        name: "North field",
        parent_asset_id: "farm-1",
      }), { status: 201 }),
    );
    vi.stubGlobal("fetch", fetcher);

    await expect(createAsset({ name: "North field", parent_asset_id: "farm-1" })).resolves.toMatchObject({
      id: "field-1",
      name: "North field",
      parent_id: "farm-1",
    });

    expect(fetcher).toHaveBeenCalledWith(
      "/api/v1/assets",
      expect.objectContaining({ method: "POST" }),
    );
    expect(JSON.parse(fetcher.mock.calls[0][1].body as string)).toEqual({
      metadata: {},
      name: "North field",
      parent_asset_id: "farm-1",
    });
  });

  it("sends a device claim code only in the JSON request body", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ device_id: "pairing-device", display_name: "Pairing device" }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetcher);

    await expect(claimDevice("PM-PAIRING-001", "ABCD-2345-EFGH")).resolves.toMatchObject({
      id: "pairing-device",
      name: "Pairing device",
    });

    expect(fetcher).toHaveBeenCalledWith(
      "/api/v1/devices/claim",
      expect.objectContaining({
        body: JSON.stringify({ serial_number: "PM-PAIRING-001", code: "ABCD-2345-EFGH" }),
        method: "POST",
      }),
    );
    expect(String(fetcher.mock.calls[0][0])).not.toContain("ABCD-2345-EFGH");
  });

  it("normalizes authorized public device fields while preserving top-level device state", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({
        items: [{
          asset_id: "asset-1",
          device_profile_id: "profile-meter",
          device_id: "meter-1",
          display_name: "Main meter",
          effective_permission: "viewer",
          brightness_pct: 72,
          capabilities: ["switch", "brightness"],
          metadata: { capabilities: ["switch"], switch_state: true },
          online: true,
          switch_state: false,
        }],
      }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetcher);

    await expect(listDevices()).resolves.toEqual([{
      asset_id: "asset-1",
      brightness_pct: 72,
      capabilities: ["switch", "brightness"],
      id: "meter-1",
      device_profile_id: "profile-meter",
      name: "Main meter",
      online: true,
      permission: "viewer",
      switch_state: false,
    }]);
  });

  it("preserves resource profile IDs and loads the matching public catalog", async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        items: [{ asset_profile_id: "profile-farm", id: "farm-1", name: "Power Farm 1" }],
      }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        items: [{ id: "profile-farm", name: "Power Farm" }],
      }), { status: 200 }));
    vi.stubGlobal("fetch", fetcher);

    await expect(listAssets()).resolves.toEqual([{
      asset_profile_id: "profile-farm",
      id: "farm-1",
      name: "Power Farm 1",
    }]);
    await expect(listResourceProfiles("asset")).resolves.toEqual([{
      id: "profile-farm",
      name: "Power Farm",
    }]);
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "/api/v1/asset-profiles",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

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

  it("sends resource invitation actions through the same-origin BFF", async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [{
        id: "invite-1",
        permission: "viewer",
        resource_id: "meter-1",
        resource_kind: "device",
        resource_name: "Main meter",
        sender_username: "owner-a",
      }] }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        id: "invite-2",
        permission: "manager",
        resource_id: "meter-1",
        resource_kind: "device",
        resource_name: "Main meter",
        sender_username: "owner-a",
      }), { status: 201 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetcher);

    await expect(listResourceInvitations()).resolves.toHaveLength(1);
    await expect(createDeviceResourceInvitation("meter-1", "user-b", "manager")).resolves.toMatchObject({
      id: "invite-2",
      permission: "manager",
    });
    await expect(acceptResourceInvitation("invite-1")).resolves.toBeUndefined();
    await expect(cancelResourceInvitation("invite-2")).resolves.toBeUndefined();

    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "/api/v1/devices/meter-1/resource-invitations",
      expect.objectContaining({
        body: JSON.stringify({ username: "user-b", permission: "manager" }),
        credentials: "same-origin",
        headers: { "content-type": "application/json" },
        method: "POST",
      }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      3,
      "/api/v1/resource-invitations/invite-1/accept",
      expect.objectContaining({ method: "POST" }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      4,
      "/api/v1/resource-invitations/invite-2/cancel",
      expect.objectContaining({ method: "POST" }),
    );
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

  it("loads only the selected resource profile chart configuration", async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        profile: { id: "profile-meter", name: "Power Meter v1" },
        charts: [{
          metric: "power_w",
          label: "Active power",
          unit: "W",
          color: "#167b83",
          aggregation: "last",
        }],
      }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        profile: { id: "profile-farm", name: "Power Farm v1" },
        charts: [{
          metric: "power_w",
          label: "Farm demand",
          unit: "W",
          aggregation: "sum",
        }],
      }), { status: 200 }));
    vi.stubGlobal("fetch", fetcher);

    await expect(getDeviceLiveView("meter-1")).resolves.toEqual({
      profile: { id: "profile-meter", name: "Power Meter v1" },
      charts: [{
        metric: "power_w",
        label: "Active power",
        unit: "W",
        color: "#167b83",
        aggregation: "last",
      }],
    });
    await expect(getAssetLiveView("asset-1")).resolves.toEqual({
      profile: { id: "profile-farm", name: "Power Farm v1" },
      charts: [{
        metric: "power_w",
        label: "Farm demand",
        unit: "W",
        aggregation: "sum",
      }],
    });

    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "/api/v1/devices/meter-1/live-view",
      expect.objectContaining({ credentials: "same-origin" }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "/api/v1/assets/asset-1/live-view",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("loads tenant profile catalogs read-only", async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { id: "meter-v1", name: "Power Meter v1" },
      ]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([
        { id: "farm-v1", name: "Power Farm v1" },
      ]), { status: 200 }));
    vi.stubGlobal("fetch", fetcher);

    await expect(listTenantProfiles("device")).resolves.toEqual([{ id: "meter-v1", name: "Power Meter v1" }]);
    await expect(listTenantProfiles("asset")).resolves.toEqual([{ id: "farm-v1", name: "Power Farm v1" }]);

    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "/api/v1/tenant-profile/profiles?kind=device",
      expect.objectContaining({ credentials: "same-origin" }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "/api/v1/tenant-profile/profiles?kind=asset",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("uses owner-scoped resource configuration routes through the BFF", async () => {
    const rule = {
      comparison: "gt" as const,
      enabled: true,
      for_seconds: 0,
      metric_key: "power_w",
      name: "High active power",
      reopen_grace_seconds: 3_600,
      reminder_interval_seconds: 86_400,
      resolve_after_seconds: 300,
      rule_type: "event_threshold" as const,
      severity: "warning" as const,
      threshold: 500,
      window_seconds: null,
    };
    const fetcher = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ device_id: "meter-1", display_name: "Renamed meter" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: "farm-1", name: "Renamed farm" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [] }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...rule, device_id: "meter-1", id: "rule-1" }), { status: 201 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...rule, device_id: "meter-1", id: "rule-1", threshold: 600 }), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetcher);

    await expect(updateDevice("meter-1", { asset_id: null, display_name: "Renamed meter" })).resolves.toMatchObject({ name: "Renamed meter" });
    await expect(updateAsset("farm-1", { name: "Renamed farm", parent_asset_id: null })).resolves.toMatchObject({ name: "Renamed farm" });
    await expect(listDeviceAlertRules("meter-1")).resolves.toEqual([]);
    await expect(createDeviceAlertRule("meter-1", rule)).resolves.toMatchObject({ id: "rule-1" });
    await expect(updateDeviceAlertRule("meter-1", "rule-1", { ...rule, threshold: 600 })).resolves.toMatchObject({ threshold: 600 });
    await expect(archiveDeviceAlertRule("meter-1", "rule-1")).resolves.toBeUndefined();

    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "/api/v1/devices/meter-1",
      expect.objectContaining({ body: JSON.stringify({ asset_id: null, display_name: "Renamed meter" }), method: "PATCH" }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      5,
      "/api/v1/devices/meter-1/alert-rules/rule-1",
      expect.objectContaining({ method: "PUT" }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      6,
      "/api/v1/devices/meter-1/alert-rules/rule-1",
      expect.objectContaining({ method: "DELETE" }),
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

  it("rejects an oversized raw array at the accumulated item budget", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(Array.from({ length: 2_501 }, (_, index) => ({ id: "device-" + index }))), {
        status: 200,
      }),
    );
    vi.stubGlobal("fetch", fetcher);

    await expect(listDevices()).rejects.toThrow("Platform pagination item budget exceeded.");
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("rejects cursor pages that exceed the accumulated item budget", async () => {
    const page = (offset: number) => Array.from(
      { length: 1_500 },
      (_, index) => ({ id: "device-" + (offset + index) }),
    );
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ has_more: true, items: page(0), next_cursor: "second" }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ has_more: false, items: page(1_500), next_cursor: null }), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetcher);

    await expect(listDevices()).rejects.toThrow("Platform pagination item budget exceeded.");
    expect(fetcher).toHaveBeenCalledTimes(2);
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
