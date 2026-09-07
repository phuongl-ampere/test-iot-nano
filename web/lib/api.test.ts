import { afterEach, describe, expect, it, vi } from "vitest";

import {
  UnauthorizedApiError,
  archiveAlertRule,
  createAlertRule,
  createApiClient,
  deleteManagementAsset,
  deleteManagementDevice,
  fetchDevices,
  fetchSystemConfiguration,
  getCurrentUser,
  login,
  sendDeviceCommand,
  telemetryRequest,
  updateSystemConfiguration,
  updateAlertRule,
  updateManagementAsset,
} from "./api";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("telemetryRequest", () => {
  it("uses raw points for the one-hour range", () => {
    const request = telemetryRequest(
      "http://127.0.0.1:8080",
      "esp-000123",
      "1h",
      new Date("2026-09-04T12:00:00Z"),
    );

    expect(request).toBe(
      "http://127.0.0.1:8080/api/devices/esp-000123/telemetry?from=2026-09-04T11%3A00%3A00.000Z&to=2026-09-04T12%3A00%3A00.000Z&bucket=raw",
    );
  });

  it("uses one-hour aggregates for the seven-day range", () => {
    const request = telemetryRequest(
      "http://127.0.0.1:8080/",
      "esp-000123",
      "7d",
      new Date("2026-09-04T12:00:00Z"),
    );

    expect(request).toContain("bucket=1h");
    expect(request).toContain("from=2026-08-28T12%3A00%3A00.000Z");
  });
});

describe("sendDeviceCommand", () => {
  it("posts a selected command to the device command endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 202 }));
    vi.stubGlobal("fetch", fetchMock);
    const client = createApiClient("http://127.0.0.1:8080", "session_test");

    await sendDeviceCommand(client, "esp-000123", "sample_now");

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/api/devices/esp-000123/commands",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ authorization: "Session session_test" }),
        body: JSON.stringify({ command: "sample_now", parameters: {} }),
      }),
    );
  });
});

describe("createAlertRule", () => {
  it("posts a window average rule", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "rule-1" }), { status: 201 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const client = createApiClient("http://127.0.0.1:8080", "session_test");

    await createAlertRule(client, {
      name: "High average",
      metric_key: "temperature_c",
      rule_type: "window_average",
      comparison: "gt",
      threshold: 40,
      window_seconds: 300,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/api/alert-rules",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ authorization: "Session session_test" }),
        body: JSON.stringify({
          name: "High average",
          metric_key: "temperature_c",
          rule_type: "window_average",
          comparison: "gt",
          threshold: 40,
          window_seconds: 300,
        }),
      }),
    );
  });
});

describe("authenticated alert lifecycle API", () => {
  it("logs in with username and password then receives an opaque session", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          role: "admin",
          session_id: "session_test",
          username: "admin",
          default_app: "/apps/powermonitor",
          granted_apps: ["powermonitor"],
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(login("http://127.0.0.1:8080", "admin", "Aa1!bcDe")).resolves.toEqual({
      role: "admin",
      sessionId: "session_test",
      username: "admin",
      defaultApp: "/apps/powermonitor",
      grantedApps: ["powermonitor"],
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/api/auth/login",
      expect.objectContaining({
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ username: "admin", password: "Aa1!bcDe" }),
      }),
    );
  });

  it("maps the current user routing metadata from the session API", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify({
        role: "viewer",
        username: "viewer",
        default_app: "/apps/powermonitor",
        granted_apps: ["powermonitor"],
      }), { status: 200 })),
    );
    const client = createApiClient("http://127.0.0.1:8080", "session_viewer");

    await expect(getCurrentUser(client)).resolves.toEqual({
      role: "viewer",
      username: "viewer",
      defaultApp: "/apps/powermonitor",
      grantedApps: ["powermonitor"],
    });
  });

  it("updates and archives rules with the authenticated client", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: "rule-1" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const client = createApiClient("http://127.0.0.1:8080", "session_test");
    const rule = {
      name: "High temperature",
      metric_key: "temperature_c",
      rule_type: "event_threshold" as const,
      comparison: "gt" as const,
      threshold: 42,
    };

    await updateAlertRule(client, "rule-1", rule);
    await archiveAlertRule(client, "rule-1");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "http://127.0.0.1:8080/api/alert-rules/rule-1",
      expect.objectContaining({
        method: "PUT",
        headers: expect.objectContaining({ authorization: "Session session_test" }),
        body: JSON.stringify(rule),
      }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8080/api/alert-rules/rule-1",
      expect.objectContaining({
        method: "DELETE",
        headers: expect.objectContaining({ authorization: "Session session_test" }),
      }),
    );
  });

  it("turns an expired session into an UnauthorizedApiError", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 401 })));
    const client = createApiClient("http://127.0.0.1:8080", "session_test");

    await expect(fetchDevices(client)).rejects.toBeInstanceOf(UnauthorizedApiError);
  });
});

describe("system configuration API", () => {
  const configuration = {
    mqtt: {
      host: "127.0.0.1",
      port: 1883,
    },
    smtp: {
      enabled: true,
      host: "smtp.example.test",
      port: 465,
      username: "alerts@example.test",
      password_configured: true,
      from: "alerts@example.test",
      to: "ops@example.test",
      timeout_seconds: 15,
    },
    tuning: {
      retention_bytes: 2147483648,
      retention_seconds: 86400,
      segment_bytes: 134217728,
      max_record_bytes: 1048576,
      writer_batch_size: 1000,
      alert_batch_size: 250,
      notification_batch_size: 10,
      writer_flush_seconds: 1,
      alert_event_interval_milliseconds: 250,
      alert_window_interval_seconds: 60,
      notification_interval_seconds: 1,
      retention_interval_seconds: 60,
      notification_lease_seconds: 30,
      notification_retry_base_seconds: 1,
      notification_retry_max_seconds: 3600,
    },
  };

  it("reads and saves only through authenticated system endpoints", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(configuration), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(configuration), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const client = createApiClient("http://127.0.0.1:8080", "session_admin");

    await expect(fetchSystemConfiguration(client)).resolves.toEqual(configuration);
    await updateSystemConfiguration(client, {
      ...configuration,
      smtp: { ...configuration.smtp, password: "new-secret" },
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "http://127.0.0.1:8080/api/system-configuration",
      expect.objectContaining({
        headers: expect.objectContaining({ authorization: "Session session_admin" }),
      }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8080/api/system-configuration",
      expect.objectContaining({
        method: "PUT",
        body: expect.stringContaining("\"password\":\"new-secret\""),
      }),
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});

describe("management entity mutations", () => {
  it("updates and deletes assets and devices through management routes", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: "asset-1" }), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const client = createApiClient("http://127.0.0.1:8080", "session_admin");

    await updateManagementAsset(client, "asset-1", {
      name: "Edited asset",
      asset_profile_id: null,
      parent_asset_id: null,
      metadata: {},
      attributes: {},
    });
    await deleteManagementAsset(client, "asset-1");
    await deleteManagementDevice(client, "device-1");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "http://127.0.0.1:8080/api/management/assets/asset-1",
      expect.objectContaining({ method: "PUT" }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8080/api/management/assets/asset-1",
      expect.objectContaining({ method: "DELETE" }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      "http://127.0.0.1:8080/api/management/devices/device-1",
      expect.objectContaining({ method: "DELETE" }),
    );
  });
});
