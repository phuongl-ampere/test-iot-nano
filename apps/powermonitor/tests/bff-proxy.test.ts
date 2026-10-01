import { describe, expect, it, vi } from "vitest";

import { createBffResponse } from "../lib/bff";
import { PlatformApiError } from "../lib/platform-client";

describe("generic platform BFF", () => {
  it("forwards a browser command only to the matching generic API resource", async () => {
    const platformRequest = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "command-1", state: "queued" }), {
        headers: { "content-type": "application/json" },
        status: 202,
      }),
    );
    const request = new Request(
      "https://powermonitor.example.test/api/v1/devices/meter-1/commands?wait=true",
      {
        body: JSON.stringify({ method: "switch_on", params: {} }),
        headers: {
          "content-type": "application/json",
          "idempotency-key": "command-key-1",
        },
        method: "POST",
      },
    );

    const response = await createBffResponse({
      platformRequest,
      request,
      session: { accessToken: "opaque-user-token" },
    });

    expect(response.status).toBe(202);
    expect(await response.json()).toEqual({ id: "command-1", state: "queued" });
    expect(platformRequest).toHaveBeenCalledTimes(1);
    const [path, session, init] = platformRequest.mock.calls[0] as [
      string,
      { accessToken: string },
      RequestInit,
    ];
    expect(path).toBe("/devices/meter-1/commands?wait=true");
    expect(session).toEqual({ accessToken: "opaque-user-token" });
    expect(init.method).toBe("POST");
    expect(init.body).toBe(JSON.stringify({ method: "switch_on", params: {} }));
    expect(new Headers(init.headers).get("content-type")).toBe("application/json");
    expect(new Headers(init.headers).get("idempotency-key")).toBe("command-key-1");
    expect(new Headers(init.headers).get("authorization")).toBeNull();
  });

  it("returns a bounded forbidden response when the platform denies a user scope", async () => {
    const platformRequest = vi.fn().mockRejectedValue(
      new PlatformApiError(403, "Platform request denied"),
    );

    const response = await createBffResponse({
      platformRequest,
      request: new Request("https://powermonitor.example.test/api/v1/alerts"),
      session: { accessToken: "opaque-user-token" },
    });

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toMatchObject({ code: "forbidden" });
  });

  it("never proxies profile assignment mutations from PowerMonitor", async () => {
    const platformRequest = vi.fn();

    const response = await createBffResponse({
      platformRequest,
      request: new Request(
        "https://powermonitor.example.test/api/v1/devices/meter-1/tenant-profile",
        { method: "PUT" },
      ),
      session: { accessToken: "opaque-user-token" },
    });

    expect(response.status).toBe(405);
    await expect(response.json()).resolves.toMatchObject({ code: "profile_read_only" });
    expect(platformRequest).not.toHaveBeenCalled();
  });

  it("never forwards a profile field through a generic resource patch", async () => {
    const platformRequest = vi.fn();

    const response = await createBffResponse({
      platformRequest,
      request: new Request("https://powermonitor.example.test/api/v1/devices/meter-1", {
        body: JSON.stringify({ device_profile_id: "other-profile" }),
        headers: { "content-type": "application/json" },
        method: "PATCH",
      }),
      session: { accessToken: "opaque-user-token" },
    });

    expect(response.status).toBe(405);
    await expect(response.json()).resolves.toMatchObject({ code: "profile_read_only" });
    expect(platformRequest).not.toHaveBeenCalled();
  });

  it("never proxies device token reveal or regeneration from PowerMonitor", async () => {
    const platformRequest = vi.fn();

    const response = await createBffResponse({
      platformRequest,
      request: new Request("https://powermonitor.example.test/api/v1/devices/meter-1/token", {
        method: "POST",
      }),
      session: { accessToken: "opaque-user-token" },
    });

    expect(response.status).toBe(405);
    await expect(response.json()).resolves.toMatchObject({ code: "token_unavailable" });
    expect(platformRequest).not.toHaveBeenCalled();
  });
});
