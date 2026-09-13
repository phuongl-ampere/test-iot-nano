import { describe, expect, it, vi } from "vitest";

import { PlatformApiError, platformRequest } from "../lib/platform-client";

process.env.PLATFORM_BASE_URL = "https://platform.example.test";

describe("platformRequest", () => {
  it("calls only the versioned platform API with the session bearer token", async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response('{"devices":[]}', { status: 200 }));

    const response = await platformRequest("/devices", {
      accessToken: "access-token",
      fetcher,
    });

    expect(response.status).toBe(200);
    expect(fetcher).toHaveBeenCalledTimes(1);
    const [url, init] = fetcher.mock.calls[0] as [URL, RequestInit];
    expect(String(url)).toBe("https://platform.example.test/api/v1/devices");
    expect(init.cache).toBe("no-store");
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer access-token");
  });

  it("raises a bounded error when the platform denies the request", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response('{"code":"forbidden","message":"scope required"}', { status: 403 }),
    );

    await expect(
      platformRequest("/alerts", {
        accessToken: "access-token",
        fetcher,
      }),
    ).rejects.toEqual(new PlatformApiError(403, "Platform request denied"));
  });
});
