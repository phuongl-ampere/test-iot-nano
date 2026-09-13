import { describe, expect, it, vi } from "vitest";

import { exchangeServiceAccessToken } from "../lib/service-client";

describe("confidential service client", () => {
  it("uses only its explicitly configured scope and confidential credentials", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ access_token: "service-token", expires_in: 300 }), {
        status: 200,
      }),
    );

    const token = await exchangeServiceAccessToken(
      {
        clientId: "powermonitor-service",
        clientSecret: "server-only-secret",
        platformBaseUrl: "https://platform.example.test",
        scope: "telemetry:aggregate",
      },
      fetcher,
    );

    expect(token).toEqual({ accessToken: "service-token", expiresIn: 300 });
    const [url, init] = fetcher.mock.calls[0] as [URL, RequestInit];
    expect(String(url)).toBe("https://platform.example.test/oauth/token");
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get("authorization")).toBe(
      "Basic " + Buffer.from("powermonitor-service:server-only-secret").toString("base64"),
    );
    expect(String(init.body)).toBe("grant_type=client_credentials&scope=telemetry%3Aaggregate");
  });
});
