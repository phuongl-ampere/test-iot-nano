import { describe, expect, it, vi } from "vitest";

const {
  cookies,
  createLoginHandler,
  createPasswordLoginHandler,
  oauthConfigFromEnvironment,
  platformAuthBaseUrlFromEnvironment,
} = vi.hoisted(() => ({
  cookies: vi.fn(),
  createLoginHandler: vi.fn(),
  createPasswordLoginHandler: vi.fn(),
  oauthConfigFromEnvironment: vi.fn(),
  platformAuthBaseUrlFromEnvironment: vi.fn(),
}));

vi.mock("next/headers", () => ({ cookies }));
vi.mock("../lib/oauth", () => ({
  createLoginHandler,
  createPasswordLoginHandler,
  oauthConfigFromEnvironment,
  platformAuthBaseUrlFromEnvironment,
}));

import { POST } from "../app/api/v1/auth/login/route";

describe("PowerMonitor password login route", () => {
  it("accepts a browser origin that matches the incoming Host", async () => {
    const handoff = new Response(null, {
      headers: { location: "http://127.0.0.1:3001/api/v1/auth/callback?code=code&state=state" },
      status: 303,
    });
    cookies.mockResolvedValue({ set: vi.fn() });
    createPasswordLoginHandler.mockResolvedValue(handoff);
    oauthConfigFromEnvironment.mockReturnValue({
      clientId: "client",
      redirectUri: "https://powermonitor.example.test/api/v1/auth/callback",
    });
    platformAuthBaseUrlFromEnvironment.mockReturnValue("http://127.0.0.1:18081");

    const response = await POST(new Request("http://internal-powermonitor:3001/api/v1/auth/login", {
      body: new URLSearchParams({ password: "correct-password", username: "operator" }),
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        host: "powermonitor.example.test",
        origin: "https://powermonitor.example.test",
      },
      method: "POST",
    }));

    expect(response).toBe(handoff);
    expect(createPasswordLoginHandler).toHaveBeenCalledWith(expect.objectContaining({
      password: "correct-password",
      username: "operator",
    }));
  });

  it("rejects a cross-origin password submission", async () => {
    oauthConfigFromEnvironment.mockReturnValue({
      clientId: "client",
      redirectUri: "https://powermonitor.example.test/api/v1/auth/callback",
    });

    const response = await POST(new Request("http://internal-powermonitor:3001/api/v1/auth/login", {
      body: new URLSearchParams({ password: "correct-password", username: "operator" }),
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        host: "powermonitor.example.test",
        origin: "https://attacker.example.test",
      },
      method: "POST",
    }));

    expect(response.status).toBe(403);
  });
});
