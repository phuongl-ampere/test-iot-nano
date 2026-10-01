import { describe, expect, it, vi } from "vitest";

import {
  createPasswordLoginHandler,
  oauthStateCookieName,
  sessionCookieName,
  type OAuthConfig,
} from "../lib/oauth";

process.env.SESSION_SECRET = "test-session-secret-with-sufficient-length";

const config: OAuthConfig = {
  platformBaseUrl: "https://public.example.test",
  clientId: "powermonitor-client",
  clientSecret: "client-secret",
  redirectUri: "https://powermonitor.example.test/api/auth/callback",
  scope: "devices:read",
};

describe("PowerMonitor password OAuth handoff", () => {
  it("logs in with iot-api then exchanges the matching PKCE code server-side", async () => {
    const cookies = { set: vi.fn() };
    const exchangeCode = vi.fn().mockResolvedValue({ accessToken: "opaque-access-token" });
    const sealSession = vi.fn().mockReturnValue("sealed-powermonitor-session");
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.pathname === "/api/auth/login") {
        expect(init).toMatchObject({
          body: JSON.stringify({ username: "operator", password: "correct-password" }),
          headers: { "content-type": "application/json" },
          method: "POST",
        });
        return new Response(null, {
          headers: { "set-cookie": "iot_nano_session=platform-session; HttpOnly; Path=/" },
          status: 200,
        });
      }

      expect(init).toMatchObject({
        cache: "no-store",
        headers: { cookie: "iot_nano_session=platform-session" },
        redirect: "manual",
      });
      return new Response(null, {
        headers: {
          location: `${config.redirectUri}?code=authorization-code&state=${url.searchParams.get("state")}`,
        },
        status: 302,
      });
    });

    const response = await createPasswordLoginHandler({
      appBaseUrl: "https://powermonitor.example.test",
      authBaseUrl: "https://management.example.test",
      config,
      cookies,
      exchangeCode,
      fetcher,
      password: "correct-password",
      sealSession,
      username: "operator",
    });

    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("https://powermonitor.example.test/");
    expect(exchangeCode).toHaveBeenCalledWith(expect.objectContaining({
      code: "authorization-code",
      redirectUri: config.redirectUri,
    }));
    expect(cookies.set).toHaveBeenCalledWith(
      sessionCookieName,
      "sealed-powermonitor-session",
      expect.objectContaining({ httpOnly: true, secure: false, sameSite: "lax" }),
    );
    expect(cookies.set).not.toHaveBeenCalledWith(
      oauthStateCookieName,
      expect.anything(),
      expect.anything(),
    );
  });

  it("rejects invalid credentials without issuing OAuth state", async () => {
    const cookies = { set: vi.fn() };
    const fetcher = vi.fn().mockResolvedValue(new Response(null, { status: 401 }));

    const response = await createPasswordLoginHandler({
      appBaseUrl: "https://powermonitor.example.test",
      authBaseUrl: "https://management.example.test",
      config,
      cookies,
      fetcher,
      password: "wrong-password",
      username: "operator",
    });

    expect(response.status).toBe(303);
    expect(response.headers.get("location"))
      .toBe("https://powermonitor.example.test/?login_error=invalid_credentials");
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(cookies.set).not.toHaveBeenCalled();
  });

  it("rejects a mismatched authorization state without issuing OAuth state", async () => {
    const cookies = { set: vi.fn() };
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname === "/api/auth/login") {
        return new Response(null, {
          headers: { "set-cookie": "iot_nano_session=platform-session; HttpOnly; Path=/" },
          status: 200,
        });
      }
      return new Response(null, {
        headers: { location: `${config.redirectUri}?code=authorization-code&state=wrong-state` },
        status: 302,
      });
    });

    const response = await createPasswordLoginHandler({
      appBaseUrl: "https://powermonitor.example.test",
      authBaseUrl: "https://management.example.test",
      config,
      cookies,
      fetcher,
      password: "correct-password",
      username: "operator",
    });

    expect(response.status).toBe(303);
    expect(response.headers.get("location"))
      .toBe("https://powermonitor.example.test/?login_error=platform_unavailable");
    expect(cookies.set).not.toHaveBeenCalled();
  });
});
