import { describe, expect, it, vi } from "vitest";

import {
  createCallbackHandler,
  createLoginHandler,
  exchangeAuthorizationCode,
  readSession,
} from "../lib/oauth";

process.env.SESSION_SECRET = "test-session-secret-with-sufficient-length";

describe("PowerMonitor OAuth BFF", () => {
  it("redirects to the platform with a state-bound S256 PKCE challenge", async () => {
    const cookies = { set: vi.fn() };
    const response = await createLoginHandler({
      requestUrl: "https://powermonitor.example.test/dashboard",
      cookies,
      config: {
        platformBaseUrl: "https://platform.example.test",
        clientId: "powermonitor-client",
        redirectUri: "https://powermonitor.example.test/api/auth/callback",
        scope: "devices:read telemetry:read",
      },
    });

    const location = new URL(response.headers.get("location") ?? "");
    expect(location.pathname).toBe("/oauth/authorize");
    expect(location.searchParams.get("client_id")).toBe("powermonitor-client");
    expect(location.searchParams.get("code_challenge_method")).toBe("S256");
    expect(location.searchParams.get("state")).toHaveLength(43);
    expect(location.searchParams.get("code_challenge")).toHaveLength(43);
    expect(cookies.set).toHaveBeenCalledWith(
      "powermonitor_oauth_state",
      expect.any(String),
      expect.objectContaining({ httpOnly: true, secure: true, sameSite: "lax" }),
    );
  });

  it("exchanges the callback code server-side and creates an HttpOnly session", async () => {
    const cookies = {
      get: vi.fn().mockReturnValue({ value: "sealed-state" }),
      set: vi.fn(),
    };
    const exchangeCode = vi.fn().mockResolvedValue({
      accessToken: "opaque-access-token",
      refreshToken: "opaque-refresh-token",
    });
    const response = await createCallbackHandler({
      requestUrl:
        "https://powermonitor.example.test/api/auth/callback?code=code-123&state=state-123",
      cookies,
      unsealState: vi.fn().mockReturnValue({ state: "state-123", verifier: "verifier-123" }),
      exchangeCode,
      redirectUri: "https://powermonitor.example.test/api/auth/callback",
    });

    expect(exchangeCode).toHaveBeenCalledWith({
      code: "code-123",
      codeVerifier: "verifier-123",
      redirectUri: "https://powermonitor.example.test/api/auth/callback",
    });
    expect(response.status).toBe(307);
    expect(response.headers.get("location")).toBe("https://powermonitor.example.test/");
    expect(cookies.set).toHaveBeenCalledWith(
      "powermonitor_session",
      expect.any(String),
      expect.objectContaining({ httpOnly: true, secure: true, sameSite: "lax" }),
    );
  });

  it("uses the configured callback URI behind a proxy", async () => {
    const cookies = {
      get: vi.fn().mockReturnValue({ value: "sealed-state" }),
      set: vi.fn(),
    };
    const exchangeCode = vi.fn().mockResolvedValue({ accessToken: "opaque-access-token" });
    const redirectUri = "https://powermonitor.example.test/api/auth/callback";
    const response = await createCallbackHandler({
      requestUrl: "https://proxy.example.test/api/auth/callback?code=code-123&state=state-123",
      cookies,
      unsealState: vi.fn().mockReturnValue({ state: "state-123", verifier: "verifier-123" }),
      exchangeCode,
      redirectUri,
    });

    expect(exchangeCode).toHaveBeenCalledWith({
      code: "code-123",
      codeVerifier: "verifier-123",
      redirectUri,
    });
    expect(response.headers.get("location")).toBe("https://powermonitor.example.test/");
  });

  it("form-encodes Basic credentials individually and omits duplicate body credentials", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ access_token: "opaque-access-token" }), { status: 200 }),
    );
    const config = {
      platformBaseUrl: "https://platform.example.test",
      clientId: "client: id+%",
      clientSecret: "secret: value+%",
      redirectUri: "https://powermonitor.example.test/api/auth/callback",
      scope: "devices:read",
    };

    await exchangeAuthorizationCode(
      config,
      {
        code: "code-123",
        codeVerifier: "verifier-123",
        redirectUri: config.redirectUri,
      },
      fetcher,
    );

    const [url, init] = fetcher.mock.calls[0] as [URL, RequestInit];
    expect(String(url)).toBe("https://platform.example.test/oauth/token");
    expect(new Headers(init.headers).get("authorization")).toBe(
      `Basic ${Buffer.from("client%3A+id%2B%25:secret%3A+value%2B%25").toString("base64")}`,
    );
    expect(String(init.body)).toBe(
      "grant_type=authorization_code&code=code-123&code_verifier=verifier-123&redirect_uri=https%3A%2F%2Fpowermonitor.example.test%2Fapi%2Fauth%2Fcallback",
    );
  });

  it("recovers only a valid encrypted application session for BFF bearer requests", async () => {
    const cookies = {
      get: vi.fn().mockReturnValue({ value: "sealed-state" }),
      set: vi.fn(),
    };

    await createCallbackHandler({
      requestUrl:
        "https://powermonitor.example.test/api/auth/callback?code=code-123&state=state-123",
      cookies,
      exchangeCode: vi.fn().mockResolvedValue({ accessToken: "opaque-access-token" }),
      redirectUri: "https://powermonitor.example.test/api/auth/callback",
      unsealState: vi.fn().mockReturnValue({ state: "state-123", verifier: "verifier-123" }),
    });

    const sessionValue = cookies.set.mock.calls.find(
      ([name]) => name === "powermonitor_session",
    )?.[1] as string;
    expect(readSession(sessionValue)).toMatchObject({ accessToken: "opaque-access-token" });
    expect(readSession("not-an-encrypted-session")).toBeNull();
  });
});
