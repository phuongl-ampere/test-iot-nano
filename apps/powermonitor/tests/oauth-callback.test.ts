import { describe, expect, it, vi } from "vitest";

import { createCallbackHandler, createLoginHandler } from "../lib/oauth";

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
});
