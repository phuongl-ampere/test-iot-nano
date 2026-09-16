import { afterEach, describe, expect, it, vi } from "vitest";

const { cookies } = vi.hoisted(() => ({ cookies: vi.fn() }));

vi.mock("next/headers", () => ({ cookies }));

import { createCallbackHandler, sessionCookieName } from "../lib/oauth";
import { hasPowerMonitorSession } from "../lib/page-session";

process.env.SESSION_SECRET = "test-session-secret-with-sufficient-length";

afterEach(() => {
  vi.clearAllMocks();
});

describe("PowerMonitor page session", () => {
  it("accepts a valid sealed OAuth session cookie", async () => {
    const oauthCookies = {
      get: vi.fn().mockReturnValue({ value: "sealed-state" }),
      set: vi.fn(),
    };
    await createCallbackHandler({
      requestUrl:
        "https://powermonitor.example.test/api/auth/callback?code=code-123&state=state-123",
      cookies: oauthCookies,
      exchangeCode: vi.fn().mockResolvedValue({ accessToken: "opaque-access-token" }),
      redirectUri: "https://powermonitor.example.test/api/auth/callback",
      unsealState: vi.fn().mockReturnValue({ state: "state-123", verifier: "verifier-123" }),
    });
    const value = oauthCookies.set.mock.calls.find(([name]) => name === sessionCookieName)?.[1] as string;
    const get = vi.fn().mockReturnValue({ value });
    cookies.mockResolvedValue({ get });

    expect(await hasPowerMonitorSession()).toBe(true);
    expect(get).toHaveBeenCalledWith(sessionCookieName);
  });

  it("rejects missing and malformed session cookies", async () => {
    cookies.mockResolvedValue({ get: vi.fn().mockReturnValue(undefined) });
    expect(await hasPowerMonitorSession()).toBe(false);

    cookies.mockResolvedValue({ get: vi.fn().mockReturnValue({ value: "not-a-session" }) });
    expect(await hasPowerMonitorSession()).toBe(false);
  });
});
