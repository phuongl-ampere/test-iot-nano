import { describe, expect, it, vi } from "vitest";

const { cookies } = vi.hoisted(() => ({ cookies: vi.fn() }));

vi.mock("next/headers", () => ({ cookies }));

import { POST } from "../app/api/v1/auth/logout/route";
import { oauthStateCookieName, sessionCookieName } from "../lib/oauth";

describe("PowerMonitor logout route", () => {
  it("clears PowerMonitor cookies and returns to the login page", async () => {
    const removeCookie = vi.fn();
    cookies.mockResolvedValue({ delete: removeCookie });

    const response = await POST(new Request("http://localhost:3002/api/v1/auth/logout", {
      method: "POST",
    }));

    expect(removeCookie).toHaveBeenCalledWith(sessionCookieName);
    expect(removeCookie).toHaveBeenCalledWith(oauthStateCookieName);
    expect(response.status).toBe(303);
    expect(response.headers.get("location")).toBe("http://localhost:3002/");
  });
});
