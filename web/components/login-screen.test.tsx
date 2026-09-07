// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { LoginScreen } from "./login-screen";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("LoginScreen", () => {
  it("does not expose a session retry action", () => {
    render(
      <LoginScreen
        apiBaseUrl="http://127.0.0.1:8080"
        onAuthenticated={vi.fn()}
        sessionError="Unable to restore the current session."
      />,
    );

    expect(screen.queryByRole("button", { name: "Retry session" })).toBeNull();
  });

  it("logs in with username and password then returns an opaque session client", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({ role: "admin", session_id: "session_test_admin", username: "admin" }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const onAuthenticated = vi.fn();

    render(
      <LoginScreen
        apiBaseUrl="http://127.0.0.1:8080"
        onAuthenticated={onAuthenticated}
      />,
    );

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "admin" },
    });
    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "NanoAdmin@1234" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => {
      expect(onAuthenticated).toHaveBeenCalledWith(
        expect.objectContaining({ sessionId: "session_test_admin" }),
        "admin",
      );
    });
  });
});
