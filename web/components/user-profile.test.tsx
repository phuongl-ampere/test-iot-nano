// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { UserProfilePanel } from "./user-profile";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("UserProfilePanel", () => {
  it("changes only the signed-in user's password and clears the session", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const onPasswordChanged = vi.fn();

    render(
      <UserProfilePanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_viewer" }}
        onBack={vi.fn()}
        onPasswordChanged={onPasswordChanged}
        onUnauthorized={vi.fn()}
        role="viewer"
      />,
    );

    expect(screen.getByText("viewer")).not.toBeNull();
    fireEvent.change(screen.getByLabelText("Current password"), {
      target: { value: "NanoView@1234" },
    });
    fireEvent.change(screen.getByLabelText("New password"), {
      target: { value: "ViewerNext#2026" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save password" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "http://127.0.0.1:8080/api/auth/password",
        expect.objectContaining({
          body: JSON.stringify({
            current_password: "NanoView@1234",
            new_password: "ViewerNext#2026",
          }),
          method: "PUT",
        }),
      );
    });
    expect(onPasswordChanged).toHaveBeenCalledTimes(1);
  });

  it("delegates an expired session to the dashboard", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 401 })));
    const onUnauthorized = vi.fn();

    render(
      <UserProfilePanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_viewer" }}
        onBack={vi.fn()}
        onPasswordChanged={vi.fn()}
        onUnauthorized={onUnauthorized}
        role="viewer"
      />,
    );

    fireEvent.change(screen.getByLabelText("Current password"), {
      target: { value: "NanoView@1234" },
    });
    fireEvent.change(screen.getByLabelText("New password"), {
      target: { value: "ViewerNext#2026" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save password" }));

    await waitFor(() => expect(onUnauthorized).toHaveBeenCalledTimes(1));
  });
});
