// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ProfileMenu } from "./profile-menu";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ProfileMenu", () => {
  it("shows profile, system settings, and logout from the system gear menu", () => {
    const onOpenProfile = vi.fn();
    const onOpenSystemConfiguration = vi.fn();
    const onLogout = vi.fn();

    render(
      <ProfileMenu
        onLogout={onLogout}
        onOpenProfile={onOpenProfile}
        onOpenSystemConfiguration={onOpenSystemConfiguration}
        role="admin"
        accountClass="system"
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));
    fireEvent.click(screen.getByRole("button", { name: "User profile" }));
    fireEvent.click(screen.getByRole("button", { name: "System setting" }));
    fireEvent.click(screen.getByRole("button", { name: "Logout" }));

    expect(onOpenProfile).toHaveBeenCalledTimes(1);
    expect(onOpenSystemConfiguration).toHaveBeenCalledTimes(1);
    expect(onLogout).toHaveBeenCalledTimes(1);
  });

  it("hides the system setting action from admin and user accounts", () => {
    render(
      <ProfileMenu
        onLogout={vi.fn()}
        onOpenProfile={vi.fn()}
        onOpenSystemConfiguration={vi.fn()}
        role="admin"
        accountClass="admin"
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));

    expect(screen.getByRole("button", { name: "User profile" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "Logout" })).not.toBeNull();
    expect(screen.queryByRole("button", { name: "System setting" })).toBeNull();
  });
});
