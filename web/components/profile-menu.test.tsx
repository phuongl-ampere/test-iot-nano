// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ProfileMenu } from "./profile-menu";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ProfileMenu", () => {
  it("shows profile, admin settings, and logout from the admin gear menu", () => {
    const onOpenProfile = vi.fn();
    const onOpenSystemConfiguration = vi.fn();
    const onLogout = vi.fn();

    render(
      <ProfileMenu
        onLogout={onLogout}
        onOpenProfile={onOpenProfile}
        onOpenSystemConfiguration={onOpenSystemConfiguration}
        role="admin"
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));
    fireEvent.click(screen.getByRole("button", { name: "User profile" }));
    fireEvent.click(screen.getByRole("button", { name: "Admin setting" }));
    fireEvent.click(screen.getByRole("button", { name: "Logout" }));

    expect(onOpenProfile).toHaveBeenCalledTimes(1);
    expect(onOpenSystemConfiguration).toHaveBeenCalledTimes(1);
    expect(onLogout).toHaveBeenCalledTimes(1);
  });

  it("hides the admin setting action from a viewer", () => {
    render(
      <ProfileMenu
        onLogout={vi.fn()}
        onOpenProfile={vi.fn()}
        onOpenSystemConfiguration={vi.fn()}
        role="viewer"
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));

    expect(screen.getByRole("button", { name: "User profile" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "Logout" })).not.toBeNull();
    expect(screen.queryByRole("button", { name: "Admin setting" })).toBeNull();
  });
});
