// @vitest-environment jsdom

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { PowerMonitorLoginGate } from "../components/powermonitor-login-gate";

describe("PowerMonitorLoginGate", () => {
  it("starts the existing OAuth BFF route without rendering a password form", () => {
    render(<PowerMonitorLoginGate />);

    expect(screen.getByRole("link", { name: "Continue to sign in" }).getAttribute("href"))
      .toBe("/api/auth/login");
    expect(screen.queryByLabelText(/password/i)).toBeNull();
  });
});
