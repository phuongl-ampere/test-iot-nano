// @vitest-environment jsdom

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { PowerMonitorLoginGate } from "../components/powermonitor-login-gate";

describe("PowerMonitorLoginGate", () => {
  it("submits platform credentials to the PowerMonitor BFF", () => {
    render(<PowerMonitorLoginGate />);

    expect(screen.getByRole("form", { name: "PowerMonitor sign in" }).getAttribute("action"))
      .toBe("/api/v1/auth/login");
    expect(screen.getByRole("form", { name: "PowerMonitor sign in" }).getAttribute("method"))
      .toBe("post");
    expect(screen.getByLabelText("Username").getAttribute("name")).toBe("username");
    expect(screen.getByLabelText("Password").getAttribute("type")).toBe("password");
    expect(screen.getByRole("button", { name: "Sign in" })).toBeTruthy();
  });

  it("shows a generic credential error", () => {
    render(<PowerMonitorLoginGate error="invalid_credentials" />);

    expect(screen.getByRole("alert").textContent).toBe("Username or password is incorrect.");
  });
});
