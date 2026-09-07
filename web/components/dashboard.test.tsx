// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Dashboard } from "./dashboard";

const systemConfiguration = {
  mqtt: {
    host: "127.0.0.1",
    port: 1883,
  },
  smtp: {
    enabled: false,
    host: null,
    port: 465,
    username: null,
    password_configured: false,
    from: null,
    to: null,
    timeout_seconds: 15,
  },
  tuning: {
    retention_bytes: 2147483648,
    retention_seconds: 86400,
    segment_bytes: 134217728,
    max_record_bytes: 1048576,
    writer_batch_size: 1000,
    alert_batch_size: 250,
    notification_batch_size: 10,
    writer_flush_seconds: 1,
    alert_event_interval_milliseconds: 250,
    alert_window_interval_seconds: 60,
    notification_interval_seconds: 1,
    retention_interval_seconds: 60,
    notification_lease_seconds: 30,
    notification_retry_base_seconds: 1,
    notification_retry_max_seconds: 3600,
  },
};

afterEach(() => {
  cleanup();
  sessionStorage.clear();
  vi.restoreAllMocks();
});

describe("Dashboard", () => {
  it("keeps a stored session ID when session restoration has an infrastructure failure", async () => {
    sessionStorage.setItem("rush-iot-nano.session-id", "session_test");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 500 })));

    render(<Dashboard />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "Access" })).not.toBeNull();
    });
    expect(sessionStorage.getItem("rush-iot-nano.session-id")).toBe("session_test");
  });

  it("shows Admin setting inside the admin gear menu", async () => {
    sessionStorage.setItem("rush-iot-nano.session-id", "session_admin");
    vi.stubGlobal("fetch", vi.fn((input: string) => {
      if (input.endsWith("/api/auth/me")) {
        return Promise.resolve(new Response(JSON.stringify({ role: "admin" }), { status: 200 }));
      }
      return Promise.resolve(new Response(JSON.stringify([]), { status: 200 }));
    }));

    render(<Dashboard />);

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Open account menu" })).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));
    expect(screen.getByRole("button", { name: "Admin setting" })).not.toBeNull();
  });

  it("opens the User profile view from the gear menu", async () => {
    sessionStorage.setItem("rush-iot-nano.session-id", "session_admin");
    vi.stubGlobal("fetch", vi.fn((input: string) => {
      if (input.endsWith("/api/auth/me")) {
        return Promise.resolve(new Response(JSON.stringify({ role: "admin" }), { status: 200 }));
      }
      return Promise.resolve(new Response(JSON.stringify([]), { status: 200 }));
    }));

    render(<Dashboard />);

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Open account menu" })).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));
    fireEvent.click(screen.getByRole("button", { name: "User profile" }));

    expect(screen.getByRole("heading", { name: "User profile" })).not.toBeNull();
  });

  it("opens Admin setting from the admin gear menu", async () => {
    sessionStorage.setItem("rush-iot-nano.session-id", "session_admin");
    vi.stubGlobal("fetch", vi.fn((input: string) => {
      if (input.endsWith("/api/auth/me")) {
        return Promise.resolve(new Response(JSON.stringify({ role: "admin" }), { status: 200 }));
      }
      if (input.endsWith("/api/system-configuration")) {
        return Promise.resolve(new Response(JSON.stringify(systemConfiguration), { status: 200 }));
      }
      return Promise.resolve(new Response(JSON.stringify([]), { status: 200 }));
    }));

    render(<Dashboard />);

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Open account menu" })).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));
    fireEvent.click(screen.getByRole("button", { name: "Admin setting" }));

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "System Configuration" })).not.toBeNull();
    });
  });

  it("does not expose System Configuration to a viewer session", async () => {
    sessionStorage.setItem("rush-iot-nano.session-id", "session_viewer");
    vi.stubGlobal("fetch", vi.fn((input: string) => {
      if (input.endsWith("/api/auth/me")) {
        return Promise.resolve(new Response(JSON.stringify({ role: "viewer" }), { status: 200 }));
      }
      return Promise.resolve(new Response(JSON.stringify([]), { status: 200 }));
    }));

    render(<Dashboard />);

    await waitFor(() => {
      expect(screen.getByText("No device selected")).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Open account menu" }));
    expect(screen.queryByRole("button", { name: "Admin setting" })).toBeNull();
  });
});
